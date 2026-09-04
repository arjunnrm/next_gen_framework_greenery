#!/usr/bin/env python
"""Generate deterministic synthetic BER payloads for the five BT_Testing ASN.1 schemas.

For each schema this script builds 10 records of its root PDU, BER-encodes each one,
and writes the 10 encodings **concatenated back to back** into a single ``.ber`` file.

Framing
-------
No length prefix, no separator, no terminator: the file is exactly the concatenation of
10 self-delimiting BER TLVs. A reader delimits records by walking the TLV header of the
byte at the current offset -- read the identifier octets (long-form tag numbers continue
while bit 8 is set), then the length octets (short form = 1 byte, long form = 0x80 | n
followed by n big-endian length bytes) -- and advancing by ``header_len + content_len``.
Indefinite length (0x80) is never emitted by these encodings, so a reader does not need
to hunt for end-of-content octets. ``asn1tools``' ``decode(..., check_constraints=False)``
on the remaining buffer, paired with ``decode_length()`` to find the record boundary, is
the reference implementation -- see ``iter_records()`` below.

Root PDUs (all CHOICE types)::

    EMSC.asn1      CallDataRecord
    GGSN.asn1      CallEventRecord
    PSGW.asn1      CallEventRecord
    TAP.310.asn1   DataInterChange
    TAP.311.asn1   DataInterChange

Determinism
-----------
Every value is derived from ``random.Random(SEED + record_index)`` plus the field path,
so regenerating produces byte-identical files. No wall-clock, no unseeded randomness.

Usage::

    python scripts/generate_synthetic_ber.py            # generate + verify
    python scripts/generate_synthetic_ber.py --verify    # verify existing files only
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path
from typing import Any

import asn1tools

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = REPO_ROOT / "flowx_testing" / "BT_Testing"
OUTPUT_DIR = SCHEMA_DIR / "synthetic"

SEED = 20260903
RECORDS_PER_FILE = 10

# schema file -> (root PDU name, preferred CHOICE member for the root, output basename)
PROTOCOLS: list[tuple[str, str, str | None, str]] = [
    ("EMSC.asn1", "CallDataRecord", "uMTSGSMPLMNCallDataRecord", "emsc_synthetic.ber"),
    ("GGSN.asn1", "CallEventRecord", "sgsnMMRecord", "ggsn_synthetic.ber"),
    ("PSGW.asn1", "CallEventRecord", "pGWRecord", "psgw_synthetic.ber"),
    ("TAP.310.asn1", "DataInterChange", "notification", "tap310_synthetic.ber"),
    ("TAP.311.asn1", "DataInterChange", "notification", "tap311_synthetic.ber"),
]

# How deep to descend before refusing to expand further optional structure.
MAX_DEPTH = 12

BUILTIN_TYPES = {
    "INTEGER",
    "BOOLEAN",
    "NULL",
    "REAL",
    "ENUMERATED",
    "BIT STRING",
    "OCTET STRING",
    "OBJECT IDENTIFIER",
    "UTF8String",
    "NumericString",
    "PrintableString",
    "IA5String",
    "VisibleString",
    "GeneralString",
    "GraphicString",
    "TeletexString",
    "T61String",
    "VideotexString",
    "UniversalString",
    "BMPString",
    "ObjectDescriptor",
    "UTCTime",
    "GeneralizedTime",
    "ANY",
    "ANY DEFINED BY",
    "SEQUENCE",
    "SET",
    "SEQUENCE OF",
    "SET OF",
    "CHOICE",
    "EXTERNAL",
    "OpenType",
}

STRING_TYPES = {
    "UTF8String",
    "PrintableString",
    "IA5String",
    "VisibleString",
    "GeneralString",
    "GraphicString",
    "TeletexString",
    "T61String",
    "VideotexString",
    "UniversalString",
    "BMPString",
    "ObjectDescriptor",
}


class SpecIndex:
    """Flattened ``{type_name: definition}`` view across every module in one file."""

    def __init__(self, parsed: dict[str, Any]) -> None:
        self.types: dict[str, Any] = {}
        self.values: dict[str, Any] = {}
        for module in parsed.values():
            for name, definition in module.get("types", {}).items():
                self.types.setdefault(name, definition)
            for name, definition in module.get("values", {}).items():
                self.values.setdefault(name, definition)

    def resolve(self, type_name: str) -> dict[str, Any] | None:
        """Follow a chain of type aliases down to a structural definition."""
        seen: set[str] = set()
        current = type_name
        while current not in BUILTIN_TYPES:
            if current in seen or current not in self.types:
                return None
            seen.add(current)
            definition = self.types[current]
            nested = definition.get("type")
            if nested in BUILTIN_TYPES:
                return definition
            current = nested
        return {"type": current}


def _is_mandatory(member: dict[str, Any]) -> bool:
    return "optional" not in member and "default" not in member


def _members(definition: dict[str, Any]) -> list[dict[str, Any]]:
    """Members of a SEQUENCE/SET/CHOICE, with extension markers (``None``) dropped."""
    return [m for m in definition.get("members", []) if m is not None]


def _size_bounds(definition: dict[str, Any]) -> tuple[int | None, int | None]:
    size = definition.get("size")
    if not size:
        return None, None
    entry = size[0]
    if isinstance(entry, int):
        return entry, entry
    if isinstance(entry, (list, tuple)) and len(entry) == 2:
        low, high = entry
        low = low if isinstance(low, int) else None
        high = high if isinstance(high, int) else None
        return low, high
    return None, None


def _int_bounds(definition: dict[str, Any]) -> tuple[int | None, int | None]:
    restricted = definition.get("restricted-to")
    if not restricted:
        return None, None
    entry = restricted[0]
    if isinstance(entry, int):
        return entry, entry
    if isinstance(entry, (list, tuple)) and len(entry) == 2:
        low, high = entry
        low = low if isinstance(low, int) else None
        high = high if isinstance(high, int) else None
        return low, high
    return None, None


class ValueBuilder:
    """Builds an asn1tools-shaped Python value for any type in a parsed spec."""

    def __init__(self, index: SpecIndex, rng: random.Random) -> None:
        self.index = index
        self.rng = rng

    # -- entry point ----------------------------------------------------------

    def build_named(self, type_name: str, depth: int = 0) -> Any:
        definition = self.index.resolve(type_name)
        if definition is None:
            # Unknown/unresolvable alias: an octet string is the safest stand-in.
            return b"\x01"
        # Carry constraints declared on the alias itself (e.g. `Foo ::= INTEGER (0..99)`).
        merged = dict(definition)
        alias = self.index.types.get(type_name)
        if isinstance(alias, dict):
            for key in ("size", "restricted-to", "values", "members", "element"):
                if key in alias and key not in merged:
                    merged[key] = alias[key]
        return self.build(merged, depth)

    def build(self, definition: dict[str, Any], depth: int = 0) -> Any:
        kind = definition.get("type")

        if kind in ("SEQUENCE", "SET"):
            return self._build_struct(definition, depth)
        if kind == "CHOICE":
            return self._build_choice(definition, depth)
        if kind in ("SEQUENCE OF", "SET OF"):
            return self._build_list(definition, depth)
        if kind == "INTEGER":
            return self._build_integer(definition)
        if kind == "ENUMERATED":
            return self._build_enumerated(definition)
        if kind == "BOOLEAN":
            return bool(self.rng.getrandbits(1))
        if kind == "NULL":
            return None
        if kind == "REAL":
            return float(self.rng.randrange(1, 1000))
        if kind == "BIT STRING":
            return self._build_bit_string(definition)
        if kind in ("OCTET STRING", "ANY", "ANY DEFINED BY", "OpenType", "EXTERNAL"):
            return self._build_octet_string(definition)
        if kind == "OBJECT IDENTIFIER":
            return "1.3.6.1.4.1.99999"
        if kind == "NumericString":
            return self._build_numeric_string(definition)
        if kind in STRING_TYPES:
            return self._build_string(definition)
        if kind in ("UTCTime", "GeneralizedTime"):
            import datetime

            day = 1 + self.rng.randrange(28)
            return datetime.datetime(2026, 1, day, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # Unhandled builtin -- fall back to octets rather than emitting nothing.
        return b"\x00"

    # -- structural -----------------------------------------------------------

    def _build_struct(self, definition: dict[str, Any], depth: int) -> dict[str, Any]:
        members = _members(definition)
        out: dict[str, Any] = {}
        for member in members:
            mandatory = _is_mandatory(member)
            # Include every mandatory member; include optional ones only while there is
            # depth budget left, and then only about a third of the time, so the records
            # vary without exploding in size.
            if not mandatory:
                if depth >= MAX_DEPTH - 4 or self.rng.random() > 0.34:
                    continue
            if depth >= MAX_DEPTH:
                # Out of budget but the member is mandatory: emit the cheapest legal value.
                out[member["name"]] = self._degenerate(member)
                continue
            out[member["name"]] = self._build_member(member, depth + 1)

        # Several root PDUs here (EMSC's UMTSGSMPLMNCallDataRecord, TAP's Notification)
        # declare *every* member OPTIONAL, so the loop above can legally produce {}. An
        # empty record encodes and decodes fine but is worthless as a decoder fixture --
        # force one member in so every record carries at least one real value.
        if not out and members and depth < MAX_DEPTH:
            member = members[self.rng.randrange(len(members))]
            out[member["name"]] = self._build_member(member, depth + 1)
        return out

    def _build_choice(self, definition: dict[str, Any], depth: int) -> tuple[str, Any]:
        members = _members(definition)
        if not members:
            return ("", None)
        member = members[self.rng.randrange(len(members))]
        if depth >= MAX_DEPTH:
            return (member["name"], self._degenerate(member))
        return (member["name"], self._build_member(member, depth + 1))

    def _build_list(self, definition: dict[str, Any], depth: int) -> list[Any]:
        element = definition.get("element", {"type": "INTEGER"})
        low, high = _size_bounds(definition)
        count = low if low else 1
        count = max(count, 1)
        if high is not None:
            count = min(count, high)
        if depth >= MAX_DEPTH:
            return []
        return [self._build_member(element, depth + 1) for _ in range(count)]

    def _build_member(self, member: dict[str, Any], depth: int) -> Any:
        kind = member.get("type")
        if kind in BUILTIN_TYPES:
            return self.build(member, depth)
        return self.build_named(kind, depth)

    def _degenerate(self, member: dict[str, Any]) -> Any:
        """Cheapest value that still satisfies a mandatory member at the depth ceiling."""
        definition = (
            member if member.get("type") in BUILTIN_TYPES else self.index.resolve(member.get("type", ""))
        ) or {"type": "OCTET STRING"}
        kind = definition.get("type")
        if kind in ("SEQUENCE", "SET"):
            return {}
        if kind in ("SEQUENCE OF", "SET OF"):
            return []
        if kind == "CHOICE":
            members = _members(definition)
            return (members[0]["name"], b"\x00") if members else ("", None)
        if kind == "INTEGER":
            return self._build_integer(definition)
        if kind == "BOOLEAN":
            return True
        if kind == "NULL":
            return None
        if kind == "BIT STRING":
            return self._build_bit_string(definition)
        if kind == "NumericString":
            return self._build_numeric_string(definition)
        if kind in STRING_TYPES:
            return self._build_string(definition)
        return self._build_octet_string(definition)

    # -- primitives -----------------------------------------------------------

    def _build_integer(self, definition: dict[str, Any]) -> int:
        named = definition.get("values")
        if isinstance(named, list) and named:
            options = [v for v in named if isinstance(v, (list, tuple)) and len(v) == 2]
            if options:
                return int(options[self.rng.randrange(len(options))][1])
        low, high = _int_bounds(definition)
        if low is None and high is None:
            return self.rng.randrange(1, 10_000)
        if low is None:
            return int(high)
        if high is None:
            return int(low) + self.rng.randrange(0, 100)
        if high < low:
            return int(low)
        return int(low) + self.rng.randrange(0, int(high) - int(low) + 1)

    def _build_enumerated(self, definition: dict[str, Any]) -> str:
        options = [v for v in definition.get("values", []) if isinstance(v, (list, tuple))]
        if not options:
            return "0"
        return str(options[self.rng.randrange(len(options))][0])

    def _build_bit_string(self, definition: dict[str, Any]) -> tuple[bytes, int]:
        low, high = _size_bounds(definition)
        bits = low if low else (high if high else 8)
        bits = max(int(bits), 1)
        nbytes = (bits + 7) // 8
        raw = bytes(self.rng.randrange(256) for _ in range(nbytes))
        # Zero the unused trailing bits so the value survives a decode/encode round trip.
        spare = nbytes * 8 - bits
        if spare:
            raw = raw[:-1] + bytes([raw[-1] & (0xFF << spare) & 0xFF])
        return (raw, bits)

    def _build_octet_string(self, definition: dict[str, Any]) -> bytes:
        low, high = _size_bounds(definition)
        length = low if low else (high if high is not None else 4)
        length = max(int(length), 1)
        if high is not None:
            length = min(length, int(high))
        return bytes(self.rng.randrange(1, 255) for _ in range(length))

    def _build_numeric_string(self, definition: dict[str, Any]) -> str:
        low, high = _size_bounds(definition)
        length = low if low else (high if high is not None else 5)
        length = max(int(length), 1)
        if high is not None:
            length = min(length, int(high))
        return "".join(str(self.rng.randrange(10)) for _ in range(length))

    def _build_string(self, definition: dict[str, Any]) -> str:
        low, high = _size_bounds(definition)
        length = low if low else (high if high is not None else 6)
        length = max(int(length), 1)
        if high is not None:
            length = min(length, int(high))
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return "".join(alphabet[self.rng.randrange(len(alphabet))] for _ in range(length))


# -- TLV framing --------------------------------------------------------------


def _tlv_length(buf: bytes, offset: int) -> int:
    """Total byte length of the BER TLV starting at ``offset`` (header + content)."""
    i = offset
    if i >= len(buf):
        raise ValueError(f"offset {offset} past end of buffer ({len(buf)} bytes)")
    first = buf[i]
    i += 1
    if first & 0x1F == 0x1F:  # long-form tag: continue while bit 8 set
        while i < len(buf) and buf[i] & 0x80:
            i += 1
        i += 1
    if i >= len(buf):
        raise ValueError("truncated TLV: no length octet")
    length_byte = buf[i]
    i += 1
    if length_byte == 0x80:
        raise ValueError("indefinite-length BER is not emitted by this generator")
    if length_byte & 0x80:
        n = length_byte & 0x7F
        if i + n > len(buf):
            raise ValueError("truncated TLV: long-form length runs past end")
        content = int.from_bytes(buf[i : i + n], "big")
        i += n
    else:
        content = length_byte
    return (i - offset) + content


def iter_records(buf: bytes):
    """Yield ``(offset, record_bytes)`` for each concatenated TLV in ``buf``."""
    offset = 0
    while offset < len(buf):
        total = _tlv_length(buf, offset)
        yield offset, buf[offset : offset + total]
        offset += total


# -- generation / verification ------------------------------------------------


def build_records(
    schema_file: str, root: str, preferred_member: str | None
) -> tuple[list[Any], SpecIndex, dict[str, Any]]:
    """Deterministically build ``RECORDS_PER_FILE`` values for ``root``.

    Returns the records alongside the spec index and the chosen root CHOICE member, so
    verification can interpret the same type definitions (notably DEFAULT members).
    """
    path = SCHEMA_DIR / schema_file
    index = SpecIndex(asn1tools.parse_files([str(path)]))
    root_def = index.resolve(root)
    if root_def is None or root_def.get("type") != "CHOICE":
        raise RuntimeError(f"{schema_file}: root {root} is not a CHOICE")

    members = _members(root_def)
    by_name = {m["name"]: m for m in members}
    chosen = by_name.get(preferred_member or "") or members[0]

    records = []
    for i in range(RECORDS_PER_FILE):
        rng = random.Random(SEED + i)
        builder = ValueBuilder(index, rng)
        records.append((chosen["name"], builder._build_member(chosen, 1)))
    return records, index, chosen


def apply_defaults(index: SpecIndex, member: dict[str, Any], value: Any) -> Any:
    """Fill in every omitted DEFAULT member, mirroring what a BER decoder returns.

    ASN.1 DEFAULT members are legitimately absent from the encoding; ``asn1tools``
    materializes them on decode. Normalizing the expected value the same way keeps the
    round-trip comparison honest instead of reporting a spurious mismatch.
    """
    definition = (
        member if member.get("type") in BUILTIN_TYPES else index.resolve(member.get("type", ""))
    )
    if definition is None:
        return value
    kind = definition.get("type")

    if kind in ("SEQUENCE", "SET") and isinstance(value, dict):
        out = dict(value)
        for sub in _members(definition):
            name = sub["name"]
            if name in out:
                out[name] = apply_defaults(index, sub, out[name])
            elif "default" in sub:
                out[name] = sub["default"]
        return out

    if kind == "CHOICE" and isinstance(value, tuple) and len(value) == 2:
        for sub in _members(definition):
            if sub["name"] == value[0]:
                return (value[0], apply_defaults(index, sub, value[1]))
        return value

    if kind in ("SEQUENCE OF", "SET OF") and isinstance(value, list):
        element = definition.get("element", {})
        return [apply_defaults(index, element, item) for item in value]

    return value


def generate(verbose: bool = True) -> dict[str, dict[str, Any]]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary: dict[str, dict[str, Any]] = {}

    for schema_file, root, preferred, out_name in PROTOCOLS:
        path = SCHEMA_DIR / schema_file
        spec = asn1tools.compile_files([str(path)], "ber")
        records, _index, _chosen = build_records(schema_file, root, preferred)

        blob = b""
        for record in records:
            blob += spec.encode(root, record, check_constraints=False)

        out_path = OUTPUT_DIR / out_name
        out_path.write_bytes(blob)

        summary[schema_file] = {
            "root": root,
            "member": records[0][0],
            "records": len(records),
            "bytes": len(blob),
            "path": str(out_path),
        }
        if verbose:
            print(
                f"  wrote {out_path.name:<24} {len(records):>2} records  "
                f"{len(blob):>8} bytes  root={root}/{records[0][0]}"
            )
    return summary


def verify(verbose: bool = True) -> tuple[bool, dict[str, int]]:
    """Re-read each file, walk its TLVs, decode each record, compare to the source values."""
    ok = True
    counts: dict[str, int] = {}

    for schema_file, root, preferred, out_name in PROTOCOLS:
        path = SCHEMA_DIR / schema_file
        out_path = OUTPUT_DIR / out_name
        if not out_path.exists():
            print(f"  MISSING {out_path}")
            ok = False
            counts[schema_file] = 0
            continue

        spec = asn1tools.compile_files([str(path)], "ber")
        raw_expected, index, chosen = build_records(schema_file, root, preferred)
        # Normalize DEFAULT members the way a BER decoder materializes them.
        expected = [
            (name, apply_defaults(index, chosen, value)) for name, value in raw_expected
        ]
        blob = out_path.read_bytes()

        decoded_count = 0
        mismatches = 0
        for idx, (_, record_bytes) in enumerate(iter_records(blob)):
            decoded = spec.decode(root, record_bytes, check_constraints=False)
            decoded_count += 1
            if idx < len(expected):
                if not values_equal(decoded, expected[idx]):
                    mismatches += 1
                    if verbose and mismatches <= 1:
                        print(f"    MISMATCH {schema_file} record {idx}")

        counts[schema_file] = decoded_count
        record_ok = decoded_count == RECORDS_PER_FILE and mismatches == 0
        ok = ok and record_ok
        if verbose:
            status = "OK " if record_ok else "FAIL"
            print(
                f"  {status} {out_name:<24} decoded {decoded_count}/{RECORDS_PER_FILE} "
                f"records, {mismatches} field mismatches"
            )
    return ok, counts


def values_equal(a: Any, b: Any) -> bool:
    """Structural equality that tolerates BER's tuple/bytes representations."""
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a) != set(b):
            return False
        return all(values_equal(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        if len(a) != len(b):
            return False
        return all(values_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, (bytes, bytearray)) and isinstance(b, (bytes, bytearray)):
        return bytes(a) == bytes(b)
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b or a == b
    return a == b


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--verify",
        action="store_true",
        help="only verify the existing files in flowx_testing/BT_Testing/synthetic/",
    )
    args = parser.parse_args(argv)

    if not args.verify:
        print(f"Generating {RECORDS_PER_FILE} records per protocol into {OUTPUT_DIR}")
        generate()
        print()

    print("Verifying (walk concatenated TLVs, decode each, compare to encoded values):")
    ok, counts = verify()
    print()
    print("Decoded record counts per protocol:")
    for schema_file, count in counts.items():
        print(f"  {schema_file:<16} {count}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
