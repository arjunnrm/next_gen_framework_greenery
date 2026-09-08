"""ASN.1 BER/DER binary decoding for telecom CDR-style sources.

**Distributed by construction, end to end.** File discovery/reading happens via Auto
Loader's ``cloudFiles`` source (``ingestion/readers.py::read_asn1_source``), which lists and
reads files across executors natively -- never collects file content to the driver. The
decode step below runs via :meth:`~pyspark.sql.DataFrame.mapInPandas`, which Spark also
executes entirely on executors, one partition at a time, with no driver-side collection at
any point.

**Real perf bug fixed here**: the original implementation decoded via a plain row UDF
(``F.udf``), which called ``asn1tools.compile_files(...)`` *inside* the per-row decode
function -- Spark has no way to know that call is expensive and reusable, so it recompiled
the full ASN.1 module from scratch for every single row. ``mapInPandas`` instead hands the
decode function one *partition* at a time (as an iterator of pandas DataFrame micro-batches)
-- compiling the schema once, before iterating the batches, and reusing that one compiled
object for every row across the whole partition, cuts compilation from O(rows) to O(partitions).

**Schema source: a real ``.asn`` module file, not a hand-authored JSON field list.** The
output field list used to be duplicated by hand into an external
``asn1_schema_json_path``'s ``fields: [{name, spark_type}]`` array -- a second, drift-prone
description of exactly what the ``.asn`` module already says authoritatively. This module
now derives the Spark output schema directly from the real ASN.1 module file via
``asn1tools.parse_files`` introspection (:func:`derive_asn1_field_defs`) -- one source of
truth, the ``.asn`` file itself, exactly as a data engineer would already have it from
whoever owns the wire format.

**ASN.1 ``CHOICE`` is supported, as a discriminator-plus-nullable-arms struct.** Spark has no
union type, so a CHOICE maps to a ``StructType`` carrying one nullable field per arm plus a
:data:`CHOICE_DISCRIMINATOR_FIELD` string field naming the arm actually selected -- exactly one
arm is populated per decoded value. A CHOICE is also accepted as the *root* PDU, which is what
every real telecom module needs: TAP's ``DataInterChange``, 3GPP's ``CallEventRecord`` and
EMSC's ``CallDataRecord`` are all CHOICEs, so before this the natural top-level PDU of a real
module could not be onboarded at all.

**The root PDU is optional and auto-detected.** ``asn1_pdu_name`` no longer has to be
supplied: when it is absent, ``None``, ``""``, or whitespace-only,
:func:`detect_root_pdu_name` infers it as the one top-level ``SEQUENCE``/``CHOICE`` that no
other type in the module references -- the entry point of the module's own type-dependency
graph. Verified to resolve all five real telecom modules in ``metaflow_testing/BT_Testing/``
uniquely and correctly. A supplied ``asn1_pdu_name`` is an unconditional override, never
second-guessed; an *ambiguous* module raises rather than guessing, because a wrong root does
not fail loudly -- it silently produces a full table of garbage columns.

**Value normalization is schema-aware, not shape-guessing.** ``asn1tools`` decodes both a
``BIT STRING`` and a ``CHOICE`` to a bare 2-tuple -- ``(bytes, bit_length)`` and
``(member_name, value)`` respectively -- so the two are indistinguishable by inspecting the
decoded value. :func:`_normalize_decoded_value` therefore walks the parsed ASN.1 node tree
alongside the decoded value and dispatches on the *declared* type.
"""

import logging
from typing import Any, Callable, Dict, FrozenSet, Iterator, List

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import Asn1DecodeError

logger = logging.getLogger("common.asn1.decoder")

try:
    import asn1tools

    _ASN1TOOLS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _ASN1TOOLS_AVAILABLE = False

# ASN.1 primitive/string type -> Spark type. INTEGER maps to LongType (int64) -- ample for
# any real telecom/CDR field; an ASN.1 INTEGER constrained beyond 64 bits is not supported.
# Every ASN.1 "restricted character string" type (UTF8String, IA5String, NumericString,
# PrintableString, VisibleString, GeneralString, BMPString, UniversalString, GraphicString,
# TeletexString/T61String) maps to StringType -- Spark has no narrower equivalent, and this
# framework doesn't attempt to re-validate the SIZE/character-set constraints those types
# encode (asn1tools' own codec already enforces them during decode). GeneralizedTime/UTCTime
# -- the standard ASN.1 timestamp types (e.g. a CDR's recordingTimeStamp) -- map to
# TimestampType: confirmed live, asn1tools decodes both to a plain Python datetime.datetime.
# OBJECT IDENTIFIER (protocol/algorithm identifiers) decodes to its dotted-notation string
# (e.g. "1.2.840.113549") verbatim, so it maps to StringType with no transformation needed.
# NULL always decodes to None regardless of declared type, so any nullable type works;
# StringType is used for consistency with the other always-nullable mappings above.
_ASN1_PRIMITIVE_TYPE_MAP: Dict[str, DataType] = {
    "BOOLEAN": BooleanType(),
    "INTEGER": LongType(),
    "REAL": DoubleType(),
    "OCTET STRING": BinaryType(),
    "UTF8String": StringType(),
    "IA5String": StringType(),
    "NumericString": StringType(),
    "PrintableString": StringType(),
    "VisibleString": StringType(),
    "GeneralString": StringType(),
    "BMPString": StringType(),
    "UniversalString": StringType(),
    "GraphicString": StringType(),
    "TeletexString": StringType(),
    "T61String": StringType(),
    "GeneralizedTime": TimestampType(),
    "UTCTime": TimestampType(),
    "OBJECT IDENTIFIER": StringType(),
    "NULL": StringType(),
}

# BIT STRING decodes (confirmed live) to a bare (bytes_or_bytearray, bit_length) tuple, not a
# named-field structure Arrow can map directly -- _normalize_decoded_value below reshapes it
# into {"bytes": ..., "bit_length": ...} at decode time to match this declared output type.
BIT_STRING_SPARK_TYPE = StructType(
    [StructField("bytes", BinaryType(), True), StructField("bit_length", LongType(), True)]
)

# An ASN.1 CHOICE has no Spark equivalent -- Spark has no union type. It is mapped instead to a
# "discriminator + nullable arms" struct: a StructType carrying one nullable field per CHOICE
# arm, plus this extra field naming which arm was actually selected. Exactly one arm field is
# non-NULL per decoded value (and even that one can legitimately be NULL, e.g. an arm whose type
# is ASN.1 NULL), which is precisely why the discriminator is required: without it a consumer
# cannot tell "arm absent" from "arm present but its value is NULL". Confirmed live: asn1tools
# decodes a CHOICE to a bare ``(member_name, value)`` tuple, so the selected member name is
# handed over for free as ``tuple[0]`` -- see :func:`_normalize_decoded_value`.
#
# The leading underscore makes a collision with a real arm impossible rather than merely
# unlikely: the ASN.1 grammar requires a member identifier to begin with a lowercase letter, so
# no conforming module can declare an arm named ``_choice`` (verified live -- asn1tools raises
# a ParseError on such a module). It also matches the existing ``_asn1_decode_error`` column's
# underscore-prefixed convention for framework-added, non-source columns.
CHOICE_DISCRIMINATOR_FIELD = "_choice"

# ASN.1 "extension marker" (``...``) inside a SEQUENCE/SET/CHOICE body. ``asn1tools.parse_files``
# emits it as a bare ``None`` entry in the node's ``members`` list rather than a member dict --
# confirmed live against metaflow_testing/BT_Testing/TAP.311.asn1, where 79 SEQUENCE/SET types
# and 8 CHOICE types carry one. It is a versioning marker, not a field: it contributes no column.


def _iter_members(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Yield a node's real ``members``, skipping ASN.1 extension markers (``...``).

    ``asn1tools.parse_files`` represents an extension marker as a bare ``None`` entry in the
    ``members`` list. Iterating ``members`` without this guard raises a raw ``TypeError:
    'NoneType' object is not subscriptable`` -- which both crashes on real extensible modules
    (TAP.311) and violates the ``Asn1DecodeError``-only contract this module's public functions
    document.
    """
    return [member for member in node.get("members", []) or [] if member is not None]


def _asn1_node_to_spark_type(node: Dict[str, Any], module_types: Dict[str, Any], seen: FrozenSet[str]) -> DataType:
    """Resolve one ``asn1tools.parse_files`` node (a top-level type def, a ``SEQUENCE``
    member, or a ``SEQUENCE OF`` element) to a Spark ``DataType``, recursively.

    Every node ``asn1tools.parse_files`` returns -- whether it's a module-level named type,
    a member of a ``SEQUENCE``, or the ``element`` of a ``SEQUENCE OF`` -- shares the same
    ``{"type": ..., ...}`` shape, so one recursive function handles all three contexts:

    * ``SEQUENCE``/``SET`` -> ``StructType`` of its ``members``, each resolved recursively (an
      inline nested record, e.g. ``UserProfile.address: Address``, decodes to a Spark
      ``struct`` column, matched by ``ingestion/json_flattening.py``'s existing
      ``explode_columns`` mechanism for any downstream flattening a spec wants). ``SET`` is
      handled identically to ``SEQUENCE`` -- both decode to a plain Python ``dict`` keyed by
      member name; ``SET``'s "unordered" semantics only affect wire encoding, not the shape
      asn1tools hands back.
    * ``SEQUENCE OF``/``SET OF`` -> ``ArrayType`` of its ``element``, resolved recursively
      (confirmed live: ``asn1tools`` decodes both to a plain Python ``list``).
    * ``ENUMERATED`` -> ``StringType`` (confirmed live: ``asn1tools`` decodes an enumerated
      value to its symbolic member name, e.g. ``"administrator"``, not its integer tag).
    * ``BIT STRING`` -> :data:`BIT_STRING_SPARK_TYPE` (a 2-field struct) -- confirmed live,
      asn1tools decodes this to a bare ``(bytes_or_bytearray, bit_length)`` tuple; see
      :func:`_normalize_decoded_value` for the corresponding value reshape at decode time.
    * ``CHOICE`` -> a ``StructType`` of **all** its arms, every field nullable, plus a
      :data:`CHOICE_DISCRIMINATOR_FIELD` string field naming the selected arm. Spark has no
      union type, so a CHOICE cannot be represented as "one of these types"; the
      discriminator-plus-nullable-arms shape is the representable equivalent -- exactly one arm
      is populated per decoded value and every other arm is NULL, with the discriminator saying
      which. Confirmed live, asn1tools decodes a CHOICE to a bare ``(member_name, value)``
      tuple; see :func:`_normalize_decoded_value` for the corresponding value reshape. ASN.1
      *tags* on a CHOICE arm (``[20] IMPLICIT``, as GGSN/PSGW use) affect wire encoding only --
      asn1tools decodes to the arm's **name** regardless, so tags are ignored here.
    * An ASN.1 extension marker (``...``) inside a ``SEQUENCE``/``SET``/``CHOICE`` body is a
      versioning marker, not a member -- ``asn1tools`` emits it as a bare ``None`` in
      ``members`` and it contributes no field (see :func:`_iter_members`).
    * A primitive/string/timestamp type name (``INTEGER``, ``BOOLEAN``, ``UTF8String``,
      ``GeneralizedTime``, ``OBJECT IDENTIFIER``, ...) -> :data:`_ASN1_PRIMITIVE_TYPE_MAP`.
    * Anything else is a reference to another named type defined in the same module (e.g.
      a member's ``"type"`` is literally ``"Address"``) -- resolved by recursing into
      ``module_types[name]``. ``seen`` guards against a self-referential/recursive type
      definition, which this framework does not support (an ASN.1 type recursively
      containing itself has no finite Spark ``StructType``).

    Raises
    ------
    Asn1DecodeError
        If a member's ``"type"`` doesn't resolve to a known primitive/string type, a
        ``SEQUENCE``/``SET``/``SEQUENCE OF``/``SET OF``/``CHOICE``/``ENUMERATED``/
        ``BIT STRING`` construct, or a type actually defined in this module.
    """
    asn1_type = node.get("type")

    if asn1_type in ("SEQUENCE", "SET"):
        fields = [
            StructField(member["name"], _asn1_node_to_spark_type(member, module_types, seen), True)
            for member in _iter_members(node)
        ]
        return StructType(fields)

    if asn1_type == "CHOICE":
        # Discriminator first so it is a stable, predictable column position regardless of how
        # many arms the CHOICE declares; arms follow in the order the .asn file declares them.
        fields = [StructField(CHOICE_DISCRIMINATOR_FIELD, StringType(), True)]
        fields += [
            StructField(member["name"], _asn1_node_to_spark_type(member, module_types, seen), True)
            for member in _iter_members(node)
        ]
        return StructType(fields)

    if asn1_type in ("SEQUENCE OF", "SET OF"):
        element = node.get("element")
        if element is None:
            raise Asn1DecodeError(f"ASN.1 {asn1_type!r} node is missing its 'element' definition: {node!r}")
        return ArrayType(_asn1_node_to_spark_type(element, module_types, seen), True)

    if asn1_type == "ENUMERATED":
        return StringType()

    if asn1_type == "BIT STRING":
        return BIT_STRING_SPARK_TYPE

    if asn1_type in _ASN1_PRIMITIVE_TYPE_MAP:
        return _ASN1_PRIMITIVE_TYPE_MAP[asn1_type]

    if not asn1_type or asn1_type not in module_types:
        raise Asn1DecodeError(
            f"Unsupported or unresolvable ASN.1 type {asn1_type!r} (node: {node.get('name', '?')!r}); "
            f"known types in this module: {sorted(module_types)}"
        )
    if asn1_type in seen:
        raise Asn1DecodeError(
            f"Unsupported recursive/self-referential ASN.1 type reference: "
            f"{' -> '.join(sorted(seen))} -> {asn1_type}"
        )
    return _asn1_node_to_spark_type(module_types[asn1_type], module_types, seen | {asn1_type})


def _parse_module_types(schema_path: str) -> Dict[str, Any]:
    """Parse a single-module ``.asn`` file and return its ``{type name: node}`` map.

    Shared by :func:`derive_asn1_field_defs` (schema derivation) and
    :func:`decode_asn1_binary_stream` (which needs the same map to normalize decoded values
    schema-aware), so both enforce one identical error contract -- every failure surfaces as an
    :class:`Asn1DecodeError`, never a raw parser or OS error.
    """
    if not _ASN1TOOLS_AVAILABLE:
        raise Asn1DecodeError("asn1tools is required for ASN.1 schema derivation but is not installed.")

    try:
        parsed = asn1tools.parse_files([schema_path])
    except OSError as exc:
        raise Asn1DecodeError(f"Failed to read ASN.1 module file '{schema_path}': {exc}") from exc
    except Exception as exc:  # noqa: BLE001 - asn1tools raises its own parser-specific error types
        raise Asn1DecodeError(f"Failed to parse ASN.1 module file '{schema_path}': {exc}") from exc

    if len(parsed) != 1:
        raise Asn1DecodeError(
            f"ASN.1 module file '{schema_path}' must define exactly one module; found {sorted(parsed)}."
        )
    return next(iter(parsed.values())).get("types", {})


def _iter_referenced_type_names(node: Any, out: set) -> set:
    """Collect every named-type reference reachable from ``node``, recursively.

    A node's ``"type"`` is either a built-in construct name (``SEQUENCE``, ``INTEGER``, ...)
    or a *reference* to another type defined in the same module. This walk does not attempt to
    tell those apart -- it collects every ``"type"`` string it sees and the caller intersects
    the result with the module's own type map, so built-in names simply fall out. Nested
    structure is reached through ``members`` (``SEQUENCE``/``SET``/``CHOICE``) and ``element``
    (``SEQUENCE OF``/``SET OF``).

    Extension markers (bare ``None`` entries in ``members``, see :func:`_iter_members`) are
    skipped -- iterating them unguarded raises ``TypeError: 'NoneType' object is not
    subscriptable`` on any extensible module (TAP.311 has one in ``DataInterChange`` itself).
    """
    if not isinstance(node, dict):
        return out
    asn1_type = node.get("type")
    if isinstance(asn1_type, str):
        out.add(asn1_type)
    for key in ("members", "element"):
        child = node.get(key)
        if isinstance(child, list):
            for item in child:
                if item is not None:
                    _iter_referenced_type_names(item, out)
        elif isinstance(child, dict):
            _iter_referenced_type_names(child, out)
    return out


def detect_root_pdu_name(schema_path: str, module_types: Dict[str, Any] = None) -> str:
    """Infer the module's root PDU when a spec supplies no explicit ``asn1_pdu_name``.

    **The rule: a root PDU is a top-level SEQUENCE/CHOICE that no other type in the module
    references.** An ASN.1 module is a directed graph of type definitions; the PDU actually sent
    on the wire is the entry point of that graph, so by construction nothing depends on it,
    while every helper/component type exists precisely *because* something else references it.
    Taking the unreferenced SEQUENCE/CHOICE types is therefore not a heuristic about naming or
    declaration order -- it reads the module's own dependency structure.

    **Why a structural rule rather than a tag-driven one.** Peeking at the first BER TLV's tag
    and matching it against candidate types looks more direct, but it cannot work universally:
    an *untagged* CHOICE arm carries no tag of its own on the wire, it carries its selected
    arm's tag. Verified against the five real modules in ``metaflow_testing/BT_Testing/``:
    TAP.310/TAP.311's ``DataInterChange`` members (``transferBatch``, ``notification``) declare
    no context tags at all, GGSN/PSGW's ``CallEventRecord`` members do (``[20]`` etc.), and
    EMSC's ``CallDataRecord`` mixes both. A tag-driven strategy would resolve GGSN/PSGW and fail
    on TAP. The structural rule resolves all five uniquely (verified empirically -- see
    ``tests/unit/test_asn1_root_pdu_detection.py``, which asserts exactly this against the real
    module files rather than a mock).

    **Ambiguity is an error, never a guess.** If the module exposes two or more unreferenced
    SEQUENCE/CHOICE types there is no structural basis to prefer either, and picking wrong does
    not fail loudly -- it silently produces a full table of garbage columns. The candidates are
    listed in the error so the spec author can copy the right one into ``asn1_pdu_name``.

    Parameters
    ----------
    schema_path:
        Path to the ``.asn`` module file (used for parsing when ``module_types`` is not supplied,
        and for error messages either way).
    module_types:
        Optional pre-parsed ``{type name: node}`` map, to avoid re-parsing a module the caller
        has already parsed.

    Returns
    -------
    str
        The detected root PDU type name.

    Raises
    ------
    Asn1DecodeError
        If the module defines no top-level SEQUENCE/CHOICE at all, or if more than one is
        unreferenced (ambiguous) -- in both cases naming ``asn1_pdu_name`` explicitly is the fix.
    """
    if module_types is None:
        module_types = _parse_module_types(schema_path)

    structural = [name for name, node in module_types.items() if node.get("type") in ("SEQUENCE", "CHOICE")]
    if not structural:
        raise Asn1DecodeError(
            f"Cannot auto-detect a root PDU in ASN.1 module file '{schema_path}': it defines no "
            f"top-level SEQUENCE or CHOICE type (defined types: {sorted(module_types)}). Set "
            f"source_config.asn1_pdu_name explicitly to the record type to decode."
        )

    referenced: set = set()
    for name, node in module_types.items():
        names_from_this_type: set = set()
        _iter_referenced_type_names(node, names_from_this_type)
        # A type naming itself (its own "type" key, or a genuinely self-recursive reference)
        # must not disqualify it from being the root -- only references from OTHER types count.
        names_from_this_type.discard(name)
        referenced |= names_from_this_type

    candidates = [name for name in structural if name not in referenced]

    if len(candidates) == 1:
        detected = candidates[0]
        logger.info(
            "ASN.1 root PDU auto-detected as '%s' (%s) in module '%s': it is the only top-level "
            "SEQUENCE/CHOICE that no other type in the module references, out of %d structural "
            "candidate(s) and %d defined type(s). Set source_config.asn1_pdu_name to override.",
            detected,
            module_types[detected].get("type"),
            schema_path,
            len(structural),
            len(module_types),
        )
        return detected

    if not candidates:
        raise Asn1DecodeError(
            f"Cannot auto-detect a root PDU in ASN.1 module file '{schema_path}': every top-level "
            f"SEQUENCE/CHOICE type is referenced by another type, so none is an unambiguous entry "
            f"point (structural types: {sorted(structural)}). Set source_config.asn1_pdu_name "
            f"explicitly to the record type to decode."
        )

    raise Asn1DecodeError(
        f"Cannot auto-detect a root PDU in ASN.1 module file '{schema_path}': {len(candidates)} "
        f"top-level SEQUENCE/CHOICE types are referenced by no other type, so the root is "
        f"ambiguous: {sorted(candidates)}. Set source_config.asn1_pdu_name explicitly to whichever "
        f"of those is the record type to decode -- guessing between them would silently produce "
        f"a table of wrong columns rather than fail."
    )


def resolve_pdu_name(schema_path: str, pdu_name: Any, module_types: Dict[str, Any] = None) -> str:
    """Return the PDU to decode as: the caller's explicit ``pdu_name``, else an auto-detected root.

    **The override is unconditional.** A ``pdu_name`` that is a non-blank string is returned
    verbatim, with no detection run and no cross-check against what detection *would* have
    picked. A spec author naming a PDU is making a statement about their own wire format --
    second-guessing it (e.g. warning that it is "not the detected root") would be noise at best
    and, for a module whose real entry point genuinely is referenced elsewhere, actively wrong.
    Detection is a fallback for the absent case only.

    Auto-detection triggers on ``None``, a missing key, ``""``, or a whitespace-only string --
    the last two matter because a spec authored through the Databricks App's form UI yields
    ``""`` for an untouched optional text field, not an absent key, and an operator clearing a
    field by hand commonly leaves a space behind. All three mean the same thing to the author:
    "I did not specify one."
    """
    if isinstance(pdu_name, str) and pdu_name.strip():
        return pdu_name.strip()
    if pdu_name is not None and not isinstance(pdu_name, str):
        raise Asn1DecodeError(
            f"asn1_pdu_name must be a string naming a top-level SEQUENCE/CHOICE type (or be omitted "
            f"for auto-detection); got {pdu_name!r} ({type(pdu_name).__name__})."
        )
    return detect_root_pdu_name(schema_path, module_types)


def derive_asn1_field_defs(schema_path: str, pdu_name: Any = None) -> List[Dict[str, Any]]:
    """Derive the Spark output field list for ``pdu_name`` directly from a real ASN.1
    module file, via ``asn1tools.parse_files`` introspection -- no hand-authored field list.

    ``pdu_name`` must name a top-level ``SEQUENCE`` **or** ``CHOICE`` type in the module (the
    ASN.1 "record" this source decodes one instance of per binary payload, e.g.
    ``CallDetailRecord``, ``UserProfile``, or a real telecom module's natural root PDU such as
    TAP's ``DataInterChange`` or 3GPP's ``CallEventRecord``, both of which are CHOICEs). Each
    of that type's own direct members becomes one output column -- nested
    ``SEQUENCE``/``SEQUENCE OF``/``CHOICE`` members become ``struct``/``array``/``struct``
    columns rather than being flattened further here (a spec can flatten them downstream via
    ``explode_columns``, same as a JSON source).

    For a **CHOICE** root PDU, one column is emitted per arm (each nullable, since exactly one
    arm is populated per record and every other arm is NULL), plus a
    :data:`CHOICE_DISCRIMINATOR_FIELD` string column naming the arm that was actually selected
    -- without it a consumer cannot distinguish "arm absent" from "arm present but NULL". For a
    SEQUENCE root the behaviour is unchanged: one column per member, no discriminator.

    Parameters
    ----------
    schema_path:
        Path to the real ``.asn``/ASN.1 module definition file (a single self-contained
        module -- this framework does not support ``IMPORTS`` spanning multiple files).
    pdu_name:
        Name of the top-level ``SEQUENCE`` or ``CHOICE`` type in that module to decode each
        record as. **Optional.** When omitted, ``None``, ``""``, or whitespace-only, the root
        PDU is inferred from the module's own dependency structure by
        :func:`detect_root_pdu_name`; when supplied it wins unconditionally
        (:func:`resolve_pdu_name`).

    Returns
    -------
    list of dict
        ``[{"name": ..., "spark_type": <resolved DataType>, "asn1_node": <parse node>}, ...]``,
        in the member order the ``.asn`` file itself declares them. ``asn1_node`` is the raw
        ``asn1tools.parse_files`` node for that column, carried so
        :func:`_normalize_decoded_value` can reshape decoded values **schema-aware** rather
        than guessing from Python value shape (a CHOICE and a BIT STRING both decode to a
        2-tuple). For a CHOICE root PDU the list is led by a synthetic
        :data:`CHOICE_DISCRIMINATOR_FIELD` entry whose ``asn1_node`` is ``None``.

    Raises
    ------
    Asn1DecodeError
        If asn1tools is unavailable, the file can't be parsed, the module doesn't define
        exactly one ASN.1 module, ``pdu_name`` isn't a defined type, ``pdu_name`` is neither a
        ``SEQUENCE`` nor a ``CHOICE``, or any member's type can't be resolved (see
        :func:`_asn1_node_to_spark_type`). Also when ``pdu_name`` is omitted and the root PDU
        cannot be unambiguously detected (see :func:`detect_root_pdu_name`).
    """
    module_types = _parse_module_types(schema_path)
    pdu_name = resolve_pdu_name(schema_path, pdu_name, module_types)

    if pdu_name not in module_types:
        raise Asn1DecodeError(
            f"PDU '{pdu_name}' is not defined in ASN.1 module file '{schema_path}' "
            f"(defined types: {sorted(module_types)})."
        )
    pdu_def = module_types[pdu_name]
    pdu_type = pdu_def.get("type")
    if pdu_type not in ("SEQUENCE", "CHOICE"):
        raise Asn1DecodeError(
            f"PDU '{pdu_name}' must be a top-level ASN.1 SEQUENCE or CHOICE (a record type); "
            f"got '{pdu_type}'."
        )

    seen = frozenset({pdu_name})
    field_defs: List[Dict[str, Any]] = []
    if pdu_type == "CHOICE":
        # A root CHOICE projects to one column per arm plus the discriminator; the arm columns
        # are exactly the fields _asn1_node_to_spark_type would have produced had this CHOICE
        # appeared nested, minus the enclosing struct. See make_partition_decoder for how the
        # decoded ``(member_name, value)`` tuple is spread across them.
        field_defs.append({"name": CHOICE_DISCRIMINATOR_FIELD, "spark_type": StringType(), "asn1_node": None})
    field_defs += [
        {
            "name": member["name"],
            "spark_type": _asn1_node_to_spark_type(member, module_types, seen),
            "asn1_node": member,
        }
        for member in _iter_members(pdu_def)
    ]
    return field_defs


def _resolve_node(node: Any, module_types: Dict[str, Any], seen: FrozenSet[str]) -> Any:
    """Follow a node's named-type references until it lands on a structural/primitive node.

    A member whose ``"type"`` is a *reference* to another named type in the module (e.g.
    ``{"type": "CallEventDetail", "name": "callEventDetails"}``) tells us nothing about the
    decoded value's shape on its own -- the shape lives in ``module_types["CallEventDetail"]``.
    Normalization needs the resolved node, exactly as :func:`_asn1_node_to_spark_type` does.
    ``seen`` mirrors that function's cycle guard; a reference chain that loops returns ``None``,
    which makes normalization fall back to passing the value through untouched rather than
    recursing forever.
    """
    while isinstance(node, dict):
        asn1_type = node.get("type")
        if asn1_type in module_types and asn1_type not in seen:
            seen = seen | {asn1_type}
            node = module_types[asn1_type]
            continue
        return node
    return None


def _normalize_decoded_value(value: Any, node: Any, module_types: Dict[str, Any]) -> Any:
    """Reshape one decoded ASN.1 value into a form Arrow/pandas can map onto the Spark output
    type :func:`_asn1_node_to_spark_type` declared for it, **driven by the ASN.1 schema node**.

    Two decoded shapes do not match their declared Spark type verbatim, and -- critically --
    both of them are a bare 2-tuple, so they cannot be told apart by inspecting the *value*:

    * ``BIT STRING`` decodes to ``(bytes_or_bytearray, bit_length)``, reshaped here into
      ``{"bytes": <bytes>, "bit_length": <int>}`` to match :data:`BIT_STRING_SPARK_TYPE`.
    * ``CHOICE`` decodes to ``(selected_member_name, value)``, reshaped here into
      ``{<CHOICE_DISCRIMINATOR_FIELD>: <name>, <selected arm>: <normalized value>,
      <every other arm>: None}`` to match the struct :func:`_asn1_node_to_spark_type` declared.

    A CHOICE whose selected arm is an OCTET STRING decodes to e.g. ``("raw", b'\x01\x02')``,
    and a CHOICE arm could itself hold a BIT STRING's ``(bytes, int)`` tuple as its value --
    which is why this function takes ``node`` and dispatches on the **declared ASN.1 type**
    rather than guessing from the tuple's contents. The previous shape-guessing implementation
    (any ``(bytes|bytearray, int)`` 2-tuple is a BIT STRING) was correct only by luck for the
    cases it had seen, and silently wrong for a CHOICE arm carrying such a tuple.

    Everything else already matches its declared Spark type verbatim (confirmed live:
    ``int``/``str``/``bool``/``bytes``/``datetime.datetime``/``None`` pass straight through;
    ``dict`` for ``SEQUENCE``/``SET``, ``list`` for ``SEQUENCE OF``/``SET OF``). Recursion
    walks the schema and the value in lockstep, so a BIT STRING or CHOICE nested at any depth
    inside a ``SEQUENCE``/``SET``/``SEQUENCE OF``/``SET OF``/``CHOICE`` is reshaped correctly.

    Parameters
    ----------
    value:
        One decoded value, as ``asn1tools`` handed it back.
    node:
        The ``asn1tools.parse_files`` node declaring that value's type -- a member node, a
        ``SEQUENCE OF`` element node, or a resolved module-level type. ``None`` means "no
        schema context available" (e.g. a synthetic discriminator column), in which case the
        value is returned unchanged rather than shape-guessed.
    module_types:
        The module's full ``{type name: node}`` map, used to resolve named-type references.
    """
    if value is None:
        return None

    resolved = _resolve_node(node, module_types, frozenset())
    if resolved is None:
        return value
    asn1_type = resolved.get("type")

    if asn1_type == "BIT STRING":
        if isinstance(value, tuple) and len(value) == 2:
            return {"bytes": bytes(value[0]), "bit_length": value[1]}
        return value

    if asn1_type == "CHOICE":
        members = _iter_members(resolved)
        result: Dict[str, Any] = {CHOICE_DISCRIMINATOR_FIELD: None}
        for member in members:
            result[member["name"]] = None
        if not (isinstance(value, tuple) and len(value) == 2):
            return result
        selected_name, selected_value = value
        result[CHOICE_DISCRIMINATOR_FIELD] = selected_name
        for member in members:
            if member["name"] == selected_name:
                result[selected_name] = _normalize_decoded_value(selected_value, member, module_types)
                break
        return result

    if asn1_type in ("SEQUENCE", "SET"):
        if not isinstance(value, dict):
            return value
        members_by_name = {member["name"]: member for member in _iter_members(resolved)}
        return {
            key: _normalize_decoded_value(item, members_by_name.get(key), module_types)
            for key, item in value.items()
        }

    if asn1_type in ("SEQUENCE OF", "SET OF"):
        if not isinstance(value, list):
            return value
        element = resolved.get("element")
        return [_normalize_decoded_value(item, element, module_types) for item in value]

    return value


def make_partition_decoder(
    module_files: List[str],
    codec: str,
    pdu_name: str,
    field_defs: List[Dict[str, Any]],
    binary_column: str,
    passthrough_columns: List[str],
    module_types: Dict[str, Any] = None,
    root_is_choice: bool = False,
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """Build the ``mapInPandas`` partition function.

    Spark invokes the returned generator function exactly once per partition, handing it an
    iterator over that partition's pandas micro-batches -- ``asn1tools.compile_files(...)``
    is called exactly once, *before* the ``for batch in batches`` loop, and the resulting
    compiled schema is reused for every row in every batch of the partition. This is the
    fix for the original per-row-UDF implementation, which recompiled the ASN.1 module on
    every single row.

    Returned as a plain, independently callable/testable function -- a unit test can invoke
    it directly with a small list of pandas DataFrames (no live Spark cluster needed) and
    monkeypatch ``asn1tools.compile_files`` with a call-counting stub to prove the
    once-per-partition behavior directly.

    Parameters
    ----------
    module_files, codec, pdu_name:
        The compiled-schema identity -- ``module_files`` is ``[schema_path]`` (a single-file
        list; see :func:`decode_asn1_binary_stream`), ``codec`` is ``"ber"``/``"der"``.
    field_defs:
        The PDU's own field list, as returned by :func:`derive_asn1_field_defs`.
        ``field_def["name"]`` names the output column and ``field_def["asn1_node"]`` (when
        present) is the ASN.1 node :func:`_normalize_decoded_value` needs to reshape that
        column's decoded value schema-aware; ``field_def["spark_type"]`` is used by
        :func:`decode_asn1_binary_stream` to build the ``mapInPandas`` output schema, not by
        this function.
    module_types:
        The module's ``{type name: node}`` map (``{}`` when unavailable), used together with
        ``asn1_node`` to resolve named-type references during normalization.
    root_is_choice:
        ``True`` when ``pdu_name`` is a top-level ``CHOICE``. ``asn1tools`` then hands back a
        bare ``(selected_arm_name, value)`` tuple rather than a ``dict``, so the decoded value
        is spread across the arm columns (selected arm populated, every other arm NULL, plus
        :data:`CHOICE_DISCRIMINATOR_FIELD`) instead of being looked up by column name. A
        ``(None, None)`` -- which ``asn1tools`` returns instead of raising when the payload
        matches no arm -- is converted into a per-row ``_asn1_decode_error`` rather than
        written out as an all-NULL row reported as a success.
    binary_column:
        Name of the raw-bytes column to decode and drop from the output.
    passthrough_columns:
        Every other input column name, carried through to the output unchanged (e.g. Auto
        Loader's ``_metadata``/``path``/``modificationTime`` technical columns).
    """
    module_types = module_types or {}
    field_names = [field_def["name"] for field_def in field_defs]
    nodes_by_name = {field_def["name"]: field_def.get("asn1_node") for field_def in field_defs}
    output_columns = passthrough_columns + field_names + ["_asn1_decode_error"]

    def _decode_partition(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        compiled = asn1tools.compile_files(module_files, codec)
        for batch in batches:
            records: List[Dict[str, Any]] = []
            for row in batch.itertuples(index=False):
                row_dict = dict(zip(batch.columns, row))
                raw_bytes = row_dict.get(binary_column)
                result: Dict[str, Any] = {col: row_dict.get(col) for col in passthrough_columns}
                for name in field_names:
                    result[name] = None
                if raw_bytes is None:
                    result["_asn1_decode_error"] = "null_payload"
                else:
                    try:
                        decoded = compiled.decode(pdu_name, raw_bytes)
                        if root_is_choice:
                            # A root CHOICE decodes to a bare (arm_name, value) tuple, not a
                            # dict -- spread it across the arm columns rather than .get()-ing
                            # each column name out of something that has none.
                            #
                            # asn1tools does NOT raise when a CHOICE matches none of its arms:
                            # it returns a bare ``(None, None)``. Confirmed live against
                            # metaflow_testing/BT_Testing/tap311_sample.ber, whose outer tag is
                            # the high-tag-number form ``7f01`` where TAP.311's TransferBatch is
                            # ``[APPLICATION 1]`` = short-form ``0x61`` -- i.e. the payload is
                            # malformed for this PDU. Passing that through would write a row with
                            # every arm NULL, a NULL discriminator and a NULL _asn1_decode_error:
                            # a row indistinguishable from a successful decode of an empty record,
                            # reported as SUCCESS. That is a data-integrity hazard, not a decode
                            # result, so it is raised into this row's _asn1_decode_error instead.
                            if not (isinstance(decoded, tuple) and len(decoded) == 2) or decoded[0] is None:
                                raise Asn1DecodeError(
                                    f"ASN.1 CHOICE PDU '{pdu_name}' matched none of its arms -- "
                                    f"asn1tools returned {decoded!r} rather than raising. The payload "
                                    f"({len(raw_bytes)} bytes, leading octets "
                                    f"{bytes(raw_bytes[:8]).hex()}) does not encode this PDU: either it "
                                    f"is malformed/truncated, or asn1_pdu_name names the wrong root."
                                )
                            selected_name, selected_value = decoded
                            result[CHOICE_DISCRIMINATOR_FIELD] = selected_name
                            if selected_name in nodes_by_name:
                                result[selected_name] = _normalize_decoded_value(
                                    selected_value, nodes_by_name[selected_name], module_types
                                )
                        else:
                            for name in field_names:
                                result[name] = _normalize_decoded_value(
                                    decoded.get(name), nodes_by_name.get(name), module_types
                                )
                        result["_asn1_decode_error"] = None
                    except Exception as decode_exc:  # noqa: BLE001 - isolate bad records, don't fail the batch
                        result["_asn1_decode_error"] = f"{type(decode_exc).__name__}: {decode_exc}"
                records.append(result)
            yield pd.DataFrame.from_records(records, columns=output_columns)

    return _decode_partition


def _materialize_hidden_metadata_column(df: DataFrame) -> DataFrame:
    """Force Auto Loader's hidden ``_metadata`` pseudo-column to become a real, physical column.

    ``mapInPandas`` reconstructs the DataFrame from scratch across the Python/Arrow boundary
    -- any hidden metadata column that was never made real is silently dropped, which would
    make ``ingestion/technical_metadata.py::attach_technical_metadata`` (called downstream
    on this function's output, extracting ``_metadata.file_name`` etc.) degrade every
    ASN.1-sourced row's ``__framework_source_file_name``/``__framework_source_file_size``/etc. to NULL. Best-effort:
    a source that genuinely has no ``_metadata`` column (e.g. a non-Auto-Loader DataFrame in
    a test) is left unchanged rather than raising.

    This project runs on Spark Connect (Databricks Connect / serverless), where ``DataFrame``
    construction is lazy by design: ``withColumn("_metadata", F.col("_metadata"))`` only
    builds an unresolved-column plan node client-side and never round-trips to the server, so
    it **cannot** raise for a genuinely-absent ``_metadata`` column -- confirmed live, this
    made the surrounding ``try/except`` dead code, and the real failure (an ``AnalysisException``
    Spark only raises once the plan is actually analyzed) instead erupted, uncaught, from the
    unrelated ``df.schema.fields`` access several lines later in
    :func:`decode_asn1_binary_stream`. Accessing ``.schema`` *inside* this function's own
    ``try`` forces that same analysis eagerly, right here, where the fallback can actually
    catch it.
    """
    if "_metadata" in df.columns:
        return df
    try:
        from pyspark.sql import functions as F

        candidate = df.withColumn("_metadata", F.col("_metadata"))
        _ = candidate.schema  # noqa: B018 - force eager analysis; see docstring above
        return candidate
    except Exception:  # noqa: BLE001
        return df


def decode_asn1_binary_stream(
    df: DataFrame, schema_path: str, codec: str, pdu_name: Any = None, binary_column: str = "content"
) -> DataFrame:
    """Decode a column of raw ASN.1 BER/DER-encoded binary payloads into structured columns.

    Runs entirely via :meth:`~pyspark.sql.DataFrame.mapInPandas` (see
    :func:`make_partition_decoder`) -- fully distributed across executors, with the ASN.1
    schema compiled once per partition rather than once per row. The output schema itself is
    derived directly from ``schema_path`` (see :func:`derive_asn1_field_defs`) -- no
    hand-authored field list.

    Parameters
    ----------
    df:
        Input DataFrame containing a binary column of raw ASN.1-encoded records.
    schema_path:
        Path to the real ``.asn``/ASN.1 module definition file. Since this is a plain
        ``source_config`` field (unlike the old external-JSON-file design), any
        ``{{catalog}}``/``{{env}}`` placeholder it contains is already substituted by
        ``onboarding/spec_loader.py`` before the pipeline ever runs -- no separate
        catalog-inference step is needed here.
    codec:
        ASN.1 encoding rule, ``"ber"`` or ``"der"``.
    pdu_name:
        Name of the top-level ``SEQUENCE`` or ``CHOICE`` type in ``schema_path`` to decode each
        record as. For a ``CHOICE`` PDU the output carries one nullable column per arm plus a
        :data:`CHOICE_DISCRIMINATOR_FIELD` column naming the selected arm. **Optional** -- when
        omitted/blank the root PDU is auto-detected from the module's dependency structure
        (:func:`detect_root_pdu_name`), and the choice is logged at INFO so an operator can see
        the inference in the driver log; an explicitly supplied name always wins.
    binary_column:
        Name of the column in ``df`` holding the raw encoded bytes.

    Returns
    -------
    DataFrame
        ``df`` with the binary column replaced by one decoded column per member (or, for a
        ``CHOICE`` PDU, per arm plus the discriminator) of ``pdu_name`` -- nested
        ``SEQUENCE``/``SEQUENCE OF``/``CHOICE`` members become ``struct``/``array``/``struct``
        columns -- plus a ``_asn1_decode_error`` column (null on success) capturing per-row
        decode failures without aborting the whole micro-batch.

    Raises
    ------
    Asn1DecodeError
        If asn1tools is unavailable, ``binary_column`` doesn't exist, or the module file
        can't be parsed/doesn't resolve to a valid schema (see
        :func:`derive_asn1_field_defs`). Per-row decode failures are captured inline, not
        raised.
    """
    if not _ASN1TOOLS_AVAILABLE:
        raise Asn1DecodeError("asn1tools is required for ASN.1 decoding but is not installed.")
    if binary_column not in df.columns:
        raise Asn1DecodeError(f"binary_column '{binary_column}' not found in input DataFrame columns: {df.columns}")

    df = _materialize_hidden_metadata_column(df)
    # Resolve (explicit override, else auto-detect) exactly once, here on the driver, and pass
    # the concrete name down. Detection must not run per-partition on executors: it would repeat
    # the parse for every partition and, worse, log the same inference N times.
    module_types = _parse_module_types(schema_path)
    pdu_name = resolve_pdu_name(schema_path, pdu_name, module_types)
    field_defs = derive_asn1_field_defs(schema_path, pdu_name)
    root_is_choice = module_types.get(pdu_name, {}).get("type") == "CHOICE"

    passthrough_fields = [f for f in df.schema.fields if f.name != binary_column]
    decoded_fields = [StructField(field_def["name"], field_def["spark_type"], True) for field_def in field_defs]
    output_schema = StructType(passthrough_fields + decoded_fields + [StructField("_asn1_decode_error", StringType(), True)])

    partition_decoder = make_partition_decoder(
        [schema_path],
        codec,
        pdu_name,
        field_defs,
        binary_column,
        [f.name for f in passthrough_fields],
        module_types=module_types,
        root_is_choice=root_is_choice,
    )
    return df.mapInPandas(partition_decoder, schema=output_schema)
