#!/usr/bin/env python
"""Generate UC3 Excalibur test data (BUILD_CONTRACT.md section 3 / section 10).

This is a plain authoring script for test fixtures -- NOT framework configuration.
Hand-written Python is correct and expected here.

Column names, types and governance flags are read at runtime from the three
Excalibur governance sheets in ``BT_Usecase/UC3/data``.  No column list is hand-typed.

Outputs (default: local only -- ``--upload`` is opt-in):

    <out-dir>/streaming/<table>/<table>_stream.csv                (100 rows)
    <out-dir>/batch/<table>/batch_date=YYYY-MM-DD/<table>_batch.csv (30 rows x 4)

Contract rules implemented here
-------------------------------
* section 6.2  ingestible column  <=>  ``Feed column name`` non-empty;
              skip any row whose ``Reservoir Column Name`` is empty.
* section 6.1  header quirks: ``Sesnitive Columns`` is misspelled in the source;
              PHYSICAL_DEVICE spells the tier flag ``csql.securedro`` while the
              other two use ``csql.secured_ro``.  Lookup-with-fallback only.
* section 6.5  Oracle -> Spark type mapping; UPPERCASE sheets -> lowercase output.
* section 4    ``Drop(DF)=Y`` columns are absent from the CSV entirely (Drop wins
              over Null); ``Null(DF)=Y`` columns are present but always empty.
* section 6.3  emit ``src_deleted_flg`` (the simulator/CDC needs it); never emit
              ``hash_value`` nor the four legacy ``gcp_*`` audit columns.
* section 10   of the 120 batch rows per table, ~60% carry streaming PKs (a
              portion mutated so ``value_drift_count`` is non-zero) and ~40% are
              genuinely new (so ``missing_in_target_count`` is non-zero).

Deterministic: fixed seed, idempotent, re-runnable.

Usage
-----
    python scripts/generate_uc3_test_data.py
    python scripts/generate_uc3_test_data.py --out-dir build/uc3_test_data
    python scripts/generate_uc3_test_data.py --upload --catalog flowx --staging-schema staging
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import random
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

REPO_ROOT = Path(__file__).resolve().parents[1]
DDL_DIR = REPO_ROOT / "BT_Usecase" / "UC3" / "data"

DEFAULT_OUT_DIR = REPO_ROOT / "build" / "uc3_test_data"
DEFAULT_CATALOG = "flowx"
DEFAULT_STAGING_SCHEMA = "staging"
DEFAULT_VOLUME = "uc_3"

SEED = 20260803

STREAM_ROWS = 100
BATCH_ROWS_PER_SET = 30
BATCH_DATES = ["2026-08-01", "2026-08-02", "2026-08-03", "2026-08-04"]

#: fraction of batch rows whose PK is drawn from the streaming set (contract section 10)
BATCH_OVERLAP_FRACTION = 0.60
#: of those overlapping rows, the fraction whose non-key values are mutated
BATCH_MUTATION_FRACTION = 0.55
#: fraction of emitted rows carrying src_deleted_flg = 1
DELETE_FRACTION = 0.06

#: section 6.3 -- target-only audit columns.  ``src_deleted_flg`` IS emitted (the
#: simulator/CDC needs it); the rest are never emitted.
SRC_DELETED_FLG = "src_deleted_flg"
NEVER_EMIT_COLUMNS = frozenset(
    {
        "hash_value",
        "gcp_insert_date",
        "gcp_insert_user",
        "gcp_update_date",
        "gcp_update_user",
    }
)

TABLES = ("physical_device", "customer", "subscriber")
DDL_FILES = {
    "physical_device": "PHYSICAL_DEVICE_DDL.csv",
    "customer": "CUSTOMER_DDL.csv",
    "subscriber": "SUBSCRIBER_DDL.csv",
}

# Header names, verbatim from the sheets.  ``Sesnitive Columns`` is misspelled in
# the source -- read it as-is, do not "correct" it (contract section 6.1).
H_RESERVOIR_COLUMN = ("Reservoir Column Name",)
H_RESERVOIR_TYPE = ("Reservoir Data type",)
H_FEED_COLUMN = ("Feed column name",)
H_DATA_TYPE = ("Data type",)
H_PRIMARY_KEY = ("Primary Key",)
H_NULLABLE = ("Nullable",)
H_DROP = ("Drop(Specific to Data Fabric)",)
H_NULL = ("Null(Specific to Data Fabric)",)
# PHYSICAL_DEVICE spells this without the underscore -- fallback, never a fixed index.
H_CSQL_SECURED_RO = ("csql.secured_ro", "csql.securedro")

# Time envelope for the generated data (contract section 10: sys_update_date must
# vary so SCD2 history on customer has something to version).
CREATION_WINDOW_START = dt.datetime(2024, 1, 1, 0, 0, 0)
CREATION_WINDOW_END = dt.datetime(2026, 5, 31, 23, 59, 59)
UPDATE_WINDOW_END = dt.datetime(2026, 8, 4, 23, 59, 59)

TS_FORMAT = "%Y-%m-%d %H:%M:%S"


# --------------------------------------------------------------------------- #
# Type mapping (contract section 6.5)
# --------------------------------------------------------------------------- #

_NUMBER_RE = re.compile(r"^\s*NUMBER\s*\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\)\s*$", re.I)
_NUMERIC_RE = re.compile(r"^\s*NUMERIC\s*\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\)\s*$", re.I)
_CHARLIKE_RE = re.compile(r"^\s*(?:VARCHAR2|VARCHAR|CHAR|NVARCHAR2|NCHAR)\s*\(\s*(\d+)\s*\)\s*$", re.I)
_BARE_CHARLIKE_RE = re.compile(r"^\s*(?:VARCHAR2|VARCHAR|CHAR|CLOB|STRING)\s*$", re.I)


@dataclass(frozen=True)
class SparkType:
    """A mapped Spark type plus whatever the generator needs to make values."""

    kind: str  # "int" | "bigint" | "decimal" | "string" | "timestamp" | "boolean"
    precision: int | None = None
    scale: int | None = None
    length: int | None = None

    @property
    def name(self) -> str:
        if self.kind == "decimal":
            return f"DECIMAL({self.precision},{self.scale})"
        return self.kind.upper()


def map_oracle_type(oracle_type: str) -> SparkType:
    """Oracle -> Spark, per contract section 6.5.

    ``VARCHAR2(n)``/``CHAR(n)`` -> STRING; ``NUMBER(p,0)`` -> BIGINT (INT where
    p <= 9); ``NUMBER(p,s)`` with s > 0 -> DECIMAL(p,s); ``DATE``/``TIMESTAMP`` ->
    TIMESTAMP (Oracle DATE carries a time component); ``BOOLEAN`` -> BOOLEAN.
    """
    raw = (oracle_type or "").strip()
    upper = raw.upper()

    if upper in {"DATE", "TIMESTAMP"} or upper.startswith("TIMESTAMP"):
        return SparkType("timestamp")
    if upper == "BOOLEAN":
        return SparkType("boolean")

    for pattern in (_NUMBER_RE, _NUMERIC_RE):
        m = pattern.match(raw)
        if m:
            precision = int(m.group(1))
            scale = int(m.group(2)) if m.group(2) is not None else 0
            if scale > 0:
                return SparkType("decimal", precision=precision, scale=scale)
            if precision <= 9:
                return SparkType("int", precision=precision, scale=0)
            return SparkType("bigint", precision=precision, scale=0)

    m = _CHARLIKE_RE.match(raw)
    if m:
        return SparkType("string", length=int(m.group(1)))
    if _BARE_CHARLIKE_RE.match(raw):
        return SparkType("string", length=30)
    if upper in {"NUMBER", "NUMERIC", "INTEGER", "INT"}:
        return SparkType("bigint", precision=18, scale=0)

    # Unknown / blank -> treat as a short string.  Callers fall back to the
    # Reservoir type before reaching this point.
    return SparkType("string", length=30)


# --------------------------------------------------------------------------- #
# Sheet parsing (contract section 6.1 / 6.2)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ColumnSpec:
    name: str  # lowercase target name
    feed_name: str  # UPPERCASE feed name, verbatim from the sheet
    oracle_type: str
    spark_type: SparkType
    is_primary_key: bool
    nullable: bool
    drop_df: bool
    null_df: bool

    @property
    def emit(self) -> bool:
        """Drop(DF)=Y columns are absent from the schema entirely (contract section 4)."""
        return not self.drop_df


def _lookup(row: dict[str, str], candidates: Sequence[str]) -> str:
    """Lookup-with-fallback over header spellings.  Never a fixed column index."""
    for key in candidates:
        if key in row and row[key] is not None:
            return row[key].strip()
    # Case/whitespace-insensitive second pass, for defensive robustness only.
    normalized = {
        re.sub(r"\s+", " ", (k or "")).strip().lower(): (v or "")
        for k, v in row.items()
        if k is not None
    }
    for key in candidates:
        probe = re.sub(r"\s+", " ", key).strip().lower()
        if probe in normalized:
            return normalized[probe].strip()
    return ""


def _is_yes(value: str) -> bool:
    return (value or "").strip().upper().startswith("Y")


def parse_sheet(csv_path: Path) -> list[ColumnSpec]:
    """Read one governance sheet into an ordered list of ingestible columns.

    A row is an ingestible source column iff ``Feed column name`` is non-empty.
    Rows with an empty ``Reservoir Column Name`` are spreadsheet artefacts
    (legend rows) and are skipped (contract section 6.2).
    """
    with csv_path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"{csv_path} has no header row")
        rows = list(reader)

    columns: list[ColumnSpec] = []
    seen: set[str] = set()
    for row in rows:
        reservoir_col = _lookup(row, H_RESERVOIR_COLUMN)
        if not reservoir_col:
            continue  # spreadsheet artefact / legend row
        feed_col = _lookup(row, H_FEED_COLUMN)
        if not feed_col:
            continue  # target-only column, not ingestible

        name = reservoir_col.strip().lower()  # sheets are UPPERCASE; emit lowercase
        if name in seen:
            continue
        seen.add(name)

        oracle_type = _lookup(row, H_DATA_TYPE)
        if not oracle_type:
            # e.g. SUBSCRIBER.sub_status has a blank feed type; fall back to the
            # Reservoir type rather than guessing.
            oracle_type = _lookup(row, H_RESERVOIR_TYPE)

        # The tier flag is read purely to prove the fallback header lookup works
        # against both spellings; it does not shape the generated values.
        _lookup(row, H_CSQL_SECURED_RO)

        columns.append(
            ColumnSpec(
                name=name,
                feed_name=feed_col.strip(),
                oracle_type=oracle_type,
                spark_type=map_oracle_type(oracle_type),
                is_primary_key=_is_yes(_lookup(row, H_PRIMARY_KEY)),
                nullable=_is_yes(_lookup(row, H_NULLABLE)),
                drop_df=_is_yes(_lookup(row, H_DROP)),
                null_df=_is_yes(_lookup(row, H_NULL)),
            )
        )
    return columns


@dataclass(frozen=True)
class TableSpec:
    table: str
    columns: list[ColumnSpec]

    @property
    def emitted_columns(self) -> list[ColumnSpec]:
        return [c for c in self.columns if c.emit]

    @property
    def primary_keys(self) -> list[ColumnSpec]:
        return [c for c in self.columns if c.is_primary_key and c.emit]

    @property
    def dropped(self) -> list[str]:
        return [c.name for c in self.columns if c.drop_df]

    @property
    def nulled(self) -> list[str]:
        """Null(DF)=Y columns that survive Drop.  Drop wins (contract section 4)."""
        return [c.name for c in self.columns if c.null_df and c.emit]

    def header(self, *, include_batch_date: bool) -> list[str]:
        names = [c.name for c in self.emitted_columns]
        names.append(SRC_DELETED_FLG)
        if include_batch_date:
            names.append("batch_date")
        return names


def load_table_specs() -> dict[str, TableSpec]:
    specs: dict[str, TableSpec] = {}
    for table, filename in DDL_FILES.items():
        path = DDL_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"governance sheet not found: {path}")
        specs[table] = TableSpec(table=table, columns=parse_sheet(path))
    return specs


# --------------------------------------------------------------------------- #
# Deterministic value generation
# --------------------------------------------------------------------------- #

_WORDS = (
    "ALPHA BRAVO CHARLIE DELTA ECHO FOXTROT GOLF HOTEL INDIA JULIET KILO LIMA "
    "MIKE NOVEMBER OSCAR PAPA QUEBEC ROMEO SIERRA TANGO UNIFORM VICTOR WHISKEY "
    "XRAY YANKEE ZULU"
).split()

_STATUS_CODES = ("A", "I", "S", "P", "C", "D")
_YN = ("Y", "N")


def _stable_rng(*parts: Any) -> random.Random:
    """A Random seeded from SEED + the given parts.  Order-independent, stable."""
    digest = hashlib.sha256(
        ("|".join(str(p) for p in (SEED, *parts))).encode("utf-8")
    ).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _rand_datetime(rng: random.Random, start: dt.datetime, end: dt.datetime) -> dt.datetime:
    span = int((end - start).total_seconds())
    return start + dt.timedelta(seconds=rng.randrange(span + 1))


def _string_value(rng: random.Random, col: ColumnSpec, salt: int) -> str:
    length = col.spark_type.length or 30
    name = col.name
    if length == 1:
        if name.endswith("_flg") or name.endswith("_flag") or name.endswith("_ind"):
            return rng.choice(_YN)
        return rng.choice(_STATUS_CODES)
    if "email" in name:
        return f"{rng.choice(_WORDS).lower()}.{salt:05d}@example.com"[:length]
    if "phone" in name or name.endswith("_no") or "msisdn" in name:
        digits = "".join(str(rng.randrange(10)) for _ in range(max(length, 1)))
        return digits[:length]
    if "date" in name or "dt" == name[-2:]:
        return _rand_datetime(rng, CREATION_WINDOW_START, UPDATE_WINDOW_END).strftime(
            "%Y%m%d"
        )[:length]
    if "code" in name or "cd" == name[-2:] or length <= 6:
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ0123456789"
        return "".join(rng.choice(alphabet) for _ in range(length))
    base = f"{rng.choice(_WORDS)}_{rng.choice(_WORDS)}_{salt:05d}"
    return base[:length]


def _numeric_value(rng: random.Random, col: ColumnSpec) -> str:
    st = col.spark_type
    if st.kind == "decimal":
        precision = st.precision or 12
        scale = st.scale or 2
        int_digits = max(precision - scale, 1)
        whole = rng.randrange(10 ** min(int_digits, 9))
        frac = rng.randrange(10**scale)
        return f"{whole}.{frac:0{scale}d}"
    precision = st.precision or 9
    upper = 10 ** min(precision, 12) - 1
    return str(rng.randrange(0, min(upper, 999_999_999)))


def generate_value(rng: random.Random, col: ColumnSpec, salt: int) -> str:
    """A realistic-looking value for one non-key column, as a CSV string."""
    st = col.spark_type
    if st.kind == "timestamp":
        return _rand_datetime(rng, CREATION_WINDOW_START, UPDATE_WINDOW_END).strftime(
            TS_FORMAT
        )
    if st.kind == "boolean":
        return "true" if rng.random() < 0.5 else "false"
    if st.kind in {"int", "bigint", "decimal"}:
        return _numeric_value(rng, col)
    return _string_value(rng, col, salt)


# --------------------------------------------------------------------------- #
# Primary-key generation -- format is stable across streaming and batch sets
# --------------------------------------------------------------------------- #


def _pk_value(col: ColumnSpec, entity_id: int) -> str:
    """A PK value derived deterministically from an entity ordinal.

    The same ``entity_id`` always yields the same value for the same column, in
    the same type/format, so streaming and batch sets join cleanly (contract
    section 10 / requirement 8).
    """
    st = col.spark_type
    name = col.name
    if st.kind in {"int", "bigint", "decimal"}:
        if name == "customer_id":
            return str(700_000_000 + entity_id)
        if name == "phy_seq_no":
            return str(1 + (entity_id % 7))
        return str(100_000 + entity_id)
    length = st.length or 11
    if name == "subscriber_no":
        return f"{90_000_000_000 + entity_id:0{length}d}"[-length:]
    if name == "equipment_no":
        return f"IMEI{35_000_000 + entity_id:011d}"[:length]
    digits = f"{entity_id:0{length}d}"[-length:]
    return digits


@dataclass(frozen=True)
class EntityKey:
    """One logical row identity: the ordinal plus its rendered PK values."""

    entity_id: int
    values: tuple[tuple[str, str], ...]  # ((column_name, value), ...)

    def as_dict(self) -> dict[str, str]:
        return dict(self.values)


def make_entity(spec: TableSpec, entity_id: int) -> EntityKey:
    pks = spec.primary_keys
    if not pks:
        raise ValueError(f"{spec.table} has no primary key columns in the sheet")
    return EntityKey(
        entity_id=entity_id,
        values=tuple((c.name, _pk_value(c, entity_id)) for c in pks),
    )


# --------------------------------------------------------------------------- #
# Row building
# --------------------------------------------------------------------------- #


def build_row(
    spec: TableSpec,
    entity: EntityKey,
    *,
    variant: str,
    batch_date: str | None = None,
) -> dict[str, str]:
    """Build one full row.

    ``variant`` distinguishes independently-seeded value draws for the same
    entity, which is how a batch row gets *mutated* non-key values relative to
    its streaming counterpart while keeping identical PKs.
    """
    pk_values = entity.as_dict()
    nulled = set(spec.nulled)
    row: dict[str, str] = {}

    created = _rand_datetime(
        _stable_rng(spec.table, entity.entity_id, "created"),
        CREATION_WINDOW_START,
        CREATION_WINDOW_END,
    )
    # sys_update_date spread across a realistic range, always >= sys_creation_date
    upd_rng = _stable_rng(spec.table, entity.entity_id, variant, "updated")
    updated = _rand_datetime(upd_rng, created, UPDATE_WINDOW_END)

    for idx, col in enumerate(spec.emitted_columns):
        if col.name in pk_values:
            row[col.name] = pk_values[col.name]
            continue
        if col.name in nulled:
            # Null(DF)=Y -- present as a column, empty in every row (contract section 4)
            row[col.name] = ""
            continue
        if col.name == "sys_creation_date":
            row[col.name] = created.strftime(TS_FORMAT)
            continue
        if col.name == "sys_update_date":
            row[col.name] = updated.strftime(TS_FORMAT)
            continue
        rng = _stable_rng(spec.table, entity.entity_id, variant, col.name)
        row[col.name] = generate_value(rng, col, salt=(entity.entity_id * 31 + idx) % 100_000)

    del_rng = _stable_rng(spec.table, entity.entity_id, variant, "deleted_flg")
    row[SRC_DELETED_FLG] = "1" if del_rng.random() < DELETE_FRACTION else "0"

    if batch_date is not None:
        row["batch_date"] = batch_date
    return row


def write_csv(path: Path, header: Sequence[str], rows: Iterable[dict[str, str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    # newline="" + \n keeps the file byte-identical across reruns on Windows.
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(header), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


# --------------------------------------------------------------------------- #
# Dataset planning
# --------------------------------------------------------------------------- #


@dataclass
class TableResult:
    table: str
    stream_rows: int
    stream_columns: int
    batch_rows: dict[str, int]
    overlap_rows: int
    mutated_rows: int
    new_rows: int
    dropped: list[str]
    nulled: list[str]
    stream_deletes: int
    batch_deletes: int


def generate_table(spec: TableSpec, out_dir: Path) -> TableResult:
    table = spec.table

    # ---- streaming set: 100 rows, entities 0..99, no CDC envelope -------------
    stream_entities = [make_entity(spec, i) for i in range(STREAM_ROWS)]
    stream_header = spec.header(include_batch_date=False)
    stream_rows = [build_row(spec, e, variant="stream") for e in stream_entities]
    stream_path = out_dir / "streaming" / table / f"{table}_stream.csv"
    written = write_csv(stream_path, stream_header, stream_rows)
    stream_deletes = sum(1 for r in stream_rows if r[SRC_DELETED_FLG] == "1")

    # ---- batch sets: 4 x 30 rows, with the contract section 10 PK overlap -----
    total_batch = BATCH_ROWS_PER_SET * len(BATCH_DATES)
    overlap_target = round(total_batch * BATCH_OVERLAP_FRACTION)
    new_target = total_batch - overlap_target

    plan_rng = _stable_rng(table, "batch-plan")
    # Overlapping rows reuse streaming entity ordinals (0..99); new rows use a
    # disjoint ordinal band (>= 1000) so their PKs cannot collide.
    overlap_ids = [plan_rng.randrange(STREAM_ROWS) for _ in range(overlap_target)]
    new_ids = [1000 + i for i in range(new_target)]

    plan: list[tuple[int, str]] = []  # (entity_id, variant)
    mutated_target = round(overlap_target * BATCH_MUTATION_FRACTION)
    for pos, entity_id in enumerate(overlap_ids):
        # "stream" variant reproduces the streaming row byte-for-byte (a match);
        # "batch-mut" redraws non-key values (a value drift).
        plan.append((entity_id, "batch-mut" if pos < mutated_target else "stream"))
    for entity_id in new_ids:
        plan.append((entity_id, "batch-new"))
    plan_rng.shuffle(plan)

    batch_header = spec.header(include_batch_date=True)
    batch_counts: dict[str, int] = {}
    batch_deletes = 0
    for set_idx, batch_date in enumerate(BATCH_DATES):
        chunk = plan[set_idx * BATCH_ROWS_PER_SET : (set_idx + 1) * BATCH_ROWS_PER_SET]
        rows = [
            build_row(spec, make_entity(spec, eid), variant=variant, batch_date=batch_date)
            for eid, variant in chunk
        ]
        path = (
            out_dir
            / "batch"
            / table
            / f"batch_date={batch_date}"
            / f"{table}_batch.csv"
        )
        batch_counts[batch_date] = write_csv(path, batch_header, rows)
        batch_deletes += sum(1 for r in rows if r[SRC_DELETED_FLG] == "1")

    return TableResult(
        table=table,
        stream_rows=written,
        stream_columns=len(stream_header),
        batch_rows=batch_counts,
        overlap_rows=overlap_target,
        mutated_rows=mutated_target,
        new_rows=new_target,
        dropped=spec.dropped,
        nulled=spec.nulled,
        stream_deletes=stream_deletes,
        batch_deletes=batch_deletes,
    )


# --------------------------------------------------------------------------- #
# Upload (opt-in only -- default is LOCAL-ONLY generation)
# --------------------------------------------------------------------------- #


def upload(out_dir: Path, catalog: str, staging_schema: str, volume: str, profile: str | None) -> None:
    """Copy the generated tree to the UC volume via the Databricks CLI.

    Layout (contract section 3.3 / section 10):
        /Volumes/{catalog}/{schema}/{volume}/streaming/<table>/<table>_stream.csv
        /Volumes/{catalog}/{schema}/{volume}/batch/<table>/batch_date=YYYY-MM-DD/<table>_batch.csv
    """
    cli = shutil.which("databricks")
    if not cli:
        raise SystemExit("databricks CLI not found on PATH; cannot --upload")
    # A UC volume path is always /Volumes/{catalog}/{schema}/{volume}/..., with the volume as a
    # real path segment (cf. the framework's own specs, e.g.
    # /Volumes/{{catalog}}/EA_usecase/landing_zip/incoming/). The volume here is the EXISTING
    # flowx.staging.uc_3 -- no UC3-specific volume is created.
    base = f"/Volumes/{catalog}/{staging_schema}/{volume}"
    for local in sorted(out_dir.rglob("*.csv")):
        remote = f"{base}/{local.relative_to(out_dir).as_posix()}"
        cmd = [cli, "fs", "cp", "--overwrite", str(local), f"dbfs:{remote}"]
        if profile:
            cmd += ["--profile", profile]
        print(f"  upload {local.relative_to(out_dir).as_posix()} -> {remote}")
        subprocess.run(cmd, check=True)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate UC3 Excalibur test data (local by default; --upload is opt-in)."
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"local output directory (default: {DEFAULT_OUT_DIR})",
    )
    p.add_argument(
        "--upload",
        action="store_true",
        help="opt in to uploading the generated tree to the UC volume via the Databricks CLI",
    )
    p.add_argument("--catalog", default=DEFAULT_CATALOG, help=f"UC catalog (default: {DEFAULT_CATALOG})")
    p.add_argument(
        "--staging-schema",
        default=DEFAULT_STAGING_SCHEMA,
        help=f"UC staging schema (default: {DEFAULT_STAGING_SCHEMA})",
    )
    p.add_argument("--volume", default=DEFAULT_VOLUME, help=f"UC volume (default: {DEFAULT_VOLUME})")
    p.add_argument("--profile", default=None, help="Databricks CLI profile for --upload")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    out_dir: Path = args.out_dir.resolve()

    # Idempotent: clear only the two trees this script owns.
    for sub in ("streaming", "batch"):
        target = out_dir / sub
        if target.exists():
            shutil.rmtree(target)
    out_dir.mkdir(parents=True, exist_ok=True)

    specs = load_table_specs()
    results = [generate_table(specs[t], out_dir) for t in TABLES]

    print(f"UC3 test data written to {out_dir}")
    print(f"seed={SEED} (deterministic, re-runnable)\n")
    for r in results:
        total_batch = sum(r.batch_rows.values())
        print(f"[{r.table}]")
        print(f"  streaming : {r.stream_rows} rows x {r.stream_columns} cols "
              f"({r.stream_deletes} with src_deleted_flg=1)")
        print(f"  batch     : {total_batch} rows across {len(r.batch_rows)} sets "
              f"({', '.join(f'{d}={n}' for d, n in sorted(r.batch_rows.items()))})")
        pct_o = 100.0 * r.overlap_rows / total_batch
        pct_n = 100.0 * r.new_rows / total_batch
        print(f"  overlap   : {r.overlap_rows} rows ({pct_o:.1f}%) share streaming PKs, "
              f"of which {r.mutated_rows} mutated -> value_drift")
        print(f"  new PKs   : {r.new_rows} rows ({pct_n:.1f}%) -> missing_in_target")
        print(f"  dropped   : {r.dropped or '(none)'}")
        print(f"  nulled    : {r.nulled or '(none)'}")

    if args.upload:
        print("\nuploading to UC volume ...")
        upload(out_dir, args.catalog, args.staging_schema, args.volume, args.profile)
    else:
        print("\n(local only -- pass --upload to publish to the UC volume)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
