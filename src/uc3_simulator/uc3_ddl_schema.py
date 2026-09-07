"""# UC3 DDL schema helper (shared by the simulator tasks)

Parses the three Excalibur governance sheets in `BT_Usecase/UC3/data/*_DDL.csv` into a Spark
column list, applying **BUILD_CONTRACT.md** §6 verbatim:

| Contract rule | Implementation |
|---|---|
| §6.2 a column is ingestible iff `Feed column name` is non-empty | `_business_rows` |
| §6.2 skip rows whose `Reservoir Column Name` is empty (spreadsheet artefacts) | `_business_rows` |
| §6.1 header quirks (`Sesnitive Columns` typo, `csql.securedro` vs `csql.secured_ro`) | `_lookup` -- lookup-with-fallback, never a fixed index |
| §4 `Drop(DF)=Y` -> column absent from the schema entirely | `DROP_FLAG` filter |
| §4 `Null(DF)=Y` -> column present, nullable | flags are carried but the column is kept |
| §6.3 do **not** create `hash_value` or the four `gcp_*` columns | they are target-only rows (empty `Feed column name`) and so never reach the schema |
| §6.3 `src_deleted_flg` **must** be emitted by this simulator | appended explicitly by `build_schema` |
| §6.5 UPPERCASE sheets -> lowercase targets | `Reservoir Column Name` is already lowercase; `.lower()` applied defensively |
| §6.5 Oracle -> Spark type mapping | `oracle_to_spark` |

This file is a PLAIN PYTHON MODULE (importable), not a notebook: the simulator is the one
deliberately hand-written artefact in UC3 (contract §0.1) and deliberately does not
extend the framework package.
"""

import csv
import io
import os
import re

# BUILD_CONTRACT.md 2 - the three simulator tables, and the DDL sheet each one reads.
UC3_TABLES = {
    "physical_device": "PHYSICAL_DEVICE_DDL.csv",
    "customer": "CUSTOMER_DDL.csv",
    "subscriber": "SUBSCRIBER_DDL.csv",
}

# ---------------------------------------------------------------------------
# The CDC delete signal -- a DELIBERATE, REPORTED deviation from the sheets.
#
# The DDL sheets nominally type `src_deleted_flg` as BOOLEAN. This simulator emits it as a
# STRING instead, because of how the framework actually consumes it downstream:
#
#   spec_validator.py  cdc_operation_mapping.delete_values -> check_list_of_str(...)
#                      i.e. the delete markers MUST be a list of STRINGS.
#   cdc/scd.py:95      F.col(cdc_operation_column).isin(delete_values)
#
# A BOOLEAN column probed with `.isin(["1"])` relies on implicit boolean<->string coercion
# and does not match dependably. Every existing spec in flowx_testing/ pairs a string-valued
# operation column with string delete markers (e.g. delete_values: ["DELETED"]).
#
# So: STRING, '1' = deleted, '0' = present. Job 2/3 specs must therefore carry
#   "cdc_operation_column": "src_deleted_flg",
#   "cdc_operation_mapping": {"delete_values": ["1"]}
#
# NOTE: BUILD_CONTRACT 3 currently writes this as {"DELETE": "1"}, which the validator
# rejects -- `delete_values` is the only accepted key. 3 itself says the reference wins.
SRC_DELETED_FLG = "src_deleted_flg"
SRC_DELETED_FLG_TYPE = "STRING"
SRC_DELETED_FLG_DELETE = "1"
SRC_DELETED_FLG_PRESENT = "0"

# BUILD_CONTRACT.md 6.1 - header names, each with the variants seen across the three sheets.
# Lookup-with-fallback, NEVER a fixed column index.
_HEADER_ALIASES = {
    "reservoir_column_name": ["Reservoir Column Name"],
    "reservoir_data_type": ["Reservoir Data type", "Reservoir Data Type"],
    "feed_column_name": ["Feed column name", "Feed Column Name"],
    "data_type": ["Data type", "Data Type"],
    "primary_key": ["Primary Key"],
    "nullable": ["Nullable"],
    "field_description": ["Field Description"],
    # 6.1: misspelled in the source. Read it verbatim; do not "correct" it.
    "sensitive_columns": ["Sesnitive Columns", "Sensitive Columns"],
    "drop_df": ["Drop(Specific to Data Fabric)"],
    "null_df": ["Null(Specific to Data Fabric)"],
    # 6.1: PHYSICAL_DEVICE spells this `csql.securedro` (no underscore).
    "csql_secured_ro": ["csql.secured_ro", "csql.securedro"],
    "csql_ro": ["csql.ro"],
    "bq_deid_ro": ["bq.deid_ro"],
}


def _lookup(row, logical_name, default=""):
    """Fetch a cell by logical name, tolerating the sheets' header variants."""
    for candidate in _HEADER_ALIASES[logical_name]:
        if candidate in row and row[candidate] is not None:
            return row[candidate].strip()
    return default


def _is_yes(value):
    return (value or "").strip().upper() == "Y"


def oracle_to_spark(oracle_type):
    """BUILD_CONTRACT.md 6.5 -- Oracle -> Spark type mapping.

    VARCHAR2(n)/CHAR(n) -> STRING; NUMBER(p,0) -> BIGINT (INT where p <= 9);
    NUMBER(p,s) s>0 -> DECIMAL(p,s); DATE/TIMESTAMP -> TIMESTAMP (Oracle DATE carries a
    time component); BOOLEAN -> BOOLEAN.
    """
    raw = (oracle_type or "").strip()
    if not raw:
        raise ValueError("empty Oracle type")
    upper = raw.upper()

    if upper.startswith("VARCHAR2") or upper.startswith("VARCHAR") or upper.startswith("CHAR") or upper.startswith("NVARCHAR") or upper.startswith("NCHAR"):
        return "STRING"
    if upper.startswith("BOOLEAN"):
        return "BOOLEAN"
    if upper.startswith("DATE") or upper.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if upper.startswith("CLOB") or upper.startswith("NCLOB"):
        return "STRING"
    if upper.startswith("BLOB") or upper.startswith("RAW"):
        return "BINARY"
    if upper.startswith("FLOAT") or upper.startswith("BINARY_DOUBLE"):
        return "DOUBLE"
    if upper.startswith("BINARY_FLOAT"):
        return "FLOAT"

    match = re.match(r"^\s*(?:NUMBER|NUMERIC|DECIMAL)\s*(?:\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\))?\s*$", upper)
    if match:
        precision_text, scale_text = match.group(1), match.group(2)
        if precision_text is None:
            # Unqualified NUMBER -- Spark's widest safe landing type.
            return "DECIMAL(38,10)"
        precision = int(precision_text)
        scale = int(scale_text) if scale_text is not None else 0
        if scale <= 0:
            return "INT" if precision <= 9 else "BIGINT"
        return "DECIMAL({p},{s})".format(p=precision, s=scale)

    raise ValueError("unmapped Oracle type: {t!r}".format(t=raw))


def _business_rows(ddl_csv_text):
    """BUILD_CONTRACT.md 6.2 -- the two kinds of row.

    A row is a *source business column* iff `Feed column name` is non-empty. Rows with an
    empty `Reservoir Column Name` are spreadsheet artefacts (legend rows, trailing blanks)
    and are skipped outright. Everything else (the 6-column audit envelope: `hash_value`,
    `src_deleted_flg`, four `gcp_*`) is target-only and never reaches the schema.
    """
    reader = csv.DictReader(io.StringIO(ddl_csv_text))
    rows = []
    for raw_row in reader:
        reservoir_name = _lookup(raw_row, "reservoir_column_name")
        if not reservoir_name:
            continue
        if not _lookup(raw_row, "feed_column_name"):
            continue
        rows.append(raw_row)
    return rows


def parse_ddl(ddl_csv_text):
    """Return the ordered business-column inventory for one sheet.

    Each entry: {name, spark_type, nullable, primary_key, drop_df, null_df, description}.
    Sheet order is preserved -- BUILD_CONTRACT.md 3 calls the PK order load-bearing.
    """
    columns = []
    seen = set()
    for row in _business_rows(ddl_csv_text):
        # 6.5 -- sheets are UPPERCASE-ish; targets are lowercase.
        name = _lookup(row, "reservoir_column_name").lower()
        if name in seen:
            raise ValueError("duplicate column in DDL sheet: {n}".format(n=name))
        seen.add(name)

        # SUBSCRIBER.sub_status carries a whitespace-only feed `Data type`; the
        # `Reservoir Data type` column is the fallback authority for that one row.
        declared = _lookup(row, "data_type") or _lookup(row, "reservoir_data_type")
        columns.append(
            {
                "name": name,
                "spark_type": oracle_to_spark(declared),
                "oracle_type": declared,
                # The sheet's `Nullable` column is a POSITIVE assertion: `Y` = this column
                # is nullable, `N` = it is mandatory. (Verified against PHYSICAL_DEVICE:
                # the four PK rows all carry `Nullable=N`.) A blank is treated as nullable.
                # Contract 4: Null(DF)=Y columns are ALWAYS present and nullable, which
                # overrides a sheet `Nullable=N` -- the value is deliberately forced to NULL
                # downstream, so a NOT NULL constraint there would be self-contradictory.
                "nullable": _is_yes(_lookup(row, "nullable"))
                or _is_yes(_lookup(row, "null_df"))
                or not _lookup(row, "nullable"),
                "primary_key": _is_yes(_lookup(row, "primary_key")),
                "drop_df": _is_yes(_lookup(row, "drop_df")),
                "null_df": _is_yes(_lookup(row, "null_df")),
                "description": _lookup(row, "field_description"),
            }
        )
    return columns


def build_schema(ddl_csv_text):
    """The physical column list for `<table>_stream`, contract 4 + 6.3 applied.

    - `Drop(DF)=Y` columns are removed entirely (SUBSCRIBER: ctn_password, sub_password).
    - `Null(DF)=Y` columns stay, nullable.
    - `src_deleted_flg STRING` is appended -- it is the CDC delete signal (6.3) and is
      *not* a source column, so the simulator is what must emit it. STRING (not the sheet's
      nominal BOOLEAN) is deliberate: `cdc_operation_mapping.delete_values` is validated as a
      LIST OF STRINGS and evaluated as `col(...).isin(delete_values)`, so a real BOOLEAN column
      would depend on implicit coercion to match. See SRC_DELETED_FLG_TYPE.
    - `hash_value` and the four `gcp_*` columns are NOT created (6.3). They never appear
      here because they are target-only rows in the sheet.
    """
    kept = [c for c in parse_ddl(ddl_csv_text) if not c["drop_df"]]
    dropped = [c["name"] for c in parse_ddl(ddl_csv_text) if c["drop_df"]]

    fields = [
        {
            "name": c["name"],
            "spark_type": c["spark_type"],
            "nullable": c["nullable"],
            "comment": c["description"],
        }
        for c in kept
    ]
    fields.append(
        {
            "name": SRC_DELETED_FLG,
            "spark_type": SRC_DELETED_FLG_TYPE,
            "nullable": True,
            "comment": (
                "CDC delete signal (BUILD_CONTRACT 6.3); emitted by the UC3 simulator, not a "
                "source column. STRING '1'=delete / '0'=upsert, to match "
                "cdc_operation_mapping.delete_values, which the framework validates as a LIST "
                "OF STRINGS and evaluates as col(...).isin(delete_values)."
            ),
        }
    )
    return {
        "fields": fields,
        "primary_keys": [c["name"] for c in kept if c["primary_key"]],
        "dropped_columns": dropped,
        "nulled_columns": [c["name"] for c in kept if c["null_df"]],
    }


def _escape_comment(text):
    """Single-quote escape for a SQL COMMENT literal, trimmed to a sane length."""
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    return cleaned[:250].replace("'", "''")


def build_create_table_sql(catalog, schema, table, schema_spec, table_comment=""):
    """Idempotent CREATE TABLE IF NOT EXISTS for `<catalog>.<schema>.<table>_stream`."""
    column_clauses = []
    for field in schema_spec["fields"]:
        clause = "  `{n}` {t}".format(n=field["name"], t=field["spark_type"])
        if not field["nullable"]:
            clause += " NOT NULL"
        if field.get("comment"):
            clause += " COMMENT '{c}'".format(c=_escape_comment(field["comment"]))
        column_clauses.append(clause)

    sql = "CREATE TABLE IF NOT EXISTS `{c}`.`{s}`.`{t}` (\n{cols}\n) USING DELTA".format(
        c=catalog, s=schema, t=table, cols=",\n".join(column_clauses)
    )
    if table_comment:
        sql += "\nCOMMENT '{c}'".format(c=_escape_comment(table_comment))
    return sql


def read_ddl_text(ddl_dir, table):
    """Read one governance sheet from the deployed workspace files (or a local checkout)."""
    path = os.path.join(ddl_dir, UC3_TABLES[table])
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        return handle.read()
