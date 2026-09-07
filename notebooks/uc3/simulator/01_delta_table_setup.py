# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 Simulator -- Task A: `delta_table_setup`
# MAGIC
# MAGIC Idempotent `CREATE TABLE IF NOT EXISTS` for the three UC3 streaming staging tables
# MAGIC (`BUILD_CONTRACT.md` 2):
# MAGIC
# MAGIC ```
# MAGIC {catalog}.{schema}.physical_device_stream
# MAGIC {catalog}.{schema}.customer_stream
# MAGIC {catalog}.{schema}.subscriber_stream
# MAGIC ```
# MAGIC
# MAGIC plus the producer's own cursor table, `{catalog}.{schema}.uc3_simulator_offsets`.
# MAGIC
# MAGIC **The column list is never hand-typed.** It is derived from the three Excalibur
# MAGIC governance sheets in `BT_Usecase/UC3/data/*_DDL.csv` by `uc3_ddl_schema.py`, which applies
# MAGIC contract 6's parsing rules and 6.5's Oracle->Spark type mapping. Consequences that
# MAGIC this notebook asserts before it writes anything:
# MAGIC
# MAGIC - `Drop(DF)=Y` columns are **absent from the table entirely** -- SUBSCRIBER's
# MAGIC   `ctn_password` / `sub_password` do not exist here (contract 4).
# MAGIC - `Null(DF)=Y` columns are **present and nullable** (contract 4).
# MAGIC - `src_deleted_flg BOOLEAN` **is** created -- CDC needs it, and it is not a source
# MAGIC   column, so the simulator is what supplies it (contract 6.3).
# MAGIC - `hash_value` and the four legacy `gcp_*` columns are **not** created (contract 6.3);
# MAGIC   FlowX supplies the equivalents as `__framework_*` technical metadata.
# MAGIC
# MAGIC This notebook is safe to re-run: every statement is `IF NOT EXISTS`, and it never
# MAGIC drops, alters or truncates anything.
# MAGIC
# MAGIC ## Why this is hand-written code at all
# MAGIC
# MAGIC Contract 0.1: the simulator is the **one** legitimately custom artefact in UC3,
# MAGIC because there is no framework feature for "pretend to be a message bus". Everything
# MAGIC downstream (CDC, hashing, tagging, SCD) is FlowX configuration and is **not**
# MAGIC implemented here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Nothing is hardcoded in the body -- `flowx` / `staging` appear only as widget
# MAGIC defaults, per contract 1.

# COMMAND ----------

dbutils.widgets.text("catalog", "flowx", "Catalog")
dbutils.widgets.text("schema", "staging", "Staging schema")
dbutils.widgets.text("ddl_dir", "", "Directory holding the *_DDL.csv governance sheets")

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("uc3_delta_table_setup")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
DDL_DIR_PARAM = dbutils.widgets.get("ddl_dir").strip()

if not CATALOG or not SCHEMA:
    raise ValueError("`catalog` and `schema` are required parameters and must be non-empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locate the shared schema helper and the governance sheets
# MAGIC
# MAGIC `uc3_ddl_schema.py` lives in `src/uc3_simulator/` (the code comment below explains why it
# MAGIC is not beside this notebook). The governance sheets live in `BT_Usecase/UC3/data/`, reached
# MAGIC three levels up from this notebook in the deployed workspace tree
# MAGIC (`notebooks/uc3/simulator/` -> repo root -> `BT_Usecase/UC3/data/`). `ddl_dir` can
# MAGIC override the latter, which is what makes the notebook runnable from a local checkout
# MAGIC as well as from `${workspace.file_path}`.

# COMMAND ----------


def _notebook_dir():
    """Directory containing this notebook, in both workspace and local execution."""
    if "__file__" in globals():
        return os.path.dirname(os.path.abspath(__file__))
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        notebook_path = ctx.notebookPath().get()
        return "/Workspace" + os.path.dirname(notebook_path)
    except Exception:  # noqa: BLE001 - best-effort; falls through to cwd
        return os.getcwd()


NOTEBOOK_DIR = _notebook_dir()
# The shared helper lives at src/uc3_simulator/, NOT alongside this notebook. Anything under
# notebooks/ is deployed by DABs as a NOTEBOOK, and Databricks refuses `import` on a notebook
# ("Unable to import module ... appears to be a notebook") -- which is exactly how the first
# Phase C run of this job failed. src/ is deployed as plain files, so it imports normally.
_REPO_ROOT = os.path.abspath(os.path.join(NOTEBOOK_DIR, "..", "..", ".."))
for _p in (os.path.join(_REPO_ROOT, "src", "uc3_simulator"), NOTEBOOK_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import uc3_ddl_schema  # noqa: E402

REPO_ROOT = _REPO_ROOT
DDL_DIR = DDL_DIR_PARAM or os.path.join(REPO_ROOT, "BT_Usecase", "UC3", "data")

logger.info("notebook dir : %s", NOTEBOOK_DIR)
logger.info("DDL dir      : %s", DDL_DIR)

missing = [
    name for name in uc3_ddl_schema.UC3_TABLES.values() if not os.path.exists(os.path.join(DDL_DIR, name))
]
if missing:
    raise FileNotFoundError(
        "Governance sheets not found under {d}: {m}. Pass `ddl_dir` explicitly if the "
        "deployed layout differs.".format(d=DDL_DIR, m=missing)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract assertions
# MAGIC
# MAGIC These run **before** any DDL. If the sheets ever change in a way that violates
# MAGIC contract 3 / 4 / 6.3, this task fails loudly here rather than silently creating a
# MAGIC table with the wrong governance shape.

# COMMAND ----------

# BUILD_CONTRACT.md 3 -- PK order is load-bearing.
EXPECTED_PRIMARY_KEYS = {
    "physical_device": ["customer_id", "subscriber_no", "equipment_no", "phy_seq_no"],
    "customer": ["customer_id"],
    "subscriber": ["subscriber_no", "customer_id"],
}
# BUILD_CONTRACT.md 4 -- Drop wins for subscriber; a dropped column cannot also be nulled.
EXPECTED_DROPPED = {
    "physical_device": [],
    "customer": [],
    "subscriber": ["ctn_password", "sub_password"],
}
EXPECTED_NULLED = {
    "physical_device": ["esn_pin", "blacklist_password"],
    "customer": ["gur_cr_card_no", "acc_password", "imei_black_list_pass"],
    "subscriber": [],
}
# BUILD_CONTRACT.md 6.2 -- business-column counts, confirmed against 1.
EXPECTED_BUSINESS_COLUMNS = {"physical_device": 31, "customer": 90, "subscriber": 133}
# BUILD_CONTRACT.md 6.3 -- must never be materialised by this simulator.
FORBIDDEN_COLUMNS = {"hash_value", "gcp_insert_date", "gcp_insert_user", "gcp_update_date", "gcp_update_user"}

schemas = {}
for table in uc3_ddl_schema.UC3_TABLES:
    ddl_text = uc3_ddl_schema.read_ddl_text(DDL_DIR, table)
    parsed = uc3_ddl_schema.parse_ddl(ddl_text)
    spec = uc3_ddl_schema.build_schema(ddl_text)
    schemas[table] = spec

    names = [f["name"] for f in spec["fields"]]

    if len(parsed) != EXPECTED_BUSINESS_COLUMNS[table]:
        raise AssertionError(
            "{t}: expected {e} business columns (contract 6.2), sheet yields {a}".format(
                t=table, e=EXPECTED_BUSINESS_COLUMNS[table], a=len(parsed)
            )
        )
    if spec["primary_keys"] != EXPECTED_PRIMARY_KEYS[table]:
        raise AssertionError(
            "{t}: PK order mismatch (contract 3). expected {e}, sheet yields {a}".format(
                t=table, e=EXPECTED_PRIMARY_KEYS[table], a=spec["primary_keys"]
            )
        )
    if sorted(spec["dropped_columns"]) != sorted(EXPECTED_DROPPED[table]):
        raise AssertionError(
            "{t}: Drop(DF) mismatch (contract 4). expected {e}, sheet yields {a}".format(
                t=table, e=EXPECTED_DROPPED[table], a=spec["dropped_columns"]
            )
        )
    if sorted(spec["nulled_columns"]) != sorted(EXPECTED_NULLED[table]):
        raise AssertionError(
            "{t}: Null(DF) mismatch (contract 4). expected {e}, sheet yields {a}".format(
                t=table, e=EXPECTED_NULLED[table], a=spec["nulled_columns"]
            )
        )
    for dropped in EXPECTED_DROPPED[table]:
        if dropped in names:
            raise AssertionError(
                "{t}: Drop(DF)=Y column {c!r} must be ABSENT from the schema (contract 4)".format(t=table, c=dropped)
            )
    for nulled in EXPECTED_NULLED[table]:
        field = next(f for f in spec["fields"] if f["name"] == nulled)
        if not field["nullable"]:
            raise AssertionError("{t}: Null(DF)=Y column {c!r} must be nullable (contract 4)".format(t=table, c=nulled))
    forbidden_present = FORBIDDEN_COLUMNS.intersection(names)
    if forbidden_present:
        raise AssertionError(
            "{t}: contract 6.3 forbids materialising {c} -- FlowX supplies the "
            "equivalents as __framework_* technical metadata".format(t=table, c=sorted(forbidden_present))
        )
    if "src_deleted_flg" not in names:
        raise AssertionError("{t}: src_deleted_flg is required by CDC (contract 6.3)".format(t=table))

    logger.info(
        "%-16s OK  %3d physical columns (%d business + src_deleted_flg, %d dropped)",
        table,
        len(names),
        len(parsed) - len(spec["dropped_columns"]),
        len(spec["dropped_columns"]),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the staging tables
# MAGIC
# MAGIC The catalog and schema are workspace prerequisites (contract 1: the catalog cannot be
# MAGIC declared in YAML, and Phase C checks the schema/volume quota headroom). This task
# MAGIC verifies the schema is reachable and fails with an actionable message if it is not --
# MAGIC it deliberately does **not** `CREATE SCHEMA`, per contract 13.

# COMMAND ----------

try:
    spark.sql("DESCRIBE SCHEMA `{c}`.`{s}`".format(c=CATALOG, s=SCHEMA))
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        "Schema `{c}`.`{s}` is not reachable. It is a Phase C workspace prerequisite "
        "(BUILD_CONTRACT 1 records a metastore schema/volume quota ceiling on this "
        "catalog); this task does not create it. Original error: {e}".format(c=CATALOG, s=SCHEMA, e=exc)
    ) from exc

TABLE_COMMENT = (
    "UC3 Excalibur streaming staging -- populated by job "
    "003_lfj_uc3_excalibur_streaming_simulator, which stands in for ZeroBus/Kafka. "
    "Schema derived from BT_Usecase/UC3/data/*_DDL.csv per BUILD_CONTRACT 6."
)

created = []
for table, spec in schemas.items():
    target = "{t}_stream".format(t=table)
    ddl = uc3_ddl_schema.build_create_table_sql(CATALOG, SCHEMA, target, spec, TABLE_COMMENT)
    spark.sql(ddl)
    created.append("`{c}`.`{s}`.`{t}`".format(c=CATALOG, s=SCHEMA, t=target))
    logger.info("ensured %s.%s.%s (%d columns)", CATALOG, SCHEMA, target, len(spec["fields"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the producer's cursor table
# MAGIC
# MAGIC Task B tracks how far it has drained each source CSV in
# MAGIC `{catalog}.{schema}.uc3_simulator_offsets`, one row per `(table_name, source_signature)`.
# MAGIC Keeping the cursor in a Delta table (rather than a checkpoint) is what makes the
# MAGIC producer resumable across job runs and makes each tick load a **distinct** slice
# MAGIC instead of a full reload.

# COMMAND ----------

OFFSET_TABLE = "uc3_simulator_offsets"

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS `{c}`.`{s}`.`{o}` (
      `table_name`       STRING  NOT NULL COMMENT 'UC3 logical table: physical_device | customer | subscriber',
      `source_signature` STRING  NOT NULL COMMENT 'Fingerprint of the source CSV set; a change resets the cursor',
      `next_offset`      BIGINT  NOT NULL COMMENT 'Zero-based row index of the next unconsumed source row',
      `total_rows`       BIGINT  NOT NULL COMMENT 'Total rows available in the source CSV set',
      `rows_emitted`     BIGINT  NOT NULL COMMENT 'Cumulative rows appended into <table>_stream for this signature',
      `ticks`            BIGINT  NOT NULL COMMENT 'Number of producer ticks executed for this signature',
      `exhausted`        BOOLEAN NOT NULL COMMENT 'True once next_offset has reached total_rows',
      `updated_at`       TIMESTAMP        COMMENT 'Wall-clock time of the last cursor advance'
    ) USING DELTA
    COMMENT 'Cursor state for the UC3 streaming simulator (job 003_lfj_uc3_excalibur_streaming_simulator). Not a business table.'
    """.format(c=CATALOG, s=SCHEMA, o=OFFSET_TABLE)
)
logger.info("ensured %s.%s.%s (simulator cursor)", CATALOG, SCHEMA, OFFSET_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary_lines = ["UC3 delta_table_setup complete -- idempotent, nothing dropped or altered.", ""]
for table, spec in schemas.items():
    summary_lines.append(
        "  {t}_stream: {n} columns | pk={pk} | dropped={d} | nulled={nl}".format(
            t=table,
            n=len(spec["fields"]),
            pk=spec["primary_keys"],
            d=spec["dropped_columns"] or "(none)",
            nl=spec["nulled_columns"] or "(none)",
        )
    )
summary_lines.append("")
summary_lines.append("  cursor table: {c}.{s}.{o}".format(c=CATALOG, s=SCHEMA, o=OFFSET_TABLE))
summary = "\n".join(summary_lines)
print(summary)
logger.info("delta_table_setup finished")

dbutils.notebook.exit(summary)
