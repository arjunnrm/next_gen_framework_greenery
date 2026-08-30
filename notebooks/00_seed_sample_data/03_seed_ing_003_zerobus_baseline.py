# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-003 -- Zerobus Baseline/Append Synthetic Rows
# MAGIC
# MAGIC Generates a batch of synthetic customer rows directly on-cluster (via `spark.range`,
# MAGIC no static CSV fixture -- a 1,000+-row literal CSV would add size with no real fixture
# MAGIC value over generated data) and MERGEs them into the same
# MAGIC `{catalog}.Excalibur_usecase.zerobus_source_bus` table scenario 002/003 already seed a
# MAGIC handful of illustrative `C001`-`C007` rows into (see
# MAGIC `02_seed_metaflow_testing_data.py::seed_zerobus_style_table_from_csv`). Synthetic rows are
# MAGIC namespaced under a `<id_prefix>-NNNNNN` `customer_id` (default prefix `TC003`) so they
# MAGIC never collide with those existing fixture ids or scenario 003/004's own
# MAGIC reconciliation-match assertions, which key off the real `C00N` ids specifically.
# MAGIC
# MAGIC Called twice by `metaflow_test_ing_003_zerobus_append_job`, with different
# MAGIC `start_index`/`row_count` widget values:
# MAGIC * **Baseline**: `start_index=1`, `row_count=1000` -> `TC003-000001`..`TC003-001000`.
# MAGIC * **Append**: `start_index=1001`, `row_count=200` -> `TC003-001001`..`TC003-001200`.
# MAGIC
# MAGIC Idempotent: each call first counts existing rows already inside its own
# MAGIC `[start_index, start_index + row_count)` id range and skips generation entirely if that
# MAGIC range is already fully populated -- a job re-run never duplicates rows or inflates the
# MAGIC assertion this test exists to prove.

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_ing_003_zerobus_baseline")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
dbutils.widgets.text("id_prefix", "TC003", "Synthetic customer_id prefix")
dbutils.widgets.text("start_index", "1", "First synthetic row's 1-based index")
dbutils.widgets.text("row_count", "1000", "How many rows to generate this call")

CATALOG = dbutils.widgets.get("catalog").strip()
ID_PREFIX = dbutils.widgets.get("id_prefix").strip()
START_INDEX = int(dbutils.widgets.get("start_index").strip())
ROW_COUNT = int(dbutils.widgets.get("row_count").strip())

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if not ID_PREFIX:
    raise ValueError("The 'id_prefix' widget must be a non-empty string.")
if START_INDEX < 1:
    raise ValueError("The 'start_index' widget must be >= 1 (customer ids are 1-based).")
if ROW_COUNT <= 0:
    raise ValueError("The 'row_count' widget must be a positive integer.")

TARGET_TABLE = f"{CATALOG}.Excalibur_usecase.zerobus_source_bus"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate + idempotently MERGE this call's synthetic row range

# COMMAND ----------

from pyspark.sql import functions as F


def seed_synthetic_zerobus_rows(target_table: str, id_prefix: str, start_index: int, row_count: int) -> None:
    if not spark.catalog.tableExists(target_table):
        raise RuntimeError(
            f"'{target_table}' does not exist yet -- run 02_seed_metaflow_testing_data.py first "
            "(it provisions the Excalibur_usecase schema and this table's baseline shape)."
        )

    end_index_inclusive = start_index + row_count - 1
    first_id = f"{id_prefix}-{start_index:06d}"
    last_id = f"{id_prefix}-{end_index_inclusive:06d}"

    try:
        existing_count = (
            spark.table(target_table)
            .where((F.col("customer_id") >= F.lit(first_id)) & (F.col("customer_id") <= F.lit(last_id)))
            .count()
        )
        if existing_count >= row_count:
            logger.info(
                "Skipping generation: '%s' already has %d/%d rows in range ['%s', '%s'] (idempotent no-op).",
                target_table, existing_count, row_count, first_id, last_id,
            )
            return

        synthetic_df = (
            spark.range(start_index, start_index + row_count)
            .withColumn("customer_id", F.format_string(f"{id_prefix}-%06d", F.col("id").cast("int")))
            .withColumn("customer_name", F.concat(F.lit("TC-ING-003 Synthetic Customer "), F.col("id").cast("string")))
            .withColumn("status", F.lit("ACTIVE"))
            .select("customer_id", "customer_name", "status")
        )

        # Explicit, source-column-scoped insert (not whenNotMatchedInsertAll()) -- this target
        # table can genuinely have MORE columns than this batch (e.g. scenario 003's own
        # reconciliation self-healing appends an extra `updated_at` column). Same
        # dynamic-column MERGE idiom as
        # 02_seed_metaflow_testing_data.py::seed_zerobus_style_table_from_csv.
        from delta.tables import DeltaTable

        target = DeltaTable.forName(spark, target_table)
        merge_columns = {col: f"s.{col}" for col in synthetic_df.columns}
        # INSERT-ONLY on purpose -- do NOT add whenMatchedUpdate() back.
        #
        # This table is the streaming SOURCE for spec 002's `zerobus_bronze` streaming
        # table. A Delta streaming source must be append-only: any commit carrying an
        # update/delete action makes the next read fail outright with
        # DELTA_SOURCE_TABLE_IGNORE_CHANGES, and because a streaming table cannot simply
        # skip past it, the pipeline stays broken until it is fully refreshed.
        #
        # whenMatchedUpdate() re-wrote every already-present row on each re-seed even when
        # the fixture values were byte-identical, so a *second* run of this idempotent
        # seeder was enough to poison the source permanently. Confirmed live on 2026-08-29:
        # SCN-002 failed with "Detected a data update ... in the source table at version 1"
        # naming this table's MERGE commit.
        #
        # Insert-only keeps the seeder just as idempotent -- re-running it inserts nothing
        # for rows that already match on the merge key -- while modelling what a Zerobus
        # event bus actually is: an append-only stream. Changing a fixture VALUE now
        # requires dropping the table rather than editing rows in place, which is the
        # correct trade for a source that must never emit a non-append commit.
        (
            target.alias("t")
            .merge(synthetic_df.alias("s"), "t.customer_id = s.customer_id")
            .whenNotMatchedInsert(values=merge_columns)
            .execute()
        )
        logger.info(
            "Merged %d synthetic rows ['%s'..'%s'] into '%s'.", row_count, first_id, last_id, target_table,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Failed to seed synthetic Zerobus rows ['{first_id}'..'{last_id}'] into '{target_table}': {exc}"
        ) from exc


seed_synthetic_zerobus_rows(TARGET_TABLE, ID_PREFIX, START_INDEX, ROW_COUNT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `{catalog}.Excalibur_usecase.zerobus_source_bus` now carries this call's synthetic row
# MAGIC range. `metaflow_test_002_zerobus_pipeline`'s next update streams any new rows into
# MAGIC `{catalog}.bronze_excalibur.zerobus_bronze` as usual.
