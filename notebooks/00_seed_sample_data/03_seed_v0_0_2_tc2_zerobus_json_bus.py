# Databricks notebook source
# MAGIC %md
# MAGIC # Seed v0.0.2 TC-2 -- Two Zerobus JSON-Payload Source Bus Tables
# MAGIC
# MAGIC Creates and populates the TWO Zerobus-style source tables that
# MAGIC `metaflow_testing/v0_0_2_tc2_zerobus_bronze.json` streams into Bronze:
# MAGIC
# MAGIC | Table | Payload shape |
# MAGIC |---|---|
# MAGIC | `{catalog}.zerobus_v0_0_2.orders_event_bus` | one STRING column `payload_json` holding an order event document |
# MAGIC | `{catalog}.zerobus_v0_0_2.devices_event_bus` | one STRING column `payload_json` holding a device-telemetry document |
# MAGIC
# MAGIC **Single JSON column on purpose.** Each table has exactly one column,
# MAGIC `payload_json STRING`. The test case asserts the raw JSON lands in Bronze
# MAGIC *intact* -- the spec deliberately sets NO `json_string_columns`, NO
# MAGIC `explode_columns` and NO `auto_flatten_all`, so the framework never parses the
# MAGIC document and the Bronze row carries the byte-identical string it was given.
# MAGIC
# MAGIC **Why no existing seed was reused.** `03_seed_ing_003_zerobus_baseline.py` and
# MAGIC `02_seed_metaflow_testing_data.py::seed_zerobus_style_table_from_csv` both seed
# MAGIC `Excalibur_usecase.zerobus_source_bus`, which is a *typed, multi-column*
# MAGIC (`customer_id`/`customer_name`/`status`) table -- the opposite of the single
# MAGIC opaque-JSON-column shape this test exists to prove, and a table whose row
# MAGIC contents scenarios 002/003/004 already assert against. Widening or reshaping it
# MAGIC would break those. A separate `zerobus_v0_0_2` schema keeps this test isolated.
# MAGIC
# MAGIC **INSERT-ONLY / append-only, deliberately.** Both tables are the streaming
# MAGIC SOURCE of a Delta streaming read. Any commit carrying an update or delete makes
# MAGIC the next read fail with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`, and a streaming
# MAGIC table cannot skip past it -- the pipeline stays broken until fully refreshed.
# MAGIC So this seeder NEVER updates a row: it computes which of its own event ids are
# MAGIC already present and appends only the missing ones. That makes it idempotent
# MAGIC (re-running appends nothing) without ever emitting a non-append commit.
# MAGIC Changing a fixture VALUE requires dropping the table, which is the correct
# MAGIC trade for a source that must model an append-only event bus.

# COMMAND ----------

import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_v0_0_2_tc2_zerobus_json_bus")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
dbutils.widgets.text("source_schema", "zerobus_v0_0_2", "Source schema holding both bus tables")
dbutils.widgets.text("row_count", "50", "Events to generate per bus table")

CATALOG = dbutils.widgets.get("catalog").strip()
SOURCE_SCHEMA = dbutils.widgets.get("source_schema").strip()
ROW_COUNT = int(dbutils.widgets.get("row_count").strip())

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if not SOURCE_SCHEMA:
    raise ValueError("The 'source_schema' widget must be a non-empty schema name.")
if ROW_COUNT <= 0:
    raise ValueError("The 'row_count' widget must be a positive integer.")

PAYLOAD_COLUMN = "payload_json"
ORDERS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.orders_event_bus"
DEVICES_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.devices_event_bus"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provision the source schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SOURCE_SCHEMA}")
logger.info("Ensured schema '%s.%s' exists.", CATALOG, SOURCE_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the two JSON payload fixtures
# MAGIC
# MAGIC Both documents are nested (an object plus an array) precisely so that "landed
# MAGIC intact" is a meaningful assertion: had the framework parsed or exploded them,
# MAGIC the Bronze row would be a flattened struct / multiple rows instead of one
# MAGIC string. `json.dumps(..., sort_keys=True)` makes the generated text stable, so a
# MAGIC re-run produces byte-identical payloads for the same event id.

# COMMAND ----------


def _order_payload(index: int) -> str:
    return json.dumps(
        {
            "event_id": f"TC2-ORD-{index:06d}",
            "event_type": "order.created",
            "emitted_at": f"2026-09-03T10:{index % 60:02d}:00Z",
            "order": {
                "order_id": f"ORD-{index:06d}",
                "customer_id": f"CUST-{(index % 17) + 1:04d}",
                "currency": "GBP",
                "total_amount": round(19.99 + index * 1.5, 2),
                "lines": [
                    {"sku": f"SKU-{index:04d}-A", "qty": (index % 3) + 1, "unit_price": 9.99},
                    {"sku": f"SKU-{index:04d}-B", "qty": (index % 5) + 1, "unit_price": 4.5},
                ],
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _device_payload(index: int) -> str:
    return json.dumps(
        {
            "event_id": f"TC2-DEV-{index:06d}",
            "event_type": "device.telemetry",
            "emitted_at": f"2026-09-03T11:{index % 60:02d}:00Z",
            "device": {
                "device_id": f"DEV-{index:06d}",
                "site": f"SITE-{(index % 7) + 1:02d}",
                "firmware": "4.2.1",
                "readings": [
                    {"metric": "temperature_c", "value": round(18.0 + (index % 12) * 0.75, 2)},
                    {"metric": "humidity_pct", "value": round(40.0 + (index % 20) * 1.1, 2)},
                ],
                "healthy": index % 11 != 0,
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Append-only, idempotent seed
# MAGIC
# MAGIC The already-present event ids are read back out of the payload with
# MAGIC `get_json_object` -- read-side only, so the stored column stays a plain STRING
# MAGIC and nothing about the table's single-column shape changes.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

_PAYLOAD_SCHEMA = StructType([StructField(PAYLOAD_COLUMN, StringType(), True)])
_EVENT_ID_JSON_PATH = "$.event_id"


def seed_json_event_bus(target_table: str, payload_builder, row_count: int) -> None:
    """Create ``target_table`` (single STRING payload column) if absent, then append
    only the events it does not already carry."""
    try:
        payloads = [(payload_builder(i),) for i in range(1, row_count + 1)]
        fixture_df = spark.createDataFrame(payloads, schema=_PAYLOAD_SCHEMA)

        if not spark.catalog.tableExists(target_table):
            spark.sql(
                f"CREATE TABLE {target_table} ("
                f"{PAYLOAD_COLUMN} STRING COMMENT "
                "'Raw Zerobus event document, one JSON object per row -- landed into Bronze unparsed'"
                ") USING DELTA COMMENT "
                "'v0.0.2 TC-2 Zerobus-style append-only event bus: exactly one JSON payload column'"
            )
            fixture_df.write.format("delta").mode("append").saveAsTable(target_table)
            logger.info("Created '%s' and appended %d JSON payload rows.", target_table, row_count)
            return

        existing_ids = [
            row["event_id"]
            for row in spark.table(target_table)
            .select(F.get_json_object(F.col(PAYLOAD_COLUMN), _EVENT_ID_JSON_PATH).alias("event_id"))
            .distinct()
            .collect()
            if row["event_id"] is not None
        ]
        if existing_ids:
            pending_df = fixture_df.where(
                ~F.get_json_object(F.col(PAYLOAD_COLUMN), _EVENT_ID_JSON_PATH).isin(existing_ids)
            )
        else:
            pending_df = fixture_df
        pending_count = pending_df.count()
        if pending_count == 0:
            logger.info(
                "Skipping '%s': all %d fixture events already present (idempotent no-op).",
                target_table,
                row_count,
            )
            return
        # APPEND ONLY -- never .mode("overwrite") and never a MERGE with whenMatchedUpdate():
        # either would emit a non-append Delta commit and permanently break the streaming read.
        pending_df.write.format("delta").mode("append").saveAsTable(target_table)
        logger.info("Appended %d new JSON payload rows into '%s'.", pending_count, target_table)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to seed JSON event bus '{target_table}': {exc}") from exc


seed_json_event_bus(ORDERS_TABLE, _order_payload, ROW_COUNT)
seed_json_event_bus(DEVICES_TABLE, _device_payload, ROW_COUNT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Both bus tables now exist with a single `payload_json STRING` column.
# MAGIC `metaflow_test_v0_0_2_tc2_zerobus_bronze_pipeline`'s next update streams them
# MAGIC into `{catalog}.bronze_v0_0_2_tc2.orders_events_bronze` and
# MAGIC `{catalog}.bronze_v0_0_2_tc2.devices_events_bronze` unparsed.

# COMMAND ----------

for _table in (ORDERS_TABLE, DEVICES_TABLE):
    logger.info("%s -> %d row(s), columns=%s", _table, spark.table(_table).count(), spark.table(_table).columns)
