# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-SNK-002 -- Pure Sink Export Without Table Materialization Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/040_snk_002_pure_sink.json` only --
# MAGIC intentionally separate from every other scenario's own seed notebook, so this test case's
# MAGIC build/run stays isolated.
# MAGIC
# MAGIC Provisions:
# MAGIC * `iot_snk002` -- landing schema + `landing_sensor`/`_schemas` Volumes for the Auto Loader
# MAGIC   source side of the ingestion flow.
# MAGIC * `bronze_iot_snk002` -- schema hosting the genuine bronze streaming table
# MAGIC   (`sensor_readings_raw`) the ingestion flow materializes -- this table IS queryable, it's
# MAGIC   the downstream transformation_flow (`target_type: "sink"`) that never materializes one.
# MAGIC * `egress_iot` schema + `partner_drops` Volume -- the actual egress destination the sink
# MAGIC   writes Delta files into directly (`target_config.sink_config.path`). Unlike a normal
# MAGIC   ingestion/transformation target schema, this Volume is never auto-provisioned by the
# MAGIC   pipeline itself (`dlt.create_sink` writes files into an existing Volume path, it does not
# MAGIC   create one) -- so it must be created here, same as scenario 001/002/003's own
# MAGIC   `egress_ea`/`export_zips` precedent (`02_seed_metaflow_testing_data.py`).
# MAGIC
# MAGIC Then lands a small, 5-row IoT sensor readings fixture
# MAGIC (`sensor_readings_snk002.csv`) in the incoming Volume. See `docs/62_tc_snk_002.md` for the
# MAGIC full scenario writeup and verification queries.
# MAGIC
# MAGIC Built from `sample_data/metaflow_testing/iot_usecase/sensor_readings_snk002.csv`.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/040_snk_002_pure_sink.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_snk_002_pure_sink_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "iot_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved iot_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

for _schema in ("iot_snk002", "bronze_iot_snk002", "egress_iot"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.iot_snk002.landing_sensor")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.iot_snk002._schemas")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.egress_iot.partner_drops")

logger.info(
    "Provisioned catalog '%s' with iot_snk002/bronze_iot_snk002/egress_iot schemas + "
    "landing_sensor/_schemas/partner_drops volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the IoT Sensor Readings Fixture

# COMMAND ----------

SENSOR_INCOMING_ZONE = f"/Volumes/{CATALOG}/iot_snk002/landing_sensor/incoming"
dbutils.fs.mkdirs(SENSOR_INCOMING_ZONE)

_sensor_fixture = os.path.join(FIXTURE_DIR, "sensor_readings_snk002.csv")
if not os.path.exists(_sensor_fixture):
    raise FileNotFoundError(f"Required sensor readings fixture missing: '{_sensor_fixture}'")

dbutils.fs.cp(f"file:{_sensor_fixture}", f"{SENSOR_INCOMING_ZONE}/sensor_readings_snk002.csv")
logger.info("Landed IoT sensor readings fixture at '%s/sensor_readings_snk002.csv' (5 rows).", SENSOR_INCOMING_ZONE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/040_snk_002_pure_sink.json` can now be onboarded and its pipeline run --
# MAGIC `{{catalog}}.bronze_iot_snk002.sensor_readings_raw` should end up with 5 rows, and
# MAGIC `/Volumes/{{catalog}}/egress_iot/partner_drops/` should receive exported Delta files with NO
# MAGIC corresponding table ever registered in `information_schema.tables`. See
# MAGIC `docs/62_tc_snk_002.md` for the full verification queries.
# MAGIC
# MAGIC Re-running this notebook is safe and idempotent: it only ever re-copies the same fixture
# MAGIC file (Auto Loader tracks already-ingested files via its own checkpoint, so re-landing the
# MAGIC same filename does not re-ingest or duplicate rows).
