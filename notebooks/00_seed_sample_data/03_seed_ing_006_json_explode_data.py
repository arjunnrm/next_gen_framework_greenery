# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-006 -- Nested JSON Struct Flattening Fixture
# MAGIC
# MAGIC Provisions the `iot`/`bronze_iot` schemas + `landing_json`/`_schemas` volumes and lands
# MAGIC `sample_data/metaflow_testing/iot_usecase/device_metrics_batch1.json` -- 2 device JSON
# MAGIC records, each carrying a nested `metrics` array of 2 `{sensor, val}` elements -- into
# MAGIC `/Volumes/{{catalog}}/iot/landing_json/` for Auto Loader to pick up.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/015_ing_006_json_explode.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_ing_006_json_explode_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "iot_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.iot")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_iot")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.iot.landing_json")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.iot._schemas")

logger.info("Provisioned catalog '%s' with iot/bronze_iot schemas + landing_json/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Nested JSON Device Metrics Batch

# COMMAND ----------

IOT_LANDING_JSON = f"/Volumes/{CATALOG}/iot/landing_json"
_fixture_file = os.path.join(FIXTURE_DIR, "device_metrics_batch1.json")
if not os.path.exists(_fixture_file):
    raise FileNotFoundError(f"Required nested-JSON fixture missing: '{_fixture_file}'")

dbutils.fs.mkdirs(IOT_LANDING_JSON)
dbutils.fs.cp(f"file:{_fixture_file}", f"{IOT_LANDING_JSON}/device_metrics_batch1.json")
logger.info("Landed nested JSON device metrics fixture at '%s/device_metrics_batch1.json'", IOT_LANDING_JSON)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/015_ing_006_json_explode.json` can now be onboarded and its pipeline
# MAGIC run. Re-running this notebook is safe: the fixture file is copied fresh each time (Auto
# MAGIC Loader itself, not this notebook, tracks which files it has already ingested via its
# MAGIC checkpoint/schema location).
