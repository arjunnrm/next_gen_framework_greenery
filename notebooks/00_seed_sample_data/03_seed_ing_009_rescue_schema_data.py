# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-009 -- Schema Evolution & Malformed Column Rescue Fixture
# MAGIC
# MAGIC Provisions the `ops`/`bronze_ops` schemas + `landing_drift`/`_schemas` volumes and lands
# MAGIC **only** `sample_data/flowx_testing/ops_usecase/feed_day1.csv` (columns `id`, `name`)
# MAGIC into `/Volumes/{{catalog}}/ops/landing_drift/` for Auto Loader to pick up.
# MAGIC
# MAGIC The day-2 fixture (`feed_day2.csv` -- adds an unannounced `unannounced_flag` column plus
# MAGIC one row with a non-numeric `id`) is **deliberately not landed here** -- see
# MAGIC `docs/39_tc_ing_009.md`'s "Triggering day-2 schema drift" section for how to drop it in
# MAGIC manually and re-trigger the pipeline once day-1 has been onboarded and run once.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/018_ing_009_rescue_schema.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_ing_009_rescue_schema_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "ops_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_ops")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ops.landing_drift")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ops._schemas")

logger.info("Provisioned catalog '%s' with ops/bronze_ops schemas + landing_drift/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Day-1 Batch Only ([id, name])

# COMMAND ----------

OPS_LANDING_DRIFT = f"/Volumes/{CATALOG}/ops/landing_drift"
_day1_fixture = os.path.join(FIXTURE_DIR, "feed_day1.csv")
if not os.path.exists(_day1_fixture):
    raise FileNotFoundError(f"Required day-1 fixture missing: '{_day1_fixture}'")

dbutils.fs.mkdirs(OPS_LANDING_DRIFT)
dbutils.fs.cp(f"file:{_day1_fixture}", f"{OPS_LANDING_DRIFT}/feed_day1.csv")
logger.info("Landed day-1 drift feed fixture at '%s/feed_day1.csv' (columns: id, name).", OPS_LANDING_DRIFT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/018_ing_009_rescue_schema.json` can now be onboarded and its pipeline
# MAGIC run against day-1 data alone. Re-running this notebook is safe: `feed_day1.csv` is
# MAGIC re-copied fresh each time (Auto Loader itself, not this notebook, tracks which files it
# MAGIC has already ingested via its checkpoint/schema location). See `docs/39_tc_ing_009.md` for
# MAGIC how to land `feed_day2.csv` afterwards and re-trigger the pipeline to exercise schema
# MAGIC rescue.
