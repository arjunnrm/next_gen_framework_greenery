# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-CDC-001 -- Streaming Append-Only Clickstream Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/019_cdc_001_append.json` only --
# MAGIC intentionally separate from `02_seed_metaflow_testing_data.py` (scenarios 001/002/003) so
# MAGIC this test case's build/run stays isolated from those other scenarios' own fixtures.
# MAGIC
# MAGIC Provisions the `web`/`bronze_web` schemas and the `landing_clicks`/`_schemas` Volumes,
# MAGIC then lands 2 clickstream CSV batches directly in the incoming Volume (no ZIP handling
# MAGIC needed here -- plain Auto Loader CSV ingestion is the whole point of this scenario), built
# MAGIC from the fixtures under `sample_data/metaflow_testing/web_usecase/`:
# MAGIC
# MAGIC * `page_clicks_batch1.csv` -- 8 rows, including one intentional duplicate `(user_id,
# MAGIC   event_timestamp)` pair (`CLK00005`/`CLK00006`, both `U1002` @ `2026-08-28T10:16:40Z`).
# MAGIC * `page_clicks_batch2.csv` -- 6 rows, including a second intentional duplicate pair within
# MAGIC   the batch (`CLK00009`/`CLK00010`, both `U1005` @ `2026-08-28T10:20:00Z`) **and** a third
# MAGIC   row (`CLK00012`) that repeats batch 1's `(U1002, 2026-08-28T10:16:40Z)` key again, in a
# MAGIC   separate file/micro-batch -- proving `APPEND` never deduplicates across files either,
# MAGIC   not just within one.
# MAGIC
# MAGIC Landing both files before the pipeline's first (and only) triggered update means Auto
# MAGIC Loader discovers and appends both as part of the same run, exercising "multiple
# MAGIC micro-batches" in one job execution.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/019_cdc_001_append.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_cdc_001_append_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "web_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved web_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.web")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_web")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.web.landing_clicks")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.web._schemas")

logger.info("Provisioned catalog '%s' with web/bronze_web schemas + landing_clicks/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land Both Clickstream CSV Batches

# COMMAND ----------

WEB_INCOMING_ZONE = f"/Volumes/{CATALOG}/web/landing_clicks/incoming"
dbutils.fs.mkdirs(WEB_INCOMING_ZONE)

for _batch_filename in ("page_clicks_batch1.csv", "page_clicks_batch2.csv"):
    _fixture_path = os.path.join(FIXTURE_DIR, _batch_filename)
    if not os.path.exists(_fixture_path):
        raise FileNotFoundError(f"Required clickstream fixture missing: '{_fixture_path}'")
    dbutils.fs.cp(f"file:{_fixture_path}", f"{WEB_INCOMING_ZONE}/{_batch_filename}")
    logger.info("Landed clickstream batch fixture at '%s/%s'", WEB_INCOMING_ZONE, _batch_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/019_cdc_001_append.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: both CSV batches are copied fresh each time (Auto
# MAGIC Loader itself, not this notebook, tracks which files it has already ingested via its
# MAGIC checkpoint, so re-landing the same filenames does not re-ingest or duplicate rows beyond
# MAGIC what this fixture already intentionally contains).
