# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-CDC-002 -- Full Materialized View Snapshot (TRUNCATE_AND_LOAD) Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/020_cdc_002_truncate.json` only --
# MAGIC intentionally separate from `02_seed_flowx_testing_data.py` (scenarios 001/002/003) and
# MAGIC every other scenario's own seed notebook, so this test case's build/run stays isolated.
# MAGIC
# MAGIC Provisions the `ref`/`bronze_ref`/`silver_ref` schemas and the `landing_fx`/`_schemas`
# MAGIC Volumes, then lands one 50-row FX currency rate extract directly in the incoming Volume
# MAGIC (no ZIP handling needed here -- plain Auto Loader CSV ingestion is the whole point of this
# MAGIC scenario), built from the fixture under `sample_data/flowx_testing/ref_usecase/`:
# MAGIC
# MAGIC * `fx_rates_batch1.csv` -- 50 rows, one per distinct ISO currency code, all dated the same
# MAGIC   `rate_date` (2026-08-28) -- a single day's full-replacement extract.
# MAGIC
# MAGIC Landing this file before the pipeline's first triggered update means Auto Loader ingests
# MAGIC all 50 rows into `{{catalog}}.bronze_ref.fx_rates_raw` in one micro-batch; the
# MAGIC `dim_fx_rates_current` materialized view (`cdc_load_strategy: "TRUNCATE_AND_LOAD"`) then
# MAGIC fully recomputes from that table on every pipeline update -- proving the target holds
# MAGIC exactly 50 rows no matter how many times the pipeline is triggered, since a materialized
# MAGIC view is a full recompute, never an incremental append.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/020_cdc_002_truncate.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_cdc_002_truncate_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "ref_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved ref_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ref")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_ref")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_ref")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ref.landing_fx")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ref._schemas")

logger.info(
    "Provisioned catalog '%s' with ref/bronze_ref/silver_ref schemas + landing_fx/_schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the FX Rates CSV Extract

# COMMAND ----------

FX_INCOMING_ZONE = f"/Volumes/{CATALOG}/ref/landing_fx/incoming"
dbutils.fs.mkdirs(FX_INCOMING_ZONE)

_fixture_filename = "fx_rates_batch1.csv"
_fixture_path = os.path.join(FIXTURE_DIR, _fixture_filename)
if not os.path.exists(_fixture_path):
    raise FileNotFoundError(f"Required FX rates fixture missing: '{_fixture_path}'")

dbutils.fs.cp(f"file:{_fixture_path}", f"{FX_INCOMING_ZONE}/{_fixture_filename}")
logger.info("Landed FX rates fixture at '%s/%s'", FX_INCOMING_ZONE, _fixture_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/020_cdc_002_truncate.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: the CSV is copied fresh each time (Auto Loader itself,
# MAGIC not this notebook, tracks which files it has already ingested via its checkpoint, so
# MAGIC re-landing the same filename does not re-ingest or duplicate rows in the Bronze table --
# MAGIC and the Silver materialized view fully recomputes from Bronze on every pipeline update
# MAGIC regardless, so it never accumulates duplicates either way).
