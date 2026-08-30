# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-FLT-002 -- Day-1 / Day-2 Customer Lifecycle Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/044_flt_002_lifecycle.json` only --
# MAGIC intentionally separate from `02_seed_metaflow_testing_data.py` and every other test
# MAGIC case's own seed notebook so this test case's build/run stays isolated.
# MAGIC
# MAGIC Provisions the `crm`/`silver_crm` schemas and the `landing_lifecycle`/`_schemas` Volumes,
# MAGIC then lands **one** CSV "day" extract per run -- controlled by the `stage` widget -- into
# MAGIC `/Volumes/{{catalog}}/crm/landing_lifecycle/incoming/`, from the fixtures under
# MAGIC `sample_data/metaflow_testing/crm_usecase/`:
# MAGIC
# MAGIC * `lifecycle_day1.csv` (`stage=day1`) -- 10 customers, the initial seed load.
# MAGIC * `lifecycle_day2.csv` (`stage=day2`) -- 3 of those same `customer_id`s re-sent with
# MAGIC   changed `city`/`tier`/`updated_at` (an update, under SCD1), plus 2 brand-new
# MAGIC   `customer_id`s (a plain insert).
# MAGIC
# MAGIC Unlike a same-run "land both files, run once" fixture, this notebook deliberately lands
# MAGIC **only one file per invocation** -- the whole point of TC-FLT-002 is proving the pipeline
# MAGIC behaves correctly across two genuinely separate triggered updates (a real Day-1 run, then a
# MAGIC real Day-2 run later), not just that `apply_changes` sorts correctly within one batch. The
# MAGIC owning job (`metaflow_test_flt_002_lifecycle_job`) calls this notebook twice, with
# MAGIC `stage=day1` then `stage=day2`, with a `run_pipeline` task in between and after.
# MAGIC
# MAGIC Run before onboarding `metaflow_testing/044_flt_002_lifecycle.json` (stage=day1), then
# MAGIC again before the second pipeline update (stage=day2).

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_flt_002_lifecycle_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

dbutils.widgets.dropdown("stage", "day1", ["day1", "day2"], "Fixture stage to land")
STAGE = dbutils.widgets.get("stage").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "crm_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved crm_usecase fixture directory: %s (stage=%s)", FIXTURE_DIR, STAGE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.crm")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_crm")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm.landing_lifecycle")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm._schemas")

logger.info("Provisioned catalog '%s' with crm/silver_crm schemas + landing_lifecycle/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land This Stage's Customer Lifecycle Extract

# COMMAND ----------

CRM_INCOMING_ZONE = f"/Volumes/{CATALOG}/crm/landing_lifecycle/incoming"
dbutils.fs.mkdirs(CRM_INCOMING_ZONE)

_batch_filename = f"lifecycle_{STAGE}.csv"
_fixture_path = os.path.join(FIXTURE_DIR, _batch_filename)
if not os.path.exists(_fixture_path):
    raise FileNotFoundError(f"Required customer lifecycle fixture missing: '{_fixture_path}'")

dbutils.fs.cp(f"file:{_fixture_path}", f"{CRM_INCOMING_ZONE}/{_batch_filename}")
logger.info("Landed customer lifecycle fixture at '%s/%s'", CRM_INCOMING_ZONE, _batch_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC After `stage=day1` runs, onboard and run `metaflow_test_flt_002_lifecycle_pipeline` once
# MAGIC to ingest the 10-row seed load. Then run this notebook again with `stage=day2` and run the
# MAGIC pipeline a second time to ingest the incremental update/insert batch. Re-running a given
# MAGIC stage is safe: the file is copied fresh each time (Auto Loader itself, not this notebook,
# MAGIC tracks which files it has already ingested via its checkpoint).
