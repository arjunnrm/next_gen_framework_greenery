# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-CDC-004 -- SCD Type 2 Full History & Active Companion View Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/022_cdc_004_scd2.json` only --
# MAGIC intentionally separate from `02_seed_metaflow_testing_data.py` and every other test
# MAGIC case's own seed notebook so this test case's build/run stays isolated.
# MAGIC
# MAGIC Provisions the `hr`/`silver_hr` schemas and the `landing_emp`/`_schemas` Volumes, then
# MAGIC lands 2 employee-dimension CSV "day" extracts directly in the incoming Volume (no ZIP
# MAGIC handling needed -- plain Auto Loader CSV ingestion is the whole point of this scenario),
# MAGIC built from the fixtures under `sample_data/metaflow_testing/hr_usecase/`:
# MAGIC
# MAGIC * `dim_employee_day1.csv` -- 3 employees, including `E101` in dept `D1`.
# MAGIC * `dim_employee_day2.csv` -- same 3 employees re-sent (as a daily dimension extract would)
# MAGIC   plus one new hire (`E104`). `E101` has moved from dept `D1` to `D2`; `E102`/`E103` carry
# MAGIC   an advanced `updated_at` but an **unchanged** `dept_id` -- since the onboarding spec's
# MAGIC   `columns_to_check` scopes SCD2 history-tracking to `dept_id` only, this proves a bump to
# MAGIC   an untracked/sequence column alone does not open a spurious new history version.
# MAGIC
# MAGIC Landing both files before the pipeline's first (and only) triggered update is safe --
# MAGIC `dlt.apply_changes` orders strictly by `sequence_by_column` (`updated_at`), not by file
# MAGIC arrival order, so both "days" land in one micro-batch and still resolve to the correct
# MAGIC history (`E101`: `D1` closed, `D2` current).
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/022_cdc_004_scd2.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_cdc_004_scd2_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "hr_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved hr_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.hr")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_hr")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.hr.landing_emp")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.hr._schemas")

logger.info("Provisioned catalog '%s' with hr/silver_hr schemas + landing_emp/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land Both Employee Dimension "Day" Extracts

# COMMAND ----------

HR_INCOMING_ZONE = f"/Volumes/{CATALOG}/hr/landing_emp/incoming"
dbutils.fs.mkdirs(HR_INCOMING_ZONE)

for _batch_filename in ("dim_employee_day1.csv", "dim_employee_day2.csv"):
    _fixture_path = os.path.join(FIXTURE_DIR, _batch_filename)
    if not os.path.exists(_fixture_path):
        raise FileNotFoundError(f"Required employee dimension fixture missing: '{_fixture_path}'")
    dbutils.fs.cp(f"file:{_fixture_path}", f"{HR_INCOMING_ZONE}/{_batch_filename}")
    logger.info("Landed employee dimension fixture at '%s/%s'", HR_INCOMING_ZONE, _batch_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/022_cdc_004_scd2.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: both CSV "day" extracts are copied fresh each time
# MAGIC (Auto Loader itself, not this notebook, tracks which files it has already ingested via
# MAGIC its checkpoint, so re-landing the same filenames does not re-ingest or re-open history
# MAGIC versions beyond what this fixture already intentionally contains).
