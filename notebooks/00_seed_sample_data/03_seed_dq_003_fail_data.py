# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-DQ-003 -- `fail` Pipeline Execution Abort Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/031_dq_003_fail.json` only -- kept isolated
# MAGIC from every other scenario's own fixtures, following the same per-test-case seed-notebook
# MAGIC convention as `03_seed_dq_001_warn_data.py`.
# MAGIC
# MAGIC Provisions the `banking`/`bronze_banking` schemas and the `landing_dq_fail`/`_schemas`
# MAGIC Volumes, then lands a single plain (non-ZIP) CSV directly in the landing Volume: 5 banking
# MAGIC account rows, 1 of which (`Ama Boateng`) deliberately carries a **null `account_id`** --
# MAGIC this is the row the onboarding spec's `dq_config` rule (`account_id IS NOT NULL`,
# MAGIC `action: "fail"`) is designed to catch, aborting the pipeline update.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/031_dq_003_fail.json`.
# MAGIC
# MAGIC **This scenario is deliberately expected to make its own pipeline run FAIL** -- see
# MAGIC `docs/52_tc_dq_003.md`'s Expected Results section. That is the correct, intended outcome of
# MAGIC this test case, not a bug in this seed step or the onboarded spec.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_dq_003_fail_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "banking_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved banking_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.banking")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_banking")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.banking.landing_dq_fail")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.banking._schemas")

logger.info("Provisioned catalog '%s' with banking/bronze_banking schemas + landing_dq_fail/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plain CSV Fixture
# MAGIC
# MAGIC No ZIP handling for this scenario -- the onboarding spec's `source_config.path` points
# MAGIC directly at this landing Volume, so the fixture is copied in as a plain CSV file.

# COMMAND ----------

DQ_FAIL_LANDING = f"/Volumes/{CATALOG}/banking/landing_dq_fail"
_fixture_csv = os.path.join(FIXTURE_DIR, "accounts_dq_fail.csv")
if not os.path.exists(_fixture_csv):
    raise FileNotFoundError(f"Required DQ 'fail' fixture missing: '{_fixture_csv}'")

dbutils.fs.mkdirs(DQ_FAIL_LANDING)
dbutils.fs.cp(f"file:{_fixture_csv}", f"{DQ_FAIL_LANDING}/accounts_dq_fail.csv")
logger.info("Landed DQ 'fail' fixture at '%s/accounts_dq_fail.csv' (5 rows, 1 with a null account_id).", DQ_FAIL_LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/031_dq_003_fail.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: the CSV fixture is copied fresh each time. Note that the
# MAGIC downstream pipeline run (`run_pipeline` task) is expected to FAIL every time this fixture
# MAGIC is used, by design -- see `docs/52_tc_dq_003.md`.
