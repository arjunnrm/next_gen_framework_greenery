# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-DQ-001 -- `warn` Non-Blocking Validation Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/029_dq_001_warn.json` only -- intentionally
# MAGIC separate from `02_seed_flowx_testing_data.py` (scenarios 001/002/003) and
# MAGIC `03_seed_ing_001_zip_filter_data.py` (TC-ING-001), so this test case's build/run stays
# MAGIC isolated from those other scenarios' own fixtures.
# MAGIC
# MAGIC Provisions the `crm`/`bronze_crm` schemas and the `landing_dq_warn`/`_schemas` Volumes,
# MAGIC then lands a single plain (non-ZIP) CSV directly in the landing Volume: 10 customer rows,
# MAGIC 2 of which (`C003` age 16, `C006` age 15) violate the onboarding spec's
# MAGIC `dq_config` rule (`age >= 18`, `action: "warn"`) -- scaled down from the TESTING_PLAN.md
# MAGIC template's 100/15 to a minimal 10/2 fixture, per this pass's "minimal, real, onboardable"
# MAGIC convention.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/029_dq_001_warn.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_dq_001_warn_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "crm_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved crm_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC `crm`/`bronze_crm` and the `crm._schemas` Volume may already exist (TC-ING-001 provisions
# MAGIC the same catalog objects) -- `CREATE ... IF NOT EXISTS` makes this idempotent either way,
# MAGIC and this test case only ever adds its own new `landing_dq_warn` Volume and
# MAGIC `_schemas/customer_warn/` subfolder alongside them.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.crm")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_crm")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm.landing_dq_warn")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm._schemas")

logger.info("Provisioned catalog '%s' with crm/bronze_crm schemas + landing_dq_warn/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plain CSV Fixture
# MAGIC
# MAGIC No ZIP handling for this scenario -- the onboarding spec's `source_config.path` points
# MAGIC directly at this landing Volume, so the fixture is copied in as a plain CSV file.

# COMMAND ----------

DQ_WARN_LANDING = f"/Volumes/{CATALOG}/crm/landing_dq_warn"
_fixture_csv = os.path.join(FIXTURE_DIR, "customer_dq_warn.csv")
if not os.path.exists(_fixture_csv):
    raise FileNotFoundError(f"Required DQ 'warn' fixture missing: '{_fixture_csv}'")

dbutils.fs.mkdirs(DQ_WARN_LANDING)
dbutils.fs.cp(f"file:{_fixture_csv}", f"{DQ_WARN_LANDING}/customer_dq_warn.csv")
logger.info("Landed DQ 'warn' fixture at '%s/customer_dq_warn.csv' (10 rows, 2 with age < 18).", DQ_WARN_LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/029_dq_001_warn.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: the CSV fixture is copied fresh each time (Auto Loader
# MAGIC itself, not this notebook, tracks which files it has already ingested via its checkpoint).
