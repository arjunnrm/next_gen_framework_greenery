# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-DQ-004 -- `quarantine` Routing & Diagnostics Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/032_dq_004_quarantine.json` only -- intentionally
# MAGIC separate from `02_seed_metaflow_testing_data.py` (scenarios 001/002/003) and every other
# MAGIC `03_seed_*` notebook, so this test case's build/run stays isolated from those other
# MAGIC scenarios' own fixtures.
# MAGIC
# MAGIC Provisions the `test`/`bronze_test` schemas and the `landing_dq_quarantine`/`_schemas`
# MAGIC Volumes, then lands a single plain (non-ZIP) CSV directly in the landing Volume: 10 customer
# MAGIC rows, 8 valid, 1 with a null `Country` (`CUST006`), and 1 with a malformed `Email`
# MAGIC (`CUST007`) -- scaled down from `TESTING_PLAN.md`'s 85/10/5 template to a minimal 8/1/1
# MAGIC fixture, per this pass's "minimal, real, onboardable" convention.
# MAGIC
# MAGIC NOTE: this scenario's target (`{{catalog}}.bronze_test.customer_raw`) intentionally reuses
# MAGIC the same schema as the pre-existing `metaflow_test_100_zipcsv_pipeline` scenario, per
# MAGIC `TESTING_PLAN.md`'s own "Databricks Object Names" column for TC-DQ-004 -- see this test
# MAGIC case's own doc (`docs/53_tc_dq_004.md`) for the real table-name collision this creates
# MAGIC against scenario 100's own `customer_raw` table, flagged there rather than silently worked
# MAGIC around here.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/032_dq_004_quarantine.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_dq_004_quarantine_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "crm_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved crm_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC `bronze_test` may already exist (the pre-existing scenario 100 provisions the same schema) --
# MAGIC `CREATE ... IF NOT EXISTS` makes this idempotent either way, and this test case only ever
# MAGIC adds its own new `test` schema and `landing_dq_quarantine`/`_schemas` Volumes alongside it.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.test")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_test")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.test.landing_dq_quarantine")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.test._schemas")

logger.info("Provisioned catalog '%s' with test/bronze_test schemas + landing_dq_quarantine/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plain CSV Fixture
# MAGIC
# MAGIC No ZIP handling for this scenario -- the onboarding spec's `source_config.path` points
# MAGIC directly at this landing Volume, so the fixture is copied in as a plain CSV file.

# COMMAND ----------

DQ_QUARANTINE_LANDING = f"/Volumes/{CATALOG}/test/landing_dq_quarantine"
_fixture_csv = os.path.join(FIXTURE_DIR, "customer_dq_quarantine.csv")
if not os.path.exists(_fixture_csv):
    raise FileNotFoundError(f"Required DQ 'quarantine' fixture missing: '{_fixture_csv}'")

dbutils.fs.mkdirs(DQ_QUARANTINE_LANDING)
dbutils.fs.cp(f"file:{_fixture_csv}", f"{DQ_QUARANTINE_LANDING}/customer_dq_quarantine.csv")
logger.info(
    "Landed DQ 'quarantine' fixture at '%s/customer_dq_quarantine.csv' "
    "(10 rows: 8 valid, 1 null Country [CUST006], 1 malformed Email [CUST007]).",
    DQ_QUARANTINE_LANDING,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/032_dq_004_quarantine.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: the CSV fixture is copied fresh each time (Auto Loader
# MAGIC itself, not this notebook, tracks which files it has already ingested via its checkpoint).
