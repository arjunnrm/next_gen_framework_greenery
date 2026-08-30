# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-DQ-002 -- `drop` Silent Row Filtering Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/030_dq_002_drop.json` only -- intentionally
# MAGIC separate from `02_seed_metaflow_testing_data.py` and every other test case's own seed
# MAGIC notebook (e.g. `03_seed_dq_001_warn_data.py`), so this test case's build/run stays isolated
# MAGIC from those other scenarios' own fixtures.
# MAGIC
# MAGIC Provisions the `txns`/`bronze_txns` schemas and the `landing_dq_drop`/`_schemas` Volumes,
# MAGIC then lands a single plain (non-ZIP) CSV directly in the landing Volume: 10 transaction
# MAGIC rows, 2 of which (`TXN-2003` amount -25.00, `TXN-2005` amount 0.00) violate the onboarding
# MAGIC spec's `dq_config` rule (`amount > 0`, `action: "drop"`) -- scaled down from the
# MAGIC TESTING_PLAN.md template's 100/10 to a minimal 10/2 fixture, per this pass's "minimal,
# MAGIC real, onboardable" convention.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/030_dq_002_drop.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_dq_002_drop_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "txns_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved txns_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC `CREATE ... IF NOT EXISTS` makes this idempotent -- safe to re-run across environments and
# MAGIC across repeated builds of this same test case.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.txns")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_txns")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.txns.landing_dq_drop")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.txns._schemas")

logger.info("Provisioned catalog '%s' with txns/bronze_txns schemas + landing_dq_drop/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plain CSV Fixture
# MAGIC
# MAGIC No ZIP handling for this scenario -- the onboarding spec's `source_config.path` points
# MAGIC directly at this landing Volume, so the fixture is copied in as a plain CSV file.

# COMMAND ----------

DQ_DROP_LANDING = f"/Volumes/{CATALOG}/txns/landing_dq_drop"
_fixture_csv = os.path.join(FIXTURE_DIR, "txns_dq_drop.csv")
if not os.path.exists(_fixture_csv):
    raise FileNotFoundError(f"Required DQ 'drop' fixture missing: '{_fixture_csv}'")

dbutils.fs.mkdirs(DQ_DROP_LANDING)
dbutils.fs.cp(f"file:{_fixture_csv}", f"{DQ_DROP_LANDING}/txns_dq_drop.csv")
logger.info("Landed DQ 'drop' fixture at '%s/txns_dq_drop.csv' (10 rows, 2 with amount <= 0).", DQ_DROP_LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/030_dq_002_drop.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: the CSV fixture is copied fresh each time (Auto Loader
# MAGIC itself, not this notebook, tracks which files it has already ingested via its checkpoint).
