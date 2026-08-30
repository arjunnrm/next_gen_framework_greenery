# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-007 -- Inline Data Standardization SQL Fixture
# MAGIC
# MAGIC Provisions the `master`/`bronze_master` schemas + `landing_companies`/`_schemas` volumes
# MAGIC and lands `sample_data/metaflow_testing/master_usecase/companies_batch1.csv` -- 3 dirty
# MAGIC company records (extra leading/trailing whitespace, mixed-case region and email values)
# MAGIC -- into `/Volumes/{{catalog}}/master/landing_companies/` for Auto Loader to pick up.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/016_ing_007_standardize.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_ing_007_standardize_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "master_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.master")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_master")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.master.landing_companies")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.master._schemas")

logger.info("Provisioned catalog '%s' with master/bronze_master schemas + landing_companies/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Dirty Companies Batch

# COMMAND ----------

MASTER_LANDING_COMPANIES = f"/Volumes/{CATALOG}/master/landing_companies"
_fixture_file = os.path.join(FIXTURE_DIR, "companies_batch1.csv")
if not os.path.exists(_fixture_file):
    raise FileNotFoundError(f"Required dirty-companies fixture missing: '{_fixture_file}'")

dbutils.fs.mkdirs(MASTER_LANDING_COMPANIES)
dbutils.fs.cp(f"file:{_fixture_file}", f"{MASTER_LANDING_COMPANIES}/companies_batch1.csv")
logger.info("Landed dirty companies fixture at '%s/companies_batch1.csv'", MASTER_LANDING_COMPANIES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/016_ing_007_standardize.json` can now be onboarded and its pipeline
# MAGIC run. Re-running this notebook is safe: the fixture file is copied fresh each time (Auto
# MAGIC Loader itself, not this notebook, tracks which files it has already ingested via its
# MAGIC checkpoint/schema location).
