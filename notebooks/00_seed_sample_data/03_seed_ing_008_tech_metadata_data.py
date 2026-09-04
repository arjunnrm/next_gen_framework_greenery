# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-008 -- Technical Metadata Enrichment & Column Ordering Fixture
# MAGIC
# MAGIC Provisions a dedicated `ea`/`bronze_ea` landing volume (`landing_csv`, distinct from
# MAGIC scenario 001's own `EA_usecase`/`landing_zip` volume -- this scenario lands a **plain**
# MAGIC CSV file directly, no ZIP involved) and copies the already-seeded
# MAGIC `sample_data/flowx_testing/ea_usecase/departments.csv` fixture (dept_id, dept_name,
# MAGIC region -- reused as-is from scenario 001 rather than inventing a new 2-column file, since
# MAGIC it already carries the `dept_id`/`dept_name` business columns this scenario cares about)
# MAGIC into `/Volumes/{{catalog}}/ea/landing_csv/` for Auto Loader to pick up.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/017_ing_008_tech_metadata.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_ing_008_tech_metadata_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "ea_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ea")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_ea")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ea.landing_csv")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ea._schemas")

logger.info("Provisioned catalog '%s' with ea/bronze_ea schemas + landing_csv/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plain CSV Departments Fixture

# COMMAND ----------

EA_LANDING_CSV = f"/Volumes/{CATALOG}/ea/landing_csv"
_fixture_file = os.path.join(FIXTURE_DIR, "departments.csv")
if not os.path.exists(_fixture_file):
    raise FileNotFoundError(f"Required departments CSV fixture missing: '{_fixture_file}'")

dbutils.fs.mkdirs(EA_LANDING_CSV)
dbutils.fs.cp(f"file:{_fixture_file}", f"{EA_LANDING_CSV}/departments.csv")
logger.info("Landed plain CSV departments fixture at '%s/departments.csv'", EA_LANDING_CSV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/017_ing_008_tech_metadata.json` can now be onboarded and its pipeline
# MAGIC run. Re-running this notebook is safe: the fixture file is copied fresh each time (Auto
# MAGIC Loader itself, not this notebook, tracks which files it has already ingested via its
# MAGIC checkpoint/schema location).
