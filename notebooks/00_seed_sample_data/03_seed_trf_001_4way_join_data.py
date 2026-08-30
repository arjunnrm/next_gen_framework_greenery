# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-TRF-001 -- Multi-Source Streaming-to-Batch 4-Way Inner Join Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/026_trf_001_4way_join.json` only --
# MAGIC intentionally separate from `02_seed_metaflow_testing_data.py` (scenario 001), even
# MAGIC though both reuse the exact same underlying CSV fixtures under
# MAGIC `sample_data/metaflow_testing/ea_usecase/`. Scenario 001 zips those same 4 files into
# MAGIC `EA_usecase.landing_zip` and ingests them into `bronze_ea.{departments,employees,
# MAGIC projects,assignments}_raw`; this test case instead lands them as **plain, unzipped**
# MAGIC CSVs (no `source_zip_handling` -- ZIP extraction isn't what TC-TRF-001 is testing) under
# MAGIC a new `ea` schema's `landing_*` volumes, ingested into this test case's own dedicated
# MAGIC `bronze_ea.trf001_*_raw` tables -- so this pipeline never collides with scenario 001's
# MAGIC already-deployed bronze tables of (nearly) the same name. See
# MAGIC docs/47_tc_trf_001.md for the full naming-collision rationale.
# MAGIC
# MAGIC Fixture reused as-is (no new sample data needed -- it already has exactly the shape this
# MAGIC test case needs):
# MAGIC * `employees.csv` -- 5 employees (E001-E005).
# MAGIC * `assignments.csv` -- 5 assignment rows, covering E001-E004 only -- **E005 deliberately
# MAGIC   has no assignment row**, so the 4-way inner join must exclude it.
# MAGIC * `departments.csv` / `projects.csv` -- reference dimensions, every `dept_id`/`project_id`
# MAGIC   referenced by `assignments.csv`/`employees.csv` resolves in both.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/026_trf_001_4way_join.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_trf_001_4way_join_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "ea_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved ea_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ea")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_ea")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_ea")

for _volume in ("landing_assignments", "landing_employees", "landing_departments", "landing_projects", "_schemas"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ea.{_volume}")

logger.info("Provisioned catalog '%s' with ea/bronze_ea/silver_ea schemas + landing_*/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the 4 EA Reference/Fact CSVs (Plain, Unzipped)

# COMMAND ----------

_LANDING_VOLUME_BY_FIXTURE = {
    "assignments.csv": "landing_assignments",
    "employees.csv": "landing_employees",
    "departments.csv": "landing_departments",
    "projects.csv": "landing_projects",
}

for _csv_filename, _volume_name in _LANDING_VOLUME_BY_FIXTURE.items():
    _fixture_path = os.path.join(FIXTURE_DIR, _csv_filename)
    if not os.path.exists(_fixture_path):
        raise FileNotFoundError(f"Required EA fixture missing: '{_fixture_path}'")
    _incoming_zone = f"/Volumes/{CATALOG}/ea/{_volume_name}/incoming"
    dbutils.fs.mkdirs(_incoming_zone)
    dbutils.fs.cp(f"file:{_fixture_path}", f"{_incoming_zone}/{_csv_filename}")
    logger.info("Landed EA fixture at '%s/%s'", _incoming_zone, _csv_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/026_trf_001_4way_join.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: each CSV is copied fresh each time (Auto Loader itself,
# MAGIC not this notebook, tracks which files it has already ingested via its checkpoint, so
# MAGIC re-landing the same filenames does not re-ingest or duplicate rows).
