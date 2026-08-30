# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-TRF-002 -- Multi-Regional `UNION ALL` Consolidation Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/027_trf_002_union_all.json` only --
# MAGIC intentionally separate from `02_seed_metaflow_testing_data.py` (scenarios 001/002/003)
# MAGIC and from any other test case's own seed notebook, so this test case's build/run stays
# MAGIC isolated.
# MAGIC
# MAGIC Provisions the `sales`/`bronze_sales`/`silver_sales` schemas and two independent
# MAGIC per-region landing Volumes (`landing_orders_na`, `landing_orders_eu` -- kept separate,
# MAGIC not one shared incoming folder, so each Auto Loader ingestion flow only ever discovers
# MAGIC its own region's file), then lands one CSV batch per region, built from the fixtures
# MAGIC under `sample_data/metaflow_testing/sales_usecase/`:
# MAGIC
# MAGIC * `orders_na_raw.csv` -- 5 rows, `region = 'NA'`.
# MAGIC * `orders_eu_raw.csv` -- 4 rows, `region = 'EU'` -- deliberately fewer rows than the NA
# MAGIC   side (scaled down from the TESTING_PLAN's own 500/400 -- the ratio, not the absolute
# MAGIC   count, is what this scenario proves) so a passing row-count assertion can't be
# MAGIC   explained by both sides coincidentally being read as the same table twice.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/027_trf_002_union_all.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_trf_002_union_all_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "sales_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved sales_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.sales")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_sales")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_sales")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sales.landing_orders_na")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sales.landing_orders_eu")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sales._schemas")

logger.info(
    "Provisioned catalog '%s' with sales/bronze_sales/silver_sales schemas + "
    "landing_orders_na/landing_orders_eu/_schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land Each Region's Order Batch Into Its Own Volume

# COMMAND ----------

NA_INCOMING_ZONE = f"/Volumes/{CATALOG}/sales/landing_orders_na/incoming"
EU_INCOMING_ZONE = f"/Volumes/{CATALOG}/sales/landing_orders_eu/incoming"

for _incoming_zone, _fixture_filename in (
    (NA_INCOMING_ZONE, "orders_na_raw.csv"),
    (EU_INCOMING_ZONE, "orders_eu_raw.csv"),
):
    _fixture_path = os.path.join(FIXTURE_DIR, _fixture_filename)
    if not os.path.exists(_fixture_path):
        raise FileNotFoundError(f"Required regional orders fixture missing: '{_fixture_path}'")
    dbutils.fs.mkdirs(_incoming_zone)
    dbutils.fs.cp(f"file:{_fixture_path}", f"{_incoming_zone}/{_fixture_filename}")
    logger.info("Landed regional orders fixture at '%s/%s'", _incoming_zone, _fixture_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/027_trf_002_union_all.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: both CSV batches are copied fresh each time (Auto
# MAGIC Loader itself, not this notebook, tracks which files it has already ingested via its
# MAGIC checkpoint, so re-landing the same filenames does not re-ingest or duplicate rows).
