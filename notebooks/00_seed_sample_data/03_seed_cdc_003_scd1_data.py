# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-CDC-003 -- SCD1 Overwrite with Delete Mapping Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/021_cdc_003_scd1.json` only --
# MAGIC intentionally separate from `02_seed_flowx_testing_data.py` (scenarios 001/002/003) and
# MAGIC every other scenario's own seed notebook, so this test case's build/run stays isolated.
# MAGIC
# MAGIC Provisions the `crm` schema and `landing_customer`/`_schemas` Volumes, then lands **only**
# MAGIC the Day-1 customer fixture (`customer_scd1_day1.csv`, `C001` @ `tier=SILVER`) in the
# MAGIC incoming Volume. This job's own pipeline run therefore only ever sees Day-1 data -- the
# MAGIC Day-2 fixture (`customer_scd1_day2.csv`, `C001` updated to `tier=PLATINUM` plus `C002` sent
# MAGIC with `cdc_op=DELETED`) is deliberately **not** landed by this notebook; it is copied in
# MAGIC manually as a second step, per `docs/42_tc_cdc_003.md`, to prove the SCD1 overwrite-in-place
# MAGIC and delete-mapping behavior across two separate pipeline updates.
# MAGIC
# MAGIC Built from the fixtures under `sample_data/flowx_testing/crm_usecase/`.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/021_cdc_003_scd1.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_cdc_003_scd1_data")

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
# MAGIC `crm` hosts the landing Volume; `silver_crm` (the SCD1 target schema) is created
# MAGIC automatically by the pipeline's own `CREATE SCHEMA IF NOT EXISTS` at deployment time, same
# MAGIC as every other scenario -- no need to pre-create it here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.crm")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm.landing_customer")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm._schemas")

logger.info("Provisioned catalog '%s' with crm schema + landing_customer/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land Day-1 Customer Fixture Only

# COMMAND ----------

CRM_INCOMING_ZONE = f"/Volumes/{CATALOG}/crm/landing_customer/incoming"
dbutils.fs.mkdirs(CRM_INCOMING_ZONE)

_day1_fixture = os.path.join(FIXTURE_DIR, "customer_scd1_day1.csv")
if not os.path.exists(_day1_fixture):
    raise FileNotFoundError(f"Required Day-1 customer fixture missing: '{_day1_fixture}'")

dbutils.fs.cp(f"file:{_day1_fixture}", f"{CRM_INCOMING_ZONE}/customer_scd1_day1.csv")
logger.info("Landed Day-1 customer fixture at '%s/customer_scd1_day1.csv' (C001 @ tier=SILVER).", CRM_INCOMING_ZONE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/021_cdc_003_scd1.json` can now be onboarded and its pipeline run for the
# MAGIC Day-1 baseline (`C001` @ `tier=SILVER`, 1 row). See `docs/42_tc_cdc_003.md` for the manual
# MAGIC Day-2 step (copying `customer_scd1_day2.csv` into this same incoming Volume and re-running
# MAGIC the pipeline) that this notebook deliberately does not automate.
# MAGIC
# MAGIC Re-running this notebook on its own is safe and idempotent: it only ever re-copies the
# MAGIC Day-1 file (Auto Loader tracks already-ingested files via its own checkpoint, so re-landing
# MAGIC the same filename does not re-ingest or duplicate rows), and it never touches or removes a
# MAGIC Day-2 file a tester may have already copied in separately.
