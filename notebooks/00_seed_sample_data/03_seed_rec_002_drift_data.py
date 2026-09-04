# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-REC-002 -- Attribute Value Drift Detection (VALUE_DRIFT) Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/037_rec_002_drift.json` only --
# MAGIC intentionally separate from every other scenario's own seed notebook, so this test case's
# MAGIC build/run stays isolated.
# MAGIC
# MAGIC Provisions the `crm` schema and a dedicated `landing_customer_drift` Volume (with
# MAGIC `baseline/`/`cdc/` subfolders so Auto Loader gives each ingestion flow its own incoming
# MAGIC zone) plus `_schemas`, then lands one small fixture file into each side:
# MAGIC
# MAGIC * `customer_baseline_drift.csv` -> `.../landing_customer_drift/baseline/` -- `C001` @
# MAGIC   `status=SUSPENDED`.
# MAGIC * `customer_cdc_drift.csv` -> `.../landing_customer_drift/cdc/` -- `C001` @
# MAGIC   `status=ACTIVE` (the same customer, a deliberately drifted `status` value).
# MAGIC
# MAGIC Both land into `{{catalog}}.silver_crm.customer_baseline`/`.customer_cdc` once
# MAGIC `037_rec_002_drift.json` is onboarded and its pipeline runs; the reconciliation flow in that
# MAGIC same spec then compares them on `customer_id` and classifies `C001` as `VALUE_DRIFT` on
# MAGIC `status`. See `docs/59_tc_rec_002.md`.
# MAGIC
# MAGIC Built from the fixtures under `sample_data/flowx_testing/crm_usecase/`.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/037_rec_002_drift.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_rec_002_drift_data")

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
# MAGIC ## 1. Provision Schema & Volumes
# MAGIC
# MAGIC `crm` hosts the landing Volume; `silver_crm` (both ingestion flows' target schema) is
# MAGIC created automatically by the pipeline's own `CREATE SCHEMA IF NOT EXISTS` at deployment
# MAGIC time, same as every other scenario -- no need to pre-create it here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.crm")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm.landing_customer_drift")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm._schemas")

logger.info("Provisioned catalog '%s' with crm schema + landing_customer_drift/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Baseline and CDC Fixtures Into Their Own Incoming Zones

# COMMAND ----------

BASELINE_INCOMING_ZONE = f"/Volumes/{CATALOG}/crm/landing_customer_drift/baseline"
CDC_INCOMING_ZONE = f"/Volumes/{CATALOG}/crm/landing_customer_drift/cdc"
dbutils.fs.mkdirs(BASELINE_INCOMING_ZONE)
dbutils.fs.mkdirs(CDC_INCOMING_ZONE)

_baseline_fixture = os.path.join(FIXTURE_DIR, "customer_baseline_drift.csv")
_cdc_fixture = os.path.join(FIXTURE_DIR, "customer_cdc_drift.csv")
for _fixture in (_baseline_fixture, _cdc_fixture):
    if not os.path.exists(_fixture):
        raise FileNotFoundError(f"Required TC-REC-002 fixture missing: '{_fixture}'")

dbutils.fs.cp(f"file:{_baseline_fixture}", f"{BASELINE_INCOMING_ZONE}/customer_baseline_drift.csv")
logger.info("Landed baseline customer fixture at '%s/customer_baseline_drift.csv' (C001 @ status=SUSPENDED).", BASELINE_INCOMING_ZONE)

dbutils.fs.cp(f"file:{_cdc_fixture}", f"{CDC_INCOMING_ZONE}/customer_cdc_drift.csv")
logger.info("Landed CDC customer fixture at '%s/customer_cdc_drift.csv' (C001 @ status=ACTIVE).", CDC_INCOMING_ZONE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/037_rec_002_drift.json` can now be onboarded and its pipeline run --
# MAGIC both `{{catalog}}.silver_crm.customer_baseline` and `.customer_cdc` will land exactly one
# MAGIC `C001` row each, with `status` deliberately drifted between them.
# MAGIC
# MAGIC Re-running this notebook is safe and idempotent: it only ever re-copies the same two
# MAGIC filenames (Auto Loader tracks already-ingested files via its own checkpoint, so re-landing
# MAGIC an identical filename does not re-ingest or duplicate rows).
