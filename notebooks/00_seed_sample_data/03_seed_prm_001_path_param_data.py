# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-PRM-001 -- Parameterized Dynamic Volume & File Path Fixture
# MAGIC
# MAGIC Provisions the `finance_ops`/`bronze_finance` schemas + `finance_ops.landing`/
# MAGIC `finance_ops._schemas` Volumes and lands
# MAGIC `sample_data/metaflow_testing/finance_usecase/daily_txns_batch1.csv` -- 4 daily
# MAGIC transaction records -- directly under the **date-partitioned** landing path
# MAGIC `/Volumes/{{catalog}}/finance_ops/landing/2026-08-28/`, i.e. the exact path
# MAGIC `metaflow_testing/045_prm_001_path_param.json`'s `pipeline_parameters` (`data_domain:
# MAGIC "finance_ops"`, `batch_date: "2026-08-28"`) resolve to once
# MAGIC `transformation/parameters.py::substitute_path_parameters` substitutes
# MAGIC `${data_domain}`/`${batch_date}` into `source_config.path` at pipeline-update time.
# MAGIC
# MAGIC `data_domain`/`batch_date` are exposed as widgets here (rather than hardcoded) so this
# MAGIC notebook stays in lockstep with the spec's own `pipeline_parameters` if either is ever
# MAGIC changed -- the job task that invokes this notebook passes the same literal values the
# MAGIC spec declares.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/045_prm_001_path_param.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_prm_001_path_param_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
dbutils.widgets.text("data_domain", "finance_ops", "Data domain (must match the spec's pipeline_parameters.data_domain)")
dbutils.widgets.text("batch_date", "2026-08-28", "Batch date (must match the spec's pipeline_parameters.batch_date)")

CATALOG = dbutils.widgets.get("catalog").strip()
DATA_DOMAIN = dbutils.widgets.get("data_domain").strip()
BATCH_DATE = dbutils.widgets.get("batch_date").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if not DATA_DOMAIN:
    raise ValueError("The 'data_domain' widget must be set (must match the spec's pipeline_parameters.data_domain).")
if not BATCH_DATE:
    raise ValueError("The 'batch_date' widget must be set (must match the spec's pipeline_parameters.batch_date).")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "finance_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC `{catalog}.{data_domain}` hosts the parameterized landing Volume (`landing`) that
# MAGIC `source_config.path`'s `${data_domain}` placeholder resolves into;
# MAGIC `{catalog}.bronze_finance` is the fixed (non-parameterized) Bronze target schema the
# MAGIC spec's `target_schema` names directly.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DATA_DOMAIN}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_finance")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{DATA_DOMAIN}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{DATA_DOMAIN}._schemas")

logger.info(
    "Provisioned catalog '%s' with '%s'/bronze_finance schemas + '%s'.landing/_schemas volumes.",
    CATALOG, DATA_DOMAIN, DATA_DOMAIN,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Daily Transactions Batch Under the Date-Partitioned Path
# MAGIC
# MAGIC Writes directly to `/Volumes/{catalog}/{data_domain}/landing/{batch_date}/` -- the
# MAGIC literal, fully-resolved path the engine notebook's `substitute_path_parameters` call is
# MAGIC expected to produce from `${data_domain}`/`${batch_date}` at pipeline-update time. If
# MAGIC either widget value drifts from the spec's own `pipeline_parameters`, Auto Loader simply
# MAGIC finds nothing here -- a visible symptom, not a silent pass.

# COMMAND ----------

FINANCE_LANDING_DATED_PATH = f"/Volumes/{CATALOG}/{DATA_DOMAIN}/landing/{BATCH_DATE}"
_fixture_file = os.path.join(FIXTURE_DIR, "daily_txns_batch1.csv")
if not os.path.exists(_fixture_file):
    raise FileNotFoundError(f"Required daily-transactions fixture missing: '{_fixture_file}'")

dbutils.fs.mkdirs(FINANCE_LANDING_DATED_PATH)
dbutils.fs.cp(f"file:{_fixture_file}", f"{FINANCE_LANDING_DATED_PATH}/daily_txns_batch1.csv")
logger.info("Landed daily transactions fixture at '%s/daily_txns_batch1.csv'", FINANCE_LANDING_DATED_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/045_prm_001_path_param.json` can now be onboarded and its pipeline
# MAGIC run. Re-running this notebook is safe: schema/Volume provisioning is idempotent and the
# MAGIC fixture file is copied fresh each time (Auto Loader itself, not this notebook, tracks
# MAGIC which files it has already ingested via its checkpoint/schema location).
