# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-SEC-001 -- Column-Level AES-GCM Encryption Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/033_sec_001_aes_encrypt.json` only --
# MAGIC intentionally separate from every other scenario's own seed notebook, so this test case's
# MAGIC build/run stays isolated.
# MAGIC
# MAGIC Provisions the `customers` schema and `landing_pii`/`_schemas` Volumes, then lands a small,
# MAGIC 3-row customer PII fixture (`customer_pii_day1.csv` -- plaintext `ssn`/`credit_card` values,
# MAGIC as they would arrive from a real source system) in the incoming Volume. This scenario's own
# MAGIC `ingestion_flow` (`df_sec_001_customer_pii_ingest`) is what encrypts `ssn`/`credit_card` via
# MAGIC `target_config.encrypted_columns` before the target table is ever materialized -- this
# MAGIC notebook only lands the raw, still-plaintext source file; it does not touch encryption at
# MAGIC all. See `docs/54_tc_sec_001.md` for the full scenario writeup.
# MAGIC
# MAGIC Built from `sample_data/flowx_testing/sec_usecase/customer_pii_day1.csv`.
# MAGIC
# MAGIC **Does not provision the `security` secret scope/`pii_encryption_key` secret** -- this
# MAGIC scenario deliberately reuses the same Unity Catalog secret every other encryption spec in
# MAGIC this project already relies on (see `docs/05_deployment_guide.md` §0 prerequisites); that
# MAGIC secret must already exist in the target workspace/catalog before this job's pipeline task
# MAGIC runs, exactly as it must for every other encryption scenario.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/033_sec_001_aes_encrypt.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sec_001_aes_encrypt_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "sec_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved sec_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schema & Volumes
# MAGIC
# MAGIC `customers` hosts the landing Volume; `bronze_customers` (the encrypted target schema) is
# MAGIC created automatically by the pipeline's own `CREATE SCHEMA IF NOT EXISTS` at deployment
# MAGIC time, same as every other scenario -- no need to pre-create it here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.customers")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.customers.landing_pii")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.customers._schemas")

logger.info("Provisioned catalog '%s' with customers schema + landing_pii/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Customer PII Fixture

# COMMAND ----------

CUSTOMERS_INCOMING_ZONE = f"/Volumes/{CATALOG}/customers/landing_pii/incoming"
dbutils.fs.mkdirs(CUSTOMERS_INCOMING_ZONE)

_pii_fixture = os.path.join(FIXTURE_DIR, "customer_pii_day1.csv")
if not os.path.exists(_pii_fixture):
    raise FileNotFoundError(f"Required customer PII fixture missing: '{_pii_fixture}'")

dbutils.fs.cp(f"file:{_pii_fixture}", f"{CUSTOMERS_INCOMING_ZONE}/customer_pii_day1.csv")
logger.info("Landed customer PII fixture at '%s/customer_pii_day1.csv' (3 rows, plaintext ssn/credit_card).", CUSTOMERS_INCOMING_ZONE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/033_sec_001_aes_encrypt.json` can now be onboarded and its pipeline run --
# MAGIC `{{catalog}}.bronze_customers.customer_pii_encrypted` should end up with 3 rows, `ssn`/
# MAGIC `credit_card` stored as AES-GCM ciphertext, never the plaintext values landed here. See
# MAGIC `docs/54_tc_sec_001.md` for the full verification query.
# MAGIC
# MAGIC Re-running this notebook is safe and idempotent: it only ever re-copies the same fixture
# MAGIC file (Auto Loader tracks already-ingested files via its own checkpoint, so re-landing the
# MAGIC same filename does not re-ingest or duplicate rows).
