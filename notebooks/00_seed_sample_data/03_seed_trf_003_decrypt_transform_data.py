# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-TRF-003 -- In-DAG Column Decryption for Business Computations Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/028_trf_003_decrypt_transform.json` only --
# MAGIC intentionally separate from `02_seed_flowx_testing_data.py` and every other scenario's
# MAGIC own seed notebook, so this test case's build/run stays isolated.
# MAGIC
# MAGIC Provisions the `health`/`bronze_health`/`silver_health` schemas and the
# MAGIC `landing_visits`/`_schemas` Volumes, then lands one 5-row patient visit extract directly in
# MAGIC the incoming Volume (no ZIP handling needed here -- plain Auto Loader CSV ingestion is the
# MAGIC on-ramp, the actual point of this scenario is AES-GCM `billing_amount` encryption in Bronze
# MAGIC + `decrypted_columns` decryption in the downstream transformation), built from the fixture
# MAGIC under `sample_data/flowx_testing/health_usecase/`:
# MAGIC
# MAGIC * `patient_visits_batch1.csv` -- 5 rows, known plaintext `billing_amount` values
# MAGIC   (250.00, 175.50, 320.25, 99.75, 410.50) summing to exactly **1256.00** -- the number the
# MAGIC   `patient_billing_summary` materialized view's `sum(billing_amount)` must reproduce after a
# MAGIC   full encrypt-then-decrypt round trip through `{{catalog}}.security.pii_encryption_key`.
# MAGIC
# MAGIC **Required prerequisite, external to this notebook and this bundle**: the UC secret
# MAGIC `{{catalog}}.security.pii_encryption_key` referenced by
# MAGIC `028_trf_003_decrypt_transform.json`'s `encrypted_columns`/`decrypted_columns` must already
# MAGIC be provisioned -- this is the exact same key `docs/05_deployment_guide.md`'s prerequisites
# MAGIC section already documents provisioning for this project's other encryption scenarios; it is
# MAGIC never re-provisioned or overwritten here. See `docs/49_tc_trf_003.md` for the full command.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/028_trf_003_decrypt_transform.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_trf_003_decrypt_transform_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "health_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved health_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.health")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_health")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_health")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.health.landing_visits")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.health._schemas")

logger.info(
    "Provisioned catalog '%s' with health/bronze_health/silver_health schemas + landing_visits/_schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Patient Visits CSV Extract

# COMMAND ----------

VISITS_INCOMING_ZONE = f"/Volumes/{CATALOG}/health/landing_visits/incoming"
dbutils.fs.mkdirs(VISITS_INCOMING_ZONE)

_fixture_filename = "patient_visits_batch1.csv"
_fixture_path = os.path.join(FIXTURE_DIR, _fixture_filename)
if not os.path.exists(_fixture_path):
    raise FileNotFoundError(f"Required patient visits fixture missing: '{_fixture_path}'")

dbutils.fs.cp(f"file:{_fixture_path}", f"{VISITS_INCOMING_ZONE}/{_fixture_filename}")
logger.info("Landed patient visits fixture at '%s/%s'", VISITS_INCOMING_ZONE, _fixture_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/028_trf_003_decrypt_transform.json` can now be onboarded and its pipeline
# MAGIC run -- provided `{{catalog}}.security.pii_encryption_key` has already been provisioned (see
# MAGIC `docs/49_tc_trf_003.md`). Re-running this notebook is safe: the CSV is copied fresh each
# MAGIC time (Auto Loader itself, not this notebook, tracks which files it has already ingested via
# MAGIC its checkpoint, so re-landing the same filename does not re-ingest or duplicate rows in the
# MAGIC Bronze table -- and the Silver materialized view fully recomputes from Bronze on every
# MAGIC pipeline update regardless, so it never accumulates duplicates either way).
