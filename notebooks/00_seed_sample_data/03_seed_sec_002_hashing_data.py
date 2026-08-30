# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-SEC-002 -- SHA-256 Hash Key & Value Determinism Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/034_sec_002_hashing.json` only --
# MAGIC intentionally separate from every other scenario's own seed notebook, so this test case's
# MAGIC build/run stays isolated.
# MAGIC
# MAGIC Provisions the `sec_accounts` schema and `landing_accounts`/`_schemas` Volumes, then lands
# MAGIC a small, 6-row accounts fixture (`accounts_hashed_batch1.csv`) in the incoming Volume. The
# MAGIC same `account_id` (`ACC001`) deliberately recurs under 3 different `region` values (and
# MAGIC `ACC002` under 2) -- proving the composite `primary_keys: ["region", "account_id"]`
# MAGIC actually matters: a hash keyed on `account_id` alone would collide these rows together,
# MAGIC while the real composite-key hash correctly gives each `(region, account_id)` pair its own
# MAGIC distinct `__framework_hash_key`. See `docs/55_tc_sec_002.md` for the full scenario writeup
# MAGIC and verification queries.
# MAGIC
# MAGIC Built from `sample_data/metaflow_testing/sec_usecase/accounts_hashed_batch1.csv`.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/034_sec_002_hashing.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_sec_002_hashing_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "sec_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved sec_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schema & Volumes
# MAGIC
# MAGIC `sec_accounts` hosts the landing Volume; `silver_accounts` (the hashed target schema) is
# MAGIC created automatically by the pipeline's own `CREATE SCHEMA IF NOT EXISTS` at deployment
# MAGIC time, same as every other scenario -- no need to pre-create it here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.sec_accounts")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sec_accounts.landing_accounts")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sec_accounts._schemas")

logger.info("Provisioned catalog '%s' with sec_accounts schema + landing_accounts/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Accounts Fixture

# COMMAND ----------

ACCOUNTS_INCOMING_ZONE = f"/Volumes/{CATALOG}/sec_accounts/landing_accounts/incoming"
dbutils.fs.mkdirs(ACCOUNTS_INCOMING_ZONE)

_accounts_fixture = os.path.join(FIXTURE_DIR, "accounts_hashed_batch1.csv")
if not os.path.exists(_accounts_fixture):
    raise FileNotFoundError(f"Required accounts fixture missing: '{_accounts_fixture}'")

dbutils.fs.cp(f"file:{_accounts_fixture}", f"{ACCOUNTS_INCOMING_ZONE}/accounts_hashed_batch1.csv")
logger.info(
    "Landed accounts fixture at '%s/accounts_hashed_batch1.csv' (6 rows, composite key region+account_id).",
    ACCOUNTS_INCOMING_ZONE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/034_sec_002_hashing.json` can now be onboarded and its pipeline run --
# MAGIC `{{catalog}}.silver_accounts.accounts_hashed` should end up with 6 rows, each carrying a
# MAGIC `__framework_hash_key` equal to `sha2(concat_ws('||', region, account_id), 256)`. See
# MAGIC `docs/55_tc_sec_002.md` for the full verification query.
# MAGIC
# MAGIC Re-running this notebook is safe and idempotent: it only ever re-copies the same fixture
# MAGIC file (Auto Loader tracks already-ingested files via its own checkpoint, so re-landing the
# MAGIC same filename does not re-ingest or duplicate rows).
