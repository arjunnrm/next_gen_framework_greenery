# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-OBS-001 -- In-Pipeline Structured JSON Logging Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/042_obs_001_json_log.json` only -- intentionally
# MAGIC separate from every other `03_seed_*` notebook, so this test case's build/run stays isolated
# MAGIC from those other scenarios' own fixtures.
# MAGIC
# MAGIC Provisions the already-established `ea`/`bronze_ea` schemas (created idempotently by
# MAGIC scenario 001 / TC-ING-008 / TC-TRF-001 / TC-SNK-001 alike -- `CREATE ... IF NOT EXISTS`
# MAGIC makes re-provisioning them here harmless) plus a **new**, dedicated `landing_obs_json_log`
# MAGIC Volume (distinct from TC-ING-008's own `ea.landing_csv`), then lands a single plain
# MAGIC (non-ZIP) CSV directly in it: 5 synthetic "event" rows, 1 of which (`E003`, a `NULL`
# MAGIC `amount`) violates the onboarding spec's `dq_config` rule (`amount IS NOT NULL`,
# MAGIC `action: "quarantine"`) so the flow's quarantine-routing structured log path
# MAGIC (`operation: "dq_staging"`) actually has something to route, not just an always-empty
# MAGIC quarantine table.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/042_obs_001_json_log.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_obs_001_json_log_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "obs_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved obs_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC `ea`/`bronze_ea` may already exist (several other test cases provision the same two
# MAGIC schemas) -- `CREATE ... IF NOT EXISTS` makes this idempotent either way, and this test
# MAGIC case only ever adds its own new `landing_obs_json_log` Volume (and reuses the shared
# MAGIC `ea._schemas` Volume) alongside them.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ea")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_ea")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ea.landing_obs_json_log")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ea._schemas")

logger.info("Provisioned catalog '%s' with ea/bronze_ea schemas + landing_obs_json_log/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plain CSV Fixture
# MAGIC
# MAGIC No ZIP handling for this scenario -- the onboarding spec's `source_config.path` points
# MAGIC directly at this landing Volume, so the fixture is copied in as a plain CSV file.

# COMMAND ----------

OBS_JSON_LOG_LANDING = f"/Volumes/{CATALOG}/ea/landing_obs_json_log"
_fixture_csv = os.path.join(FIXTURE_DIR, "structured_log_events.csv")
if not os.path.exists(_fixture_csv):
    raise FileNotFoundError(f"Required structured-log-events fixture missing: '{_fixture_csv}'")

dbutils.fs.mkdirs(OBS_JSON_LOG_LANDING)
dbutils.fs.cp(f"file:{_fixture_csv}", f"{OBS_JSON_LOG_LANDING}/structured_log_events.csv")
logger.info(
    "Landed structured-log-events fixture at '%s/structured_log_events.csv' (5 rows, 1 with NULL amount [E003]).",
    OBS_JSON_LOG_LANDING,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/042_obs_001_json_log.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: the CSV fixture is copied fresh each time (Auto Loader
# MAGIC itself, not this notebook, tracks which files it has already ingested via its checkpoint).
