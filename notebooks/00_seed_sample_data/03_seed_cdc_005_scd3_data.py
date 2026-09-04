# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-CDC-005 -- SCD Type 3 Current & Previous State Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/023_cdc_005_scd3.json` only -- kept
# MAGIC separate from every other scenario's own seed notebook so this test case's build/run
# MAGIC stays isolated.
# MAGIC
# MAGIC Unlike a single-shot seed notebook, this one is **parameterized by `batch_number`** and
# MAGIC is meant to be invoked **3 separate times across the same job run**, interleaved with 3
# MAGIC separate pipeline updates -- that is the whole point of TC-CDC-005: SCD3 has no native
# MAGIC Lakeflow equivalent (`cdc/scd.py::register_scd3`), so its `current_<col>`/`previous_<col>`
# MAGIC pivot can only be observed by actually advancing subscription `S100` through 3 real,
# MAGIC separately-landed-and-processed status changes:
# MAGIC
# MAGIC | `batch_number` | Fixture | `status` landed | Pipeline update # | Expected `current_status` / `previous_status` after that update |
# MAGIC |---|---|---|---|---|
# MAGIC | `1` | `subscription_batch1.csv` | `TRIAL` | 1st | `TRIAL` / `NULL` (only one version so far) |
# MAGIC | `2` | `subscription_batch2.csv` | `ACTIVE` | 2nd | `ACTIVE` / `TRIAL` |
# MAGIC | `3` | `subscription_batch3.csv` | `CHURNED` | 3rd | `CHURNED` / `ACTIVE` |
# MAGIC
# MAGIC Each fixture (`sample_data/flowx_testing/sub_usecase/subscription_batch{1,2,3}.csv`) is
# MAGIC a single row for `sub_id = S100`, differing only in `status` and `event_ts` -- landed one
# MAGIC at a time so Auto Loader's checkpoint (and the downstream SCD3 transformation's own
# MAGIC hidden `_dim_subscription_scd3_scd2_history` streaming table) only ever see one new event
# MAGIC per pipeline update, exactly matching a real subscription lifecycle arriving over time.
# MAGIC
# MAGIC Run once per batch, immediately before that batch's pipeline update -- see
# MAGIC `resources/feature_tests/flowx_test_cdc_005_scd3_job.yml` for the full
# MAGIC `seed(1) -> run -> seed(2) -> run -> seed(3) -> run` task chain and docs/44_tc_cdc_005.md
# MAGIC for the full 3-batch drop-and-rerun narrative.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_cdc_005_scd3_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
dbutils.widgets.text("batch_number", "1", "Which batch to land (1, 2, or 3)")

CATALOG = dbutils.widgets.get("catalog").strip()
BATCH_NUMBER = dbutils.widgets.get("batch_number").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if BATCH_NUMBER not in ("1", "2", "3"):
    raise ValueError(f"The 'batch_number' widget must be '1', '2', or '3' -- got '{BATCH_NUMBER}'.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "sub_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved sub_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC Idempotent -- safe to run before every one of the 3 batches, not just the first.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.sub")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_sub")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_sub")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sub.landing_sub")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sub._schemas")

logger.info(
    "Provisioned catalog '%s' with sub/bronze_sub/silver_sub schemas + landing_sub/_schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land This Batch's Subscription Status-Change Row

# COMMAND ----------

SUB_INCOMING_ZONE = f"/Volumes/{CATALOG}/sub/landing_sub/incoming"
dbutils.fs.mkdirs(SUB_INCOMING_ZONE)

_batch_filename = f"subscription_batch{BATCH_NUMBER}.csv"
_fixture_path = os.path.join(FIXTURE_DIR, _batch_filename)
if not os.path.exists(_fixture_path):
    raise FileNotFoundError(f"Required subscription fixture missing: '{_fixture_path}'")

dbutils.fs.cp(f"file:{_fixture_path}", f"{SUB_INCOMING_ZONE}/{_batch_filename}")
logger.info("Landed subscription batch %s fixture at '%s/%s'", BATCH_NUMBER, SUB_INCOMING_ZONE, _batch_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Batch `{{BATCH_NUMBER}}`'s single-row status-change event is now landed at
# MAGIC `/Volumes/{catalog}/sub/landing_sub/incoming/subscription_batch{N}.csv`. Trigger this
# MAGIC batch's pipeline update next (see `resources/feature_tests/flowx_test_cdc_005_scd3_job.yml`) before
# MAGIC re-running this notebook for the next batch -- landing all 3 files before any pipeline
# MAGIC update would collapse all 3 status changes into a single Auto Loader micro-batch, defeating
# MAGIC the point of this scenario (proving SCD3's current/previous pivot advances correctly across
# MAGIC *separate* pipeline runs, not just separate rows within one run).
