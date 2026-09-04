# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-CDC-006 -- Full Snapshot Diffing With Natural PK Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `flowx_testing/024_cdc_006_snapshot_pk.json` only --
# MAGIC intentionally separate from `02_seed_flowx_testing_data.py` and every other scenario's
# MAGIC own seed notebook, so this test case's build/run stays isolated.
# MAGIC
# MAGIC Provisions the `inventory` schema and `landing_dumps`/`_schemas` Volumes, then lands
# MAGIC **only** the Day-1 inventory extract (`inventory_snapshot_day1.csv`, 3 rows, keys
# MAGIC `[1, 2, 3]`) in the landing Volume. This job's own pipeline run therefore only ever sees
# MAGIC the Day-1 full snapshot -- the Day-2 extract (`inventory_snapshot_day2.csv`: key `1`
# MAGIC removed, key `3` modified, key `4` added) is deliberately **not** landed by this notebook;
# MAGIC it is copied in manually as a second step, per
# MAGIC `docs/archive/legacy_docs/45_tc_cdc_006.md`, to prove `apply_changes_from_snapshot`
# MAGIC insert/update/delete diffing across two separate pipeline updates.
# MAGIC
# MAGIC Uses 2 new, hand-authored fixtures under
# MAGIC `sample_data/flowx_testing/inventory_usecase/` -- a compact, integer-keyed
# MAGIC (`item_id`) full-snapshot pair whose Day-2 file is a textbook one-delete/one-update/
# MAGIC one-insert diff. The other `FULL_SNAPSHOT_CDC` pair in the repo,
# MAGIC `sample_mainframe_customer_master_day1/2.csv`, is deliberately left to TC-CDC-007: it is
# MAGIC keyed on a wide string column (`customer_name`) over 10 rows, which exercises a different
# MAGIC shape of the same strategy rather than duplicating this one.
# MAGIC
# MAGIC Run once per environment before onboarding `flowx_testing/024_cdc_006_snapshot_pk.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_cdc_006_snapshot_pk_data")

dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "flowx_testing", "inventory_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved inventory fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schema & Volumes
# MAGIC
# MAGIC `inventory` hosts the landing Volume; `silver_inventory` (the `FULL_SNAPSHOT_CDC` target
# MAGIC schema) is created automatically by the pipeline's own `CREATE SCHEMA IF NOT EXISTS` at
# MAGIC deployment time, same as every other scenario -- no need to pre-create it here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.inventory")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.inventory.landing_dumps")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.inventory._schemas")

logger.info("Provisioned catalog '%s' with inventory schema + landing_dumps/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land Day-1 Inventory Extract Only

# COMMAND ----------

INVENTORY_LANDING_ZONE = f"/Volumes/{CATALOG}/inventory/landing_dumps"
dbutils.fs.mkdirs(INVENTORY_LANDING_ZONE)

_day1_fixture = os.path.join(FIXTURE_DIR, "inventory_snapshot_day1.csv")
if not os.path.exists(_day1_fixture):
    raise FileNotFoundError(f"Required Day-1 inventory fixture missing: '{_day1_fixture}'")

dbutils.fs.cp(f"file:{_day1_fixture}", f"{INVENTORY_LANDING_ZONE}/inventory_snapshot_day1.csv")
logger.info(
    "Landed Day-1 inventory extract at '%s/inventory_snapshot_day1.csv' (3 rows, keys [1, 2, 3]).",
    INVENTORY_LANDING_ZONE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `flowx_testing/024_cdc_006_snapshot_pk.json` can now be onboarded and its pipeline run
# MAGIC for the Day-1 baseline (3 rows, keys `[1, 2, 3]`). See
# MAGIC `docs/archive/legacy_docs/45_tc_cdc_006.md` for the manual Day-2 step (copying
# MAGIC `inventory_snapshot_day2.csv` into this same landing Volume and re-running the pipeline)
# MAGIC that this notebook deliberately does not automate.
# MAGIC
# MAGIC Re-running this notebook on its own is safe and idempotent: it only ever re-copies the
# MAGIC Day-1 file (Auto Loader tracks already-ingested files via its own checkpoint, so
# MAGIC re-landing the same filename does not re-ingest or duplicate rows), and it never touches
# MAGIC or removes a Day-2 file a tester may have already copied in separately.
