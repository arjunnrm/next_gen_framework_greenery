# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-REC-003 -- Pre-Computed Hash Matcher Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/038_rec_003_precomputed_hash.json` only --
# MAGIC intentionally separate from every other scenario's own seed notebook, so this test case's
# MAGIC build/run stays isolated.
# MAGIC
# MAGIC Provisions the `rec_orders_usecase` schema and its landing Volumes, then lands two
# MAGIC independent 24-ish-row order batches -- `orders_src_batch1.csv` (baseline) and
# MAGIC `orders_tgt_batch1.csv` (downstream copy) -- each ingested by its own `ingestion_flow` in
# MAGIC the spec (`cdc_load_strategy: "SCD1"`, `generate_hash_columns: true`) into
# MAGIC `{{catalog}}.silver_sales.orders_src` / `orders_tgt`. The two fixtures are deliberately NOT
# MAGIC identical -- see `docs/60_tc_rec_003.md` for the full row-by-row breakdown -- so the
# MAGIC reconciliation flow's `hash_precomputed: true` matcher has a real MATCHED / VALUE_DRIFT /
# MAGIC MISSING_IN_TARGET / MISSING_IN_SOURCE mix to classify, not just an all-MATCHED trivial case.
# MAGIC
# MAGIC Built from `sample_data/metaflow_testing/rec_orders_usecase/orders_{src,tgt}_batch1.csv`.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/038_rec_003_precomputed_hash.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_rec_003_precomputed_hash_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "rec_orders_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved rec_orders_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schema & Volumes
# MAGIC
# MAGIC `rec_orders_usecase` hosts the two landing Volumes; `silver_sales` (the CDC-dispatched
# MAGIC target schema, already used by TC-TRF-002's own `all_global_orders` table) is created
# MAGIC automatically by the pipeline's own `CREATE SCHEMA IF NOT EXISTS` at deployment time, same
# MAGIC as every other scenario -- no need to pre-create it here.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.rec_orders_usecase")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.rec_orders_usecase.landing_orders_src")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.rec_orders_usecase.landing_orders_tgt")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.rec_orders_usecase._schemas")

logger.info(
    "Provisioned catalog '%s' with rec_orders_usecase schema + landing_orders_src/landing_orders_tgt/_schemas volumes.",
    CATALOG,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land Both Order Batches

# COMMAND ----------

ORDERS_SRC_INCOMING_ZONE = f"/Volumes/{CATALOG}/rec_orders_usecase/landing_orders_src/incoming"
ORDERS_TGT_INCOMING_ZONE = f"/Volumes/{CATALOG}/rec_orders_usecase/landing_orders_tgt/incoming"
dbutils.fs.mkdirs(ORDERS_SRC_INCOMING_ZONE)
dbutils.fs.mkdirs(ORDERS_TGT_INCOMING_ZONE)

_orders_src_fixture = os.path.join(FIXTURE_DIR, "orders_src_batch1.csv")
_orders_tgt_fixture = os.path.join(FIXTURE_DIR, "orders_tgt_batch1.csv")
for _fixture in (_orders_src_fixture, _orders_tgt_fixture):
    if not os.path.exists(_fixture):
        raise FileNotFoundError(f"Required orders fixture missing: '{_fixture}'")

dbutils.fs.cp(f"file:{_orders_src_fixture}", f"{ORDERS_SRC_INCOMING_ZONE}/orders_src_batch1.csv")
dbutils.fs.cp(f"file:{_orders_tgt_fixture}", f"{ORDERS_TGT_INCOMING_ZONE}/orders_tgt_batch1.csv")
logger.info(
    "Landed orders_src (24 rows) at '%s' and orders_tgt (24 rows, 1 drifted/1 missing/1 extra) at '%s'.",
    ORDERS_SRC_INCOMING_ZONE,
    ORDERS_TGT_INCOMING_ZONE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/038_rec_003_precomputed_hash.json` can now be onboarded and its pipeline
# MAGIC run, followed by `05_reconciliation_engine.py` with
# MAGIC `reconciliation_id=recon_rec_003_orders_precomputed_hash` -- see `docs/60_tc_rec_003.md` for
# MAGIC the full expected classification breakdown and `EXPLAIN` plan verification query.
# MAGIC
# MAGIC Re-running this notebook is safe and idempotent: it only ever re-copies the same 2 fixture
# MAGIC files (Auto Loader tracks already-ingested files via its own checkpoint, so re-landing the
# MAGIC same filenames does not re-ingest or duplicate rows).
