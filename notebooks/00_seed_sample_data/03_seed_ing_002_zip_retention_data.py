# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-002 -- ZIP Source Purge Retention Policy
# MAGIC
# MAGIC Provisions the `sales` schema/Volumes and lands a single `orders_batch_01.zip` archive
# MAGIC into the incoming landing zone, generated on-cluster from the existing
# MAGIC `sample_data/sample_raw_orders.csv` fixture (10 order rows -- reused as-is, no new sample
# MAGIC data needed for this scenario). See `metaflow_testing/011_ing_002_zip_retention.json`,
# MAGIC whose `source_zip_handling.delete_source_after_extract: true` is what this scenario
# MAGIC exists to prove: after a successful pipeline update, this ZIP should be gone from
# MAGIC `incoming/` while its extracted CSV remains under `extracted/orders/`.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/011_ing_002_zip_retention.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Path Resolution

# COMMAND ----------

import io
import logging
import os
import zipfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_ing_002_zip_retention_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
ORDERS_FIXTURE_CSV = os.path.join(REPO_ROOT, "sample_data", "sample_raw_orders.csv")

if not os.path.isfile(ORDERS_FIXTURE_CSV):
    raise FileNotFoundError(f"Required orders fixture missing: '{ORDERS_FIXTURE_CSV}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved orders fixture at: %s", ORDERS_FIXTURE_CSV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the `sales` Schema & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.sales")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sales.landing_zip")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sales._schemas")

logger.info("Provisioned catalog '%s'.sales with landing_zip/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate `orders_batch_01.zip` On-Cluster and Land It in `incoming/`
# MAGIC
# MAGIC Built directly in-process (never synced as a pre-built `.zip` through the bundle --
# MAGIC Workspace Files auto-extracts `.zip` uploads on sync, see
# MAGIC `docs/12_zip_ingestion_pipeline.md`'s platform-gotchas section and this same pattern in
# MAGIC `02_seed_metaflow_testing_data.py`), then written to the Volume with one plain
# MAGIC sequential write (Volumes' FUSE mount doesn't support seeking on an open-for-write handle,
# MAGIC which a ZIP writer needs -- hence building the archive in an in-memory buffer first).

# COMMAND ----------

INCOMING_ZONE = f"/Volumes/{CATALOG}/sales/landing_zip/incoming"
ZIP_PATH = f"{INCOMING_ZONE}/orders_batch_01.zip"

try:
    with open(ORDERS_FIXTURE_CSV, "r", encoding="utf-8", newline="") as fixture_file:
        csv_text = fixture_file.read()

    dbutils.fs.mkdirs(INCOMING_ZONE)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("orders_batch_01.csv", csv_text)

    with open(ZIP_PATH, "wb") as destination:
        destination.write(zip_buffer.getvalue())

    logger.info("Generated ZIP fixture directly on cluster: '%s'", ZIP_PATH)
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(f"Failed to generate ZIP fixture '{ZIP_PATH}': {exc}") from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/011_ing_002_zip_retention.json` can now be onboarded and its pipeline
# MAGIC run. Re-running this notebook is safe/idempotent: `orders_batch_01.zip` regenerates with
# MAGIC identical content, overwriting any prior copy in `incoming/` -- if a previous pipeline
# MAGIC run already purged it (the behavior this scenario tests), this step re-lands a fresh copy
# MAGIC so the scenario can be exercised again from a clean starting state.
