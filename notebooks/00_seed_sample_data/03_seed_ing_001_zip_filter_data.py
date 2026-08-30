# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-ING-001 -- Selective Glob Pattern ZIP Extraction Fixtures
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/010_ing_001_zip_filter.json` only --
# MAGIC intentionally separate from `02_seed_metaflow_testing_data.py` (which seeds scenarios
# MAGIC 001/002/003) so this test case's build/run stays isolated from those other scenarios'
# MAGIC own fixtures.
# MAGIC
# MAGIC Provisions the `crm`/`bronze_crm` schemas and the `landing_zip`/`_schemas` Volumes, then
# MAGIC lands 2 ZIP archives directly in the incoming Volume, built in-process from the CSV
# MAGIC fixtures under `sample_data/metaflow_testing/crm_usecase/` (workspace-bundle sync can
# MAGIC mangle a pre-built `.zip` upload, so this notebook builds the archive bytes itself --
# MAGIC same convention as `02_seed_metaflow_testing_data.py`'s own scenario-001 EA ZIPs):
# MAGIC
# MAGIC * `customer_data_20260828.zip` -- 100 rows, filename matches the onboarding spec's
# MAGIC   `zip_file_pattern: "customer_*.zip"`. Expected to be extracted and ingested.
# MAGIC * `vendor_feed_20260828.zip` -- 50 rows, filename does NOT match that pattern. Expected
# MAGIC   to remain untouched in the incoming Volume -- never extracted, never ingested.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/010_ing_001_zip_filter.json`.

# COMMAND ----------

import csv
import io
import logging
import os
import zipfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_ing_001_zip_filter_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "crm_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

logger.info("Resolved crm_usecase fixture directory: %s", FIXTURE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.crm")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_crm")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm.landing_zip")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.crm._schemas")

logger.info("Provisioned catalog '%s' with crm/bronze_crm schemas + landing_zip/_schemas volumes.", CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate the 2 ZIP Archives On-Cluster

# COMMAND ----------


def _write_csv_zip_to_volume_from_fixture(zip_path: str, csv_filename: str, fixture_csv_path: str) -> None:
    """Read an already-CSV-shaped fixture file and re-zip it directly into a landing Volume --
    avoids syncing a pre-built .zip through the bundle (see 02_seed_metaflow_testing_data.py's
    identical note on why that's unreliable)."""
    try:
        with open(fixture_csv_path, "r", encoding="utf-8", newline="") as fixture_file:
            csv_text = fixture_file.read()
        dbutils.fs.mkdirs(os.path.dirname(zip_path))
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(csv_filename, csv_text)
        with open(zip_path, "wb") as destination:
            destination.write(zip_buffer.getvalue())
        row_count = sum(1 for _ in csv.reader(io.StringIO(csv_text))) - 1
        logger.info("Generated ZIP fixture directly on cluster: '%s' (%d data row(s))", zip_path, row_count)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to generate ZIP fixture '{zip_path}': {exc}") from exc


CRM_INCOMING_ZONE = f"/Volumes/{CATALOG}/crm/landing_zip/incoming"

# Matches the onboarding spec's zip_file_pattern ("customer_*.zip") -- expected to be extracted.
_write_csv_zip_to_volume_from_fixture(
    f"{CRM_INCOMING_ZONE}/customer_data_20260828.zip",
    "customer_data.csv",
    os.path.join(FIXTURE_DIR, "customer_data.csv"),
)

# Does NOT match "customer_*.zip" -- expected to remain untouched in the incoming Volume.
_write_csv_zip_to_volume_from_fixture(
    f"{CRM_INCOMING_ZONE}/vendor_feed_20260828.zip",
    "vendor_feed.csv",
    os.path.join(FIXTURE_DIR, "vendor_feed.csv"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/010_ing_001_zip_filter.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: both ZIP archives regenerate idempotently (same
# MAGIC content, overwritten each time).
