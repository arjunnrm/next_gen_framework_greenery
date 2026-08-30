# Databricks notebook source
# MAGIC %md
# MAGIC # Seed TC-PRM-005 -- Parameterized Egress Sink Paths & Export File Naming Fixture
# MAGIC
# MAGIC Dedicated seed notebook for `metaflow_testing/047_prm_005_sink_param.json` only -- lands a
# MAGIC small (5-row) `acme_export_staging.csv` fixture into the INPUT landing Volume and provisions
# MAGIC the `egress` schema plus a **per-client** Unity Catalog Volume named after
# MAGIC `pipeline_parameters.client_code` (`ACME_CORP`) -- the exact Volume
# MAGIC `transformation/parameters.py::substitute_path_parameters` is expected to resolve
# MAGIC `sink_config.path`/`post_export_archive.output_zip_path`'s `${client_code}` placeholder into
# MAGIC at pipeline-update time. `${export_tier}` (`GOLD`) resolves into a subdirectory *inside* that
# MAGIC same per-client volume (`_staging/GOLD/`), not a second Volume -- Unity Catalog Volumes are
# MAGIC only a 3-level `catalog.schema.volume` namespace; everything past that third path segment is
# MAGIC just a directory inside the Volume's own storage.
# MAGIC
# MAGIC `client_code`/`export_tier` are exposed as widgets here (rather than hardcoded), mirroring
# MAGIC `03_seed_prm_001_path_param_data.py`'s own convention, so this notebook stays in lockstep with
# MAGIC the spec's own `pipeline_parameters` if either is ever changed.
# MAGIC
# MAGIC `silver_exports` (the pipeline's own default `catalog`/`schema`, where `acme_export_staging` is
# MAGIC actually materialized) is created automatically by the pipeline's own `CREATE SCHEMA IF NOT
# MAGIC EXISTS` at deployment time, same as every other scenario in this repo -- not seeded here.
# MAGIC
# MAGIC Run once per environment before onboarding `metaflow_testing/047_prm_005_sink_param.json`.

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_prm_005_sink_param_data")

dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")
dbutils.widgets.text("client_code", "ACME_CORP", "Client code (must match the spec's pipeline_parameters.client_code)")
dbutils.widgets.text("export_tier", "GOLD", "Export tier (must match the spec's pipeline_parameters.export_tier)")

CATALOG = dbutils.widgets.get("catalog").strip()
CLIENT_CODE = dbutils.widgets.get("client_code").strip()
EXPORT_TIER = dbutils.widgets.get("export_tier").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if not CLIENT_CODE:
    raise ValueError("The 'client_code' widget must be set (must match the spec's pipeline_parameters.client_code).")
if not EXPORT_TIER:
    raise ValueError("The 'export_tier' widget must be set (must match the spec's pipeline_parameters.export_tier).")

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(_this_dir, "..", ".."))
FIXTURE_DIR = os.path.join(REPO_ROOT, "sample_data", "metaflow_testing", "acme_export_usecase")

if not os.path.isdir(FIXTURE_DIR):
    raise FileNotFoundError(f"Expected fixture directory at '{FIXTURE_DIR}' -- is sample_data/ synced alongside this notebook?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision Schemas & Volumes
# MAGIC
# MAGIC * `exports_ingest` -- the INPUT landing zone (`landing`/`incoming` + `_schemas`), not the
# MAGIC   pipeline's own default schema, so it needs explicit provisioning.
# MAGIC * `egress` -- the sink's OUTPUT schema, holding one Volume per client
# MAGIC   (`{CLIENT_CODE}`, here `ACME_CORP`) -- also never auto-created by the pipeline itself.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.exports_ingest")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.exports_ingest.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.exports_ingest._schemas")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.egress")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.egress.{CLIENT_CODE}")

logger.info(
    "Provisioned catalog '%s' with exports_ingest/landing+_schemas volumes and the "
    "per-client sink volume egress.%s (staging subdir '_staging/%s/', output subdir 'output/').",
    CATALOG, CLIENT_CODE, EXPORT_TIER,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Land the Plaintext ACME Export-Staging CSV

# COMMAND ----------

EXPORTS_INCOMING_ZONE = f"/Volumes/{CATALOG}/exports_ingest/landing/incoming"
dbutils.fs.mkdirs(EXPORTS_INCOMING_ZONE)

_fixture_path = os.path.join(FIXTURE_DIR, "acme_export_staging.csv")
if not os.path.exists(_fixture_path):
    raise FileNotFoundError(f"Required ACME export-staging fixture missing: '{_fixture_path}'")

dbutils.fs.cp(f"file:{_fixture_path}", f"{EXPORTS_INCOMING_ZONE}/acme_export_staging.csv")
logger.info("Landed ACME export-staging fixture at '%s/acme_export_staging.csv'", EXPORTS_INCOMING_ZONE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `metaflow_testing/047_prm_005_sink_param.json` can now be onboarded and its pipeline run.
# MAGIC Re-running this notebook is safe: schema/Volume provisioning is idempotent and the fixture
# MAGIC file is copied fresh each time (Auto Loader itself, not this notebook, tracks which files it
# MAGIC has already ingested via its checkpoint/schema location).
