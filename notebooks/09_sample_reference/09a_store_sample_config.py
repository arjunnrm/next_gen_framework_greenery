# Databricks notebook source
# MAGIC %md
# MAGIC # Store Sample Onboarding Specs in the Developer-Reference Volume
# MAGIC
# MAGIC Tiny reusable utility -- the LAST task of every `resources/sample_jobs/*_job.yml` sample
# MAGIC job. Copies each workspace-synced onboarding spec JSON named in `spec_paths`
# MAGIC (comma-separated) into the Unity Catalog Volume
# MAGIC `/Volumes/<catalog>/metaflow_sample/sample_configs/`, so a developer browsing the
# MAGIC `metaflow_sample` schema finds, next to every table the sample suite produced, the exact
# MAGIC spec document that produced it.
# MAGIC
# MAGIC Idempotent: the schema/Volume are provisioned `IF NOT EXISTS` and each spec is copied
# MAGIC with overwrite, so re-running any sample job refreshes its stored spec in place. Each
# MAGIC spec is `json.loads`-checked before publishing -- an unparseable document is refused
# MAGIC loudly rather than stored as a broken reference.

# COMMAND ----------

import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("store_sample_config")

dbutils.widgets.text("spec_paths", "", "Comma-separated workspace paths of onboarding spec JSONs")
dbutils.widgets.text("catalog", "metaflow", "Target Unity Catalog")

SPEC_PATHS = [path.strip() for path in dbutils.widgets.get("spec_paths").split(",") if path.strip()]
CATALOG = dbutils.widgets.get("catalog").strip()

if not SPEC_PATHS:
    raise ValueError("The 'spec_paths' widget is required: a comma-separated list of workspace spec JSON paths.")
if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

SAMPLE_SCHEMA = "metaflow_sample"
TARGET_DIR = f"/Volumes/{CATALOG}/{SAMPLE_SCHEMA}/sample_configs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the Reference Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SAMPLE_SCHEMA}.sample_configs")
dbutils.fs.mkdirs(TARGET_DIR)

logger.info("Provisioned reference Volume directory '%s'.", TARGET_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Copy Each Spec

# COMMAND ----------

for spec_path in SPEC_PATHS:
    # ${workspace.file_path}-rooted paths arrive as /Workspace/... and are readable through the
    # workspace-files FUSE mount; tolerate a caller passing the un-prefixed spelling too.
    local_path = spec_path
    if not os.path.exists(local_path) and not spec_path.startswith("/Workspace"):
        local_path = f"/Workspace{spec_path}"
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f"Onboarding spec not found at '{spec_path}' (also tried '{local_path}') -- is the bundle's "
            "resources/sample_jobs/onboarding/ tree synced alongside this notebook?"
        )

    with open(local_path, "r", encoding="utf-8") as source:
        content = source.read()
    json.loads(content)  # refuse to publish an unparseable reference document

    destination_path = f"{TARGET_DIR}/{os.path.basename(local_path)}"
    try:
        dbutils.fs.cp(f"file:{local_path}", destination_path)
    except Exception as exc:  # noqa: BLE001
        # Some runtimes disallow the file: scheme for workspace paths -- fall back to a plain
        # read/write copy of the exact bytes already read (and validated) above.
        logger.warning("dbutils.fs.cp failed for '%s' (%s) -- falling back to a direct write.", local_path, exc)
        with open(destination_path, "w", encoding="utf-8") as destination:
            destination.write(content)
    logger.info("Stored '%s' -> '%s' (%d bytes).", local_path, destination_path, len(content))

logger.info("Stored %d spec(s) in '%s'.", len(SPEC_PATHS), TARGET_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Every spec named in `spec_paths` is now browsable at
# MAGIC `/Volumes/<catalog>/metaflow_sample/sample_configs/`.
