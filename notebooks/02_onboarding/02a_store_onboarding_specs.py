# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Onboarding Specs into the Onboarding-Specs Volume
# MAGIC
# MAGIC Copies each workspace-synced onboarding spec JSON named in `spec_paths`
# MAGIC (comma-separated) into a Unity Catalog Volume, so the Onboarding App -- and anyone
# MAGIC browsing the catalog -- finds the exact spec documents the bundle deployed.
# MAGIC
# MAGIC **Why this exists.** `bundle deploy` cannot put a file in a Volume. DABs moves files in
# MAGIC exactly two ways: the `artifacts` block (which uploads the wheel to `artifact_path`) and
# MAGIC `sync` (which mirrors the repo into the *workspace* file tree, never into a Volume). The
# MAGIC Volume itself is a bundle resource -- `resources/flowx_app/flowx_onboarding_specs_volume.yml`
# MAGIC creates it -- but a resource declares an empty container and says nothing about its
# MAGIC contents. Staging files into it is therefore a *job task*, and this notebook is that task.
# MAGIC Without it the specs reach the workspace tree and the Volume stays empty, which is how a
# MAGIC hand-run `databricks fs cp` ends up being the only thing that ever filled it.
# MAGIC
# MAGIC **Why not `09a_store_sample_config.py`.** That notebook does the same job for the sample
# MAGIC suite, and `tests/unit/test_sample_suite_layout.py::test_store_sample_config_targets_exactly_one_volume`
# MAGIC pins its `TARGET_DIR` f-string and asserts it provisions exactly one Volume
# MAGIC (`flowx_sample.sample_configs`). That guard is deliberate -- it keeps "every sample's spec
# MAGIC lives in one Volume" true by construction -- so parameterising that notebook to reach a
# MAGIC second destination would break it for a good reason. This is its framework-side sibling:
# MAGIC same validate-then-copy logic, destination supplied by the caller.
# MAGIC
# MAGIC **Idempotent.** The Volume is provisioned `IF NOT EXISTS` and every spec is copied with
# MAGIC overwrite, so each run refreshes the published specs in place -- which is the property
# MAGIC that keeps the Volume from drifting behind `BT_Usecase/` as specs are edited.
# MAGIC
# MAGIC Each spec is `json.loads`-checked before publishing: an unparseable document is refused
# MAGIC loudly rather than published as a broken reference.
# MAGIC
# MAGIC ### Parameters
# MAGIC
# MAGIC | widget | meaning |
# MAGIC |---|---|
# MAGIC | `spec_paths` | comma-separated workspace paths of the spec JSONs to publish (required) |
# MAGIC | `catalog` | target Unity Catalog |
# MAGIC | `schema` | schema holding the destination Volume (default `config`) |
# MAGIC | `volume` | destination Volume name (default `onboarding_specs`) |
# MAGIC | `subdirectory` | optional folder under the Volume, e.g. `UC7`; blank publishes at the root |

# COMMAND ----------

import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("store_onboarding_specs")

dbutils.widgets.text("spec_paths", "", "Comma-separated workspace paths of onboarding spec JSONs")
dbutils.widgets.text("catalog", "", "Target Unity Catalog")
dbutils.widgets.text("schema", "config", "Schema holding the destination Volume")
dbutils.widgets.text("volume", "onboarding_specs", "Destination Volume name")
dbutils.widgets.text("subdirectory", "", "Optional folder under the Volume (e.g. UC7)")

SPEC_PATHS = [path.strip() for path in dbutils.widgets.get("spec_paths").split(",") if path.strip()]
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VOLUME = dbutils.widgets.get("volume").strip()
SUBDIRECTORY = dbutils.widgets.get("subdirectory").strip().strip("/")

if not SPEC_PATHS:
    raise ValueError("The 'spec_paths' widget is required: a comma-separated list of workspace spec JSON paths.")
if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")
if not SCHEMA:
    raise ValueError("The 'schema' widget must be set to the schema holding the destination Volume.")
if not VOLUME:
    raise ValueError("The 'volume' widget must be set to the destination Volume name.")

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
TARGET_DIR = f"{VOLUME_ROOT}/{SUBDIRECTORY}" if SUBDIRECTORY else VOLUME_ROOT

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provision the Destination
# MAGIC
# MAGIC The schema and Volume are normally already bundle-declared resources; provisioning them
# MAGIC `IF NOT EXISTS` keeps this notebook runnable on a workspace where the deploy that creates
# MAGIC them has not run yet, and is a no-op everywhere else.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
# `fs cp` does NOT create parent directories -- an un-created subdirectory fails the copy with
# "no such directory", so make it explicitly rather than relying on the copy to do it.
dbutils.fs.mkdirs(TARGET_DIR)

logger.info("Provisioned destination directory '%s'.", TARGET_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate and Publish Each Spec

# COMMAND ----------

published = []

for spec_path in SPEC_PATHS:
    # ${workspace.file_path}-rooted paths arrive as /Workspace/... and are readable through the
    # workspace-files FUSE mount; tolerate a caller passing the un-prefixed spelling too.
    local_path = spec_path
    if not os.path.exists(local_path) and not spec_path.startswith("/Workspace"):
        local_path = f"/Workspace{spec_path}"
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f"Onboarding spec not found at '{spec_path}' (also tried '{local_path}') -- is the "
            "spec's directory synced alongside this notebook by the bundle?"
        )

    with open(local_path, "r", encoding="utf-8") as source:
        content = source.read()
    json.loads(content)  # refuse to publish an unparseable spec

    destination_path = f"{TARGET_DIR}/{os.path.basename(local_path)}"
    try:
        dbutils.fs.cp(f"file:{local_path}", destination_path)
    except Exception as exc:  # noqa: BLE001
        # Some runtimes disallow the file: scheme for workspace paths -- fall back to a plain
        # read/write copy of the exact bytes already read (and validated) above.
        logger.warning("dbutils.fs.cp failed for '%s' (%s) -- falling back to a direct write.", local_path, exc)
        with open(destination_path, "w", encoding="utf-8") as destination:
            destination.write(content)

    published.append(destination_path)
    logger.info("Published '%s' -> '%s' (%d bytes).", local_path, destination_path, len(content))

logger.info("Published %d spec(s) into '%s'.", len(published), TARGET_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Every spec named in `spec_paths` is now readable at the destination Volume path, which is
# MAGIC the root the Onboarding App reads (`FLOWX_SPEC_VOLUME_ROOT`).
