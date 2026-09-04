# Databricks notebook source
# MAGIC %md
# MAGIC # ZIP Ingestion Pipeline
# MAGIC
# MAGIC Thin orchestration notebook: reads a `zip_ingestion_configs/*.json` config describing
# MAGIC a batch of input ZIP archives, and delegates to
# MAGIC `flowx.lakeflow_framework.archive.zip_ingestion_pipeline` for
# MAGIC validation, extraction, staging, the configured join/transform, and re-archiving the
# MAGIC result. See `docs/12_zip_ingestion_pipeline.md`.
# MAGIC
# MAGIC Not control-table-driven (unlike ingestion/transformation/reconciliation flows) --
# MAGIC a one-shot batch job's configuration doesn't need the same always-on-schema
# MAGIC treatment; a plain JSON config, templated the same way onboarding specs are, is a
# MAGIC better fit. Reuses `onboarding/spec_loader.py`'s `{{catalog}}`/`{{env}}` templating
# MAGIC rather than re-implementing it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("zip_ingestion_pipeline")

try:
    import flowx.lakeflow_framework  # noqa: F401
except ImportError:
    try:
        this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        dev_src_root = os.path.abspath(os.path.join(this_dir, "..", "..", "src"))
        if dev_src_root not in sys.path:
            sys.path.insert(0, dev_src_root)
        import flowx.lakeflow_framework  # noqa: F401
        logger.warning("Loaded 'flowx' from local 'src/' (dev fallback) -- not from an installed wheel.")
    except ImportError as exc:
        raise ImportError(
            "Could not import 'flowx'. In production this must be attached as a "
            "wheel library (see resources/*.yml); for local development, run from within the repo so "
            f"'../../src' resolves. Original error: {exc}"
        ) from exc

import json  # noqa: E402

from flowx.lakeflow_framework.archive.zip_ingestion_pipeline import ingest_zip_batch  # noqa: E402
from flowx.lakeflow_framework.exceptions import FrameworkConfigError  # noqa: E402
from flowx.lakeflow_framework.onboarding.spec_loader import (  # noqa: E402
    read_raw_spec_text,
    substitute_environment_placeholders,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Config Loading

# COMMAND ----------

dbutils.widgets.text("catalog", "poc", "Target Unity Catalog")
dbutils.widgets.text("env", "dev", "Target environment")
dbutils.widgets.text("config_path", "", "Path to a zip_ingestion_configs/*.json file")

CATALOG = dbutils.widgets.get("catalog").strip()
ENVIRONMENT = dbutils.widgets.get("env").strip()
CONFIG_PATH = dbutils.widgets.get("config_path").strip()

if not CONFIG_PATH:
    raise ValueError("The 'config_path' widget is required.")

raw_text = read_raw_spec_text(dbutils, CONFIG_PATH)
templated_text = substitute_environment_placeholders(raw_text, CATALOG, ENVIRONMENT)
try:
    config = json.loads(templated_text)
except json.JSONDecodeError as exc:
    raise FrameworkConfigError(f"ZIP ingestion config '{CONFIG_PATH}' is not valid JSON after templating: {exc}") from exc

REQUIRED_KEYS = {
    "zip_paths",
    "extract_dir",
    "staging_view_configs",
    "join_sql",
    "output_table",
    "output_csv_dir",
    "output_zip_path",
}
missing_keys = REQUIRED_KEYS.difference(config)
if missing_keys:
    raise FrameworkConfigError(f"ZIP ingestion config '{CONFIG_PATH}' missing required key(s): {sorted(missing_keys)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

result = ingest_zip_batch(
    spark,
    zip_paths=config["zip_paths"],
    extract_dir=config["extract_dir"],
    staging_view_configs=config["staging_view_configs"],
    join_sql=config["join_sql"],
    output_table=config["output_table"],
    output_csv_dir=config["output_csv_dir"],
    output_zip_path=config["output_zip_path"],
    secret_catalog=config.get("secret_catalog"),
    secret_schema=config.get("secret_schema"),
    secret_key=config.get("secret_key"),
)
logger.info("ZIP ingestion '%s' complete: %s", config.get("config_id", CONFIG_PATH), result)
