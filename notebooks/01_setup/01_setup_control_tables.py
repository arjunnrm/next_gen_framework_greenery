# Databricks notebook source
# MAGIC %md
# MAGIC # Control Metadata Setup
# MAGIC
# MAGIC Thin orchestration notebook: provisions the `config` schema and the four Unity
# MAGIC Catalog control tables that drive the metadata-driven Lakeflow framework. All DDL
# MAGIC text lives in `flowx.lakeflow_framework.control_plane.ddl_definitions`
# MAGIC (see `src/flowx/lakeflow_framework/`) -- this notebook only
# MAGIC resolves parameters and executes/verifies.
# MAGIC
# MAGIC | Table                        | Purpose                                                            |
# MAGIC |------------------------------|---------------------------------------------------------------------|
# MAGIC | `dataflow_group_spec`        | One row per orchestrated pipeline group (unified/ingestion/transform)|
# MAGIC | `ingestion_flow_spec`        | One row per ingestion flow (Bronze landing) belonging to a group    |
# MAGIC | `transformation_flow_spec`   | One row per transformation flow (Silver/Gold) belonging to a group  |
# MAGIC | `onboarding_audit_log`       | Perception audit trail for every onboarding action                  |
# MAGIC
# MAGIC This notebook is idempotent: it is safe to re-run against an already-provisioned catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap
# MAGIC
# MAGIC In production, `flowx` is installed as a wheel library attached
# MAGIC to this job (see `resources/metadata_framework_job.yml` -- `environment.dependencies`),
# MAGIC so a plain `import` resolves it from site-packages with no path tricks needed. The
# MAGIC fallback below only kicks in for local, wheel-less notebook development: it adds the
# MAGIC repo's `src/` folder to `sys.path` so the same `import` statements work before you've
# MAGIC ever run `databricks bundle deploy`.

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("setup_control_tables")

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

from flowx.lakeflow_framework.control_plane.ddl_definitions import (  # noqa: E402
    get_all_control_table_ddls,
    get_preflight_function_ddl,
    get_schema_ddl,
)
from flowx.lakeflow_framework.control_plane.schema_provisioner import (  # noqa: E402
    is_already_exists_race,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Constants

# COMMAND ----------

dbutils.widgets.text("catalog", "poc", "Target Unity Catalog")
CATALOG = dbutils.widgets.get("catalog").strip()

if not CATALOG:
    raise ValueError("The 'catalog' widget must be set to a valid Unity Catalog name.")

CONTROL_SCHEMA = f"{CATALOG}.config"

# Table properties applied uniformly to every control table for write efficiency on
# small, high-frequency-upsert metadata tables.
COMMON_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

logger.info("Provisioning control metadata schema '%s'", CONTROL_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Helper

# COMMAND ----------


def execute_ddl(statement: str, description: str) -> None:
    """Execute a single DDL statement with structured logging and exception context.

    Raises
    ------
    RuntimeError
        Wraps any underlying Spark SQL exception with the failed statement's description.
    """
    try:
        logger.info("Applying DDL: %s", description)
        spark.sql(statement)
        logger.info("Successfully applied: %s", description)
    except Exception as exc:  # noqa: BLE001
        # A concurrent session creating the same object is not a failure: this notebook is
        # idempotent by design and several jobs legitimately run it at once (every
        # flowx_test_* job starts with setup_control_tables). Unity Catalog's
        # CREATE OR REPLACE FUNCTION is idempotent in intent but not atomic -- the loser of a race
        # still gets [ROUTINE_ALREADY_EXISTS]. Observed live 2026-08-29: three concurrent test jobs
        # ran this notebook and one failed outright, skipping every downstream task (TC-CDC-007).
        # The predicate is shared with the framework's own provisioner so both paths tolerate
        # exactly the same, narrow set of conditions -- anything else still fails loudly.
        if is_already_exists_race(exc):
            logger.info("Object for '%s' was created concurrently by another session; continuing.", description)
            return
        logger.error("Failed to apply DDL for '%s': %s", description, exc)
        raise RuntimeError(f"DDL execution failed for '{description}': {exc}") from exc


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema Provisioning

# COMMAND ----------

execute_ddl(get_schema_ddl(CONTROL_SCHEMA), f"create schema {CONTROL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Control Tables

# COMMAND ----------

for description, ddl_statement in get_all_control_table_ddls(CONTROL_SCHEMA, COMMON_TABLE_PROPERTIES):
    execute_ddl(ddl_statement, description)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verification

# COMMAND ----------

try:
    provisioned_tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {CONTROL_SCHEMA}").collect()]
    expected_tables = {
        "dataflow_group_spec",
        "ingestion_flow_spec",
        "transformation_flow_spec",
        "onboarding_audit_log",
    }
    missing = expected_tables.difference(provisioned_tables)
    if missing:
        raise RuntimeError(f"Control tables missing after provisioning: {sorted(missing)}")
    logger.info("Control schema '%s' fully provisioned: %s", CONTROL_SCHEMA, sorted(provisioned_tables))
except Exception as exc:  # noqa: BLE001
    logger.error("Verification of control schema '%s' failed: %s", CONTROL_SCHEMA, exc)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preflight Validation UC Function (Genie / MCP tool)
# MAGIC
# MAGIC Deploys `preflight_check_onboarding_spec` as a genuine Unity Catalog Python Function in
# MAGIC `<catalog>.config` -- SQL/Genie/MCP-tool-callable with no Python host process required.
# MAGIC It validates a candidate onboarding spec against
# MAGIC `onboarding_templates/onboarding_spec.schema.json` (structural-only; see
# MAGIC `control_plane/ddl_definitions.py::get_preflight_function_ddl` for exactly why, and
# MAGIC `onboarding/uc_spec_preflight.py` for the complete, live-Unity-Catalog-checking sibling
# MAGIC tool meant to be called from a Python/Spark host process instead).
# MAGIC
# MAGIC Example, once deployed:
# MAGIC ```sql
# MAGIC SELECT flowx.config.preflight_check_onboarding_spec(:spec_text, 'flowx');
# MAGIC ```

# COMMAND ----------

_this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
_schema_file_path = os.path.abspath(os.path.join(_this_dir, "..", "..", "onboarding_templates", "onboarding_spec.schema.json"))
with open(_schema_file_path, "r", encoding="utf-8") as _schema_file:
    ONBOARDING_SPEC_SCHEMA_JSON = _schema_file.read()

execute_ddl(
    get_preflight_function_ddl(CONTROL_SCHEMA, ONBOARDING_SPEC_SCHEMA_JSON),
    f"create function {CONTROL_SCHEMA}.preflight_check_onboarding_spec",
)
