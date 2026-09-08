# Databricks notebook source
# MAGIC %md
# MAGIC # Onboarding & Perception Engine
# MAGIC
# MAGIC Thin orchestration notebook: reads a declarative onboarding spec (JSON), delegates to
# MAGIC `flowx.lakeflow_framework.onboarding.*` for templating,
# MAGIC validation, client-context capture, metadata upsert, and audit logging. Run as a
# MAGIC Databricks Job/Workflow task, parameterized with the widgets below (e.g. from a CI/CD
# MAGIC pipeline onboarding a new `dataflow_group_id`).
# MAGIC
# MAGIC Two behaviors worth calling out:
# MAGIC
# MAGIC * **Self-provisioning**: if the `config` schema/control tables don't exist yet in the
# MAGIC   target catalog (e.g. a brand-new environment nobody has run `01_setup` against),
# MAGIC   this notebook creates them itself before validating/upserting -- see step 1 below.
# MAGIC * **Exhaustive validation**: every mandatory field, allowed value, and type mismatch in
# MAGIC   the spec is checked and reported *all at once* (not one-error-at-a-time), each with a
# MAGIC   fully-qualified path and a plain-English explanation of what was wrong -- see step 2.

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
logger = logging.getLogger("onboarding_engine")

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

from flowx.lakeflow_framework.control_plane.schema_provisioner import (
    ensure_control_schema_exists,  # noqa: E402
)
from flowx.lakeflow_framework.onboarding.spec_pruning import (  # noqa: E402
    deactivate_flows_absent_from_spec,
)
from flowx.lakeflow_framework.exceptions import (  # noqa: E402
    FrameworkError,
    OnboardingValidationError,
)
from flowx.lakeflow_framework.onboarding.audit_logger import write_audit_log_entry  # noqa: E402
from flowx.lakeflow_framework.onboarding.client_context import (
    build_client_context_json,  # noqa: E402
)
from flowx.lakeflow_framework.onboarding.metadata_upsert import (  # noqa: E402
    upsert_dataflow_group_spec,
    upsert_ingestion_flow_spec,
    upsert_observability_config,
    upsert_reconciliation_flow_spec,
    upsert_transformation_flow_spec,
)
from flowx.lakeflow_framework.onboarding.spec_loader import load_and_template_spec  # noqa: E402
from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Constants

# COMMAND ----------

dbutils.widgets.text("spec_file_path", "", "Path to onboarding spec JSON (Workspace Files / UC Volume)")
dbutils.widgets.text("catalog", "poc", "Target Unity Catalog")
dbutils.widgets.text("env", "dev", "Target environment")
dbutils.widgets.dropdown("action_type", "CREATE", ["CREATE", "UPDATE", "VALIDATE_ONLY"], "Onboarding action")
# v1.7.07, opt-in: after the upsert, soft-disable this group's control rows for flows the spec no
# longer declares. Off by default because a reconciliation flow may legitimately belong to a spec
# other than the one owning its dataflow_group_id (see onboarding/spec_pruning.py).
dbutils.widgets.dropdown("prune_missing_flows", "false", ["false", "true"], "Deactivate rows absent from the spec")

SPEC_FILE_PATH = dbutils.widgets.get("spec_file_path").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
ENVIRONMENT = dbutils.widgets.get("env").strip()
ACTION_TYPE = dbutils.widgets.get("action_type").strip()
PRUNE_MISSING_FLOWS = dbutils.widgets.get("prune_missing_flows").strip().lower() == "true"

if not SPEC_FILE_PATH:
    raise ValueError("The 'spec_file_path' widget is required.")

CONTROL_SCHEMA = f"{CATALOG}.config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Self-Provisioning: Ensure Control Tables Exist
# MAGIC
# MAGIC If this is the first onboarding action against a catalog nobody has run
# MAGIC `01_setup_control_tables.py` against yet, create the `config` schema and all four
# MAGIC control tables now (all statements are `CREATE ... IF NOT EXISTS`, so this is a no-op
# MAGIC on an already-provisioned catalog).

# COMMAND ----------

ensure_control_schema_exists(spark, CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load, Template & Validate
# MAGIC
# MAGIC Every mandatory field, allowed value, and type mismatch is collected across the whole
# MAGIC spec before raising -- so a malformed spec gets one complete, actionable error report
# MAGIC instead of a fix-one-field-at-a-time loop. For example, setting a boolean-typed field
# MAGIC (like `is_streaming` or `capture_technical_metadata`) to the string `"abc"` produces:
# MAGIC
# MAGIC ```text
# MAGIC transformation_flow[ts_x].source_inputs[0].is_streaming: expected a boolean
# MAGIC (true/false in JSON), got 'abc' (str). Use the JSON literals `true`/`false`,
# MAGIC not a quoted string.
# MAGIC ```

# COMMAND ----------

spec, templated_spec_text, spec_version = load_and_template_spec(dbutils, SPEC_FILE_PATH, CATALOG, ENVIRONMENT)
ingestion_flows, transformation_flows, reconciliation_flows, observability_destinations, validation_errors = validate_spec(
    spark, spec
)

if validation_errors:
    raise OnboardingValidationError(
        f"Onboarding spec validation failed with {len(validation_errors)} issue(s):\n  - "
        + "\n  - ".join(validation_errors)
    )

logger.info(
    "Spec validated successfully: %d ingestion flow(s), %d transformation flow(s), %d reconciliation flow(s), "
    "%d observability destination(s)",
    len(ingestion_flows),
    len(transformation_flows),
    len(reconciliation_flows),
    len(observability_destinations),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Client Context / Perception Capture

# COMMAND ----------

CLIENT_CONTEXT_JSON = build_client_context_json(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execute Onboarding (Upsert + Audit)

# COMMAND ----------

try:
    if ACTION_TYPE == "VALIDATE_ONLY":
        logger.info("action_type=VALIDATE_ONLY: spec is valid; skipping metadata upsert.")
    else:
        upsert_dataflow_group_spec(
            spark, CONTROL_SCHEMA, spec, ingestion_flows, transformation_flows, CATALOG, ENVIRONMENT
        )
        upsert_ingestion_flow_spec(spark, CONTROL_SCHEMA, spec["dataflow_group_id"], ingestion_flows)
        upsert_transformation_flow_spec(spark, CONTROL_SCHEMA, spec["dataflow_group_id"], transformation_flows)
        upsert_reconciliation_flow_spec(spark, CONTROL_SCHEMA, spec["dataflow_group_id"], reconciliation_flows)
        upsert_observability_config(spark, CONTROL_SCHEMA, spec["dataflow_group_id"], observability_destinations)
        logger.info("Onboarding upsert complete for group '%s'", spec["dataflow_group_id"])
        if PRUNE_MISSING_FLOWS:
            pruned = deactivate_flows_absent_from_spec(
                spark,
                CONTROL_SCHEMA,
                spec["dataflow_group_id"],
                ingestion_flows,
                transformation_flows,
                reconciliation_flows,
            )
            logger.info("prune_missing_flows=true: deactivated rows per table: %s", pruned)
        else:
            logger.info(
                "prune_missing_flows=false: control rows of group '%s' for flows this spec no longer declares "
                "(if any) stay ACTIVE and keep driving the pipeline -- re-run with prune_missing_flows=true "
                "to deactivate them.",
                spec["dataflow_group_id"],
            )

    write_audit_log_entry(
        spark=spark,
        control_schema=CONTROL_SCHEMA,
        dataflow_group_id=spec.get("dataflow_group_id"),
        action_type=ACTION_TYPE,
        environment=ENVIRONMENT,
        onboarded_by=json.loads(CLIENT_CONTEXT_JSON).get("user_principal", "unknown"),
        spec_version=spec_version,
        client_context_json=CLIENT_CONTEXT_JSON,
        status="SUCCESS",
        raw_spec_payload=templated_spec_text,
    )
except FrameworkError as exc:
    logger.error("Onboarding failed for spec '%s': %s", SPEC_FILE_PATH, exc)
    try:
        write_audit_log_entry(
            spark=spark,
            control_schema=CONTROL_SCHEMA,
            dataflow_group_id=spec.get("dataflow_group_id"),
            action_type=ACTION_TYPE,
            environment=ENVIRONMENT,
            onboarded_by=json.loads(CLIENT_CONTEXT_JSON).get("user_principal", "unknown"),
            spec_version=spec_version,
            client_context_json=CLIENT_CONTEXT_JSON,
            status="FAILED",
            raw_spec_payload=templated_spec_text,
            error_message=str(exc),
        )
    except Exception as audit_exc:  # noqa: BLE001
        logger.error("Additionally failed to write audit log entry: %s", audit_exc)
    raise
