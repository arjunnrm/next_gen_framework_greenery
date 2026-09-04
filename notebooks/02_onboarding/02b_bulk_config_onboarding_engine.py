# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Config Onboarding Engine (generic)
# MAGIC
# MAGIC Onboards **every** spec in a directory in one run, rather than one spec per run like its
# MAGIC sibling `02_onboarding_engine.py`. Entirely generic -- point `spec_dir` at any folder of
# MAGIC onboarding specs (JSON or YAML). It is *not* test-specific; the test corpus
# MAGIC (`flowx_testing/`) is simply its default target because that is the directory most
# MAGIC often onboarded wholesale.
# MAGIC
# MAGIC Like every notebook in this repo it is a **thin orchestration layer**: discovery,
# MAGIC iteration and reporting live in
# MAGIC `flowx.lakeflow_framework.onboarding.bulk_onboarding`, and each
# MAGIC individual spec goes through the exact same `load_and_template_spec` -> `validate_spec`
# MAGIC -> `upsert_*` -> `write_audit_log_entry` path `02_onboarding_engine.py` uses. Nothing
# MAGIC about onboarding *semantics* differs between the two notebooks.
# MAGIC
# MAGIC **Fail-soft by default.** Every spec is attempted even after one fails; the run raises at
# MAGIC the very end if anything failed. That makes one run tell you about *all* the broken specs
# MAGIC instead of just the first -- the same "collect every error, report once" principle
# MAGIC `validate_spec` applies within a single spec, applied here across specs. Set
# MAGIC `fail_fast=true` for a gating CI step that should abort immediately.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap
# MAGIC
# MAGIC In production `flowx` is installed as a wheel attached to this job
# MAGIC (see `resources/flowx_config_jobs/framework_config_onboarding_job.yml`), so a plain `import` resolves from
# MAGIC site-packages. The fallback below only kicks in for local, wheel-less notebook
# MAGIC development.

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bulk_config_onboarding_engine")

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
from flowx.lakeflow_framework.exceptions import OnboardingValidationError  # noqa: E402
from flowx.lakeflow_framework.onboarding.bulk_onboarding import (  # noqa: E402
    ALLOWED_ACTION_TYPES,
    discover_spec_files,
    format_results_table,
    onboard_single_spec,
    summarize_results,
)
from flowx.lakeflow_framework.onboarding.client_context import (
    build_client_context_json,  # noqa: E402
)
from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC
# MAGIC `spec_dir` is the only one that usually needs setting. `exclude_specs` exists for
# MAGIC deliberate negative-test specs (e.g. `048_prm_006_negative_param.json`, which is
# MAGIC *designed* to fail validation) that would otherwise show up as false failures in the
# MAGIC report.

# COMMAND ----------

dbutils.widgets.text("spec_dir", "", "Directory of onboarding specs (Workspace Files / UC Volume)")
dbutils.widgets.text("catalog", "flowx", "Target Unity Catalog")
dbutils.widgets.text("env", "dev", "Target environment")
dbutils.widgets.dropdown("action_type", "CREATE", ["CREATE", "UPDATE", "VALIDATE_ONLY"], "Onboarding action")
dbutils.widgets.dropdown("fail_fast", "false", ["true", "false"], "Abort on first failure")
dbutils.widgets.text("exclude_specs", "", "Comma-separated spec file names to skip")

SPEC_DIR = dbutils.widgets.get("spec_dir").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
ENVIRONMENT = dbutils.widgets.get("env").strip()
ACTION_TYPE = dbutils.widgets.get("action_type").strip()
FAIL_FAST = dbutils.widgets.get("fail_fast").strip().lower() == "true"
EXCLUDE_SPECS = [name.strip() for name in dbutils.widgets.get("exclude_specs").split(",") if name.strip()]

if not SPEC_DIR:
    raise ValueError("The 'spec_dir' widget is required.")
if ACTION_TYPE not in ALLOWED_ACTION_TYPES:
    raise ValueError(f"action_type must be one of {sorted(ALLOWED_ACTION_TYPES)}, got '{ACTION_TYPE}'")

CONTROL_SCHEMA = f"{CATALOG}.config"

logger.info(
    "Bulk onboarding: spec_dir='%s' catalog='%s' env='%s' action_type='%s' fail_fast=%s excluding=%s",
    SPEC_DIR, CATALOG, ENVIRONMENT, ACTION_TYPE, FAIL_FAST, EXCLUDE_SPECS or "nothing",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Self-Provisioning: Ensure Control Tables Exist
# MAGIC
# MAGIC Done once for the whole run rather than per spec -- every statement is
# MAGIC `CREATE ... IF NOT EXISTS`, so repeating it 48 times would be 47 no-ops.

# COMMAND ----------

ensure_control_schema_exists(spark, CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Discover Specs

# COMMAND ----------

spec_paths = discover_spec_files(dbutils, SPEC_DIR, exclude_names=EXCLUDE_SPECS)
print(f"Discovered {len(spec_paths)} spec file(s) in {SPEC_DIR}:")
for path in spec_paths:
    print(f"  - {os.path.basename(path)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Client Context / Perception Capture
# MAGIC
# MAGIC Captured once and reused for every spec's audit row -- it describes *who ran this bulk
# MAGIC onboarding*, which is identical across the run.

# COMMAND ----------

CLIENT_CONTEXT_JSON = build_client_context_json(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Onboard Every Spec

# COMMAND ----------

results = []
for index, spec_path in enumerate(spec_paths, start=1):
    logger.info("[%d/%d] %s", index, len(spec_paths), os.path.basename(spec_path))
    result = onboard_single_spec(
        spark=spark,
        dbutils=dbutils,
        spec_path=spec_path,
        catalog=CATALOG,
        environment=ENVIRONMENT,
        action_type=ACTION_TYPE,
        control_schema=CONTROL_SCHEMA,
        client_context_json=CLIENT_CONTEXT_JSON,
        validate_spec_fn=validate_spec,
    )
    results.append(result)

    if FAIL_FAST and result["status"] != "SUCCESS":
        print(format_results_table(results))
        raise OnboardingValidationError(
            f"fail_fast=true and spec '{result['spec_file']}' failed: {result['error']}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Report

# COMMAND ----------

summary = summarize_results(results)

print(format_results_table(results))
print()
print("=" * 96)
print(
    f"BULK ONBOARDING SUMMARY  action={ACTION_TYPE}  catalog={CATALOG}  env={ENVIRONMENT}\n"
    f"  specs: {summary['succeeded']}/{summary['total']} succeeded, {summary['failed']} failed\n"
    f"  flows onboarded: {summary['total_ingestion_flows']} ingestion, "
    f"{summary['total_transformation_flows']} transformation, "
    f"{summary['total_reconciliation_flows']} reconciliation, "
    f"{summary['total_observability_destinations']} observability destination(s)"
)
print("=" * 96)

for failed in [r for r in results if r["status"] != "SUCCESS"]:
    print(f"\n--- FAILED: {failed['spec_file']} ---\n{failed['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exit
# MAGIC
# MAGIC The full result payload is returned via `dbutils.notebook.exit` so an orchestrating job
# MAGIC (or `databricks jobs get-run-output`) can consume the per-spec outcome programmatically,
# MAGIC not just read it out of the driver log. The run is then failed if any spec failed --
# MAGIC after the report has been printed, never before it.

# COMMAND ----------

if summary["failed"]:
    raise OnboardingValidationError(
        f"Bulk onboarding completed with {summary['failed']} failed spec(s): "
        f"{', '.join(summary['failed_specs'])}. See the per-spec detail above."
    )

dbutils.notebook.exit(json.dumps({"summary": summary, "results": results}))
