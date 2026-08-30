# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Observability Engine
# MAGIC
# MAGIC Runs as a downstream Workflow task, chained *after* the DLT pipeline update task
# MAGIC (conventionally ``task_key: run_pipeline_update`` -- see
# MAGIC ``resources/dlt_observability_job.yml``). Takes that task's own ``run_id`` (via the
# MAGIC ``pipeline_task_run_id`` parameter, set to ``{{tasks.<pipeline_task_key>.run_id}}``),
# MAGIC resolves its execution window, pulls the DLT event log for that window, aggregates it per
# MAGIC ``dataflow_group_id``, builds strict OTel ``ResourceLogs`` payloads, and dispatches them to
# MAGIC every enabled destination configured in ``<catalog>.config.observability_config``.
# MAGIC
# MAGIC **Four required parameters (v1.4.0).** `dataflow_group_id`, `catalog`, `env` and
# MAGIC `pipeline_task_run_id` are all mandatory, validated together by
# MAGIC `observability/runtime_params.py` before a single API call is made. Two of them were
# MAGIC previously derived or optional and are now declared:
# MAGIC
# MAGIC - `dataflow_group_id` is still *also* read back off the upstream pipeline's own
# MAGIC   `dataflow.group.id`, but now as a **cross-check** -- a derived value cannot tell you the
# MAGIC   task is wired to the wrong pipeline, because whatever pipeline it lands on reports its
# MAGIC   own group id happily. A disagreement is a hard error naming both.
# MAGIC - `env` replaces the optional `deployment_environment` widget and becomes the OTel
# MAGIC   `deployment.environment` resource attribute. Optional environment labelling silently
# MAGIC   merges dev telemetry into prod's in the consumer.
# MAGIC
# MAGIC **`pipeline_task_run_id` is a task parameter, never a pipeline parameter.**
# MAGIC `{{tasks.<key>.run_id}}` is a Jobs *dynamic value reference*, substituted per job run at
# MAGIC the moment this task is dispatched. A pipeline's `pipeline_parameters` / `configuration:`
# MAGIC block is resolved per *pipeline update* and is static for the life of that deployment --
# MAGIC there is no job run in scope for it to resolve a task value against. Declaring it there
# MAGIC cannot work; see `docs/08_observability_and_telemetry.md`.
# MAGIC
# MAGIC **This is the `triggered`-mode engine, and only that.** A destination row's `mode`
# MAGIC column decides which of the two observability engines serves it: `triggered` (this
# MAGIC notebook -- bounded, one pipeline, runs once after an update finishes) or `continuous`
# MAGIC (`notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py` -- an
# MAGIC always-on `continuous: true` pipeline streaming N event-log tables at once). Mode is
# MAGIC selected by *which notebook runs*, not by a switch inside one entrypoint, because the two
# MAGIC have fundamentally different lifecycles; the `obs_mode` widget below exists purely to
# MAGIC make a mis-wired job fail loudly instead of silently exporting the wrong destination set.
# MAGIC A pre-v1.3.0 `observability_config` row has no `mode` value at all and resolves to
# MAGIC `triggered`, so this notebook's destination set is unchanged for every existing
# MAGIC deployment.
# MAGIC
# MAGIC All business logic lives in
# MAGIC `NextGen_Metadata_Framework.lakeflow_framework.observability` (see that package's
# MAGIC `__init__.py` for the 5-module breakdown) -- this notebook is deliberately thin
# MAGIC orchestration, per this repo's own convention (`AGENTS.md`/`SKILL.md`).
# MAGIC
# MAGIC See `docs/25_dlt_observability_module.md` for the full architecture writeup and
# MAGIC `docs/26_dlt_observability_testing_runbook.md` for how to exercise this end to end.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dlt_observability_engine")

try:
    import NextGen_Metadata_Framework.lakeflow_framework  # noqa: F401
except ImportError:
    try:
        this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        dev_src_root = os.path.abspath(os.path.join(this_dir, "..", "..", "src"))
        if dev_src_root not in sys.path:
            sys.path.insert(0, dev_src_root)
        import NextGen_Metadata_Framework.lakeflow_framework  # noqa: F401
        logger.warning("Loaded 'NextGen_Metadata_Framework' from local 'src/' (dev fallback) -- not from an installed wheel.")
    except ImportError as exc:
        raise ImportError(
            "Could not import 'NextGen_Metadata_Framework'. In production this must be attached as a "
            "wheel library (see resources/*.yml); for local development, run from within the repo so "
            f"'../../src' resolves. Original error: {exc}"
        ) from exc

from databricks.sdk import WorkspaceClient  # noqa: E402

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import (  # noqa: E402
    ObservabilityConfigError,
    ObservabilityDispatchError,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.config_loader import (  # noqa: E402
    filter_destinations_by_mode,
    load_destination_configs,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.destination_dispatcher import (
    dispatch_all,  # noqa: E402
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.event_log_extractor import (  # noqa: E402
    aggregate_flow_metrics,
    extract_raw_events,
    resolve_dataflow_group_id,
    resolve_update_ids_for_window,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.otel_payload_builder import (  # noqa: E402
    build_resource_logs,
    validate_resource_logs,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.runtime_params import (  # noqa: E402
    assert_dataflow_group_id_matches,
    resolve_triggered_run_parameters,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.structured_logger import logged_operation  # noqa: E402
from NextGen_Metadata_Framework.lakeflow_framework.observability.task_context_resolver import (
    resolve_task_context,  # noqa: E402
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Constants

# COMMAND ----------

# The four REQUIRED parameters. Declared with empty defaults so a missing one arrives as ""
# and is reported by resolve_triggered_run_parameters alongside every other missing one, rather
# than defaulting to something plausible and exporting under it.
dbutils.widgets.text("dataflow_group_id", "", "Dataflow group this export is about (REQUIRED)")
dbutils.widgets.text("catalog", "", "Control catalog -- observability_config lives in <catalog>.config (REQUIRED)")
dbutils.widgets.text("env", "", "Deployment environment, e.g. dev/uat/prod -- OTel deployment.environment (REQUIRED)")
dbutils.widgets.text("pipeline_task_run_id", "", "{{tasks.<pipeline_task_key>.run_id}} (REQUIRED)")
# Optional, with real defaults.
dbutils.widgets.text("service_name", "dlt-observability", "OTel service.name resource attribute default")
dbutils.widgets.dropdown("fail_task_on_dispatch_error", "true", ["true", "false"], "Fail this task if any destination dispatch fails")
dbutils.widgets.dropdown("obs_mode", "triggered", ["triggered", "continuous"], "Observability mode served by this notebook (must be 'triggered')")
dbutils.widgets.dropdown("narrow_to_task_updates", "true", ["true", "false"], "Narrow the event_log query to the update(s) this task produced")

SERVICE_NAME = dbutils.widgets.get("service_name").strip() or "dlt-observability"
FAIL_ON_DISPATCH_ERROR = dbutils.widgets.get("fail_task_on_dispatch_error").strip().lower() == "true"
OBS_MODE = dbutils.widgets.get("obs_mode").strip().lower() or "triggered"
NARROW_TO_TASK_UPDATES = dbutils.widgets.get("narrow_to_task_updates").strip().lower() == "true"

# One call validates all four and reports every problem at once -- missing values, a
# pipeline_task_run_id left as an unresolved "{{tasks...}}" literal (what a mistyped task_key
# actually produces: the Jobs service substitutes nothing and passes the text through), and a
# non-numeric run id. This runs BEFORE the WorkspaceClient is constructed and before any API
# call, table read or dispatch, so a mis-wired job fails in a second rather than part-way through
# an export. See observability/runtime_params.py.
RUN_PARAMS = resolve_triggered_run_parameters(
    {name: dbutils.widgets.get(name) for name in ("dataflow_group_id", "catalog", "env", "pipeline_task_run_id")}
)
CATALOG = RUN_PARAMS.catalog
UPSTREAM_RUN_ID = RUN_PARAMS.pipeline_task_run_id
DECLARED_DATAFLOW_GROUP_ID = RUN_PARAMS.dataflow_group_id
DEPLOYMENT_ENVIRONMENT = RUN_PARAMS.env

# Fail fast, before a single API call: running this bounded, post-update engine in "continuous"
# mode is a job-wiring mistake with no sensible interpretation -- it cannot be honoured by
# degrading to something, because the continuous destination set names event_log_tables this
# notebook has no streaming graph to read them with.
if OBS_MODE != "triggered":
    raise ObservabilityConfigError(
        "The 'obs_mode' widget must be 'triggered' for this notebook -- continuous-mode destinations are served "
        "by notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py, which has a "
        "fundamentally different (always-on) lifecycle."
    )

workspace_client = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Context Resolution

# COMMAND ----------

with logged_operation("observability_context_resolution", flow_id=DECLARED_DATAFLOW_GROUP_ID) as op:
    task_context = resolve_task_context(workspace_client, UPSTREAM_RUN_ID)
    # Derived value is now a cross-check, not the source of truth: the DECLARED group id wins,
    # and a disagreement raises rather than quietly filing one group's telemetry under another's
    # name. A pipeline that declares no dataflow.group.id at all is not an error -- the
    # cross-check simply has nothing to check, and says so at INFO.
    dataflow_group_id = assert_dataflow_group_id_matches(
        DECLARED_DATAFLOW_GROUP_ID,
        resolve_dataflow_group_id(workspace_client, task_context.pipeline_id),
        task_context.pipeline_id,
    )
    # Narrowed to mode='triggered' -- a continuous destination is served by the streaming
    # pipeline instead, and dispatching to it from here too would double-export it. Rows written
    # before v1.3.0 carry no mode and resolve to 'triggered', so nothing existing is dropped.
    all_destinations = load_destination_configs(spark, CATALOG, dataflow_group_id)
    destinations = filter_destinations_by_mode(all_destinations, "triggered")

    logger.info(
        "Context resolved: pipeline_task_run_id=%s -> pipeline_id=%s, dataflow_group_id=%s, window=[%d, %d], "
        "%d of %d active destination(s) served by this triggered engine: %s (continuous, skipped here: %s)",
        task_context.upstream_task_run_id,
        task_context.pipeline_id,
        dataflow_group_id,
        task_context.start_time_ms,
        task_context.end_time_ms,
        len(destinations),
        len(all_destinations),
        [d.destination_id for d in destinations],
        [d.destination_id for d in all_destinations if d.mode != "triggered"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extraction
# MAGIC
# MAGIC The task run's wall clock is the outer bound, but a pipeline can legitimately have run
# MAGIC more than one update inside it (a retry, or a manually-started update racing the
# MAGIC scheduled one). `resolve_update_ids_for_window` asks the Pipelines API which updates this
# MAGIC pipeline actually created inside the window, so the event-log query is narrowed to
# MAGIC exactly the update(s) this task produced. It is **best-effort**: when it cannot resolve
# MAGIC anything it returns `None`, and `extract_raw_events` falls back to the
# MAGIC timestamp-window-only query that has always been used -- narrowing improves precision,
# MAGIC it is never a prerequisite for exporting. The `narrow_to_task_updates` widget turns it off
# MAGIC entirely for an operator who wants the pre-v1.3.0 behaviour back without a code change.

# COMMAND ----------

with logged_operation("observability_extraction", flow_id=dataflow_group_id, pipeline_id=task_context.pipeline_id) as op:
    update_ids = (
        resolve_update_ids_for_window(
            workspace_client, task_context.pipeline_id, task_context.start_time_ms, task_context.end_time_ms
        )
        if NARROW_TO_TASK_UPDATES
        else None
    )
    extraction = extract_raw_events(
        spark,
        task_context.pipeline_id,
        task_context.start_time_ms,
        task_context.end_time_ms,
        update_ids=update_ids,
    )
    op.records_read = len(extraction["events"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Metric Aggregation & OTel Structuring (Transformation)

# COMMAND ----------

with logged_operation("observability_transformation", flow_id=dataflow_group_id) as op:
    telemetry = aggregate_flow_metrics(
        extraction["events"], dataflow_group_id, task_context.pipeline_id, task_context.start_time_ms, task_context.end_time_ms
    )
    resource_logs = build_resource_logs(
        telemetry,
        job_context={"job_id": task_context.job_id, "task_run_id": task_context.upstream_task_run_id, "pipeline_config": {}},
        service_name=SERVICE_NAME,
        deployment_environment=DEPLOYMENT_ENVIRONMENT,
    )
    validate_resource_logs(resource_logs)
    op.records_written = len(resource_logs)

    logger.info(
        "Transformed dataflow_group_id='%s': %d flow(s), %d update(s), %d ResourceLogs entr(y/ies), "
        "%d DQ expectation(s) evaluated, %d pipeline-level error(s)",
        dataflow_group_id,
        len(telemetry.flows),
        len(telemetry.updates),
        len(resource_logs),
        sum(len(f.expectations) for f in telemetry.flows),
        len(telemetry.pipeline_level_errors),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Telemetry Dispatch

# COMMAND ----------


def _secret_resolver(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


try:
    with logged_operation("observability_dispatch_all", flow_id=dataflow_group_id) as op:
        results = dispatch_all(
            resource_logs, destinations, dataflow_group_id, task_context.upstream_task_run_id, secret_resolver=_secret_resolver
        )
        op.records_written = sum(1 for r in results if r.status == "SUCCESS")
        op.records_rejected = sum(1 for r in results if r.status == "FAILED")

    failed = [r for r in results if r.status == "FAILED"]
    if failed and FAIL_ON_DISPATCH_ERROR:
        raise ObservabilityDispatchError(
            f"{len(failed)}/{len(results)} destination(s) failed for dataflow_group_id='{dataflow_group_id}': "
            f"{[(r.destination_id, r.error) for r in failed]}"
        )
    elif failed:
        logger.warning(
            "%d/%d destination(s) failed for dataflow_group_id='%s' (fail_task_on_dispatch_error=false, continuing): %s",
            len(failed), len(results), dataflow_group_id, [(r.destination_id, r.error) for r in failed],
        )

    logger.info("Observability dispatch complete for dataflow_group_id='%s': %d/%d succeeded", dataflow_group_id, len(results) - len(failed), len(results))
except Exception as exc:  # noqa: BLE001
    logger.error("Observability engine failed for dataflow_group_id='%s': %s", dataflow_group_id, exc)
    raise
