# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliation Engine
# MAGIC
# MAGIC Thin orchestration notebook: reads one `reconciliation_flow_spec` control-table row and,
# MAGIC for each of its (possibly several) `target_configs[]` entries, compares the flow's
# MAGIC `source_config` dataset against that target and appends `source_to_target` misses
# MAGIC (missing or drifted records) into the target's own `append_target_table` -- typically the
# MAGIC CDC/Zerobus source table feeding that target's own downstream materialization cycle, so a
# MAGIC subsequent pipeline run picks the correction up. `target_to_source` misses are logged for
# MAGIC audit only and never appended/remediated. All business logic lives in
# MAGIC `flowx.lakeflow_framework.reconciliation` (`dataset_reader`,
# MAGIC `matcher`, `appender`, `mismatch_logging`, `streaming`) -- see
# MAGIC `docs/07_reconciliation.md` for the matching/key strategy and assumptions.
# MAGIC
# MAGIC Restartable/idempotent per target: a `read_mode: "batch"` target's miss-set fingerprint is
# MAGIC checked against `reconciliation_run_log` before appending -- an unchanged fingerprint
# MAGIC short-circuits to `SKIPPED_ALREADY_PROCESSED` rather than appending duplicate correction
# MAGIC rows (see `appender.py`). A `read_mode: "streaming"` target instead relies on Spark
# MAGIC Structured Streaming's own checkpoint for restart safety (see `streaming.py`).
# MAGIC
# MAGIC `error_handling.on_failure` is evaluated **per target**: `"fail"` (the default) propagates
# MAGIC the first target's failure and stops this run; `"warn"` logs it, writes a `FAILED`
# MAGIC `reconciliation_run_log` row for that target, and continues on to the next target.
# MAGIC
# MAGIC **Triggered-only (v1.4.0).** `recon_mode` is removed -- from the spec, the control
# MAGIC table, the validator and this notebook's widgets. Every reconciliation run is a bounded
# MAGIC job task: batch reads, `availableNow`-triggered streaming reads that drain and stop, and,
# MAGIC for any side declaring a `task_run_id_column`, a read narrowed to the `task_run_id` job
# MAGIC parameter's own rows. That narrowing is now unconditional rather than mode-gated.
# MAGIC
# MAGIC The withdrawn `"continuous"` mode wrapped a standing stream around a batch-shaped unit of
# MAGIC work: `run_target_reconciliation` writes one `reconciliation_run_log` row per invocation
# MAGIC and checks a batch fingerprint for idempotency, so a never-ending query produced a log row
# MAGIC per micro-batch whose fingerprint could never repeat. Reconciliation answers "do these two
# MAGIC datasets agree *as of now*", which has a boundary in it. To ask more often, schedule this
# MAGIC job more often.
# MAGIC
# MAGIC **Runtime log controls (v1.3.0; defaults flipped in v1.7.3).** The
# MAGIC `recon_run_log_capture` / `recon_mismatch_log` job parameters are a tri-state
# MAGIC (`""`/`"true"`/`"false"`) runtime layer *over* the flow's own
# MAGIC `logging_config.run_log_capture` / `logging_config.mismatch_log_capture`; blank still
# MAGIC defers to the flow, exactly as before.
# MAGIC
# MAGIC **What changed in v1.7.3:** when the flow's `logging_config` does not set a flag either,
# MAGIC the final fallback is now `false`, not `true` -- reconciliation is **silent by default**.
# MAGIC So a blank widget against a flow with no `logging_config` now writes NOTHING, where
# MAGIC through v1.7.2 it wrote everything. To audit such a run without re-onboarding the flow,
# MAGIC set the widgets to `"true"` explicitly.
# MAGIC
# MAGIC Two earlier claims on this page are now obsolete and have been corrected here:
# MAGIC `reconciliation_result` is **not** written regardless of the flags -- since v1.6.0 it is
# MAGIC gated by the resolved `run_log_capture` exactly like `reconciliation_run_log`, so a
# MAGIC fully-silenced run leaves NO control-table record that it happened; its only signal is
# MAGIC the job/pipeline run state plus the structured log events. See
# MAGIC `appender.py::resolve_log_capture_flags`.
# MAGIC
# MAGIC **Two-tier verification (v1.3.0).** With `two_tier_verification` (default `true`), a cheap
# MAGIC per-side fingerprint -- row count plus order-independent XOR folds of
# MAGIC `__framework_hash_key`/`__framework_hash_value` -- is compared before any join, and an
# MAGIC identical pair short-circuits the whole comparison (Phase 1). The full hash-key join and
# MAGIC column-level discrepancy mapping (Phase 2) run unchanged whenever the fingerprints differ.
# MAGIC See `matcher.py` for the fold's documented pair-cancellation property.
# MAGIC
# MAGIC **Delta tables only (v1.3.0, breaking).** `source_config.type` / `target_configs[].type`
# MAGIC must be `"table"`, and that table must be Delta; the previous `"file"`/`"sink"` types are
# MAGIC rejected. Read a file/sink location into a Delta table first, then reconcile against it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("reconciliation_engine")

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
import uuid  # noqa: E402
from typing import Optional  # noqa: E402

from flowx.lakeflow_framework.control_plane.schema_provisioner import ensure_control_schema_exists  # noqa: E402
from flowx.lakeflow_framework.exceptions import FrameworkConfigError, FrameworkError  # noqa: E402
from flowx.lakeflow_framework.reconciliation.appender import (  # noqa: E402
    resolve_log_capture_flags,
    run_target_reconciliation,
    write_reconciliation_result,
    write_run_log_entry,
)
from flowx.lakeflow_framework.reconciliation.dataset_reader import read_reconciliation_dataset  # noqa: E402
from flowx.lakeflow_framework.reconciliation.matcher import prepare_dataset_for_matching  # noqa: E402
from flowx.lakeflow_framework.reconciliation.streaming import run_streaming_target_reconciliation  # noqa: E402
from flowx.lakeflow_framework.transformation.parameters import substitute_path_parameters  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Constants

# COMMAND ----------

dbutils.widgets.text("catalog", "poc", "Target Unity Catalog")
dbutils.widgets.text("reconciliation_id", "", "reconciliation_flow_spec.reconciliation_id to execute")
dbutils.widgets.text(
    "checkpoint_root",
    "",
    "Root path for streaming target checkpoints (required only if any target_configs[] entry, "
    "or source_config, uses read_mode: 'streaming')",
)
dbutils.widgets.text(
    "task_run_id",
    "",
    "Parent job's own run id ({{job.run_id}} or {{job.parameters.task_run_id}}), threaded into "
    "every reconciliation_run_log/reconciliation_mismatch_log/reconciliation_result row written "
    "this run for correlation. It ALSO narrows any side that declares a task_run_id_column "
    "(typically __framework_pipeline_run_id) to that producing run's rows. Optional -- blank "
    "means a standalone run, same as before this widget existed.",
)
dbutils.widgets.dropdown(
    "recon_run_log_capture",
    "",
    ["", "true", "false"],
    "RUNTIME OVERRIDE of this flow's logging_config.run_log_capture. Tri-state: '' (default) "
    "defers to the flow's own logging_config -- which since v1.7.3 itself defaults to FALSE when "
    "the flow does not set the flag (silent by default; it defaulted to true through v1.7.2). "
    "'true'/'false' force reconciliation_run_log AND reconciliation_result writes on/off for "
    "this run only (v1.6.0: reconciliation_result is gated by this flag too, NOT unconditional).",
)
dbutils.widgets.dropdown(
    "recon_mismatch_log",
    "",
    ["", "true", "false"],
    "RUNTIME OVERRIDE of this flow's logging_config.mismatch_log_capture. Tri-state: '' (default) "
    "defers to the flow's own logging_config -- which since v1.7.3 itself defaults to FALSE when "
    "the flow does not set the flag (silent by default; it defaulted to true through v1.7.2). "
    "'true'/'false' force reconciliation_mismatch_log writes on/off for this run only.",
)


def _resolve_tristate_widget(widget_name: str) -> Optional[bool]:
    """Read a ''/'true'/'false' widget into an Optional[bool].

    Tri-state rather than a plain boolean widget because an operator firefighting a flow that is
    flooding the log tables must be able to *silence* log writes for one run without re-onboarding
    the flow, and must equally be able to leave the decision to the flow's own onboarded metadata
    -- a two-state boolean widget cannot express "I am not expressing an opinion", and would
    silently override logging_config on every single run.
    """
    raw = dbutils.widgets.get(widget_name).strip().lower()
    if raw not in ("", "true", "false"):
        raise FrameworkConfigError(f"'{widget_name}' widget must be '', 'true', or 'false', got {raw!r}")
    return None if raw == "" else raw == "true"


CATALOG = dbutils.widgets.get("catalog").strip()
RECONCILIATION_ID = dbutils.widgets.get("reconciliation_id").strip()
CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root").strip()
TASK_RUN_ID = dbutils.widgets.get("task_run_id").strip() or None
RECON_RUN_LOG_CAPTURE = _resolve_tristate_widget("recon_run_log_capture")
RECON_MISMATCH_LOG = _resolve_tristate_widget("recon_mismatch_log")

if not RECONCILIATION_ID:
    raise ValueError("The 'reconciliation_id' widget is required.")

CONTROL_SCHEMA = f"{CATALOG}.config"
ensure_control_schema_exists(spark, CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load the Reconciliation Flow Spec

# COMMAND ----------

flow_rows = (
    spark.table(f"{CONTROL_SCHEMA}.reconciliation_flow_spec")
    .filter(f"reconciliation_id = '{RECONCILIATION_ID}' AND is_active")
    .collect()
)
if not flow_rows:
    raise FrameworkConfigError(f"No active reconciliation_flow_spec row found for reconciliation_id='{RECONCILIATION_ID}'")
flow_row = flow_rows[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolve Parent Group's Pipeline Parameters (for `${param}` substitution)
# MAGIC
# MAGIC `reconciliation_flow_spec.dataflow_group_id` is optional -- a reconciliation flow need not
# MAGIC belong to any dataflow group. When it does, its parent group's own
# MAGIC `pipeline_parameters_json` is resolved exactly the way `03_lakeflow_declarative_pipeline.py`
# MAGIC resolves it for `transformation_sql`, since `filter_condition`/`transform_sql` reuse the
# MAGIC same `${param}` substitution mechanism (`transformation/parameters.py`).

# COMMAND ----------

PIPELINE_PARAMETERS = {}
if flow_row.dataflow_group_id:
    group_rows = (
        spark.table(f"{CONTROL_SCHEMA}.dataflow_group_spec")
        .filter(f"dataflow_group_id = '{flow_row.dataflow_group_id}'")
        .collect()
    )
    if group_rows and group_rows[0].pipeline_parameters_json:
        PIPELINE_PARAMETERS = json.loads(group_rows[0].pipeline_parameters_json)
    elif not group_rows:
        logger.warning(
            "reconciliation_flow_spec.dataflow_group_id='%s' does not match any dataflow_group_spec row -- "
            "proceeding with no pipeline parameters.",
            flow_row.dataflow_group_id,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse Flow Configuration

# COMMAND ----------

try:
    # ${param} placeholders in source_config/target_configs path fields are resolved fresh
    # from PIPELINE_PARAMETERS on every run -- same mechanism/rationale as
    # 03_lakeflow_declarative_pipeline.py's ingestion/target_config parsing, see
    # transformation/parameters.py::substitute_path_parameters's docstring.
    source_config = json.loads(substitute_path_parameters(flow_row.source_config_json, PIPELINE_PARAMETERS))
    target_configs = json.loads(substitute_path_parameters(flow_row.target_configs_json, PIPELINE_PARAMETERS))
    match_keys = json.loads(flow_row.match_keys_json)
    compare_columns = json.loads(flow_row.compare_columns_json) if flow_row.compare_columns_json else []
    error_handling = json.loads(flow_row.error_handling_json) if flow_row.error_handling_json else {}
    # getattr, not flow_row.logging_config_json: this column may not exist yet on a
    # reconciliation_flow_spec table provisioned before this field was added (01_setup only ever
    # runs CREATE TABLE IF NOT EXISTS, never a migration) -- absent means both flags default to
    # their documented true, same as an empty {} would.
    _logging_config_json = getattr(flow_row, "logging_config_json", None)
    logging_config = json.loads(_logging_config_json) if _logging_config_json else {}
except json.JSONDecodeError as exc:
    raise FrameworkConfigError(f"Reconciliation flow '{RECONCILIATION_ID}': malformed JSON configuration: {exc}") from exc

transform_sql = flow_row.transform_sql
on_failure = error_handling.get("on_failure", "fail")

source_hash_precomputed = bool(source_config.get("hash_precomputed", False))
source_is_streaming = source_config.get("read_mode") == "streaming"

# two_tier_verification defaults to True -- the same column-may-not-exist-yet caveat applies, and
# a NULL in an existing column means "not configured" for the same reason.
_spec_two_tier = getattr(flow_row, "two_tier_verification", None)
TWO_TIER_VERIFICATION = True if _spec_two_tier is None else bool(_spec_two_tier)

# task_run_id plays two distinct roles: it is ALWAYS written to the log/result rows for
# correlation, and it is ADDITIONALLY used to narrow a side's read to one producing run, for any
# side that declares a task_run_id_column (see reconciliation/dataset_reader.py). Both roles now
# take the same value -- the separate TASK_RUN_ID_FILTER existed only to be None in the withdrawn
# continuous mode, which had no bounded run for a single task_run_id to scope.
TASK_RUN_ID_FILTER = TASK_RUN_ID

logger.info(
    "Reconciliation '%s': two_tier_verification=%s, task_run_id=%r (filtering=%s).",
    RECONCILIATION_ID,
    TWO_TIER_VERIFICATION,
    TASK_RUN_ID,
    TASK_RUN_ID_FILTER is not None,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare the Shared Source (batch path only)
# MAGIC
# MAGIC `source_config` is shared across every `target_configs[]` entry. When it is *not* itself
# MAGIC streaming, its hash columns are computed once, here, rather than once per target --
# MAGIC `matcher.py`'s `match_reconciliation_target` reads its prepared source DataFrame twice per
# MAGIC target regardless (Delta's own file skipping keeps each re-read bounded; this framework's
# MAGIC jobs run on serverless compute, which does not support `DataFrame.cache()`/`.persist()` as
# MAGIC a cheaper alternative -- see `matcher.py`'s performance note), so preparing it once here
# MAGIC still avoids repeating the read/filter/standardization/hash step itself once per target.
# MAGIC When `source_config` *is* streaming, every target that reads it goes through `streaming.py`
# MAGIC instead, which reads and prepares the source fresh per micro-batch -- there is nothing to
# MAGIC prepare here in that case.

# COMMAND ----------

prepared_source_df = None
if not source_is_streaming:
    raw_source_df = read_reconciliation_dataset(
        spark, source_config, PIPELINE_PARAMETERS, task_run_id=TASK_RUN_ID_FILTER
    )
    prepared_source_df = prepare_dataset_for_matching(
        raw_source_df, match_keys, compare_columns, source_hash_precomputed
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compare, Append, and Log -- Per Target, Independently

# COMMAND ----------

first_failure = None

for target_config in target_configs:
    target_id = target_config.get("target_id", "<missing target_id>")
    try:
        target_is_streaming = target_config.get("read_mode") == "streaming"

        if source_is_streaming or target_is_streaming:
            if not CHECKPOINT_ROOT:
                raise FrameworkConfigError(
                    f"target_id='{target_id}' requires read_mode: 'streaming' handling (source_config or this "
                    "target is streaming) but the 'checkpoint_root' widget was not set."
                )
            run_streaming_target_reconciliation(
                spark,
                CONTROL_SCHEMA,
                RECONCILIATION_ID,
                source_config,
                target_config,
                match_keys,
                compare_columns,
                transform_sql,
                checkpoint_location=f"{CHECKPOINT_ROOT.rstrip('/')}/{RECONCILIATION_ID}/{target_id}",
                parameters=PIPELINE_PARAMETERS,
                logging_config=logging_config,
                task_run_id=TASK_RUN_ID,
                recon_run_log_capture=RECON_RUN_LOG_CAPTURE,
                recon_mismatch_log=RECON_MISMATCH_LOG,
                two_tier_verification=TWO_TIER_VERIFICATION,
            )
        else:
            target_df = read_reconciliation_dataset(
                spark, target_config, PIPELINE_PARAMETERS, task_run_id=TASK_RUN_ID_FILTER
            )
            run_target_reconciliation(
                spark,
                CONTROL_SCHEMA,
                RECONCILIATION_ID,
                prepared_source_df,
                target_df,
                target_config,
                match_keys,
                compare_columns,
                source_hash_precomputed,
                transform_sql,
                parameters=PIPELINE_PARAMETERS,
                logging_config=logging_config,
                task_run_id=TASK_RUN_ID,
                recon_run_log_capture=RECON_RUN_LOG_CAPTURE,
                recon_mismatch_log=RECON_MISMATCH_LOG,
                two_tier_verification=TWO_TIER_VERIFICATION,
            )
    except FrameworkError as exc:
        logger.error("Reconciliation '%s'/target '%s' failed: %s", RECONCILIATION_ID, target_id, exc)
        _failed_run_id = str(uuid.uuid4())
        try:
            # Same precedence the happy path uses (job parameter over per-flow logging_config over
            # True) -- resolved through the one shared helper so a run whose logs were silenced by
            # the recon_run_log_capture parameter cannot suddenly start writing them on failure.
            _failed_run_log_capture, _ = resolve_log_capture_flags(
                logging_config, RECON_RUN_LOG_CAPTURE, RECON_MISMATCH_LOG
            )
            if _failed_run_log_capture:
                write_run_log_entry(
                    spark,
                    CONTROL_SCHEMA,
                    RECONCILIATION_ID,
                    target_id,
                    run_id=_failed_run_id,
                    fingerprint="unknown",
                    status="FAILED",
                    error_message=str(exc),
                    task_run_id=TASK_RUN_ID,
                )
        except Exception as log_exc:  # noqa: BLE001
            logger.error("Additionally failed to write reconciliation_run_log entry: %s", log_exc)
        try:
            # v1.6.0: reconciliation_result is gated by run_log_capture too -- with logging
            # suppressed, reconciliation persists to NOTHING but its business targets. The
            # job run's own FAILED state (this task re-raises below) and the structured log
            # events remain the failure signal for a suppressed-logging flow.
            if _failed_run_log_capture:
                write_reconciliation_result(
                    spark, CONTROL_SCHEMA, RECONCILIATION_ID, target_id, run_id=_failed_run_id, status="FAILED", task_run_id=TASK_RUN_ID
                )
        except Exception as log_exc:  # noqa: BLE001
            logger.error("Additionally failed to write reconciliation_result entry: %s", log_exc)

        if on_failure == "fail":
            first_failure = exc
            break
        logger.warning("error_handling.on_failure='warn': suppressing failure for target '%s', continuing.", target_id)

# COMMAND ----------

if first_failure is not None:
    raise first_failure

logger.info("Reconciliation '%s' complete for %d target(s).", RECONCILIATION_ID, len(target_configs))
