"""Per-target reconciliation orchestration: match, append, log -- one target at a time.

:func:`run_target_reconciliation` is the single entrypoint ``05_reconciliation_engine.py``
calls once per ``target_configs[]`` entry; every other function in this module is a building
block it composes (also independently unit tested and independently reusable from
``reconciliation/streaming.py``'s ``foreachBatch`` handler, which calls the same building
blocks per micro-batch instead of once per whole table).

**Restartability strategy (``read_mode: "batch"`` only -- see ``streaming.py`` for the
streaming counterpart).** Every run computes a deterministic fingerprint of this target's
``source_to_target`` miss set (entirely-missing + drifted records). Before appending, this
target's slice of ``reconciliation_run_log`` is checked for a prior ``SUCCESS`` run with that
exact fingerprint -- if found, this run is a no-op (``SKIPPED_ALREADY_PROCESSED``), so
re-running reconciliation against an unchanged source snapshot never appends duplicate
correction rows into ``append_target_table``.

**Direction gating.** ``matcher.py`` always classifies every record (MATCHED/MISSING_IN_TARGET/
MISSING_IN_SOURCE/VALUE_DRIFT) in one pass regardless of this target's configured
``comparison_direction`` -- computing all four is effectively free once the join has already
run (see ``matcher.py``'s docstring). What ``comparison_direction`` actually gates, here, is
*action*: ``source_to_target``/``both`` appends the miss set and mismatch-logs
MISSING_IN_TARGET/VALUE_DRIFT; ``target_to_source``/``both`` mismatch-logs MISSING_IN_SOURCE
(never appends, never mutates the target -- audit-only by explicit design). A target configured
``source_to_target``-only still gets its ``missing_in_source_count`` populated in
``reconciliation_run_log`` (it's part of the same free aggregation), but no MISSING_IN_SOURCE
rows are written to ``reconciliation_mismatch_log`` for it -- writing an audit trail for a
direction the flow author explicitly didn't ask to audit would be misleading, not merely extra.

**Two-tier verification (v1.3.0).** ``run_target_reconciliation`` now runs ``matcher.py``'s cheap
Phase 1 fingerprint on both sides *before* the join, and returns early -- with a full, honest
``reconciliation_run_log``/``reconciliation_result`` row -- when the two fingerprints agree. Phase
2 (the join, the per-key collapse, the classification, the mismatch detail) is completely
unchanged and runs whenever the fingerprints disagree or the flow sets
``two_tier_verification: false``. The early-out is exactly that: an early-out. It never changes
what Phase 2 computes, and it can never *report* a difference of its own -- see ``matcher.py``'s
module docstring for the XOR fold's documented pair-cancellation property and why the asymmetry
matters.

**Two layers of log control.** ``logging_config.run_log_capture``/``mismatch_log_capture`` are the
onboarded, per-flow layer; the ``recon_run_log_capture``/``recon_mismatch_log`` job parameters
(``05_reconciliation_engine.py``'s widgets) are the runtime layer over them, for an operator who
needs to silence log writes for one run without re-onboarding the flow.
:func:`resolve_log_capture_flags` is the single place that precedence is decided.
"""

import datetime
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from NextGen_Metadata_Framework.lakeflow_framework.cdc.hashing import (
    HASH_KEY_COLUMN,
    assemble_xor_folded_digest,
    deterministic_hash_expression,
    xor_fold_hex_digest,
)
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.observability.structured_logger import log_flow_event
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.matcher import (
    MISMATCH_TYPE_COLUMN,
    MISMATCH_TYPE_MISSING_IN_SOURCE,
    MISMATCH_TYPE_MISSING_IN_TARGET,
    MISMATCH_TYPE_VALUE_DRIFT,
    PHASE_1_MATCH,
    PHASE_2_DETAIL,
    ReconciliationFingerprint,
    compute_side_fingerprint,
    fingerprints_match,
    match_reconciliation_target,
    prepare_dataset_for_matching,
)
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.metrics import ReconciliationMetrics
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.mismatch_logging import write_mismatch_log_rows
from NextGen_Metadata_Framework.lakeflow_framework.transformation.parameters import substitute_dynamic_parameters

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.reconciliation.appender")

# transform_sql's documented FROM source -- see apply_transform_sql. Fixed (not per-run-unique)
# because targets within one reconciliation_id are processed sequentially by
# 05_reconciliation_engine.py's own loop, never concurrently within one Spark session.
UNMATCHED_RECORDS_VIEW_NAME = "_reconciliation_unmatched_records"

_RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("reconciliation_id", StringType(), nullable=False),
        StructField("target_id", StringType(), nullable=False),
        StructField("source_batch_fingerprint", StringType(), nullable=False),
        StructField("source_record_count", LongType(), nullable=True),
        StructField("target_record_count", LongType(), nullable=True),
        StructField("matched_count", LongType(), nullable=True),
        StructField("missing_in_target_count", LongType(), nullable=True),
        StructField("missing_in_source_count", LongType(), nullable=True),
        StructField("value_drift_count", LongType(), nullable=True),
        StructField("appended_count", LongType(), nullable=True),
        StructField("failed_count", LongType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("error_message", StringType(), nullable=True),
        StructField("task_run_id", StringType(), nullable=True),
        StructField("run_at", TimestampType(), nullable=False),
    ]
)

_RESULT_SCHEMA = StructType(
    [
        StructField("result_id", StringType(), nullable=False),
        StructField("run_id", StringType(), nullable=False),
        StructField("reconciliation_id", StringType(), nullable=False),
        StructField("target_id", StringType(), nullable=False),
        StructField("task_run_id", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("matched_count", LongType(), nullable=True),
        StructField("missing_in_target_count", LongType(), nullable=True),
        StructField("missing_in_source_count", LongType(), nullable=True),
        StructField("value_drift_count", LongType(), nullable=True),
        StructField("run_at", TimestampType(), nullable=False),
    ]
)


#: Intermediate column name for the per-row digest :func:`compute_batch_fingerprint` folds.
#: Named in the framework's own reserved namespace so it can never collide with a real
#: ``match_keys`` column projected alongside it.
_ROW_HASH_COLUMN = "__framework_fingerprint_row_hash"


def compute_batch_fingerprint(df: DataFrame, match_keys: List[str]) -> str:
    """Deterministic sha256-style hash of ``df``'s distinct ``match_keys`` value combinations.

    Computed entirely in Spark, never pulling the key set itself to the driver: each distinct
    match-key combination is hashed with ``cdc/hashing.py::deterministic_hash_expression`` on the
    workers -- the framework's single canonical construction, the same one that produces
    ``__framework_hash_key``/``__framework_hash_value`` -- and the
    resulting (arbitrarily many) per-row digests are folded into one fixed-size, 64-character hex
    string via ``cdc/hashing.py::xor_fold_hex_digest``: XOR is commutative and associative, so the
    result does not depend on row or partition order (no ``orderBy`` needed), and ``F.bit_xor``
    never materializes more than one aggregate row on any executor or the driver, regardless of
    how many distinct keys ``df`` has -- unlike the pre-redesign
    ``.distinct().orderBy(...).collect()``, which pulled every distinct key combination into the
    driver's Python heap (the actual OOM risk this replaces at 500K-5M+ keys). An empty distinct
    key set folds to a well-defined ``"0" * 64`` rather than raising or returning ``None``.

    **v1.3.0:** the hash expression and the XOR fold both moved to ``cdc/hashing.py`` (E08);
    this function no longer carries its own copies of either. That is what makes it *the same*
    hash as ``__framework_hash_key`` rather than a third near-identical construction that had
    already drifted (the old local copy used a ``" NULL "`` sentinel a real source value can
    spell, and no ``trim``/``lower`` normalization). Signature and return shape are unchanged;
    the digest **values** change, so a pre-upgrade ``reconciliation_run_log.source_batch_fingerprint``
    will not match a post-upgrade one and the first run after the upgrade cannot short-circuit to
    ``SKIPPED_ALREADY_PROCESSED`` -- documented, one-time, and harmless (an empty miss set still
    appends nothing).

    Raises
    ------
    FrameworkConfigError
        If computing the fingerprint fails (e.g. an unreadable source table, or a
        ``match_keys`` column that doesn't exist on ``df``).
    """
    try:
        distinct_hashes = (
            df.select(*match_keys).distinct().select(deterministic_hash_expression(match_keys).alias(_ROW_HASH_COLUMN))
        )
        lane_row = distinct_hashes.agg(*xor_fold_hex_digest(F.col(_ROW_HASH_COLUMN))).collect()[0]
        return assemble_xor_folded_digest(list(lane_row))
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to compute reconciliation batch fingerprint: {exc}") from exc


def is_target_batch_already_processed(
    spark: SparkSession, control_schema: str, reconciliation_id: str, target_id: str, fingerprint: str
) -> bool:
    """Check whether this exact target's miss set was already successfully appended.

    Raises
    ------
    FrameworkConfigError
        If the run-log ledger cannot be read.
    """
    try:
        matches = (
            spark.table(f"{control_schema}.reconciliation_run_log")
            .filter(
                f"reconciliation_id = '{reconciliation_id}' AND target_id = '{target_id}' AND "
                f"source_batch_fingerprint = '{fingerprint}' AND status = 'SUCCESS'"
            )
            .limit(1)
            .count()
        )
        return matches > 0
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(
            f"Failed to check reconciliation_run_log for reconciliation_id='{reconciliation_id}', "
            f"target_id='{target_id}': {exc}"
        ) from exc


def apply_transform_sql(
    missing_df: DataFrame, transform_sql: Optional[str], parameters: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """Reshape ``missing_df`` (a target's ``source_to_target`` miss set) via the flow-level
    ``transform_sql``, when configured, to match ``append_target_table``'s own schema.

    Unlike ``data_standardization_sql`` (a restricted, single-column-expression grammar --
    see ``ingestion/standardization_sql.py``), ``transform_sql`` legitimately needs full
    ``SELECT``/``FROM`` (and joins, unions, etc) to reshape a record set from the source's
    column layout into the target's -- so it is executed as arbitrary Spark SQL against a
    temp view of ``missing_df``, named :data:`UNMATCHED_RECORDS_VIEW_NAME`. Every
    ``transform_sql`` is expected/documented to read ``FROM _reconciliation_unmatched_records``.

    ``${param}`` placeholders are substituted first (same mechanism as ``filter_condition``
    and ``transformation_sql`` -- see ``transformation/parameters.py``), so this must receive
    the same ``parameters`` dict the flow's ``dataflow_group_spec.pipeline_parameters_json``
    resolves to.

    Parameters
    ----------
    missing_df:
        The miss set to reshape (unchanged, returned as-is, when ``transform_sql`` is falsy).
    transform_sql:
        Flow-level SQL, or ``None``/empty for a no-op passthrough.
    parameters:
        Dynamic runtime parameters for ``${param}`` substitution.

    Raises
    ------
    FrameworkConfigError
        If parameter substitution or SQL execution fails.
    """
    if not transform_sql:
        return missing_df
    try:
        resolved_sql = substitute_dynamic_parameters(transform_sql, parameters or {})
        missing_df.createOrReplaceTempView(UNMATCHED_RECORDS_VIEW_NAME)
        return missing_df.sparkSession.sql(resolved_sql)
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to apply reconciliation transform_sql: {exc}") from exc


def append_missing_records(missing_df: DataFrame, append_target_table: str) -> int:
    """Append ``missing_df`` (already ``transform_sql``-reshaped, if configured) into
    ``append_target_table``, returning the row count appended.

    Two storage-optimization behaviors:

    * ``mergeSchema=true`` on every append -- lets an appended row introduce a column
      ``append_target_table`` didn't have yet (most commonly ``__framework_hash_key`` -- see
      below) without requiring the table's original DDL to have anticipated it; existing rows
      simply get ``NULL`` for the new column, standard Delta schema-evolution behavior.
    * Liquid Clustering on ``__framework_hash_key``, applied *only* the first time this
      function creates ``append_target_table`` (an already-existing table keeps whatever
      clustering/schema it already has -- Delta clustering is fixed at creation, not
      alterable via a later write) and only when that column is present on ``missing_df``.
      This is this framework's explicit "fast at scale" requirement for reconciliation (see
      this package's module docstrings) applied to every table this module writes into,
      mirroring how ``storage/table_properties.py``/``dq/quarantine.py`` apply ``cluster_by``
      for CDC-dispatched targets elsewhere in the framework, just via the plain
      ``DataFrameWriter.clusterBy(...)`` API instead of a ``@dlt.table`` decorator kwarg,
      since this module runs in a plain job-task notebook, not a Lakeflow Declarative
      Pipeline graph.

    Raises
    ------
    FrameworkConfigError
        If the append write fails (e.g. a genuine schema mismatch ``mergeSchema`` can't
        reconcile, such as an incompatible type change on an existing column).
    """
    try:
        count = missing_df.count()
        if count == 0:
            return 0
        spark = missing_df.sparkSession
        writer = missing_df.write.format("delta").mode("append").option("mergeSchema", "true")
        if HASH_KEY_COLUMN in missing_df.columns and not spark.catalog.tableExists(append_target_table):
            writer = writer.clusterBy(HASH_KEY_COLUMN)
        writer.saveAsTable(append_target_table)
        return count
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to append missing records into '{append_target_table}': {exc}") from exc


def write_run_log_entry(
    spark: SparkSession,
    control_schema: str,
    reconciliation_id: str,
    target_id: str,
    run_id: str,
    fingerprint: str,
    status: str,
    metrics: Optional[ReconciliationMetrics] = None,
    error_message: Optional[str] = None,
    task_run_id: Optional[str] = None,
) -> None:
    """Append one row to ``reconciliation_run_log`` for this target's run.

    ``run_id`` is a required, caller-supplied value (rather than generated internally, as the
    pre-redesign version did) because ``reconciliation_mismatch_log.run_id`` is a foreign key
    into this exact row -- ``run_target_reconciliation`` generates one ``run_id`` up front and
    threads it through both writes so the two tables actually join.

    ``task_run_id`` is the parent job's own run id ({{job.run_id}}/{{job.parameters.task_run_id}}
    -- see ``05_reconciliation_engine.py``'s ``task_run_id`` widget), threaded through purely for
    correlation; ``None`` for a standalone run, same as today.

    Raises
    ------
    FrameworkConfigError
        If the append write fails.
    """
    metric_values = (metrics or ReconciliationMetrics()).as_dict()
    values_by_column = {
        "run_id": run_id,
        "reconciliation_id": reconciliation_id,
        "target_id": target_id,
        "source_batch_fingerprint": fingerprint,
        "status": status,
        "error_message": error_message,
        "task_run_id": task_run_id,
        "run_at": datetime.datetime.now(datetime.timezone.utc),
        **metric_values,
    }
    try:
        # Row(**kwargs)'s field order is its *construction* (kwarg) order, not the schema's --
        # spark.createDataFrame(rows, schema=...) then zips a Row's values against the given
        # schema *positionally*, not by name. Building the Row's positional values by walking
        # _RUN_LOG_SCHEMA.fields directly (instead of hand-ordering the Row(...) call to match
        # and hoping nobody reorders either one later) makes this immune to that mismatch by
        # construction.
        row = Row(*[values_by_column[field.name] for field in _RUN_LOG_SCHEMA.fields])
        spark.createDataFrame([row], schema=_RUN_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(
            f"{control_schema}.reconciliation_run_log"
        )
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(
            f"Failed to write reconciliation_run_log entry for reconciliation_id='{reconciliation_id}', "
            f"target_id='{target_id}': {exc}"
        ) from exc


def write_reconciliation_result(
    spark: SparkSession,
    control_schema: str,
    reconciliation_id: str,
    target_id: str,
    run_id: str,
    status: str,
    metrics: Optional[ReconciliationMetrics] = None,
    task_run_id: Optional[str] = None,
) -> None:
    """Append one row to ``reconciliation_result`` -- always, regardless of ``logging_config``.

    Deliberately lighter than ``reconciliation_run_log`` (no fingerprint, no source/target row
    counts, no error detail) so it stays cheap to write unconditionally even for a
    high-frequency continuous flow with both ``logging_config`` flags off. See
    ``control_plane/ddl_definitions.py::get_reconciliation_result_ddl``.

    Raises
    ------
    FrameworkConfigError
        If the append write fails.
    """
    metric_values = (metrics or ReconciliationMetrics()).as_dict()
    values_by_column = {
        "result_id": str(uuid.uuid4()),
        "run_id": run_id,
        "reconciliation_id": reconciliation_id,
        "target_id": target_id,
        "task_run_id": task_run_id,
        "status": status,
        "matched_count": metric_values.get("matched_count"),
        "missing_in_target_count": metric_values.get("missing_in_target_count"),
        "missing_in_source_count": metric_values.get("missing_in_source_count"),
        "value_drift_count": metric_values.get("value_drift_count"),
        "run_at": datetime.datetime.now(datetime.timezone.utc),
    }
    try:
        row = Row(*[values_by_column[field.name] for field in _RESULT_SCHEMA.fields])
        spark.createDataFrame([row], schema=_RESULT_SCHEMA).write.format("delta").mode("append").saveAsTable(
            f"{control_schema}.reconciliation_result"
        )
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(
            f"Failed to write reconciliation_result entry for reconciliation_id='{reconciliation_id}', "
            f"target_id='{target_id}': {exc}"
        ) from exc


def resolve_log_capture_flags(
    logging_config: Optional[Dict[str, Any]],
    recon_run_log_capture: Optional[bool] = None,
    recon_mismatch_log: Optional[bool] = None,
) -> Tuple[bool, bool]:
    """Resolve ``(run_log_capture, mismatch_log_capture)`` for one reconciliation run.

    Pure -- no Spark, no I/O -- so the precedence rule is unit-testable with plain dicts, and so
    there is exactly one place in the codebase where it is decided.

    Precedence, highest first:

    1. The ``recon_run_log_capture`` / ``recon_mismatch_log`` **job parameters**. ``None`` means
       "not set" (the widgets are tri-state: ``""``/``"true"``/``"false"``, and ``""`` maps to
       ``None`` here), which is what lets an operator leave the decision to the flow's own
       metadata rather than being forced to restate it on every run.
    2. This flow's own ``logging_config.run_log_capture`` / ``logging_config.mismatch_log_capture``
       -- the onboarded, per-flow layer.
    3. ``True``.

    The two naming schemes are deliberately distinct rather than unified: ``recon_*`` names a
    runtime job/pipeline parameter (it lives on a job run, is set by whoever launches it, and
    evaporates afterwards); ``logging_config.*`` names onboarded flow metadata (it lives in
    ``reconciliation_flow_spec``, is reviewed like any other spec field, and persists). Giving
    them the same name would suggest they are one setting written twice, which would make the
    override look like a conflict rather than a layer.

    Parameters
    ----------
    logging_config:
        This flow's raw ``logging_config`` dict, or ``None``/``{}`` when the flow declares none.
    recon_run_log_capture, recon_mismatch_log:
        The job-parameter layer. ``None`` (the default) means unset.

    Returns
    -------
    Tuple[bool, bool]
        ``(run_log_capture, mismatch_log_capture)``. ``reconciliation_result`` is written
        unconditionally regardless of both -- see :func:`write_reconciliation_result`.
    """
    config = logging_config or {}
    run_log_capture = (
        recon_run_log_capture if recon_run_log_capture is not None else config.get("run_log_capture", True)
    )
    mismatch_log_capture = (
        recon_mismatch_log if recon_mismatch_log is not None else config.get("mismatch_log_capture", True)
    )
    return bool(run_log_capture), bool(mismatch_log_capture)


def _complete_phase_1_match(
    spark: SparkSession,
    control_schema: str,
    reconciliation_id: str,
    target_id: str,
    source_fingerprint: ReconciliationFingerprint,
    target_fingerprint: ReconciliationFingerprint,
    logging_config: Optional[Dict[str, Any]],
    task_run_id: Optional[str],
    recon_run_log_capture: Optional[bool],
    recon_mismatch_log: Optional[bool],
    log_event_start: float,
) -> ReconciliationMetrics:
    """Book-keeping for a Phase 1 early-out: write the same log/result rows a Phase 2 run would.

    Split out of :func:`run_target_reconciliation` purely so the early-out is a single,
    reviewable exit rather than a branch threaded through the whole Phase 2 body -- the one
    shape of change most likely to disturb logic (``matcher.py``'s duplicate-key collapse and
    this module's append gating) that must not be disturbed.

    Every count is derived from the two fingerprints, not from a fresh scan: the sides are
    byte-for-byte equal by Phase 1's own finding, so ``matched_count`` is the whole source row
    count and all four discrepancy counts are ``0``. ``appended_count``/``failed_count`` are ``0``
    because an early-out appends nothing and fails nothing.

    ``fingerprint`` is written as the **source-side** ``hash_key_xor`` rather than
    :func:`compute_batch_fingerprint`'s miss-set digest: there is no miss set to fingerprint, and
    the run-log column must still carry something deterministic and meaningful for this run. It
    is deliberately *not* fed to :func:`is_target_batch_already_processed` -- that ledger exists
    to stop a re-run from appending the same corrections twice, and a run that appends nothing
    has nothing to guard.

    ``mismatch_log_capture`` is resolved but unused here: an all-matched run produces no
    per-record mismatch rows to gate in the first place. ``reconciliation_result`` is written
    unconditionally, exactly as on every other path.
    """
    run_log_capture, _unused_mismatch_log_capture = resolve_log_capture_flags(
        logging_config, recon_run_log_capture, recon_mismatch_log
    )
    status = "SUCCESS"
    run_id = str(uuid.uuid4())
    fingerprint = source_fingerprint.hash_key_xor
    metrics = ReconciliationMetrics(
        source_record_count=source_fingerprint.row_count,
        target_record_count=target_fingerprint.row_count,
        matched_count=source_fingerprint.row_count,
        missing_in_target_count=0,
        missing_in_source_count=0,
        value_drift_count=0,
        appended_count=0,
        failed_count=0,
    )

    if run_log_capture:
        write_run_log_entry(
            spark, control_schema, reconciliation_id, target_id, run_id, fingerprint, status, metrics, task_run_id=task_run_id
        )
    write_reconciliation_result(spark, control_schema, reconciliation_id, target_id, run_id, status, metrics, task_run_id=task_run_id)

    log_flow_event(
        operation="reconciliation_match",
        flow_id=f"{reconciliation_id}:{target_id}",
        status="SUCCESS",
        records_read=metrics.source_record_count,
        records_written=metrics.appended_count,
        duration_ms=(time.monotonic() - log_event_start) * 1000.0,
        reconciliation_id=reconciliation_id,
        target_id=target_id,
        run_id=run_id,
        run_status=status,
        target_record_count=metrics.target_record_count,
        matched_count=metrics.matched_count,
        missing_in_target_count=metrics.missing_in_target_count,
        missing_in_source_count=metrics.missing_in_source_count,
        value_drift_count=metrics.value_drift_count,
        phase=PHASE_1_MATCH,
    )
    return metrics


def run_target_reconciliation(
    spark: SparkSession,
    control_schema: str,
    reconciliation_id: str,
    source_df: DataFrame,
    target_df: DataFrame,
    target_config: Dict[str, Any],
    match_keys: List[str],
    compare_columns: Optional[List[str]],
    source_hash_precomputed: bool,
    transform_sql: Optional[str],
    parameters: Optional[Dict[str, Any]] = None,
    logging_config: Optional[Dict[str, Any]] = None,
    task_run_id: Optional[str] = None,
    recon_run_log_capture: Optional[bool] = None,
    recon_mismatch_log: Optional[bool] = None,
    two_tier_verification: bool = True,
) -> ReconciliationMetrics:
    """Match, append, and log for exactly one ``target_configs[]`` entry -- the happy path only.

    Callers (``05_reconciliation_engine.py``, ``streaming.py``'s micro-batch handler) are
    responsible for catching any raised exception, writing their own ``FAILED``
    ``reconciliation_run_log`` row (this function does not -- it has no way to know, on
    failure, how much of the run actually completed), and applying
    ``error_handling.on_failure`` (``"fail"`` vs. ``"warn"``).

    Parameters
    ----------
    source_df:
        Already prepared (``matcher.py::prepare_dataset_for_matching`` already applied) --
        callers processing multiple targets against the same ``source_config`` should prepare
        it once, outside their per-target loop, so its read/filter/standardization/hash steps
        aren't repeated per target (Delta's own file skipping keeps the cost of each re-read
        bounded -- see ``matcher.py``'s performance note; serverless compute, which every job
        in this framework runs on, does not support ``DataFrame.cache()``/``.persist()`` as a
        cheaper alternative).
    target_df:
        The raw read for this target (``dataset_reader.py::read_reconciliation_dataset``
        output) -- *not* yet prepared; this function prepares it internally, since each
        target's dataset is only ever used once.
    target_config:
        This target's own ``target_configs[]`` entry (``target_id``, ``hash_precomputed``,
        ``comparison_direction``, ``append_target_table``, ...).
    match_keys, compare_columns, transform_sql:
        Flow-level fields, shared across every target in this reconciliation flow. The
        ``generate_surrogate_key`` flag that used to sit between them was removed in v1.4.0 with
        the surrogate-key engine -- callers passing it positionally must drop the argument.
    source_hash_precomputed:
        ``source_config.hash_precomputed`` -- an assertion that the SOURCE side already carries
        ``__framework_hash_key``/``__framework_hash_value``, not a request to build them. Each
        target's own ``target_config.hash_precomputed`` is read separately, below, because the two
        sides are independently materialized and can legitimately disagree (a framework-managed
        target with ``true`` reconciled against a third-party source with ``false`` is the common
        shape). See ``matcher.py::prepare_dataset_for_matching``.
    parameters:
        Dynamic runtime parameters for ``${param}`` substitution in ``transform_sql`` (and
        already applied to both sides' ``filter_condition`` by ``dataset_reader.py`` before
        this function ever sees ``source_df``/``target_df``).
    logging_config:
        This flow's raw ``logging_config`` dict (``run_log_capture``/``mismatch_log_capture``,
        both default ``true``) -- gates whether ``reconciliation_run_log``/
        ``reconciliation_mismatch_log`` get written for this run. ``reconciliation_result`` is
        always written regardless -- see ``write_reconciliation_result``.
    task_run_id:
        Parent job's own run id, threaded into every log/result row written by this call for
        correlation -- ``None`` for a standalone run. It is *not* a filter here: narrowing a
        side's read to one producing run happens upstream, in
        ``dataset_reader.py::read_reconciliation_dataset``, and only when that side declares a
        ``task_run_id_column``.
    recon_run_log_capture, recon_mismatch_log:
        **SIGNATURE CHANGE (v1.3.0, backward compatible):** new *trailing* keyword arguments,
        the job-parameter layer over ``logging_config`` -- ``None`` (the default) means "not
        set", leaving this flow's own ``logging_config`` in charge exactly as before. See
        :func:`resolve_log_capture_flags` for the precedence rule and why the two naming
        schemes stay distinct.
    two_tier_verification:
        **SIGNATURE CHANGE (v1.3.0, backward compatible):** a new *trailing* keyword argument,
        default ``True``. Gates the Phase 1 early-out: both sides' cheap
        ``matcher.py::compute_side_fingerprint`` aggregates are compared before the join, and a
        match returns immediately with an all-matched result and no join at all. ``False``
        always runs Phase 2 -- the correct setting for a flow that cannot tolerate the XOR
        fold's documented pair-cancellation property (see ``matcher.py``'s module docstring).
        A Phase 1 early-out never appends, never writes mismatch rows, and never consults the
        restartability fingerprint ledger: there is by definition nothing to append, nothing to
        log per-record, and nothing whose re-processing needs guarding against. It also skips
        ``match_reconciliation_target``'s own defensive check that every ``match_keys``/
        ``compare_columns`` entry is physically present on both sides -- reachable only for a
        ``hash_precomputed`` side that carries the hash columns but not the raw ones, and only on
        a run where the two sides agreed anyway, in which case there was nothing that check could
        have protected. The next run that actually differs escalates to Phase 2 and reports it.

    Returns
    -------
    ReconciliationMetrics
        The metrics written to this target's ``reconciliation_run_log`` row.

    Raises
    ------
    FrameworkConfigError
        Propagated from any of matching, appending, or logging. On any successful (or
        skipped-as-already-processed) completion, also emits a structured
        ``"reconciliation_match"`` JSON log event (Phase 10) carrying every
        :class:`ReconciliationMetrics` count plus ``target_id``/``reconciliation_id`` and a
        ``phase`` field (``PHASE_1_MATCH`` when the run short-circuited on the fingerprint
        early-out, ``PHASE_2_DETAIL`` when it actually joined) -- without that field, a run that
        skipped the join would be indistinguishable in the event stream from a run that did one
        and found everything matched, which is precisely the thing a two-tier optimization has to
        stay auditable about. Unlike
        ``reconciliation_run_log`` (which this function also always writes), this event is not
        attempted on a raised exception, for the same reason ``write_run_log_entry`` isn't
        called here on failure either (see this function's own docstring, above): this function
        genuinely has no way to know, mid-failure, how much of the run actually completed, so a
        best-effort partial event here would be misleading rather than merely incomplete. The
        caller's own FAILED ``reconciliation_run_log`` row (see ``05_reconciliation_engine.py``)
        remains the authoritative failure record for a target's reconciliation run.
    """
    _log_event_start = time.monotonic()
    target_id = target_config["target_id"]
    target_hash_precomputed = bool(target_config.get("hash_precomputed", False))
    comparison_direction = target_config.get("comparison_direction", "both")
    append_target_table = target_config.get("append_target_table")

    prepared_target_df = prepare_dataset_for_matching(
        target_df, match_keys, compare_columns, target_hash_precomputed
    )

    evaluate_source_to_target = comparison_direction in ("source_to_target", "both")
    evaluate_target_to_source = comparison_direction in ("target_to_source", "both")

    # Validated up front, BEFORE the Phase 1 early-out below, rather than at the point of use:
    # a target that can never append is misconfigured whether or not this particular run happens
    # to find anything to append, and a config error that only surfaces on the runs that find a
    # mismatch is a config error that surfaces at the worst possible moment.
    if evaluate_source_to_target and not append_target_table:
        raise FrameworkConfigError(
            f"target_id='{target_id}' has comparison_direction={comparison_direction!r} but no "
            "append_target_table configured (the onboarding validator should already reject this)"
        )

    # PHASE 1 -- the cheap, shuffle-free early-out (see matcher.py's module docstring). Both
    # fingerprints are computed BEFORE anything is appended, which also makes their row_counts
    # safe to reuse as this run's source/target record counts (see the comment below on why
    # counting the target after an append is a real, previously-observed bug).
    source_fingerprint = None
    target_fingerprint = None
    if two_tier_verification:
        source_fingerprint = compute_side_fingerprint(source_df)
        target_fingerprint = compute_side_fingerprint(prepared_target_df)
        if fingerprints_match(source_fingerprint, target_fingerprint):
            logger.info(
                "Reconciliation '%s'/target '%s': Phase 1 fingerprints match (row_count=%d) -- "
                "skipping the Phase 2 join entirely.",
                reconciliation_id,
                target_id,
                source_fingerprint.row_count,
            )
            return _complete_phase_1_match(
                spark,
                control_schema,
                reconciliation_id,
                target_id,
                source_fingerprint,
                target_fingerprint,
                logging_config,
                task_run_id,
                recon_run_log_capture,
                recon_mismatch_log,
                _log_event_start,
            )

    # Captured NOW, as a plain int, not deferred to a `.count()` call at the end of this
    # function. Real bug found live (via test_reconciliation.py's own run-log assertions):
    # `prepared_target_df` is a *lazy* Spark DataFrame built from `spark.table(...)` -- a live
    # reference to this target's current state, re-evaluated on every action -- and this
    # target_id's own `append_target_table` is frequently the SAME table `prepared_target_df`
    # reads from (append_target_table is typically the CDC/Zerobus source feeding this
    # target's own materialization cycle -- see this module's docstring). Calling
    # `prepared_target_df.count()` only after `append_missing_records` (below) has already
    # written this run's own corrections into that same table silently counted this run's OWN
    # just-appended rows as if they'd already been present when the run started, corrupting
    # the "how many records were in the target before this run compared anything" metric
    # (confirmed live: a run that appended 3 corrective rows logged target_record_count as
    # target-count-plus-3, not the actual pre-run count).
    # When Phase 1 ran, its aggregate already counted exactly these rows in the same
    # pre-append instant, so its count is reused rather than paying for a second full scan --
    # the invariant above is preserved, not relaxed.
    target_record_count = target_fingerprint.row_count if target_fingerprint is not None else prepared_target_df.count()

    # PHASE 2 -- the full hash-key join, per-key collapse, four-way classification and
    # column-level discrepancy mapping. Completely unchanged by two-tier verification: Phase 1
    # is an early-out in front of this, never a replacement for it.
    match_result = match_reconciliation_target(
        source_df, prepared_target_df, match_keys, compare_columns, source_hash_precomputed
    )

    appended_count = 0
    fingerprint = "n/a"
    status = "SUCCESS"

    if evaluate_source_to_target:
        fingerprint = compute_batch_fingerprint(match_result.missing_in_target_df, match_keys)
        if match_result.missing_in_target_count == 0:
            logger.info(
                "Reconciliation '%s'/target '%s': no source_to_target misses -- nothing to append.",
                reconciliation_id,
                target_id,
            )
        elif is_target_batch_already_processed(spark, control_schema, reconciliation_id, target_id, fingerprint):
            status = "SKIPPED_ALREADY_PROCESSED"
            logger.info(
                "Reconciliation '%s'/target '%s': miss-set fingerprint '%s' already processed -- skipping append.",
                reconciliation_id,
                target_id,
                fingerprint,
            )
        else:
            reshaped_df = apply_transform_sql(match_result.missing_in_target_df, transform_sql, parameters)
            appended_count = append_missing_records(reshaped_df, append_target_table)
            logger.info(
                "Reconciliation '%s'/target '%s': appended %d record(s) into '%s'.",
                reconciliation_id,
                target_id,
                appended_count,
                append_target_table,
            )

    mismatch_frames = []
    if evaluate_source_to_target:
        mismatch_frames.append(
            match_result.mismatch_detail_df.filter(
                F.col(MISMATCH_TYPE_COLUMN).isin(MISMATCH_TYPE_MISSING_IN_TARGET, MISMATCH_TYPE_VALUE_DRIFT)
            )
        )
    if evaluate_target_to_source:
        mismatch_frames.append(
            match_result.mismatch_detail_df.filter(F.col(MISMATCH_TYPE_COLUMN) == MISMATCH_TYPE_MISSING_IN_SOURCE)
        )

    run_log_capture, mismatch_log_capture = resolve_log_capture_flags(
        logging_config, recon_run_log_capture, recon_mismatch_log
    )

    run_id = str(uuid.uuid4())
    if mismatch_frames and mismatch_log_capture:
        mismatch_df_to_log = mismatch_frames[0]
        for extra_df in mismatch_frames[1:]:
            mismatch_df_to_log = mismatch_df_to_log.unionByName(extra_df)
        write_mismatch_log_rows(
            spark,
            control_schema,
            run_id,
            reconciliation_id,
            target_id,
            mismatch_df_to_log,
            match_keys,
            compare_columns,
            task_run_id=task_run_id,
        )

    metrics = ReconciliationMetrics(
        # Same reuse rationale as target_record_count above: Phase 1's aggregate already counted
        # this side, so a second full scan of it would buy nothing.
        source_record_count=source_fingerprint.row_count if source_fingerprint is not None else source_df.count(),
        target_record_count=target_record_count,
        matched_count=match_result.matched_count,
        missing_in_target_count=match_result.missing_in_target_count,
        missing_in_source_count=match_result.missing_in_source_count,
        value_drift_count=match_result.value_drift_count,
        appended_count=appended_count,
        failed_count=0,
    )
    if run_log_capture:
        write_run_log_entry(
            spark, control_schema, reconciliation_id, target_id, run_id, fingerprint, status, metrics, task_run_id=task_run_id
        )
    write_reconciliation_result(spark, control_schema, reconciliation_id, target_id, run_id, status, metrics, task_run_id=task_run_id)

    log_flow_event(
        operation="reconciliation_match",
        flow_id=f"{reconciliation_id}:{target_id}",
        # SKIPPED_ALREADY_PROCESSED is neither a failure nor a fresh success (no new rows were
        # actually evaluated this run, by restartability design -- see this module's docstring)
        # -- WARNING keeps it visible in an ERROR-level alert scan without conflating it with a
        # genuine FAILED reconciliation run.
        status="WARNING" if status == "SKIPPED_ALREADY_PROCESSED" else "SUCCESS",
        records_read=metrics.source_record_count,
        records_written=metrics.appended_count,
        duration_ms=(time.monotonic() - _log_event_start) * 1000.0,
        reconciliation_id=reconciliation_id,
        target_id=target_id,
        run_id=run_id,
        run_status=status,
        target_record_count=metrics.target_record_count,
        matched_count=metrics.matched_count,
        missing_in_target_count=metrics.missing_in_target_count,
        missing_in_source_count=metrics.missing_in_source_count,
        value_drift_count=metrics.value_drift_count,
        phase=PHASE_2_DETAIL,
    )
    return metrics
