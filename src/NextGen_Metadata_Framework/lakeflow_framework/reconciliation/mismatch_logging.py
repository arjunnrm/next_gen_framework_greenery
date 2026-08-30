"""Per-record mismatch detail: ``matcher.py``'s classification, written to ``reconciliation_mismatch_log``.

``reconciliation_run_log`` (``appender.py``) reports aggregate counts per target per run; this
module is what lets someone drill from those aggregates down to the exact offending rows and
columns -- an explicit user requirement to capture "as much mismatching information as
possible". For a ``VALUE_DRIFT`` record that means every individual compared column that
actually differs (not merely "this row drifted"), with both sides' raw values.

Everything here is built as native Spark column expressions (``to_json(struct(...))``,
``array``/``filter``/``to_json`` for the differing-columns array) rather than a driver-side
``collect()`` + per-row ``json.dumps`` loop -- a reconciliation flow with a pathologically large
mismatch set must still write via a normal distributed DataFrame write, not funnel every
mismatched row through the driver, which is the difference between "fast at any scale" and "an
OOM waiting to happen" for a flow with millions of drifted/missing records.
"""

import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.cdc.hashing import HASH_VALUE_COLUMN
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.observability.structured_logger import logged_operation
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.matcher import (
    MISMATCH_TYPE_COLUMN,
    MISMATCH_TYPE_VALUE_DRIFT,
    target_prefixed_column,
)

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.reconciliation.mismatch_logging")


def write_mismatch_log_rows(
    spark: SparkSession,
    control_schema: str,
    run_id: str,
    reconciliation_id: str,
    target_id: str,
    mismatch_detail_df: DataFrame,
    match_keys: List[str],
    compare_columns: Optional[List[str]] = None,
    task_run_id: Optional[str] = None,
) -> int:
    """Project ``mismatch_detail_df`` (``matcher.py::ReconciliationMatchResult.mismatch_detail_df``)
    into ``reconciliation_mismatch_log`` rows and append them, returning the row count written.

    Parameters
    ----------
    spark:
        Active SparkSession (unused directly -- kept for signature symmetry with
        ``appender.py``'s control-table writers and to make the control-table target explicit
        at call sites; the actual write runs through ``mismatch_detail_df``'s own session).
    control_schema:
        ``<catalog>.config`` -- ``reconciliation_mismatch_log`` lives here.
    run_id:
        The ``reconciliation_run_log.run_id`` this batch of mismatches was detected during
        (one run_id per target per run -- see ``appender.py``).
    reconciliation_id, target_id:
        Identify which flow / which ``target_configs[]`` entry these mismatches belong to.
    mismatch_detail_df:
        Every non-``MATCHED`` record, as classified by ``matcher.py::match_reconciliation_target``
        -- already carries ``__recon_mismatch_type``, both sides' raw ``match_keys``/
        ``compare_columns`` values, and both sides' ``__framework_hash_value``.
    match_keys, compare_columns:
        Same flow-level lists used to produce ``mismatch_detail_df`` -- needed here to know
        which columns to fold into ``match_key_values_json``/``differing_columns_json``.
    task_run_id:
        Parent job's own run id, if any -- same value as the owning ``reconciliation_run_log``
        row's ``task_run_id`` -- threaded through purely for correlation.

    Returns
    -------
    int
        Number of mismatch rows written (``0`` is a valid, common outcome -- most runs find no
        mismatches at all).

    Raises
    ------
    FrameworkConfigError
        If projecting or writing the mismatch rows fails. Also emits a structured
        ``"reconciliation_mismatch_log_write"`` JSON log event (Phase 10) -- ``SUCCESS`` with
        the row count written (``records_written``), or ``FAILED`` with the
        ``FrameworkConfigError``'s own message (the exception is still re-raised unchanged --
        see ``observability/structured_logger.py``'s "never mask the real processing error"
        guarantee).
    """
    compare_columns = compare_columns or []
    with logged_operation(
        "reconciliation_mismatch_log_write", f"{reconciliation_id}:{target_id}", reconciliation_id=reconciliation_id, target_id=target_id, run_id=run_id
    ) as op:
        try:
            match_key_values_json = F.to_json(
                F.struct(*[F.coalesce(F.col(k), F.col(target_prefixed_column(k))).alias(k) for k in match_keys])
            )

            if compare_columns:
                # One struct per compare_column that actually differs (null-safe: a NULL on either
                # side counts as "differs" whenever the other side is non-null, matching the hash
                # comparison's own coalesce-based semantics in matcher.py) -- F.filter drops the
                # nulls contributed by columns that *didn't* differ, so the final JSON array lists
                # only the columns genuinely responsible for this VALUE_DRIFT, not every compared
                # column.
                diff_candidates = [
                    F.when(
                        ~F.col(c).eqNullSafe(F.col(target_prefixed_column(c))),
                        F.struct(
                            F.lit(c).alias("column"),
                            F.col(c).cast("string").alias("source_value"),
                            F.col(target_prefixed_column(c)).cast("string").alias("target_value"),
                        ),
                    )
                    for c in compare_columns
                ]
                differing_columns_json = F.when(
                    F.col(MISMATCH_TYPE_COLUMN) == MISMATCH_TYPE_VALUE_DRIFT,
                    F.to_json(F.filter(F.array(*diff_candidates), lambda element: element.isNotNull())),
                ).otherwise(F.lit(None).cast("string"))
            else:
                differing_columns_json = F.lit(None).cast("string")

            rows_df = mismatch_detail_df.select(
                F.expr("uuid()").alias("mismatch_id"),
                F.lit(run_id).alias("run_id"),
                F.lit(reconciliation_id).alias("reconciliation_id"),
                F.lit(target_id).alias("target_id"),
                match_key_values_json.alias("match_key_values_json"),
                F.col(MISMATCH_TYPE_COLUMN).alias("mismatch_type"),
                differing_columns_json.alias("differing_columns_json"),
                F.col(HASH_VALUE_COLUMN).alias("source_hash_value"),
                F.col(target_prefixed_column(HASH_VALUE_COLUMN)).alias("target_hash_value"),
                F.lit(task_run_id).alias("task_run_id"),
                F.current_timestamp().alias("detected_at"),
            )

            count = rows_df.count()
            op.records_written = count
            if count == 0:
                return 0
            rows_df.write.format("delta").mode("append").saveAsTable(f"{control_schema}.reconciliation_mismatch_log")
            logger.info(
                "Reconciliation '%s'/target '%s': wrote %d reconciliation_mismatch_log row(s) for run '%s'",
                reconciliation_id,
                target_id,
                count,
                run_id,
            )
            return count
        except Exception as exc:  # noqa: BLE001
            raise FrameworkConfigError(
                f"Failed to write reconciliation_mismatch_log rows for reconciliation_id='{reconciliation_id}', "
                f"target_id='{target_id}': {exc}"
            ) from exc
