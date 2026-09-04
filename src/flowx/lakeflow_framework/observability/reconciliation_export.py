"""Reconciliation control-table BACKSTOP export -- the audit guarantee for a reconciliation flow
that runs inside a Lakeflow Declarative Pipeline (``execution_mode`` ``"pipeline"`` /
``"pipeline_audit_only"``).

**Why this module exists.** In pipeline mode the three shared control tables
(``reconciliation_run_log`` / ``reconciliation_result`` / ``reconciliation_mismatch_log``) are
written by the L5 ``foreach_batch_sink`` healing handler registered by
``reconciliation/graph_registration.py``. That handler is the PRIMARY writer and stays the primary
writer -- but it only fires when Lakeflow actually delivers a micro-batch to the sink, i.e. when
the L5 *pulse* carried rows. Two entirely normal situations produce no pulse and therefore no
handler invocation, and in both of them the comparison itself ran perfectly well:

* an update in which the reconciliation source stream produced no new rows (an empty pulse), and
* a ``"pipeline_audit_only"`` flow, which registers L3+L4 and deliberately has **no** L5 lane at
  all -- so nothing ever writes its control rows.

The published L4 datasets (``recon__<id>__<target>__metrics`` when ``run_log_capture`` resolves
true, ``__mismatch`` when ``mismatch_log_capture`` does -- since v1.6.0 each is registered ONLY
when its capture flag is on) are materialized regardless in both cases: the comparison is visible
in Unity Catalog, but the audit trail an operator actually queries would silently have no row for
that update. This module closes that gap. It reads what the pipeline already published and appends
the missing control rows, so **an audit row exists for every update the flow's logging_config asks
to be logged** whether or not the handler fired. A flow whose flags both resolve false publishes
no L4 audit datasets, writes no control rows anywhere, and is deliberately skipped here --
reconciliation then persists only to its business targets (the v1.6.0 contract; this includes
``reconciliation_result``, which is now gated by ``run_log_capture`` instead of unconditional).
**Since v1.7.3 that is the DEFAULT**: both flags fall back to ``False`` rather than ``True``, so a
flow that never declared a ``logging_config`` is skipped here too. Nothing is lost by the export
in that case -- there was nothing published to back-fill from -- but an operator expecting rows
must opt the flow in with ``run_log_capture``/``mismatch_log_capture: true``.

**Idempotency is the whole contract.** Every write is keyed on
``(reconciliation_id, target_id, pipeline_update_id)`` -- the update id is threaded through as each
control row's ``task_run_id`` by both writers -- and a key already present in a given control table
is skipped for that table. The three tables are checked *independently*, so the common partial case
(the handler wrote ``reconciliation_result`` and ``reconciliation_run_log`` but
``logging_config.mismatch_log_capture`` was resolved differently, or a write failed part-way)
converges instead of double-writing. Running this export twice over the same update writes nothing
the second time; running it after a handler that already fired writes nothing at all.

``run_id`` is therefore **deterministic**, not a fresh ``uuid4``: ``uuid5`` over
``(reconciliation_id, target_id, pipeline_update_id)``. A ``reconciliation_mismatch_log`` row
written by a later invocation than the ``reconciliation_run_log`` row it belongs to still joins it.

**R3 is untouched.** This is a plain, eager, job-task function -- no ``dlt`` import, no
``@dlt.table``, no graph registration. Observability stays a normal Lakeflow *job* task
(``notebooks/08_observability/08_dlt_observability_engine.py``, which calls this after its
transformation step and before dispatch), no observability notebook enters any pipeline's
``libraries:`` block, and the ``observability_export`` task keeps its four required
``base_parameters`` and its ``{{tasks.<key>.run_id}}`` dependency. The coupling runs one way only:
observability *hosts* the backstop, and audit is never coupled to observability running -- the
handler already wrote the rows on every update where it fired.

**Where the published datasets live.** ``reconciliation_flow_spec.publish_schema`` is documented as
"defaults to the hosting pipeline's own schema when NULL", and the hosting pipeline's schema is not
recorded in ``dataflow_group_spec``. So the location is *resolved by probing*: the flow's own
``publish_schema`` (against the group's ``catalog_name`` and every catalog its flows publish into)
when set, otherwise every ``(target_catalog, target_schema)`` pair this group's ingestion and
transformation flows use, in control-table order. The first candidate whose probe dataset
(``__metrics`` when ``run_log_capture`` is on, else ``__mismatch``) is readable wins; a flow whose
datasets cannot be located anywhere is logged at WARNING and skipped, never guessed at.
"""

import json
import logging
import uuid
from typing import Any, List, Optional, Sequence, Set, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flowx.lakeflow_framework.control_plane.repository import load_active_group_metadata
# dq.table_errors, NOT dq.quarantine: this module is imported by
# notebooks/08_observability/08_dlt_observability_engine.py, a plain JOB notebook task.
# dq/quarantine.py does a module-level `import dlt`, which dies outside a pipeline context
# with `NoSuchElementException: None.get` before any framework code runs (live, 2026-09-01).
from flowx.lakeflow_framework.dq.table_errors import is_table_not_found
from flowx.lakeflow_framework.engine.identifiers import sanitize_identifier
from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.observability.structured_logger import logged_operation
from flowx.lakeflow_framework.reconciliation.appender import (
    resolve_log_capture_flags,
    write_reconciliation_result,
    write_run_log_entry,
)
from flowx.lakeflow_framework.reconciliation.metrics import ReconciliationMetrics
from flowx.lakeflow_framework.storage.table_properties import qualified_table_name

logger = logging.getLogger("flowx.lakeflow_framework.observability.reconciliation_export")

#: ``reconciliation_run_log.source_batch_fingerprint`` is ``NOT NULL`` and, for a job-mode run, is a
#: bare sha256 hex digest of the unmatched-record set (``appender.py``). A backstop row has no such
#: set to digest -- it reports a comparison the pipeline already performed -- so it writes a
#: deliberately prefixed, deliberately NON-hex sentinel. A prefixed value can never be mistaken for
#: a real fingerprint, so it can never let a later job-mode run short-circuit to
#: ``SKIPPED_ALREADY_PROCESSED`` against a fingerprint this module invented.
BACKSTOP_FINGERPRINT_PREFIX = "pipeline-export-backstop:"

#: Status written on a backstop row. The comparison itself succeeded -- the published ``__metrics``
#: row is the evidence -- so ``SUCCESS`` is the honest value; the fingerprint prefix above is what
#: distinguishes a backstop row from a handler-written one.
BACKSTOP_STATUS = "SUCCESS"

#: Namespace for the deterministic ``run_id`` (see this module's docstring).
_RUN_ID_NAMESPACE = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")

#: ``reconciliation_mismatch_log``'s own DDL column order -- the published ``__mismatch`` dataset
#: carries every one of these except ``run_id``/``task_run_id``, which are spliced in here (the
#: identical splice ``mismatch_logging.py::write_mismatch_log_rows`` performs for the handler).
_MISMATCH_LOG_COLUMNS = (
    "mismatch_id",
    "run_id",
    "reconciliation_id",
    "target_id",
    "match_key_values_json",
    "mismatch_type",
    "differing_columns_json",
    "source_hash_value",
    "target_hash_value",
    "task_run_id",
    "detected_at",
)


def _row_get(row: Any, name: str, default: Any = None) -> Any:
    """Attribute-style field access tolerant of a control-table row that predates a column
    (``01_setup`` only ever runs ``CREATE TABLE IF NOT EXISTS``, never a migration) -- mirrors
    ``engine/source_plane.py``'s and ``reconciliation/graph_registration.py``'s identical helper.
    """
    return getattr(row, name, default)


def _json_loads(raw: Optional[str], default: Any) -> Any:
    """``json.loads`` a control-table JSON-string column, tolerating ``None``/empty."""
    if not raw:
        return default
    return json.loads(raw)


def _table_is_readable(spark: SparkSession, table_name: str) -> bool:
    """``True`` when ``table_name`` resolves to a readable table.

    Probes by attempting a read and matching Spark's stable not-found error-condition names
    (``dq/quarantine.py::_is_table_not_found``) rather than by ``spark.catalog.tableExists`` --
    the same choice, for the same reason, as
    ``reconciliation/appender.py::_append_target_table_exists``. Any other exception (permission
    denial, catalog outage) propagates: "I am not allowed to read this" must not be silently
    reported as "this dataset was never published".
    """
    try:
        spark.read.table(table_name).schema  # noqa: B018 -- forces analysis now, inside this try
        return True
    except Exception as exc:  # noqa: BLE001 -- narrowed immediately by condition name
        if is_table_not_found(exc):
            return False
        raise


def _publish_location_candidates(group_row: Any, flow_row: Any, flow_rows: Sequence[Any]) -> List[Tuple[str, str]]:
    """Ordered, de-duplicated ``(catalog, schema)`` candidates for where ``flow_row``'s L4 datasets
    were published -- see this module's docstring for why this must be probed rather than read.
    """
    group_catalog = _row_get(group_row, "catalog_name", None)
    flow_pairs: List[Tuple[str, str]] = []
    for row in flow_rows:
        catalog = _row_get(row, "target_catalog", None)
        schema = _row_get(row, "target_schema", None)
        if catalog and schema and (catalog, schema) not in flow_pairs:
            flow_pairs.append((catalog, schema))

    publish_schema = _row_get(flow_row, "publish_schema", None)
    if publish_schema:
        catalogs = [group_catalog] + [catalog for catalog, _ in flow_pairs]
        candidates = [(catalog, publish_schema) for catalog in catalogs if catalog]
    else:
        candidates = list(flow_pairs)
        if group_catalog:
            candidates += [(group_catalog, schema) for _, schema in flow_pairs]

    ordered: List[Tuple[str, str]] = []
    for candidate in candidates:
        if candidate not in ordered:
            ordered.append(candidate)
    return ordered


def _existing_keys(spark: SparkSession, table_name: str, pipeline_update_id: str) -> Set[Tuple[str, str]]:
    """The ``(reconciliation_id, target_id)`` pairs already present in ``table_name`` for this
    update -- one query per control table, for the whole group.

    A control table that does not exist yet yields the empty set (``01_setup`` provisions all
    three, but a workspace mid-provisioning must not fail an export); every other read failure
    propagates as :class:`FrameworkConfigError`, because silently treating "I could not check"
    as "nothing is there" is exactly how a backstop double-writes an audit trail.
    """
    if not _table_is_readable(spark, table_name):
        logger.warning(
            "Control table '%s' does not exist -- treating every reconciliation key as unwritten for "
            "pipeline_update_id='%s'.",
            table_name,
            pipeline_update_id,
        )
        return set()
    try:
        rows = (
            spark.table(table_name)
            .filter(F.col("task_run_id") == F.lit(pipeline_update_id))
            .select("reconciliation_id", "target_id")
            .distinct()
            .collect()
        )
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(
            f"Failed to read '{table_name}' while checking which reconciliation control rows already exist for "
            f"pipeline_update_id='{pipeline_update_id}': {exc}"
        ) from exc
    return {(row["reconciliation_id"], row["target_id"]) for row in rows}


def _backstop_run_id(reconciliation_id: str, target_id: str, pipeline_update_id: str) -> str:
    """Deterministic ``run_id`` for one ``(reconciliation_id, target_id, pipeline_update_id)`` key.

    Deterministic rather than ``uuid4`` so a ``reconciliation_mismatch_log`` row written by a later
    invocation of this export still joins the ``reconciliation_run_log`` row an earlier invocation
    wrote -- the two tables are keyed independently (see this module's docstring).
    """
    return str(uuid.uuid5(_RUN_ID_NAMESPACE, f"{reconciliation_id}/{target_id}/{pipeline_update_id}"))


def _metrics_from_row(metrics_row: Any) -> ReconciliationMetrics:
    """Project one published ``__metrics`` row into :class:`ReconciliationMetrics`.

    ``appended_count``/``failed_count`` are left ``None``, not zeroed: the L4 metrics dataset is a
    pure comparison summary and knows nothing about healing, and writing ``0`` would assert that
    nothing was appended -- which a backstop row (written precisely because the healing handler may
    never have run) is in no position to claim.
    """
    return ReconciliationMetrics(
        source_record_count=_row_get(metrics_row, "source_record_count"),
        target_record_count=_row_get(metrics_row, "target_record_count"),
        matched_count=_row_get(metrics_row, "matched_count"),
        missing_in_target_count=_row_get(metrics_row, "missing_in_target_count"),
        missing_in_source_count=_row_get(metrics_row, "missing_in_source_count"),
        value_drift_count=_row_get(metrics_row, "value_drift_count"),
    )


def _export_mismatch_rows(
    spark: SparkSession,
    control_schema: str,
    mismatch_dataset: str,
    run_id: str,
    pipeline_update_id: str,
) -> int:
    """Splice ``run_id``/``task_run_id`` into the published ``__mismatch`` dataset and append it to
    ``reconciliation_mismatch_log``, returning the row count written.

    The published dataset is already in ``mismatch_logging.py::build_mismatch_rows`` shape (that is
    the projection the ``__mismatch`` Lakeflow dataset itself calls), so this is the same two-column
    splice ``write_mismatch_log_rows`` performs -- including ``mismatch_id``, which is generated at
    dataset-materialization time and is therefore *stable* across re-exports.
    """
    try:
        projection = []
        for column in _MISMATCH_LOG_COLUMNS:
            if column == "run_id":
                projection.append(F.lit(run_id).alias("run_id"))
            elif column == "task_run_id":
                projection.append(F.lit(pipeline_update_id).alias("task_run_id"))
            else:
                projection.append(F.col(column))
        rows_df = spark.table(mismatch_dataset).select(*projection)
        count = rows_df.count()
        if count == 0:
            return 0
        rows_df.write.format("delta").mode("append").saveAsTable(f"{control_schema}.reconciliation_mismatch_log")
        return count
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(
            f"Failed to export '{mismatch_dataset}' into {control_schema}.reconciliation_mismatch_log for "
            f"pipeline_update_id='{pipeline_update_id}': {exc}"
        ) from exc


def export_reconciliation_control_rows(
    spark: SparkSession,
    control_catalog: str,
    group_id: str,
    pipeline_update_id: str,
) -> int:
    """Back-fill ``reconciliation_run_log`` / ``reconciliation_result`` /
    ``reconciliation_mismatch_log`` from the L4 datasets this group's pipeline-mode reconciliation
    flows published during ``pipeline_update_id``.

    Idempotent on ``(reconciliation_id, target_id, pipeline_update_id)``, per control table -- a key
    already present in a table is skipped for that table. See this module's docstring for the full
    contract and for why the handler remains the primary writer.

    Parameters
    ----------
    spark:
        Active SparkSession (a plain job-task session -- this function never runs inside a Lakeflow
        graph).
    control_catalog:
        Catalog holding the ``config`` schema with the control tables.
    group_id:
        ``dataflow_group_id`` whose pipeline just updated.
    pipeline_update_id:
        The Lakeflow pipeline update this export is about -- written as every exported row's
        ``task_run_id``, and half the idempotency key.

    Returns
    -------
    int
        Number of ``(reconciliation_id, target_id)`` targets for which at least one control row was
        appended. ``0`` is the *expected* outcome whenever the L5 handler already fired for every
        target, and is not an error.

    Raises
    ------
    FrameworkConfigError
        If ``pipeline_update_id`` is blank (the idempotency key would be meaningless, and the
        exported rows uncorrelatable), if a flow's JSON configuration is malformed, or if any
        control-table read/write fails.
    """
    if not (pipeline_update_id or "").strip():
        raise FrameworkConfigError(
            "export_reconciliation_control_rows requires a non-empty pipeline_update_id -- it is written as every "
            "exported row's task_run_id and is half the (reconciliation_id, target_id, pipeline_update_id) "
            "idempotency key, so exporting without one would both lose correlation and re-write on every run."
        )

    control_schema = f"{control_catalog}.config"
    metadata = load_active_group_metadata(spark, control_catalog, group_id)
    if not metadata.reconciliation_rows:
        logger.info(
            "dataflow_group_id='%s' has no active pipeline-mode reconciliation flows -- nothing to export for "
            "pipeline_update_id='%s'.",
            group_id,
            pipeline_update_id,
        )
        return 0

    flow_rows = list(metadata.ingestion_rows) + list(metadata.transformation_rows)

    with logged_operation(
        "reconciliation_control_export",
        group_id,
        dataflow_group_id=group_id,
        pipeline_update_id=pipeline_update_id,
    ) as op:
        run_log_keys = _existing_keys(spark, f"{control_schema}.reconciliation_run_log", pipeline_update_id)
        result_keys = _existing_keys(spark, f"{control_schema}.reconciliation_result", pipeline_update_id)
        mismatch_keys = _existing_keys(spark, f"{control_schema}.reconciliation_mismatch_log", pipeline_update_id)

        exported_targets = 0
        skipped_targets = 0

        for flow_row in metadata.reconciliation_rows:
            reconciliation_id = flow_row.reconciliation_id
            sanitized_reconciliation_id = sanitize_identifier(reconciliation_id)
            try:
                target_configs = _json_loads(_row_get(flow_row, "target_configs_json"), [])
                logging_config = _json_loads(_row_get(flow_row, "logging_config_json"), {})
            except json.JSONDecodeError as exc:
                raise FrameworkConfigError(
                    f"Reconciliation flow '{reconciliation_id}': malformed JSON configuration: {exc}"
                ) from exc

            if not target_configs:
                logger.warning(
                    "Reconciliation flow '%s' declares no target_configs[] -- nothing to export.", reconciliation_id
                )
                continue

            # No job-parameter layer exists inside/after a pipeline update, so the flow's own
            # logging_config is the only layer -- exactly the defaulting the L5 handler applies.
            run_log_capture, mismatch_log_capture = resolve_log_capture_flags(logging_config)

            if not run_log_capture and not mismatch_log_capture:
                # v1.6.0: with both captures suppressed the flow registers no __metrics/
                # __mismatch datasets and writes no control rows at all -- nothing to export.
                # v1.7.3: both flags default to FALSE, so this branch is now the DEFAULT path
                # for any flow that never declared a logging_config -- the export correctly has
                # nothing to back-fill, because the pipeline published nothing to back-fill it
                # from. This is the expected shape of "silent by default", not a missing export.
                logger.info(
                    "Reconciliation flow '%s': both log captures resolve false -- nothing to export.",
                    reconciliation_id,
                )
                skipped_targets += len(target_configs)
                continue

            target_ids = [tc["target_id"] for tc in target_configs]
            first_sanitized_target_id = sanitize_identifier(target_ids[0])

            # v1.6.0: __metrics exists only when run_log_capture is true, __mismatch only when
            # mismatch_log_capture is -- probe whichever dataset this flow actually registers.
            _probe_suffix = "metrics" if run_log_capture else "mismatch"
            location = None
            for candidate_catalog, candidate_schema in _publish_location_candidates(
                metadata.group_row, flow_row, flow_rows
            ):
                probe = qualified_table_name(
                    candidate_catalog,
                    candidate_schema,
                    f"recon__{sanitized_reconciliation_id}__{first_sanitized_target_id}__{_probe_suffix}",
                )
                if _table_is_readable(spark, probe):
                    location = (candidate_catalog, candidate_schema)
                    break

            if location is None:
                logger.warning(
                    "Reconciliation flow '%s': could not locate its published L4 metrics dataset in any candidate "
                    "(catalog, schema) for dataflow_group_id='%s' -- skipping its control-row export for "
                    "pipeline_update_id='%s'. The datasets may not have been materialized by this update.",
                    reconciliation_id,
                    group_id,
                    pipeline_update_id,
                )
                continue

            publish_catalog, publish_schema = location

            for target_id in target_ids:
                sanitized_target_id = sanitize_identifier(target_id)
                key = (reconciliation_id, target_id)
                # v1.6.0: reconciliation_result is gated by run_log_capture too -- with the
                # run log suppressed, reconciliation persists nothing but business targets.
                needs_result = run_log_capture and key not in result_keys
                needs_run_log = run_log_capture and key not in run_log_keys
                needs_mismatch = mismatch_log_capture and key not in mismatch_keys
                if not (needs_result or needs_run_log or needs_mismatch):
                    skipped_targets += 1
                    continue

                metrics = None
                if needs_run_log or needs_result:
                    metrics_dataset = qualified_table_name(
                        publish_catalog,
                        publish_schema,
                        f"recon__{sanitized_reconciliation_id}__{sanitized_target_id}__metrics",
                    )
                    if not _table_is_readable(spark, metrics_dataset):
                        logger.warning(
                            "Reconciliation flow '%s' target '%s': '%s' is not readable -- skipping this target's "
                            "run_log/result export for pipeline_update_id='%s'.",
                            reconciliation_id,
                            target_id,
                            metrics_dataset,
                            pipeline_update_id,
                        )
                        needs_run_log = needs_result = False
                    else:
                        metrics_rows = spark.table(metrics_dataset).limit(1).collect()
                        if not metrics_rows:
                            # The L4 metrics dataset is a single .agg(...) and is one row by
                            # construction; zero rows means it was never materialized. Inventing
                            # all-NULL counts here would file an audit row asserting a comparison
                            # that has no evidence.
                            logger.warning(
                                "Reconciliation flow '%s' target '%s': '%s' published no rows -- skipping this "
                                "target's run_log/result export for pipeline_update_id='%s'.",
                                reconciliation_id,
                                target_id,
                                metrics_dataset,
                                pipeline_update_id,
                            )
                            needs_run_log = needs_result = False
                        else:
                            metrics = _metrics_from_row(metrics_rows[0])

                if not (needs_result or needs_run_log or needs_mismatch):
                    skipped_targets += 1
                    continue

                run_id = _backstop_run_id(reconciliation_id, target_id, pipeline_update_id)

                if needs_run_log:
                    write_run_log_entry(
                        spark,
                        control_schema,
                        reconciliation_id,
                        target_id,
                        run_id,
                        f"{BACKSTOP_FINGERPRINT_PREFIX}{pipeline_update_id}",
                        BACKSTOP_STATUS,
                        metrics=metrics,
                        task_run_id=pipeline_update_id,
                    )
                    run_log_keys.add(key)

                if needs_result:
                    write_reconciliation_result(
                        spark,
                        control_schema,
                        reconciliation_id,
                        target_id,
                        run_id,
                        BACKSTOP_STATUS,
                        metrics=metrics,
                        task_run_id=pipeline_update_id,
                    )
                    result_keys.add(key)

                mismatch_written = 0
                if needs_mismatch:
                    mismatch_dataset = qualified_table_name(
                        publish_catalog,
                        publish_schema,
                        f"recon__{sanitized_reconciliation_id}__{sanitized_target_id}__mismatch",
                    )
                    if _table_is_readable(spark, mismatch_dataset):
                        mismatch_written = _export_mismatch_rows(
                            spark, control_schema, mismatch_dataset, run_id, pipeline_update_id
                        )
                        mismatch_keys.add(key)
                    else:
                        logger.warning(
                            "Reconciliation flow '%s' target '%s': '%s' is not readable -- no mismatch detail "
                            "exported for pipeline_update_id='%s'.",
                            reconciliation_id,
                            target_id,
                            mismatch_dataset,
                            pipeline_update_id,
                        )

                exported_targets += 1
                logger.info(
                    "Backstop-exported reconciliation '%s' target '%s' for pipeline_update_id='%s' "
                    "(run_log=%s, result=%s, mismatch_rows=%d, run_id=%s)",
                    reconciliation_id,
                    target_id,
                    pipeline_update_id,
                    needs_run_log,
                    needs_result,
                    mismatch_written,
                    run_id,
                )

        op.records_written = exported_targets
        logger.info(
            "Reconciliation control-row export complete for dataflow_group_id='%s', pipeline_update_id='%s': "
            "%d target(s) exported, %d already present (handler-written).",
            group_id,
            pipeline_update_id,
            exported_targets,
            skipped_targets,
        )
        return exported_targets
