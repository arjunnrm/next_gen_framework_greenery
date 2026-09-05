"""Post-deployment governance (ABAC tag application), run *after* a pipeline update.

``ALTER TABLE ... SET TAGS`` is a Unity Catalog DDL operation against a materialized table,
so it must run *after* the Lakeflow Declarative Pipeline update that creates/updates that
table has already completed. Kept as a plain function (not notebook top-level code) so a
dedicated job task can import and call it without re-triggering the pipeline's
`@dlt.table`/`@dlt.view` graph-definition code -- see
``notebooks/04_governance/04_apply_governance_and_egress.py``.

**Phase 7 -- ``run_external_sink_exports`` removed.** This module used to also expose
``run_external_sink_exports``, which ran `external_sink` egress as a *separate,
post-deployment* plain-Spark step: ``spark.read.table(qualified_main_table).write.format(...)
.save(sink_path)``, executed as a distinct downstream job task after the pipeline update
finished. That was never a genuine Lakeflow sink -- it was an ordinary batch table read/write
racing whatever the pipeline had most recently materialized, which is exactly the
anti-pattern this project's core requirement forbids ("all external outputs must use genuine
Lakeflow/DLT sink functionality (`dlt.create_sink` + `@dlt.append_flow`), not ordinary DAG
table writes"). Every `external_sink`/`sink` export now happens *inside* the pipeline's own
graph, as a real `@dlt.append_flow` targeting a `dlt.create_sink` -- see
``engine/sink_registration.py`` (and, for the `pgp_zip` archive/PGP-encryption format,
``archive/pgp_zip_sink.py``) -- so there is nothing left for a post-deployment step to do for
sink egress at all; the whole function has been deleted rather than left as a no-op stub.

**Phase 10 -- ``capture_all_scd_change_counts`` added.** ``cdc/change_metrics.py::
capture_scd_change_counts`` computes exact insert/update/delete counts for a CDC-dispatched
target via Delta Change Data Feed, given a Delta commit *version range* -- but that range
("the version right before this pipeline update" / "the version right after") is only knowable
once the update has actually run and committed. There is no way to read a table's own
post-update version from *inside* that same update's own graph-definition code: every
`@dlt.table` closure that materializes a CDC target (``dq/quarantine.py``, ``cdc/scd.py``,
``cdc/snapshot.py``) only ever builds a lazy query plan -- Lakeflow itself executes and commits
that plan *after* graph-definition finishes, the same "closures run at execution time, plans
are built at graph-definition time" distinction ``engine/flow_registration.py``'s module
docstring describes for the ingestion/transformation engines. So, like
``apply_all_governance_tags`` above, this is a genuine *post*-deployment step.

**Persisted watermark (supersedes the original single-latest-commit heuristic).** The first
version of this function had no persisted "last processed version" anywhere, so it queried
``table_changes`` for only the target's single most-recent commit -- silently wrong whenever a
triggered update produced more than one commit to the same target, or produced none at all
since the last run (see the architecture review's Finding 5.1). It now reads/advances a
``last_processed_cdc_version`` watermark column on the owning flow's own spec row
(``ingestion_flow_spec``/``transformation_flow_spec``, whichever the flow belongs to) and
queries the *full* range since that watermark -- see :func:`capture_all_scd_change_counts` for
the exact logic.
"""

import json
import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from flowx.lakeflow_framework.cdc.change_metrics import capture_scd_change_counts
from flowx.lakeflow_framework.control_plane.repository import load_active_group_metadata
# apply_all_governance_tags MOVED to governance/tags.py in v1.7.x -- governance tagging is a
# governance concern and now lives with the rest of the governance model, instead of sitting in
# this module beside the unrelated CDC change-count capture. Re-exported here so the original
# import path (and notebooks/04_governance/04_apply_governance_and_egress.py) keeps working.
from flowx.lakeflow_framework.governance.tags import (  # noqa: F401
    apply_all_governance_tags,
    apply_governance_tags,
)
from flowx.lakeflow_framework.observability.structured_logger import log_flow_event
from flowx.lakeflow_framework.storage.table_properties import qualified_table_name

logger = logging.getLogger("flowx.lakeflow_framework.control_plane.post_deployment")

# Mirrors storage/table_properties.py's own _CDC_DISPATCHED_STRATEGIES -- kept as a local
# literal (rather than importing that module's private constant) for the same reason
# engine/sink_registration.py duplicates dq/quarantine.py's _QUARANTINE_PROCESS_COLUMNS: this
# module's own private set of "which strategies actually commit through apply_changes/
# apply_changes_from_snapshot, and so have Change Data Feed enabled at all" is a small, stable
# fact about the CDC engine, not something worth a shared-constant refactor across module
# ownership boundaries for this phase.
_CDC_DISPATCHED_STRATEGIES = {"SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"}


def _advance_cdc_watermark(
    spark: SparkSession,
    control_schema: str,
    spec_table: str,
    pk_column: str,
    pk_value: str,
    new_version: int,
) -> None:
    """Persist ``new_version`` as ``last_processed_cdc_version`` for one flow's spec row.

    Mirrors the MERGE-based upsert pattern every control-table write in this project already
    uses (see ``onboarding/metadata_upsert.py``: ``DeltaTable.forName(...).merge(...)
    .whenMatchedUpdate(...)``) rather than a hand-rolled ``UPDATE ... SET`` SQL string. Only
    ``whenMatchedUpdate`` is needed -- the row being updated was, by construction, already read
    by this same call via ``load_active_group_metadata``, so it is guaranteed to already exist.

    Raises
    ------
    Exception
        Propagated to the caller on any MERGE failure (e.g. the target catalog/schema is
        unreachable, or ``last_processed_cdc_version`` doesn't exist yet on a control table
        that hasn't been re-provisioned since this column was added -- see
        ``ddl_definitions.py``). The caller wraps this in its own try/except, exactly like the
        CDF/history read it guards alongside.
    """
    source_df = spark.createDataFrame(
        [(pk_value, int(new_version))],
        schema=f"{pk_column} STRING, last_processed_cdc_version LONG",
    )
    target = DeltaTable.forName(spark, f"{control_schema}.{spec_table}")
    (
        target.alias("t")
        .merge(source_df.alias("s"), f"t.{pk_column} = s.{pk_column}")
        .whenMatchedUpdate(set={"last_processed_cdc_version": "s.last_processed_cdc_version"})
        .execute()
    )


def capture_all_scd_change_counts(spark: SparkSession, control_catalog: str, group_id: str) -> None:
    """Best-effort insert/update/delete count capture (Phase 10) for every CDC-dispatched flow
    in ``group_id``, emitted as structured JSON log events via
    ``observability.structured_logger.log_flow_event`` -- the "records updated, inserted,
    deleted" half of this framework's structured-logging requirement for SCD/CDC flows, closing
    out the dependency an earlier phase's own code comment in ``cdc/change_metrics.py`` left for
    this one.

    **Why this runs here, post-deployment, instead of inside the pipeline graph** -- see this
    module's own docstring's "Phase 10" section.

    **The watermark-based version range.** ``cdc/change_metrics.py::capture_scd_change_counts``
    needs a Delta commit version range. Rather than looking only at the target's single most
    recent commit (the original, provably wrong heuristic -- see the architecture review's
    Finding 5.1 and this module's own docstring), this function persists a
    ``last_processed_cdc_version`` watermark on the flow's own spec row
    (``ingestion_flow_spec.last_processed_cdc_version`` /
    ``transformation_flow_spec.last_processed_cdc_version`` -- whichever table the flow belongs
    to; that row already carries this exact target's identity 1:1, and is already loaded once
    per call by ``load_active_group_metadata``, so reading the watermark costs no extra round
    trip) and queries ``table_changes(target, last_processed_cdc_version + 1, current_version)``
    -- the full range of commits since the last successful capture, not just the latest one:

    * ``last_processed_cdc_version`` is ``NULL`` (never captured before): starts from version 0
      (table creation), so the first capture after this fix ships reflects every commit the
      target has ever had, not just its latest.
    * ``current_version <= last_processed_cdc_version`` (no new commit since the last run --
      e.g. an ingestion flow with no new source data): the range query is skipped entirely and
      a zero-count ``SUCCESS`` event is emitted, rather than re-reporting the prior run's counts
      as if they were new.
    * Otherwise: the range covers every commit since the watermark, so a triggered update that
      produced more than one internal micro-batch commit to the same target is fully reflected,
      not just its last commit.

    The watermark is advanced (via :func:`_advance_cdc_watermark`) only *after*
    ``capture_scd_change_counts`` succeeds, immediately before the ``SUCCESS`` event is logged --
    if persisting the new watermark itself fails, no ``SUCCESS`` event is emitted for this flow
    this pass (the exception falls through to the same ``FAILED``-event handling below), and the
    next run naturally retries the same range from the last successfully persisted watermark
    (over-counting on a rare retry is the safe failure mode here, not under-counting).

    Never raises -- a CDF/history query failure, or a watermark-persist failure, for one target
    (e.g. a flow with a CDC strategy configured but whose target table was never actually
    created/committed, on a group's very first deployment attempt) is logged as a FAILED
    structured event and skipped, exactly like every other call site in this phase: a
    metrics-capture bug must never fail the governance job it rides alongside.
    """
    control_schema = f"{control_catalog}.config"
    md = load_active_group_metadata(spark, control_catalog, group_id)

    tagged_rows = [("ingestion_flow_spec", "dataflow_id", row) for row in md.ingestion_rows] + [
        ("transformation_flow_spec", "flow_step_id", row) for row in md.transformation_rows
    ]

    for spec_table, pk_column, flow_row in tagged_rows:
        cdc_load_strategy = getattr(flow_row, "cdc_load_strategy", None)
        if cdc_load_strategy not in _CDC_DISPATCHED_STRATEGIES:
            continue

        flow_id = getattr(flow_row, "dataflow_id", None) or getattr(flow_row, "flow_step_id", None) or flow_row.target_table
        start = time.monotonic()
        try:
            pk_value = getattr(flow_row, pk_column)
            qualified_table = qualified_table_name(flow_row.target_catalog, flow_row.target_schema, flow_row.target_table)
            history = spark.sql(f"DESCRIBE HISTORY {qualified_table} LIMIT 1").collect()
            if not history:
                logger.info(
                    "No commit history yet for '%s' (flow '%s') -- skipping SCD change-count capture.",
                    qualified_table,
                    flow_id,
                )
                continue

            current_version = history[0]["version"]
            last_processed_version = getattr(flow_row, "last_processed_cdc_version", None)

            if last_processed_version is not None and current_version <= last_processed_version:
                # No new commit since the last successful capture -- report zero cleanly rather
                # than re-reporting the previous run's already-captured counts (see this
                # function's docstring).
                logger.info(
                    "No new commits for '%s' (flow '%s') since last-processed version %d -- "
                    "reporting zero change counts.",
                    qualified_table,
                    flow_id,
                    last_processed_version,
                )
                log_flow_event(
                    operation="cdc_change_capture",
                    flow_id=flow_id,
                    status="SUCCESS",
                    records_written=0,
                    duration_ms=(time.monotonic() - start) * 1000.0,
                    target_table=qualified_table,
                    cdc_load_strategy=cdc_load_strategy,
                    captured_version=current_version,
                    inserted_count=0,
                    updated_count=0,
                    deleted_count=0,
                )
                continue

            starting_version = 0 if last_processed_version is None else last_processed_version + 1
            ending_version = current_version
            counts = capture_scd_change_counts(spark, qualified_table, starting_version, ending_version)

            _advance_cdc_watermark(spark, control_schema, spec_table, pk_column, pk_value, ending_version)

            log_flow_event(
                operation="cdc_change_capture",
                flow_id=flow_id,
                status="SUCCESS",
                records_written=counts["inserted_count"] + counts["updated_count"] + counts["deleted_count"],
                duration_ms=(time.monotonic() - start) * 1000.0,
                target_table=qualified_table,
                cdc_load_strategy=cdc_load_strategy,
                captured_version=ending_version,
                starting_version=starting_version,
                **counts,
            )
        except Exception as exc:  # noqa: BLE001 -- never fail the governance job over a metrics-capture problem
            log_flow_event(
                operation="cdc_change_capture",
                flow_id=flow_id,
                status="FAILED",
                error=str(exc),
                duration_ms=(time.monotonic() - start) * 1000.0,
                cdc_load_strategy=cdc_load_strategy,
            )
            logger.warning("Failed to capture SCD change counts for flow '%s': %s", flow_id, exc)

    logger.info("SCD change-count capture complete for group '%s'", group_id)
