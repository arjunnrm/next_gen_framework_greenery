"""``read_mode: "streaming"`` reconciliation -- a ``foreachBatch``-driven incremental
alternative to ``appender.py``'s batch point-in-time-snapshot + fingerprint-dedup path.

Restartability here comes from Spark Structured Streaming's own checkpoint (source offset
tracking), not ``reconciliation_run_log.source_batch_fingerprint`` -- the DDL's own comment on
that column is explicit that the fingerprint mechanism is "batch read_mode only". A crashed or
manually-cancelled streaming run resumes exactly where its checkpoint left off on the next
invocation, the same restart guarantee every other streaming source in this framework already
gets from Structured Streaming, without this module reimplementing it.

**Reuse, not duplication.** Each micro-batch handed to ``foreachBatch`` is, by Spark's own
contract, a plain *static* DataFrame -- already materialized for that batch's offset range --
so it is indistinguishable from a batch-mode read once it reaches this module. Rather than
reimplementing matching/appending/logging for the streaming case, every micro-batch is run
through the exact same ``matcher.py``/``appender.py``/``mismatch_logging.py`` functions the
batch path uses (via ``appender.py::run_target_reconciliation``), just scoped to one
micro-batch's rows instead of one whole-table snapshot. This does mean each micro-batch also
pays ``run_target_reconciliation``'s own (batch-oriented) fingerprint check -- harmless,
slightly redundant overhead given Spark's checkpoint already guarantees each micro-batch's data
is new, not a correctness concern.

**Trigger choice: ``availableNow``, always (v1.4.0).** ``05_reconciliation_engine.py`` is a
plain job-task notebook, run on demand by a Lakeflow Job -- not an always-on service.
``trigger(availableNow=True)`` processes every currently-available record (across as many
internally-chunked micro-batches as needed) and then stops on its own, so
``query.awaitTermination()`` returns once the run is caught up, matching a bounded job-task's own
run/finish lifecycle instead of running forever.

There used to be a second trigger here, selected by ``recon_mode: "continuous"``: the same
``foreachBatch`` handler under Spark's default (or a fixed ``processingTime``) trigger, running
indefinitely. ``recon_mode`` is removed, and so is that branch. It was a standing stream wrapped
around a *batch-shaped* unit of work -- ``run_target_reconciliation`` writes one
``reconciliation_run_log`` row per invocation, checks a batch fingerprint for idempotency, and
reports one set of aggregate counts -- so a continuous query produced a log row per micro-batch
whose fingerprint could never repeat and whose counts described an arbitrary slice of time rather
than a comparison anyone asked for. Reconciliation answers "do these two datasets agree *as of
now*", which has a boundary in it; the way to ask it more often is to schedule the job more often,
not to remove the boundary.

**``task_run_id``.** The caller passes the job's ``task_run_id`` down; this module forwards it to
the *static* side's per-micro-batch read only, so a side declaring a ``task_run_id_column`` is
narrowed to the producing run this reconciliation is about (see ``dataset_reader.py``). The
*streaming* side is never narrowed that way: its micro-batch boundary already is the batch
boundary, and filtering it by a single producing run id would silently discard every offset that
belongs to any other one -- data the checkpoint has already advanced past and will never re-offer.
``task_run_id`` also travels into every log/result row as a correlation value -- narrowing and
correlating are two different jobs done by the same value.

**Unsupported: both sides streaming.** A given target's comparison supports at most one
streaming side (``source_config`` *or* that ``target_configs[]`` entry, not both) -- a
stream-stream join would need watermarking and only supports inner/left-outer semantics,
incompatible with this framework's full four-way MATCHED/MISSING_IN_TARGET/MISSING_IN_SOURCE/
VALUE_DRIFT classification (see ``matcher.py``). This is a deliberate, documented scope
boundary, not an oversight -- configure the non-streaming side as ``read_mode: "batch"``.
"""

import logging
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.appender import run_target_reconciliation
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.dataset_reader import read_reconciliation_dataset
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.matcher import prepare_dataset_for_matching

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.reconciliation.streaming")


def run_streaming_target_reconciliation(
    spark: SparkSession,
    control_schema: str,
    reconciliation_id: str,
    source_config: Dict[str, Any],
    target_config: Dict[str, Any],
    match_keys: List[str],
    compare_columns: Optional[List[str]],
    transform_sql: Optional[str],
    checkpoint_location: str,
    parameters: Optional[Dict[str, Any]] = None,
    logging_config: Optional[Dict[str, Any]] = None,
    task_run_id: Optional[str] = None,
    recon_run_log_capture: Optional[bool] = None,
    recon_mismatch_log: Optional[bool] = None,
    two_tier_verification: bool = True,
) -> None:
    """Run one target's reconciliation as a ``foreachBatch`` streaming query.

    Called instead of ``appender.run_target_reconciliation`` (the batch entrypoint) whenever
    ``source_config.read_mode`` or this ``target_config``'s own ``read_mode`` is ``"streaming"``.
    Blocks until the ``availableNow`` trigger's backlog is fully drained (see this module's
    docstring) -- matched to a Lakeflow Job task's own bounded run/finish lifecycle.

    **v1.4.0 signature change (breaking):** ``generate_surrogate_key``, ``continuous`` and
    ``processing_time`` are all removed. ``generate_surrogate_key`` went with the surrogate-key
    engine; ``continuous``/``processing_time`` went with ``recon_mode``, leaving
    ``trigger(availableNow=True)`` as the only trigger this module ever installs. Callers passing
    any of the three must drop the argument.

    Parameters
    ----------
    source_config, target_config:
        This flow's raw ``source_config`` dict and this target's raw ``target_configs[]``
        entry -- read directly here (rather than pre-read DataFrames, as
        ``appender.run_target_reconciliation`` takes) because the streaming side must be read
        via ``.writeStream`` on this module's own schedule, and the static side must be
        re-read fresh for every micro-batch (see below).
    checkpoint_location:
        Where Structured Streaming persists this query's source offsets for restart safety.
        Must be unique per ``(reconciliation_id, target_id)`` -- this module has no naming
        convention of its own for it; the caller (``05_reconciliation_engine.py``) owns that
        decision, since it is an operational/storage concern, not a matching one.
    parameters:
        Dynamic runtime parameters for ``${param}`` substitution (``filter_condition``,
        ``transform_sql``).
    logging_config, task_run_id:
        Threaded straight through to ``appender.run_target_reconciliation`` for every
        micro-batch -- see that function's docstring. ``task_run_id`` is *additionally* used to
        narrow the static side's per-micro-batch read when that side declares a
        ``task_run_id_column`` -- see this module's docstring for why the streaming side is never
        narrowed that way.
    recon_run_log_capture, recon_mismatch_log, two_tier_verification:
        **SIGNATURE CHANGE (v1.3.0, backward compatible):** three new *trailing* keyword
        arguments, forwarded verbatim into ``appender.run_target_reconciliation`` for every
        micro-batch. They are not interpreted here at all -- this module's whole design point is
        that a micro-batch takes the identical code path a batch run takes, so anything that
        changes that path's behaviour must be passed through untouched rather than re-decided
        per batch. ``two_tier_verification`` is especially worth having on a streaming flow: a
        micro-batch that turns out to match its target end-to-end skips the join entirely, and
        on a backlog of many small batches that is the common case.

    Raises
    ------
    FrameworkConfigError
        If neither side is actually configured ``read_mode: "streaming"`` (use the batch path
        instead), if *both* sides are (unsupported -- see this module's docstring), or
        propagated from any function this delegates to.
    """
    target_id = target_config.get("target_id")
    source_streaming = source_config.get("read_mode") == "streaming"
    target_streaming = target_config.get("read_mode") == "streaming"

    if not source_streaming and not target_streaming:
        raise FrameworkConfigError(
            f"run_streaming_target_reconciliation called for target_id={target_id!r} but neither source_config "
            "nor this target's read_mode is 'streaming' -- use appender.run_target_reconciliation (the batch "
            "path) instead"
        )
    if source_streaming and target_streaming:
        raise FrameworkConfigError(
            f"target_id={target_id!r}: both source_config and this target are configured read_mode='streaming' "
            "-- stream-stream reconciliation is not supported (see streaming.py's module docstring). Configure "
            "at most one side of this target as streaming."
        )

    source_hash_precomputed = bool(source_config.get("hash_precomputed", False))
    streaming_config = source_config if source_streaming else target_config
    # The static side IS narrowed by task_run_id (when it declares a task_run_id_column) -- every
    # run is a bounded one now that reconciliation is triggered-only, so there is always exactly
    # one producing run to scope to. The streaming side is never narrowed; its own offsets are
    # already the batch boundary (see this module's docstring).
    static_side_task_run_id = task_run_id

    def _process_micro_batch(micro_batch_df: DataFrame, batch_id: int) -> None:
        """Spark's own ``foreachBatch`` contract hands this a plain, static DataFrame -- already
        materialized for this batch's offset range -- so every function it calls into is
        completely unmodified from the batch reconciliation path."""
        logger.info("Reconciliation '%s'/target '%s': processing streaming batch %d", reconciliation_id, target_id, batch_id)

        # The non-streaming side is re-read fresh on every micro-batch (never cached across
        # batches) so each comparison runs against that side's current state, not a snapshot
        # frozen from whenever this streaming query first started.
        static_df = read_reconciliation_dataset(
            spark,
            target_config if source_streaming else source_config,
            parameters,
            task_run_id=static_side_task_run_id,
        )
        source_raw_df = micro_batch_df if source_streaming else static_df
        target_raw_df = static_df if source_streaming else micro_batch_df

        prepared_source_df = prepare_dataset_for_matching(
            source_raw_df, match_keys, compare_columns, source_hash_precomputed
        )
        run_target_reconciliation(
            spark,
            control_schema,
            reconciliation_id,
            prepared_source_df,
            target_raw_df,
            target_config,
            match_keys,
            compare_columns,
            source_hash_precomputed,
            transform_sql,
            parameters,
            logging_config=logging_config,
            task_run_id=task_run_id,
            recon_run_log_capture=recon_run_log_capture,
            recon_mismatch_log=recon_mismatch_log,
            two_tier_verification=two_tier_verification,
        )

    try:
        # The streaming side is deliberately read WITHOUT a task_run_id narrowing -- see this
        # module's docstring: its own offsets are already the batch boundary, and filtering it
        # would silently drop offsets the checkpoint then advances past for good.
        streaming_df = read_reconciliation_dataset(spark, streaming_config, parameters)
        writer = (
            streaming_df.writeStream.foreachBatch(_process_micro_batch)
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
        )
        query = writer.start()
        query.awaitTermination()
    except FrameworkConfigError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(
            f"Streaming reconciliation failed for reconciliation_id='{reconciliation_id}', target_id='{target_id}': {exc}"
        ) from exc
