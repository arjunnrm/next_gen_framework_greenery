"""Shared flow-output registration: staged view -> main/quarantine tables -> CDC dispatch.

``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` has two callers -- ingestion and
transformation -- that build their staged DataFrame differently (read a source vs. run SQL
over registered inputs) but converge on an *identical* shape once the staged view exists:
apply DQ-driven quarantine columns, register the main table (and quarantine sibling, if
configured), then apply the configured CDC/materialization strategy. Centralizing that
convergent shape here means a fix or enhancement (e.g. new quarantine metadata, a new CDC
strategy) is written once and used by both engines -- the two callers only differ in how
they build the DataFrame that feeds `dlt.view`.

Note that "no CDC dispatch" (``_NO_OP_CDC_STRATEGIES``) has never meant "no per-strategy
behaviour at all": those flows are materialized directly by
``dq/quarantine.py::register_main_and_quarantine_tables``. (v1.3.0 also applied a
``TRUNCATE_AND_LOAD`` empty-source guard on that path; it was withdrawn on 2026-08-29 as
inexpressible in a Lakeflow graph -- see ``cdc/dispatcher.py::resolve_truncate_and_load_source``.)
This module's dispatch decision is therefore about *which registration mechanism*
publishes the target, not about which strategies carry semantics -- see
:func:`register_flow_output`.
"""

import logging
from typing import Any, Callable, Dict, List, Optional

import dlt
from pyspark.sql import DataFrame

from flowx.lakeflow_framework.cdc.dispatcher import register_cdc_strategy
from flowx.lakeflow_framework.crypto.column_crypto import apply_aes_column_encryption
from flowx.lakeflow_framework.dq.expectations import apply_dq_expectations
from flowx.lakeflow_framework.dq.quarantine import (
    add_quarantine_columns,
    register_main_and_quarantine_tables,
)
from flowx.lakeflow_framework.engine.sink_registration import (
    register_external_sink_export,
    register_sink_target,
)
from flowx.lakeflow_framework.ingestion.technical_metadata import (
    attach_framework_ingestion_timestamp,
)
from flowx.lakeflow_framework.observability.structured_logger import logged_operation
from flowx.lakeflow_framework.storage.table_properties import build_table_properties, qualified_table_name

logger = logging.getLogger("flowx.lakeflow_framework.engine.flow_registration")

# "No-op" strictly in the sense of *CDC dispatch*: APPEND and TRUNCATE_AND_LOAD need no
# apply_changes[_from_snapshot] call, so their target is published directly as a @dlt.table fed
# by register_main_and_quarantine_tables's _clean_upstream(). It does NOT mean these strategies
# are semantically inert -- TRUNCATE_AND_LOAD's empty-source guard (E09) lives in that same
# _clean_upstream closure. Kept in sync with cdc/dispatcher.py::_NO_OP_STRATEGIES, which returns
# early for exactly this set.
_NO_OP_CDC_STRATEGIES = {"APPEND", "TRUNCATE_AND_LOAD"}


def register_staged_view(
    staged_view_name: str,
    comment: str,
    dq_rules: List[Dict[str, Any]],
    build_dataframe: Callable[[], DataFrame],
    target_config: Dict[str, Any],
    pipeline_run_id: Optional[str] = None,
    record_id_column: Optional[str] = None,
    capture_technical_metadata: bool = True,
    flow_id: Optional[str] = None,
    read_operation_name: str = "flow_read",
    materialize: bool = False,
) -> str:
    """Register the staged intermediate shared by both engines, as a ``@dlt.view`` (default)
    or, when ``materialize`` is set, a pipeline-scoped **temporary** ``@dlt.table``.

    **Intra-flow read-once fix.** A ``@dlt.view`` is inlined into every consumer, so each
    consumer opens its own independent read of whatever this view's closure reads -- for a
    streaming Auto Loader source with more than one quarantine/sink reader of the staged
    view, that means more than one ``cloudFiles`` stream sharing one
    ``cloudFiles.schemaLocation``, which is exactly the intra-flow Rule-2 (read-once)
    violation this parameter exists to close. Passing ``materialize=True`` registers this
    same closure as a ``@dlt.table(temporary=True)`` instead, so every downstream reader
    shares ONE materialized result while the intermediate never appears in Unity Catalog
    (the v1.6.0 Intermediate Object Rule -- only final sinks are durable published tables;
    before v1.6.0 this was a published, fully-qualified ``@dlt.table``). The three call
    sites this serves today (all read the staged view/table by name after this function
    returns): ``dq/quarantine.py``'s ``_clean_upstream`` and ``_quarantine_table`` closures
    inside ``register_main_and_quarantine_tables``, and ``engine/sink_registration.py``'s
    sink/external-sink registration path.

    Parameters
    ----------
    staged_view_name:
        Name the staged view/table is registered under (by convention
        ``_<target_table>_staged``). Always the bare, pipeline-local ``name=`` argument --
        for the ``@dlt.view`` and the temporary ``@dlt.table`` alike.
    comment:
        DLT comment for the view/table.
    dq_rules:
        This flow's DQ rules -- drives both the native ``warn``/``drop``/``fail``
        expectations decorator and the manual quarantine-column derivation.
    build_dataframe:
        Zero-argument callable that produces the flow's raw staged DataFrame (reads the
        ingestion source, or runs the transformation SQL) -- the one thing that genuinely
        differs between the ingestion and transformation engines. The ingestion engine's
        callable owns the whole source-shaping chain (schema_config -> column normalization ->
        technical metadata -> JSON-string parsing -> explode/auto-flatten -> full-row dedup ->
        data_standardization_sql; see ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``
        for why that order is the only correct one). Everything this function adds afterwards --
        the framework ingestion timestamp, AES column encryption, quarantine columns -- is
        therefore applied to the *final* ingested row shape. That split matters for the dedup
        step: the per-run/per-file ``__framework_*`` columns already on the DataFrame at that
        point are excluded from the dedup subset by prefix (``ingestion/dedup.py``), and
        ``__framework_ingestion_timestamp_utc`` is not even attached until after the callable
        returns -- so neither can make an otherwise-duplicate row look distinct.
    target_config:
        Parsed ``target_config_json``; consulted for ``encrypted_columns`` (output-column
        encryption only in the v2 schema -- decryption happens per-``source_inputs`` entry,
        not here; see ``transformation/inputs.py``).
    pipeline_run_id:
        Best-effort run/update identifier attached to quarantine metadata for traceability.
    record_id_column:
        ``dq_config.record_id_column`` (v2 schema -- moved off ``target_config``), or
        ``None``.
    capture_technical_metadata:
        Gates ``__framework_ingestion_timestamp_utc`` (see
        ``ingestion/technical_metadata.py::attach_framework_ingestion_timestamp``) --
        ``source_config.capture_technical_metadata`` for ingestion flows,
        ``target_config.capture_technical_metadata`` for transformation flows (which have
        no ``source_config``). Default ``True``.
    flow_id:
        ``dataflow_id``/``flow_step_id`` -- when supplied, ``build_dataframe()`` (the one
        genuinely engine-specific step: reading the ingestion source, or running the
        transformation SQL) is timed and wrapped in
        ``observability.structured_logger.logged_operation``, emitting one structured JSON
        log event (``read_operation_name``) recording success/failure and duration. This is
        this phase's (Phase 10) "structured logging for every ingestion/transformation
        operation" wiring point: both engines converge on this one call, at the one place a
        ``flow_id`` is actually in scope for the shared code path -- see this framework's
        Phase 10 module (``observability/structured_logger.py``) for why record counts are
        deliberately *not* attempted here (``build_dataframe()`` only builds a lazy plan --
        for a streaming source, or any batch source, calling ``.count()`` at this point would
        mean an extra, and for a streaming plan an outright illegal, eager Spark action before
        Lakeflow itself ever materializes anything). ``None`` (the default) skips structured
        logging entirely -- kept optional rather than required so any caller/test that doesn't
        care about it is unaffected.
    read_operation_name:
        The ``operation`` label used for the structured log event above -- callers pass
        ``"ingestion_read"``/``"transformation_execute"`` to distinguish the two engines in
        the emitted JSON. Ignored when ``flow_id`` is ``None``.
    materialize:
        ``False`` (default): register as ``@dlt.view(name=staged_view_name,
        comment=comment)``. ``True``: register as ``@dlt.table(name=staged_view_name,
        temporary=True, comment=comment)`` instead -- materialized once for every
        downstream reader, but pipeline-scoped and never published to Unity Catalog. Both
        registrations use the same bare, pipeline-local name, so downstream readers resolve
        the staged intermediate identically either way. The
        ``@apply_dq_expectations(dq_rules)`` decorator stays stacked directly under either
        one, unchanged. Callers decide ``materialize`` per flow (e.g. "this flow has
        quarantine rules or more than one reader of the staged view"); this function only
        carries out that decision.

    Returns
    -------
    str
        The name downstream readers must use for this staged intermediate -- always the
        bare, pipeline-local ``staged_view_name`` (both a ``@dlt.view`` and a temporary
        ``@dlt.table`` are resolved by their pipeline-local name). Kept as the return
        contract so callers stay agnostic to the view-vs-temporary-table decision.

    Raises
    ------
    FrameworkConfigError
        Propagated from encryption/decryption or quarantine-column derivation on malformed
        configuration.
    """
    if materialize:
        # temporary=True: materialized (so the multi-reader R2 guarantee holds -- every
        # downstream reader shares ONE physical result) but pipeline-scoped, never published
        # to Unity Catalog. The v1.6.0 Intermediate Object Rule: an intermediate is a
        # @dlt.view when it has a single reader, a temporary @dlt.table when it has more --
        # only final sinks (targets, quarantine, SCD2 _current) are durable published tables.
        # A temporary table always uses its bare, pipeline-local name; the pre-v1.6.0
        # qualified catalog.schema publication of staged intermediates is gone.
        register_dataset = dlt.table(name=staged_view_name, temporary=True, comment=comment)
    else:
        register_dataset = dlt.view(name=staged_view_name, comment=comment)

    @register_dataset
    @apply_dq_expectations(dq_rules)
    def _staged_view():
        if flow_id is not None:
            with logged_operation(read_operation_name, flow_id, staged_view_name=staged_view_name):
                staged_df = build_dataframe()
        else:
            staged_df = build_dataframe()
        staged_df = attach_framework_ingestion_timestamp(staged_df, capture_technical_metadata)

        encrypted_columns = target_config.get("encrypted_columns", [])
        if encrypted_columns:
            # original_types (output_column -> pre-encryption Spark type) is intentionally
            # not consumed here: this closure runs lazily, at Lakeflow graph-EXECUTION time,
            # while the original_data_type column tag can only be applied once the target
            # table is materialized -- a separate, later step (see
            # docs/16_encryption_and_secrets.md's "type-tag capture" note; tracked as
            # follow-up work, not yet wired to a post-deployment tag-application pass).
            staged_df, _original_types = apply_aes_column_encryption(staged_df, encrypted_columns)

        return add_quarantine_columns(
            staged_df,
            dq_rules,
            pipeline_run_id=pipeline_run_id,
            record_id_column=record_id_column,
        )

    return staged_view_name


def register_flow_output(
    flow_label: str,
    staged_view_name: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    cdc_load_strategy: str,
    target_config: Dict[str, Any],
    dq_rules: List[Dict[str, Any]],
    comment: Optional[str],
    is_streaming: bool,
    target_type: str,
    quarantine_table_override: Optional[str] = None,
) -> bool:
    """Register this flow's output according to its ``target_type`` -- the shared tail end
    of both ``generate_ingestion_flow`` and ``generate_transformation_flow``.

    Quarantine-row traceability (``__framework_pipeline_run_id``/``__framework_record_id``) is already baked into
    the staged view's columns by :func:`register_staged_view` -- nothing extra to thread
    through here.

    Parameters
    ----------
    flow_label:
        The flow's identifier (``dataflow_id`` or ``flow_step_id``), used only for error
        context.
    staged_view_name, target_table, target_catalog, target_schema,
    cdc_load_strategy, target_config, dq_rules, comment, is_streaming:
        Same meaning as the corresponding ``ingestion_flow_spec``/``transformation_flow_spec``
        columns. ``target_catalog``/``target_schema`` are forwarded to every table/view this
        flow publishes -- a bare ``target_table`` resolves against the *pipeline's* own
        default catalog/schema, not this flow's configured target (see
        ``common.storage.table_properties.qualified_table_name``).
    target_type:
        ``flow_row.target_type`` -- one of ``streaming_table``/``materialized_view``/
        ``batch_table`` (all three: unchanged, see below)/``external_sink``/``sink``
        (Phase 7 dispatch, see ``engine/sink_registration.py``).
    quarantine_table_override:
        ``dq_config.quarantine_table`` (v2 schema -- moved off ``target_config``), or
        ``None`` to use the default ``"<target_table>_quarantine"`` name.

    ``target_type`` dispatch (Phase 7 -- "Sink rebuild"):

    * ``"sink"``: NO materialized table, ever -- delegates entirely to
      :func:`sink_registration.register_sink_target`, which feeds the staged view directly
      into a genuine ``dlt.create_sink``/``@dlt.append_flow`` pair. Neither
      ``register_main_and_quarantine_tables`` nor CDC dispatch ever runs for this target
      type (a sink flow has no main/clean table -- see that function's docstring for why
      its DQ *quarantine* table can still exist independently).
    * ``"external_sink"``: registered exactly like ``streaming_table``/``materialized_view``/
      ``batch_table`` below (real, qualified main table + CDC dispatch, unchanged), PLUS a
      second, separate ``@dlt.append_flow`` exporting that now-materialized table via
      :func:`sink_registration.register_external_sink_export`. Before this phase,
      ``external_sink`` egress ran as a *post-deployment* plain-Spark write (the now-removed
      ``control_plane/post_deployment.py::run_external_sink_exports``) -- a real design
      defect the project's own requirement calls out directly ("all external outputs must
      use genuine Lakeflow/DLT sink functionality... not ordinary DAG table writes"), on top
      of an earlier, separately-discovered bug where that post-deployment step tried to read
      the flow's ephemeral staged `@dlt.view` by name: a `@dlt.view` is never a durable,
      queryable object outside the pipeline's own graph execution (confirmed empirically --
      it never appears in `SHOW TABLES` for any schema once the update finishes), which is
      exactly why that export must run *inside* the same graph execution that materializes
      the table it reads from, never as a separate, later job task.
    * ``"streaming_table"``/``"materialized_view"``/``"batch_table"``: unchanged by Phase 7
      -- real, qualified main table (or internal clean view + CDC-dispatched target) via
      ``register_main_and_quarantine_tables``/``register_cdc_strategy``, same as before that
      phase, zero behavior difference.

    ``cdc_load_strategy`` dispatch (v1.3.0 note): ``needs_cdc_dispatch`` being ``False`` for
    ``APPEND``/``TRUNCATE_AND_LOAD`` (see ``_NO_OP_CDC_STRATEGIES``) selects the *registration
    mechanism*, not the absence of strategy semantics. A ``TRUNCATE_AND_LOAD`` target is a full
    recompute on every update, so an empty source can silently blank it. v1.3.0 guarded that with
    ``target_config.empty_target_if_source_empty`` inside
    ``register_main_and_quarantine_tables``'s ``_clean_upstream`` closure; the guard was withdrawn
    on 2026-08-29 because preserving the target requires it to read itself, which Lakeflow rejects
    as a graph cycle. The option is currently unenforced and needs a post-update check outside the
    pipeline graph -- see ``cdc/dispatcher.py::resolve_truncate_and_load_source``.

    This whole function's body (Phase 10) is wrapped in
    ``observability.structured_logger.logged_operation("flow_registration", flow_label, ...)``,
    emitting one structured JSON log event on exit: ``SUCCESS`` on either ``return True`` path
    below, or ``FAILED`` (carrying the real exception's message) if anything raises -- the
    exception itself is always re-raised unchanged afterward, never swallowed. This is the
    "flow-registration-complete event (status, target table, CDC strategy)" call point both
    engines converge on -- see :func:`register_staged_view` for the companion read/execute-side
    event.

    Returns
    -------
    bool
        Always ``True`` on success.

    Raises
    ------
    FrameworkConfigError, CdcStrategyError
        Propagated from table/CDC/sink registration on malformed configuration -- including,
        for ``"sink"``/``"external_sink"``, the flow (or its main table) not being genuinely
        streaming, which ``dlt.create_sink``/``@dlt.append_flow`` requires.
    """
    with logged_operation(
        "flow_registration",
        flow_label,
        target_table=target_table,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_type=target_type,
        cdc_load_strategy=cdc_load_strategy,
    ):
        if target_type == "sink":
            register_sink_target(
                flow_label,
                staged_view_name,
                target_table,
                target_catalog,
                target_schema,
                target_config,
                dq_rules,
                is_streaming,
                quarantine_table_override=quarantine_table_override,
            )
            logger.info(
                "Registered flow '%s' -> Lakeflow sink for '%s.%s.%s' (target_type=sink, no materialized table)",
                flow_label,
                target_catalog,
                target_schema,
                target_table,
            )
            return True

        needs_cdc_dispatch = cdc_load_strategy not in _NO_OP_CDC_STRATEGIES
        cdc_source_view = register_main_and_quarantine_tables(
            staged_view_name,
            target_table,
            target_catalog,
            target_schema,
            target_config,
            dq_rules,
            comment,
            is_streaming,
            needs_cdc_dispatch,
            quarantine_table_override=quarantine_table_override,
            flow_label=flow_label,
        )

        if needs_cdc_dispatch:
            register_cdc_strategy(
                flow_label,
                cdc_load_strategy,
                cdc_source_view,
                target_table,
                target_catalog,
                target_schema,
                target_config,
                build_table_properties(target_config),
                is_streaming,
            )

        if target_type == "external_sink":
            if needs_cdc_dispatch:
                # Every CDC-dispatched strategy except SCD3 publishes its target via
                # dlt.create_streaming_table + apply_changes[_from_snapshot] (see
                # cdc/scd.py, cdc/snapshot.py) -- a genuine Lakeflow Streaming Table,
                # unconditionally, regardless of this flow's own is_streaming value. SCD3 is the
                # one exception: its *public* target table is a derived @dlt.table pivot
                # (window functions over an internal streaming history table, via a plain
                # dlt.read -- see cdc/scd.py::register_scd3), which is a batch dataset, not a
                # streaming one.
                main_table_is_streaming = cdc_load_strategy != "SCD3"
            else:
                # APPEND/TRUNCATE_AND_LOAD: the main table's streaming-ness is exactly this
                # flow's own is_streaming (it's fed directly by dlt.read_stream/dlt.read on the
                # staged view inside register_main_and_quarantine_tables's _clean_upstream()).
                main_table_is_streaming = is_streaming
            qualified_main_table = qualified_table_name(target_catalog, target_schema, target_table)
            register_external_sink_export(flow_label, qualified_main_table, target_table, target_config, main_table_is_streaming)

        logger.info(
            "Registered flow '%s' -> '%s.%s.%s' (target_type=%s, strategy=%s)",
            flow_label,
            target_catalog,
            target_schema,
            target_table,
            target_type,
            cdc_load_strategy,
        )
        return True
