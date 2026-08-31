"""The three per-row flow generators -- ingestion, transformation, reconciliation.

**Why this module exists.** ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``'s own
header claims it "only wires metadata rows to ``@dlt.table``/``@dlt.view`` registrations", but
until v1.5.0 it also *contained* ~130 lines of ordering-critical ingestion/transformation logic
that nothing could import and therefore nothing ever unit-tested: the source-shaping overlay
chain, the ``auto_flatten_all`` present-vs-absent distinction, and the streaming-propagation
rule. Those bodies now live here, verbatim, as ordinary importable functions; the notebook keeps
only the three bare ``for`` loops that call them (a Lakeflow source notebook still has to
*invoke* registration at top level, because that is when Lakeflow's graph-definition phase
observes it -- but it no longer has to *implement* it).

Everything a generator used to read from notebook globals (``spark``, ``dbutils``,
``PIPELINE_PARAMETERS``, ``PIPELINE_RUN_ID``) is now an explicit argument, plus one genuinely new
one: the dataflow group's :class:`~engine.source_plane.SourcePlanePlan`. That plan is what makes
requirement R2 ("every physical source table/path is read exactly once per pipeline update")
true: instead of each flow issuing its own read, every generator resolves its input through
:func:`~engine.source_plane.bind` under a stable ``consumer_id``, and the plane decides whether
that resolves to an in-graph sibling reference, one shared materialized node, or today's inline
read.

**Consumer-id contract** (must stay byte-identical to ``engine/source_plane.py``'s own request
builders -- ``bind`` raises on an unknown id, it does not guess):

* ingestion source      -- ``f"{flow_row.dataflow_id}:source"``
* transformation input  -- ``f"{flow_row.flow_step_id}:input:{input_name}"`` (issued by
  ``transformation/inputs.py::register_transformation_inputs``, not here)
* reconciliation source -- ``f"{reconciliation_id}:source"`` (issued by
  ``reconciliation/graph_registration.py``, not here)

**What must not be "cleaned up" in here.** Three things in the moved bodies look like they could
be simplified and cannot; each is called out inline at its own line as well:

1. The ingestion overlay chain's ORDER -- ``schema_config`` -> ``normalize_column_names`` ->
   ``attach_technical_metadata`` -> ``parse_json_string_columns`` -> ``apply_explode_columns``
   -> ``apply_stream_dedup`` -> ``apply_data_standardization_sql``. Every adjacent pair has a
   documented reason (raw-vs-normalized column names, structs must exist before explode
   consumes them, dedup must see post-explode rows and pre-standardization determinism).
2. ``resolve_auto_flatten_all(source_config)`` takes the whole dict, NOT
   ``source_config.get("auto_flatten_all", False)``: a present-but-empty ``"explode_columns":
   []`` means "auto-flatten everything" while an absent/null one is a schema-preserving
   pass-through, and a ``.get()`` collapses "absent" and "present-but-empty" to one value.
3. The transformation ``is_streaming`` rule -- ``target_type == "streaming_table" or
   any(input.is_streaming)``. Dropping the second half raised a live
   ``AnalysisException: View '...' is a streaming view and must be referenced using
   readStream``.

**Staged-view materialization.** Both flow generators now pass ``materialize=`` to
:func:`~engine.flow_registration.register_staged_view` and use its RETURN VALUE as the name
handed to :func:`~engine.flow_registration.register_flow_output` -- a materialized staged
intermediate is published under a fully qualified ``catalog.schema.table`` name, and every
downstream ``dlt.read``/``dlt.read_stream`` of it must use that name rather than the bare
``_<target_table>_staged`` string. ``materialize`` is ``True`` exactly when the staged
intermediate has more than one reader: when the flow has ``action: "quarantine"`` DQ rules
(``dq/quarantine.py`` reads it once for the clean side and again for the quarantine sibling) or
when ``target_type`` is ``sink``/``external_sink`` (``engine/sink_registration.py`` reads it
again). That is the INTRA-flow half of R2 -- a ``@dlt.view`` is inlined into each consumer, so
"declared once" is not "read once".
"""

import json
import logging
from typing import Any, Dict, List, Optional

from NextGen_Metadata_Framework.lakeflow_framework.engine.flow_registration import (
    register_flow_output,
    register_staged_view,
)
from NextGen_Metadata_Framework.lakeflow_framework.engine.source_plane import SourcePlanePlan, bind
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.column_normalization import normalize_column_names
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.dedup import apply_stream_dedup
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.json_flattening import (
    apply_explode_columns,
    parse_json_string_columns,
    resolve_auto_flatten_all,
)
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.schema_config import (
    apply_schema_config,
    load_schema_config,
)
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.standardization_sql import apply_data_standardization_sql
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.technical_metadata import attach_technical_metadata
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.graph_registration import (
    register_reconciliation_flow,
)
from NextGen_Metadata_Framework.lakeflow_framework.transformation.inputs import (
    mark_streaming_references,
    register_transformation_inputs,
)
from NextGen_Metadata_Framework.lakeflow_framework.transformation.parameters import (
    substitute_dynamic_parameters,
    substitute_path_parameters,
)

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.engine.flow_generators")

__all__ = [
    "generate_ingestion_flow",
    "generate_transformation_flow",
    "generate_reconciliation_flow",
]

#: ``target_type`` values whose staged intermediate is read a second time by
#: ``engine/sink_registration.py`` (``register_sink_target``/``register_external_sink_export``),
#: making a ``@dlt.view`` staged intermediate a genuine second physical read.
_SINK_TARGET_TYPES = ("sink", "external_sink")

#: The only ``dq_rules`` action that causes ``dq/quarantine.py`` to read the staged intermediate
#: twice (``_clean_upstream`` for the non-quarantined side, ``_quarantine_table`` for the
#: quarantined sibling). ``warn``/``drop``/``fail`` rules are native expectations on the single
#: existing read and add no reader.
_QUARANTINE_RULE_ACTION = "quarantine"


def _needs_materialized_staged_view(dq_rules: List[Dict[str, Any]], target_type: Optional[str]) -> bool:
    """``True`` when this flow's staged intermediate has more than one reader.

    See this module's docstring ("Staged-view materialization") -- the two multi-reader cases
    are ``action: "quarantine"`` DQ rules and a ``sink``/``external_sink`` target.
    """
    has_quarantine_rules = any(rule.get("action") == _QUARANTINE_RULE_ACTION for rule in (dq_rules or []))
    return has_quarantine_rules or target_type in _SINK_TARGET_TYPES


#: Spark conf keys carrying the hosting pipeline's DECLARED target schema, most current first.
#: ``pipelines.schema`` is today's key; ``pipelines.target`` is its pre-``schema`` spelling, still
#: set by pipelines created before the rename.
_PIPELINE_SCHEMA_CONF_KEYS = ("pipelines.schema", "pipelines.target")


def resolve_pipeline_schema(spark: Any, group_row: Any = None) -> Optional[str]:
    """Resolve the schema this pipeline publishes into, or ``None`` if nothing can determine it.

    This is what a reconciliation flow's datasets are named under when the flow sets no explicit
    ``publish_schema``, so getting it wrong is not cosmetic: ``qualified_table_name`` rejects a
    ``None`` schema and the first ``_node_name()`` call in
    :mod:`~reconciliation.graph_registration` dies with
    ``ValueError: Unsafe or malformed target_schema: None``.

    Resolution order, and why it is not simply ``currentDatabase()``:

    1. ``spark.conf`` ``pipelines.schema`` / ``pipelines.target`` -- the pipeline's DECLARED target
       schema. Asked first because during Lakeflow's graph-definition phase the session's current
       database is NOT the pipeline's target schema. Observed live on 2026-08-31: pipeline
       ``be78d88d`` declares ``schema: bronze_excalibur`` and still resolved to no usable current
       database, which is the defect this function exists to close.
    2. ``spark.catalog.currentDatabase()`` -- correct outside a pipeline update (a plain notebook
       or a local Databricks Connect session).
    3. The group row's ``target_schema``, if the control table carries one.

    Lives here, rather than inline in the engine notebook, because
    ``tests/unit/test_pipeline_notebook_is_thin.py`` enforces that the notebook contains bare
    registration fan-outs and no computation -- a module-level ``for`` loop over conf keys is
    exactly the "decision logic escaping a module" that test rejects, and it would also have been
    unreachable by any unit test.

    Never raises: an unset conf key and an unavailable catalog are both normal, and a pipeline
    with no reconciliation flow never needs this value at all. The caller decides whether ``None``
    is fatal.
    """
    for conf_key in _PIPELINE_SCHEMA_CONF_KEYS:
        try:
            declared = spark.conf.get(conf_key)
        except Exception:  # noqa: BLE001 -- an unset conf key is normal, not an error
            declared = None
        if declared:
            return declared

    try:
        current = spark.catalog.currentDatabase()
    except Exception:  # noqa: BLE001 -- unavailable outside a live session
        current = None
    if current:
        return current

    return getattr(group_row, "target_schema", None)


def generate_ingestion_flow(
    spark,
    dbutils,
    flow_row,
    *,
    plan: SourcePlanePlan,
    pipeline_parameters: Dict[str, Any],
    pipeline_run_id: Optional[str] = None,
) -> None:
    """Register one ``ingestion_flow_spec`` row's staged intermediate + flow output.

    Parameters
    ----------
    spark:
        Active ``SparkSession``. Kept for interface symmetry with the other two generators and
        for any future direct use; the source read itself no longer goes through it -- see
        ``plan`` below (:func:`~engine.source_plane.bind` resolves its own active session for an
        ``inline`` binding).
    dbutils:
        The notebook's ``dbutils`` handle, used only by
        ``ingestion/schema_config.py::load_schema_config`` to read
        ``source_config.schema_config_path`` off a workspace/volume path.
    flow_row:
        One active ``ingestion_flow_spec`` row (a Spark ``Row``, or any duck-typed stand-in).
    plan:
        This dataflow group's :class:`~engine.source_plane.SourcePlanePlan`, already built by
        :func:`~engine.source_plane.plan_source_plane` and registered by
        :func:`~engine.source_plane.register_source_plane`. This row's source is resolved
        through it under consumer id ``f"{flow_row.dataflow_id}:source"``.
    pipeline_parameters:
        The group's ``${param}`` substitution map (``dataflow_group_spec.pipeline_parameters_json``).
    pipeline_run_id:
        Best-effort run/update identifier, attached to quarantine metadata for traceability.

    Raises
    ------
    FrameworkConfigError
        On malformed JSON in ``source_config_json``/``target_config_json``/``dq_config_json``,
        or propagated from :func:`~engine.source_plane.bind` (unknown consumer id / an illegal
        streaming-vs-batch bind) and from the registration helpers.
    """
    try:
        # ${param} placeholders in path-bearing fields (source_config/target_config) are
        # resolved fresh from pipeline_parameters on every pipeline update -- see
        # transformation/parameters.py::substitute_path_parameters's docstring. dq_config is
        # deliberately excluded (its expr fields are SQL predicates, not paths).
        source_config = (
            json.loads(substitute_path_parameters(flow_row.source_config_json, pipeline_parameters))
            if flow_row.source_config_json
            else {}
        )
        target_config = (
            json.loads(substitute_path_parameters(flow_row.target_config_json, pipeline_parameters))
            if flow_row.target_config_json
            else {}
        )
        dq_config = json.loads(flow_row.dq_config_json) if flow_row.dq_config_json else {}
    except json.JSONDecodeError as exc:
        raise FrameworkConfigError(f"Ingestion flow '{flow_row.dataflow_id}': malformed JSON configuration: {exc}") from exc

    dq_rules = dq_config.get("rules", [])
    is_streaming = flow_row.target_type == "streaming_table"
    staged_view_name = f"_{flow_row.target_table}_staged"
    source_consumer_id = f"{flow_row.dataflow_id}:source"

    def _build_ingestion_dataframe():
        # bind(), not a direct read_ingestion_source(spark, ...): this flow's physical source
        # is resolved through the group's L0 source plane, so a locator two or more consumers
        # share is read ONCE per update (requirement R2) and a locator this same group
        # publishes becomes a real in-graph edge instead of a second physical read. A
        # single-consumer external locator still resolves to the byte-identical inline
        # read_ingestion_source(...) call this line replaced -- see engine/source_plane.py.
        staged_df = bind(plan, source_consumer_id, want_stream=is_streaming)
        # schema_config (explicit type/rename/comment) runs first -- its source_name keys
        # reference the source's true raw column names, before anything else here touches
        # them. normalize_column_names runs next, over whatever names remain (including any
        # column schema_config didn't cover) -- see ingestion/column_normalization.py's module
        # docstring for why this exact order matters.
        schema_config_path = source_config.get("schema_config_path")
        if schema_config_path:
            schema_config = load_schema_config(dbutils, schema_config_path)
            staged_df = apply_schema_config(staged_df, schema_config)
        staged_df = normalize_column_names(staged_df, source_config)
        staged_df = attach_technical_metadata(staged_df, source_config)
        # parse_json_string_columns turns STRING columns holding a JSON document into real
        # structs, which is what lets a Parquet/CSV/Delta source with an embedded JSON payload
        # get the same struct-flatten/array-explode treatment as a native JSON source. It must
        # run *after* normalize_column_names (its configured column names are the normalized
        # ones) and *immediately before* apply_explode_columns (the structs it produces are
        # exactly what explode consumes -- run it afterwards and they would never be flattened).
        staged_df = parse_json_string_columns(staged_df, source_config.get("json_string_columns"))
        # resolve_auto_flatten_all, not source_config.get("auto_flatten_all", False): a
        # PRESENT-but-empty "explode_columns": [] means "auto-flatten everything", while an
        # ABSENT (or null) explode_columns stays a schema-preserving pass-through. That
        # distinction is load-bearing -- it is what prevents silent cartesian row explosion on
        # un-configured sources (see ingestion/json_flattening.py's module docstring) -- and it
        # can only be made against the raw dict here, because a `.get()` inside the function
        # collapses "absent" and "present-but-empty" to the same value.
        staged_df = apply_explode_columns(
            staged_df, source_config.get("explode_columns"), resolve_auto_flatten_all(source_config)
        )
        # Full-row dedup (source_config.remove_dups, default False -- a no-op otherwise) sits
        # after explode and before standardization on purpose: a source row delivered twice
        # becomes 2xM rows once an array is explode_outer'ed, so only a post-explode dedup
        # collapses it correctly; and standardization must run over the surviving rows only,
        # since a standardization expression built on current_timestamp() (or any other
        # non-deterministic function) would otherwise make every duplicate look distinct and
        # defeat the dedup entirely. See ingestion/dedup.py for the unbounded-state warning that
        # applies to a watermark-less streaming source.
        staged_df = apply_stream_dedup(staged_df, source_config)
        staged_df = apply_data_standardization_sql(staged_df, source_config.get("data_standardization_sql"))
        return staged_df

    # The RETURN VALUE, not staged_view_name: when materialize is True the staged intermediate
    # is a qualified @dlt.table, and every downstream reader (quarantine's clean/quarantine
    # split, the sink export) must resolve it by that qualified name.
    staged_dataset_name = register_staged_view(
        staged_view_name,
        f"Staged intermediate view for ingestion flow {flow_row.dataflow_id}",
        dq_rules,
        _build_ingestion_dataframe,
        target_config,
        pipeline_run_id=pipeline_run_id,
        record_id_column=dq_config.get("record_id_column"),
        capture_technical_metadata=source_config.get("capture_technical_metadata", True),
        flow_id=flow_row.dataflow_id,
        read_operation_name="ingestion_read",
        materialize=_needs_materialized_staged_view(dq_rules, flow_row.target_type),
        target_catalog=flow_row.target_catalog,
        target_schema=flow_row.target_schema,
    )

    register_flow_output(
        flow_row.dataflow_id,
        staged_dataset_name,
        flow_row.target_table,
        flow_row.target_catalog,
        flow_row.target_schema,
        flow_row.cdc_load_strategy,
        target_config,
        dq_rules,
        flow_row.source_description,
        is_streaming,
        flow_row.target_type,
        quarantine_table_override=dq_config.get("quarantine_table"),
    )


def generate_transformation_flow(
    spark,
    dbutils,
    flow_row=None,
    *,
    plan: SourcePlanePlan,
    pipeline_parameters: Dict[str, Any],
    pipeline_run_id: Optional[str] = None,
) -> None:
    """Register one ``transformation_flow_spec`` row's input views, staged intermediate and
    flow output -- the twin of :func:`generate_ingestion_flow`.

    Parameters
    ----------
    spark:
        Active ``SparkSession`` -- genuinely used here, to execute the substituted
        ``transformation_sql``.
    dbutils:
        Accepted only so this generator is a literal twin of :func:`generate_ingestion_flow`
        (same positional shape, so the notebook's three registration loops read identically).
        A transformation flow has no ``source_config`` and therefore no
        ``schema_config_path``, so nothing here reads it. Callers that omit it entirely --
        ``generate_transformation_flow(spark, flow_row, plan=..., ...)`` -- are supported: the
        second positional is then taken as ``flow_row``. See the shim immediately below.
    flow_row:
        One active ``transformation_flow_spec`` row (a Spark ``Row``, or any duck-typed
        stand-in).
    plan:
        This dataflow group's :class:`~engine.source_plane.SourcePlanePlan`. Forwarded to
        ``transformation/inputs.py::register_transformation_inputs``, which resolves each input
        under consumer id ``f"{flow_row.flow_step_id}:input:{input_name}"`` -- ``input_name``
        stays the SQL identifier the ``transformation_sql`` references but is demoted from
        *being* the read to being an ALIAS over the plane (the CROSS-flow half of R2).
    pipeline_parameters:
        The group's ``${param}`` substitution map -- applied to ``target_config_json`` as a
        *path* substitution and to ``transformation_sql`` as a *dynamic parameter*
        substitution (two different mechanisms; see ``transformation/parameters.py``).
    pipeline_run_id:
        Best-effort run/update identifier, attached to quarantine metadata for traceability.

    Raises
    ------
    FrameworkConfigError
        On malformed JSON in ``source_inputs_json``/``target_config_json``/``dq_config_json``,
        or propagated from :func:`~engine.source_plane.bind` and the registration helpers.
    """
    if flow_row is None:
        # Called as generate_transformation_flow(spark, flow_row, plan=..., ...) -- the
        # dbutils-less short form. Re-seat the arguments rather than failing on a signature
        # difference that has no behavioural meaning for a transformation flow.
        flow_row, dbutils = dbutils, None

    try:
        source_inputs = json.loads(flow_row.source_inputs_json) if flow_row.source_inputs_json else []
        target_config = (
            json.loads(substitute_path_parameters(flow_row.target_config_json, pipeline_parameters))
            if flow_row.target_config_json
            else {}
        )
        dq_config = json.loads(flow_row.dq_config_json) if flow_row.dq_config_json else {}
    except json.JSONDecodeError as exc:
        raise FrameworkConfigError(
            f"Transformation flow '{flow_row.flow_step_id}': malformed JSON configuration: {exc}"
        ) from exc

    dq_rules = dq_config.get("rules", [])
    register_transformation_inputs(spark, source_inputs, plan, flow_row.flow_step_id)
    resolved_sql = substitute_dynamic_parameters(flow_row.transformation_sql, pipeline_parameters)
    resolved_sql = mark_streaming_references(resolved_sql, source_inputs)

    # A transformation's staged view is a genuinely streaming computation whenever *any*
    # of its source_inputs is streaming (Spark propagates streaming through the whole
    # query plan once one input is), independent of this flow's own target_type -- a
    # windowed streaming aggregation feeding an `external_sink`/`batch_table`/
    # `materialized_view` target is still a streaming view under the hood. Missing this
    # raised `AnalysisException: View '...' is a streaming view and must be referenced
    # using readStream` the moment `register_main_and_quarantine_tables` read such a
    # staged view via a plain (non-streaming) `dlt.read(...)`.
    is_streaming = flow_row.target_type == "streaming_table" or any(
        input_config.get("is_streaming") for input_config in source_inputs
    )
    staged_view_name = f"_{flow_row.target_table}_staged"

    # The RETURN VALUE, not staged_view_name -- see generate_ingestion_flow's twin comment.
    staged_dataset_name = register_staged_view(
        staged_view_name,
        f"Staged transformation output for {flow_row.flow_step_id}",
        dq_rules,
        lambda: spark.sql(resolved_sql),
        target_config,
        pipeline_run_id=pipeline_run_id,
        record_id_column=dq_config.get("record_id_column"),
        capture_technical_metadata=target_config.get("capture_technical_metadata", True),
        flow_id=flow_row.flow_step_id,
        read_operation_name="transformation_execute",
        materialize=_needs_materialized_staged_view(dq_rules, flow_row.target_type),
        target_catalog=flow_row.target_catalog,
        target_schema=flow_row.target_schema,
    )

    register_flow_output(
        flow_row.flow_step_id,
        staged_dataset_name,
        flow_row.target_table,
        flow_row.target_catalog,
        flow_row.target_schema,
        flow_row.cdc_load_strategy,
        target_config,
        dq_rules,
        f"Transformation target for flow step {flow_row.flow_step_id}",
        is_streaming,
        flow_row.target_type,
        quarantine_table_override=dq_config.get("quarantine_table"),
    )


def generate_reconciliation_flow(
    spark,
    flow_row,
    *,
    plan: SourcePlanePlan,
    publish_catalog: str,
    publish_schema: str,
    control_schema: str,
    pipeline_update_id: Optional[str] = None,
    log_capture_overrides: Optional[Dict[str, Optional[bool]]] = None,
    pipeline_parameters: Optional[Dict[str, Any]] = None,
) -> None:
    """Register one ``reconciliation_flow_spec`` row as the DAG's THIRD flow type.

    A thin delegation to
    :func:`~reconciliation.graph_registration.register_reconciliation_flow`, which registers
    this row's L3 (prepare) + L4 (compare) + L5 (heal) datasets/flows. It exists as a peer of
    :func:`generate_ingestion_flow`/:func:`generate_transformation_flow` so the notebook's three
    registration loops are literally the same shape, and so "which module owns a flow type's
    graph registration" has exactly one answer per flow type.

    A ``"job"``-mode row is a no-op (``register_reconciliation_flow`` returns immediately) --
    those rows belong to the standalone ``05_reconciliation_engine.py`` job task, whose
    behaviour requirement R3 leaves entirely unchanged.

    Parameters
    ----------
    spark:
        Active ``SparkSession``.
    flow_row:
        One active ``reconciliation_flow_spec`` row.
    plan:
        This dataflow group's :class:`~engine.source_plane.SourcePlanePlan` -- both
        reconciliation sides are resolved through it (consumer ids
        ``f"{reconciliation_id}:source"`` and ``f"{reconciliation_id}:target:{target_id}"``).
    publish_catalog, publish_schema, control_schema, pipeline_update_id,
    log_capture_overrides, pipeline_parameters:
        Forwarded verbatim -- see
        :func:`~reconciliation.graph_registration.register_reconciliation_flow` for each one's
        meaning.

    Raises
    ------
    FrameworkConfigError
        Propagated unchanged from
        :func:`~reconciliation.graph_registration.register_reconciliation_flow`.
    """
    register_reconciliation_flow(
        spark,
        flow_row,
        plan=plan,
        publish_catalog=publish_catalog,
        publish_schema=publish_schema,
        control_schema=control_schema,
        pipeline_update_id=pipeline_update_id,
        log_capture_overrides=log_capture_overrides,
        pipeline_parameters=pipeline_parameters,
    )
