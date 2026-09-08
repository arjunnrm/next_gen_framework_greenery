"""L3/L4/L5 reconciliation graph registration -- the ``execution_mode: "pipeline"``/
``"pipeline_audit_only"`` counterpart to ``05_reconciliation_engine.py``'s standalone job-task
path.

**Why this module exists.** Today, reconciliation always runs as an independent job task:
``05_reconciliation_engine.py`` reads ``reconciliation_flow_spec``, prepares the source once,
then loops ``target_configs[]`` calling ``reconciliation/appender.py::run_target_reconciliation``
per target -- entirely outside any Lakeflow Declarative Pipeline graph, and always AFTER the
ingestion/transformation pipeline that fed it has already committed. This module is what lets a
reconciliation flow instead become a genuine third flow type *inside* its own
``dataflow_group_id`` pipeline update, so its source/target reads share the group's L0 source
plane (``engine/source_plane.py``) instead of re-reading tables the same update already
materialized, and its comparison becomes a queryable, lineage-tracked, ``dq_config``-gated set of
Unity Catalog tables instead of opaque job-task Python.

**The five layers this module registers, per reconciliation flow** (v1.6.0 Intermediate Object
Rule: every true intermediate below is a pipeline-scoped ``@dlt.table(temporary=True)`` under
its bare name -- materialized once, never published to Unity Catalog; the only published
datasets are ``__metrics``/``__mismatch``, which are the staging feed for the control-table
sinks and exist only when their capture flag resolves true, plus -- for a healing flow only --
the L3 ``_src``/healing ``_tgt`` nodes the L5 handler must read back through the metastore):

* **L3 RECON PREPARE** -- ``_recon__<reconciliation_id>__src`` (one shared, hash-prepared read of
  ``source_config``, paid ONCE regardless of how many targets this flow compares against) and one
  ``_recon__<reconciliation_id>__<target_id>__tgt`` per target (the far side, always batch).
  Temporary, except published for a healing flow (``_src`` always, ``_tgt`` per healing target)
  because the L5 handler reads them back via a plain ``spark.read.table``.
* **L4 RECON COMPARE** -- per target: ``_recon__<reconciliation_id>__<target_id>__classified``
  (the full-outer-join classification -- temporary, read up to 3x downstream), ``__metrics``
  (one published row, registered ONLY when ``run_log_capture`` resolves true; carries this
  flow's ``dq_config`` expectations -- reconciliation's first declarative way to fail a pipeline
  update), ``__mismatch`` (the published per-record mismatch detail, direction-gated, registered
  ONLY when ``mismatch_log_capture`` resolves true), and
  ``_recon__<reconciliation_id>__<target_id>__missing`` (the ``source_to_target`` append set,
  temporary, registered only for a target that actually heals).
* **L5 RECON HEAL** -- only when ``execution_mode == "pipeline"`` *and* at least one target
  heals: ``_recon__<reconciliation_id>__pulse`` (a temporary one-column streaming projection of
  the source, whose sole purpose is to give ``dlt.foreach_batch_sink`` -- streaming-only --
  something to trigger on), ``recon__<reconciliation_id>__heal_flow`` (the ``@dlt.append_flow``
  joining the pulse against a one-row aggregate of the first healing target's ``__classified``
  dataset purely as an ORDERING EDGE -- see below), and
  ``_recon__<reconciliation_id>__heal_sink`` (the ``foreach_batch_sink`` handler, never itself a
  Unity Catalog dataset).

**Why the append_flow joins the pulse against a one-row ``__classified`` aggregate at all.** A
pulse's own rows carry nothing but ``__recon_gate``; the join's real job is to make Lakeflow
schedule this flow *after* the target's whole-snapshot L4 classification has materialized this
update, by making the flow structurally depend on it (``dlt.read`` of a materialized dataset is
a real graph edge). An equi-join on a constant literal column is the one documented legal
stream-static join shape (a non-equi ``lit(True)`` join raises Lakeflow's "Detected implicit
cartesian product"); ``INNER`` is safe here specifically because the gate side is a single
``.agg(...)`` with no ``groupBy`` and therefore always emits exactly one row, even over an
entirely empty relation -- so the join can never silently degenerate into "no batch this
update" on an all-matched run. (Pre-v1.6.0 this edge anchored on ``__metrics``, which carried
the same one-row guarantee; ``__metrics`` is now conditional on ``run_log_capture``, and a
healing flow with logging suppressed must still heal.)

**Why the healing handler re-derives its own match/append/log via
:func:`~reconciliation.appender.run_target_reconciliation` instead of reading back the published
``__missing``/``__mismatch``/``__metrics`` tables.** ``run_target_reconciliation`` already
carries the full, tested job-mode contract this change must not disturb: the Phase 1
fingerprint early-out, the restartability ledger check
(``is_target_batch_already_processed``), ``apply_transform_sql``'s reshaping, Liquid Clustering
on first write, and both log tables' precedence rules -- all exercised byte-identically whether
the call comes from ``05_reconciliation_engine.py`` or from here. Re-deriving the classification
from a *published* dataset shaped for SQL/BI consumption (target-prefixed columns, no
restartability fingerprinting) would mean re-implementing that entire contract a second time;
calling the existing function unchanged, against the already-materialized ``_..._src``/
``_..._tgt`` L3 nodes (each read exactly once more, cheaply, since they are themselves
materialized-once-per-update datasets), keeps exactly one implementation of "match, append, log"
in the codebase. The append_flow's join with the ``__classified`` gate (above) is what guarantees L3/L4 have
already fully materialized by the time this handler runs, so this second pass over ``_src``/
``_tgt`` sees the same, final, this-update snapshot L4 already classified.

**Lakeflow rule compliance (see ``docs/13`` and this repository's hard rules):**

* *Rule 1 (no self-read).* Every ``dlt.read``/``dlt.read_stream`` call in this module names a
  *different* dataset than the one whose body it appears in -- ``_..._src``/``_..._tgt`` bind an
  external/shared/sibling identity via :func:`~engine.source_plane.bind`, never a name this
  module itself registers; ``__classified`` reads ``_..._src``/``_..._tgt``; ``__metrics``/
  ``__mismatch``/``_..._missing`` all read ``__classified`` (plus, for ``__metrics``/
  ``_..._missing``, ``_..._src``/``_..._tgt`` again); the pulse binds the source identity fresh;
  the heal append_flow reads the pulse and the first healing target's ``__classified``.
* *Rule 2 (no eager action reachable from a streaming plan).* Every ``@dlt.table`` body below
  returns a lazy DataFrame -- ``matcher.classify_reconciliation_target`` is used specifically
  because it performs no ``.collect()``/``.count()``, and ``__metrics``' own ``.agg(...)`` is
  returned directly as the dataset, never collected. The only ``.collect()``/``.count()``/
  ``.saveAsTable()`` calls this module's registration touches at all live inside the
  ``foreach_batch_sink`` handler -- execution-time code outside any ``@dlt.table`` closure,
  exactly like ``cdc/snapshot.py``'s existing snapshot lambda and
  ``dq/quarantine.py::_quarantine_table``'s own shipped batch-branch ``.agg(...).collect()[0]``.
* *Rule 3 (a snapshot lambda may reference only a path).* Not implicated -- this module never
  registers an ``apply_changes_from_snapshot`` call; ``FULL_SNAPSHOT_CDC`` sources are excluded
  from the source plane entirely (``engine/source_plane.py``).
* *Rule 4 (``dlt.read``/``dlt.read_stream`` must match registration).* The same local variable
  (``needs_heal``) drives both the ``@dlt.table``-inferred mode of ``_..._src`` (stream when
  ``True``, MV when ``False`` -- Lakeflow infers this from whether the returned DataFrame is
  itself streaming, not from a decorator kwarg) and the ``want_stream`` argument passed to
  :func:`~engine.source_plane.bind` for that same consumer id, so the two can never disagree.
* *Rule 6 (append-only streaming source).* Enforced upstream, at plan time, by
  ``engine/source_plane.py``'s G-STREAM guard and its reconciliation-source ``want_stream`` rule
  -- this module only ever requests a streaming bind of the reconciliation source when
  ``execution_mode == "pipeline"``, which is exactly the case that guard already covers.
* *Rule 7 (no ``CREATE OR REPLACE FUNCTION`` under concurrency).* Not implicated -- this module
  declares no UC function.

**Handler closure serializability.** :func:`register_reconciliation_flow`'s
``foreach_batch_sink`` handler closes over ONLY plain Python values captured at
graph-definition time (``control_schema``, ``reconciliation_id``, the healing ``target_configs``
list, ``match_keys``/``compare_columns``, ``source_hash_precomputed``, ``transform_sql``,
``parameters``, ``logging_config``, ``pipeline_update_id``, the log-capture overrides,
``two_tier_verification``, ``on_failure``, and the two L3 node names it reads back) -- never the
``SourcePlanePlan`` (which is not meaningfully picklable/reusable outside graph-definition time)
and never a ``SparkSession`` reference (the handler uses ``batch_df.sparkSession`` exclusively,
per Lakeflow's own ``foreachBatch`` contract).

**Signature deviation from this step's plan text.** The plan's signature for
:func:`register_reconciliation_flow` does not list a parameter for the group's ``${param}``
substitution map, but ``source_config``/``target_configs``/``filter_condition`` resolution
genuinely needs one (see ``transformation/parameters.py`` and every other reconciliation call
site -- ``05_reconciliation_engine.py``, ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``).
An additional keyword-only ``pipeline_parameters`` argument (default ``None``, treated as ``{}``)
is added for exactly this purpose, mirroring how the job-task notebook resolves and threads its
own ``PIPELINE_PARAMETERS``.
"""

import json
import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flowx.lakeflow_framework.cdc.hashing import HASH_KEY_COLUMN, HASH_VALUE_COLUMN
from flowx.lakeflow_framework.dq.expectations import apply_dq_expectations
from flowx.lakeflow_framework.engine.identifiers import sanitize_identifier
from flowx.lakeflow_framework.engine.sink_registration import (
    register_foreach_batch_sink,
    require_streaming_source,
)
from flowx.lakeflow_framework.engine.source_plane import SourcePlanePlan, bind
from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.observability.structured_logger import log_flow_event
from flowx.lakeflow_framework.reconciliation import matcher, mismatch_logging
from flowx.lakeflow_framework.reconciliation.appender import (
    resolve_log_capture_flags,
    run_target_reconciliation,
    write_reconciliation_result,
    write_run_log_entry,
)
from flowx.lakeflow_framework.reconciliation.dataset_reader import apply_reconciliation_overlays
from flowx.lakeflow_framework.storage.table_properties import qualified_table_name
from flowx.lakeflow_framework.transformation.parameters import substitute_path_parameters

logger = logging.getLogger("flowx.lakeflow_framework.reconciliation.graph_registration")

#: ``execution_mode`` values -- see ``control_plane/ddl_definitions.py::get_reconciliation_flow_spec_ddl``.
_JOB_EXECUTION_MODE = "job"
_PIPELINE_EXECUTION_MODE = "pipeline"
_PIPELINE_AUDIT_ONLY_EXECUTION_MODE = "pipeline_audit_only"
_VALID_PIPELINE_EXECUTION_MODES = (_PIPELINE_EXECUTION_MODE, _PIPELINE_AUDIT_ONLY_EXECUTION_MODE)

#: ``comparison_direction`` values under which a target's ``source_to_target`` miss set is
#: appended -- shared with ``reconciliation/appender.py::run_target_reconciliation``'s own
#: ``evaluate_source_to_target`` gate, kept as a local literal (this module has no import-safe
#: access to that function-local name).
_HEAL_DIRECTIONS = ("source_to_target", "both")


def _row_get(row: Any, name: str, default: Any = None) -> Any:
    """Attribute-style field access tolerant of a control-table row that predates a column
    (``01_setup`` only ever runs ``CREATE TABLE IF NOT EXISTS``, never a migration) -- mirrors
    ``engine/source_plane.py``'s identical helper.
    """
    return getattr(row, name, default)


def _json_loads(raw: Optional[str], default: Any) -> Any:
    """``json.loads`` a control-table JSON-string column, tolerating ``None``/empty -- mirrors
    ``engine/source_plane.py``'s identical helper."""
    if not raw:
        return default
    return json.loads(raw)


def _counts_query(classified_df: DataFrame, source_df: DataFrame, target_df: DataFrame) -> DataFrame:
    """Lazy, one-row aggregate: the four classification counts plus both sides' record counts.

    Three independent single-row aggregates (`classified_df`'s per-type sums, `source_df`'s
    count, `target_df`'s count) combined via ``crossJoin`` -- each side of a ``crossJoin`` of
    exactly-one-row DataFrames still yields exactly one row, so the result is guaranteed
    single-row even when every input relation is empty. Nothing here is collected -- see this
    module's docstring, Rule 2.
    """
    counts = classified_df.agg(
        F.sum(F.when(F.col(matcher.MISMATCH_TYPE_COLUMN) == matcher.MISMATCH_TYPE_MATCHED, 1).otherwise(0)).alias(
            "matched_count"
        ),
        F.sum(
            F.when(
                F.col(matcher.MISMATCH_TYPE_COLUMN).isin(
                    matcher.MISMATCH_TYPE_MISSING_IN_TARGET, matcher.MISMATCH_TYPE_VALUE_DRIFT
                ),
                1,
            ).otherwise(0)
        ).alias("missing_in_target_count"),
        F.sum(F.when(F.col(matcher.MISMATCH_TYPE_COLUMN) == matcher.MISMATCH_TYPE_VALUE_DRIFT, 1).otherwise(0)).alias(
            "value_drift_count"
        ),
        F.sum(F.when(F.col(matcher.MISMATCH_TYPE_COLUMN) == matcher.MISMATCH_TYPE_MISSING_IN_SOURCE, 1).otherwise(0)).alias(
            "missing_in_source_count"
        ),
    )
    source_count = source_df.agg(F.count(F.lit(1)).alias("source_record_count"))
    target_count = target_df.agg(F.count(F.lit(1)).alias("target_record_count"))
    return counts.crossJoin(source_count).crossJoin(target_count)


def _mismatch_query(classified_df: DataFrame, comparison_direction: str) -> DataFrame:
    """Direction-gated slice of ``classified_df`` -- the same gating
    ``reconciliation/appender.py::run_target_reconciliation`` applies to its own
    ``mismatch_frames`` list, re-expressed against the published classification table.
    """
    frames = []
    if comparison_direction in ("source_to_target", "both"):
        frames.append(
            classified_df.filter(
                F.col(matcher.MISMATCH_TYPE_COLUMN).isin(
                    matcher.MISMATCH_TYPE_MISSING_IN_TARGET, matcher.MISMATCH_TYPE_VALUE_DRIFT
                )
            )
        )
    if comparison_direction in ("target_to_source", "both"):
        frames.append(classified_df.filter(F.col(matcher.MISMATCH_TYPE_COLUMN) == matcher.MISMATCH_TYPE_MISSING_IN_SOURCE))

    mismatch_detail_df = frames[0]
    for extra_df in frames[1:]:
        mismatch_detail_df = mismatch_detail_df.unionByName(extra_df)
    return mismatch_detail_df


def _wants_heal(target_config: Dict[str, Any]) -> bool:
    """``True`` when this target's ``source_to_target`` miss set is actually appended anywhere
    -- the same condition that gates ``_..._missing`` registration and healing-loop membership.
    """
    comparison_direction = target_config.get("comparison_direction", "both")
    return comparison_direction in _HEAL_DIRECTIONS and bool(target_config.get("append_target_table"))


def register_reconciliation_flow(
    spark: SparkSession,
    flow_row: Any,
    *,
    plan: SourcePlanePlan,
    publish_catalog: str,
    publish_schema: str,
    control_schema: str,
    pipeline_update_id: Optional[str] = None,
    log_capture_overrides: Optional[Dict[str, Optional[bool]]] = None,
    pipeline_parameters: Optional[Dict[str, Any]] = None,
) -> None:
    """Register one ``reconciliation_flow_spec`` row's L3 (prepare) + L4 (compare) + L5 (heal)
    datasets/flows into the currently-building Lakeflow Declarative Pipeline graph.

    A no-op (returns immediately) for a ``"job"``-mode row -- defensive, mirroring
    ``engine/source_plane.py``'s own filter; ``control_plane/repository.py::load_active_group_metadata``
    should never hand this function a job-mode row in the first place.

    Parameters
    ----------
    spark:
        Active SparkSession (used only where a plain read is unavoidable outside a ``@dlt.table``
        closure -- e.g. none, at graph-definition time; kept for signature symmetry with every
        other ``register_*`` entry point in this framework and threaded into the healing
        handler's log-write helpers instead of a driver-time reference).
    flow_row:
        One active ``reconciliation_flow_spec`` row (a Spark ``Row``, or any duck-typed
        stand-in with the same attributes).
    plan:
        This dataflow group's :class:`~engine.source_plane.SourcePlanePlan` -- already built and
        (for a real pipeline run) already registered via
        :func:`~engine.source_plane.register_source_plane` by the caller.
    publish_catalog, publish_schema:
        ``publish_catalog`` is the catalog every published recon dataset lands in (the hosting
        pipeline's own). ``publish_schema`` is RETAINED FOR CALL-SITE COMPATIBILITY ONLY: since
        v1.7.07 it is not consulted -- ``flow_row.publish_schema`` is the sole publish decision,
        and a row without one publishes nothing (its audit datasets are registered as
        pipeline-scoped temporary tables). Before v1.7.07 it was the fallback schema for a NULL
        ``flow_row.publish_schema``, which put ``recon__*__metrics`` beside the business tables.
    control_schema:
        ``<catalog>.config`` -- where ``reconciliation_run_log``/``reconciliation_mismatch_log``/
        ``reconciliation_result`` live; forwarded unchanged to
        :func:`~reconciliation.appender.run_target_reconciliation` inside the L5 handler.
    pipeline_update_id:
        This pipeline update's own identifier, threaded through as every log/result row's
        ``task_run_id`` for correlation -- the pipeline-mode analogue of
        ``05_reconciliation_engine.py``'s ``TASK_RUN_ID`` job parameter.
    log_capture_overrides:
        ``{"recon_run_log_capture": bool | None, "recon_mismatch_log": bool | None}`` -- the
        pipeline-mode replacement for ``05_reconciliation_engine.py``'s
        ``recon_run_log_capture``/``recon_mismatch_log`` job-parameter widgets (which have no
        equivalent inside a running pipeline update -- see this framework's documented
        known-limitation on this point). ``None``/omitted keys defer to
        ``flow_row.logging_config_json`` exactly as the job-mode default does. Forwarded
        verbatim to :func:`~reconciliation.appender.resolve_log_capture_flags` inside the L5
        handler.
    pipeline_parameters:
        This flow's ``${param}`` substitution map (see this module's docstring for why this
        keyword-only argument exists beyond this step's plan text). ``None`` is treated as
        ``{}``.

    Raises
    ------
    FrameworkConfigError
        If ``flow_row.execution_mode`` is neither ``"pipeline"`` nor ``"pipeline_audit_only"``
        (after the defensive job-mode no-op above), if the row's JSON columns are malformed, or
        if any downstream registration call (:func:`~engine.source_plane.bind`,
        :func:`~engine.sink_registration.register_foreach_batch_sink`) raises.
    """
    execution_mode = _row_get(flow_row, "execution_mode", None) or _JOB_EXECUTION_MODE
    if execution_mode == _JOB_EXECUTION_MODE:
        return
    if execution_mode not in _VALID_PIPELINE_EXECUTION_MODES:
        raise FrameworkConfigError(
            f"reconciliation_flow_spec.execution_mode={execution_mode!r} is not a recognized pipeline-mode "
            f"value -- expected one of {_VALID_PIPELINE_EXECUTION_MODES!r} (or 'job'/absent for the standalone "
            "job-task path)."
        )

    reconciliation_id = flow_row.reconciliation_id
    parameters = pipeline_parameters or {}

    try:
        source_config = _json_loads(substitute_path_parameters(flow_row.source_config_json, parameters), {})
        target_configs = _json_loads(substitute_path_parameters(flow_row.target_configs_json, parameters), [])
        match_keys = _json_loads(flow_row.match_keys_json, [])
        compare_columns = _json_loads(_row_get(flow_row, "compare_columns_json"), [])
        error_handling = _json_loads(_row_get(flow_row, "error_handling_json"), {})
        logging_config = _json_loads(_row_get(flow_row, "logging_config_json"), {})
        dq_config = _json_loads(_row_get(flow_row, "dq_config_json"), {})
    except json.JSONDecodeError as exc:
        raise FrameworkConfigError(
            f"Reconciliation flow '{reconciliation_id}': malformed JSON configuration: {exc}"
        ) from exc

    transform_sql = _row_get(flow_row, "transform_sql")
    on_failure = error_handling.get("on_failure", "fail")
    dq_rules = dq_config.get("rules") or []

    # v1.6.0: the log-capture flags participate in GRAPH-DEFINITION decisions, not just the
    # L5 handler's writes -- `__metrics` is registered only when run_log_capture resolves
    # true, `__mismatch` only when mismatch_log_capture does. Resolution here uses exactly
    # the same precedence the handler used at execution time (pipeline-conf override wins,
    # then the onboarded logging_config, then True), over exactly the same inputs, so the
    # two decisions can never disagree.
    recon_run_log_capture = (log_capture_overrides or {}).get("recon_run_log_capture")
    recon_mismatch_log = (log_capture_overrides or {}).get("recon_mismatch_log")
    run_log_capture, mismatch_log_capture = resolve_log_capture_flags(
        logging_config, recon_run_log_capture, recon_mismatch_log
    )

    # two_tier_verification defaults to True when the column is absent/NULL -- same
    # column-may-not-exist-yet caveat as source_plane.py / 05_reconciliation_engine.py.
    _two_tier_raw = _row_get(flow_row, "two_tier_verification", None)
    two_tier_verification = True if _two_tier_raw is None else bool(_two_tier_raw)

    # v1.7.07: `publish_schema` is the ONLY thing that publishes. Until v1.7.07 an absent
    # publish_schema fell back to the hosting pipeline's own schema (the `publish_schema`
    # argument), so every audit-only flow that never asked for anything to be published still
    # landed real `recon__*__metrics` materialized views beside the business tables (UC6: six of
    # them in `bronze`). Now an absent publish_schema means "publish nothing": the audit datasets
    # this flow still needs are registered as pipeline-scoped TEMPORARY tables with bare names,
    # exactly like the L3/L4 intermediates, and anything that genuinely requires a published
    # table is rejected below with the fix named. The `publish_schema` argument is retained for
    # call-site compatibility and is deliberately not consulted here.
    effective_publish_schema = _row_get(flow_row, "publish_schema", None)
    publishes = bool(effective_publish_schema)
    source_hash_precomputed = bool(source_config.get("hash_precomputed", False))
    sanitized_reconciliation_id = sanitize_identifier(reconciliation_id)

    source_consumer_id = f"{reconciliation_id}:source"
    heal_targets = [tc for tc in target_configs if _wants_heal(tc)]
    needs_heal = execution_mode == _PIPELINE_EXECUTION_MODE and bool(heal_targets)

    if needs_heal and not publishes:
        raise FrameworkConfigError(
            f"Reconciliation flow '{reconciliation_id}': execution_mode='pipeline' with a healing "
            "target (append_target_table) requires publish_schema. The L5 heal handler reads the "
            "flow's prepared source and healing target back through the metastore "
            "(spark.read.table), so those two nodes must be published tables -- and since v1.7.07 "
            "nothing is published without an explicit publish_schema. Set publish_schema to a "
            "dedicated reconciliation schema in the pipeline's catalog."
        )

    if not publishes and (run_log_capture or mismatch_log_capture):
        raise FrameworkConfigError(
            f"Reconciliation flow '{reconciliation_id}': run_log_capture/mismatch_log_capture "
            f"resolve to ({run_log_capture}, {mismatch_log_capture}) but publish_schema is absent. "
            "The reconciliation_run_log / reconciliation_mismatch_log rows are exported from the "
            "PUBLISHED `recon__*__metrics` / `__mismatch` datasets; without publish_schema those "
            "datasets are pipeline-scoped temporary tables and nothing can be captured. Either set "
            "publish_schema, or set both capture flags false (a dq_config gate still works on the "
            "pipeline-scoped `__metrics` dataset). Check the dataflow.recon.* pipeline-conf "
            "overrides too -- they can turn a flag on that the spec left off."
        )

    # `__metrics` exists whenever anything needs it: a run-log capture (published) or a
    # dq_config gate (published or temporary). An audit-only flow with neither, and no
    # mismatch capture, would register a full self-comparison that nothing ever reads.
    registers_metrics = bool(run_log_capture or dq_rules)
    if (
        execution_mode == _PIPELINE_AUDIT_ONLY_EXECUTION_MODE
        and not registers_metrics
        and not mismatch_log_capture
    ):
        raise FrameworkConfigError(
            f"Reconciliation flow '{reconciliation_id}': execution_mode='pipeline_audit_only' "
            "with both run_log_capture and mismatch_log_capture resolved false and no dq_config "
            "rules registers compute with no output at all -- the audit-only mode exists to "
            "produce the `__metrics`/`__mismatch` datasets, their control-table exports, or a "
            "dq_config gate. Enable a capture flag (with publish_schema), declare dq_config "
            "rules, or switch the flow to execution_mode='job'/'pipeline'."
        )

    def _node_name(bare: str) -> str:
        if not effective_publish_schema:
            raise FrameworkConfigError(
                f"Reconciliation flow '{reconciliation_id}': internal error -- asked to publish "
                f"'{bare}' with no publish_schema. Nothing may be published without one."
            )
        return qualified_table_name(publish_catalog, effective_publish_schema, bare)

    def _audit_node(bare_suffix: str) -> Tuple[str, bool]:
        """Name and temporariness of an L4 audit dataset (`__metrics` / `__mismatch`).

        Published (three-part name, permanent) only when the flow sets publish_schema;
        otherwise a bare, pipeline-scoped temporary table -- the Intermediate Object Rule
        applied to the audit lane. The bare spelling carries the leading `_` every
        temporary recon node carries, so `_recon__<rid>__<tid>__metrics` cannot be mistaken
        for a published `recon__...__metrics`.
        """
        if publishes:
            return _node_name(f"recon__{bare_suffix}"), False
        return f"_recon__{bare_suffix}", True

    # -----------------------------------------------------------------------------------------
    # L3 RECON PREPARE -- one shared source, one target per target_configs[] entry.
    #
    # v1.6.0 Intermediate Object Rule: L3/L4 prepare/classify nodes are transient plumbing, so
    # they are pipeline-scoped temporary tables (materialized -- read-once holds -- but never
    # published to Unity Catalog) under their bare names. The ONE exception is a healing flow:
    # the L5 foreach_batch_sink handler reads `_..._src` and each healing target's `_..._tgt`
    # back via a plain `spark.read.table(...)`, which resolves through the metastore and
    # therefore requires those specific nodes to remain published qualified tables.
    # -----------------------------------------------------------------------------------------

    src_published = needs_heal
    _src_bare_name = f"_recon__{sanitized_reconciliation_id}__src"
    src_table_name = _node_name(_src_bare_name) if src_published else _src_bare_name

    def _make_src_table(_source_config=source_config, _want_stream=needs_heal):
        @dlt.table(
            name=src_table_name,
            temporary=not src_published,
            comment=(
                f"L3 RECON PREPARE -- shared, hash-prepared read of reconciliation "
                f"'{reconciliation_id}''s source_config ({'streaming table' if _want_stream else 'materialized view'}"
                f"{'' if src_published else ', pipeline-scoped temporary'}), "
                f"paid once regardless of target count."
            ),
        )
        def _recon_src():
            raw_df = bind(plan, source_consumer_id, _want_stream)
            overlaid_df = apply_reconciliation_overlays(raw_df, _source_config, parameters, task_run_id=None)
            return matcher.prepare_dataset_for_matching(
                overlaid_df, match_keys, compare_columns, source_hash_precomputed
            )

    _make_src_table()

    target_table_names: Dict[str, str] = {}
    classified_table_names: Dict[str, str] = {}
    metrics_table_names: Dict[str, str] = {}
    missing_table_names: Dict[str, str] = {}

    for target_config in target_configs:
        target_id = target_config["target_id"]
        sanitized_target_id = sanitize_identifier(target_id)
        target_hash_precomputed = bool(target_config.get("hash_precomputed", False))
        comparison_direction = target_config.get("comparison_direction", "both")

        # A target's L3 node must stay published only when the L5 handler will read it back
        # via spark.read.table -- i.e. this flow heals AND this specific target is a healer.
        tgt_published = needs_heal and _wants_heal(target_config)
        _tgt_bare_name = f"_recon__{sanitized_reconciliation_id}__{sanitized_target_id}__tgt"
        tgt_table_name = _node_name(_tgt_bare_name) if tgt_published else _tgt_bare_name
        target_table_names[target_id] = tgt_table_name

        def _make_tgt_table(
            _target_config=target_config,
            _target_id=target_id,
            _tgt_table_name=tgt_table_name,
            _hash_precomputed=target_hash_precomputed,
            _tgt_published=tgt_published,
        ):
            @dlt.table(
                name=_tgt_table_name,
                temporary=not _tgt_published,
                comment=f"L3 RECON PREPARE -- hash-prepared far side for reconciliation '{reconciliation_id}' target '{_target_id}' (always batch{'' if _tgt_published else ', pipeline-scoped temporary'}).",
            )
            def _recon_tgt():
                target_consumer_id = f"{reconciliation_id}:target:{_target_id}"
                raw_df = bind(plan, target_consumer_id, False)
                overlaid_df = apply_reconciliation_overlays(raw_df, _target_config, parameters, task_run_id=None)
                return matcher.prepare_dataset_for_matching(overlaid_df, match_keys, compare_columns, _hash_precomputed)

        _make_tgt_table()

        # -------------------------------------------------------------------------------------
        # L4 RECON COMPARE -- classified (temporary intermediate, read up to 3x downstream),
        # metrics (published IFF run_log_capture), mismatch (published IFF
        # mismatch_log_capture), missing (temporary, heal-only). The published metrics/
        # mismatch datasets are not gratuitous intermediates: they are the staging feed the
        # post-pipeline backstop export (observability/reconciliation_export.py) reads from a
        # plain job session to populate reconciliation_run_log/_mismatch_log -- which is why
        # they stay published while every true intermediate is temporary, and why their
        # registration is gated by the same flags that gate those control-table writes.
        # -------------------------------------------------------------------------------------

        classified_table_name = f"_recon__{sanitized_reconciliation_id}__{sanitized_target_id}__classified"
        classified_table_names[target_id] = classified_table_name

        def _make_classified_table(
            _target_config=target_config,
            _target_id=target_id,
            _tgt_table_name=tgt_table_name,
            _classified_table_name=classified_table_name,
            _target_hash_precomputed=target_hash_precomputed,
        ):
            @dlt.table(
                name=_classified_table_name,
                temporary=True,
                comment=(
                    f"L4 RECON COMPARE -- full MATCHED/MISSING_IN_TARGET/MISSING_IN_SOURCE/VALUE_DRIFT "
                    f"classification of reconciliation '{reconciliation_id}' target '{_target_id}'."
                ),
            )
            def _recon_classified():
                source_df = dlt.read(src_table_name)
                target_df = dlt.read(_tgt_table_name)
                classification = matcher.classify_reconciliation_target(
                    source_df,
                    target_df,
                    match_keys,
                    compare_columns,
                    source_hash_precomputed=source_hash_precomputed,
                    target_hash_precomputed=_target_hash_precomputed,
                )
                return classification.deduped_df

        _make_classified_table()

        metrics_table_name, metrics_temporary = _audit_node(
            f"{sanitized_reconciliation_id}__{sanitized_target_id}__metrics"
        )
        metrics_table_names[target_id] = metrics_table_name

        def _make_metrics_table(
            _target_id=target_id,
            _tgt_table_name=tgt_table_name,
            _classified_table_name=classified_table_name,
            _metrics_table_name=metrics_table_name,
            _metrics_temporary=metrics_temporary,
        ):
            @dlt.table(
                name=_metrics_table_name,
                temporary=_metrics_temporary,
                comment=(
                    f"L4 RECON COMPARE -- one-row metrics summary for reconciliation '{reconciliation_id}' "
                    f"target '{_target_id}'. Carries this flow's dq_config expectations, if any -- "
                    f"reconciliation's first declarative way to fail a pipeline update. Registered "
                    f"when run_log_capture resolves true (it feeds reconciliation_run_log) or when "
                    f"dq_config rules exist; published only when the flow sets publish_schema, "
                    f"otherwise pipeline-scoped."
                ),
            )
            @apply_dq_expectations(dq_rules)
            def _recon_metrics():
                classified_df = dlt.read(_classified_table_name)
                source_df = dlt.read(src_table_name)
                target_df = dlt.read(_tgt_table_name)
                return _counts_query(classified_df, source_df, target_df)

        if registers_metrics:
            _make_metrics_table()

        _mismatch_table_name, _mismatch_temporary = _audit_node(
            f"{sanitized_reconciliation_id}__{sanitized_target_id}__mismatch"
        )

        def _make_mismatch_table(
            _target_id=target_id,
            _classified_table_name=classified_table_name,
            _comparison_direction=comparison_direction,
            _name=_mismatch_table_name,
            _temporary=_mismatch_temporary,
        ):
            @dlt.table(
                name=_name,
                temporary=_temporary,
                comment=(
                    f"L4 RECON COMPARE -- per-record mismatch detail for reconciliation "
                    f"'{reconciliation_id}' target '{_target_id}', gated by comparison_direction={_comparison_direction!r}. "
                    f"Registered only when mismatch_log_capture resolves true (it feeds reconciliation_mismatch_log)."
                ),
            )
            def _recon_mismatch():
                classified_df = dlt.read(_classified_table_name)
                mismatch_detail_df = _mismatch_query(classified_df, _comparison_direction)
                return mismatch_logging.build_mismatch_rows(
                    mismatch_detail_df, reconciliation_id, _target_id, match_keys, compare_columns
                )

        if mismatch_log_capture:
            _make_mismatch_table()

        # `needs_heal`, NOT `_wants_heal(target_config)` alone: the miss set exists ONLY to feed
        # the L5 heal lane, and L5 does not register at all unless execution_mode is genuinely
        # "pipeline" (see the `if not needs_heal: return` below). Gating on _wants_heal alone
        # meant that under "pipeline_audit_only" this table still materialized a full left-semi
        # join over the whole source that NOTHING in the update reads -- dead compute, once per
        # update per healing target. `needs_heal` already ANDs in the execution-mode check.
        if needs_heal and _wants_heal(target_config):
            missing_table_name = f"_recon__{sanitized_reconciliation_id}__{sanitized_target_id}__missing"
            missing_table_names[target_id] = missing_table_name

            def _make_missing_table(
                _target_id=target_id,
                _classified_table_name=classified_table_name,
                _missing_table_name=missing_table_name,
            ):
                @dlt.table(
                    name=_missing_table_name,
                    temporary=True,
                    comment=(
                        f"L4 RECON COMPARE -- source_to_target miss set (MISSING_IN_TARGET + VALUE_DRIFT) for "
                        f"reconciliation '{reconciliation_id}' target '{_target_id}', feeding the L5 heal lane."
                    ),
                )
                def _recon_missing():
                    source_df = dlt.read(src_table_name)
                    classified_df = dlt.read(_classified_table_name)
                    missing_keys = classified_df.filter(
                        F.col(matcher.MISMATCH_TYPE_COLUMN).isin(
                            matcher.MISMATCH_TYPE_MISSING_IN_TARGET, matcher.MISMATCH_TYPE_VALUE_DRIFT
                        )
                    ).select(HASH_KEY_COLUMN)
                    # Same append_columns rule as matcher.classify_reconciliation_target: keep
                    # __framework_hash_key always, drop __framework_hash_value unless it was a
                    # genuine source column already (source_hash_precomputed=True).
                    append_columns = [
                        c for c in source_df.columns if source_hash_precomputed or c != HASH_VALUE_COLUMN
                    ]
                    return source_df.join(missing_keys, on=HASH_KEY_COLUMN, how="left_semi").select(*append_columns)

            _make_missing_table()

    # -----------------------------------------------------------------------------------------
    # L5 RECON HEAL -- pulse + append_flow + foreach_batch_sink handler. Only when this flow's
    # execution_mode is genuinely "pipeline" (not "pipeline_audit_only") and at least one target
    # actually appends.
    # -----------------------------------------------------------------------------------------

    if not needs_heal:
        return

    pulse_table_name = f"_recon__{sanitized_reconciliation_id}__pulse"

    def _make_pulse_table():
        @dlt.table(
            name=pulse_table_name,
            temporary=True,
            comment=(
                f"L5 RECON HEAL -- one-column streaming pulse for reconciliation '{reconciliation_id}', "
                f"triggering the foreach_batch_sink healing handler once per source-advancing update."
            ),
        )
        def _recon_pulse():
            raw_df = bind(plan, source_consumer_id, True)
            return raw_df.select(F.lit(1).alias("__recon_gate"))

    _make_pulse_table()

    first_heal_target_id = heal_targets[0]["target_id"]
    # The heal flow's ordering edge joins against the first healing target's CLASSIFIED
    # dataset (always registered), not its `__metrics` row as pre-v1.6.0 -- `__metrics` is
    # now conditional on run_log_capture, and a healing flow with logging suppressed must
    # still heal. Classified sits at the same L4 root (it derives from the L3 src/tgt nodes
    # the handler re-reads), so "heal only after this update's L3/L4 have materialized"
    # holds identically.
    first_gate_table_name = classified_table_names[first_heal_target_id]

    require_streaming_source(
        reconciliation_id,
        True,
        "reconciliation L5 healing (dlt.append_flow into the foreach_batch_sink)",
        "the pulse dataset is constructed directly from a streaming source-plane bind, so this "
        "should never trip in practice -- kept for defense-in-depth consistency with every other "
        "Lakeflow sink-adjacent construct in this framework (see engine/sink_registration.py).",
    )

    heal_sink_name = f"_recon__{sanitized_reconciliation_id}__heal_sink"
    heal_flow_name = f"recon__{sanitized_reconciliation_id}__heal_flow"

    # recon_run_log_capture / recon_mismatch_log / run_log_capture were resolved at the top of
    # this function (they gate the L4 metrics/mismatch registrations too).

    def _build_heal_handler(
        _control_schema: str = control_schema,
        _reconciliation_id: str = reconciliation_id,
        _heal_targets: List[Dict[str, Any]] = list(heal_targets),
        _match_keys: List[str] = list(match_keys),
        _compare_columns: List[str] = list(compare_columns),
        _source_hash_precomputed: bool = source_hash_precomputed,
        _transform_sql: Optional[str] = transform_sql,
        _parameters: Dict[str, Any] = dict(parameters),
        _logging_config: Dict[str, Any] = dict(logging_config),
        _pipeline_update_id: Optional[str] = pipeline_update_id,
        _recon_run_log_capture: Optional[bool] = recon_run_log_capture,
        _recon_mismatch_log: Optional[bool] = recon_mismatch_log,
        _run_log_capture: bool = run_log_capture,
        _two_tier_verification: bool = two_tier_verification,
        _on_failure: str = on_failure,
        _src_table_name: str = src_table_name,
        _target_table_names: Dict[str, str] = dict(target_table_names),
    ) -> Callable[[DataFrame, int], None]:
        """Build the per-micro-batch handler. Every default-valued parameter above is captured
        NOW, as a plain Python value, into the returned closure -- see this module's docstring's
        "Handler closure serializability" section for why (never ``plan``, never a
        ``SparkSession``).
        """

        def _heal_handler(batch_df: DataFrame, batch_id: int) -> None:  # noqa: ARG001 -- batch_df/batch_id are Lakeflow's foreachBatch contract; the payload itself is ignored (see this module's docstring)
            _handler_log_start = time.monotonic()
            spark_session = batch_df.sparkSession
            prepared_source_df = spark_session.read.table(_src_table_name)
            first_failure: Optional[Exception] = None

            for target_config in _heal_targets:
                target_id = target_config["target_id"]
                try:
                    target_df = spark_session.read.table(_target_table_names[target_id])
                    run_target_reconciliation(
                        spark_session,
                        _control_schema,
                        _reconciliation_id,
                        prepared_source_df,
                        target_df,
                        target_config,
                        _match_keys,
                        _compare_columns,
                        _source_hash_precomputed,
                        _transform_sql,
                        parameters=_parameters,
                        logging_config=_logging_config,
                        task_run_id=_pipeline_update_id,
                        recon_run_log_capture=_recon_run_log_capture,
                        recon_mismatch_log=_recon_mismatch_log,
                        two_tier_verification=_two_tier_verification,
                    )
                except Exception as exc:  # noqa: BLE001 -- mirrors 05_reconciliation_engine.py's own per-target except block, verbatim
                    logger.error(
                        "Reconciliation '%s'/target '%s' healing failed (pipeline update '%s'): %s",
                        _reconciliation_id,
                        target_id,
                        _pipeline_update_id,
                        exc,
                    )
                    failed_run_id = str(uuid.uuid4())
                    try:
                        if _run_log_capture:
                            write_run_log_entry(
                                spark_session,
                                _control_schema,
                                _reconciliation_id,
                                target_id,
                                run_id=failed_run_id,
                                fingerprint="unknown",
                                status="FAILED",
                                error_message=str(exc),
                                task_run_id=_pipeline_update_id,
                            )
                    except Exception as log_exc:  # noqa: BLE001
                        logger.error("Additionally failed to write reconciliation_run_log entry: %s", log_exc)
                    try:
                        # v1.6.0: reconciliation_result is gated by run_log_capture too --
                        # with logging suppressed, reconciliation persists to NOTHING but its
                        # business targets. The pipeline update's own FAILED state (and the
                        # structured log event below) remain the failure signal.
                        if _run_log_capture:
                            write_reconciliation_result(
                                spark_session,
                                _control_schema,
                                _reconciliation_id,
                                target_id,
                                run_id=failed_run_id,
                                status="FAILED",
                                task_run_id=_pipeline_update_id,
                            )
                    except Exception as log_exc:  # noqa: BLE001
                        logger.error("Additionally failed to write reconciliation_result entry: %s", log_exc)

                    if _on_failure == "fail":
                        first_failure = exc
                        break
                    logger.warning(
                        "error_handling.on_failure='warn': suppressing failure for target '%s', continuing.",
                        target_id,
                    )

            log_flow_event(
                operation="reconciliation_heal_batch",
                flow_id=_reconciliation_id,
                status="FAILED" if first_failure is not None else "SUCCESS",
                duration_ms=(time.monotonic() - _handler_log_start) * 1000.0,
                reconciliation_id=_reconciliation_id,
                batch_id=batch_id,
                pipeline_update_id=_pipeline_update_id,
                target_count=len(_heal_targets),
                error=str(first_failure) if first_failure is not None else None,
            )

            if first_failure is not None:
                raise first_failure

        return _heal_handler

    register_foreach_batch_sink(heal_sink_name, _build_heal_handler())

    def _make_heal_flow():
        @dlt.append_flow(
            name=heal_flow_name,
            target=heal_sink_name,
            comment=(
                f"L5 RECON HEAL -- pulse-gated trigger for reconciliation '{reconciliation_id}''s "
                f"foreach_batch_sink healing handler; the join against '{first_gate_table_name}' is "
                f"purely an ordering edge (see this module's docstring)."
            ),
        )
        def _recon_heal_flow():
            pulse_df = dlt.read_stream(pulse_table_name).withColumnRenamed("__recon_gate", "__recon_pulse_gate")
            # A groupBy-less .agg() emits exactly one row even over an empty relation -- the
            # same guarantee the pre-v1.6.0 `__metrics` join leaned on. `__metrics` itself is
            # now conditional on run_log_capture, so the ordering edge anchors on the
            # always-registered classified dataset and derives its own one-row gate.
            gate_df = (
                dlt.read(first_gate_table_name)
                .agg(F.count(F.lit(1)).alias("__recon_classified_rowcount"))
                .withColumn("__recon_gate", F.lit(1))
            )
            joined = pulse_df.join(
                gate_df, on=(pulse_df["__recon_pulse_gate"] == gate_df["__recon_gate"]), how="inner"
            )
            return joined.drop("__recon_pulse_gate", "__recon_gate", "__recon_classified_rowcount")

    _make_heal_flow()

    logger.info(
        "Registered reconciliation flow '%s' (execution_mode=%s) into the pipeline graph: "
        "%d target(s), %d healing.",
        reconciliation_id,
        execution_mode,
        len(target_configs),
        len(heal_targets),
    )
