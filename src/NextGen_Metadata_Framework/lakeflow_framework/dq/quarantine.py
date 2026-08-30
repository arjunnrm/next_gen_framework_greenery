"""Dynamic quarantine routing: derived flag/rule-id columns plus a main+quarantine table pair.

Lakeflow has no built-in "route failing rows to a different table" expectation action -- native
``dlt.expect_all*`` (see ``dq/expectations.py``) can only warn, drop, or fail the whole flow. This
module is the framework's own implementation of ``action: "quarantine"``, in two parts:

* :func:`add_quarantine_columns` evaluates every ``quarantine``-action rule as a plain column
  expression (via ``F.expr``) and attaches ``__framework_dq_quarantine_flag``/``__framework_dq_failed_rule_ids``/
  ``__framework_dq_failure_reasons`` (plus ``__framework_pipeline_run_id``/``__framework_record_id`` for traceability) to every
  row, on the staged view, before any table is registered.
* :func:`register_main_and_quarantine_tables` reads that staged view/table twice -- once
  filtered to non-quarantined rows for the "clean" dataset, once filtered to quarantined rows
  (with the failed-rule diagnostics still attached) for the sibling ``<target_table>_quarantine``
  table -- so both outputs share one upstream read instead of duplicating I/O against the
  original source.

This module is also where ``__framework_hash_key``/
``__framework_hash_value`` actually get computed on a CDC-dispatched flow's clean upstream
(:func:`_apply_hash_columns`), where a *non*-CDC-dispatched flow's clean
upstream is published directly (v1.3.0 shipped a ``TRUNCATE_AND_LOAD`` empty-source guard on
that branch; it was withdrawn on 2026-08-29 because a dataset cannot read itself inside a
Lakeflow graph -- see :func:`register_main_and_quarantine_tables` and
``cdc/dispatcher.py::resolve_truncate_and_load_source``), and where the clean/quarantine outputs
get their columns reordered for Delta stats coverage (``storage/column_ordering.py::
reorder_columns_for_delta_stats``) -- both deliberately centralized here, the one place every
CDC-dispatched strategy's clean source converges, rather than duplicated per ``cdc/`` strategy
module. For the same reason the *physical layout* kwargs of the main table
(``partition_cols``/``cluster_by``) are not derived here either: they come from
``storage/table_properties.py::build_partition_and_cluster_kwargs``, which owns the rules about
explicitly-empty column lists and the liquid-clustering column cap, so this module can never
drift from a second writer's interpretation of the same ``target_config`` fields.

**Real bug fixed here (``needs_cdc_dispatch``):** when a flow's ``cdc_load_strategy`` is anything
other than ``APPEND``/``TRUNCATE_AND_LOAD``, ``cdc/dispatcher.py`` goes on to publish the actual
target table under this same ``target_table`` name via ``dlt.apply_changes``/
``create_streaming_table``. Registering a second, independent ``@dlt.table`` here under that
identical name unconditionally raised ``Cannot redefine dataset`` the first time an SCD/
snapshot-CDC flow was actually deployed. So the clean side is now registered as an internal
``@dlt.view`` (``_<target_table>_clean``) whenever a CDC strategy will own the real table name,
freeing that name for the dispatcher and handing it a quarantine-filtered source to read from
instead of the raw staged view (which, before this fix, let quarantined rows flow straight into
``apply_changes`` regardless of any configured quarantine rule). See
:func:`register_main_and_quarantine_tables`'s own docstring for the full account, including why
the quarantine table itself is always a real physical table either way.
"""

import logging
from typing import Any, Dict, List, Optional

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.cdc.comparison_columns import resolve_comparison_columns
from NextGen_Metadata_Framework.lakeflow_framework.cdc.hashing import compute_hash_columns
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.observability.structured_logger import log_flow_event
from NextGen_Metadata_Framework.lakeflow_framework.storage.column_ordering import reorder_columns_for_delta_stats
from NextGen_Metadata_Framework.lakeflow_framework.storage.table_properties import (
    build_auto_ttl_kwarg,
    build_partition_and_cluster_kwargs,
    build_table_properties,
    qualified_table_name,
)

logger = logging.getLogger("common.dq.quarantine")


_QUARANTINE_PROCESS_COLUMNS = ("__framework_dq_quarantine_flag", "__framework_dq_failed_rule_ids", "__framework_dq_failure_reasons")


def add_quarantine_columns(
    df: DataFrame,
    dq_rules: List[Dict[str, Any]],
    pipeline_run_id: Optional[str] = None,
    record_id_column: Optional[str] = None,
) -> DataFrame:
    """Attach quarantine routing + diagnostic metadata for ``action: quarantine`` rules.

    Adds, for every row:

    * ``__framework_dq_failed_rule_ids`` -- array of ``rule_id`` values whose expression evaluated to
      ``False`` for this row (i.e. the record violates the rule).
    * ``__framework_dq_failure_reasons`` -- array of human-readable ``"<rule_id>: failed expression
      '<expression>'"`` strings, one per failed rule -- the "failure reason" / "error
      message" a downstream consumer of the quarantine table needs without having to join
      back to the DQ rule configuration.
    * ``__framework_dq_quarantine_flag`` -- ``True`` when at least one quarantine rule failed; drives
      the main/quarantine table split in :func:`register_main_and_quarantine_tables`.
    * ``__framework_pipeline_run_id`` -- best-effort run/update identifier (whatever the caller
      resolved and passed in), for traceability from a quarantined row back to the run
      that produced it. Always added (``NULL`` if not supplied) so the column is stable
      across flows.
    * ``__framework_record_id`` -- value of ``record_id_column`` (from ``dq_config``, v2 schema --
      moved off ``target_config``) when configured and present on the DataFrame, else
      ``NULL``. Lets a quarantine consumer look up "which source record failed" without
      relying on row order.

    Parameters
    ----------
    df:
        Input DataFrame.
    dq_rules:
        This flow's full DQ rule list (only ``action: quarantine`` rules are evaluated
        here; ``warn``/``drop``/``fail`` are handled natively by
        :func:`common.dq.expectations.apply_dq_expectations`).
    pipeline_run_id:
        Best-effort run/update identifier; pass ``None`` when unavailable.
    record_id_column:
        Name of a column on ``df`` that uniquely identifies the source record, or ``None``.

    Raises
    ------
    FrameworkConfigError
        If a quarantine rule's expression cannot be parsed by ``F.expr``.
    """
    quarantine_rules = [r for r in dq_rules if r.get("action") == "quarantine"]

    if not quarantine_rules:
        df = (
            df.withColumn("__framework_dq_failed_rule_ids", F.array().cast("array<string>"))
            .withColumn("__framework_dq_failure_reasons", F.array().cast("array<string>"))
            .withColumn("__framework_dq_quarantine_flag", F.lit(False))
        )
    else:
        try:
            failed_id_exprs = [F.when(~F.expr(rule["expression"]), F.lit(rule["rule_id"])) for rule in quarantine_rules]
            failed_reason_exprs = [
                F.when(~F.expr(rule["expression"]), F.lit(f"{rule['rule_id']}: failed expression '{rule['expression']}'"))
                for rule in quarantine_rules
            ]
            df = df.withColumn(
                "__framework_dq_failed_rule_ids",
                F.array_except(F.array(*failed_id_exprs), F.array(F.lit(None).cast("string"))),
            ).withColumn(
                "__framework_dq_failure_reasons",
                F.array_except(F.array(*failed_reason_exprs), F.array(F.lit(None).cast("string"))),
            )
            df = df.withColumn("__framework_dq_quarantine_flag", F.size("__framework_dq_failed_rule_ids") > 0)
        except Exception as exc:  # noqa: BLE001
            raise FrameworkConfigError(f"Failed to evaluate quarantine rules {quarantine_rules}: {exc}") from exc

    df = df.withColumn("__framework_pipeline_run_id", F.lit(pipeline_run_id).cast("string"))

    if record_id_column and record_id_column in df.columns:
        df = df.withColumn("__framework_record_id", F.col(record_id_column).cast("string"))
    else:
        df = df.withColumn("__framework_record_id", F.lit(None).cast("string"))

    return df


def _apply_hash_columns(df: DataFrame, target_config: Dict[str, Any]) -> DataFrame:
    """Add ``__framework_hash_key``/``__framework_hash_value`` (framework design principle,
    default on) to a CDC-dispatched flow's clean upstream, before it ever reaches
    ``dlt.apply_changes``/``apply_changes_from_snapshot``.

    Only called for CDC-dispatched strategies (SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC) -- see
    ``register_main_and_quarantine_tables``'s ``needs_cdc_dispatch`` gate.

    **v1.4.0: surrogate keys are gone, and this function is what is left.** It was
    ``_apply_hash_and_surrogate_key_columns``, and the surrogate half was the larger one: it
    hashed every payload column of every row on every run to manufacture a diff key for
    ``FULL_SNAPSHOT_CDC_NO_PK``, forced itself on over an explicit ``false``, and carried two
    spec fields (``surrogate_key_columns``/``surrogate_key_exclude_columns``) whose only job was
    to narrow that hash back down. All of it is removed: ``FULL_SNAPSHOT_CDC_NO_PK`` no longer
    exists as a strategy, and every remaining CDC strategy diffs on ``target_config.primary_keys``
    -- a real key the source already has -- which is what ``dlt.apply_changes`` /
    ``apply_changes_from_snapshot`` were built to take (https://docs.databricks.com/aws/en/ldp/cdc).

    ``__framework_hash_key``/``__framework_hash_value`` are unaffected and still default on: they
    are cheap (one SHA-256 over the declared keys, one over the resolved comparison columns), and
    reconciliation reads them directly rather than recomputing (see
    ``reconciliation/matcher.py`` and each dataset's ``hash_precomputed`` flag).

    With no surrogate key to fall back on, a flow with no ``primary_keys`` now has no key at all,
    so hash generation is skipped with a WARNING. That combination is unreachable through
    onboarding -- ``spec_validator.py`` requires ``primary_keys`` for every CDC-dispatched
    strategy -- so it only fires for a hand-edited control-table row.
    """
    cdc_load_strategy = target_config.get("cdc_load_strategy")
    result_df = df

    if target_config.get("generate_hash_columns", True):
        primary_keys = target_config.get("primary_keys") or []
        if primary_keys:
            comparison_columns = resolve_comparison_columns(
                result_df.columns, primary_keys, target_config.get("columns_to_check"), target_config.get("columns_to_exclude")
            )
            result_df = compute_hash_columns(result_df, primary_keys, comparison_columns)
        else:
            logger.warning(
                "generate_hash_columns is set but no primary_keys are configured for cdc_load_strategy '%s' -- "
                "skipping __framework_hash_key/__framework_hash_value for this flow.",
                cdc_load_strategy,
            )

    return result_df


# Spark error-condition names meaning "this table/view does not exist". Matched by name rather
# than by message text so a Databricks-runtime wording change cannot silently turn a
# never-materialized target into a hard failure (or vice versa). DELTA_TABLE_NOT_FOUND and
# DELTA_PATH_DOES_NOT_EXIST cover the Delta-specific spellings; PATH_NOT_FOUND covers a target
# whose storage location was removed out from under the metastore entry.
_TABLE_NOT_FOUND_CONDITIONS = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "DELTA_TABLE_NOT_FOUND",
    "DELTA_PATH_DOES_NOT_EXIST",
    "PATH_NOT_FOUND",
)


def _is_table_not_found(exc: Exception) -> bool:
    """True only when ``exc`` specifically means "that table/view does not exist".

    Prefers PySpark's structured ``getErrorClass()``/``getCondition()`` when the exception
    exposes one; falls back to scanning the rendered message for a condition name only when it
    does not. The fallback is a substring check against those same uppercase condition tokens,
    never against free-form prose, so it stays insensitive to message rewording.
    """
    for accessor in ("getCondition", "getErrorClass"):
        getter = getattr(exc, accessor, None)
        if callable(getter):
            try:
                condition = getter()
            except Exception:  # noqa: BLE001 - a shim that raises tells us nothing; try the next one
                condition = None
            if condition:
                return any(name in str(condition).upper() for name in _TABLE_NOT_FOUND_CONDITIONS)
    rendered = str(exc).upper()
    return any(name in rendered for name in _TABLE_NOT_FOUND_CONDITIONS)


def _read_existing_target_or_none(qualified_table: str) -> Optional[DataFrame]:
    """Read a previously-materialized target table, or ``None`` when it does not exist yet.

    The ``existing_target_provider`` half of the ``TRUNCATE_AND_LOAD`` empty-source guard
    (``cdc/dispatcher.py::resolve_truncate_and_load_source``): when a full recompute produces zero
    records, the only way to leave a materialized view's contents alone is to hand its defining
    query exactly what the table already holds, so something has to read that prior state back.

    Deliberately a plain ``spark.read.table`` rather than a ``dlt.read``. This reads the table's
    PRIOR materialized state from *outside* the pipeline graph, which is exactly what preserving
    it across an empty-source recompute requires; a ``dlt.read`` of the very dataset this flow is
    in the middle of defining would resolve to the in-flight graph node -- the empty recompute we
    are trying not to publish -- or fail outright as a self-reference. It lives in this module
    rather than in the dispatcher because this is the only place holding the fully-qualified
    target name *and* the knowledge that the read must bypass the graph, which is precisely why
    the guard takes a callable instead of catalog/schema/table arguments (see its own docstring).

    **Existence is decided by attempting the read and matching the not-found condition
    precisely -- NOT by ``spark.catalog.tableExists``.** The original implementation used
    ``spark.catalog.tableExists`` on the reasoning that it separates "never materialized" from
    "catalog/permission failure" more crisply than exception-matching does. That reasoning is
    sound in general and wrong *here*: the ``spark.catalog`` API is not usable inside the
    Lakeflow graph-execution context this function runs in, and it fails with
    ``py4j.protocol.Py4JError: An error occurred while calling o<N>.tableExists`` rather than
    returning a boolean. Confirmed live on 2026-08-29 -- it took down ``TC-CDC-002``'s whole
    pipeline update.

    The safety property the original reasoning was protecting is preserved exactly, just
    enforced differently: ``None`` is returned **only** when the failure is specifically a
    missing table/view, matched on Spark's stable error-condition names. Anything else -- a
    permission denial, a corrupt table, a catalog outage -- propagates untouched. That
    distinction is load-bearing: mistaking a permission failure for a first-ever run would let
    an empty source through and blank a table that does exist, which is the precise data loss
    this guard exists to prevent. A missing ``SparkSession`` raises for the same reason.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise FrameworkConfigError(
            f"No active SparkSession available to read the existing contents of '{qualified_table}' for the "
            "TRUNCATE_AND_LOAD empty_target_if_source_empty guard -- this must run inside a Lakeflow "
            "Declarative Pipeline's graph-execution context."
        )
    try:
        target_df = spark.read.table(qualified_table)
        # Touching the schema forces analysis now, inside this try, so a not-found table is
        # classified here rather than surfacing later from a lazily-analysed DataFrame.
        target_df.schema  # noqa: B018 - deliberate, forces resolution
        return target_df
    except Exception as exc:  # noqa: BLE001 - narrowed immediately below by condition name
        if _is_table_not_found(exc):
            logger.info(
                "TRUNCATE_AND_LOAD empty-source guard: target '%s' has never been materialized -- "
                "treating as first-ever run.",
                qualified_table,
            )
            return None
        raise


def register_main_and_quarantine_tables(
    base_view_name: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    dq_rules: List[Dict[str, Any]],
    comment: Optional[str],
    is_streaming: bool,
    needs_cdc_dispatch: bool,
    quarantine_table_override: Optional[str] = None,
    flow_label: Optional[str] = None,
) -> str:
    """Register the quarantine-filtered "clean" dataset, plus its sibling quarantine table if configured.

    Both read from the same upstream staged view/table (``base_view_name``, always
    unqualified: staged views are graph-internal and stay in the pipeline's own default
    schema) so quarantine routing never duplicates read I/O against the original source.
    Published datasets go under ``target_catalog.target_schema`` (see
    :func:`common.storage.table_properties.qualified_table_name`) -- a bare ``name=``
    always resolves against the *pipeline's* default catalog/schema, not a flow's own
    configured target, so every final published table must be qualified explicitly.

    ``needs_cdc_dispatch`` controls what the clean (non-quarantined) side becomes, and is
    the fix for a real bug found via live deployment: when a flow's ``cdc_load_strategy``
    is anything other than ``APPEND``/``TRUNCATE_AND_LOAD``, :mod:`cdc.dispatcher` goes on
    to publish the *actual* target table under this same ``target_table`` name (via
    ``dlt.apply_changes``/``create_streaming_table``) -- registering a second, independent
    ``@dlt.table`` here under that identical name unconditionally raised ``Cannot redefine
    dataset`` for every SCD1/SCD2/SCD3/snapshot-CDC flow the first time one was actually
    deployed and run. So:

    * ``needs_cdc_dispatch=False`` (``APPEND``/``TRUNCATE_AND_LOAD``): the clean side *is*
      the final published table -- registered as ``@dlt.table(name=<qualified target>)``,
      same as before this fix. v1.3.0 briefly applied a ``TRUNCATE_AND_LOAD`` empty-source
      guard on this branch (``cdc/dispatcher.py::resolve_truncate_and_load_source``, fed by
      :func:`_read_existing_target_or_none`), preserving the target's previous contents when a
      full recompute produced zero rows. **It was withdrawn on 2026-08-29**: preserving those
      contents means the target reads itself, which Lakeflow rejects as a graph cycle, and the
      guard's eager emptiness test cannot distinguish "source is empty" from "source has not
      been materialized yet" during graph construction. ``empty_target_if_source_empty`` is
      consequently not enforced in-graph; see the inline note in ``_clean_upstream``.
    * ``needs_cdc_dispatch=True``: the clean side is registered as an internal
      ``@dlt.view`` (``_<target_table>_clean``) instead -- freeing the real
      ``target_table`` name for the CDC dispatcher to own, and giving it (via this
      function's return value) a quarantine-filtered source to read from. Before this
      fix, the CDC/SCD engine read the *raw* staged view directly and never saw the
      quarantine flag at all, silently letting quarantined rows flow into
      ``apply_changes`` regardless of any configured quarantine rule.

    The quarantine table (when any ``dq_rules`` has ``action: "quarantine"``) is
    registered identically either way -- a real physical table, never a view -- keeping
    every one of the DQ-process columns (``__framework_dq_quarantine_flag``, ``__framework_dq_failed_rule_ids``,
    ``__framework_dq_failure_reasons``) plus ``__framework_quarantine_validated_at`` (processing timestamp),
    together with ``__framework_pipeline_run_id``/``__framework_record_id`` (added upstream by
    :func:`add_quarantine_columns`) and ``__framework_source_file_name`` (added upstream by
    :func:`common.ingestion.technical_metadata.attach_technical_metadata`, when present) --
    giving every quarantined row: source file, record identifier, failed rule id(s),
    human-readable failure reason(s), processing timestamp, and pipeline/run identifier.

    ``quarantine_table_override`` is ``dq_config.quarantine_table`` (v2 schema -- moved off
    ``target_config``), used only when a quarantine table is actually created (see above);
    falls back to ``"<target_table>_quarantine"`` when absent.

    ``flow_label`` (``dataflow_id``/``flow_step_id``, Phase 10) is used only as the
    ``flow_id`` on the structured ``"dq_staging"`` JSON log event the quarantine-table closure
    below emits (falls back to ``target_table`` when omitted, so this stays optional/
    backward-compatible for any caller that doesn't have a flow identifier handy). See that
    closure's own comment for why real rejected/quarantined counts are only obtainable there
    (Lakeflow execution time, inside the ``@dlt.table`` closure) and only for a non-streaming
    target -- never at this function's own graph-*definition*-time scope, and never via an
    eager count against a streaming plan.

    Physical layout (``partition_cols``/``cluster_by``) is only ever applied on the
    ``needs_cdc_dispatch=False`` branch, because that is the only branch here that registers a
    real physical table -- the CDC branch registers a ``@dlt.view``, and the table it feeds is
    created later by ``cdc/dispatcher.py``. Both kwargs come from
    ``storage/table_properties.py::build_partition_and_cluster_kwargs`` rather than being read off
    ``target_config`` here, so that an *explicitly empty* ``partition_columns``/
    ``liquid_clustering_columns`` (a valid, deliberate "no partitioning"/"no clustering"
    configuration) is translated into an omitted kwarg instead of a zero-column
    ``partition_cols=[]``, and so that the maximum-clustering-columns cap is enforced at runtime
    as well as by the onboarding validator.

    Returns
    -------
    str
        The name a downstream CDC dispatch should read from: the internal clean view's
        name when ``needs_cdc_dispatch`` is ``True``, otherwise the qualified main table's
        own name (returned for completeness; ``APPEND``/``TRUNCATE_AND_LOAD`` callers don't
        dispatch to a CDC strategy and so never consume it).

    Raises
    ------
    FrameworkConfigError
        If DLT table registration fails (e.g. malformed ``target_config``, or an unsafe
        ``target_catalog``/``target_schema``/``target_table`` identifier), or if
        ``target_config.liquid_clustering_columns`` names more than
        ``storage/table_properties.py::MAX_LIQUID_CLUSTERING_COLUMNS`` columns.
    """
    try:
        qualified_main_table = qualified_table_name(target_catalog, target_schema, target_table)
        table_properties = build_table_properties(target_config)

        def _clean_upstream():
            upstream = dlt.read_stream(base_view_name) if is_streaming else dlt.read(base_view_name)
            clean_df = upstream.filter(~F.col("__framework_dq_quarantine_flag")).drop(*_QUARANTINE_PROCESS_COLUMNS)
            if needs_cdc_dispatch:
                clean_df = _apply_hash_columns(clean_df, target_config)
            # NOTE: no empty-source guard runs here. v1.3.0's E09 guard
            # (cdc/dispatcher.py::resolve_truncate_and_load_source) was wired in at this point and
            # preserved a TRUNCATE_AND_LOAD target by recomputing it from its OWN previous
            # contents, read via _read_existing_target_or_none(qualified_main_table). Lakeflow
            # cannot express that: a dataset reading itself is a self-edge, and graph construction
            # aborts with "Graph is not topologically sorted. There is a cycle between
            # <target> and <target>" -- observed live on 2026-08-29 for TC-CDC-002's
            # metaflow.silver_ref.dim_fx_rates_current, which failed before a single flow ran.
            #
            # The eager emptiness test the guard used is equally unusable here. This closure is
            # evaluated during graph CONSTRUCTION, when an upstream dataset produced by the same
            # update legitimately has no data yet, so "source is empty" cannot be distinguished
            # from "source has not been materialized yet" -- which is why the guard fired on a
            # flow whose source was never actually empty.
            #
            # `target_config.empty_target_if_source_empty` is therefore NOT enforced in-graph
            # today. Enforcing it needs a check that runs OUTSIDE the pipeline graph (a
            # post-update job task comparing the target's row count against the previous update,
            # in the style of reconciliation/), which is deliberately left for a follow-up rather
            # than shipped as a guard that makes every TRUNCATE_AND_LOAD pipeline fail outright.
            # Reorder once, here -- the single point both the non-CDC main-table path and the
            # CDC clean-source-view path converge on -- so every target table this flow can
            # produce gets its key/clustering/hash columns positioned within Delta's default
            # dataSkippingNumIndexedCols (32) stats-collection window. See
            # storage/column_ordering.py for why this must be a schema reorder, not a
            # whitelist.
            return reorder_columns_for_delta_stats(clean_df, target_config)

        if needs_cdc_dispatch:
            clean_view_name = f"_{target_table}_clean"

            # A view is correct for EVERY CDC strategy, including the snapshot ones. The snapshot
            # strategies cannot read this dataset from inside an apply_changes_from_snapshot
            # lambda (Lakeflow rejects referencing any pipeline dataset outside a dataset query
            # definition), but that is not this module's problem to solve: cdc/snapshot.py
            # registers its own materialized `_<target>_snapshot_input` dataset which reads this
            # view inside a real query definition, where dlt.read() is legal. See the comment
            # block in cdc/snapshot.py::register_full_snapshot_cdc for the two distinct
            # Lakeflow errors that pin this design down.
            @dlt.view(name=clean_view_name, comment=f"Quarantine-filtered input to the CDC strategy for {target_table}")
            def _clean_view():
                return _clean_upstream()

            cdc_source_view = clean_view_name
        else:
            auto_ttl_kwarg = build_auto_ttl_kwarg(target_config)

            table_kwargs: Dict[str, Any] = {
                "name": qualified_main_table,
                "comment": comment,
                "table_properties": table_properties,
            }
            # partition_cols/cluster_by are derived in exactly one place -- storage/
            # table_properties.py::build_partition_and_cluster_kwargs -- so this writer (today the
            # only one) and any future one share a single implementation of "an explicitly empty
            # partition_columns/liquid_clustering_columns means NO partitioning/clustering and must
            # never become a zero-column partition_cols=[]/cluster_by=[]" and of the
            # MAX_LIQUID_CLUSTERING_COLUMNS cap. The cap is a hard FrameworkConfigError; it
            # surfaces through this function's own except-wrapper below, which keeps the exception
            # type and embeds the helper's verbatim message in the "Failed to register main/
            # quarantine tables for '<target_table>'" wrapper.
            table_kwargs.update(build_partition_and_cluster_kwargs(target_config, table_label=qualified_main_table))
            if auto_ttl_kwarg:
                table_kwargs["auto_ttl"] = auto_ttl_kwarg

            @dlt.table(**table_kwargs)
            def _main_table():
                return _clean_upstream()

            cdc_source_view = qualified_main_table

        if any(rule.get("action") == "quarantine" for rule in dq_rules):
            quarantine_table_bare = quarantine_table_override or f"{target_table}_quarantine"
            quarantine_table_name = qualified_table_name(target_catalog, target_schema, quarantine_table_bare)

            @dlt.table(
                name=quarantine_table_name,
                comment=f"Quarantined records failing DQ quarantine rules for {target_table}",
                table_properties=table_properties,
            )
            def _quarantine_table():
                # This closure body runs at Lakeflow's graph-EXECUTION time (DLT calls it once,
                # to build the query plan for this table), unlike register_main_and_quarantine_
                # tables' own body above, which only ever runs at graph-DEFINITION time and
                # never sees real rows -- see engine/flow_registration.py's module docstring for
                # that distinction. That makes this the one place in the quarantine path a real
                # rejected/quarantined *count* is even possible to obtain, for the Phase 10
                # "records rejected/quarantined" structured-logging requirement.
                upstream = dlt.read_stream(base_view_name) if is_streaming else dlt.read(base_view_name)
                if is_streaming:
                    # A streaming DataFrame cannot be eagerly aggregated/collected here --
                    # Spark raises "Queries with streaming sources must be executed with
                    # writeStream.start()" -- and there is no lightweight, framework-owned hook
                    # into "this micro-batch just finished" from inside a plain @dlt.table
                    # closure (that would need a StreamingQueryListener wired into the
                    # pipeline's own query lifecycle, out of scope for this phase). Emit a
                    # registration-only event (no counts) so this flow/target_table is still
                    # discoverable in the structured log stream; Lakeflow's own native
                    # flow_progress event is the honest source of real per-microbatch counts
                    # for a streaming target -- see observability/structured_logger.py's module
                    # docstring.
                    log_flow_event(
                        operation="dq_staging",
                        flow_id=flow_label or target_table,
                        status="SUCCESS",
                        target_table=target_table,
                        is_streaming=True,
                        note="streaming target -- per-microbatch counts not eagerly countable here; see Lakeflow's native flow_progress event",
                    )
                else:
                    # Non-streaming (batch_table/materialized_view) target: this closure runs
                    # exactly once per pipeline update, so one extra single-pass aggregation
                    # (total + quarantined count together, not two separate .count() scans) is
                    # an acceptable, deliberate trade-off for an accurate business-level count --
                    # see this function's docstring. Still one full extra scan beyond the write
                    # DLT itself performs from the DataFrame this closure returns.
                    agg_row = upstream.agg(
                        F.count(F.lit(1)).alias("total"),
                        F.sum(F.col("__framework_dq_quarantine_flag").cast("long")).alias("quarantined"),
                    ).collect()[0]
                    total = agg_row["total"] or 0
                    quarantined = agg_row["quarantined"] or 0
                    log_flow_event(
                        operation="dq_staging",
                        flow_id=flow_label or target_table,
                        status="SUCCESS",
                        records_read=total,
                        records_rejected=quarantined,
                        records_quarantined=quarantined,
                        target_table=target_table,
                        is_streaming=False,
                    )
                quarantined_df = upstream.filter(F.col("__framework_dq_quarantine_flag")).withColumn(
                    "__framework_quarantine_validated_at", F.current_timestamp()
                )
                # The quarantine table's own diagnostic columns -- what a quarantine-table
                # consumer is most likely to filter/join on (which rule failed, why, when) --
                # aren't in reorder_columns_for_delta_stats' fixed priority list (they don't
                # exist on the main/clean table its other caller uses), so they're passed
                # explicitly here via extra_priority_columns.
                return reorder_columns_for_delta_stats(
                    quarantined_df,
                    target_config,
                    extra_priority_columns=(*_QUARANTINE_PROCESS_COLUMNS, "__framework_quarantine_validated_at"),
                )

        return cdc_source_view
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to register main/quarantine tables for '{target_table}': {exc}") from exc
