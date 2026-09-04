"""Single entry point routing a ``cdc_load_strategy`` name to its implementing module.

Called once per flow from ``engine/flow_registration.py``, after the shared main/quarantine
table registration (``dq/quarantine.py::register_main_and_quarantine_tables``) has already
produced a quarantine-filtered source view or table. This module's only job is the fan-out:
translate a spec's ``target_config.cdc_load_strategy`` string into a call against whichever
module actually implements it -- ``cdc/scd.py`` for ``SCD1``/``SCD2``/``SCD3``, ``cdc/snapshot.py``
for ``FULL_SNAPSHOT_CDC`` -- so neither ``flow_registration.py`` nor
any one strategy module needs to know the others exist.

``APPEND``/``TRUNCATE_AND_LOAD`` are deliberately absent from the dispatch table: those two
strategies have no post-processing step at all (a streaming_table is a pure append; a
materialized_view is always a full recompute), so the caller has already published
``source_view`` directly as the target table before this dispatcher is ever consulted -- see
:func:`register_cdc_strategy`'s own docstring. Adding a new CDC strategy to the framework means
adding one new implementing module/function plus one new branch here; nothing else needs to
change.

One piece of ``TRUNCATE_AND_LOAD`` *semantics* nonetheless lives in this module even though the
dispatcher never dispatches that strategy: :func:`resolve_truncate_and_load_source`, the guard
that stops a zero-record source from silently blanking an already-populated target. It belongs
here because deciding what ``TRUNCATE_AND_LOAD`` means is strategy knowledge -- the function
reads ``target_config.cdc_load_strategy`` itself and is inert for every other strategy -- whereas
``dq/quarantine.py`` only ever knows the coarser fact "this flow does not need a CDC dispatch".
Keeping it here means the quarantine module never grows a second, parallel notion of what
``TRUNCATE_AND_LOAD`` is. It is therefore called directly from
``dq/quarantine.py::register_main_and_quarantine_tables``'s ``_clean_upstream`` closure and
**not** from :func:`register_cdc_strategy`: by the time the dispatcher runs, the materialized
view defining the target has already been registered and there is nothing left to guard.
"""

import logging
from typing import Any, Callable, Dict, Optional

from pyspark.sql import DataFrame

from flowx.lakeflow_framework.cdc.scd import register_scd1, register_scd2, register_scd3
from flowx.lakeflow_framework.cdc.snapshot import register_full_snapshot_cdc
from flowx.lakeflow_framework.exceptions import CdcStrategyError

logger = logging.getLogger("common.cdc.dispatcher")

#: Named rather than inlined because two very different parts of this module reference it: the
#: no-op dispatch set below, and :func:`resolve_truncate_and_load_source`'s inertness check. One
#: spelling keeps them from ever drifting apart.
TRUNCATE_AND_LOAD_STRATEGY = "TRUNCATE_AND_LOAD"

_NO_OP_STRATEGIES = {"APPEND", TRUNCATE_AND_LOAD_STRATEGY}
# FULL_SNAPSHOT_CDC_NO_PK was removed in v1.4.0 with the surrogate-key engine -- see
# cdc/snapshot.py. Kept as a set (not collapsed to an equality test) because the dispatch
# shape is per-family, and a future keyed snapshot variant belongs in this set, not in a
# second branch.
SNAPSHOT_STRATEGIES = {"FULL_SNAPSHOT_CDC"}


def register_cdc_strategy(
    flow_id: str,
    cdc_load_strategy: str,
    source_view: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    table_properties: Dict[str, str],
    is_streaming: bool = False,
) -> None:
    """Apply the configured CDC / load strategy on top of a staged source view.

    ``APPEND``/``TRUNCATE_AND_LOAD`` are no-ops here: the caller registers ``source_view``
    directly as ``target_table`` (streaming_table -> APPEND, materialized_view -> full
    recompute == TRUNCATE_AND_LOAD) before this dispatcher is ever consulted.
    ``target_catalog``/``target_schema`` are forwarded unchanged to every strategy so the
    published table lands in the flow's own configured schema rather than the pipeline's
    default (see ``common.storage.table_properties.qualified_table_name``).

    ``TRUNCATE_AND_LOAD``'s empty-source guard is deliberately *not* applied from here: it has to
    run while the target's defining DataFrame is still being built, one layer upstream in
    ``dq/quarantine.py``. See :func:`resolve_truncate_and_load_source`.

    Raises
    ------
    CdcStrategyError
        If ``cdc_load_strategy`` is not one of the supported strategies.
    """
    if cdc_load_strategy in _NO_OP_STRATEGIES:
        return

    if cdc_load_strategy == "SCD1":
        register_scd1(flow_id, source_view, target_table, target_catalog, target_schema, target_config, table_properties)
        return
    if cdc_load_strategy == "SCD2":
        register_scd2(flow_id, source_view, target_table, target_catalog, target_schema, target_config, table_properties)
        return
    if cdc_load_strategy == "SCD3":
        register_scd3(flow_id, source_view, target_table, target_catalog, target_schema, target_config, table_properties)
        return
    if cdc_load_strategy in SNAPSHOT_STRATEGIES:
        register_full_snapshot_cdc(
            flow_id, source_view, target_table, target_catalog, target_schema, target_config, table_properties,
            cdc_load_strategy, is_streaming
        )
        return

    raise CdcStrategyError(f"Flow '{flow_id}': unsupported cdc_load_strategy '{cdc_load_strategy}'")


def resolve_truncate_and_load_source(
    source_df: DataFrame,
    existing_target_provider: Callable[[], Optional[DataFrame]],
    target_config: Dict[str, Any],
    flow_label: str,
    target_label: str,
) -> DataFrame:
    """Guard a ``TRUNCATE_AND_LOAD`` flow against an empty source blanking its target.

    .. warning::
       **Currently UNWIRED -- nothing calls this.** It was called from
       ``dq/quarantine.py::register_main_and_quarantine_tables``'s ``_clean_upstream`` closure
       until 2026-08-29, when live execution showed the approach cannot work inside a Lakeflow
       graph at all. Step 5's ``existing_target_provider()`` makes the target read itself, and
       Lakeflow aborts graph construction with ``Graph is not topologically sorted. There is a
       cycle between <target> and <target>`` (TC-CDC-002). Step 4's eager emptiness test is
       unusable for the same reason: the closure runs during graph CONSTRUCTION, when an upstream
       produced by the same update has legitimately not been materialized yet, so "empty source"
       and "not yet built" are indistinguishable.

       The function is kept, unchanged, because the *policy* it encodes is still wanted -- but it
       has to be evaluated OUTSIDE the pipeline graph (a post-update job task comparing row counts
       across updates, in the style of ``reconciliation/``). Do not re-wire it into a dataset
       query definition; that is the exact configuration that fails.

    A ``TRUNCATE_AND_LOAD`` target is registered as a ``@dlt.table`` fed by a full recompute of
    its clean upstream, so the strategy's "truncate" half was never an explicit statement
    anywhere -- it is simply what a materialized view *does* when its defining query returns
    nothing. That made a missed or short delivery indistinguishable from an intentionally empty
    snapshot: a zero-record extract silently wiped a populated production table with no signal of
    any kind. This function turns that distinction into an explicit, opt-in configuration
    decision (``target_config.empty_target_if_source_empty``).

    Resolution, in order -- each step returns ``source_df`` unchanged, i.e. exactly the
    pre-v1.3.0 behaviour:

    1. ``target_config.cdc_load_strategy`` is anything other than ``TRUNCATE_AND_LOAD``. The
       function is fully inert for every other strategy, which is what lets the caller apply it
       unconditionally on its non-CDC branch without first re-deriving the strategy itself.
    2. ``target_config.empty_target_if_source_empty`` is true -- the operator has explicitly
       asked for the old unconditional behaviour. **The default is false**: blanking a table is
       irreversible from inside a pipeline update, and in the field a zero-record extract is far
       more often a delivery failure than a deliberate "there is genuinely nothing left" signal,
       so the safe default has to be "keep what we have and tell someone".
    3. ``source_df.isStreaming`` -- an existence check is an eager action, and Spark rejects one
       against a streaming plan outright (``Queries with streaming sources must be executed with
       writeStream.start()``). This should be unreachable today: a ``TRUNCATE_AND_LOAD`` target is
       a materialized view / batch table, and its clean upstream is read with ``dlt.read`` rather
       than ``dlt.read_stream``. It is checked anyway so that a future wiring change degrades to
       today's behaviour with a WARNING, instead of failing an entire update from inside a guard
       whose only purpose is to make things safer.
    4. The source has at least one row -- the overwhelmingly common case, behaviourally identical
       to today.

    Only when all four fall through is the target preserved: ``existing_target_provider()`` is
    called and its DataFrame is returned *in place of* the empty source, so the materialized view
    recomputes itself from its own previously-materialized contents. That round trip is the whole
    mechanism -- an MV has no "skip this update" option, so the only way to leave its contents
    alone is to hand its defining query exactly what the table already holds. When the provider
    returns ``None`` (the target has never been materialized -- first ever run) there is nothing
    to preserve and the empty source is used unchanged, logged at INFO rather than WARNING
    because creating a brand-new table empty is not a data-loss event.

    Parameters
    ----------
    source_df:
        The flow's fully-built clean upstream, immediately before it becomes the materialized
        view's defining DataFrame.
    existing_target_provider:
        Zero-argument callable returning the target's previously-materialized contents, or
        ``None`` when the target does not exist yet. Passed in as a callable rather than having
        this function take catalog/schema/table names and read the table itself, for two reasons:
        the read must not happen at all on the hot path (steps 1-4 above return without ever
        touching the catalog), and the caller already holds both the fully-qualified name and the
        knowledge that this read has to be a plain ``spark.read.table`` of the table's *prior*
        state rather than a ``dlt.read`` of the in-flight graph node. The callable is required to
        return ``None`` rather than raise when the target is missing; anything it does raise is
        deliberately left to propagate, since a genuine catalog or permission failure must fail
        the update rather than be quietly mistaken for "the target does not exist".
    target_config:
        The flow's ``target_config``. Read for ``cdc_load_strategy`` and
        ``empty_target_if_source_empty`` only.
    flow_label:
        Human-readable flow identifier, used only in the log lines -- an operator reading the
        "target preserved" WARNING needs to know which flow produced nothing.
    target_label:
        Fully-qualified name of the target table, likewise for the log lines.

    Returns
    -------
    DataFrame
        ``source_df`` in every case above, or the target's previously-materialized contents when
        the guard fires.
    """
    if target_config.get("cdc_load_strategy") != TRUNCATE_AND_LOAD_STRATEGY:
        return source_df

    if target_config.get("empty_target_if_source_empty", False):
        return source_df

    if source_df.isStreaming:
        logger.warning(
            "Flow '%s': empty_target_if_source_empty cannot be evaluated against a STREAMING source -- an "
            "existence check is an eager action and is illegal on a streaming plan. Leaving the source untouched.",
            flow_label,
        )
        return source_df

    # limit(1).take(1), never count(): count() forces a full aggregate over the entire recomputed
    # plan -- for a TRUNCATE_AND_LOAD flow that is the whole source, on every single update, just
    # to answer a yes/no question. limit(1) lets Spark stop as soon as it has seen one row.
    if source_df.limit(1).take(1):
        return source_df

    existing_target_df = existing_target_provider()
    if existing_target_df is None:
        logger.info(
            "Flow '%s': TRUNCATE_AND_LOAD source produced zero records and the target '%s' does not exist yet "
            "-- nothing to preserve; the target will be created empty.",
            flow_label,
            target_label,
        )
        return source_df

    logger.warning(
        "Flow '%s': TRUNCATE_AND_LOAD source produced zero records and "
        "target_config.empty_target_if_source_empty is false -- preserving the existing contents of '%s' "
        "instead of truncating it. Set empty_target_if_source_empty: true to allow an empty source to "
        "blank this target.",
        flow_label,
        target_label,
    )
    return existing_target_df
