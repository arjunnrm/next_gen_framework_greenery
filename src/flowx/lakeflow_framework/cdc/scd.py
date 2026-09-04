"""Slowly Changing Dimension strategies: SCD1/SCD2 (native ``apply_changes``) and SCD3 (derived pivot).

Dispatched from ``cdc/dispatcher.py::register_cdc_strategy`` for any flow whose
``target_config.cdc_load_strategy`` is ``SCD1``, ``SCD2``, or ``SCD3``. All three strategies
share the same key/sequencing resolution (:func:`_resolve_keys_and_sequence` -- ``primary_keys``
is required, ``sequence_by_column`` falls back to the framework's own
``__framework_ingestion_timestamp_utc``, see ``ingestion/technical_metadata.py``, since
``dlt.apply_changes`` always needs *some* monotonic sequencer) and the same optional
delete-marking (:func:`_build_apply_as_deletes_expr`, for CDC feeds that carry an explicit
``cdc_operation_column``/``cdc_operation_mapping.delete_values``) and column-exclusion
(:func:`_build_except_column_list`) knobs.

SCD1/SCD2 are native ``dlt.apply_changes`` -- Lakeflow does all the merge/history-tracking work.
SCD3 has no native Lakeflow equivalent at all: it is built here by materializing full history
into a hidden ``_<target_table>_scd2_history`` table via ``stored_as_scd_type="2"``, then
ranking each key's versions by ``__START_AT`` and pivoting the two most recent into
``current_<col>``/``previous_<col>`` columns on the public target table (:func:`register_scd3`).

SCD2 also registers a companion ``<target_table>_current`` reporting table
(:func:`register_scd2_reporting_view`) that aliases Lakeflow's native ``__START_AT``/
``__END_AT`` tracking columns to the more conventional ``valid_from``/``valid_to``/``is_current``
names. That reporting dataset is registered as a real ``@dlt.table``, not a ``@dlt.view`` (despite
its name), after two real bugs found via live deployment: qualifying a ``@dlt.view``'s ``name=``
the same way a table's is qualified raised ``AnalysisException`` (multipart names aren't
supported for views), and even once made unqualified, a ``@dlt.view`` turned out not to be a
durable, queryable catalog object once the pipeline update that defined it finished
(``TABLE_OR_VIEW_NOT_FOUND``) -- see that function's own docstring for the full account.
"""

import logging
from typing import Any, Dict, List, Optional

import dlt
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from flowx.lakeflow_framework.exceptions import CdcStrategyError
from flowx.lakeflow_framework.ingestion.technical_metadata import (
    FRAMEWORK_INGESTION_TIMESTAMP_COLUMN,
)
from flowx.lakeflow_framework.storage.table_properties import qualified_table_name

logger = logging.getLogger("common.cdc.scd")


def _resolve_keys_and_sequence(flow_id: str, strategy: str, target_config: Dict[str, Any]) -> "tuple[List[str], str]":
    """Resolve ``primary_keys`` (required) and ``sequence_by_column`` (optional).

    ``sequence_by_column`` falls back to ``__framework_ingestion_timestamp_utc`` when unset
    -- ``dlt.apply_changes`` itself has no concept of "no sequencer", so a source that has
    no natural version/timestamp column of its own still needs *something* monotonic to
    sequence by; the framework's own ingestion-time technical timestamp (always present,
    see ``ingestion/technical_metadata.py``) is the natural default.
    """
    keys = target_config.get("primary_keys", [])
    if not keys:
        raise CdcStrategyError(f"Flow '{flow_id}': {strategy} requires target_config.primary_keys")
    sequence_by = target_config.get("sequence_by_column") or FRAMEWORK_INGESTION_TIMESTAMP_COLUMN
    return keys, sequence_by


def _build_except_column_list(target_config: Dict[str, Any]) -> Optional[List[str]]:
    """Read the optional ``columns_to_exclude`` passthrough for ``dlt.apply_changes``.

    Maps directly onto ``apply_changes``'s native ``except_column_list`` parameter --
    lets a target with many source columns (e.g. a 50-column wide extract) name the
    handful of technical/audit columns (load timestamps, batch/run ids, checksum hashes)
    that should never land in the target table at all, instead of having to enumerate
    every column to *keep*. Excluded columns are simply absent from the resulting Delta
    table's schema -- they never participate in the upsert, so a technical column
    changing on every run (as it naturally does) can't affect what SCD1 or SCD2 store.
    Returns ``None`` when unset, meaning "keep every source column" (the prior default
    behavior, unchanged).
    """
    return target_config.get("columns_to_exclude") or None


def _build_apply_as_deletes_expr(target_config: Dict[str, Any]) -> Optional[Column]:
    """Derive an optional ``apply_as_deletes`` predicate from an operation column + mapping.

    Reuses the same ``cdc_operation_column``/``cdc_operation_mapping.delete_values`` shape
    already validated for snapshot CDC (see ``cdc/snapshot.py``) so a CDC feed carrying an
    explicit operation indicator (e.g. Zerobus/CDC-bus records with ``op`` in
    ``{"I", "U", "D"}``) can mark rows as deletes for ``dlt.apply_changes`` -- without this,
    SCD1/SCD2 targets could only ever grow (inserts/updates), never honor a source delete.

    Returns ``None`` when no operation column is configured, meaning "this flow's CDC feed
    never carries deletes" -- a normal, valid configuration.
    """
    operation_column = target_config.get("cdc_operation_column")
    delete_values = (target_config.get("cdc_operation_mapping") or {}).get("delete_values")
    if not operation_column or not delete_values:
        return None
    return F.col(operation_column).isin(delete_values)


def register_scd1(
    flow_id: str,
    source_view: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    table_properties: Dict[str, str],
) -> None:
    """Register an SCD Type 1 (overwrite-on-match) target via ``dlt.apply_changes``.

    Honors an optional ``cdc_operation_column``/``cdc_operation_mapping.delete_values`` in
    ``target_config`` to mark matching source rows as deletes (``apply_as_deletes``) --
    e.g. a Zerobus/CDC-bus feed where a row's ``op`` column is ``"D"``. Published under
    ``target_catalog.target_schema`` (a bare name resolves against the pipeline's own
    default schema, not this flow's configured target -- see
    ``common.storage.table_properties.qualified_table_name``).

    Also honors an optional ``columns_to_exclude`` (see :func:`_build_except_column_list`)
    so a wide source (e.g. 50 columns) can drop a handful of technical/audit columns from
    the target without having to enumerate every column to keep.
    """
    keys, sequence_by = _resolve_keys_and_sequence(flow_id, "SCD1", target_config)
    apply_as_deletes = _build_apply_as_deletes_expr(target_config)
    except_column_list = _build_except_column_list(target_config)

    try:
        qualified_target = qualified_table_name(target_catalog, target_schema, target_table)
        dlt.create_streaming_table(name=qualified_target, table_properties=table_properties)
        dlt.apply_changes(
            target=qualified_target,
            source=source_view,
            keys=keys,
            sequence_by=F.col(sequence_by),
            apply_as_deletes=apply_as_deletes,
            except_column_list=except_column_list,
            stored_as_scd_type="1",
        )
    except Exception as exc:  # noqa: BLE001
        raise CdcStrategyError(f"Flow '{flow_id}': failed to register SCD1 target '{target_table}': {exc}") from exc


def register_scd2(
    flow_id: str,
    source_view: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    table_properties: Dict[str, str],
) -> None:
    """Register an SCD Type 2 (full history) target via ``dlt.apply_changes``.

    Honors the same optional delete-marking as :func:`register_scd1` (see
    :func:`_build_apply_as_deletes_expr`) -- a deleted key's currently-open version is
    closed (``__END_AT`` set) rather than the row disappearing outright, preserving history.
    Published under ``target_catalog.target_schema``, same reasoning as
    :func:`register_scd1`.

    Also registers a companion reporting view (``<target_table>_current``, see
    :func:`register_scd2_reporting_view`) that aliases Lakeflow's native
    ``__START_AT``/``__END_AT`` SCD2 tracking columns to the more conventional
    ``valid_from``/``valid_to``/``is_current`` names, without duplicating storage or
    history-tracking logic.
    """
    keys, sequence_by = _resolve_keys_and_sequence(flow_id, "SCD2", target_config)
    apply_as_deletes = _build_apply_as_deletes_expr(target_config)
    except_column_list = _build_except_column_list(target_config)

    try:
        qualified_target = qualified_table_name(target_catalog, target_schema, target_table)
        dlt.create_streaming_table(name=qualified_target, table_properties=table_properties)
        dlt.apply_changes(
            target=qualified_target,
            source=source_view,
            keys=keys,
            sequence_by=F.col(sequence_by),
            apply_as_deletes=apply_as_deletes,
            except_column_list=except_column_list,
            stored_as_scd_type="2",
            track_history_column_list=target_config.get("columns_to_check"),
        )
        register_scd2_reporting_view(target_table, target_catalog, target_schema, table_properties)
    except Exception as exc:  # noqa: BLE001
        raise CdcStrategyError(f"Flow '{flow_id}': failed to register SCD2 target '{target_table}': {exc}") from exc


def register_scd2_reporting_view(
    target_table: str, target_catalog: str, target_schema: str, table_properties: Dict[str, str]
) -> None:
    """Register ``<target_table>_current``, a friendly-column table over a native SCD2 table.

    Lakeflow's ``apply_changes(stored_as_scd_type="2")`` manages history internally via
    ``__START_AT``/``__END_AT`` (the current version has ``__END_AT IS NULL``) -- this is
    the correct, native mechanism and is left untouched. This dataset simply re-labels
    those columns as ``valid_from``/``valid_to``/``is_current`` for consumers who expect
    conventional SCD2 column names, without a second copy of the history-*tracking logic*
    (the SCD2 table above is still the sole source of truth `apply_changes` maintains).

    Registered as a real, materialized ``@dlt.table`` -- **not** a ``@dlt.view``, despite
    the name. Two real bugs were found in sequence getting here, both via live
    deployment: (1) qualifying a ``@dlt.view``'s own ``name=`` the same way a table's is
    qualified raised ``AnalysisException: View with multipart name '...' is not
    supported``, so this was changed to an *unqualified* view; but (2) once an SCD2 flow
    actually ran end to end and something tried to query the reporting view afterward, it
    raised ``TABLE_OR_VIEW_NOT_FOUND`` -- confirmed empirically that a ``@dlt.view`` is
    never a durable, queryable catalog object once the pipeline update that defined it
    finishes, making a "reporting view" meant for consumers to query *after* the pipeline
    runs completely useless as a view. A real ``@dlt.table`` doesn't have either problem:
    its `name=` accepts full qualification like any other table, and it's durably
    queryable afterward like any other table -- the storage cost of one extra small,
    derived Delta table per SCD2 target is the right trade for a reporting dataset that
    actually needs to be queried outside the pipeline.
    """
    try:
        qualified_target = qualified_table_name(target_catalog, target_schema, target_table)
        qualified_view_name = qualified_table_name(target_catalog, target_schema, f"{target_table}_current")

        @dlt.table(
            name=qualified_view_name,
            comment=f"SCD2 reporting table over '{qualified_target}': valid_from/valid_to/is_current aliases",
            table_properties=table_properties,
        )
        def _scd2_reporting_view():
            history_df = dlt.read(qualified_target)
            return history_df.withColumn("valid_from", F.col("__START_AT")).withColumn(
                "valid_to", F.col("__END_AT")
            ).withColumn("is_current", F.col("__END_AT").isNull())
    except Exception as exc:  # noqa: BLE001
        raise CdcStrategyError(f"Failed to register SCD2 reporting view over '{target_table}': {exc}") from exc


def register_scd3(
    flow_id: str,
    source_view: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    table_properties: Dict[str, str],
) -> None:
    """Implement SCD3 (current/previous columns) as a pivot over an internal SCD2 history table.

    Lakeflow's ``apply_changes`` natively supports only SCD type 1/2. SCD3 is realized here
    by materializing full history via ``stored_as_scd_type="2"`` into a hidden internal
    table, then ranking each key's versions by ``__START_AT`` and pivoting the two most
    recent into ``current_<col>`` / ``previous_<col>`` columns on the public target table.
    Both the hidden history table and the public target table are published under
    ``target_catalog.target_schema``, same reasoning as :func:`register_scd1`.

    The current/previous pivot is computed as a single windowed pass with **no self-join**:
    a ``row_number()`` window (partitioned by ``keys``, ordered by ``__START_AT`` desc) tags
    each row's rank, and a second, unordered window over the same partitioning (so its default
    frame is the whole partition, not a running total) uses
    ``F.max_by(col, F.when(rank == 2, __START_AT))`` to pull each key's rank-2 value onto
    every row of that key's partition -- the ``F.when`` makes the "order-by" argument NULL for
    every row except the rank-2 one, and ``max_by`` ignores NULL ordering values, so the result
    is exactly that key's previous value with no join required. Filtering down to rank 1
    afterward then yields one row per key carrying both ``current_<col>`` (its own tracked
    column, no aggregation needed since this row already *is* the latest version) and
    ``previous_<col>`` (picked up from its sibling row via the window above). The old
    implementation built two separate rank-1/rank-2 DataFrames and joined them on ``keys`` --
    functionally equivalent, but a full shuffle-heavy self-join over the whole history table on
    every run that gets more expensive as history grows; this version shuffles/sorts by
    ``keys`` once and never duplicates rows across two sides of a join. Verified empirically
    (via Databricks Connect against serverless compute) to produce byte-identical output
    (columns, order, and values, including the single-version-so-far and tied-``__START_AT``
    edge cases) to the prior self-join implementation, and confirmed via ``.explain(True)``
    that the physical plan contains no ``Join`` node.
    """
    keys, sequence_by = _resolve_keys_and_sequence(flow_id, "SCD3", target_config)
    tracked_columns = target_config.get("columns_to_check")
    if not tracked_columns:
        raise CdcStrategyError(f"Flow '{flow_id}': SCD3 requires target_config.columns_to_check")
    except_column_list = _build_except_column_list(target_config)

    try:
        qualified_target = qualified_table_name(target_catalog, target_schema, target_table)
        qualified_history = qualified_table_name(target_catalog, target_schema, f"_{target_table}_scd2_history")
        dlt.create_streaming_table(name=qualified_history, table_properties=table_properties)
        dlt.apply_changes(
            target=qualified_history,
            source=source_view,
            keys=keys,
            sequence_by=F.col(sequence_by),
            except_column_list=except_column_list,
            stored_as_scd_type="2",
        )

        @dlt.table(
            name=qualified_target,
            comment="SCD3 current/previous attribute tracking, derived from SCD2 history",
            table_properties=table_properties,
        )
        def _scd3_view():
            history_df = dlt.read(qualified_history)

            # Rank each key's versions by recency; rank 1 is the row we ultimately keep.
            rank_window = Window.partitionBy(*keys).orderBy(F.col("__START_AT").desc())
            ranked_df = history_df.withColumn("_version_rank", F.row_number().over(rank_window))

            # No ORDER BY here on purpose: an aggregate window function's default frame is the
            # *whole partition* only when the window has no orderBy. (Reusing rank_window instead
            # would silently narrow max_by's frame to a running total by __START_AT and break it.)
            partition_window = Window.partitionBy(*keys)

            passthrough_columns = [c for c in history_df.columns if c not in tracked_columns]
            current_columns = [F.col(c).alias(f"current_{c}") for c in tracked_columns]
            previous_columns = [
                F.max_by(F.col(c), F.when(F.col("_version_rank") == 2, F.col("__START_AT")))
                .over(partition_window)
                .alias(f"previous_{c}")
                for c in tracked_columns
            ]

            return (
                ranked_df.select(*passthrough_columns, "_version_rank", *current_columns, *previous_columns)
                .filter(F.col("_version_rank") == 1)
                .drop("_version_rank")
            )
    except Exception as exc:  # noqa: BLE001
        raise CdcStrategyError(f"Flow '{flow_id}': failed to register SCD3 target '{target_table}': {exc}") from exc
