"""Reorder a target table's output schema so Delta's default data-skipping statistics --
``delta.dataSkippingNumIndexedCols``, default 32 -- actually cover the columns queries and
joins depend on most.
"""

from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame

from flowx.lakeflow_framework.cdc.hashing import HASH_KEY_COLUMN, HASH_VALUE_COLUMN

# Priority order (highest first) for the columns moved to the front of a target table's
# schema -- see reorder_columns_for_delta_stats. Primary/match keys and clustering columns
# come from target_config (variable names); the framework's own generated columns are fixed
# names, appended after whatever the caller configured.
_FRAMEWORK_GENERATED_FRONT_COLUMNS = (HASH_KEY_COLUMN, HASH_VALUE_COLUMN)


def reorder_columns_for_delta_stats(
    df: DataFrame, target_config: Dict[str, Any], extra_priority_columns: Optional[Sequence[str]] = None
) -> DataFrame:
    """Move key/clustering/hash columns to the front of ``df``'s schema, before it becomes a
    target table's actual materialized output.

    Delta's automatic data-skipping (min/max/null-count) statistics are collected for only
    the *first* ``delta.dataSkippingNumIndexedCols`` columns of a table (default 32) -- a
    column-**position** limit, not a configurable whitelist. Appending a framework-computed
    column via ``withColumn`` (the natural, simplest way to add one -- see
    ``cdc/hashing.py::compute_hash_columns``) always places it *last* in the schema, which silently drops it out of stats coverage the
    moment a table has more than ~32 business columns ahead of it -- exactly the columns
    (primary keys, clustering keys, the hash columns reconciliation joins on) that most need
    that coverage at any real scale.

    This is a pure column projection (``DataFrame.select`` over existing columns), not a data
    transform -- no shuffle, no value recomputation, just a schema-level reordering applied
    once, immediately before a flow's clean/quarantine output becomes its actual
    materialized (or CDC-dispatched) table.

    Priority order (highest first), each included only if actually present on ``df`` (a
    caller may configure a key/clustering column that hasn't reached this DataFrame yet, or
    a strategy -- e.g. ``APPEND`` -- that never gets hash columns at all):

    1. ``target_config.primary_keys`` -- the natural or CDC match-key columns.
    2. ``target_config.liquid_clustering_columns`` -- deliberately *not* ``partition_columns``:
       partition pruning is directory-based, not stats-based, so a partition column has no
       32-column stats concern to solve here. (This is also why an *explicitly empty*
       ``partition_columns``, which ``storage/table_properties.py::
       build_partition_and_cluster_kwargs`` treats as a deliberate "no partitioning" decision,
       needs no handling at all in this function: ``partition_columns`` is never read here in the
       first place.) The clustering list is bounded to
       ``table_properties.py::MAX_LIQUID_CLUSTERING_COLUMNS`` entries by the onboarding validator
       and by that same kwarg builder, so this front group can never be widened past that cap by
       clustering configuration alone -- no count check is repeated here, since front-loading is a
       pure projection and would be harmless even if it were.
    3. ``__framework_hash_key`` / ``__framework_hash_value`` (added when
       ``generate_hash_columns`` is set -- see ``cdc/hashing.py``). ``__framework_surrogate_key``
       used to sit ahead of these; it was removed in v1.4.0 with the rest of the surrogate-key
       engine, so this front group is one column shorter.
    4. ``extra_priority_columns`` -- caller-supplied additions, lowest priority of the group.
       Used by ``dq/quarantine.py``'s quarantine-table closure to also front-load
       ``__framework_dq_quarantine_flag``/``__framework_dq_failed_rule_ids``/``__framework_dq_failure_reasons``/
       ``__framework_quarantine_validated_at`` -- the columns a quarantine-table consumer is most likely
       to filter/join on, which don't exist on the main/clean table this function's other
       callers use and so can't live in the fixed priority list above.

    Column matching is **case-insensitive**, matching Spark's own default
    ``spark.sql.caseSensitive=false`` column-reference resolution -- a ``target_config``
    entry whose casing differs from the DataFrame's actual physical column name (e.g.
    ``primary_keys: ["CustomerID"]`` against a physical column ``customerid``) still gets
    front-loaded, using the DataFrame's own actual casing in the output (never the config's).

    Every other column keeps its existing relative order, appended after the front group.
    Returns ``df`` unchanged (no ``select`` at all) when none of the above are present, so
    calling this unconditionally on every flow -- including ones with no keys/clustering/hash
    columns configured at all -- is always safe and free.
    """
    df_columns = df.columns
    column_lookup = {c.lower(): c for c in df_columns}

    front_priority: List[str] = []
    front_priority.extend(target_config.get("primary_keys") or [])
    front_priority.extend(target_config.get("liquid_clustering_columns") or [])
    front_priority.extend(_FRAMEWORK_GENERATED_FRONT_COLUMNS)
    front_priority.extend(extra_priority_columns or [])

    seen = set()
    front_columns: List[str] = []
    for column in front_priority:
        actual_column = column_lookup.get(column.lower())
        if actual_column is not None and actual_column not in seen:
            front_columns.append(actual_column)
            seen.add(actual_column)

    if not front_columns:
        return df

    remaining_columns = [c for c in df_columns if c not in seen]
    return df.select(*front_columns, *remaining_columns)
