"""Resolve which columns count as a "change" for CDC comparison purposes.

Single source of truth shared by SCD2's native ``track_history_column_list`` and the
``__framework_hash_value`` computation (``cdc/hashing.py``) -- both need exactly the same
answer to "which columns matter for change detection", so it's resolved once, here, rather
than duplicated.

**Comparison vs. storage are fully decoupled in v2**: ``columns_to_exclude`` no longer
reaches ``dlt.apply_changes``'s ``except_column_list`` for user columns (a real behavior
change from v1) -- it only affects this resolution. Excluding a column from comparison
never drops it from the target table.
"""

from typing import Iterable, List, Optional, Sequence

# Framework-managed technical columns never participate in change comparison -- they
# change on every run by construction (a timestamp, a hash of the very columns being
# compared) and would make every row look "changed" every time if included.
FRAMEWORK_TECHNICAL_COLUMNS = {
    "__framework_ingestion_timestamp_utc",
    "__framework_hash_key",
    "__framework_hash_value",
    # Removed as a generated column in v1.4.0, retained here as an exclusion: a target table
    # materialized before the upgrade still physically carries it, and a comparison set that
    # picked it up would compare a stale payload-wide hash against nothing.
    "__framework_surrogate_key",
}


def resolve_comparison_columns(
    all_columns: Sequence[str],
    primary_keys: Optional[Iterable[str]],
    columns_to_check: Optional[Iterable[str]],
    columns_to_exclude: Optional[Iterable[str]],
) -> List[str]:
    """Resolve the final list of columns that count as a "change" for this CDC flow.

    Parameters
    ----------
    all_columns:
        Every column present on the flow's staged DataFrame (``df.columns``, resolved at
        execution time -- comparison columns are never hardcoded against a design-time
        schema).
    primary_keys:
        Excluded from comparison -- a key column defines row identity, not "did the row
        change".
    columns_to_check:
        When non-empty, comparison is scoped to exactly these columns (still filtered by
        ``columns_to_exclude`` and ``primary_keys``, in case of an author mistake). When
        empty/``None``, every applicable column on ``all_columns`` is compared.
    columns_to_exclude:
        Comparison-only exclusion (v2 semantics) -- removed from whichever base set applies
        above. Never affects what's stored on the target table.

    Returns
    -------
    list[str]
        The resolved, deduplicated comparison-column list, in a stable (sorted) order so
        hash computation is deterministic regardless of source column ordering.
    """
    excluded = set(columns_to_exclude or []) | set(primary_keys or []) | FRAMEWORK_TECHNICAL_COLUMNS
    base = list(columns_to_check) if columns_to_check else list(all_columns)
    return sorted({c for c in base if c not in excluded})
