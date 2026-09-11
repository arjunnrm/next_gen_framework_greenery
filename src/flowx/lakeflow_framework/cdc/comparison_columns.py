"""Resolve which columns count as a "change" for CDC comparison purposes.

Single source of truth shared by SCD2's native ``track_history_column_list`` and the
``__framework_hash_value`` computation (``cdc/hashing.py``) -- both need exactly the same
answer to "which columns matter for change detection", so it's resolved once, here, rather
than duplicated.

**``columns_to_exclude`` has TWO jobs, and this module implements only the first.** It narrows
the comparison basis resolved here (change detection + ``__framework_hash_value``), *and* it is
passed to ``dlt.apply_changes``'s ``except_column_list`` by ``cdc/scd.py`` for SCD1/SCD2/SCD3,
which drops those columns from the target table's schema entirely. The two compose correctly:
the drop happens at apply time, after this resolution, so narrowing the hash basis and dropping
the column agree rather than fighting.

That overloading is deliberate and load-bearing: ``except_column_list`` is the framework's
**only** mechanism for "don't store this column at all" on a CDC target. Nothing else in the
spec can do it -- ``data_standardization_sql`` is strictly add/replace (every expression must
end in ``AS <name>``), ``column_normalization`` and ``schema_config`` only rename/cast/comment,
and ``columns_to_check`` scopes comparison. Removing the passthrough would delete a capability
with no replacement *and* silently re-add columns to every already-materialized SCD target.

A prior docstring here asserted the opposite -- that comparison and storage had been decoupled,
so an excluded column stayed in the target. That was an aspiration written down but never
implemented: there is no attribute delta, release note or migration for such a change, and a
live Bronze table inspection (2026-09-11, ``bt_digital_poc.bronze.physical_device``) confirms
the excluded columns are genuinely ABSENT from the target. The prose was corrected rather than
the code, because the code is the contract that shipped. If the overloading is ever to be undone,
the remedy is to ADD a storage-exclusion attribute and migrate onto it under this repo's
reject-never-ignore removal protocol -- not to silently narrow this one.
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
        Removed from whichever base set applies above. **Also** drops the column from the
        target table's schema -- ``cdc/scd.py`` passes the same list to ``apply_changes``'s
        ``except_column_list``, which this function is deliberately unaware of (it resolves
        comparison; scd.py applies storage). To narrow comparison WITHOUT dropping the column
        from the target, use ``columns_to_check`` to name the columns that should be compared
        and leave this unset.

    Returns
    -------
    list[str]
        The resolved, deduplicated comparison-column list, in a stable (sorted) order so
        hash computation is deterministic regardless of source column ordering.
    """
    excluded = set(columns_to_exclude or []) | set(primary_keys or []) | FRAMEWORK_TECHNICAL_COLUMNS
    base = list(columns_to_check) if columns_to_check else list(all_columns)
    return sorted({c for c in base if c not in excluded})
