"""Unit tests for reconciliation/matcher.py -- hash-first matching, duplicate-key safety.

Uses the live `spark` fixture (Databricks Connect) since these are DataFrame transforms, not
persisted-table assertions -- no table is created, so this stays fast and side-effect-free
(mirrors test_hashing.py/test_comparison_columns.py's own convention for this repo).
"""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.cdc.hashing import (
    HASH_KEY_COLUMN,
    HASH_VALUE_COLUMN,
    compute_hash_columns,
)
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.matcher import (
    MISMATCH_TYPE_COLUMN,
    MISMATCH_TYPE_MISSING_IN_SOURCE,
    MISMATCH_TYPE_MISSING_IN_TARGET,
    MISMATCH_TYPE_VALUE_DRIFT,
    match_reconciliation_target,
    prepare_dataset_for_matching,
    target_prefixed_column,
)


# ---------------------------------------------------------------------------
# prepare_dataset_for_matching
# ---------------------------------------------------------------------------


def test_prepare_computes_hash_columns_when_not_precomputed(spark):
    df = spark.createDataFrame([("C001", "GOLD")], ["id", "tier"])
    result = prepare_dataset_for_matching(df, ["id"], ["tier"], hash_precomputed=False)
    assert HASH_KEY_COLUMN in result.columns
    assert HASH_VALUE_COLUMN in result.columns


def test_prepare_trusts_existing_hash_columns_when_precomputed(spark):
    df = spark.createDataFrame([("C001", "GOLD")], ["id", "tier"])
    df = compute_hash_columns(df, ["id"], ["tier"])
    result = prepare_dataset_for_matching(df, ["id"], ["tier"], hash_precomputed=True)
    # Unchanged -- same hash values as the pre-existing computation, not recomputed.
    assert result.collect()[0][HASH_KEY_COLUMN] == df.collect()[0][HASH_KEY_COLUMN]


def test_prepare_hash_precomputed_true_but_missing_columns_raises(spark):
    df = spark.createDataFrame([("C001", "GOLD")], ["id", "tier"])
    with pytest.raises(FrameworkConfigError, match="hash_precomputed=True"):
        prepare_dataset_for_matching(df, ["id"], ["tier"], hash_precomputed=True)


def test_prepare_missing_match_key_column_raises(spark):
    df = spark.createDataFrame([("GOLD",)], ["tier"])
    with pytest.raises(FrameworkConfigError, match="match_keys"):
        prepare_dataset_for_matching(df, ["id"], [], hash_precomputed=False)


def test_prepare_empty_compare_columns_produces_null_hash_value(spark):
    """DDL semantics: null/empty compare_columns means key-presence-only matching --
    __framework_hash_value must be a constant NULL, not 'every column' (resolve_comparison_columns'
    own default for columns_to_check=None, which is the wrong meaning here)."""
    df = spark.createDataFrame([("C001", "GOLD")], ["id", "tier"])
    result = prepare_dataset_for_matching(df, ["id"], None, hash_precomputed=False)
    assert result.collect()[0][HASH_VALUE_COLUMN] is None


def test_prepare_no_longer_accepts_a_surrogate_key_flag():
    """v1.4.0 removed the trailing generate_surrogate_key parameter. A caller still passing it
    -- positionally or by keyword -- must fail loudly rather than have it swallowed by a **kwargs
    or silently bound to `hash_precomputed`."""
    with pytest.raises(TypeError):
        prepare_dataset_for_matching(None, ["id"], [], False, True)


# ---------------------------------------------------------------------------
# match_reconciliation_target
# ---------------------------------------------------------------------------


def _prepared(spark, rows, columns, match_keys, compare_columns, hash_precomputed=False):
    # An empty `rows` list can't be schema-inferred from bare column names alone (Spark
    # Connect raises CANNOT_INFER_EMPTY_SCHEMA) -- fall back to an explicit DDL schema string
    # for that case; every test here only ever needs an (int, string) shape.
    schema = "id INT, val STRING" if not rows else columns
    df = spark.createDataFrame(rows, schema)
    return prepare_dataset_for_matching(df, match_keys, compare_columns, hash_precomputed)


def test_match_classifies_matched_drifted_missing_in_target_and_missing_in_source(spark):
    source = _prepared(
        spark,
        [(1, "a"), (2, "b"), (3, "c")],
        ["id", "val"],
        ["id"],
        ["val"],
    )
    target = _prepared(
        spark,
        [(1, "a"), (2, "DRIFTED"), (4, "d")],
        ["id", "val"],
        ["id"],
        ["val"],
    )
    result = match_reconciliation_target(source, target, ["id"], ["val"], source_hash_precomputed=False)

    assert result.matched_count == 1  # id=1
    assert result.value_drift_count == 1  # id=2
    assert result.missing_in_target_count == 2  # id=2 (drift) + id=3 (entirely missing)
    assert result.missing_in_source_count == 1  # id=4

    missing_ids = {row["id"] for row in result.missing_in_target_df.collect()}
    assert missing_ids == {2, 3}

    mismatch_types = {row[MISMATCH_TYPE_COLUMN] for row in result.mismatch_detail_df.collect()}
    assert mismatch_types == {MISMATCH_TYPE_VALUE_DRIFT, MISMATCH_TYPE_MISSING_IN_TARGET, MISMATCH_TYPE_MISSING_IN_SOURCE}


def test_key_presence_only_matching_ignores_value_drift(spark):
    """compare_columns=[] (or None): any co-present key counts as matched, regardless of
    other column values -- differs from resolve_comparison_columns' own "compare everything"
    default for an unset columns_to_check, which is the wrong meaning for reconciliation."""
    source = _prepared(spark, [(1, "a")], ["id", "val"], ["id"], [])
    target = _prepared(spark, [(1, "COMPLETELY_DIFFERENT")], ["id", "val"], ["id"], [])
    result = match_reconciliation_target(source, target, ["id"], [], source_hash_precomputed=False)

    assert result.matched_count == 1
    assert result.value_drift_count == 0
    assert result.missing_in_target_count == 0


def test_duplicate_target_keys_self_healing_correction_counts_as_matched(spark):
    """Regression guard for the exact bug the pre-redesign matcher fixed: a target/CDC bus
    holding both a stale drifted row and a corrected row for the same key must classify that
    key as matched (not also as drifted/re-appended) -- otherwise a "correction" gets
    re-appended forever and the self-healing append never converges."""
    source = _prepared(spark, [(1, "NEW")], ["id", "val"], ["id"], ["val"])
    target = _prepared(
        spark,
        [(1, "OLD"), (1, "NEW")],  # stale drifted row + the already-applied correction
        ["id", "val"],
        ["id"],
        ["val"],
    )
    result = match_reconciliation_target(source, target, ["id"], ["val"], source_hash_precomputed=False)

    assert result.matched_count == 1
    assert result.value_drift_count == 0
    assert result.missing_in_target_count == 0
    assert result.mismatch_detail_df.count() == 0


def test_duplicate_source_keys_are_all_preserved_in_append_set(spark):
    """Deduplication is a classification-decision safeguard only -- it must never silently
    drop a genuine duplicate *source* row from the records actually appended."""
    source = _prepared(spark, [(1, "a"), (1, "a")], ["id", "val"], ["id"], ["val"])
    target = _prepared(spark, [], ["id", "val"], ["id"], ["val"])
    result = match_reconciliation_target(source, target, ["id"], ["val"], source_hash_precomputed=False)

    assert result.missing_in_target_df.count() == 2


def test_source_hash_precomputed_false_strips_hash_value_but_keeps_hash_key(spark):
    source = _prepared(spark, [(1, "a")], ["id", "val"], ["id"], ["val"], hash_precomputed=False)
    target = _prepared(spark, [], ["id", "val"], ["id"], ["val"], hash_precomputed=False)
    result = match_reconciliation_target(source, target, ["id"], ["val"], source_hash_precomputed=False)

    assert HASH_KEY_COLUMN in result.missing_in_target_df.columns
    assert HASH_VALUE_COLUMN not in result.missing_in_target_df.columns


def test_source_hash_precomputed_true_keeps_hash_value(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "val"])
    df = compute_hash_columns(df, ["id"], ["val"])
    source = prepare_dataset_for_matching(df, ["id"], ["val"], hash_precomputed=True)
    target = _prepared(spark, [], ["id", "val"], ["id"], ["val"], hash_precomputed=False)
    result = match_reconciliation_target(source, target, ["id"], ["val"], source_hash_precomputed=True)

    assert HASH_KEY_COLUMN in result.missing_in_target_df.columns
    assert HASH_VALUE_COLUMN in result.missing_in_target_df.columns


def test_mismatch_detail_carries_both_sides_raw_values_for_value_drift(spark):
    source = _prepared(spark, [(1, "ACTIVE")], ["id", "status"], ["id"], ["status"])
    target = _prepared(spark, [(1, "INACTIVE")], ["id", "status"], ["id"], ["status"])
    result = match_reconciliation_target(source, target, ["id"], ["status"], source_hash_precomputed=False)

    row = result.mismatch_detail_df.collect()[0]
    assert row["status"] == "ACTIVE"
    assert row[target_prefixed_column("status")] == "INACTIVE"
    assert row[MISMATCH_TYPE_COLUMN] == MISMATCH_TYPE_VALUE_DRIFT


def test_missing_match_keys_or_compare_columns_raises(spark):
    source = spark.createDataFrame([(1,)], ["id"])
    source = compute_hash_columns(source, ["id"], [])
    target = spark.createDataFrame([(1,)], ["id"])
    target = compute_hash_columns(target, ["id"], [])
    with pytest.raises(FrameworkConfigError):
        match_reconciliation_target(source, target, ["id"], ["missing_col"], source_hash_precomputed=False)


def test_missing_hash_columns_raises(spark):
    source = spark.createDataFrame([(1,)], ["id"])
    target = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(FrameworkConfigError, match="prepare_dataset_for_matching"):
        match_reconciliation_target(source, target, ["id"], [], source_hash_precomputed=False)
