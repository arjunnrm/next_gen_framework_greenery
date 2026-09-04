"""Unit tests for cdc/hashing.py -- __framework_hash_key/__framework_hash_value computation.

Uses the live `spark` fixture (Databricks Connect) since these are DataFrame transforms, not
persisted-table assertions -- no table is created, so this stays fast and side-effect-free.
"""

from flowx.lakeflow_framework.cdc.hashing import (
    HASH_KEY_COLUMN,
    HASH_VALUE_COLUMN,
    compute_hash_columns,
)


def test_hash_key_and_value_columns_are_added(spark):
    df = spark.createDataFrame([("C001", "Alice", "GOLD")], ["customer_id", "name", "tier"])
    result = compute_hash_columns(df, primary_keys=["customer_id"], comparison_columns=["name", "tier"])
    assert HASH_KEY_COLUMN in result.columns
    assert HASH_VALUE_COLUMN in result.columns


def test_hash_key_is_deterministic_for_the_same_primary_key_value(spark):
    df1 = spark.createDataFrame([("C001", "Alice")], ["customer_id", "name"])
    df2 = spark.createDataFrame([("C001", "Bob")], ["customer_id", "name"])
    row1 = compute_hash_columns(df1, ["customer_id"], []).collect()[0]
    row2 = compute_hash_columns(df2, ["customer_id"], []).collect()[0]
    assert row1[HASH_KEY_COLUMN] == row2[HASH_KEY_COLUMN], "same key value must hash identically regardless of other columns"


def test_hash_key_differs_for_different_primary_key_values(spark):
    df = spark.createDataFrame([("C001", "Alice"), ("C002", "Alice")], ["customer_id", "name"])
    rows = compute_hash_columns(df, ["customer_id"], []).collect()
    assert rows[0][HASH_KEY_COLUMN] != rows[1][HASH_KEY_COLUMN]


def test_hash_value_changes_when_a_comparison_column_changes(spark):
    df1 = spark.createDataFrame([("C001", "GOLD")], ["customer_id", "tier"])
    df2 = spark.createDataFrame([("C001", "PLATINUM")], ["customer_id", "tier"])
    row1 = compute_hash_columns(df1, ["customer_id"], ["tier"]).collect()[0]
    row2 = compute_hash_columns(df2, ["customer_id"], ["tier"]).collect()[0]
    assert row1[HASH_VALUE_COLUMN] != row2[HASH_VALUE_COLUMN]


def test_hash_value_stable_when_only_a_non_comparison_column_changes(spark):
    df1 = spark.createDataFrame([("C001", "GOLD", "a@x.com")], ["customer_id", "tier", "email"])
    df2 = spark.createDataFrame([("C001", "GOLD", "b@x.com")], ["customer_id", "tier", "email"])
    row1 = compute_hash_columns(df1, ["customer_id"], ["tier"]).collect()[0]
    row2 = compute_hash_columns(df2, ["customer_id"], ["tier"]).collect()[0]
    assert row1[HASH_VALUE_COLUMN] == row2[HASH_VALUE_COLUMN], "email isn't a comparison column -- must not perturb the hash"


def test_empty_comparison_columns_produces_null_hash_value(spark):
    df = spark.createDataFrame([("C001",)], ["customer_id"])
    row = compute_hash_columns(df, ["customer_id"], []).collect()[0]
    assert row[HASH_VALUE_COLUMN] is None


def test_null_values_do_not_crash_and_are_distinguished_from_present_values(spark):
    df1 = spark.createDataFrame([("C001", None)], "customer_id STRING, tier STRING")
    df2 = spark.createDataFrame([("C001", "GOLD")], "customer_id STRING, tier STRING")
    row1 = compute_hash_columns(df1, ["customer_id"], ["tier"]).collect()[0]
    row2 = compute_hash_columns(df2, ["customer_id"], ["tier"]).collect()[0]
    assert row1[HASH_VALUE_COLUMN] != row2[HASH_VALUE_COLUMN]


def test_multi_column_primary_key_order_matters_for_the_hash(spark):
    """Documents current behavior: primary_keys is hashed in the order given -- callers must
    pass a stable, consistent order (e.g. the validator-preserved config order) for the hash
    to remain comparable across runs/datasets."""
    df = spark.createDataFrame([("C001", "R1")], ["customer_id", "region"])
    row_ab = compute_hash_columns(df, ["customer_id", "region"], []).collect()[0]
    row_ba = compute_hash_columns(df, ["region", "customer_id"], []).collect()[0]
    assert row_ab[HASH_KEY_COLUMN] != row_ba[HASH_KEY_COLUMN]
