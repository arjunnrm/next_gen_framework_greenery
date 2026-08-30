"""Unit tests for storage/column_ordering.py -- moving key/clustering/hash columns to the
front of a target table's schema so Delta's default 32-column data-skipping stats window
(delta.dataSkippingNumIndexedCols) actually covers them.

Uses the live `spark` fixture (Databricks Connect) since these are DataFrame transforms, not
persisted-table assertions -- no table is created, so this stays fast and side-effect-free.
"""

from NextGen_Metadata_Framework.lakeflow_framework.cdc.hashing import HASH_KEY_COLUMN, HASH_VALUE_COLUMN
from NextGen_Metadata_Framework.lakeflow_framework.storage.column_ordering import reorder_columns_for_delta_stats


def test_no_config_and_no_framework_columns_leaves_column_order_unchanged(spark):
    df = spark.createDataFrame([("C001", "Alice", "GOLD")], ["customer_id", "name", "tier"])
    result = reorder_columns_for_delta_stats(df, {})
    assert result.columns == ["customer_id", "name", "tier"]


def test_primary_key_moves_to_front(spark):
    df = spark.createDataFrame([("Alice", "GOLD", "C001")], ["name", "tier", "customer_id"])
    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["customer_id"]})
    assert result.columns == ["customer_id", "name", "tier"]


def test_multi_column_primary_key_preserves_configured_order(spark):
    df = spark.createDataFrame([("R1", "Alice", "C001")], ["region", "name", "customer_id"])
    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["customer_id", "region"]})
    assert result.columns == ["customer_id", "region", "name"]


def test_clustering_columns_follow_primary_keys(spark):
    df = spark.createDataFrame([("Alice", "2026", "C001", "R1")], ["name", "load_year", "customer_id", "region"])
    result = reorder_columns_for_delta_stats(
        df, {"primary_keys": ["customer_id"], "liquid_clustering_columns": ["region"]}
    )
    assert result.columns == ["customer_id", "region", "name", "load_year"]


def test_hash_columns_move_to_front_when_present(spark):
    df = spark.createDataFrame(
        [("Alice", "hashval", "keyval", "C001")],
        ["name", HASH_VALUE_COLUMN, HASH_KEY_COLUMN, "customer_id"],
    )
    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["customer_id"]})
    assert result.columns == ["customer_id", HASH_KEY_COLUMN, HASH_VALUE_COLUMN, "name"]


def test_a_legacy_surrogate_key_column_is_treated_as_an_ordinary_column(spark):
    """__framework_surrogate_key is no longer generated (v1.4.0), but a table materialized
    before the upgrade still physically carries it. It must not error and must not be
    front-loaded -- it keeps its relative position among the non-priority columns."""
    df = spark.createDataFrame(
        [("Alice", "surrkey", "keyval", "C001")],
        ["name", "__framework_surrogate_key", HASH_KEY_COLUMN, "customer_id"],
    )
    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["customer_id"]})
    assert result.columns == ["customer_id", HASH_KEY_COLUMN, "name", "__framework_surrogate_key"]


def test_a_configured_column_absent_from_the_dataframe_is_silently_skipped(spark):
    """A key/clustering column can be configured but not (yet) present on this particular
    flow's DataFrame -- must not KeyError, just fall through to whatever else applies."""
    df = spark.createDataFrame([("Alice", "C001")], ["name", "customer_id"])
    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["customer_id"], "liquid_clustering_columns": ["region"]})
    assert result.columns == ["customer_id", "name"]


def test_overlapping_primary_key_and_clustering_column_is_not_duplicated(spark):
    df = spark.createDataFrame([("Alice", "C001")], ["name", "customer_id"])
    result = reorder_columns_for_delta_stats(
        df, {"primary_keys": ["customer_id"], "liquid_clustering_columns": ["customer_id"]}
    )
    assert result.columns == ["customer_id", "name"]


def test_no_matching_columns_at_all_returns_the_same_dataframe_object(spark):
    """A pure no-op (not even a redundant .select()) when nothing configured is actually
    present -- e.g. an APPEND flow with no primary_keys/liquid_clustering_columns and no
    hash columns generated."""
    df = spark.createDataFrame([("Alice",)], ["name"])
    result = reorder_columns_for_delta_stats(df, {"primary_keys": [], "liquid_clustering_columns": []})
    assert result is df


def test_duplicate_value_within_the_same_config_list_is_not_duplicated(spark):
    """Distinct from test_overlapping_primary_key_and_clustering_column_is_not_duplicated
    above (which covers a duplicate ACROSS two different config lists) -- this covers a
    duplicate value repeated WITHIN one single list, e.g. a typo'd primary_keys entry. Both
    are deduped by the same shared `seen` set (see column_ordering.py), but a future refactor
    that deduped per-source-list instead of globally could silently reintroduce a Spark
    duplicate-column .select() error for this specific case without this test catching it."""
    df = spark.createDataFrame([("Alice", "C001")], ["name", "customer_id"])
    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["customer_id", "customer_id"]})
    assert result.columns == ["customer_id", "name"]


def test_column_matching_is_case_insensitive_like_spark_itself(spark):
    """Spark resolves string column references case-insensitively by default
    (spark.sql.caseSensitive=false) -- target_config casing that differs from the DataFrame's
    actual physical column name must still be front-loaded, using the DataFrame's own actual
    casing in the result (not the config's)."""
    df = spark.createDataFrame([("Alice", "C001")], ["name", "customerid"])
    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["CustomerID"]})
    assert result.columns == ["customerid", "name"]


def test_extra_priority_columns_are_front_loaded_after_the_standard_priority_columns(spark):
    """dq/quarantine.py's quarantine-table closure uses this to front-load its own DQ
    diagnostic columns (__framework_dq_quarantine_flag etc.), which don't exist on the main/clean table
    and so can't live in the fixed priority list."""
    df = spark.createDataFrame(
        [("Alice", "C001", True, "r1")], ["name", "customer_id", "__framework_dq_quarantine_flag", "__framework_dq_failed_rule_ids"]
    )
    result = reorder_columns_for_delta_stats(
        df, {"primary_keys": ["customer_id"]}, extra_priority_columns=["__framework_dq_quarantine_flag", "__framework_dq_failed_rule_ids"]
    )
    assert result.columns == ["customer_id", "__framework_dq_quarantine_flag", "__framework_dq_failed_rule_ids", "name"]


def test_extra_priority_columns_absent_from_the_dataframe_are_silently_skipped(spark):
    df = spark.createDataFrame([("Alice", "C001")], ["name", "customer_id"])
    result = reorder_columns_for_delta_stats(
        df, {"primary_keys": ["customer_id"]}, extra_priority_columns=["__framework_quarantine_validated_at"]
    )
    assert result.columns == ["customer_id", "name"]


def test_wide_table_places_key_and_hash_columns_within_the_first_32(spark):
    """The literal scenario this exists for: a target table with more business columns than
    Delta's default dataSkippingNumIndexedCols (32) still gets its primary key and hash
    columns positioned inside that window, even though withColumn naturally appended them
    last."""
    business_columns = [f"col_{i}" for i in range(40)]
    row = tuple(str(i) for i in range(40)) + ("C001", "keyval", "hashval")
    columns = business_columns + ["customer_id", HASH_KEY_COLUMN, HASH_VALUE_COLUMN]
    df = spark.createDataFrame([row], columns)

    result = reorder_columns_for_delta_stats(df, {"primary_keys": ["customer_id"]})

    first_32 = result.columns[:32]
    assert "customer_id" in first_32
    assert HASH_KEY_COLUMN in first_32
    assert HASH_VALUE_COLUMN in first_32
