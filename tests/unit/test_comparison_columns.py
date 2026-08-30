"""Unit tests for cdc/comparison_columns.py -- pure Python, no Spark needed.

Explicit user-requested test matrix: empty/populated/overlapping/invalid columns_to_check
and columns_to_exclude combinations.
"""

from NextGen_Metadata_Framework.lakeflow_framework.cdc.comparison_columns import (
    FRAMEWORK_TECHNICAL_COLUMNS,
    resolve_comparison_columns,
)

ALL_COLUMNS = ["customer_id", "name", "email", "tier", "__framework_ingestion_timestamp_utc", "__framework_hash_key"]


def test_empty_columns_to_check_compares_all_applicable_columns():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=None)
    assert result == sorted(["name", "email", "tier"])


def test_populated_columns_to_check_scopes_to_exactly_those():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=["name", "tier"], columns_to_exclude=None)
    assert result == sorted(["name", "tier"])


def test_columns_to_exclude_removes_from_the_all_columns_base():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=["email"])
    assert result == sorted(["name", "tier"])


def test_columns_to_exclude_also_removes_from_a_populated_columns_to_check():
    """An author mistake -- naming a column in both columns_to_check and columns_to_exclude
    -- must not silently include it; exclusion always wins."""
    result = resolve_comparison_columns(
        ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=["name", "email"], columns_to_exclude=["email"]
    )
    assert result == ["name"]


def test_primary_keys_never_appear_in_comparison_columns():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id", "name"], columns_to_check=None, columns_to_exclude=None)
    assert "customer_id" not in result
    assert "name" not in result


def test_framework_technical_columns_never_appear_in_comparison_columns():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=None)
    assert not (set(result) & FRAMEWORK_TECHNICAL_COLUMNS)


def test_framework_technical_columns_excluded_even_if_explicitly_requested_in_columns_to_check():
    result = resolve_comparison_columns(
        ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=["__framework_hash_key", "name"], columns_to_exclude=None
    )
    assert result == ["name"]


def test_no_primary_keys_still_excludes_technical_columns_only():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=None, columns_to_check=None, columns_to_exclude=None)
    assert result == sorted(["customer_id", "name", "email", "tier"])


def test_result_is_sorted_and_deduplicated():
    result = resolve_comparison_columns(
        ["b", "a", "a"], primary_keys=None, columns_to_check=["a", "a", "b"], columns_to_exclude=None
    )
    assert result == ["a", "b"]


def test_all_columns_excluded_returns_empty_list():
    result = resolve_comparison_columns(["customer_id"], primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=None)
    assert result == []
