"""Unit tests for ingestion/column_normalization.py.

``TestNormalizeColumnName`` is pure Python, no Spark session needed. ``TestNormalizeColumnNames``
uses the live ``spark`` fixture (Databricks Connect) since it exercises real DataFrame
transforms -- no table is created, matching ``test_column_ordering.py``'s same precedent for
DataFrame-transform-only tests living in ``tests/unit/`` rather than ``tests/integration/``.
"""

import pytest

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.ingestion.column_normalization import (
    normalize_column_name,
    normalize_column_names,
)


class TestNormalizeColumnName:
    def test_trims_leading_and_trailing_whitespace(self):
        assert normalize_column_name("  customer_id  ") == "customer_id"

    def test_lowercases(self):
        assert normalize_column_name("CustomerID") == "customerid"

    def test_replaces_embedded_whitespace_with_underscore(self):
        assert normalize_column_name("Customer ID") == "customer_id"

    def test_replaces_special_characters_with_underscore(self):
        assert normalize_column_name("Customer#ID!") == "customer_id"

    def test_collapses_repeated_underscores(self):
        assert normalize_column_name("Customer   ID") == "customer_id"

    def test_strips_leading_and_trailing_underscores_produced_by_special_chars(self):
        assert normalize_column_name("#CustomerID#") == "customerid"

    def test_already_normalized_name_is_unchanged(self):
        assert normalize_column_name("customer_id") == "customer_id"

    def test_mixed_whitespace_and_punctuation(self):
        assert normalize_column_name("  Full Name (Legal) ") == "full_name_legal"

    def test_digits_are_preserved(self):
        assert normalize_column_name("Address Line 2") == "address_line_2"

    def test_empty_string_falls_back_to_underscore(self):
        assert normalize_column_name("") == "_"

    def test_all_special_characters_falls_back_to_underscore(self):
        assert normalize_column_name("###") == "_"

    def test_leading_digit_is_preserved_not_special_cased(self):
        assert normalize_column_name("2ndAddress") == "2ndaddress"

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Country_Code", "country_code"),
            ("SignupDate", "signupdate"),
            ("Order-ID", "order_id"),
            ("Order.ID", "order_id"),
            ("Order/ID", "order_id"),
            ("Order\tID", "order_id"),
            ("Order\nID", "order_id"),
        ],
    )
    def test_various_special_characters(self, raw, expected):
        assert normalize_column_name(raw) == expected


class TestNormalizeColumnNames:
    """v1.4.0: source_config.column_normalization is the only switch.

    Every case below used to be written against the legacy ``normalize_column_names`` boolean.
    That key is removed -- and, crucially, a spec still carrying it now normalizes NOTHING (the
    onboarding validator rejects it before a pipeline ever runs, but this function's own
    behaviour is what these tests pin). ``test_removed_legacy_boolean_is_inert`` exists for
    exactly that: it is the regression guard against someone re-introducing a fallback read of
    the old key "for compatibility", which would silently resurrect two ways to say one thing.
    """

    def test_disabled_by_default_leaves_columns_unchanged(self, spark):
        df = spark.createDataFrame([("C001", "Alice")], ["Customer ID", "Full Name"])
        result = normalize_column_names(df, {})
        assert result.columns == ["Customer ID", "Full Name"]

    def test_explicitly_false_leaves_columns_unchanged(self, spark):
        df = spark.createDataFrame([("C001", "Alice")], ["Customer ID", "Full Name"])
        result = normalize_column_names(df, {"column_normalization": {"enabled": False}})
        assert result.columns == ["Customer ID", "Full Name"]

    def test_object_without_enabled_is_off(self, spark):
        """Before v1.4.0 an object omitting `enabled` deferred to the legacy boolean, so it could
        not be read as "off". With the boolean gone there is nothing to defer to, and absent
        means off -- which is what `.get("enabled", False)` says."""
        df = spark.createDataFrame([("C001", "Alice")], ["Customer ID", "Full Name"])
        result = normalize_column_names(df, {"column_normalization": {"case": "upper"}})
        assert result.columns == ["Customer ID", "Full Name"]

    def test_removed_legacy_boolean_is_inert(self, spark):
        """The removed key must do NOTHING -- not partially, not as a fallback."""
        df = spark.createDataFrame([("C001", "Alice")], ["Customer ID", "Full Name"])
        result = normalize_column_names(df, {"normalize_column_names": True})
        assert result.columns == ["Customer ID", "Full Name"]

    def test_enabled_normalizes_every_column(self, spark):
        df = spark.createDataFrame([("C001", "Alice")], ["Customer ID", "Full Name"])
        result = normalize_column_names(df, {"column_normalization": {"enabled": True}})
        assert result.columns == ["customer_id", "full_name"]

    def test_case_preserve_keeps_source_casing(self, spark):
        df = spark.createDataFrame([("C001",)], ["Customer ID"])
        result = normalize_column_names(df, {"column_normalization": {"enabled": True, "case": "preserve"}})
        assert result.columns == ["Customer_ID"]

    def test_case_upper(self, spark):
        df = spark.createDataFrame([("C001",)], ["Customer ID"])
        result = normalize_column_names(df, {"column_normalization": {"enabled": True, "case": "upper"}})
        assert result.columns == ["CUSTOMER_ID"]

    def test_data_is_preserved_after_rename(self, spark):
        df = spark.createDataFrame([("C001", "Alice")], ["Customer ID", "Full Name"])
        result = normalize_column_names(df, {"column_normalization": {"enabled": True}})
        row = result.collect()[0]
        assert row["customer_id"] == "C001"
        assert row["full_name"] == "Alice"

    def test_already_normalized_columns_are_untouched(self, spark):
        df = spark.createDataFrame([("C001", "Alice")], ["customer_id", "full_name"])
        result = normalize_column_names(df, {"column_normalization": {"enabled": True}})
        assert result.columns == ["customer_id", "full_name"]

    def test_colliding_normalized_names_raise(self, spark):
        df = spark.createDataFrame([("C001", "C002")], ["Customer ID", "customer_id"])
        with pytest.raises(FrameworkConfigError, match="duplicate column names"):
            normalize_column_names(df, {"column_normalization": {"enabled": True}})
