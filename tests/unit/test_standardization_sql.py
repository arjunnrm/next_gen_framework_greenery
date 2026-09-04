"""Unit tests for ingestion/standardization_sql.py -- data_standardization_sql runtime application."""

import pytest

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.ingestion.standardization_sql import (
    apply_data_standardization_sql,
)


def test_none_expressions_is_a_no_op(spark):
    df = spark.createDataFrame([("  Alice  ",)], ["name"])
    result = apply_data_standardization_sql(df, None)
    assert result.collect() == df.collect()


def test_empty_expressions_is_a_no_op(spark):
    df = spark.createDataFrame([("  Alice  ",)], ["name"])
    result = apply_data_standardization_sql(df, [])
    assert result.collect() == df.collect()


def test_expression_overwrites_the_named_existing_column(spark):
    df = spark.createDataFrame([("  Alice  ",)], ["name"])
    result = apply_data_standardization_sql(df, ["trim(name) AS name"])
    assert result.columns == ["name"], "must overwrite in place, not add a duplicate column"
    assert result.collect()[0]["name"] == "Alice"


def test_expression_adds_a_new_column_when_alias_does_not_already_exist(spark):
    df = spark.createDataFrame([("US",)], ["country_code"])
    result = apply_data_standardization_sql(df, ["upper(country_code) AS country_code_upper"])
    assert "country_code_upper" in result.columns
    assert "country_code" in result.columns
    assert result.collect()[0]["country_code_upper"] == "US"


def test_multiple_expressions_applied_in_order(spark):
    df = spark.createDataFrame([("  bob  ", "us")], ["name", "country_code"])
    result = apply_data_standardization_sql(df, ["trim(name) AS name", "upper(country_code) AS country_code"])
    row = result.collect()[0]
    assert row["name"] == "bob"
    assert row["country_code"] == "US"


def test_expression_without_as_clause_raises_on_a_real_dataframe(spark):
    """A malformed-grammar error (missing 'AS <column_name>') is caught by this module's own
    regex check before Spark is ever involved -- unlike a semantic error (e.g. an unresolved
    column reference), which Spark Connect only surfaces once the DataFrame is actually
    executed (a real DLT pipeline run makes that failure, and its message, plainly visible
    at that point -- see the live pipeline verification for this feature)."""
    df = spark.createDataFrame([("x",)], ["name"])
    with pytest.raises(FrameworkConfigError, match="AS <column_name>"):
        apply_data_standardization_sql(df, ["trim(name)"])
