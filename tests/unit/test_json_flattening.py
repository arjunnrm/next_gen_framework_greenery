"""Unit tests for ingestion/json_flattening.py -- explode_columns runtime application.

Uses the live `spark` fixture to build real nested DataFrames (struct/array columns can't be
constructed without Spark).
"""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.json_flattening import apply_explode_columns


def _nested_df(spark):
    return spark.createDataFrame(
        [
            (
                "C001",
                {"city": "NYC", "zip": "10001"},
                [{"sku": "A1", "qty": 2}, {"sku": "A2", "qty": 1}],
            )
        ],
        "customer_id STRING, address STRUCT<city:STRING, zip:STRING>, line_items ARRAY<STRUCT<sku:STRING, qty:INT>>",
    )


def test_none_explode_columns_is_schema_preserving_pass_through_by_default(spark):
    """Omitting explode_columns must NOT implicitly flatten/explode anything -- the
    DataFrame is returned unchanged unless auto_flatten_all is explicitly opted into."""
    df = _nested_df(spark)
    result = apply_explode_columns(df, None)
    assert result.columns == df.columns
    assert result.collect() == df.collect()


def test_empty_list_explode_columns_is_schema_preserving_pass_through_by_default(spark):
    df = _nested_df(spark)
    result = apply_explode_columns(df, [])
    assert result.columns == df.columns
    assert result.collect() == df.collect()


def test_auto_flatten_all_flattens_struct_and_explodes_array_when_explode_columns_omitted(spark):
    """"Flatten everything" is fully recursive -- the array column is exploded, and its
    resulting struct element is then flattened too, in a later pass over the same
    (now-changed) schema. Only happens when explicitly opted into via auto_flatten_all."""
    result = apply_explode_columns(_nested_df(spark), None, auto_flatten_all=True)
    assert set(result.columns) == {"customer_id", "address_city", "address_zip", "line_items_sku", "line_items_qty"}
    rows = result.collect()
    assert len(rows) == 2, "the array column must be exploded into one row per element"
    skus = {row["line_items_sku"] for row in rows}
    assert skus == {"A1", "A2"}


def test_auto_flatten_all_recurses_into_flattened_array_of_structs(spark):
    result = apply_explode_columns(_nested_df(spark), [], auto_flatten_all=True)
    assert "line_items_sku" in result.columns
    assert "line_items_qty" in result.columns
    assert "line_items" not in result.columns
    skus = {row["line_items_sku"] for row in result.collect()}
    assert skus == {"A1", "A2"}


def test_auto_flatten_all_is_ignored_once_explode_columns_is_populated(spark):
    """auto_flatten_all only governs the empty/absent explode_columns case -- a populated
    list must still scope treatment to exactly the named columns, regardless of the flag."""
    result = apply_explode_columns(_nested_df(spark), ["address"], auto_flatten_all=True)
    assert "address_city" in result.columns
    assert "address_zip" in result.columns
    assert "line_items" in result.columns, "line_items was not named -- must stay untouched even with auto_flatten_all=True"


def test_populated_explode_columns_scopes_to_only_the_named_column(spark):
    result = apply_explode_columns(_nested_df(spark), ["address"])
    assert "address_city" in result.columns
    assert "address_zip" in result.columns
    assert "line_items" in result.columns, "line_items was not named -- must stay untouched as a raw array column"


def test_populated_explode_columns_on_array_of_struct_fully_denests_in_one_entry(spark):
    result = apply_explode_columns(_nested_df(spark), ["line_items"])
    assert "line_items_sku" in result.columns
    assert "line_items_qty" in result.columns
    assert "address" in result.columns, "address was not named -- must stay untouched as a raw struct column"


def test_unknown_column_name_raises_framework_config_error(spark):
    with pytest.raises(FrameworkConfigError, match="does_not_exist"):
        apply_explode_columns(_nested_df(spark), ["does_not_exist"])


def test_scalar_column_name_raises_framework_config_error(spark):
    with pytest.raises(FrameworkConfigError, match="customer_id"):
        apply_explode_columns(_nested_df(spark), ["customer_id"])


def test_flat_dataframe_with_no_nested_columns_is_unchanged_by_empty_explode(spark):
    flat_df = spark.createDataFrame([("C001", "Alice")], ["customer_id", "name"])
    result = apply_explode_columns(flat_df, None)
    assert set(result.columns) == {"customer_id", "name"}
    assert result.collect() == flat_df.collect()
