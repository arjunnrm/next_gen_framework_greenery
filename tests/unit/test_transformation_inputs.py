"""Unit tests for transformation/inputs.py::mark_streaming_references -- pure Python, no Spark needed."""

from flowx.lakeflow_framework.transformation.inputs import mark_streaming_references


def _input(input_name, is_streaming=True, **overrides):
    config = {"input_name": input_name, "table": f"poc.bronze.{input_name}", "is_streaming": is_streaming}
    config.update(overrides)
    return config


def test_from_reference_to_streaming_input_gets_stream_marker():
    resolved = mark_streaming_references("SELECT * FROM bronze_telemetry", [_input("bronze_telemetry")])
    assert resolved == "SELECT * FROM STREAM bronze_telemetry"


def test_join_reference_to_streaming_input_gets_stream_marker():
    sql = "SELECT * FROM dim_accounts JOIN raw_transactions ON dim_accounts.account_id = raw_transactions.account_id"
    resolved = mark_streaming_references(
        sql, [_input("dim_accounts", is_streaming=False), _input("raw_transactions")]
    )
    assert resolved == (
        "SELECT * FROM dim_accounts JOIN STREAM raw_transactions "
        "ON dim_accounts.account_id = raw_transactions.account_id"
    )


def test_batch_input_is_never_marked():
    resolved = mark_streaming_references("SELECT * FROM dim_accounts", [_input("dim_accounts", is_streaming=False)])
    assert resolved == "SELECT * FROM dim_accounts"


def test_input_not_marked_when_is_streaming_missing():
    config = {"input_name": "dim_accounts", "table": "poc.bronze.dim_accounts"}
    assert mark_streaming_references("SELECT * FROM dim_accounts", [config]) == "SELECT * FROM dim_accounts"


def test_multiple_streaming_inputs_all_marked():
    sql = "SELECT * FROM raw_transactions JOIN raw_orders ON raw_transactions.id = raw_orders.txn_id"
    resolved = mark_streaming_references(sql, [_input("raw_transactions"), _input("raw_orders")])
    assert resolved == (
        "SELECT * FROM STREAM raw_transactions JOIN STREAM raw_orders ON raw_transactions.id = raw_orders.txn_id"
    )


def test_name_that_is_a_prefix_of_another_identifier_is_not_falsely_matched():
    sql = "SELECT * FROM dim_customer_region_scd1_src_extra"
    resolved = mark_streaming_references(sql, [_input("dim_customer_region_scd1_src")])
    assert resolved == sql  # word boundary must prevent matching the shorter name as a substring


def test_from_keyword_matched_case_insensitively():
    resolved = mark_streaming_references("select * from bronze_telemetry", [_input("bronze_telemetry")])
    assert resolved == "select * from STREAM bronze_telemetry"


def test_no_source_inputs_leaves_sql_unchanged():
    sql = "SELECT * FROM t"
    assert mark_streaming_references(sql, []) == sql


def test_self_referenced_streaming_input_marked_at_every_occurrence():
    sql = "SELECT a.* FROM raw_transactions a JOIN raw_transactions b ON a.id = b.prev_id"
    resolved = mark_streaming_references(sql, [_input("raw_transactions")])
    assert resolved == "SELECT a.* FROM STREAM raw_transactions a JOIN STREAM raw_transactions b ON a.id = b.prev_id"
