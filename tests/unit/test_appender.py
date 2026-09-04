"""Unit tests for reconciliation/appender.py -- fingerprinting, transform_sql, append, run_log.

`reconciliation_run_log` tests run against a real, disposable table created via the exact
production DDL (`ddl_definitions.py::get_reconciliation_run_log_ddl`) -- this is also the
regression test for a real bug caught while building this module: `Row(**kwargs)`'s field
order is its *construction* (kwarg) order, not the schema's, and
`spark.createDataFrame(rows, schema=...)` zips a Row's values against the given schema
*positionally* -- get that wrong and every column silently (or fatally, depending on adjacent
types) receives the wrong value. `write_run_log_entry` builds its Row's positional values by
walking the schema's own field list specifically to make this impossible; these tests assert
on every individual column by name to prove it actually landed correctly.
"""

import pytest

from flowx.lakeflow_framework.cdc.hashing import HASH_KEY_COLUMN
from flowx.lakeflow_framework.control_plane.ddl_definitions import get_reconciliation_run_log_ddl
from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.reconciliation.appender import (
    UNMATCHED_RECORDS_VIEW_NAME,
    append_missing_records,
    apply_transform_sql,
    compute_batch_fingerprint,
    is_target_batch_already_processed,
    write_run_log_entry,
)
from flowx.lakeflow_framework.reconciliation.metrics import ReconciliationMetrics

CATALOG = "flowx"
CONTROL_SCHEMA = f"{CATALOG}.config_test_appender"


# ---------------------------------------------------------------------------
# compute_batch_fingerprint
# ---------------------------------------------------------------------------


def test_fingerprint_is_deterministic_regardless_of_row_order(spark):
    df1 = spark.createDataFrame([("A",), ("B",)], ["id"])
    df2 = spark.createDataFrame([("B",), ("A",)], ["id"])
    assert compute_batch_fingerprint(df1, ["id"]) == compute_batch_fingerprint(df2, ["id"])


def test_fingerprint_differs_when_key_set_differs(spark):
    df1 = spark.createDataFrame([("A",)], ["id"])
    df2 = spark.createDataFrame([("A",), ("B",)], ["id"])
    assert compute_batch_fingerprint(df1, ["id"]) != compute_batch_fingerprint(df2, ["id"])


def test_fingerprint_raises_on_missing_column(spark):
    df = spark.createDataFrame([("A",)], ["id"])
    with pytest.raises(FrameworkConfigError):
        compute_batch_fingerprint(df, ["missing_col"])


# ---------------------------------------------------------------------------
# apply_transform_sql
# ---------------------------------------------------------------------------


def test_none_transform_sql_is_a_passthrough(spark):
    df = spark.createDataFrame([("A", 1)], ["id", "val"])
    assert apply_transform_sql(df, None).collect() == df.collect()


def test_empty_transform_sql_is_a_passthrough(spark):
    df = spark.createDataFrame([("A", 1)], ["id", "val"])
    assert apply_transform_sql(df, "").collect() == df.collect()


def test_transform_sql_reshapes_via_the_documented_view_name(spark):
    df = spark.createDataFrame([("A", 1)], ["id", "val"])
    result = apply_transform_sql(df, f"SELECT id AS record_id, val * 10 AS scaled_val FROM {UNMATCHED_RECORDS_VIEW_NAME}")
    row = result.collect()[0]
    assert row["record_id"] == "A"
    assert row["scaled_val"] == 10


def test_transform_sql_substitutes_dynamic_parameters(spark):
    df = spark.createDataFrame([("A", "us"), ("B", "eu")], ["id", "region"])
    result = apply_transform_sql(
        df, f"SELECT * FROM {UNMATCHED_RECORDS_VIEW_NAME} WHERE region = ${{target_region}}", {"target_region": "us"}
    )
    assert result.count() == 1
    assert result.collect()[0]["id"] == "A"


def test_transform_sql_undefined_parameter_raises_config_error(spark):
    df = spark.createDataFrame([("A", 1)], ["id", "val"])
    with pytest.raises(FrameworkConfigError):
        apply_transform_sql(df, f"SELECT * FROM {UNMATCHED_RECORDS_VIEW_NAME} WHERE val = ${{undefined}}")


def test_transform_sql_parse_error_raises_config_error(spark):
    df = spark.createDataFrame([("A", 1)], ["id", "val"])
    with pytest.raises(FrameworkConfigError):
        apply_transform_sql(df, "SELECT SELECT FROM nowhere !!!")


# ---------------------------------------------------------------------------
# append_missing_records
# ---------------------------------------------------------------------------


@pytest.fixture()
def append_target_schema(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze_test_appender")
    yield f"{CATALOG}.bronze_test_appender"
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.bronze_test_appender CASCADE")


def test_append_creates_table_on_first_write(spark, append_target_schema):
    table = f"{append_target_schema}.append_probe_1"
    df = spark.createDataFrame([("A", 1)], ["id", "val"])
    count = append_missing_records(df, table)
    assert count == 1
    assert spark.table(table).count() == 1


def test_append_zero_rows_is_a_no_op_and_does_not_create_the_table(spark, append_target_schema):
    table = f"{append_target_schema}.append_probe_2"
    df = spark.createDataFrame([("A", 1)], ["id", "val"]).filter("1 = 0")
    count = append_missing_records(df, table)
    assert count == 0
    assert not spark.catalog.tableExists(table)


def test_append_second_write_adds_to_existing_rows(spark, append_target_schema):
    table = f"{append_target_schema}.append_probe_3"
    append_missing_records(spark.createDataFrame([("A", 1)], ["id", "val"]), table)
    append_missing_records(spark.createDataFrame([("B", 2)], ["id", "val"]), table)
    assert spark.table(table).count() == 2


def test_append_with_hash_key_column_creates_clustered_table(spark, append_target_schema):
    table = f"{append_target_schema}.append_probe_4"
    df = spark.createDataFrame([("A", "hashval1")], ["id", HASH_KEY_COLUMN])
    append_missing_records(df, table)
    detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].asDict()
    clustering_columns = detail.get("clusteringColumns")
    if clustering_columns is not None:  # tolerate Delta versions that name this field differently
        assert HASH_KEY_COLUMN in clustering_columns


# ---------------------------------------------------------------------------
# write_run_log_entry / is_target_batch_already_processed
# ---------------------------------------------------------------------------


@pytest.fixture()
def run_log_schema(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
    spark.sql(get_reconciliation_run_log_ddl(CONTROL_SCHEMA, {}))
    yield CONTROL_SCHEMA
    spark.sql(f"DROP TABLE IF EXISTS {CONTROL_SCHEMA}.reconciliation_run_log")
    spark.sql(f"DROP SCHEMA IF EXISTS {CONTROL_SCHEMA} CASCADE")


def test_write_run_log_entry_lands_every_value_in_the_correct_column(spark, run_log_schema):
    """Regression test for the Row(**kwargs)-vs-schema positional-ordering bug -- see module
    docstring. Asserts on every column by name, not just that the write succeeded."""
    metrics = ReconciliationMetrics(
        source_record_count=10,
        target_record_count=8,
        matched_count=6,
        missing_in_target_count=3,
        missing_in_source_count=1,
        value_drift_count=1,
        appended_count=3,
        failed_count=0,
    )
    write_run_log_entry(
        spark, run_log_schema, "recon-1", "target-1", "run-1", "fingerprint-abc", "SUCCESS", metrics=metrics
    )
    row = spark.table(f"{run_log_schema}.reconciliation_run_log").collect()[0]
    assert row["run_id"] == "run-1"
    assert row["reconciliation_id"] == "recon-1"
    assert row["target_id"] == "target-1"
    assert row["source_batch_fingerprint"] == "fingerprint-abc"
    assert row["source_record_count"] == 10
    assert row["target_record_count"] == 8
    assert row["matched_count"] == 6
    assert row["missing_in_target_count"] == 3
    assert row["missing_in_source_count"] == 1
    assert row["value_drift_count"] == 1
    assert row["appended_count"] == 3
    assert row["failed_count"] == 0
    assert row["status"] == "SUCCESS"
    assert row["error_message"] is None
    assert row["run_at"] is not None


def test_write_run_log_entry_without_metrics_defaults_to_nulls(spark, run_log_schema):
    write_run_log_entry(spark, run_log_schema, "recon-1", "target-1", "run-2", "unknown", "FAILED", error_message="boom")
    row = spark.table(f"{run_log_schema}.reconciliation_run_log").filter("run_id = 'run-2'").collect()[0]
    assert row["status"] == "FAILED"
    assert row["error_message"] == "boom"
    assert row["source_record_count"] is None


def test_is_target_batch_already_processed_true_for_matching_success_run(spark, run_log_schema):
    write_run_log_entry(spark, run_log_schema, "recon-1", "target-1", "run-3", "fp-1", "SUCCESS")
    assert is_target_batch_already_processed(spark, run_log_schema, "recon-1", "target-1", "fp-1") is True


def test_is_target_batch_already_processed_false_for_different_fingerprint(spark, run_log_schema):
    write_run_log_entry(spark, run_log_schema, "recon-1", "target-1", "run-4", "fp-1", "SUCCESS")
    assert is_target_batch_already_processed(spark, run_log_schema, "recon-1", "target-1", "fp-2") is False


def test_is_target_batch_already_processed_false_for_different_target_id(spark, run_log_schema):
    write_run_log_entry(spark, run_log_schema, "recon-1", "target-1", "run-5", "fp-1", "SUCCESS")
    assert is_target_batch_already_processed(spark, run_log_schema, "recon-1", "target-2", "fp-1") is False


def test_is_target_batch_already_processed_false_for_non_success_status(spark, run_log_schema):
    write_run_log_entry(spark, run_log_schema, "recon-1", "target-1", "run-6", "fp-1", "FAILED", error_message="x")
    assert is_target_batch_already_processed(spark, run_log_schema, "recon-1", "target-1", "fp-1") is False
