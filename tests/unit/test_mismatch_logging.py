"""Unit tests for reconciliation/mismatch_logging.py -- reconciliation_mismatch_log writes.

Runs against a real, disposable reconciliation_mismatch_log table, created via the exact
production DDL (`ddl_definitions.py::get_reconciliation_mismatch_log_ddl`) so column
names/types match what `05_reconciliation_engine.py` actually writes into in production --
mirrors test_change_metrics.py's own convention of testing against a real disposable table
rather than mocking the write.
"""

import json

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.cdc.hashing import HASH_VALUE_COLUMN
from NextGen_Metadata_Framework.lakeflow_framework.control_plane.ddl_definitions import get_reconciliation_mismatch_log_ddl
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.matcher import (
    MISMATCH_TYPE_COLUMN,
    MISMATCH_TYPE_MISSING_IN_SOURCE,
    MISMATCH_TYPE_MISSING_IN_TARGET,
    MISMATCH_TYPE_VALUE_DRIFT,
    target_prefixed_column,
)
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.mismatch_logging import write_mismatch_log_rows

CATALOG = "metaflow"
CONTROL_SCHEMA = f"{CATALOG}.config_test_mismatch_logging"
MATCH_KEYS = ["id"]
COMPARE_COLUMNS = ["status"]


@pytest.fixture()
def control_schema(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
    spark.sql(get_reconciliation_mismatch_log_ddl(CONTROL_SCHEMA, {}))
    yield CONTROL_SCHEMA
    spark.sql(f"DROP TABLE IF EXISTS {CONTROL_SCHEMA}.reconciliation_mismatch_log")
    spark.sql(f"DROP SCHEMA IF EXISTS {CONTROL_SCHEMA} CASCADE")


def _mismatch_detail_df(spark):
    """Builds a DataFrame shaped exactly like matcher.py's
    ReconciliationMatchResult.mismatch_detail_df output: unprefixed source-side match_keys/
    compare_columns/hash_value, target_prefixed_column(...) for the target side, plus
    __recon_mismatch_type."""
    columns = [
        "id",
        "status",
        target_prefixed_column("id"),
        target_prefixed_column("status"),
        HASH_VALUE_COLUMN,
        target_prefixed_column(HASH_VALUE_COLUMN),
        MISMATCH_TYPE_COLUMN,
    ]
    rows = [
        ("1", "ACTIVE", None, None, "hash_src_1", None, MISMATCH_TYPE_MISSING_IN_TARGET),
        ("2", "ACTIVE", "2", "INACTIVE", "hash_src_2", "hash_tgt_2", MISMATCH_TYPE_VALUE_DRIFT),
        (None, None, "3", "X", None, "hash_tgt_3", MISMATCH_TYPE_MISSING_IN_SOURCE),
    ]
    return spark.createDataFrame(rows, columns)


def test_writes_one_row_per_mismatch_record(spark, control_schema):
    df = _mismatch_detail_df(spark)
    count = write_mismatch_log_rows(spark, control_schema, "run-1", "recon-1", "target-1", df, MATCH_KEYS, COMPARE_COLUMNS)
    assert count == 3
    assert spark.table(f"{control_schema}.reconciliation_mismatch_log").count() == 3


def test_zero_rows_is_a_no_op(spark, control_schema):
    df = _mismatch_detail_df(spark).filter("1 = 0")
    count = write_mismatch_log_rows(spark, control_schema, "run-1", "recon-1", "target-1", df, MATCH_KEYS, COMPARE_COLUMNS)
    assert count == 0
    assert spark.table(f"{control_schema}.reconciliation_mismatch_log").count() == 0


def test_match_key_values_json_coalesces_across_both_sides(spark, control_schema):
    write_mismatch_log_rows(spark, control_schema, "run-1", "recon-1", "target-1", _mismatch_detail_df(spark), MATCH_KEYS, COMPARE_COLUMNS)
    rows = {
        row["mismatch_type"]: json.loads(row["match_key_values_json"])
        for row in spark.table(f"{control_schema}.reconciliation_mismatch_log").collect()
    }
    assert rows[MISMATCH_TYPE_MISSING_IN_TARGET] == {"id": "1"}
    assert rows[MISMATCH_TYPE_VALUE_DRIFT] == {"id": "2"}
    assert rows[MISMATCH_TYPE_MISSING_IN_SOURCE] == {"id": "3"}  # only present target-side, coalesced in


def test_differing_columns_json_populated_only_for_value_drift(spark, control_schema):
    write_mismatch_log_rows(spark, control_schema, "run-1", "recon-1", "target-1", _mismatch_detail_df(spark), MATCH_KEYS, COMPARE_COLUMNS)
    by_type = {
        row["mismatch_type"]: row["differing_columns_json"]
        for row in spark.table(f"{control_schema}.reconciliation_mismatch_log").collect()
    }
    assert by_type[MISMATCH_TYPE_MISSING_IN_TARGET] is None
    assert by_type[MISMATCH_TYPE_MISSING_IN_SOURCE] is None

    drift_detail = json.loads(by_type[MISMATCH_TYPE_VALUE_DRIFT])
    assert drift_detail == [{"column": "status", "source_value": "ACTIVE", "target_value": "INACTIVE"}]


def test_differing_columns_json_null_when_compare_columns_empty(spark, control_schema):
    write_mismatch_log_rows(spark, control_schema, "run-1", "recon-1", "target-1", _mismatch_detail_df(spark), MATCH_KEYS, [])
    rows = spark.table(f"{control_schema}.reconciliation_mismatch_log").collect()
    assert all(row["differing_columns_json"] is None for row in rows)


def test_source_and_target_hash_values_are_carried_through(spark, control_schema):
    write_mismatch_log_rows(spark, control_schema, "run-1", "recon-1", "target-1", _mismatch_detail_df(spark), MATCH_KEYS, COMPARE_COLUMNS)
    by_type = {
        row["mismatch_type"]: (row["source_hash_value"], row["target_hash_value"])
        for row in spark.table(f"{control_schema}.reconciliation_mismatch_log").collect()
    }
    assert by_type[MISMATCH_TYPE_MISSING_IN_TARGET] == ("hash_src_1", None)
    assert by_type[MISMATCH_TYPE_VALUE_DRIFT] == ("hash_src_2", "hash_tgt_2")
    assert by_type[MISMATCH_TYPE_MISSING_IN_SOURCE] == (None, "hash_tgt_3")


def test_run_id_reconciliation_id_target_id_are_set_correctly(spark, control_schema):
    write_mismatch_log_rows(spark, control_schema, "run-42", "recon-99", "target-7", _mismatch_detail_df(spark), MATCH_KEYS, COMPARE_COLUMNS)
    rows = spark.table(f"{control_schema}.reconciliation_mismatch_log").collect()
    assert all(row["run_id"] == "run-42" for row in rows)
    assert all(row["reconciliation_id"] == "recon-99" for row in rows)
    assert all(row["target_id"] == "target-7" for row in rows)
    assert all(row["mismatch_id"] is not None for row in rows)
    ids = {row["mismatch_id"] for row in rows}
    assert len(ids) == 3  # every mismatch_id is unique
