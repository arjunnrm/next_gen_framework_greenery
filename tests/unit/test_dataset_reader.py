"""Unit tests for reconciliation/dataset_reader.py -- type-dispatched dataset reading.

`type: "table"` is exercised against a real, disposable Delta table (mirrors
test_change_metrics.py's own convention) since `spark.read.table(...)` needs a real catalog
entry. `type: "file"`/`"sink"` and streaming dispatch are covered by config-validation-error
paths only here (no actual file I/O) -- real file/sink reads are exercised by the reconciliation
integration test, which runs against a live pipeline.
"""

import pytest

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.reconciliation.dataset_reader import read_reconciliation_dataset

CATALOG = "flowx"
SCHEMA = "reconciliation_test"
TABLE = "dataset_reader_probe"
QUALIFIED_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"


@pytest.fixture()
def probe_table(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE OR REPLACE TABLE {QUALIFIED_TABLE} (id STRING, region STRING) USING DELTA")
    spark.sql(f"INSERT INTO {QUALIFIED_TABLE} VALUES ('C001', 'us'), ('C002', 'eu'), ('C003', 'us')")
    yield QUALIFIED_TABLE
    spark.sql(f"DROP TABLE IF EXISTS {QUALIFIED_TABLE}")


def test_table_type_reads_full_table(spark, probe_table):
    df = read_reconciliation_dataset(spark, {"type": "table", "table": probe_table})
    assert df.count() == 3


def test_type_defaults_to_table(spark, probe_table):
    df = read_reconciliation_dataset(spark, {"table": probe_table})
    assert df.count() == 3


def test_filter_condition_applied_with_parameter_substitution(spark, probe_table):
    df = read_reconciliation_dataset(
        spark,
        {"type": "table", "table": probe_table, "filter_condition": "region = ${region}"},
        parameters={"region": "us"},
    )
    assert df.count() == 2


def test_data_standardization_sql_applied_after_read(spark, probe_table):
    df = read_reconciliation_dataset(
        spark, {"type": "table", "table": probe_table, "data_standardization_sql": ["upper(region) AS region"]}
    )
    regions = {row["region"] for row in df.collect()}
    assert regions == {"US", "EU"}

    # eu row got upper()'d too, both distinct values present in upper case only
    assert "eu" not in regions and "us" not in regions


def test_streaming_read_mode_returns_a_streaming_dataframe(spark, probe_table):
    df = read_reconciliation_dataset(spark, {"type": "table", "table": probe_table, "read_mode": "streaming"})
    assert df.isStreaming


def test_missing_table_key_raises_config_error(spark):
    with pytest.raises(FrameworkConfigError):
        read_reconciliation_dataset(spark, {"type": "table"})


def test_file_type_missing_path_raises_config_error(spark):
    with pytest.raises(FrameworkConfigError):
        read_reconciliation_dataset(spark, {"type": "file", "format": "csv"})


def test_unsupported_type_raises_config_error(spark):
    with pytest.raises(FrameworkConfigError, match="Unsupported reconciliation dataset type"):
        read_reconciliation_dataset(spark, {"type": "bogus"})


def test_hash_precomputed_rejected_for_file_type(spark):
    with pytest.raises(FrameworkConfigError, match="hash_precomputed"):
        read_reconciliation_dataset(spark, {"type": "file", "path": "/x/", "format": "csv", "hash_precomputed": True})


def test_hash_precomputed_rejected_for_sink_type(spark):
    with pytest.raises(FrameworkConfigError, match="hash_precomputed"):
        read_reconciliation_dataset(spark, {"type": "sink", "path": "/x/", "format": "delta", "hash_precomputed": True})


def test_filter_condition_undefined_parameter_raises_config_error(spark, probe_table):
    with pytest.raises(FrameworkConfigError):
        read_reconciliation_dataset(
            spark, {"type": "table", "table": probe_table, "filter_condition": "region = ${undefined_param}"}, parameters={}
        )
