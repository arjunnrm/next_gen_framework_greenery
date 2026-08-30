"""Unit tests for cdc/change_metrics.py -- Change-Data-Feed-based insert/update/delete counts.

Exercises capture_scd_change_counts against a real, disposable Delta table (CDF works on any
Delta table, not only DLT-managed ones, so this doesn't need a live pipeline run) rather than
mocking table_changes() -- the whole point of this module is trusting Delta's own native
mechanism over a manual diff, so the test should prove the real SQL function behaves as
expected.
"""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.cdc.change_metrics import capture_scd_change_counts

CATALOG = "metaflow"
SCHEMA = "silver_test"
TABLE = "change_metrics_probe"
QUALIFIED_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"


@pytest.fixture()
def cdf_table(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(
        f"CREATE OR REPLACE TABLE {QUALIFIED_TABLE} (id STRING, tier STRING) USING DELTA "
        "TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )
    yield QUALIFIED_TABLE
    spark.sql(f"DROP TABLE IF EXISTS {QUALIFIED_TABLE}")


def _current_version(spark, qualified_table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {qualified_table}").agg({"version": "max"}).collect()[0][0]


def test_insert_update_delete_counts_match_the_known_batch(spark, cdf_table):
    start_version = _current_version(spark, cdf_table)

    spark.sql(f"INSERT INTO {cdf_table} VALUES ('C001', 'GOLD'), ('C002', 'SILVER'), ('C003', 'BRONZE')")
    spark.sql(f"UPDATE {cdf_table} SET tier = 'PLATINUM' WHERE id = 'C001'")
    spark.sql(f"DELETE FROM {cdf_table} WHERE id = 'C003'")

    end_version = _current_version(spark, cdf_table)
    counts = capture_scd_change_counts(spark, cdf_table, start_version, end_version)

    assert counts["inserted_count"] == 3
    assert counts["updated_count"] == 1
    assert counts["deleted_count"] == 1


def test_no_changes_in_range_returns_all_zero_counts(spark, cdf_table):
    version = _current_version(spark, cdf_table)
    counts = capture_scd_change_counts(spark, cdf_table, version, version)
    assert counts == {"inserted_count": 0, "updated_count": 0, "deleted_count": 0}


def test_invalid_table_raises_framework_config_error(spark):
    with pytest.raises(FrameworkConfigError):
        capture_scd_change_counts(spark, f"{CATALOG}.{SCHEMA}.table_that_does_not_exist", 0, 1)
