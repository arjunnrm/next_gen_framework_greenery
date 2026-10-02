"""Unit tests for soft-delete processor.

Tests cover:
- Configuration validation
- Key file reading and filtering
- Primary key matching (single and composite)
- Delta MERGE execution
- Idempotency
- Error handling
- Edge cases
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

from flowx.lakeflow_framework.soft_delete.processor import (
    apply_soft_deletes,
    _build_merge_sql,
    _build_match_condition,
)
from flowx.lakeflow_framework.exceptions import FrameworkConfigError


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock(spec=SparkSession)
    spark.read = MagicMock()
    spark.sql = MagicMock()
    return spark


@pytest.fixture
def sample_key_dataframe():
    """Create a sample key DataFrame."""
    df = MagicMock(spec=DataFrame)
    df.columns = ["customer_id", "is_marked_deleted"]
    df.count.return_value = 3
    df.filter.return_value = df
    df.select.return_value = df
    df.distinct.return_value = df
    df.createOrReplaceTempView = MagicMock()
    return df


@pytest.fixture
def sample_bronze_dataframe():
    """Create a sample Bronze DataFrame."""
    df = MagicMock(spec=DataFrame)
    df.columns = ["customer_id", "name", "amount"]
    df.count.return_value = 100
    return df


class TestApplySoftDeletes:
    """Test suite for apply_soft_deletes function."""

    def test_successful_soft_delete_single_key(self, mock_spark, sample_key_dataframe, sample_bronze_dataframe):
        """Test successful soft-delete with single primary key."""
        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = sample_bronze_dataframe
        mock_spark.sql.return_value.collect.return_value = [{"cnt": 2}]

        result = apply_soft_deletes(
            spark=mock_spark,
            catalog="main",
            schema="bronze",
            table="customers",
            key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
            primary_keys=["customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
            flow_id="df_customer_ingest",
        )

        assert result["status"] == "success"
        assert result["flow_id"] == "df_customer_ingest"
        assert result["target_table"] == "main.bronze.customers"
        assert result["rows_in_key_file"] == 3
        assert result["rows_marked_for_deletion"] == 3
        assert mock_spark.sql.called

    def test_successful_soft_delete_composite_keys(self, mock_spark, sample_key_dataframe, sample_bronze_dataframe):
        """Test successful soft-delete with composite primary keys."""
        sample_key_dataframe.columns = ["org_id", "customer_id", "is_marked_deleted"]
        sample_bronze_dataframe.columns = ["org_id", "customer_id", "name", "amount"]

        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = sample_bronze_dataframe
        mock_spark.sql.return_value.collect.return_value = [{"cnt": 2}]

        result = apply_soft_deletes(
            spark=mock_spark,
            catalog="main",
            schema="bronze",
            table="customers",
            key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
            primary_keys=["org_id", "customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        assert result["status"] == "success"
        # Verify MERGE SQL was called with composite key condition
        merge_call = [c for c in mock_spark.sql.call_args_list if "MERGE" in str(c)]
        assert len(merge_call) > 0

    def test_no_deletes_returns_skipped(self, mock_spark, sample_key_dataframe, sample_bronze_dataframe):
        """Test that zero deletion marks returns 'skipped' status."""
        empty_df = MagicMock(spec=DataFrame)
        empty_df.columns = ["customer_id", "is_marked_deleted"]
        empty_df.count.return_value = 0

        sample_key_dataframe.filter.return_value = empty_df
        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = sample_bronze_dataframe

        result = apply_soft_deletes(
            spark=mock_spark,
            catalog="main",
            schema="bronze",
            table="customers",
            key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
            primary_keys=["customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        assert result["status"] == "skipped"
        assert result["rows_marked_for_deletion"] == 0

    def test_key_file_not_found(self, mock_spark):
        """Test error handling when key file doesn't exist."""
        mock_spark.read.parquet.side_effect = FileNotFoundError("File not found")

        with pytest.raises(FrameworkConfigError) as exc_info:
            apply_soft_deletes(
                spark=mock_spark,
                catalog="main",
                schema="bronze",
                table="customers",
                key_file_path="/Volumes/main/prod/deletes/missing.parquet",
                primary_keys=["customer_id"],
                key_file_deleted_indicator_column="is_marked_deleted",
            )

        assert "Failed to read key file" in str(exc_info.value)

    def test_indicator_column_not_found(self, mock_spark):
        """Test error when delete indicator column doesn't exist in key file."""
        key_df = MagicMock(spec=DataFrame)
        key_df.columns = ["customer_id"]  # Missing is_marked_deleted
        key_df.filter.side_effect = Exception("Column not found")

        mock_spark.read.parquet.return_value = key_df

        with pytest.raises(FrameworkConfigError) as exc_info:
            apply_soft_deletes(
                spark=mock_spark,
                catalog="main",
                schema="bronze",
                table="customers",
                key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
                primary_keys=["customer_id"],
                key_file_deleted_indicator_column="is_marked_deleted",
            )

        assert "not found in key file" in str(exc_info.value)

    def test_primary_key_not_in_bronze(self, mock_spark, sample_key_dataframe):
        """Test error when primary key column doesn't exist in Bronze table."""
        bronze_df = MagicMock(spec=DataFrame)
        bronze_df.columns = ["name", "amount"]  # Missing customer_id

        sample_key_dataframe.filter.return_value = sample_key_dataframe
        sample_key_dataframe.select.return_value = sample_key_dataframe
        sample_key_dataframe.distinct.return_value = sample_key_dataframe
        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = bronze_df

        with pytest.raises(FrameworkConfigError) as exc_info:
            apply_soft_deletes(
                spark=mock_spark,
                catalog="main",
                schema="bronze",
                table="customers",
                key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
                primary_keys=["customer_id"],
                key_file_deleted_indicator_column="is_marked_deleted",
            )

        assert "Primary key columns not found" in str(exc_info.value)
        assert "customer_id" in str(exc_info.value)

    def test_bronze_table_not_found(self, mock_spark, sample_key_dataframe):
        """Test error when Bronze table doesn't exist."""
        sample_key_dataframe.filter.return_value = sample_key_dataframe
        sample_key_dataframe.select.return_value = sample_key_dataframe
        sample_key_dataframe.distinct.return_value = sample_key_dataframe
        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.side_effect = Exception("Table not found")

        with pytest.raises(FrameworkConfigError) as exc_info:
            apply_soft_deletes(
                spark=mock_spark,
                catalog="main",
                schema="bronze",
                table="missing_customers",
                key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
                primary_keys=["customer_id"],
                key_file_deleted_indicator_column="is_marked_deleted",
            )

        assert "Could not read target table" in str(exc_info.value)

    def test_merge_sql_execution_failure(self, mock_spark, sample_key_dataframe, sample_bronze_dataframe):
        """Test error handling when MERGE SQL fails."""
        sample_key_dataframe.filter.return_value = sample_key_dataframe
        sample_key_dataframe.select.return_value = sample_key_dataframe
        sample_key_dataframe.distinct.return_value = sample_key_dataframe
        sample_key_dataframe.createOrReplaceTempView = MagicMock()

        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = sample_bronze_dataframe

        # Make MERGE fail
        mock_spark.sql.side_effect = [MagicMock(), Exception("Syntax error in MERGE")]

        with pytest.raises(FrameworkConfigError) as exc_info:
            apply_soft_deletes(
                spark=mock_spark,
                catalog="main",
                schema="bronze",
                table="customers",
                key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
                primary_keys=["customer_id"],
                key_file_deleted_indicator_column="is_marked_deleted",
            )

        assert "Delta MERGE failed" in str(exc_info.value)

    def test_is_deleted_column_created_if_missing(self, mock_spark, sample_key_dataframe, sample_bronze_dataframe):
        """Test that is_deleted column is created if it doesn't exist."""
        sample_bronze_dataframe.columns = ["customer_id", "name"]  # No is_deleted
        sample_key_dataframe.filter.return_value = sample_key_dataframe
        sample_key_dataframe.select.return_value = sample_key_dataframe
        sample_key_dataframe.distinct.return_value = sample_key_dataframe
        sample_key_dataframe.createOrReplaceTempView = MagicMock()

        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = sample_bronze_dataframe
        mock_spark.sql.return_value.collect.return_value = [{"cnt": 2}]

        result = apply_soft_deletes(
            spark=mock_spark,
            catalog="main",
            schema="bronze",
            table="customers",
            key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
            primary_keys=["customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        assert result["status"] == "success"
        # Verify ALTER TABLE was called
        alter_calls = [c for c in mock_spark.sql.call_args_list if "ALTER TABLE" in str(c)]
        assert len(alter_calls) > 0


class TestBuildMergeSql:
    """Test suite for _build_merge_sql helper function."""

    def test_merge_sql_single_key(self):
        """Test MERGE SQL generation with single primary key."""
        deleted_keys_df = MagicMock(spec=DataFrame)
        deleted_keys_df.createOrReplaceTempView = MagicMock()

        sql = _build_merge_sql(
            target_table_fqn="main.bronze.customers",
            primary_keys=["customer_id"],
            deleted_keys_df=deleted_keys_df,
        )

        assert "MERGE INTO main.bronze.customers t" in sql
        assert "_soft_delete_temp_keys" in sql
        assert "t.customer_id = k.customer_id" in sql
        assert "t.is_deleted = true" in sql
        assert "WHEN MATCHED THEN UPDATE" in sql

    def test_merge_sql_composite_keys(self):
        """Test MERGE SQL generation with composite primary keys."""
        deleted_keys_df = MagicMock(spec=DataFrame)
        deleted_keys_df.createOrReplaceTempView = MagicMock()

        sql = _build_merge_sql(
            target_table_fqn="main.bronze.customers",
            primary_keys=["org_id", "customer_id"],
            deleted_keys_df=deleted_keys_df,
        )

        assert "t.org_id = k.org_id" in sql
        assert "t.customer_id = k.customer_id" in sql
        # Both keys should be ANDed together
        assert "t.org_id = k.org_id AND t.customer_id = k.customer_id" in sql


class TestBuildMatchCondition:
    """Test suite for _build_match_condition helper function."""

    def test_match_condition_single_key(self):
        """Test match condition with single key."""
        condition = _build_match_condition(["customer_id"])
        assert condition == "t.customer_id = k.customer_id"

    def test_match_condition_composite_keys(self):
        """Test match condition with composite keys."""
        condition = _build_match_condition(["org_id", "customer_id"])
        assert condition == "t.org_id = k.org_id AND t.customer_id = k.customer_id"

    def test_match_condition_three_keys(self):
        """Test match condition with three keys."""
        condition = _build_match_condition(["region", "org_id", "customer_id"])
        assert condition == (
            "t.region = k.region AND t.org_id = k.org_id AND t.customer_id = k.customer_id"
        )


class TestIdempotency:
    """Test suite for idempotency guarantee."""

    def test_idempotent_execution(self, mock_spark, sample_key_dataframe, sample_bronze_dataframe):
        """Test that running twice with same key file produces same result."""
        sample_key_dataframe.filter.return_value = sample_key_dataframe
        sample_key_dataframe.select.return_value = sample_key_dataframe
        sample_key_dataframe.distinct.return_value = sample_key_dataframe
        sample_key_dataframe.createOrReplaceTempView = MagicMock()

        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = sample_bronze_dataframe
        mock_spark.sql.return_value.collect.return_value = [{"cnt": 2}]

        result1 = apply_soft_deletes(
            spark=mock_spark,
            catalog="main",
            schema="bronze",
            table="customers",
            key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
            primary_keys=["customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        # Reset mocks and run again
        mock_spark.reset_mock()
        sample_key_dataframe.filter.return_value = sample_key_dataframe
        sample_key_dataframe.select.return_value = sample_key_dataframe
        sample_key_dataframe.distinct.return_value = sample_key_dataframe
        sample_key_dataframe.createOrReplaceTempView = MagicMock()

        mock_spark.read.parquet.return_value = sample_key_dataframe
        mock_spark.read.table.return_value = sample_bronze_dataframe
        mock_spark.sql.return_value.collect.return_value = [{"cnt": 2}]

        result2 = apply_soft_deletes(
            spark=mock_spark,
            catalog="main",
            schema="bronze",
            table="customers",
            key_file_path="/Volumes/main/prod/deletes/customer_keys.parquet",
            primary_keys=["customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        # Both should have same status and structure
        assert result1["status"] == result2["status"]
        assert result1["target_table"] == result2["target_table"]
