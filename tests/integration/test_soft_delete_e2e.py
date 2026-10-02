"""End-to-end integration tests for soft-delete feature.

These tests require a live Spark session and Databricks workspace with
access to Unity Catalog and Volumes. They verify the complete soft-delete
workflow from key file creation through Bronze table updates.

Tests cover:
- Single and composite primary keys
- Multiple pipeline runs (idempotency)
- Transformation filters on soft-deleted rows
- Data integrity after soft-deletes
- Observability/logging
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType

from flowx.lakeflow_framework.soft_delete.processor import apply_soft_deletes


@pytest.fixture(scope="session")
def spark_session():
    """Create a live Spark session for integration tests."""
    spark = (
        SparkSession.builder
        .appName("soft_delete_integration_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def test_schema(spark_session):
    """Create a temporary test schema."""
    schema_name = "soft_delete_test"
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    yield schema_name
    spark_session.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


@pytest.fixture
def temp_volume():
    """Create a temporary directory for key files."""
    temp_dir = tempfile.mkdtemp(prefix="soft_delete_keys_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestSoftDeleteSingleKey:
    """Integration tests for soft-delete with single primary key."""

    def test_e2e_soft_delete_single_key(self, spark_session, test_schema, temp_volume):
        """End-to-end test: create Bronze table, key file, apply soft-deletes."""
        # Step 1: Create Bronze table with sample data
        bronze_table = f"spark_catalog.{test_schema}.customers"
        bronze_data = [
            ("C001", "Alice", 100.0),
            ("C002", "Bob", 200.0),
            ("C003", "Charlie", 300.0),
            ("C004", "David", 400.0),
            ("C005", "Eve", 500.0),
        ]
        bronze_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("amount", DoubleType(), False),
        ])
        bronze_df = spark_session.createDataFrame(bronze_data, schema=bronze_schema)
        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(bronze_table)

        # Verify initial state
        initial_count = spark_session.sql(f"SELECT COUNT(*) as cnt FROM {bronze_table}").collect()[0]["cnt"]
        assert initial_count == 5

        # Step 2: Create key file with deletes
        key_file_data = [
            ("C001", True),   # Delete Alice
            ("C003", True),   # Delete Charlie
            ("C005", True),   # Delete Eve
            ("C999", False),  # Not in Bronze, but marked false
        ]
        key_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("is_marked_deleted", BooleanType(), False),
        ])
        key_df = spark_session.createDataFrame(key_file_data, schema=key_schema)
        key_file_path = f"{temp_volume}/customer_keys.parquet"
        key_df.write.format("parquet").mode("overwrite").save(key_file_path)

        # Step 3: Apply soft-deletes
        result = apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="customers",
            key_file_path=key_file_path,
            primary_keys=["customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
            flow_id="df_customers",
        )

        # Verify result metadata
        assert result["status"] == "success"
        assert result["rows_in_key_file"] == 4
        assert result["rows_marked_for_deletion"] == 3
        assert result["rows_updated"] > 0

        # Step 4: Verify soft-deleted rows
        soft_deleted_rows = spark_session.sql(
            f"SELECT customer_id, name FROM {bronze_table} WHERE is_deleted = true ORDER BY customer_id"
        ).collect()

        soft_deleted_ids = [row["customer_id"] for row in soft_deleted_rows]
        assert soft_deleted_ids == ["C001", "C003", "C005"]

        # Verify non-deleted rows
        active_rows = spark_session.sql(
            f"SELECT customer_id, name FROM {bronze_table} WHERE is_deleted IS NULL OR is_deleted = false ORDER BY customer_id"
        ).collect()

        active_ids = [row["customer_id"] for row in active_rows]
        assert active_ids == ["C002", "C004"]

    def test_transformation_filters_soft_deleted(self, spark_session, test_schema, temp_volume):
        """Test that transformation filters out soft-deleted rows."""
        # Step 1: Create Bronze table
        bronze_table = f"spark_catalog.{test_schema}.orders"
        bronze_data = [
            ("ORD001", "C001", 100.0),
            ("ORD002", "C002", 200.0),
            ("ORD003", "C003", 300.0),
        ]
        bronze_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", DoubleType(), False),
        ])
        bronze_df = spark_session.createDataFrame(bronze_data, schema=bronze_schema)
        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(bronze_table)

        # Step 2: Create key file
        key_data = [
            ("C001", True),
        ]
        key_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("is_marked_deleted", BooleanType(), False),
        ])
        key_df = spark_session.createDataFrame(key_data, schema=key_schema)
        key_file_path = f"{temp_volume}/customer_keys_orders.parquet"
        key_df.write.format("parquet").mode("overwrite").save(key_file_path)

        # Step 3: Apply soft-deletes
        apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="orders",
            key_file_path=key_file_path,
            primary_keys=["customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        # Step 4: Simulate transformation that filters soft-deleted
        silver_table = f"spark_catalog.{test_schema}.orders_silver"
        spark_session.sql(f"""
            CREATE OR REPLACE TABLE {silver_table} AS
            SELECT order_id, customer_id, amount
            FROM {bronze_table}
            WHERE is_deleted IS NULL OR is_deleted = false
        """)

        # Verify Silver table has only non-deleted rows
        silver_count = spark_session.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").collect()[0]["cnt"]
        assert silver_count == 2

        silver_ids = spark_session.sql(f"SELECT customer_id FROM {silver_table} ORDER BY customer_id").collect()
        silver_customer_ids = [row["customer_id"] for row in silver_ids]
        assert silver_customer_ids == ["C002", "C003"]


class TestSoftDeleteCompositeKeys:
    """Integration tests for soft-delete with composite primary keys."""

    def test_e2e_soft_delete_composite_keys(self, spark_session, test_schema, temp_volume):
        """End-to-end test with composite keys (org_id, customer_id)."""
        # Step 1: Create Bronze table
        bronze_table = f"spark_catalog.{test_schema}.org_customers"
        bronze_data = [
            ("ORG1", "C001", "Alice"),
            ("ORG1", "C002", "Bob"),
            ("ORG2", "C001", "Alice_ORG2"),
            ("ORG2", "C003", "Charlie"),
        ]
        bronze_schema = StructType([
            StructField("org_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), False),
        ])
        bronze_df = spark_session.createDataFrame(bronze_data, schema=bronze_schema)
        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(bronze_table)

        # Step 2: Create key file with composite keys
        key_data = [
            ("ORG1", "C001", True),   # Delete Alice from ORG1
            ("ORG2", "C001", True),   # Delete Alice_ORG2 from ORG2
        ]
        key_schema = StructType([
            StructField("org_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("is_marked_deleted", BooleanType(), False),
        ])
        key_df = spark_session.createDataFrame(key_data, schema=key_schema)
        key_file_path = f"{temp_volume}/org_customer_keys.parquet"
        key_df.write.format("parquet").mode("overwrite").save(key_file_path)

        # Step 3: Apply soft-deletes
        result = apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="org_customers",
            key_file_path=key_file_path,
            primary_keys=["org_id", "customer_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        assert result["status"] == "success"
        assert result["rows_marked_for_deletion"] == 2

        # Step 4: Verify correct rows soft-deleted
        soft_deleted = spark_session.sql(
            f"SELECT org_id, customer_id, name FROM {bronze_table} WHERE is_deleted = true ORDER BY org_id, customer_id"
        ).collect()

        assert len(soft_deleted) == 2
        assert soft_deleted[0]["org_id"] == "ORG1"
        assert soft_deleted[0]["customer_id"] == "C001"
        assert soft_deleted[1]["org_id"] == "ORG2"
        assert soft_deleted[1]["customer_id"] == "C001"

        # Verify other rows remain active
        active = spark_session.sql(
            f"SELECT org_id, customer_id FROM {bronze_table} WHERE is_deleted IS NULL OR is_deleted = false ORDER BY org_id, customer_id"
        ).collect()

        assert len(active) == 2
        active_tuples = [(row["org_id"], row["customer_id"]) for row in active]
        assert ("ORG1", "C002") in active_tuples
        assert ("ORG2", "C003") in active_tuples


class TestIdempotency:
    """Integration tests for idempotency guarantee."""

    def test_multiple_runs_same_key_file(self, spark_session, test_schema, temp_volume):
        """Test that running soft-deletes twice with same key file = same result."""
        # Setup: Create Bronze table
        bronze_table = f"spark_catalog.{test_schema}.idempotent_test"
        bronze_data = [
            ("ID001", "Item A"),
            ("ID002", "Item B"),
            ("ID003", "Item C"),
        ]
        bronze_schema = StructType([
            StructField("item_id", StringType(), False),
            StructField("description", StringType(), False),
        ])
        bronze_df = spark_session.createDataFrame(bronze_data, schema=bronze_schema)
        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(bronze_table)

        # Create key file
        key_data = [("ID001", True), ("ID002", True)]
        key_schema = StructType([
            StructField("item_id", StringType(), False),
            StructField("is_marked_deleted", BooleanType(), False),
        ])
        key_df = spark_session.createDataFrame(key_data, schema=key_schema)
        key_file_path = f"{temp_volume}/item_keys.parquet"
        key_df.write.format("parquet").mode("overwrite").save(key_file_path)

        # Run 1: Apply soft-deletes
        result1 = apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="idempotent_test",
            key_file_path=key_file_path,
            primary_keys=["item_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        count_after_run1 = spark_session.sql(
            f"SELECT COUNT(*) as cnt FROM {bronze_table} WHERE is_deleted = true"
        ).collect()[0]["cnt"]

        # Run 2: Apply same soft-deletes again
        result2 = apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="idempotent_test",
            key_file_path=key_file_path,
            primary_keys=["item_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        count_after_run2 = spark_session.sql(
            f"SELECT COUNT(*) as cnt FROM {bronze_table} WHERE is_deleted = true"
        ).collect()[0]["cnt"]

        # Verify idempotency: same count, same status
        assert result1["status"] == result2["status"] == "success"
        assert count_after_run1 == count_after_run2 == 2

    def test_incremental_deletes(self, spark_session, test_schema, temp_volume):
        """Test that new deletes are added incrementally on subsequent runs."""
        # Setup: Create Bronze table
        bronze_table = f"spark_catalog.{test_schema}.incremental_test"
        bronze_data = [
            ("R001", "Record A"),
            ("R002", "Record B"),
            ("R003", "Record C"),
            ("R004", "Record D"),
        ]
        bronze_schema = StructType([
            StructField("record_id", StringType(), False),
            StructField("value", StringType(), False),
        ])
        bronze_df = spark_session.createDataFrame(bronze_data, schema=bronze_schema)
        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(bronze_table)

        # Run 1: Delete R001 and R002
        key_data_run1 = [("R001", True), ("R002", True)]
        key_schema = StructType([
            StructField("record_id", StringType(), False),
            StructField("is_marked_deleted", BooleanType(), False),
        ])
        key_df_run1 = spark_session.createDataFrame(key_data_run1, schema=key_schema)
        key_file_path = f"{temp_volume}/record_keys_incremental.parquet"
        key_df_run1.write.format("parquet").mode("overwrite").save(key_file_path)

        apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="incremental_test",
            key_file_path=key_file_path,
            primary_keys=["record_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        count_after_run1 = spark_session.sql(
            f"SELECT COUNT(*) as cnt FROM {bronze_table} WHERE is_deleted = true"
        ).collect()[0]["cnt"]
        assert count_after_run1 == 2

        # Run 2: Key file now marks R001, R002, R003 as deleted (R003 added)
        key_data_run2 = [
            ("R001", True),
            ("R002", True),
            ("R003", True),  # NEW delete
            ("R004", False),
        ]
        key_df_run2 = spark_session.createDataFrame(key_data_run2, schema=key_schema)
        key_df_run2.write.format("parquet").mode("overwrite").save(key_file_path)

        apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="incremental_test",
            key_file_path=key_file_path,
            primary_keys=["record_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        count_after_run2 = spark_session.sql(
            f"SELECT COUNT(*) as cnt FROM {bronze_table} WHERE is_deleted = true"
        ).collect()[0]["cnt"]

        # Verify incremental addition: now 3 deleted instead of 2
        assert count_after_run2 == 3

        # Verify exact soft-deleted records
        deleted_ids = spark_session.sql(
            f"SELECT record_id FROM {bronze_table} WHERE is_deleted = true ORDER BY record_id"
        ).collect()
        deleted_id_list = [row["record_id"] for row in deleted_ids]
        assert deleted_id_list == ["R001", "R002", "R003"]


class TestEdgeCases:
    """Integration tests for edge cases and error scenarios."""

    def test_no_deletes_in_key_file(self, spark_session, test_schema, temp_volume):
        """Test handling when key file has no rows marked for deletion."""
        # Setup: Create Bronze table
        bronze_table = f"spark_catalog.{test_schema}.no_deletes"
        bronze_data = [("E001", "Entry A"), ("E002", "Entry B")]
        bronze_schema = StructType([
            StructField("entry_id", StringType(), False),
            StructField("name", StringType(), False),
        ])
        bronze_df = spark_session.createDataFrame(bronze_data, schema=bronze_schema)
        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(bronze_table)

        # Create key file with NO deletions marked
        key_data = [("E001", False), ("E002", False)]
        key_schema = StructType([
            StructField("entry_id", StringType(), False),
            StructField("is_marked_deleted", BooleanType(), False),
        ])
        key_df = spark_session.createDataFrame(key_data, schema=key_schema)
        key_file_path = f"{temp_volume}/no_delete_keys.parquet"
        key_df.write.format("parquet").mode("overwrite").save(key_file_path)

        # Apply soft-deletes
        result = apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="no_deletes",
            key_file_path=key_file_path,
            primary_keys=["entry_id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        # Verify status is skipped
        assert result["status"] == "skipped"
        assert result["rows_marked_for_deletion"] == 0

        # Verify no rows were marked
        soft_deleted = spark_session.sql(
            f"SELECT COUNT(*) as cnt FROM {bronze_table} WHERE is_deleted = true"
        ).collect()[0]["cnt"]
        assert soft_deleted == 0

    def test_empty_key_file(self, spark_session, test_schema, temp_volume):
        """Test handling when key file is empty."""
        # Setup: Create Bronze table
        bronze_table = f"spark_catalog.{test_schema}.empty_keys"
        bronze_data = [("EMPTY001", "Data")]
        bronze_schema = StructType([
            StructField("id", StringType(), False),
            StructField("data", StringType(), False),
        ])
        bronze_df = spark_session.createDataFrame(bronze_data, schema=bronze_schema)
        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(bronze_table)

        # Create EMPTY key file
        key_schema = StructType([
            StructField("id", StringType(), False),
            StructField("is_marked_deleted", BooleanType(), False),
        ])
        empty_df = spark_session.createDataFrame([], schema=key_schema)
        key_file_path = f"{temp_volume}/empty_keys.parquet"
        empty_df.write.format("parquet").mode("overwrite").save(key_file_path)

        # Apply soft-deletes
        result = apply_soft_deletes(
            spark=spark_session,
            catalog="spark_catalog",
            schema=test_schema,
            table="empty_keys",
            key_file_path=key_file_path,
            primary_keys=["id"],
            key_file_deleted_indicator_column="is_marked_deleted",
        )

        # Should be skipped since no rows to process
        assert result["status"] == "skipped"
        assert result["rows_in_key_file"] == 0
