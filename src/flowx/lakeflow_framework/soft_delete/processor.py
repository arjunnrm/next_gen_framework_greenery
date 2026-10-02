"""Core soft-delete processor: reads key file and applies soft-delete markers to Bronze table.

This module implements the soft-delete workflow:
1. Read external key file (READ-ONLY) from configurable path
2. Filter to rows where delete_indicator_column = true
3. Match keys against Bronze table using primary_keys
4. Use Delta MERGE to set is_deleted = true on matched rows
5. Log execution metadata

All operations are idempotent: running with the same key file multiple times
produces identical results.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.storage.table_properties import qualified_table_name

logger = logging.getLogger("flowx.lakeflow_framework.soft_delete.processor")


def apply_soft_deletes(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    key_file_path: str,
    primary_keys: List[str],
    key_file_deleted_indicator_column: str,
    flow_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Apply soft-delete markers to a Bronze table based on external key file.

    Reads a key file (READ-ONLY) containing primary keys marked for deletion,
    matches them against the target Bronze table, and sets is_deleted = true
    on matching rows using Delta MERGE.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    catalog : str
        Target table catalog.
    schema : str
        Target table schema.
    table : str
        Target table name (Bronze).
    key_file_path : str
        Path to external key file (Volumes path or Delta table path).
        Should be a Parquet file or Delta table readable by spark.read.
    primary_keys : List[str]
        List of column names that identify a unique record.
        Must be 1 or more. Matched against Bronze table columns.
    key_file_deleted_indicator_column : str
        Column name in the key file that indicates deletion (boolean/bit).
        Rows where this column = true are processed.
    flow_id : Optional[str]
        Flow identifier for logging/observability.

    Returns
    -------
    Dict[str, Any]
        Metadata dictionary:
        {
            "flow_id": str,
            "key_file_path": str,
            "target_table": str,
            "rows_in_key_file": int,
            "rows_marked_for_deletion": int,
            "rows_matched_in_bronze": int,
            "rows_updated": int,
            "status": "success" | "skipped" | "failed",
            "error_message": str (if status = "failed")
        }

    Raises
    ------
    FrameworkConfigError
        On any validation/execution error (missing file, schema mismatch, etc).

    Notes
    -----
    - Idempotent: running with the same key file multiple times = same result.
    - Uses Delta MERGE for atomic, distributed operation.
    - Key file is never modified.
    - is_deleted column is created in Bronze table if it doesn't exist.
    """
    target_table_fqn = qualified_table_name(catalog, schema, table)
    metadata = {
        "flow_id": flow_id,
        "key_file_path": key_file_path,
        "target_table": target_table_fqn,
        "rows_in_key_file": 0,
        "rows_marked_for_deletion": 0,
        "rows_matched_in_bronze": 0,
        "rows_updated": 0,
        "status": "success",
    }

    try:
        # Step 1: Read key file (READ-ONLY)
        logger.info(f"[{flow_id}] Reading key file from {key_file_path}")
        try:
            # Try as Delta table first (in case it's a Delta path)
            try:
                key_df = spark.read.table(key_file_path)
            except Exception:
                # Fall back to parquet/csv file read
                if key_file_path.endswith('.parquet'):
                    key_df = spark.read.parquet(key_file_path)
                elif key_file_path.endswith('.csv'):
                    key_df = spark.read.option("inferSchema", "true").csv(key_file_path)
                else:
                    key_df = spark.read.parquet(key_file_path)
        except Exception as exc:
            raise FrameworkConfigError(
                f"[{flow_id}] Failed to read key file '{key_file_path}': {exc}"
            ) from exc

        metadata["rows_in_key_file"] = key_df.count()

        # Step 2: Filter to rows marked for deletion
        logger.info(
            f"[{flow_id}] Filtering to deletion markers "
            f"(where {key_file_deleted_indicator_column} = true)"
        )
        try:
            deleted_keys_df = key_df.filter(
                F.col(key_file_deleted_indicator_column) == True  # noqa: E712
            )
        except Exception as exc:
            raise FrameworkConfigError(
                f"[{flow_id}] Column '{key_file_deleted_indicator_column}' not found "
                f"in key file or type mismatch: {exc}"
            ) from exc

        deleted_keys_count = deleted_keys_df.count()
        metadata["rows_marked_for_deletion"] = deleted_keys_count

        if deleted_keys_count == 0:
            logger.info(f"[{flow_id}] No rows marked for deletion in key file. Skipping.")
            metadata["status"] = "skipped"
            return metadata

        # Step 3: Select only primary key columns from key file
        logger.info(f"[{flow_id}] Extracting primary keys: {primary_keys}")
        try:
            deleted_keys_df = deleted_keys_df.select(primary_keys).distinct()
        except Exception as exc:
            raise FrameworkConfigError(
                f"[{flow_id}] Could not extract primary keys {primary_keys} from key file: {exc}"
            ) from exc

        # Step 4: Read Bronze table to check schema
        logger.info(f"[{flow_id}] Reading Bronze table {target_table_fqn}")
        try:
            bronze_df = spark.read.table(target_table_fqn)
        except Exception as exc:
            raise FrameworkConfigError(
                f"[{flow_id}] Could not read target table {target_table_fqn}: {exc}"
            ) from exc

        # Step 5: Validate primary keys exist in Bronze
        bronze_columns = set(bronze_df.columns)
        missing_keys = set(primary_keys) - bronze_columns
        if missing_keys:
            raise FrameworkConfigError(
                f"[{flow_id}] Primary key columns not found in {target_table_fqn}: {missing_keys}. "
                f"Available columns: {bronze_columns}"
            )

        # Step 6: Check if is_deleted column exists; create if not
        if "is_deleted" not in bronze_columns:
            logger.info(f"[{flow_id}] is_deleted column not found. Adding it to {target_table_fqn}")
            spark.sql(
                f"ALTER TABLE {target_table_fqn} ADD COLUMNS (is_deleted BOOLEAN)"
            )

        # Step 7: Perform Delta MERGE to soft-delete matched rows
        logger.info(f"[{flow_id}] Executing Delta MERGE to apply soft-deletes")
        merge_sql = _build_merge_sql(
            target_table_fqn, primary_keys, deleted_keys_df
        )

        try:
            spark.sql(merge_sql)
            logger.info(f"[{flow_id}] Delta MERGE completed successfully")
        except Exception as exc:
            raise FrameworkConfigError(
                f"[{flow_id}] Delta MERGE failed: {exc}\nSQL: {merge_sql}"
            ) from exc

        # Step 8: Count rows that were actually updated (for observability)
        # Note: Delta MERGE doesn't easily return row counts, so we count matches post-hoc
        try:
            # Count rows in Bronze that match deleted keys AND have is_deleted = true
            match_condition = _build_match_condition(primary_keys)
            matched_count_sql = f"""
            SELECT COUNT(*) as cnt
            FROM {target_table_fqn} t
            INNER JOIN (SELECT {', '.join(primary_keys)} FROM {deleted_keys_df.createOrReplaceTempView('_deleted_keys')})
            WHERE {match_condition}
            """
            # Simpler approach: just count rows with is_deleted = true
            soft_deleted_count = spark.sql(
                f"SELECT COUNT(*) as cnt FROM {target_table_fqn} WHERE is_deleted = true"
            ).collect()[0]["cnt"]
            metadata["rows_updated"] = soft_deleted_count
        except Exception:
            # If counting fails, still succeed with the MERGE (it completed)
            logger.warning(f"[{flow_id}] Could not count soft-deleted rows, continuing")
            metadata["rows_updated"] = -1  # Unknown

        logger.info(
            f"[{flow_id}] Soft-delete complete. "
            f"Rows marked for deletion: {deleted_keys_count}, "
            f"Total soft-deleted rows in target: {metadata['rows_updated']}"
        )

    except FrameworkConfigError:
        raise
    except Exception as exc:
        metadata["status"] = "failed"
        metadata["error_message"] = str(exc)
        logger.error(f"[{flow_id}] Unexpected error in soft-delete processor: {exc}", exc_info=True)
        raise FrameworkConfigError(
            f"[{flow_id}] Soft-delete processor failed: {exc}"
        ) from exc

    return metadata


def _build_merge_sql(
    target_table_fqn: str,
    primary_keys: List[str],
    deleted_keys_df,
) -> str:
    """Build Delta MERGE SQL for soft-deleting matched rows.

    Parameters
    ----------
    target_table_fqn : str
        Fully-qualified target table name.
    primary_keys : List[str]
        Primary key column names.
    deleted_keys_df
        Spark DataFrame containing primary key values to delete.

    Returns
    -------
    str
        Complete MERGE SQL statement.
    """
    # Create temporary view for deleted keys (Spark handles this internally)
    temp_view = "_soft_delete_temp_keys"
    deleted_keys_df.createOrReplaceTempView(temp_view)

    # Build ON clause: match on all primary keys
    on_clauses = [f"t.{key} = k.{key}" for key in primary_keys]
    on_condition = " AND ".join(on_clauses)

    merge_sql = f"""
    MERGE INTO {target_table_fqn} t
    USING {temp_view} k
    ON {on_condition}
    WHEN MATCHED THEN UPDATE SET t.is_deleted = true
    """

    return merge_sql


def _build_match_condition(primary_keys: List[str]) -> str:
    """Build WHERE condition for matching primary keys across two tables.

    Parameters
    ----------
    primary_keys : List[str]
        Column names.

    Returns
    -------
    str
        SQL condition like "t.key1 = k.key1 AND t.key2 = k.key2".
    """
    return " AND ".join(f"t.{key} = k.{key}" for key in primary_keys)
