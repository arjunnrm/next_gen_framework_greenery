"""Soft-delete feature for Bronze tables using external key files.

This module provides metadata-driven soft-delete capability for ingestion flows.
A separate key file (per table) contains primary keys marked for deletion, and the
framework applies soft-delete markers (is_deleted = true) to matching rows in the
Bronze table during pipeline execution.

Key features:
- Metadata-driven via soft_delete_config in target_config
- Read-only external key files (one per table)
- Idempotent execution (safe to run multiple times per cycle)
- Automatic per-pipeline-run via orchestration
- Spark-native Delta MERGE operation for scalability

See processor.py for the main apply_soft_deletes() function.
"""

__all__ = ["apply_soft_deletes"]

from flowx.lakeflow_framework.soft_delete.processor import apply_soft_deletes
