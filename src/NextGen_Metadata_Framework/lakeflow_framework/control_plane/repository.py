"""Read access to the active dataflow-group / ingestion-flow / transformation-flow / pipeline-mode
reconciliation-flow metadata.

This is what ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` calls -- exactly once,
right after resolving ``dataflow.group.id``/``dataflow.control.catalog`` from the pipeline's own
Spark conf -- to pull everything the engine needs out of the control tables in one round trip:
the group row (whether it has ingestion/transformation flows, its ``pipeline_parameters_json``)
plus every active row in ``ingestion_flow_spec``/``transformation_flow_spec`` for that group, plus
every active ``reconciliation_flow_spec`` row whose ``execution_mode`` is NOT ``"job"`` (the
standalone job-task reconciliation path keeps reading job-mode rows itself, unchanged).
Only ``is_active`` rows are read, so soft-disabling a flow (flipping ``is_active`` to ``false``
via re-onboarding) removes it from the next pipeline update without a DDL change.

Deliberately fails loudly rather than degrading: a missing group row, an unreadable control
table, or a group with zero active flows of any of these three kinds all raise
``FrameworkConfigError`` immediately, since a Lakeflow pipeline update with nothing to build is
virtually always a misconfiguration (wrong ``dataflow_group_id``, or every flow accidentally left
inactive) rather than a legitimate empty state.
"""

import logging
from typing import Any, List, NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("common.control_plane.repository")


class GroupMetadata(NamedTuple):
    """The active group row plus its active ingestion/transformation/reconciliation flow rows."""

    group_row: Any
    ingestion_rows: List[Any]
    transformation_rows: List[Any]
    reconciliation_rows: List[Any]


def load_active_group_metadata(spark: SparkSession, control_catalog: str, group_id: str) -> GroupMetadata:
    """Load the active group row plus its active ingestion/transformation/reconciliation flow rows.

    Parameters
    ----------
    spark:
        Active SparkSession.
    control_catalog:
        Catalog containing the ``config`` schema with the four control tables.
    group_id:
        The ``dataflow_group_id`` to resolve.

    Returns
    -------
    GroupMetadata
        ``(group_row, ingestion_rows, transformation_rows, reconciliation_rows)``. Only
        pipeline-mode reconciliation rows (``execution_mode`` other than ``"job"``/absent/``None``)
        are included in ``reconciliation_rows`` -- job-mode rows belong to the standalone
        reconciliation job engine, not this pipeline loader.

    Raises
    ------
    FrameworkConfigError
        If no active group row exists, if reading a control table fails, or if ingestion,
        transformation AND pipeline-mode reconciliation rows are all empty (nothing to run).
    """
    control_schema = f"{control_catalog}.config"

    try:
        group_rows = (
            spark.table(f"{control_schema}.dataflow_group_spec")
            .filter((F.col("dataflow_group_id") == group_id) & (F.col("is_active")))
            .collect()
        )
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to read dataflow_group_spec for group '{group_id}': {exc}") from exc

    if not group_rows:
        raise FrameworkConfigError(f"No active dataflow_group_spec row found for dataflow_group_id='{group_id}'")
    group_row = group_rows[0]

    try:
        ingestion_rows = (
            spark.table(f"{control_schema}.ingestion_flow_spec")
            .filter((F.col("dataflow_group_id") == group_id) & (F.col("is_active")))
            .collect()
        )
        transformation_rows = (
            spark.table(f"{control_schema}.transformation_flow_spec")
            .filter((F.col("dataflow_group_id") == group_id) & (F.col("is_active")))
            .collect()
        )
        recon_rows = (
            spark.table(f"{control_schema}.reconciliation_flow_spec")
            .filter((F.col("dataflow_group_id") == group_id) & (F.col("is_active")))
            .collect()
        )
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to read flow specs for group '{group_id}': {exc}") from exc

    # Filtered in Python, never via a Spark .filter(F.col("execution_mode") ...): an
    # already-provisioned reconciliation_flow_spec table may still lack this column entirely
    # (01_setup only ever runs CREATE TABLE IF NOT EXISTS), so a row with no execution_mode
    # attribute at all must be treated as the "job" default rather than erroring.
    reconciliation_rows = [r for r in recon_rows if (getattr(r, "execution_mode", None) or "job") != "job"]

    if not ingestion_rows and not transformation_rows and not reconciliation_rows:
        raise FrameworkConfigError(
            f"dataflow_group_id='{group_id}' has no active ingestion, transformation or pipeline-mode reconciliation flows."
        )

    logger.info(
        "Resolved group '%s': %d ingestion flow(s), %d transformation flow(s), %d pipeline-mode reconciliation flow(s)",
        group_id,
        len(ingestion_rows),
        len(transformation_rows),
        len(reconciliation_rows),
    )
    return GroupMetadata(
        group_row=group_row,
        ingestion_rows=ingestion_rows,
        transformation_rows=transformation_rows,
        reconciliation_rows=reconciliation_rows,
    )
