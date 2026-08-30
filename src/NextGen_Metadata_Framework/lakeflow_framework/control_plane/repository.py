"""Read access to the active dataflow-group / ingestion-flow / transformation-flow metadata.

This is what ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` calls -- exactly once,
right after resolving ``dataflow.group.id``/``dataflow.control.catalog`` from the pipeline's own
Spark conf -- to pull everything the engine needs out of the control tables in one round trip:
the group row (whether it has ingestion/transformation flows, its ``pipeline_parameters_json``)
plus every active row in ``ingestion_flow_spec``/``transformation_flow_spec`` for that group.
Only ``is_active`` rows are read, so soft-disabling a flow (flipping ``is_active`` to ``false``
via re-onboarding) removes it from the next pipeline update without a DDL change.

Deliberately fails loudly rather than degrading: a missing group row, an unreadable control
table, or a group with zero active flows of either kind all raise ``FrameworkConfigError``
immediately, since a Lakeflow pipeline update with nothing to build is virtually always a
misconfiguration (wrong ``dataflow_group_id``, or every flow accidentally left inactive) rather
than a legitimate empty state.
"""

import logging
from typing import Any, List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("common.control_plane.repository")


def load_active_group_metadata(spark: SparkSession, control_catalog: str, group_id: str) -> Tuple[Any, List[Any], List[Any]]:
    """Load the active group row plus its active ingestion/transformation flow rows.

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
    tuple
        ``(group_row, ingestion_rows, transformation_rows)``.

    Raises
    ------
    FrameworkConfigError
        If no active group row exists, if reading a control table fails, or if both flow
        lists are empty (nothing to run).
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
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to read flow specs for group '{group_id}': {exc}") from exc

    if not ingestion_rows and not transformation_rows:
        raise FrameworkConfigError(f"dataflow_group_id='{group_id}' has no active ingestion or transformation flows.")

    logger.info(
        "Resolved group '%s': %d ingestion flow(s), %d transformation flow(s)",
        group_id,
        len(ingestion_rows),
        len(transformation_rows),
    )
    return group_row, ingestion_rows, transformation_rows
