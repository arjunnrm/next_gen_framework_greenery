"""Insert/update/delete row counts for a CDC-strategy target, via Delta Change Data Feed.

``dlt.apply_changes``/``apply_changes_from_snapshot`` don't expose a per-update
insert/update/delete breakdown as a return value, but Delta's native Change Data Feed
(``table_changes()``) does, exactly and cheaply, once ``delta.enableChangeDataFeed = true``
is set on the target (``storage/table_properties.py``, gated to every CDC-dispatched
strategy). This module queries it after a pipeline update completes rather than
implementing a manual before/after diff, which would double-scan the table for the same
answer Delta already tracks natively.
"""

import logging
from typing import Any, Dict

from flowx.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("common.cdc.change_metrics")

_CHANGE_TYPE_TO_METRIC = {
    "insert": "inserted_count",
    "update_postimage": "updated_count",
    "delete": "deleted_count",
}


def capture_scd_change_counts(
    spark: Any,
    qualified_table: str,
    starting_version: int,
    ending_version: int,
) -> Dict[str, int]:
    """Return ``{"inserted_count", "updated_count", "deleted_count"}`` for one CDC target.

    Queries ``table_changes(qualified_table, starting_version, ending_version)`` and
    aggregates row counts by ``_change_type``. ``update_preimage`` rows are intentionally
    excluded from the count (each update produces one ``update_preimage`` + one
    ``update_postimage`` row in the feed; counting only the postimage avoids double-counting
    a single logical update).

    Parameters
    ----------
    spark:
        An active ``SparkSession``/``DatabricksSession``.
    qualified_table:
        Fully-qualified ``catalog.schema.table`` name (see
        ``storage/table_properties.py::qualified_table_name``).
    starting_version, ending_version:
        Delta commit version range to inspect -- typically the target table's version
        immediately before this pipeline update started, and its version immediately after.

    Returns
    -------
    dict
        Always contains all three keys, defaulting unseen ``_change_type`` values to ``0``.

    Raises
    ------
    FrameworkConfigError
        If the Change Data Feed query fails (e.g. CDF not enabled on this table, or the
        version range is out of the table's retained history).
    """
    try:
        changes_df = spark.sql(
            f"SELECT _change_type, COUNT(*) AS row_count FROM table_changes('{qualified_table}', "
            f"{int(starting_version)}, {int(ending_version)}) GROUP BY _change_type"
        )
        counts = {"inserted_count": 0, "updated_count": 0, "deleted_count": 0}
        for row in changes_df.collect():
            metric = _CHANGE_TYPE_TO_METRIC.get(row["_change_type"])
            if metric:
                counts[metric] = row["row_count"]
        return counts
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(
            f"Failed to capture SCD change counts for '{qualified_table}' (versions "
            f"{starting_version}->{ending_version}): {exc}"
        ) from exc
