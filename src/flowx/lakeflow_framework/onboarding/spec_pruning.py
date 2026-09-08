"""Deactivate control rows for flows a re-onboarded spec no longer declares (opt-in).

Why this exists
---------------
``metadata_upsert.py`` only ever writes ``is_active = true``: it MERGEs the flows a spec declares
and never touches the rows it does not. So deleting a flow from a spec and re-onboarding leaves the
old row active, and ``load_active_group_metadata`` keeps handing it to the pipeline -- the removed
flow keeps registering its datasets, keeps running, and keeps succeeding or failing on behalf of a
document that no longer describes it. That is AGENTS.md's "removals are rejected, never ignored"
in its worst form: the removal is neither rejected nor ignored but silently *retained*. Observed on
2026-09-03 (a deleted reconciliation flow failing every update from its stale row) and again on
2026-09-08 (UC6 replacing six reconciliation flows with one gate MV).

What it does
------------
For ONE dataflow group, set ``is_active = false`` on every active row in ``ingestion_flow_spec``,
``transformation_flow_spec`` and ``reconciliation_flow_spec`` whose id is NOT in the spec being
onboarded. Soft-disable only -- rows are never deleted, so history and audit joins survive and the
flow can be revived by re-adding it to the spec.

Why opt-in
----------
A ``reconciliation_flows[]`` entry may name a *different* ``dataflow_group_id`` than the spec that
carries it (the supported way to reconcile against another group's tables). Pruning by group id
cannot tell that row from a genuinely removed one, so re-onboarding group X with pruning on would
deactivate a recon flow that spec Y still declares (until Y is re-onboarded, which reactivates it).
Groups owned by exactly one spec -- every BT_Usecase group -- can turn it on safely; the job
parameter ``prune_missing_flows`` (default ``false``) is the switch, per onboarding run.

The SQL is built by a pure function (``build_flow_deactivation_statements``) so the exact statements
are unit-testable without Spark; ``deactivate_flows_absent_from_spec`` executes them.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from flowx.lakeflow_framework.crypto.secrets import assert_safe_identifier

logger = logging.getLogger(__name__)

#: (control table, its flow-id column) -- the three flow tables a spec declares rows in.
FLOW_TABLES: Tuple[Tuple[str, str], ...] = (
    ("ingestion_flow_spec", "dataflow_id"),
    ("transformation_flow_spec", "flow_step_id"),
    ("reconciliation_flow_spec", "reconciliation_id"),
)


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _qualified_control_schema(control_schema: str) -> str:
    parts = control_schema.split(".")
    if len(parts) != 2:
        raise ValueError(f"control_schema must be '<catalog>.<schema>', got {control_schema!r}")
    return ".".join(assert_safe_identifier(part, "control_schema") for part in parts)


def _predicate(dataflow_group_id: str, id_column: str, keep_ids: Sequence[str]) -> str:
    clauses = [f"dataflow_group_id = {_sql_string(dataflow_group_id)}", "is_active = true"]
    if keep_ids:
        clauses.append(f"{id_column} NOT IN ({', '.join(_sql_string(i) for i in keep_ids)})")
    return " AND ".join(clauses)


def build_flow_deactivation_statements(
    control_schema: str,
    dataflow_group_id: str,
    ingestion_ids: Iterable[str],
    transformation_ids: Iterable[str],
    reconciliation_ids: Iterable[str],
) -> List[Tuple[str, str, str]]:
    """Return ``(table, count_sql, update_sql)`` per flow table.

    ``count_sql`` selects how many rows the update will touch (for the log line); ``update_sql``
    soft-disables them. An EMPTY id list for a kind means the spec declares no flows of that kind
    any more, so every active row of that kind for the group is deactivated -- exactly the UC6
    "reconciliation_flows removed" case.
    """
    schema = _qualified_control_schema(control_schema)
    keep = {
        "ingestion_flow_spec": list(ingestion_ids),
        "transformation_flow_spec": list(transformation_ids),
        "reconciliation_flow_spec": list(reconciliation_ids),
    }
    statements: List[Tuple[str, str, str]] = []
    for table, id_column in FLOW_TABLES:
        where = _predicate(dataflow_group_id, id_column, keep[table])
        statements.append(
            (
                table,
                f"SELECT count(*) AS n FROM {schema}.{table} WHERE {where}",
                f"UPDATE {schema}.{table} SET is_active = false, updated_at = current_timestamp() WHERE {where}",
            )
        )
    return statements


def deactivate_flows_absent_from_spec(
    spark: Any,
    control_schema: str,
    dataflow_group_id: str,
    ingestion_flows: Sequence[Dict[str, Any]],
    transformation_flows: Sequence[Dict[str, Any]],
    reconciliation_flows: Sequence[Dict[str, Any]],
) -> Dict[str, int]:
    """Soft-disable this group's control rows for flows the spec no longer declares.

    Returns ``{table: rows_deactivated}``. Reconciliation rows whose ``dataflow_group_id`` column
    is NULL (legacy job-mode rows) are never matched and therefore never touched.
    """
    statements = build_flow_deactivation_statements(
        control_schema,
        dataflow_group_id,
        [f["dataflow_id"] for f in ingestion_flows],
        [f["flow_step_id"] for f in transformation_flows],
        [f["reconciliation_id"] for f in reconciliation_flows],
    )
    deactivated: Dict[str, int] = {}
    for table, count_sql, update_sql in statements:
        count = int(spark.sql(count_sql).collect()[0][0])
        if count:
            spark.sql(update_sql)
            logger.warning(
                "prune_missing_flows: deactivated %d %s row(s) of group '%s' that the spec no longer declares",
                count,
                table,
                dataflow_group_id,
            )
        else:
            logger.info("prune_missing_flows: %s has no stale rows for group '%s'", table, dataflow_group_id)
        deactivated[table] = count
    return deactivated
