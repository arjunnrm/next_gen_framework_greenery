"""Unity Catalog governance: tags-only model (v2 schema, replaces governance/abac.py).

**Design**: the framework applies key-value tags to columns and tables -- it does **not**
create or administer the Unity Catalog masking/row-filter policies that give a tag its
actual enforcement behavior. That's a workspace admin's Unity Catalog tag-policy
configuration, external to this repo. A column tag like ``mask=PII`` or a table tag like
``row_filter=region_restricted`` is a declarative label; what (if anything) enforces
masking/filtering based on that label is out of scope here.

**Important**: ``ALTER TABLE ... SET TAGS`` / ``ALTER TABLE ... ALTER COLUMN ... SET TAGS``
are Unity Catalog DDL operations against a materialized table. They must be invoked *after*
a Lakeflow Declarative Pipeline update has created/updated the target table -- never from
inside the pipeline's own graph-definition code. Call :func:`apply_all_governance_tags` (this module's group-level entrypoint) from a
downstream orchestration step -- it is what ``notebooks/04_governance/
04_apply_governance_and_egress.py`` invokes as its own job task. :func:`apply_governance_tags`
is the single-table primitive underneath it.

**Module ownership (v1.7.x)**: ``apply_all_governance_tags`` lives HERE, with the rest of the
governance model, rather than in ``control_plane/post_deployment.py`` where it originally sat
next to unrelated CDC change-count capture. Governance tagging is a governance concern: it reads
``governance_tags_json`` and emits tag DDL, and shares nothing with the CDC watermark logic it
used to be filed beside. ``post_deployment`` re-exports it for backward compatibility, so both
import paths keep working.

**Verifying that tags landed -- ALWAYS qualify the catalog.** ``information_schema`` is
**catalog-scoped**, so an unqualified ``SELECT ... FROM information_schema.column_tags`` resolves
against ``current_catalog()``. A SQL warehouse with no default catalog puts every session in
``workspace``, where a freshly tagged table in another catalog does not appear -- the query
truthfully returns 0 rows for the wrong catalog. This produced a real, long-lived misdiagnosis
("tags are not supported on this workspace") when tags had in fact been applied correctly all
along. Use one of::

    SELECT * FROM <catalog>.information_schema.column_tags WHERE schema_name = '...';
    SELECT * FROM system.information_schema.column_tags;   -- metastore-wide

There is no ``SHOW TAGS`` statement in Databricks SQL; its ``PARSE_SYNTAX_ERROR`` says nothing
about whether tagging works.

**Idempotency**: unlike the v1 ABAC-policy-binding model (which needed an idempotency
ledger, since re-issuing ``SET ROW FILTER``/``SET MASK`` with a *different* function could
silently rebind), tag DDL is naturally idempotent -- re-applying an identical
key-value pair is a no-op, and applying a *changed* value for an existing key simply
overwrites it. No ledger table is needed (see docs/23 for the removal rationale).
"""

import json
import logging
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from flowx.lakeflow_framework.crypto.secrets import assert_safe_identifier
from flowx.lakeflow_framework.exceptions import AbacApplicationError

logger = logging.getLogger("flowx.lakeflow_framework.governance.tags")


def _render_tags_clause(tags: Dict[str, str]) -> str:
    """Render a tag key/value dict as a SQL ``SET TAGS (...)`` clause body.

    Tag keys/values are user-supplied strings, so they're SQL-escaped (single quotes
    doubled) rather than assumed to be safe identifiers -- a tag value like ``"O'Brien's
    team"`` is a plausible legitimate value, unlike a column/table identifier.
    """
    rendered = ", ".join(f"'{key.replace(chr(39), chr(39) * 2)}' = '{value.replace(chr(39), chr(39) * 2)}'" for key, value in tags.items())
    return f"({rendered})"


def apply_governance_tags(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    governance_tags: Dict[str, Any],
) -> None:
    """Apply every configured column tag and table tag to a target Delta table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    catalog, schema, table:
        Fully-qualifying coordinates of the target table.
    governance_tags:
        Dict shaped like::

            {
              "column_tags": [
                {"column": "ssn", "tags": {"mask": "PII", "classification": "restricted"}}
              ],
              "table_tags": {"row_filter": "region_restricted", "domain": "finance"}
            }

        Both ``column_tags[].tags`` and ``table_tags`` are free-form key-value maps --
        multiple tags per column and multiple tags per table are supported natively (Unity
        Catalog's ``SET TAGS`` clause accepts any number of key/value pairs in one
        statement; only *multiple columns* require separate ``ALTER TABLE`` statements).

    Notes
    -----
    Each column's tags (and the table's tags) are applied independently and best-effort: a
    failure on one does not prevent the others from being attempted. All collected failures
    are raised together at the end via a single ``AbacApplicationError``.

    Raises
    ------
    AbacApplicationError
        If one or more tag applications fail (identifies the exact column/table and DDL
        statement that failed).
    """
    catalog = assert_safe_identifier(catalog, "catalog")
    schema = assert_safe_identifier(schema, "schema")
    table = assert_safe_identifier(table, "table")
    qualified_table = f"{catalog}.{schema}.{table}"

    errors: List[str] = []

    table_tags = governance_tags.get("table_tags") or {}
    if table_tags:
        try:
            statement = f"ALTER TABLE {qualified_table} SET TAGS {_render_tags_clause(table_tags)}"
            logger.info("Applying table tags on %s: %s", qualified_table, statement)
            spark.sql(statement)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"table_tags {table_tags} on {qualified_table}: {exc}")
            logger.error("Failed to apply table tags %s on %s: %s", table_tags, qualified_table, exc)

    for column_tag_entry in governance_tags.get("column_tags", []) or []:
        column = column_tag_entry.get("column", "<missing column>")
        tags = column_tag_entry.get("tags") or {}
        if not tags:
            continue
        try:
            column = assert_safe_identifier(column_tag_entry["column"], "column")
            statement = f"ALTER TABLE {qualified_table} ALTER COLUMN `{column}` SET TAGS {_render_tags_clause(tags)}"
            logger.info("Applying column tags on %s.%s: %s", qualified_table, column, statement)
            spark.sql(statement)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"column_tags {column_tag_entry} on {qualified_table}: {exc}")
            logger.error("Failed to apply column tags %s on %s: %s", column_tag_entry, qualified_table, exc)

    if errors:
        raise AbacApplicationError(f"{len(errors)} governance tag application failure(s) on {qualified_table}: {errors}")

    logger.info("Successfully processed all governance tags for %s", qualified_table)


def apply_all_governance_tags(spark: SparkSession, control_catalog: str, group_id: str) -> None:
    """Apply governance tags (column + table) for every active flow in ``group_id``.

    The group-level entrypoint: loads the group's active control-table metadata, and for each
    ingestion/transformation flow carrying a non-empty ``governance_tags_json`` delegates to
    :func:`apply_governance_tags`. Flows with no governance block are skipped silently -- tagging
    is opt-in per flow.

    This is a genuine *post-deployment* step and needs its own orchestration task. It issues
    ``ALTER TABLE ... SET TAGS`` DDL against an already-materialized table, so it must run AFTER
    the pipeline update that creates that table -- a Lakeflow pipeline cannot apply it from
    inside its own graph-definition code. A pipeline that runs green with no tagging task applies
    NO tags and reports no error, so verify the effect in ``information_schema.column_tags``
    rather than inferring it from a successful run.

    Tag DDL is naturally idempotent -- see this module's docstring for why no idempotency ledger
    is needed (v1's ``governance_applied_log`` is removed in the v2 schema).

    Parameters
    ----------
    spark:
        Active session.
    control_catalog:
        Catalog holding the framework's control tables.
    group_id:
        ``dataflow_group_id`` whose active flows should be tagged.
    """
    # Imported lazily: control_plane.post_deployment re-exports this function, so a module-level
    # import of the control plane here would create a governance <-> control_plane import cycle.
    from flowx.lakeflow_framework.control_plane.repository import load_active_group_metadata

    md = load_active_group_metadata(spark, control_catalog, group_id)

    for flow_row in list(md.ingestion_rows) + list(md.transformation_rows):
        governance_tags_json = getattr(flow_row, "governance_tags_json", None)
        if not governance_tags_json:
            continue
        if getattr(flow_row, "target_type", None) == "sink":
            # A "sink" flow never materializes target_table -- it is a dlt.create_sink +
            # append_flow with no persisted dataset (engine/sink_registration.py), so there is
            # nothing to ALTER. Tagging it raised TABLE_OR_VIEW_NOT_FOUND and failed the whole
            # governance task for the group, taking every OTHER flow's tags down with it.
            # "external_sink" is deliberately NOT skipped: it materializes a real main table
            # first and only additionally exports it, so its tags apply as normal.
            logger.info(
                "Skipping governance tags for sink flow '%s' -- a sink has no materialized table to tag",
                getattr(flow_row, "target_table", "?"),
            )
            continue
        governance_tags = json.loads(governance_tags_json)
        if not governance_tags:
            continue
        apply_governance_tags(
            spark,
            flow_row.target_catalog,
            flow_row.target_schema,
            flow_row.target_table,
            governance_tags,
        )
    logger.info("Governance tag application complete for group '%s'", group_id)
