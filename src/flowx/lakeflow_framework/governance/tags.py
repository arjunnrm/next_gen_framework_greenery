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
inside the pipeline's own graph-definition code. Call ``apply_governance_tags`` from a
downstream orchestration step -- see ``03_engine/03_lakeflow_declarative_pipeline.py``'s
``apply_all_governance_tags``.

**Idempotency**: unlike the v1 ABAC-policy-binding model (which needed an idempotency
ledger, since re-issuing ``SET ROW FILTER``/``SET MASK`` with a *different* function could
silently rebind), tag DDL is naturally idempotent -- re-applying an identical
key-value pair is a no-op, and applying a *changed* value for an existing key simply
overwrites it. No ledger table is needed (see docs/23 for the removal rationale).
"""

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
