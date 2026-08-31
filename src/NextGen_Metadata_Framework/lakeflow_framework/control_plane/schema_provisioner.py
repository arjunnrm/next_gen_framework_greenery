"""Idempotent control-schema/table provisioning, shared by the setup notebook and onboarding.

`01_setup_control_tables.py` is still the dedicated, verbose "run this once" experience.
This module exists so the onboarding engine can *also* guarantee the control tables exist
before it tries to upsert into them -- e.g. a brand-new environment where nobody has run
setup yet -- without duplicating the DDL text (both call into `ddl_definitions`).
"""

import logging
from typing import Dict, Optional

from pyspark.sql import SparkSession

from NextGen_Metadata_Framework.lakeflow_framework.control_plane.ddl_definitions import (
    ADDITIVE_CONTROL_TABLE_COLUMNS,
    get_add_column_ddl,
    get_all_control_table_ddls,
    get_preflight_function_ddl,
    get_schema_ddl,
)
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.control_plane.schema_provisioner")

DEFAULT_CONTROL_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def ensure_control_schema_exists(
    spark: SparkSession, control_catalog: str, table_properties: Optional[Dict[str, str]] = None
) -> None:
    """Create the ``config`` schema and all four control tables if they don't already exist.

    Every statement is ``CREATE ... IF NOT EXISTS``, so this is safe to call on every
    onboarding run (or any other entrypoint) without risk of clobbering existing data.

    Parameters
    ----------
    spark:
        Active SparkSession.
    control_catalog:
        Catalog that should contain the ``config`` schema.
    table_properties:
        Delta table properties applied to newly-created tables; defaults to
        ``DEFAULT_CONTROL_TABLE_PROPERTIES`` (autoOptimize write/compact).

    Raises
    ------
    FrameworkConfigError
        If schema or table creation fails.
    """
    table_properties = table_properties or DEFAULT_CONTROL_TABLE_PROPERTIES
    control_schema = f"{control_catalog}.config"

    try:
        spark.sql(get_schema_ddl(control_schema))
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to ensure schema '{control_schema}' exists: {exc}") from exc

    for description, ddl_statement in get_all_control_table_ddls(control_schema, table_properties):
        try:
            spark.sql(ddl_statement)
        except Exception as exc:  # noqa: BLE001
            if is_already_exists_race(exc):
                logger.info("Control object already created concurrently (%s); treating as success.", description)
                continue
            raise FrameworkConfigError(f"Failed to ensure control table exists ({description}): {exc}") from exc

    ensure_control_table_columns(spark, control_catalog)

    logger.info("Verified/created control schema and tables under '%s'", control_schema)


def ensure_control_table_columns(spark: SparkSession, control_catalog: str) -> None:
    """Add any post-v1.0 control-table column that an already-provisioned table is missing.

    The ``CREATE TABLE IF NOT EXISTS`` statements in ``get_all_control_table_ddls`` are a no-op
    against a table that already exists, so a column added to one of them reaches NEW
    installations only. This closes that gap for existing ones, and is called from
    ``ensure_control_schema_exists`` so every entrypoint that provisions also migrates.

    Strictly additive: the only statement issued is ``ALTER TABLE ... ADD COLUMNS``, which never
    rewrites data and leaves every existing row's new column NULL -- which is exactly what each
    of these columns documents as its default (``execution_mode`` NULL means ``"job"``, so an
    already-onboarded flow keeps its current behaviour rather than silently switching to a mode
    nobody asked for).

    Idempotence is enforced here rather than by the statement: Databricks SQL rejects
    ``ADD COLUMNS IF NOT EXISTS`` outright with ``PARSE_SYNTAX_ERROR`` (verified live on
    2026-08-31), so this function skips any column already present and additionally swallows the
    lost-race error, for two setup runs racing each other.

    A missing TABLE is not an error here: ``ensure_control_schema_exists`` has just run the
    CREATE statements, and a table absent after that means a permission problem that its own
    error already reported more precisely.
    """
    control_schema = f"{control_catalog}.config"

    for table_name, columns in ADDITIVE_CONTROL_TABLE_COLUMNS.items():
        qualified = f"{control_schema}.{table_name}"
        try:
            existing = {field.lower() for field in spark.table(qualified).columns}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping column migration for %s (cannot read it): %s", qualified, exc)
            continue

        for column_name, sql_type, comment in columns:
            if column_name.lower() in existing:
                continue
            statement = get_add_column_ddl(control_schema, table_name, column_name, sql_type, comment)
            try:
                spark.sql(statement)
                logger.info("Added missing column %s.%s (%s)", qualified, column_name, sql_type)
            except Exception as exc:  # noqa: BLE001
                if is_already_exists_race(exc) or _is_duplicate_column_race(exc):
                    logger.info("Column %s.%s added concurrently; treating as success.", qualified, column_name)
                    continue
                raise FrameworkConfigError(f"Failed to add column {qualified}.{column_name}: {exc}") from exc


#: Unity Catalog error conditions meaning "another session created this object first".
#: ``CREATE OR REPLACE FUNCTION`` is idempotent in intent but NOT atomic in Unity Catalog: when two
#: sessions issue it concurrently, the loser's managed-catalog call still raises
#: ``FunctionAlreadyExistsException`` / ``[ROUTINE_ALREADY_EXISTS]``. Observed live on 2026-08-29,
#: when three concurrent test jobs each ran ``01_setup_control_tables`` and one died with
#: ``Cannot create the routine `metaflow`.`config`.`preflight_check_onboarding_spec` because a
#: routine of that name already exists`` -- taking every downstream task with it (TC-CDC-007).
#:
#: Swallowing exactly these conditions restores the idempotence the DDL already intends. It is
#: narrow on purpose: a permission error, a bad schema or a syntax error in the function body must
#: still fail loudly, because those mean the object is genuinely NOT in the state we require.
_ALREADY_EXISTS_CONDITIONS = (
    "ROUTINE_ALREADY_EXISTS",
    "FUNCTIONALREADYEXISTSEXCEPTION",
    "TABLE_OR_VIEW_ALREADY_EXISTS",
    "SCHEMA_ALREADY_EXISTS",
)


#: Unity Catalog / Delta error conditions meaning "this column was added concurrently".
#:
#: Narrow on purpose, and deliberately NOT a substring test for the bare phrase "ALREADY EXISTS":
#: that phrase also appears in unrelated failures (a TABLE already existing where a different
#: object was expected, for instance), and swallowing one of those would turn a genuine
#: provisioning error into a silent no-op -- leaving the column absent and the next write failing
#: with the ``UNRESOLVED_COLUMN`` this whole migration exists to prevent.
#: Both the SQLSTATE-style condition names and Delta's prose form are listed, because which one
#: surfaces depends on the runtime: DBR raises ``[FIELDS_ALREADY_EXIST]`` while the Delta library
#: itself raises the plain sentence "Column already exists in the target table".
_DUPLICATE_COLUMN_CONDITIONS = (
    "FIELD_ALREADY_EXISTS",
    "FIELDS_ALREADY_EXIST",
    "COLUMN_ALREADY_EXISTS",
    "COLUMN ALREADY EXISTS",
)


def _is_duplicate_column_race(exc: Exception) -> bool:
    """True when ``exc`` means "another session already added this column"."""
    rendered = str(exc).upper()
    return any(condition in rendered for condition in _DUPLICATE_COLUMN_CONDITIONS)


def is_already_exists_race(exc: Exception) -> bool:
    """True when ``exc`` means "another session created this object concurrently"."""
    rendered = str(exc).upper()
    return any(condition in rendered for condition in _ALREADY_EXISTS_CONDITIONS)


def ensure_preflight_function_exists(spark: SparkSession, control_catalog: str, onboarding_spec_schema_json: str) -> None:
    """Create/replace the ``preflight_check_onboarding_spec`` Unity Catalog Python Function in
    ``<control_catalog>.config``, so it's directly SQL/Genie/MCP-tool-callable (see
    ``ddl_definitions.get_preflight_function_ddl`` for exactly what it checks and why it's a
    structural-only sibling of ``onboarding/uc_spec_preflight.py``'s full Python tool).

    Requires the ``config`` schema to already exist -- call after :func:`ensure_control_schema_exists`
    (or ``01_setup_control_tables.py``'s schema-creation step), not before.

    Parameters
    ----------
    spark:
        Active SparkSession.
    control_catalog:
        Catalog that contains the ``config`` schema, e.g. ``"metaflow"``.
    onboarding_spec_schema_json:
        Full text of ``onboarding_templates/onboarding_spec.schema.json``.

    Raises
    ------
    FrameworkConfigError
        If function creation fails.
    """
    control_schema = f"{control_catalog}.config"
    try:
        spark.sql(get_preflight_function_ddl(control_schema, onboarding_spec_schema_json))
    except Exception as exc:  # noqa: BLE001
        if is_already_exists_race(exc):
            # Another session won the race and the function now exists -- which is precisely the
            # post-condition this function promises. See _ALREADY_EXISTS_CONDITIONS.
            logger.info(
                "UC function '%s.preflight_check_onboarding_spec' was created concurrently by another "
                "session; treating as success.", control_schema,
            )
            return
        raise FrameworkConfigError(
            f"Failed to create/replace UC function '{control_schema}.preflight_check_onboarding_spec': {exc}"
        ) from exc

    logger.info("Verified/created UC function '%s.preflight_check_onboarding_spec'", control_schema)
