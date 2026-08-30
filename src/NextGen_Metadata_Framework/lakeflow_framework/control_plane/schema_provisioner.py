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

    logger.info("Verified/created control schema and tables under '%s'", control_schema)


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
