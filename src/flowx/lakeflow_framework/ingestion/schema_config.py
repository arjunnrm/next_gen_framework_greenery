"""Explicit, external schema definition for Bronze/raw ingestion: type mapping, nullability
documentation, Unity Catalog column comments, and source-to-target field renaming.

Follows the same "external file referenced by a `source_config` path field" pattern this
framework already uses for ASN.1 sources (``source_config.asn1_schema_path`` ->
``asn1/decoder.py::derive_asn1_field_defs``) -- ``schema_config_path`` here is that same shape,
generalized to every ``source_type``, pointing at a JSON/YAML file with the shape documented in
:func:`_validate_schema_config_shape` (see ``onboarding_templates/schema_config_example.json``
for a complete worked example and ``docs/28_ingestion_schema_config.md`` for the full guide).

``schema_config_path`` may name an exact file, **or** a directory -- when it's a directory,
:func:`resolve_schema_config_path` picks the file with the most recent modification time
inside it (ties broken by the lexicographically-largest filename, so a
``schema_v1.json``/``schema_v2.json`` naming convention resolves predictably even if both files
happen to share a modification timestamp). This lets an operator drop a new schema version into
a Volume directory without having to also update the onboarding spec's path on every change.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flowx.lakeflow_framework.exceptions import FrameworkConfigError, OnboardingValidationError
from flowx.lakeflow_framework.onboarding.spec_loader import (
    parse_spec_text,
    read_raw_spec_text,
    substitute_environment_placeholders,
)

logger = logging.getLogger("flowx.lakeflow_framework.ingestion.schema_config")


def resolve_schema_config_path(path: str) -> str:
    """Resolve ``schema_config_path`` to one concrete file.

    If ``path`` is already a file, it's returned unchanged. If it's a directory, the file
    directly inside it with the most recent modification time is returned (ties broken by the
    lexicographically-largest filename).

    Raises
    ------
    FrameworkConfigError
        If ``path`` doesn't exist as either a file or a directory, or exists as a directory
        containing no files at all.
    """
    if os.path.isfile(path):
        return path
    if not os.path.isdir(path):
        raise FrameworkConfigError(f"schema_config_path '{path}' does not exist (checked as both a file and a directory).")

    entries = [os.path.join(path, name) for name in os.listdir(path)]
    entries = [entry for entry in entries if os.path.isfile(entry)]
    if not entries:
        raise FrameworkConfigError(f"schema_config_path '{path}' is a directory but contains no files.")

    entries.sort(key=lambda entry: (os.path.getmtime(entry), os.path.basename(entry)))
    latest = entries[-1]
    logger.info("schema_config_path '%s' is a directory -- resolved to latest file '%s'", path, latest)
    return latest


def _validate_schema_config_shape(schema_config: Any, source_label: str) -> List[Dict[str, Any]]:
    if not isinstance(schema_config, dict) or "columns" not in schema_config:
        raise FrameworkConfigError(f"schema_config file '{source_label}' must be a JSON/YAML object with a top-level 'columns' array.")
    columns = schema_config["columns"]
    if not isinstance(columns, list) or not columns:
        raise FrameworkConfigError(f"schema_config file '{source_label}': 'columns' must be a non-empty array.")
    for index, entry in enumerate(columns):
        if not isinstance(entry, dict) or not entry.get("source_name"):
            raise FrameworkConfigError(f"schema_config file '{source_label}': columns[{index}] is missing required key 'source_name'.")
    return columns


def load_schema_config(dbutils: Any, schema_config_path: str, catalog: str = "", environment: str = "") -> Dict[str, Any]:
    """Resolve, read, template, and parse a ``schema_config`` file.

    Parameters
    ----------
    dbutils:
        Notebook ``dbutils``, forwarded to :func:`onboarding.spec_loader.read_raw_spec_text`
        as its ``dbutils.fs.head`` fallback for a path not reachable via a plain ``open()``.
    schema_config_path:
        An exact file path, or a directory (resolved to its latest file -- see
        :func:`resolve_schema_config_path`).
    catalog, environment:
        Substituted for any ``{{catalog}}``/``{{env}}`` placeholder in the file, identically to
        how the main onboarding spec is templated.

    Raises
    ------
    FrameworkConfigError
        If the resolved file can't be read/parsed, or doesn't match the expected shape.
    """
    resolved_path = resolve_schema_config_path(schema_config_path)
    try:
        raw_text = read_raw_spec_text(dbutils, resolved_path)
        templated_text = substitute_environment_placeholders(raw_text, catalog, environment)
        file_extension = os.path.splitext(resolved_path)[1].lower()
        schema_config = parse_spec_text(templated_text, file_extension, resolved_path)
    except OnboardingValidationError as exc:
        # spec_loader.py's own functions raise OnboardingValidationError -- normalized to
        # FrameworkConfigError here so every exception this module raises, regardless of which
        # internal step failed, shares one consistent contract for callers.
        raise FrameworkConfigError(f"Failed to load schema_config file '{resolved_path}': {exc}") from exc
    _validate_schema_config_shape(schema_config, resolved_path)
    return schema_config


def apply_schema_config(df: DataFrame, schema_config: Dict[str, Any]) -> DataFrame:
    """Apply ``schema_config``'s type casts, renames, and column comments to ``df``.

    For every ``columns[]`` entry: casts ``source_name`` to ``data_type`` (when given -- a
    plain Spark SQL type string, passed straight to ``Column.cast()``, e.g. ``"STRING"``,
    ``"BIGINT"``, ``"DECIMAL(10,2)"``), renames it to ``target_name`` (defaults to
    ``source_name`` when omitted -- i.e. cast/comment only, no rename), and attaches
    ``comment`` as the output column's metadata (``{"comment": ...}``) -- the same schema
    metadata key Delta/Unity Catalog read as a genuine column comment when a table is created
    from this DataFrame's schema, so no separate DDL step is needed for it to take effect.
    Streaming-safe: built entirely from ``Column`` expressions (``.cast().alias(...,
    metadata=...)``), never a driver-side schema/RDD round-trip, so it works identically for a
    streaming or batch ``df``.

    Columns **not** named in ``columns[]`` pass through completely unchanged, in their
    original position relative to each other (renamed/cast columns are projected first, in
    ``columns[]`` order, followed by every untouched column in its original order).

    ``nullable`` is accepted and preserved as informational-only metadata for now -- it is
    **not** enforced at ingestion time (Spark's ``.cast()`` cannot force a column non-nullable
    independent of the data actually flowing through it). To genuinely enforce non-nullability,
    add a companion ``dq_config`` rule (e.g. ``{"expression": "<target_name> IS NOT NULL",
    "action": "fail"}``) -- see ``docs/28_ingestion_schema_config.md``.

    Raises
    ------
    FrameworkConfigError
        If a ``columns[]` entry's ``source_name`` isn't present on ``df``.
    """
    columns = _validate_schema_config_shape(schema_config, "<in-memory schema_config>")

    configured_source_names = {entry["source_name"] for entry in columns}
    missing = configured_source_names - set(df.columns)
    if missing:
        raise FrameworkConfigError(
            f"schema_config references source column(s) not present on the incoming DataFrame: {sorted(missing)} "
            f"(available columns: {sorted(df.columns)})"
        )

    projected = []
    for entry in columns:
        source_name = entry["source_name"]
        target_name = entry.get("target_name") or source_name
        data_type = entry.get("data_type")
        comment = entry.get("comment")

        column_expr = F.col(source_name)
        if data_type:
            column_expr = column_expr.cast(data_type)
        metadata: Optional[Dict[str, str]] = {"comment": comment} if comment else None
        projected.append(column_expr.alias(target_name, metadata=metadata) if metadata else column_expr.alias(target_name))

    passthrough_columns = [F.col(c) for c in df.columns if c not in configured_source_names]
    result_df = df.select(*projected, *passthrough_columns)

    logger.info(
        "apply_schema_config: applied %d configured column(s), passed through %d unconfigured column(s)",
        len(projected),
        len(passthrough_columns),
    )
    return result_df
