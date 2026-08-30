"""Delta/Lakeflow storage optimization: table properties, Liquid Clustering, UniForm, TTL.

**Real bug found: row-level auto-TTL was completely non-functional.** Auto TTL
(https://docs.databricks.com/aws/en/tables/operations/auto-ttl) is *not* a generic Delta
table property like ``delta.logRetentionDuration`` -- it has no ``delta.`` prefix at all,
its real properties are ``autottl.timestampColumn``/``autottl.expireInDays``, and for a
Lakeflow streaming table it can *only* be set via a dedicated ``auto_ttl={"timestamp_column":
..., "expire_in_days": ...}`` keyword argument passed directly to the table-creating
``@dlt.table``/``dlt.create_streaming_table`` call -- ``ALTER TABLE ... DELETE ROWS`` is
explicitly unsupported for streaming tables; the only way to set or change it is pipeline
code + republish. The original implementation set ``properties["delta.autoTTL.duration"]
= target_config["auto_ttl_duration"]`` -- a property name Delta has never recognized,
silently ignored as an arbitrary custom key, meaning every flow that ever configured
``auto_ttl_duration`` got no TTL behavior at all, with no error to reveal it. It was also
missing the one piece of configuration Auto TTL cannot work without: *which column*
determines a row's age. Fixed by replacing ``auto_ttl_duration`` (a bare duration string)
with ``auto_ttl`` (an object naming both the expiration window and the
``DATE``/``TIMESTAMP``/``TIMESTAMP_NTZ`` column to measure it from), built here as its own
decorator-kwarg dict via :func:`build_auto_ttl_kwarg` rather than folded into
:func:`build_table_properties`'s properties dict -- see
docs/21_auto_ttl_row_expiration.md.

The same "some Lakeflow storage settings are *decorator keyword arguments*, not table
properties" split is why :func:`build_partition_and_cluster_kwargs` exists (v1.3.0): both
``partition_cols`` and ``cluster_by`` are ``@dlt.table`` kwargs, and both carry a rule that is
easy to silently re-implement wrongly in a second writer -- an *explicitly empty*
``partition_columns`` must never become ``partition_cols=[]`` (a zero-column ``partitionBy`` is
not a contracted Delta configuration), and ``liquid_clustering_columns`` must never exceed
:data:`MAX_LIQUID_CLUSTERING_COLUMNS`. Deriving both kwargs in one function means every current
and future writer inherits both rules instead of re-deriving them from ``target_config`` by hand.
"""

import logging
from typing import Any, Dict, Optional

from NextGen_Metadata_Framework.lakeflow_framework.crypto.secrets import assert_safe_identifier
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.storage.table_properties")

# The framework's ``landing_retention_policy.clean_source`` spellings -> Auto Loader's own
# ``cloudFiles.cleanSource`` values. This map is also the authoritative set of *accepted* modes:
# ``ingestion/readers.py`` rejects anything absent from it with "Unsupported clean_source value",
# which is why ``"off"`` has an entry of its own even though "off" is Auto Loader's own default
# and a reader may legitimately choose to emit no ``cloudFiles.cleanSource`` option for it -- an
# explicit ``clean_source: "off"`` must validate as a *known* mode, never as a typo.
CLEAN_SOURCE_MODE_MAP = {"archive": "MOVE", "delete": "DELETE", "off": "OFF"}

# Maximum number of columns Delta Liquid Clustering supports on one table. The onboarding
# validator rejects a longer list at spec-validation time; this constant backs the *runtime*
# guard in build_partition_and_cluster_kwargs as defense in depth, because a control-table row
# can also be hand-edited or upserted by something other than the onboarding engine, and the
# failure mode without a guard is an opaque Delta error at pipeline-publish time rather than a
# named configuration error.
# Kept in sync with the identical constant in onboarding/spec_validator.py -- see the v1.3.0
# contract, E07.
MAX_LIQUID_CLUSTERING_COLUMNS = 3

# Every CDC-dispatched strategy (everything except the two no-op loads) gets Change Data
# Feed enabled so cdc/change_metrics.py::capture_scd_change_counts can query
# table_changes(...) for exact inserted/updated/deleted row counts per pipeline update,
# instead of a manual, double-scanning diff.
_CDC_DISPATCHED_STRATEGIES = {"SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"}


def qualified_table_name(catalog: str, schema: str, table: str) -> str:
    """Build a fully-qualified ``catalog.schema.table`` name for a ``@dlt.table``/``@dlt.view``
    ``name=`` argument.

    Lakeflow Declarative Pipelines resolve every *unqualified* ``name=`` against the
    pipeline resource's own default catalog/schema (set in ``resources/*.yml``), **not**
    against a flow's own ``target_catalog``/``target_schema`` control-table columns --
    passing a fully-qualified name is how a single pipeline publishes tables across
    multiple schemas/catalogs (confirmed against Lakeflow's own multi-schema publishing
    docs). Every table/view this framework registers as a final, externally-queryable
    dataset (main table, quarantine table, SCD reporting views, SCD3's internal history
    table) must go through this -- passing a bare ``target_table`` was a real bug: every
    flow silently landed in the pipeline's own default schema instead of its configured
    ``target_schema``, only surfaced once a pipeline had flows spanning more than one
    schema and something tried to read a table by its *intended* qualified name.

    Raises
    ------
    ValueError
        If any component is not a safe, unquoted identifier.
    """
    catalog = assert_safe_identifier(catalog, "target_catalog")
    schema = assert_safe_identifier(schema, "target_schema")
    table = assert_safe_identifier(table, "target_table")
    return f"{catalog}.{schema}.{table}"


def build_table_properties(target_config: Dict[str, Any]) -> Dict[str, str]:
    """Translate a ``target_config`` dict into Delta/Lakeflow table properties.

    Supports Liquid Clustering (via ``cluster_by``/``partition_cols`` on the ``@dlt.table``
    decorator, not here -- build them with :func:`build_partition_and_cluster_kwargs`), UniForm
    Iceberg read-compatibility, row-level auto-TTL, and Delta log / deleted-file retention, per
    the framework's storage-optimization requirements.

    v2 schema: ``log_retention_duration``/``deleted_file_retention_duration`` moved from
    top-level ``target_config`` fields to key-value entries under ``target_config.table_properties``.
    ``table_format: "delta_uniform_iceberg"`` (v1) is replaced by
    ``table_properties.enable_iceberg_read_uniformity: true`` (v2) -- every Lakeflow table is
    physically Delta regardless of ``storage_format``; ``storage_format: "iceberg"`` (valid only
    for ``target_type == "batch_table"``, enforced by the validator) is implemented as this same
    UniForm read-compatibility property, since Lakeflow Declarative Pipelines has no separate
    native-Iceberg physical storage engine to opt into.

    Raises
    ------
    FrameworkConfigError
        If ``target_config`` is not a dict-like mapping.
    """
    try:
        properties: Dict[str, str] = {}
        table_properties_config = target_config.get("table_properties") or {}

        if target_config.get("storage_format") == "iceberg" or table_properties_config.get("enable_iceberg_read_uniformity"):
            properties["delta.universalFormat.enabledFormats"] = "iceberg"

        if table_properties_config.get("log_retention_duration"):
            properties["delta.logRetentionDuration"] = table_properties_config["log_retention_duration"]

        if table_properties_config.get("deleted_file_retention_duration"):
            properties["delta.deletedFileRetentionDuration"] = table_properties_config["deleted_file_retention_duration"]

        if target_config.get("cdc_load_strategy") in _CDC_DISPATCHED_STRATEGIES:
            properties["delta.enableChangeDataFeed"] = "true"

        properties.setdefault("delta.autoOptimize.optimizeWrite", "true")
        properties.setdefault("delta.autoOptimize.autoCompact", "true")
        return properties
    except AttributeError as exc:
        raise FrameworkConfigError(f"target_config must be a dict, got {type(target_config).__name__}") from exc


def build_auto_ttl_kwarg(target_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Build the ``auto_ttl`` keyword argument for a ``@dlt.table``/``dlt.create_streaming_table`` call.

    Expects ``target_config.auto_ttl`` shaped like::

        {"timestamp_column": "event_ts", "expire_in_days": 90}

    ``timestamp_column`` must name a ``DATE``/``TIMESTAMP``/``TIMESTAMP_NTZ`` column
    present on the target; ``expire_in_days`` is a positive integer. See this module's
    docstring for why this must be passed as its own decorator kwarg (``table_kwargs["auto_ttl"]
    = ...``, alongside ``partition_cols``/``cluster_by``) rather than merged into
    :func:`build_table_properties`'s output.

    Returns
    -------
    dict or None
        ``None`` when ``target_config`` has no ``auto_ttl`` configured (the default,
        unchanged behavior for every flow that doesn't opt in), **or** when ``auto_ttl`` is
        present but missing ``timestamp_column``/``expire_in_days``. Auto TTL is an opt-in
        feature -- an incomplete block is treated the same as "not configured" (logged as a
        warning, never a hard failure), since skipping TTL has no correctness impact, only
        the absence of auto-expiration.

    Raises
    ------
    FrameworkConfigError
        If ``auto_ttl`` supplies *both* keys but ``expire_in_days`` isn't a positive integer,
        or ``timestamp_column`` isn't a safe identifier -- a present-but-invalid value is a
        real mistake, not an intentional omission.
    """
    auto_ttl = target_config.get("auto_ttl")
    if not auto_ttl:
        return None
    timestamp_column = auto_ttl.get("timestamp_column")
    expire_in_days = auto_ttl.get("expire_in_days")
    if not timestamp_column or expire_in_days is None:
        logger.warning(
            "target_config.auto_ttl is present but missing timestamp_column/expire_in_days -- "
            "Auto TTL will NOT be applied for this table. Provide both sub-fields to enable "
            "row expiration."
        )
        return None
    try:
        timestamp_column = assert_safe_identifier(timestamp_column, "auto_ttl.timestamp_column")
        if isinstance(expire_in_days, bool) or not isinstance(expire_in_days, int) or expire_in_days <= 0:
            raise ValueError(f"auto_ttl.expire_in_days must be a positive integer, got {expire_in_days!r}")
        return {"timestamp_column": timestamp_column, "expire_in_days": expire_in_days}
    except ValueError as exc:
        raise FrameworkConfigError(str(exc)) from exc


def build_partition_and_cluster_kwargs(
    target_config: Dict[str, Any], table_label: Optional[str] = None
) -> Dict[str, Any]:
    """Build the ``partition_cols``/``cluster_by`` keyword arguments for a ``@dlt.table`` call.

    This is the single place either kwarg is derived from ``target_config``, so the two rules
    below are enforced once rather than re-implemented (and eventually re-implemented *wrongly*)
    by every writer that registers a physical target table:

    * **An empty list must never reach the decorator.** ``partition_columns: []`` is a legal,
      deliberate configuration meaning "no partitioning", but ``partition_cols=[]`` is *not* a
      contracted Delta/Lakeflow configuration -- a zero-column ``partitionBy()`` has no defined
      behavior, so the correct translation of "explicitly empty" is to omit the kwarg entirely,
      exactly as an omitted field does. The same holds for ``cluster_by``.
    * **``liquid_clustering_columns`` is capped at** :data:`MAX_LIQUID_CLUSTERING_COLUMNS`. See
      that constant for why the check lives at runtime as well as in the onboarding validator.

    "Omitted" and "explicitly empty" are therefore **behaviourally identical** (both produce an
    unpartitioned / unclustered table) but **diagnostically distinct**: an explicitly empty
    ``partition_columns`` emits an INFO recording that a human deliberately opted out, so an
    operator reading a pipeline log can tell "nobody ever configured partitioning for this
    target" apart from "someone considered it and decided against it". That distinction is the
    entire reason this function reads ``target_config`` directly instead of taking two already-
    ``.get()``-ed lists: after a ``.get()``, absent and explicitly-empty are indistinguishable.

    Parameters
    ----------
    target_config:
        The flow's ``target_config`` dict; ``partition_columns`` and
        ``liquid_clustering_columns`` are read from it, both optional.
    table_label:
        Human-readable identifier for the table being configured (the caller's fully-qualified
        table name is the useful choice), used only to make the explicitly-empty INFO log
        actionable -- a pipeline registers many tables in one update, so an unlabelled log line
        would say nothing about *which* target opted out.

    Returns
    -------
    dict
        Zero, one, or both of ``{"partition_cols": [...], "cluster_by": [...]}``. A key is
        present **only** when its configured list is non-empty; an omitted field and an
        explicitly empty list both produce no key at all. Callers are expected to
        ``table_kwargs.update(...)`` the result, so an empty dict is a valid, meaningful return.

    Raises
    ------
    FrameworkConfigError
        If ``liquid_clustering_columns`` names more than :data:`MAX_LIQUID_CLUSTERING_COLUMNS`
        columns. Raised (rather than truncated or warned about) because silently dropping a
        clustering column an author explicitly asked for would produce a table whose physical
        layout does not match its declared configuration -- a difference nobody would notice
        until a query ran slowly.
    """
    kwargs: Dict[str, Any] = {}

    partition_columns = target_config.get("partition_columns")
    if partition_columns:
        kwargs["partition_cols"] = partition_columns
    elif isinstance(partition_columns, (list, tuple)):
        # Reached only for a present-but-empty list: an absent field, or an explicit JSON null,
        # leaves partition_columns as None and falls through silently (there is nothing to
        # report -- "not configured" is the norm, not a decision worth a log line).
        logger.info(
            "target_config.partition_columns is an explicitly empty list for '%s' -- the target "
            "table will be created with NO partitioning (this is a deliberate configuration, not "
            "a missing one).",
            table_label or "<unnamed target>",
        )

    liquid_clustering_columns = target_config.get("liquid_clustering_columns")
    if liquid_clustering_columns:
        if (
            isinstance(liquid_clustering_columns, (list, tuple))
            and len(liquid_clustering_columns) > MAX_LIQUID_CLUSTERING_COLUMNS
        ):
            raise FrameworkConfigError(
                f"target_config.liquid_clustering_columns: at most {MAX_LIQUID_CLUSTERING_COLUMNS} columns are "
                f"supported by Delta Liquid Clustering, got {len(liquid_clustering_columns)} "
                f"({liquid_clustering_columns}) -- reduce the list to {MAX_LIQUID_CLUSTERING_COLUMNS} or fewer columns"
            )
        kwargs["cluster_by"] = liquid_clustering_columns

    return kwargs
