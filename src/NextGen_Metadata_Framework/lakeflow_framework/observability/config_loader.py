"""Loads and resolves telemetry destination configuration from ``observability_config``.

Resolution rule: a ``destination_id`` may be configured once per-group (``dataflow_group_id`` --
same scoping as every other control table, see ``control_plane/ddl_definitions.py``) and/or once
globally (``dataflow_group_id`` = literal ``"*"``, applied to every group that doesn't define
that same ``destination_id`` itself). When both exist for the same ``destination_id``, the
group-specific row wins -- this is what makes ``"*"`` a genuine *fallback*, not a second,
always-additive row. Disabled (``enabled = false``) rows are dropped entirely rather than
surfaced with an "enabled" flag callers must remember to check.

Rows are populated by ``onboarding/metadata_upsert.py::upsert_observability_config`` from the
``observability[]`` array inside the *same* onboarding spec as every other flow (see
``docs/25_dlt_observability_module.md``) -- there is no separate observability config file or
seed step. This module only ever *reads* the table.

Parsing (:func:`parse_config_rows`) is pure Python -- takes plain dicts, returns
:class:`DestinationConfig` objects, raises nothing but ``ObservabilityConfigError`` on
malformed JSON -- so it is unit-testable with zero Spark session. :func:`load_destination_configs`
is the thin Spark-touching wrapper that reads the Delta table and hands rows to it.

**Destination ``mode`` (v1.3.0).** A destination is served by exactly *one* of this framework's
two observability engines, and ``mode`` is what decides which:

- ``"triggered"`` (the default) -- ``notebooks/08_observability/08_dlt_observability_engine.py``,
  the bounded post-update export that runs as a downstream Workflow task after one pipeline's
  update finishes.
- ``"continuous"`` -- ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py``,
  the always-on (``continuous: true``) pipeline streaming N event-log tables at once.

There is deliberately no single entrypoint that switches between them: the two have
fundamentally different lifecycles (a bounded job task versus a standing pipeline), so "which
mode am I" is decided by *which notebook is running*, and ``mode`` exists purely to stop one
destination being served -- and therefore double-exported -- by both. Callers narrow the loaded
list with :func:`filter_destinations_by_mode` before dispatching.

**Where a continuous destination's event-log tables are configured.** On the observability row
itself: ``destination_config.event_log_tables``, a JSON array of table names, resolved by
:func:`resolve_event_log_tables`. It is *not* a pipeline setting, and the distinction is not
cosmetic. A continuous export fans one streaming read out to N destinations, so the table list
belongs to the *destination set*, not to the pipeline: two destinations covering different
subsets of the estate is a normal shape, and a pipeline-level list cannot express it. It also has
to be editable without redeploying -- adding a newly-onboarded pipeline's event log to an existing
export is an onboarding-spec change (``observability[]``, upserted like every other flow), not a
bundle deploy, and the standing pipeline picks it up on its next restart. And putting it here
keeps one place to look: the mode, the destination type, the credentials, the retry policy and
the source tables are all one row.

``dataflow.otel_streaming.event_log_tables`` remains as a **bootstrap fallback** in the
pipeline's ``configuration:`` block, consulted only when the control-table lookup yields nothing
(see ``notebooks/06_observability_streaming/``). It exists for the deployment that has not
onboarded an observability row yet -- including every pre-v1.3.0 one -- and for standing up the
streaming pipeline before the control plane is populated. Two sources, one of them explicitly
subordinate: the control table wins whenever it has anything to say.

``mode`` is read **defensively** (``row.get("mode") or DEFAULT_DESTINATION_MODE``): the
``observability_config`` column is nullable *and* may be missing from the table altogether on a
control plane provisioned before v1.3.0, because ``01_setup`` only ever runs
``CREATE TABLE IF NOT EXISTS`` and never a migration. Treating both absence and NULL as
``"triggered"`` is what keeps a pre-v1.3.0 control table working with zero migration -- the same
defensive pattern ``notebooks/05_reconciliation/05_reconciliation_engine.py`` already uses for
``logging_config_json``.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import ObservabilityConfigError

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.observability.config_loader")

GLOBAL_DATAFLOW_GROUP_ID = "*"
ALLOWED_DESTINATION_TYPES = {"DATABRICKS_VOLUME", "OTLP_CONSUMER"}
#: Which of the two observability engines serves a destination -- see this module's docstring.
ALLOWED_DESTINATION_MODES = {"triggered", "continuous"}
DEFAULT_DESTINATION_MODE = "triggered"


@dataclass
class DestinationConfig:
    """One resolved, enabled telemetry destination for a specific dataflow group."""

    config_id: str
    dataflow_group_id: str
    destination_id: str
    destination_type: str
    destination_config: Dict[str, Any] = field(default_factory=dict)
    auth_config: Dict[str, Any] = field(default_factory=dict)
    retry_config: Dict[str, Any] = field(default_factory=dict)
    mode: str = DEFAULT_DESTINATION_MODE
    """Added in v1.3.0, **last and with a default** so every existing construction of this
    dataclass -- positional or keyword, including ``otel_streaming_sink.py``'s inline one, which
    has no control-table row behind it at all -- keeps working untouched."""


def _parse_json_field(raw_value: Optional[str], *, field_name: str, config_id: str) -> Dict[str, Any]:
    if raw_value is None or raw_value == "":
        return {}
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ObservabilityConfigError(
            f"observability_config row '{config_id}': {field_name} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ObservabilityConfigError(
            f"observability_config row '{config_id}': {field_name} must be a JSON object, got {type(parsed).__name__}"
        )
    return parsed


def parse_config_rows(rows: List[Dict[str, Any]], dataflow_group_id: str) -> List[DestinationConfig]:
    """Parse raw ``observability_config`` rows (as plain dicts) into enabled, resolved
    :class:`DestinationConfig` objects for one target ``dataflow_group_id``.

    Parameters
    ----------
    rows:
        Every ``observability_config`` row whose ``dataflow_group_id`` is either the target
        ``dataflow_group_id`` or ``"*"`` -- callers should already have filtered to just these
        two (see :func:`load_destination_configs`); rows for any other ``dataflow_group_id``
        are ignored defensively if present.
    dataflow_group_id:
        The dataflow group being resolved for.

    Returns
    -------
    list of DestinationConfig
        One entry per distinct ``destination_id`` that is ``enabled = true`` for this group,
        group-specific rows overriding a same-``destination_id`` global row. Entries carry
        **both** modes -- narrowing to the one engine calling is the caller's job, via
        :func:`filter_destinations_by_mode`, so a single table read can serve either engine.

    Raises
    ------
    ObservabilityConfigError
        If a row's JSON columns are malformed, ``destination_type`` is not one of
        ``ALLOWED_DESTINATION_TYPES``, or ``mode`` is present but not one of
        ``ALLOWED_DESTINATION_MODES``.
    """
    by_destination_id: Dict[str, DestinationConfig] = {}
    # Global ("*") rows are applied first so a group-specific row for the same destination_id,
    # processed second, always overrides it -- see module docstring. dict.fromkeys(...) dedupes
    # while preserving order, so resolving for "*" itself processes it exactly once instead of
    # being skipped entirely.
    for candidate_group_id in dict.fromkeys((GLOBAL_DATAFLOW_GROUP_ID, dataflow_group_id)):
        for row in rows:
            if row.get("dataflow_group_id") != candidate_group_id:
                continue
            if not row.get("enabled", False):
                continue

            config_id = row.get("config_id") or "<unknown>"
            destination_type = row.get("destination_type")
            if destination_type not in ALLOWED_DESTINATION_TYPES:
                raise ObservabilityConfigError(
                    f"observability_config row '{config_id}': destination_type must be one of "
                    f"{sorted(ALLOWED_DESTINATION_TYPES)}, got {destination_type!r}"
                )

            destination_id = row.get("destination_id")
            if not destination_id:
                raise ObservabilityConfigError(f"observability_config row '{config_id}': destination_id is required")

            # `or DEFAULT_DESTINATION_MODE` rather than `.get("mode", DEFAULT_DESTINATION_MODE)`:
            # this must survive BOTH a control table provisioned before v1.3.0 (the key is
            # absent from the row dict entirely) AND a v1.3.0 table whose nullable `mode` column
            # is NULL for rows upserted by an older onboarding run (the key is present but None).
            # Only `or` collapses both to "triggered" -- see this module's docstring.
            mode = row.get("mode") or DEFAULT_DESTINATION_MODE
            if mode not in ALLOWED_DESTINATION_MODES:
                raise ObservabilityConfigError(
                    f"observability_config row '{config_id}': mode must be one of "
                    f"{sorted(ALLOWED_DESTINATION_MODES)}, got {mode!r}"
                )

            by_destination_id[destination_id] = DestinationConfig(
                config_id=config_id,
                dataflow_group_id=candidate_group_id,
                destination_id=destination_id,
                destination_type=destination_type,
                destination_config=_parse_json_field(
                    row.get("destination_config_json"), field_name="destination_config_json", config_id=config_id
                ),
                auth_config=_parse_json_field(row.get("auth_config_json"), field_name="auth_config_json", config_id=config_id),
                retry_config=_parse_json_field(
                    row.get("retry_config_json"), field_name="retry_config_json", config_id=config_id
                ),
                mode=mode,
            )

    resolved = list(by_destination_id.values())
    logger.info(
        "Resolved %d enabled destination(s) for dataflow_group_id='%s': %s",
        len(resolved),
        dataflow_group_id,
        [(d.destination_id, d.mode) for d in resolved],
    )
    return resolved


def load_destination_configs(spark: Any, control_catalog: str, dataflow_group_id: str) -> List[DestinationConfig]:
    """Read ``<control_catalog>.config.observability_config`` and resolve the enabled
    destinations for ``dataflow_group_id`` (see :func:`parse_config_rows` for resolution rules).

    Parameters
    ----------
    spark:
        Active SparkSession.
    control_catalog:
        Catalog containing the ``config`` schema.
    dataflow_group_id:
        The dataflow group to resolve destinations for (see
        ``event_log_extractor.py::resolve_dataflow_group_id`` for how the observability engine
        resolves this from a pipeline_id at runtime).

    Returns
    -------
    list of DestinationConfig
        Both modes; narrow with :func:`filter_destinations_by_mode`. The read is a whole-table
        ``spark.table(...)`` with no column projection, which is precisely why the v1.3.0 ``mode``
        column needed no change here: it simply appears on the Row when the table has it, and its
        absence on an older table is absorbed by :func:`parse_config_rows`'s defensive read.

    Raises
    ------
    ObservabilityConfigError
        If the table cannot be read, or a row fails to parse (see :func:`parse_config_rows`).
        Callers that must not fail on an unreadable control table -- notably the always-on
        continuous pipeline, which has a pipeline-configuration fallback -- catch this and
        degrade rather than propagate.
    """
    control_schema = f"{control_catalog}.config"
    try:
        table = spark.table(f"{control_schema}.observability_config")
        rows = table.filter(
            (F.col("dataflow_group_id") == dataflow_group_id) | (F.col("dataflow_group_id") == GLOBAL_DATAFLOW_GROUP_ID)
        ).collect()
    except Exception as exc:  # noqa: BLE001
        raise ObservabilityConfigError(
            f"Failed to read '{control_schema}.observability_config' for dataflow_group_id='{dataflow_group_id}': {exc}"
        ) from exc

    return parse_config_rows([row.asDict(recursive=True) for row in rows], dataflow_group_id)


def filter_destinations_by_mode(destinations: List[DestinationConfig], mode: str) -> List[DestinationConfig]:
    """The subset of ``destinations`` one engine is responsible for.

    Pure (no Spark, no I/O) and deliberately separate from :func:`parse_config_rows`: resolution
    (which rows exist, group-versus-global precedence) is one concern, and "which of the two
    engines serves them" is another. Keeping them apart means the same loaded list can be
    narrowed twice -- once per engine -- and means a mis-set ``mode`` shows up as *this engine
    has nothing to dispatch*, never as a row silently vanishing during resolution.

    Parameters
    ----------
    destinations:
        Resolved destinations, as returned by :func:`load_destination_configs`.
    mode:
        The calling engine's own mode -- ``"triggered"`` from
        ``notebooks/08_observability/08_dlt_observability_engine.py``, ``"continuous"`` from
        ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py``.

    Returns
    -------
    list of DestinationConfig
        Order-preserving. May be empty -- an empty *triggered* list is what makes
        ``destination_dispatcher.dispatch_all`` raise its "no enabled destinations" error, which
        is the correct signal that this group configured only continuous destinations.
    """
    matching = [destination for destination in destinations if destination.mode == mode]
    logger.info(
        "Filtered %d destination(s) to %d with mode='%s': %s",
        len(destinations),
        len(matching),
        mode,
        [d.destination_id for d in matching],
    )
    return matching


def resolve_event_log_tables(destinations: List[DestinationConfig]) -> List[str]:
    """The de-duplicated, order-preserving union of every *continuous* destination's
    ``destination_config.event_log_tables``.

    Continuous mode has no upstream task to resolve a pipeline from -- the streaming pipeline is
    always-on and observes *other* pipelines -- so each continuous destination must name the
    fully-qualified ``catalog.schema.event_log_table`` tables it wants streamed. Several
    destinations commonly name overlapping table sets (an OTLP consumer and a Volume archive of
    the same pipelines); the union is de-duplicated because each table is read **once** into one
    ``@dlt.view`` and fanned out to every sink from there, so the same table listed twice would
    otherwise register two identical streaming reads of it.

    ``mode`` is re-checked here rather than trusted from the caller so this function is correct
    whether it is handed an already-``filter_destinations_by_mode``'d list (the normal call) or
    the full unfiltered one -- a ``triggered`` destination never contributes tables, matching the
    validator rule that ``event_log_tables`` is meaningless outside continuous mode.

    Returns
    -------
    list of str
        ``[]`` when there are no continuous destinations, or none of them names any table --
        which is precisely the signal the continuous notebook uses to fall back to its own
        ``dataflow.otel_streaming.event_log_tables`` pipeline configuration.
    """
    tables: Dict[str, None] = {}  # dict-as-ordered-set: de-duplicates while preserving order
    for destination in destinations:
        if destination.mode != "continuous":
            continue
        configured = (destination.destination_config or {}).get("event_log_tables")
        if not isinstance(configured, list) or not configured:
            # A continuous destination with no event_log_tables is rejected at onboarding by
            # spec_validator, so reaching here means a hand-edited control-table row. Skipping it
            # with a WARNING (rather than raising) keeps a single malformed row from taking down
            # an always-on pipeline that may be exporting several other destinations correctly.
            logger.warning(
                "Continuous destination '%s' (config_id='%s') has no destination_config.event_log_tables list -- "
                "skipping it; it contributes no tables to the continuous stream.",
                destination.destination_id,
                destination.config_id,
            )
            continue
        for table_name in configured:
            if isinstance(table_name, str) and table_name.strip():
                tables.setdefault(table_name.strip(), None)

    resolved = list(tables)
    logger.info(
        "Resolved %d distinct event_log_table(s) from %d continuous destination(s): %s",
        len(resolved),
        sum(1 for d in destinations if d.mode == "continuous"),
        resolved,
    )
    return resolved
