"""Extracts DLT/Lakeflow event log rows for a time window and aggregates them per flow.

Two layers, deliberately kept apart for testability (see ``docs/README.md``'s unit/integration
split -- unit tests get no Spark session):

- **Impure** (:func:`extract_raw_events`, :func:`resolve_dataflow_group_id`) -- touch Spark /
  the Databricks SDK, return plain ``dict``/``str`` values, nothing Spark-typed leaks past
  this module.
- **Pure** (:func:`aggregate_flow_metrics`) -- takes the plain-dict events
  :func:`extract_raw_events` returns (or a hand-built synthetic list in a unit test) and
  produces the :class:`DataflowGroupTelemetry` structure :mod:`otel_payload_builder` consumes.
  No Spark import anywhere in this half.

Event log schema reference: https://docs.databricks.com/aws/en/ldp/monitor-event-logs --
``origin``/``error`` are native structs (dot-accessible); ``details`` is a STRING column
holding a JSON payload whose shape depends on ``event_type`` (there is no single fixed schema
for it, which is exactly why it's JSON-typed rather than a struct in the first place).

``dataflow_group_id`` resolution: this framework configures exactly one ``dataflow.group.id``
per pipeline (a Spark conf set in the pipeline's own ``resources/*.yml`` -- see
``AGENTS.md``/``SKILL.md`` §2), so a pipeline update's ``dataflow_group_id`` is a constant,
resolved once via the Pipelines API rather than appearing anywhere in the event log itself.

``update_id`` resolution (v1.3.0): the triggered engine's window is the upstream *task run*'s
wall clock, and a pipeline can legitimately run more than one update inside it (a retry, or a
manually-started update racing the scheduled one). :func:`resolve_update_ids_for_window` asks the
Pipelines API which updates that pipeline actually created inside the window, so
:func:`extract_raw_events` can narrow to exactly those and this task exports only the telemetry
it is responsible for. Like ``dataflow.group.id``, this is a Pipelines-API lookup rather than an
event-log field, and it lives here -- next to the event-log query it exists to narrow -- for the
same reason ``resolve_dataflow_group_id`` does: ``task_context_resolver.py``'s one job stays
"resolve run_id -> (pipeline_id, time window)" and nothing else.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from flowx.lakeflow_framework.exceptions import ObservabilityConfigError

logger = logging.getLogger("flowx.lakeflow_framework.observability.event_log_extractor")

# Native Lakeflow update-progress terminal states (details.update_progress.state) -- once one of
# these is observed for an update_id, its end_time_ms should stop advancing on later events.
_TERMINAL_UPDATE_STATES = {"COMPLETED", "FAILED", "CANCELED"}
# Native Lakeflow flow-progress terminal statuses (details.flow_progress.status).
_TERMINAL_FLOW_STATUSES = {"COMPLETED", "FAILED", "STOPPED", "SKIPPED", "EXCLUDED"}


@dataclass
class ExpectationMetric:
    name: str
    dataset: Optional[str]
    passed_records: Optional[int]
    failed_records: Optional[int]


@dataclass
class ErrorDetail:
    event_type: str
    level: Optional[str]
    message: Optional[str]
    fatal: Optional[bool]
    class_name: Optional[str]
    stack_trace: Optional[str]
    timestamp_ms: Optional[int]
    flow_id: Optional[str] = None
    update_id: Optional[str] = None


@dataclass
class FlowMetrics:
    flow_id: str
    flow_name: Optional[str]
    dataset_name: Optional[str]
    update_id: Optional[str]
    status: Optional[str] = None
    start_time_ms: Optional[int] = None
    end_time_ms: Optional[int] = None
    num_output_rows: Optional[int] = None
    num_upserted_rows: Optional[int] = None
    num_deleted_rows: Optional[int] = None
    dropped_records: Optional[int] = None
    backlog_bytes: Optional[int] = None
    backlog_files: Optional[int] = None
    expectations: List[ExpectationMetric] = field(default_factory=list)
    errors: List[ErrorDetail] = field(default_factory=list)

    @property
    def duration_ms(self) -> Optional[int]:
        if self.start_time_ms is None or self.end_time_ms is None:
            return None
        return self.end_time_ms - self.start_time_ms


@dataclass
class UpdateSummary:
    update_id: str
    state: Optional[str] = None
    start_time_ms: Optional[int] = None
    end_time_ms: Optional[int] = None


@dataclass
class DataflowGroupTelemetry:
    dataflow_group_id: str
    pipeline_id: str
    window_start_ms: int
    window_end_ms: int
    updates: List[UpdateSummary] = field(default_factory=list)
    flows: List[FlowMetrics] = field(default_factory=list)
    pipeline_level_errors: List[ErrorDetail] = field(default_factory=list)
    total_events: int = 0


def resolve_dataflow_group_id(workspace_client: Any, pipeline_id: str) -> str:
    """Resolve ``pipeline_id``'s configured ``dataflow.group.id`` Spark conf via the Pipelines API.

    Raises
    ------
    ObservabilityConfigError
        If the pipeline cannot be fetched, or has no ``dataflow.group.id`` configuration entry
        (this framework's engine notebook cannot run at all without one, so its absence means
        this pipeline was never actually deployed by this framework).
    """
    try:
        pipeline = workspace_client.pipelines.get(pipeline_id=pipeline_id)
    except Exception as exc:  # noqa: BLE001
        raise ObservabilityConfigError(f"Failed to fetch pipeline '{pipeline_id}': {exc}") from exc

    spec = getattr(pipeline, "spec", None)
    configuration = getattr(spec, "configuration", None) or {}
    dataflow_group_id = configuration.get("dataflow.group.id")
    if not dataflow_group_id:
        raise ObservabilityConfigError(
            f"Pipeline '{pipeline_id}' has no 'dataflow.group.id' configuration entry -- "
            "cannot resolve which dataflow_group_id its telemetry belongs to."
        )
    return dataflow_group_id


def resolve_update_ids_for_window(
    workspace_client: Any, pipeline_id: str, start_time_ms: int, end_time_ms: int
) -> Optional[List[str]]:
    """Ask the Pipelines API which of ``pipeline_id``'s updates were created inside the
    ``[start_time_ms, end_time_ms]`` window, for :func:`extract_raw_events` to narrow on.

    **Why this is best-effort and never raises.** Narrowing is an *accuracy improvement* over the
    timestamp window, not a correctness prerequisite: if the API call fails, the SDK in use is too
    old to expose ``pipelines.list_updates``, or the response simply carries no usable
    ``creation_time``, the only sane outcome is to fall back to today's timestamp-window-only
    query rather than fail an export the framework could still perform. Every failure path
    therefore logs a WARNING and returns ``None`` -- the value :func:`extract_raw_events`
    interprets as "no narrowing".

    ``creation_time`` is used (rather than any completion timestamp) because the upstream task run
    *starts* before it creates the pipeline update and *ends* after that update finishes, so every
    update this task is responsible for was necessarily created inside the task's own window,
    while an update created before the task started belongs to a different run even if it happened
    to still be finishing inside this window.

    Returns
    -------
    list of str or None
        The matching ``update_id`` values, or ``None`` when nothing could be resolved (including
        the "resolved, but zero updates matched" case -- see the WARNING below: an empty match is
        far more likely to mean the lookup is not seeing what it should than that a completed
        pipeline task genuinely produced no update, and returning ``None`` there degrades to the
        pre-v1.3.0 behaviour instead of silently exporting nothing).
    """
    try:
        updates = list(workspace_client.pipelines.list_updates(pipeline_id=pipeline_id).updates or [])
    except Exception as exc:  # noqa: BLE001 -- best-effort narrowing; see this function's docstring
        logger.warning(
            "Could not list updates for pipeline '%s' (%s) -- falling back to the timestamp-window-only "
            "event_log query. Telemetry stays correct; it may additionally include another update that ran "
            "inside this task's window.",
            pipeline_id,
            exc,
        )
        return None

    matching = []
    for update in updates:
        creation_time = getattr(update, "creation_time", None)
        update_id = getattr(update, "update_id", None)
        if not update_id or creation_time is None:
            continue
        if int(start_time_ms) <= int(creation_time) <= int(end_time_ms):
            matching.append(str(update_id))

    if not matching:
        logger.warning(
            "No pipeline '%s' update was reported as created inside window [%d, %d] (%d update(s) inspected) -- "
            "falling back to the timestamp-window-only event_log query rather than exporting nothing.",
            pipeline_id,
            start_time_ms,
            end_time_ms,
            len(updates),
        )
        return None

    logger.info(
        "Resolved %d update_id(s) for pipeline '%s' in window [%d, %d]: %s",
        len(matching),
        pipeline_id,
        start_time_ms,
        end_time_ms,
        matching,
    )
    return matching


def extract_raw_events(
    spark: Any,
    pipeline_id: str,
    start_time_ms: int,
    end_time_ms: int,
    update_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Query the ``event_log(:pipeline_id)`` table-valued function for the given window.

    Parameters
    ----------
    spark:
        Active SparkSession.
    pipeline_id:
        The pipeline whose event log is read.
    start_time_ms, end_time_ms:
        Inclusive epoch-millis bounds -- the upstream task run's own wall clock (see
        ``task_context_resolver.py``).
    update_ids:
        Optional. When given, the query is additionally narrowed to
        ``origin.update_id IN (...)``, so a pipeline that ran more than one update inside this
        task's wall-clock window exports only the update(s) this task actually produced (see
        :func:`resolve_update_ids_for_window`). ``None`` -- the default, and what every
        pre-v1.3.0 caller passes implicitly -- preserves the timestamp-window-only query exactly.
        An **empty** list is deliberately treated the same as ``None`` rather than as
        ``IN ()``: a caller that resolved zero updates has learned nothing, and degrading to the
        wider window is always preferable to exporting an empty payload for a pipeline update
        that demonstrably ran.

    Returns
    -------
    dict
        ``{"events": [<plain dict per row>, ...], "query_duration_ms": float}``. Each event
        dict has ``id``, ``timestamp_ms``, ``message``, ``level``, ``event_type``, ``origin``
        (dict), ``error`` (dict or ``None``), ``details`` (raw JSON string or ``None``) --
        exactly the shape :func:`aggregate_flow_metrics` expects.

    Raises
    ------
    ObservabilityConfigError
        If the query fails (e.g. the pipeline has never run an update, or the caller lacks
        ``CAN_VIEW``/``CAN_MANAGE`` on the pipeline).
    """
    # spark.sql(..., args=...) only accepts plain literal values (or nested SQL
    # constructor expressions) per argument -- a pyspark Column (e.g.
    # F.timestamp_millis(F.lit(...))) is rejected with INVALID_SQL_ARG (confirmed live).
    # Pass the epoch-millis values as plain int args instead and do the millis -> timestamp
    # conversion inside the SQL text itself via timestamp_millis(:start_ts).
    args: Dict[str, Any] = {
        "pipeline_id": pipeline_id,
        "start_ts": int(start_time_ms),
        "end_ts": int(end_time_ms),
    }
    query = (
        "SELECT * FROM event_log(:pipeline_id) "
        "WHERE timestamp >= timestamp_millis(:start_ts) AND timestamp <= timestamp_millis(:end_ts)"
    )
    if update_ids:
        # One named parameter per update_id rather than a single array argument or an
        # f-string-interpolated IN list: the same `args=` literal-only restriction noted above
        # applies, and a per-value marker keeps the update_ids out of the SQL text entirely
        # (they arrive from a Pipelines API response, so they are not attacker-controlled, but
        # a parameterized IN list is both free and the convention this query already follows).
        placeholders = []
        for index, update_id in enumerate(update_ids):
            parameter_name = f"update_id_{index}"
            args[parameter_name] = str(update_id)
            placeholders.append(f":{parameter_name}")
        query += f" AND origin.update_id IN ({', '.join(placeholders)})"

    # Kept as a separate local so the error message below stays one plain f-string -- its
    # "Failed to query event_log" prefix is what agent_tools.py's failure matrix pattern-matches
    # on, so it must not drift.
    narrowing = f" narrowed to update_ids={list(update_ids)}" if update_ids else ""

    start = time.monotonic()
    try:
        events_df = spark.sql(query, args=args)
        rows = events_df.collect()
    except Exception as exc:  # noqa: BLE001
        raise ObservabilityConfigError(
            f"Failed to query event_log('{pipeline_id}') for window [{start_time_ms}, {end_time_ms}]{narrowing}: {exc}"
        ) from exc
    query_duration_ms = (time.monotonic() - start) * 1000.0

    events = []
    for row in rows:
        row_dict = row.asDict(recursive=True)
        timestamp = row_dict.get("timestamp")
        events.append(
            {
                "id": row_dict.get("id"),
                "timestamp_ms": int(timestamp.timestamp() * 1000) if hasattr(timestamp, "timestamp") else None,
                "message": row_dict.get("message"),
                "level": row_dict.get("level"),
                "event_type": row_dict.get("event_type"),
                "origin": row_dict.get("origin") or {},
                "error": row_dict.get("error"),
                "details": row_dict.get("details"),
            }
        )

    logger.info(
        "event_log('%s') window=[%d, %d]%s: %d row(s) in %.1f ms",
        pipeline_id,
        start_time_ms,
        end_time_ms,
        narrowing,
        len(events),
        query_duration_ms,
    )
    return {"events": events, "query_duration_ms": query_duration_ms}


def _flow_key(update_id: Optional[str], flow_id: str) -> str:
    return f"{update_id or '_'}::{flow_id}"


def _parse_details(raw_details: Optional[str]) -> Dict[str, Any]:
    if not raw_details:
        return {}
    try:
        parsed = json.loads(raw_details)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _errors_from_event(event: Dict[str, Any]) -> List[ErrorDetail]:
    """Build zero or more ErrorDetail entries from one event, whether it carries a native
    ``error`` struct or is simply an ``ERROR``/``WARN``-level event with no structured error."""
    origin = event.get("origin") or {}
    flow_id = origin.get("flow_id")
    update_id = origin.get("update_id")
    error = event.get("error")

    if error:
        exceptions = error.get("exceptions") or []
        if exceptions:
            return [
                ErrorDetail(
                    event_type=event.get("event_type"),
                    level=event.get("level"),
                    message=exc.get("message") or event.get("message"),
                    fatal=error.get("fatal"),
                    class_name=exc.get("class_name"),
                    stack_trace=exc.get("stack_trace"),
                    timestamp_ms=event.get("timestamp_ms"),
                    flow_id=flow_id,
                    update_id=update_id,
                )
                for exc in exceptions
            ]
        return [
            ErrorDetail(
                event_type=event.get("event_type"),
                level=event.get("level"),
                message=event.get("message"),
                fatal=error.get("fatal"),
                class_name=None,
                stack_trace=None,
                timestamp_ms=event.get("timestamp_ms"),
                flow_id=flow_id,
                update_id=update_id,
            )
        ]

    if event.get("level") in ("ERROR", "WARN"):
        return [
            ErrorDetail(
                event_type=event.get("event_type"),
                level=event.get("level"),
                message=event.get("message"),
                fatal=None,
                class_name=None,
                stack_trace=None,
                timestamp_ms=event.get("timestamp_ms"),
                flow_id=flow_id,
                update_id=update_id,
            )
        ]
    return []


def aggregate_flow_metrics(events: List[Dict[str, Any]], dataflow_group_id: str, pipeline_id: str,
                            window_start_ms: int, window_end_ms: int) -> DataflowGroupTelemetry:
    """Aggregate raw event dicts (see :func:`extract_raw_events`) into one
    :class:`DataflowGroupTelemetry`, keyed per ``(update_id, flow_id)`` -- a flow that runs
    across multiple updates within the window (continuous-mode / multiple triggered updates)
    gets one :class:`FlowMetrics` entry per update, since row counts reset per update rather
    than accumulating across them.

    Pure function: no Spark, no network -- safe to call with a hand-built synthetic ``events``
    list in a unit test (single update, multiple updates, zero events, partial-failure runs).
    """
    telemetry = DataflowGroupTelemetry(
        dataflow_group_id=dataflow_group_id,
        pipeline_id=pipeline_id,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        total_events=len(events),
    )

    flows_by_key: Dict[str, FlowMetrics] = {}
    updates_by_id: Dict[str, UpdateSummary] = {}

    for event in events:
        origin = event.get("origin") or {}
        update_id = origin.get("update_id")
        flow_id = origin.get("flow_id")
        event_type = event.get("event_type")
        timestamp_ms = event.get("timestamp_ms")
        details = _parse_details(event.get("details"))

        if update_id:
            update_summary = updates_by_id.setdefault(update_id, UpdateSummary(update_id=update_id))
            if timestamp_ms is not None:
                if update_summary.start_time_ms is None or timestamp_ms < update_summary.start_time_ms:
                    update_summary.start_time_ms = timestamp_ms
                if update_summary.end_time_ms is None or timestamp_ms > update_summary.end_time_ms:
                    update_summary.end_time_ms = timestamp_ms
            if event_type == "update_progress":
                state = (details.get("update_progress") or {}).get("state")
                if state:
                    update_summary.state = state

        if flow_id:
            key = _flow_key(update_id, flow_id)
            flow_metrics = flows_by_key.setdefault(
                key,
                FlowMetrics(
                    flow_id=flow_id,
                    flow_name=origin.get("flow_name"),
                    dataset_name=origin.get("dataset_name"),
                    update_id=update_id,
                ),
            )
            if timestamp_ms is not None:
                if flow_metrics.start_time_ms is None or timestamp_ms < flow_metrics.start_time_ms:
                    flow_metrics.start_time_ms = timestamp_ms
                if flow_metrics.status not in _TERMINAL_FLOW_STATUSES and (
                    flow_metrics.end_time_ms is None or timestamp_ms >= flow_metrics.end_time_ms
                ):
                    flow_metrics.end_time_ms = timestamp_ms

            if event_type == "flow_progress":
                progress = details.get("flow_progress") or {}
                status = progress.get("status")
                if status:
                    flow_metrics.status = status
                metrics = progress.get("metrics") or {}
                for attr in ("num_output_rows", "num_upserted_rows", "num_deleted_rows", "backlog_bytes", "backlog_files"):
                    if metrics.get(attr) is not None:
                        setattr(flow_metrics, attr, metrics.get(attr))
                data_quality = progress.get("data_quality") or {}
                if data_quality.get("dropped_records") is not None:
                    flow_metrics.dropped_records = data_quality.get("dropped_records")
                if data_quality.get("expectations"):
                    flow_metrics.expectations = [
                        ExpectationMetric(
                            name=exp.get("name"),
                            dataset=exp.get("dataset"),
                            passed_records=exp.get("passed_records"),
                            failed_records=exp.get("failed_records"),
                        )
                        for exp in data_quality["expectations"]
                    ]

            flow_metrics.errors.extend(_errors_from_event(event))
        else:
            # No flow_id -> a pipeline/update-level event (create_update, update_progress,
            # planning_information, ...). Only its errors are of interest at this level.
            telemetry.pipeline_level_errors.extend(_errors_from_event(event))

    telemetry.updates = sorted(updates_by_id.values(), key=lambda u: u.update_id)
    telemetry.flows = sorted(flows_by_key.values(), key=lambda f: (f.update_id or "", f.flow_id))

    logger.info(
        "Aggregated dataflow_group_id='%s': %d event(s) -> %d update(s), %d flow(s), %d pipeline-level error(s)",
        dataflow_group_id,
        telemetry.total_events,
        len(telemetry.updates),
        len(telemetry.flows),
        len(telemetry.pipeline_level_errors),
    )
    return telemetry
