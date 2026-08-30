"""Builds strict OpenTelemetry (OTLP/HTTP JSON) ``ResourceLogs`` payloads from
:class:`event_log_extractor.DataflowGroupTelemetry`.

Shape reference: OTLP logs data model / ``ExportLogsServiceRequest`` JSON encoding
(https://opentelemetry.io/docs/specs/otlp/, https://github.com/open-telemetry/opentelemetry-proto).
Pure Python -- no Spark, no network -- so every mapping decision here is unit-testable against
a hand-built :class:`~event_log_extractor.DataflowGroupTelemetry`.

**One Resource per flow, not one per pipeline run.** An OTel ``Resource`` is meant to describe
"the entity producing the telemetry", and this module's own attribute contract (Deliverable
requirement) puts flow-scoped values -- ``databricks.dataflow_id``, ``databricks.step_id`` --
directly into the *resource* attribute set, not just onto individual log records. Since those
values vary per flow within one pipeline run, strict compliance means each flow gets its own
``ResourceLogs`` entry (sharing the same ``pipeline.*``/``databricks.job_id`` values across all
of them). A pipeline run with zero flow_id-scoped events (e.g. an update that produced no
``flow_progress`` events at all within the window) still gets exactly one ``ResourceLogs`` entry
carrying group-level attributes only, with a single diagnostic ``LogRecord`` -- see
:func:`build_resource_logs`'s zero-flow branch -- so "no telemetry" is never silently "no
payload".

Destination-specific ``resource_attributes`` (``destination_config.resource_attributes`` --
``service.name`` overrides, ``ddsource``, custom tags) are deliberately **not** applied here.
This module builds one canonical, destination-agnostic payload; ``destination_dispatcher.py``
merges each destination's own resource attributes in immediately before dispatch, so the same
canonical payload is built once and reused across every configured destination.
"""

import json
import logging
import time
from datetime import datetime
from numbers import Number
from typing import Any, Dict, List, Optional

from NextGen_Metadata_Framework.lakeflow_framework.observability.event_log_extractor import (
    DataflowGroupTelemetry,
    FlowMetrics,
)

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.observability.otel_payload_builder")

SCOPE_NAME = "NextGen_Metadata_Framework.lakeflow_framework.observability.dlt_observability"
SCOPE_VERSION = "1.0.0"

# OTel Logs Data Model SeverityNumber (base values of each range; see the OTel spec's
# "Severity Fields" table -- 1-4 TRACE, 5-8 DEBUG, 9-12 INFO, 13-16 WARN, 17-20 ERROR, 21-24 FATAL).
SEVERITY_DEBUG = 5
SEVERITY_INFO = 9
SEVERITY_WARN = 13
SEVERITY_ERROR = 17
SEVERITY_FATAL = 21

_FLOW_STATUS_SEVERITY = {
    "FAILED": SEVERITY_ERROR,
    "COMPLETED": SEVERITY_INFO,
    "STOPPED": SEVERITY_WARN,
    "SKIPPED": SEVERITY_WARN,
    "EXCLUDED": SEVERITY_WARN,
}


def _any_value(value: Any) -> Dict[str, Any]:
    """Encode a Python value as an OTLP JSON ``AnyValue``."""
    if value is None:
        return {"stringValue": ""}
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}  # OTLP JSON encodes int64 fields as strings
    if isinstance(value, Number):
        return {"doubleValue": float(value)}
    if isinstance(value, (list, tuple)):
        return {"arrayValue": {"values": [_any_value(v) for v in value]}}
    if isinstance(value, dict):
        return {"kvlistValue": {"values": [build_attribute(k, v) for k, v in value.items()]}}
    return {"stringValue": str(value)}


def build_attribute(key: str, value: Any) -> Dict[str, Any]:
    return {"key": key, "value": _any_value(value)}


def _attributes(fields: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build an OTLP attribute list, dropping keys whose value is ``None`` entirely (an
    absent attribute, not an attribute explicitly set to an empty/null value)."""
    return [build_attribute(k, v) for k, v in fields.items() if v is not None]


def _log_record(
    *,
    timestamp_ms: Optional[int],
    severity_number: int,
    severity_text: str,
    body: str,
    attributes: Dict[str, Any],
) -> Dict[str, Any]:
    time_unix_nano = str(int(timestamp_ms) * 1_000_000) if timestamp_ms is not None else "0"
    return {
        "timeUnixNano": time_unix_nano,
        "observedTimeUnixNano": time_unix_nano,
        "severityNumber": severity_number,
        "severityText": severity_text,
        "body": {"stringValue": body},
        "attributes": _attributes(attributes),
    }


def _flow_summary_log_record(flow: FlowMetrics) -> Dict[str, Any]:
    severity = _FLOW_STATUS_SEVERITY.get(flow.status or "", SEVERITY_INFO)
    severity_text = {
        SEVERITY_ERROR: "ERROR",
        SEVERITY_WARN: "WARN",
        SEVERITY_INFO: "INFO",
    }.get(severity, "INFO")
    body = f"Flow '{flow.flow_id}' status={flow.status or 'UNKNOWN'} in update '{flow.update_id or 'unknown'}'"
    return _log_record(
        timestamp_ms=flow.end_time_ms or flow.start_time_ms,
        severity_number=severity,
        severity_text=severity_text,
        body=body,
        attributes={
            "event.name": "flow_performance_summary",
            "flow.status": flow.status,
            "flow.start_time_ms": flow.start_time_ms,
            "flow.end_time_ms": flow.end_time_ms,
            "flow.duration_ms": flow.duration_ms,
            "flow.num_output_rows": flow.num_output_rows,
            "flow.num_upserted_rows": flow.num_upserted_rows,
            "flow.num_deleted_rows": flow.num_deleted_rows,
            "flow.dropped_records": flow.dropped_records,
            "flow.backlog_bytes": flow.backlog_bytes,
            "flow.backlog_files": flow.backlog_files,
        },
    )


def _expectation_log_records(flow: FlowMetrics) -> List[Dict[str, Any]]:
    records = []
    for expectation in flow.expectations:
        has_failures = bool(expectation.failed_records)
        records.append(
            _log_record(
                timestamp_ms=flow.end_time_ms or flow.start_time_ms,
                severity_number=SEVERITY_WARN if has_failures else SEVERITY_INFO,
                severity_text="WARN" if has_failures else "INFO",
                body=f"Expectation '{expectation.name}' on dataset '{expectation.dataset}': "
                f"{expectation.passed_records or 0} passed, {expectation.failed_records or 0} failed",
                attributes={
                    "event.name": "data_quality_expectation",
                    "expectation.name": expectation.name,
                    "expectation.dataset": expectation.dataset,
                    "expectation.passed_records": expectation.passed_records,
                    "expectation.failed_records": expectation.failed_records,
                },
            )
        )
    return records


def _error_log_records(errors) -> List[Dict[str, Any]]:
    records = []
    for error in errors:
        severity = SEVERITY_FATAL if error.fatal else SEVERITY_ERROR
        records.append(
            _log_record(
                timestamp_ms=error.timestamp_ms,
                severity_number=severity,
                severity_text="FATAL" if error.fatal else "ERROR",
                body=error.message or "Pipeline error",
                attributes={
                    "event.name": "pipeline_error",
                    "error.event_type": error.event_type,
                    "error.level": error.level,
                    "error.fatal": error.fatal,
                    "error.class_name": error.class_name,
                    "error.stack_trace": error.stack_trace,
                },
            )
        )
    return records


def _resource(attributes: Dict[str, Any]) -> Dict[str, Any]:
    return {"attributes": _attributes(attributes)}


def _base_resource_attributes(
    telemetry: DataflowGroupTelemetry,
    job_context: Dict[str, Any],
    service_name: str,
    deployment_environment: Optional[str],
) -> Dict[str, Any]:
    attrs: Dict[str, Any] = {
        "service.name": service_name,
        "databricks.job_id": job_context.get("job_id"),
        "databricks.task_run_id": job_context.get("task_run_id"),
        "databricks.pipeline_id": telemetry.pipeline_id,
        "databricks.dataflow_group_id": telemetry.dataflow_group_id,
    }
    if deployment_environment:
        attrs["deployment.environment"] = deployment_environment
    for key, value in (job_context.get("pipeline_config") or {}).items():
        attrs[f"pipeline.config.{key}"] = value
    return attrs


def build_resource_logs(
    telemetry: DataflowGroupTelemetry,
    job_context: Dict[str, Any],
    service_name: str = "dlt-observability",
    deployment_environment: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build one ``ResourceLogs`` entry per flow (plus pipeline-level-error/zero-flow entries).

    Parameters
    ----------
    telemetry:
        Aggregated telemetry for one ``dataflow_group_id`` (see :mod:`event_log_extractor`).
    job_context:
        ``{"job_id": ..., "task_run_id": ..., "pipeline_config": {...}}`` -- forwarded onto
        every ``ResourceLogs``' resource attributes (``databricks.job_id``,
        ``databricks.task_run_id``, ``pipeline.config.*``).
    service_name, deployment_environment:
        Fallback ``service.name``/``deployment.environment`` resource attribute values, used
        when no destination-specific override is later merged in by the dispatcher.

    Returns
    -------
    list of dict
        Each a complete ``ResourceLogs`` object: ``{"resource": {...}, "scopeLogs": [...]}``.
    """
    base_attrs = _base_resource_attributes(telemetry, job_context, service_name, deployment_environment)
    resource_logs: List[Dict[str, Any]] = []

    for flow in telemetry.flows:
        flow_attrs = dict(base_attrs)
        flow_attrs["databricks.dataflow_id"] = flow.flow_id
        flow_attrs["databricks.step_id"] = flow.flow_id
        if flow.update_id:
            flow_attrs["pipeline.update_id"] = flow.update_id

        log_records = [_flow_summary_log_record(flow)]
        log_records.extend(_expectation_log_records(flow))
        log_records.extend(_error_log_records(flow.errors))

        resource_logs.append(
            {
                "resource": _resource(flow_attrs),
                "scopeLogs": [{"scope": {"name": SCOPE_NAME, "version": SCOPE_VERSION}, "logRecords": log_records}],
            }
        )

    if telemetry.pipeline_level_errors:
        error_records = _error_log_records(telemetry.pipeline_level_errors)
        resource_logs.append(
            {
                "resource": _resource(base_attrs),
                "scopeLogs": [{"scope": {"name": SCOPE_NAME, "version": SCOPE_VERSION}, "logRecords": error_records}],
            }
        )

    if not resource_logs:
        # Zero-flow-events edge case: still emit exactly one payload, never nothing at all --
        # a silently-empty run and a genuinely-broken extraction are otherwise indistinguishable
        # downstream.
        diagnostic = _log_record(
            timestamp_ms=telemetry.window_end_ms,
            severity_number=SEVERITY_WARN,
            severity_text="WARN",
            body=f"No flow_progress events found for dataflow_group_id='{telemetry.dataflow_group_id}' "
            f"in window [{telemetry.window_start_ms}, {telemetry.window_end_ms}] ({telemetry.total_events} total event(s)).",
            attributes={"event.name": "no_flow_events_in_window", "window.total_events": telemetry.total_events},
        )
        resource_logs.append(
            {
                "resource": _resource(base_attrs),
                "scopeLogs": [{"scope": {"name": SCOPE_NAME, "version": SCOPE_VERSION}, "logRecords": [diagnostic]}],
            }
        )

    logger.info(
        "Built %d ResourceLogs entr(y/ies) for dataflow_group_id='%s' (%d flow(s))",
        len(resource_logs),
        telemetry.dataflow_group_id,
        len(telemetry.flows),
    )
    return resource_logs


_REQUIRED_LOG_RECORD_FIELDS = {"timeUnixNano", "severityNumber", "body", "attributes"}


def validate_resource_logs(resource_logs: List[Dict[str, Any]]) -> None:
    """Assert every mandatory OTel Logs field is present. Raises ``ValueError`` on the first
    violation found; used by tests and as a pre-dispatch sanity check.
    """
    if not isinstance(resource_logs, list) or not resource_logs:
        raise ValueError("resource_logs must be a non-empty list")

    for i, entry in enumerate(resource_logs):
        if "resource" not in entry or "attributes" not in entry["resource"]:
            raise ValueError(f"resourceLogs[{i}] is missing resource.attributes")
        if "scopeLogs" not in entry or not entry["scopeLogs"]:
            raise ValueError(f"resourceLogs[{i}] is missing a non-empty scopeLogs")
        for j, scope_log in enumerate(entry["scopeLogs"]):
            if "scope" not in scope_log or "name" not in scope_log["scope"]:
                raise ValueError(f"resourceLogs[{i}].scopeLogs[{j}] is missing scope.name")
            log_records = scope_log.get("logRecords")
            if not log_records:
                raise ValueError(f"resourceLogs[{i}].scopeLogs[{j}] has no logRecords")
            for k, record in enumerate(log_records):
                missing = _REQUIRED_LOG_RECORD_FIELDS - record.keys()
                if missing:
                    raise ValueError(f"resourceLogs[{i}].scopeLogs[{j}].logRecords[{k}] missing field(s): {sorted(missing)}")


def to_export_request(resource_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Wrap a list of ``ResourceLogs`` in the top-level ``ExportLogsServiceRequest`` shape an
    OTLP/HTTP consumer expects as its POST body."""
    return {"resourceLogs": resource_logs}


# ---------------------------------------------------------------------------------------
# Streaming event-log path (added for observability/otel_streaming_sink.py).
#
# Unlike build_resource_logs above, this does NOT take a pre-aggregated
# DataflowGroupTelemetry -- it takes a flat list of raw event-log row dicts for one
# streaming micro-batch (each already tagged with a `source_pipeline` column by the
# streaming pipeline that reads N event-log tables as a union) and produces one
# ResourceLogs entry per distinct `source_pipeline` value found in that batch, one
# logRecord per row. Reuses _resource/_log_record/SCOPE_NAME/SCOPE_VERSION from above so
# both payload-building paths stay byte-for-byte consistent in the parts of the OTel shape
# they share; kept as a separate function (not a mode of build_resource_logs) because the
# input shape is fundamentally different (raw rows, no flow/expectation/error aggregation)
# and this module's existing build_resource_logs must not be touched (batch observability
# path, unrelated to this streaming one).
# ---------------------------------------------------------------------------------------

_EVENT_LOG_LEVEL_SEVERITY = {
    "ERROR": SEVERITY_ERROR,
    "WARN": SEVERITY_WARN,
    "WARNING": SEVERITY_WARN,
    "INFO": SEVERITY_INFO,
    "METRICS": SEVERITY_INFO,
}
_SEVERITY_TEXT_BY_NUMBER = {SEVERITY_ERROR: "ERROR", SEVERITY_WARN: "WARN", SEVERITY_INFO: "INFO"}


def _severity_from_event_log_level(level: Optional[str]) -> "tuple[int, str]":
    """Map a raw Lakeflow event log row's own ``level`` column (``INFO``/``WARN``/``ERROR``/
    ``METRICS`` -- see https://docs.databricks.com/aws/en/ldp/monitor-event-logs) to an OTel
    SeverityNumber/SeverityText pair. Falls back to INFO for an unrecognized/missing value --
    a cosmetic severity mismatch must never be the reason a row fails to export."""
    severity_number = _EVENT_LOG_LEVEL_SEVERITY.get((level or "").strip().upper(), SEVERITY_INFO)
    return severity_number, _SEVERITY_TEXT_BY_NUMBER.get(severity_number, "INFO")


def _event_log_row_timestamp_ms(row: Dict[str, Any]) -> Optional[int]:
    """Best-effort epoch-millis conversion of a raw event-log row's own ``timestamp`` column.
    Returns ``None`` (caller falls back to current time) rather than raising -- one row with
    a missing/malformed timestamp must never abort an entire streaming micro-batch's export.

    Handles the shapes this value can actually arrive in: a ``datetime`` (the normal case --
    ``pyspark.sql.Row.asDict(recursive=True)`` never stringifies a TIMESTAMP column itself),
    a numeric epoch value (seconds or millis), or an ISO-8601 string (defensive -- in case a
    caller has already stringified the row upstream)."""
    value = row.get("timestamp")
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        # A genuine epoch-millis value for anything after the year 2001 is always >=
        # 10_000_000_000; anything smaller than that is a seconds-resolution value instead.
        return int(value) if value >= 10_000_000_000 else int(value * 1000)
    if isinstance(value, str):
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
        except ValueError:
            return None
    return None


def _event_log_row_record(row: Dict[str, Any]) -> Dict[str, Any]:
    severity_number, severity_text = _severity_from_event_log_level(row.get("level"))
    timestamp_ms = _event_log_row_timestamp_ms(row)
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    return _log_record(
        timestamp_ms=timestamp_ms,
        severity_number=severity_number,
        severity_text=severity_text,
        body=json.dumps(row, default=str),
        attributes={f"databricks.event_log.{column}": value for column, value in row.items()},
    )


def build_resource_logs_from_event_rows(
    rows: List[Dict[str, Any]], service_name: str = "otel-streaming-sink"
) -> List[Dict[str, Any]]:
    """Build one ``ResourceLogs`` entry per distinct ``source_pipeline`` value found in
    ``rows`` (one micro-batch's worth of raw event-log rows, each already carrying a
    ``source_pipeline`` column -- see ``observability/otel_streaming_sink.py``), with each
    row becoming one ``logRecord``.

    Parameters
    ----------
    rows:
        Plain dicts (e.g. from ``pyspark.sql.Row.asDict(recursive=True)``), each expected to
        carry a ``source_pipeline`` column plus whatever columns the source event log table
        itself has (``id``, ``timestamp``, ``level``, ``event_type``, ``origin``, ``error``,
        ``details``, ... -- see https://docs.databricks.com/aws/en/ldp/monitor-event-logs).
        A row missing ``source_pipeline`` is grouped under the literal ``"unknown"`` rather
        than dropped or raising -- this function's job is never to lose a row.
    service_name:
        ``service.name`` resource attribute for every entry built here.

    Returns
    -------
    list of dict
        Each a complete ``ResourceLogs`` object, same shape as :func:`build_resource_logs`.
        Never empty when ``rows`` is non-empty (every row lands in exactly one group).
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row.get("source_pipeline") or "unknown", []).append(row)

    resource_logs = [
        {
            "resource": _resource({"service.name": service_name, "databricks.source_pipeline": source_pipeline}),
            "scopeLogs": [
                {
                    "scope": {"name": SCOPE_NAME, "version": SCOPE_VERSION},
                    "logRecords": [_event_log_row_record(row) for row in group_rows],
                }
            ],
        }
        for source_pipeline, group_rows in grouped.items()
    ]

    logger.info(
        "Built %d ResourceLogs entr(y/ies) from %d raw event-log row(s) across source_pipeline(s): %s",
        len(resource_logs),
        len(rows),
        list(grouped.keys()),
    )
    return resource_logs
