"""Structured JSON logging for ingestion/transformation/reconciliation/sink operations (Phase 10).

**The explicit requirement this module exists to satisfy:** "Structured JSON logging for
every ingestion/transformation/reconciliation/sink operation (flow ID, records read/written/
rejected/quarantined, status, duration, errors), available via the Lakeflow event table;
logging failures must never mask the real processing error."

**Why this is NOT "write rows into the Lakeflow event log" (and why that's the honest,
correct reading of the requirement, not a shortfall).** Lakeflow Declarative Pipelines'
event log has a fixed, closed set of ``event_type`` values (``create_update``,
``user_action``, ``runtime_details``, ``flow_progress``, ``update_progress``,
``flow_definition``, ``dataset_definition``, ``sink_definition``, ``deprecation``,
``cluster_resources``, ``autoscale``, ``planning_information``, ``hook_progress``,
``operation_progress``, ``stream_progress``, ``behavior_change_in_spark_connect``) -- there is
no documented API to append a custom, application-defined event into that table. Claiming this
module writes "into the Lakeflow event table" would be overclaiming a capability Lakeflow does
not expose. What this module actually does -- and what is genuinely, practically equivalent for
an operator -- is emit one JSON line per business event via Python's standard ``logging``
module. That line lands in the pipeline/job's driver stdout, which Databricks makes available
through the cluster/driver log viewer (for a Lakeflow Declarative Pipeline update) and the
Jobs UI's per-task "Logs" tab (for a plain job-task notebook, e.g. the reconciliation engine or
this module's post-deployment CDC-change-count capture) -- the closest real, inspectable log
surface this framework's own code can write to, without a private/undocumented API.

This deliberately **complements, not duplicates**, Lakeflow's own native event log. Every
``@dlt.table``/``@dlt.view`` already gets ``flow_progress``/``dataset_definition`` events for
free, from Lakeflow itself, with zero custom code -- generic per-flow read/write row counts and
data-quality-expectation (``warn``/``drop``/``fail``) pass/fail counts are already there; this
module does not re-emit those. What Lakeflow's generic event log has *no concept of* is this
framework's own business-level semantics: which ``flow_id`` (``dataflow_id``/``flow_step_id``)
a table belongs to, quarantine counts broken down by DQ *rule* (vs. Lakeflow's native
expectations, which only cover ``warn``/``drop``/``fail``, not this framework's manual
``quarantine`` action), reconciliation match/append/mismatch counts per target, CDC
insert/update/delete counts per SCD strategy, and sink egress format/target. That business
layer is what :func:`log_flow_event` exists to carry.

**The never-mask-the-real-error guarantee.** :func:`log_flow_event` wraps its own body in a
``try/except Exception`` that swallows *any* failure in building or emitting the log line (a
malformed field, a logging handler misconfiguration, anything) -- at most attempting one
best-effort fallback ``logger.error`` call describing the swallow, itself also guarded. A
logging bug must never propagate up and either (a) crash a pipeline update / job task in place
of the real processing error the caller was about to raise, or (b) replace that real error with
an unrelated one about logging. Every call site in this framework that wraps a caller's own
try/except (:func:`logged_operation`) re-raises the *original* exception unchanged after
logging it -- ``log_flow_event`` is invoked from that context manager's ``finally``/``except``
path specifically so a raise from ``log_flow_event`` itself (impossible by construction, per
the guarantee above, but never assumed) could not shadow the original traceback.

**Status-to-log-level mapping.** ``SUCCESS`` -> ``logger.info``, ``WARNING`` ->
``logger.warning``, ``FAILED`` -> ``logger.error`` -- so a log aggregation/alerting tool
watching this logger's output at ``ERROR`` level picks up every failed flow operation without
any custom log-parsing logic, purely from the standard logging level. An unrecognized status
string is still emitted (never dropped) at ``INFO`` level, with the caller-supplied string
preserved verbatim in the JSON payload's ``status`` field.
"""

import contextlib
import datetime
import json
import logging
import time
from typing import Any, Dict, Iterator, Optional

logger = logging.getLogger("flowx.lakeflow_framework.observability.structured_logger")

_LOG_METHOD_BY_STATUS = {
    "SUCCESS": "info",
    "WARNING": "warning",
    "FAILED": "error",
}


def log_flow_event(
    operation: str,
    flow_id: str,
    status: str,
    records_read: Optional[int] = None,
    records_written: Optional[int] = None,
    records_rejected: Optional[int] = None,
    records_quarantined: Optional[int] = None,
    duration_ms: Optional[float] = None,
    error: Optional[str] = None,
    **extra: Any,
) -> None:
    """Emit one structured JSON log line describing a single business-level flow operation.

    Parameters
    ----------
    operation:
        A short, stable label for what happened, e.g. ``"ingestion_read"``,
        ``"transformation_execute"``, ``"dq_staging"``, ``"flow_registration"``,
        ``"sink_registration"``, ``"reconciliation_match"``,
        ``"reconciliation_mismatch_log_write"``, ``"cdc_change_capture"`` -- see this
        module's call sites (``engine/flow_registration.py``, ``engine/sink_registration.py``,
        ``dq/quarantine.py``, ``reconciliation/appender.py``, ``reconciliation/
        mismatch_logging.py``, ``control_plane/post_deployment.py``,
        ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``) for the actual set in use.
        Not an enum -- new operations are just new string literals at their call site, same as
        this framework's existing ``logger.info``/``logger.error`` message conventions.
    flow_id:
        The business identifier this event is about -- ``dataflow_id``/``flow_step_id`` for an
        ingestion/transformation flow, ``target_id`` (optionally composed with
        ``reconciliation_id``) for a reconciliation event. Always a plain string so this
        payload is directly filterable/groupable by an operator searching driver logs.
    status:
        ``"SUCCESS"`` / ``"FAILED"`` / ``"WARNING"`` (see this module's docstring for the
        log-level mapping); any other string is still emitted, at ``INFO`` level.
    records_read, records_written, records_rejected, records_quarantined:
        Row counts for this operation, when known and cheap to obtain (``None`` when not
        applicable or not knowable without an illegal/expensive extra action -- see each call
        site's own comments for why a given count is or isn't populated there).
    duration_ms:
        Wall-clock duration of the operation in milliseconds, when the caller tracked it
        (rounded to 3 decimal places for readability; ``None`` otherwise).
    error:
        A string description of the failure (typically ``str(exc)``) when ``status`` is
        ``"FAILED"``; ``None`` otherwise. Deliberately a plain string, not the exception object
        itself -- this function's payload must always be JSON-serializable without relying on
        the caller to have already stringified everything.
    **extra:
        Any additional operation-specific fields (e.g. ``target_id``/``reconciliation_id`` for
        a reconciliation event, ``sink_format``/``target_type`` for a sink registration event,
        ``cdc_load_strategy`` for a flow-registration event) -- merged directly into the top
        level of the emitted JSON object.

    Returns
    -------
    None
        Always -- this function's whole contract is "never raise, never propagate a failure of
        its own" (see this module's docstring). There is nothing meaningful to return.
    """
    try:
        payload: Dict[str, Any] = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "operation": operation,
            "flow_id": flow_id,
            "status": status,
            "records_read": records_read,
            "records_written": records_written,
            "records_rejected": records_rejected,
            "records_quarantined": records_quarantined,
            "duration_ms": round(duration_ms, 3) if isinstance(duration_ms, (int, float)) else duration_ms,
            "error": error,
        }
        if extra:
            payload.update(extra)

        # default=str: an operation-specific `extra` value that isn't natively JSON-serializable
        # (e.g. a Row, a Decimal, a datetime some caller forgot to stringify) must never turn an
        # observability call into a raised exception -- coerce anything json.dumps can't handle
        # natively to its str() form instead of failing.
        line = json.dumps(payload, default=str)
        method_name = _LOG_METHOD_BY_STATUS.get(status, "info")
        getattr(logger, method_name)(line)
    except Exception as exc:  # noqa: BLE001 -- logging must NEVER raise or mask the real processing error
        try:
            logger.error(
                "structured_logger.log_flow_event failed to emit a log event (operation=%r, flow_id=%r, "
                "status=%r): %s",
                operation,
                flow_id,
                status,
                exc,
            )
        except Exception:  # noqa: BLE001 -- even this best-effort fallback must never propagate
            pass


class _OperationContext:
    """Mutable bag a :func:`logged_operation` caller fills in before the ``with`` block exits.

    Plain attributes (not a dataclass/slots) -- callers are expected to just assign
    ``ctx.records_written = n`` inline at the point in their own code where that count becomes
    known, which reads more naturally at a call site than threading a return value back out of
    a context manager.
    """

    def __init__(self, extra: Dict[str, Any]) -> None:
        self.status: str = "SUCCESS"
        self.error: Optional[str] = None
        self.records_read: Optional[int] = None
        self.records_written: Optional[int] = None
        self.records_rejected: Optional[int] = None
        self.records_quarantined: Optional[int] = None
        self.extra: Dict[str, Any] = extra


@contextlib.contextmanager
def logged_operation(operation: str, flow_id: str, **extra: Any) -> Iterator[_OperationContext]:
    """Context manager that times a block of code and emits exactly one :func:`log_flow_event`
    call describing it, on both the success and failure path.

    Reduces call-site boilerplate for the common "log SUCCESS with whatever counts were
    populated, or log FAILED with the real exception's message, then re-raise that exact
    exception unchanged" pattern that would otherwise be duplicated in a
    try/except/finally at every wiring point in this phase. Usage::

        with logged_operation("ingestion_read", flow_id, source_type="autoloader") as op:
            staged_df = read_ingestion_source(spark, source_type, source_config)
            # op.records_read = ... only if a real count is cheaply available here; usually
            # not, for a lazy/streaming DataFrame -- see call sites for why.

    On normal exit, emits ``status="SUCCESS"`` with whatever ``op.records_*`` fields the caller
    set (``None`` for any left untouched) and ``error=None``. On an exception raised from inside
    the ``with`` block, emits ``status="FAILED"`` with ``error=str(exc)`` and then **re-raises
    the original exception unchanged** -- this context manager only observes and logs, it never
    swallows or replaces the caller's own exception (that would violate the "logging must never
    mask the real processing error" requirement in the other direction: hiding a real failure
    behind "successfully logged the failure" is exactly as wrong as crashing on a logging bug).

    Parameters
    ----------
    operation, flow_id:
        Forwarded to :func:`log_flow_event`.
    **extra:
        Forwarded to :func:`log_flow_event` on both the success and failure emission -- fields
        that describe the operation itself (e.g. ``target_table``, ``cdc_load_strategy``)
        rather than its outcome, and so are known up front rather than set on ``op``.
    """
    ctx = _OperationContext(dict(extra))
    start = time.monotonic()
    try:
        yield ctx
    except Exception as exc:  # noqa: BLE001 -- observed and re-raised, never swallowed here
        ctx.status = "FAILED"
        ctx.error = str(exc)
        raise
    finally:
        duration_ms = (time.monotonic() - start) * 1000.0
        log_flow_event(
            operation=operation,
            flow_id=flow_id,
            status=ctx.status,
            records_read=ctx.records_read,
            records_written=ctx.records_written,
            records_rejected=ctx.records_rejected,
            records_quarantined=ctx.records_quarantined,
            duration_ms=duration_ms,
            error=ctx.error,
            **ctx.extra,
        )
