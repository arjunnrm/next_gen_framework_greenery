"""Unit tests for observability/structured_logger.py -- pure Python, no Spark needed.

Covers: the emitted JSON payload's shape, the status-to-log-level mapping, and -- the module's
central contract -- that a broken underlying logger can never turn ``log_flow_event`` (or the
``logged_operation`` context manager built on it) into a source of raised exceptions, and can
never mask the real exception a caller was already raising.
"""

import json
import logging

import pytest

from flowx.lakeflow_framework.observability import structured_logger
from flowx.lakeflow_framework.observability.structured_logger import (
    log_flow_event,
    logged_operation,
)

LOGGER_NAME = "flowx.lakeflow_framework.observability.structured_logger"


def _emitted_payloads(caplog):
    """Parse every captured record's message on LOGGER_NAME as JSON, in emission order."""
    return [json.loads(record.message) for record in caplog.records if record.name == LOGGER_NAME]


# ---------------------------------------------------------------------------
# log_flow_event -- payload shape
# ---------------------------------------------------------------------------


def test_payload_contains_every_documented_field(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(
            operation="ingestion_read",
            flow_id="flow-1",
            status="SUCCESS",
            records_read=100,
            records_written=95,
            records_rejected=3,
            records_quarantined=2,
            duration_ms=123.456789,
            error=None,
        )
    payload = _emitted_payloads(caplog)[0]
    assert payload["operation"] == "ingestion_read"
    assert payload["flow_id"] == "flow-1"
    assert payload["status"] == "SUCCESS"
    assert payload["records_read"] == 100
    assert payload["records_written"] == 95
    assert payload["records_rejected"] == 3
    assert payload["records_quarantined"] == 2
    assert payload["error"] is None
    assert "timestamp" in payload and payload["timestamp"]


def test_duration_ms_is_rounded_to_three_decimal_places(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="SUCCESS", duration_ms=1.23456789)
    assert _emitted_payloads(caplog)[0]["duration_ms"] == 1.235


def test_unset_optional_fields_default_to_null(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="SUCCESS")
    payload = _emitted_payloads(caplog)[0]
    for field in ("records_read", "records_written", "records_rejected", "records_quarantined", "duration_ms", "error"):
        assert payload[field] is None


def test_extra_kwargs_are_merged_into_the_top_level_payload(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(
            operation="reconciliation_match", flow_id="recon-1:target-1", status="SUCCESS", target_id="target-1", reconciliation_id="recon-1"
        )
    payload = _emitted_payloads(caplog)[0]
    assert payload["target_id"] == "target-1"
    assert payload["reconciliation_id"] == "recon-1"


def test_error_field_carries_the_failure_message(caplog):
    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="FAILED", error="boom: something broke")
    assert _emitted_payloads(caplog)[0]["error"] == "boom: something broke"


def test_extra_value_that_isnt_natively_json_serializable_is_coerced_via_str(caplog):
    class Weird:
        def __str__(self):
            return "weird-repr"

    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="SUCCESS", odd_field=Weird())
    assert _emitted_payloads(caplog)[0]["odd_field"] == "weird-repr"


# ---------------------------------------------------------------------------
# status -> log level mapping
# ---------------------------------------------------------------------------


def test_success_status_logs_at_info_level(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="SUCCESS")
    records = [r for r in caplog.records if r.name == LOGGER_NAME]
    assert records[0].levelno == logging.INFO


def test_warning_status_logs_at_warning_level(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="WARNING")
    records = [r for r in caplog.records if r.name == LOGGER_NAME]
    assert records[0].levelno == logging.WARNING


def test_failed_status_logs_at_error_level(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="FAILED")
    records = [r for r in caplog.records if r.name == LOGGER_NAME]
    assert records[0].levelno == logging.ERROR


def test_unrecognized_status_is_still_emitted_at_info_level_verbatim(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        log_flow_event(operation="op", flow_id="f", status="SOMETHING_ELSE")
    records = [r for r in caplog.records if r.name == LOGGER_NAME]
    assert records[0].levelno == logging.INFO
    assert _emitted_payloads(caplog)[0]["status"] == "SOMETHING_ELSE"


# ---------------------------------------------------------------------------
# The never-raise / never-mask-the-real-error guarantee
# ---------------------------------------------------------------------------


def test_log_flow_event_never_raises_when_the_underlying_logger_raises(monkeypatch):
    """The core Phase 10 requirement: a logging bug must never propagate out of
    log_flow_event, since the caller may be about to raise (or return from) the real
    processing outcome and a raised logging exception would replace/mask it."""

    def _raise(*args, **kwargs):
        raise RuntimeError("logging backend exploded")

    monkeypatch.setattr(structured_logger.logger, "info", _raise)
    log_flow_event(operation="op", flow_id="f", status="SUCCESS")  # must not raise


def test_log_flow_event_swallow_survives_even_a_broken_fallback_log_call(monkeypatch):
    """Both the primary emit AND the best-effort fallback logger.error call are broken --
    log_flow_event must still swallow everything and return normally."""

    def _raise(*args, **kwargs):
        raise RuntimeError("logging backend is completely broken")

    monkeypatch.setattr(structured_logger.logger, "info", _raise)
    monkeypatch.setattr(structured_logger.logger, "error", _raise)
    log_flow_event(operation="op", flow_id="f", status="SUCCESS")  # must not raise


def test_log_flow_event_swallow_handles_unserializable_payload_gracefully(monkeypatch):
    """json.dumps itself failing (default=str insufficient, e.g. a circular reference) must
    also be swallowed, not raised."""

    class Circular:
        def __str__(self):
            raise RuntimeError("even str() fails")

    log_flow_event(operation="op", flow_id="f", status="SUCCESS", broken=Circular())  # must not raise


# ---------------------------------------------------------------------------
# logged_operation -- context manager
# ---------------------------------------------------------------------------


def test_logged_operation_logs_success_with_counts_set_on_ctx(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        with logged_operation("ingestion_read", "flow-1", source_type="autoloader") as ctx:
            ctx.records_read = 42
    payload = _emitted_payloads(caplog)[0]
    assert payload["status"] == "SUCCESS"
    assert payload["records_read"] == 42
    assert payload["error"] is None
    assert payload["source_type"] == "autoloader"
    assert payload["duration_ms"] is not None and payload["duration_ms"] >= 0


def test_logged_operation_reraises_the_original_exception_unchanged(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        with pytest.raises(ValueError, match="original processing failure"):
            with logged_operation("ingestion_read", "flow-1"):
                raise ValueError("original processing failure")
    payload = _emitted_payloads(caplog)[0]
    assert payload["status"] == "FAILED"
    assert "original processing failure" in payload["error"]


def test_logged_operation_reraises_original_exception_even_when_logging_itself_is_broken(monkeypatch):
    """Belt-and-suspenders version of the never-mask guarantee: even when BOTH the primary
    and fallback log calls raise, the exception that propagates out of the `with` block must
    be the caller's own original exception -- never a logging-internal one."""

    def _raise(*args, **kwargs):
        raise RuntimeError("logging backend exploded")

    monkeypatch.setattr(structured_logger.logger, "error", _raise)

    with pytest.raises(ValueError, match="real processing error"):
        with logged_operation("op", "flow-1"):
            raise ValueError("real processing error")


def test_logged_operation_does_not_swallow_or_alter_exception_type():
    class CustomError(Exception):
        pass

    with pytest.raises(CustomError):
        with logged_operation("op", "flow-1"):
            raise CustomError("custom failure")
