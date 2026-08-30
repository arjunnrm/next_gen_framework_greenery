"""Unit tests for observability/otel_payload_builder.py -- pure Python, no Spark session."""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.observability.event_log_extractor import (
    DataflowGroupTelemetry,
    ErrorDetail,
    ExpectationMetric,
    FlowMetrics,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.otel_payload_builder import (
    build_resource_logs,
    to_export_request,
    validate_resource_logs,
)


def _attrs_dict(resource_logs_entry):
    return {a["key"]: a["value"] for a in resource_logs_entry["resource"]["attributes"]}


def _log_record_attrs_dict(log_record):
    return {a["key"]: a["value"] for a in log_record["attributes"]}


class TestBuildResourceLogsMapping:
    def test_one_resource_logs_entry_per_flow(self):
        telemetry = DataflowGroupTelemetry(
            dataflow_group_id="dfg_test",
            pipeline_id="pipe-1",
            window_start_ms=1000,
            window_end_ms=5000,
            flows=[
                FlowMetrics(flow_id="df_a", flow_name="df_a", dataset_name="bronze.a", update_id="upd-1", status="COMPLETED", start_time_ms=1000, end_time_ms=2000),
                FlowMetrics(flow_id="df_b", flow_name="df_b", dataset_name="bronze.b", update_id="upd-1", status="FAILED", start_time_ms=1000, end_time_ms=2500),
            ],
            total_events=10,
        )
        resource_logs = build_resource_logs(telemetry, job_context={"job_id": "123", "task_run_id": "456"})
        assert len(resource_logs) == 2

    def test_resource_attributes_include_databricks_identity_fields(self):
        telemetry = DataflowGroupTelemetry(
            dataflow_group_id="dfg_test",
            pipeline_id="pipe-1",
            window_start_ms=1000,
            window_end_ms=5000,
            flows=[FlowMetrics(flow_id="df_a", flow_name="df_a", dataset_name="bronze.a", update_id="upd-1", status="COMPLETED")],
        )
        resource_logs = build_resource_logs(
            telemetry, job_context={"job_id": "123", "task_run_id": "456", "pipeline_config": {"filter_country": "US"}},
            deployment_environment="prod",
        )
        attrs = _attrs_dict(resource_logs[0])
        assert attrs["databricks.job_id"]["stringValue"] == "123"
        assert attrs["databricks.task_run_id"]["stringValue"] == "456"
        assert attrs["databricks.pipeline_id"]["stringValue"] == "pipe-1"
        assert attrs["databricks.dataflow_group_id"]["stringValue"] == "dfg_test"
        assert attrs["databricks.dataflow_id"]["stringValue"] == "df_a"
        assert attrs["databricks.step_id"]["stringValue"] == "df_a"
        assert attrs["pipeline.update_id"]["stringValue"] == "upd-1"
        assert attrs["deployment.environment"]["stringValue"] == "prod"
        assert attrs["pipeline.config.filter_country"]["stringValue"] == "US"

    def test_failed_flow_produces_error_severity_summary_record(self):
        telemetry = DataflowGroupTelemetry(
            dataflow_group_id="dfg_test", pipeline_id="pipe-1", window_start_ms=0, window_end_ms=5000,
            flows=[FlowMetrics(flow_id="df_a", flow_name="df_a", dataset_name="bronze.a", update_id="upd-1", status="FAILED", start_time_ms=100, end_time_ms=200)],
        )
        resource_logs = build_resource_logs(telemetry, job_context={})
        summary_record = resource_logs[0]["scopeLogs"][0]["logRecords"][0]
        assert summary_record["severityNumber"] == 17  # ERROR
        assert summary_record["severityText"] == "ERROR"

    def test_expectations_and_errors_become_additional_log_records(self):
        flow = FlowMetrics(
            flow_id="df_a", flow_name="df_a", dataset_name="silver.a", update_id="upd-1", status="COMPLETED",
            expectations=[ExpectationMetric(name="valid_amount", dataset="silver.a", passed_records=98, failed_records=2)],
            errors=[ErrorDetail(event_type="flow_progress", level="ERROR", message="boom", fatal=True, class_name="X", stack_trace="", timestamp_ms=100)],
        )
        telemetry = DataflowGroupTelemetry(dataflow_group_id="dfg_test", pipeline_id="pipe-1", window_start_ms=0, window_end_ms=5000, flows=[flow])
        resource_logs = build_resource_logs(telemetry, job_context={})
        log_records = resource_logs[0]["scopeLogs"][0]["logRecords"]
        # 1 summary + 1 expectation + 1 error
        assert len(log_records) == 3
        expectation_record = next(r for r in log_records if _log_record_attrs_dict(r).get("event.name", {}).get("stringValue") == "data_quality_expectation")
        assert _log_record_attrs_dict(expectation_record)["expectation.failed_records"]["intValue"] == "2"
        error_record = next(r for r in log_records if _log_record_attrs_dict(r).get("event.name", {}).get("stringValue") == "pipeline_error")
        assert error_record["severityNumber"] == 21  # FATAL

    def test_zero_flows_emits_single_diagnostic_resource_logs_entry(self):
        telemetry = DataflowGroupTelemetry(dataflow_group_id="dfg_test", pipeline_id="pipe-1", window_start_ms=0, window_end_ms=5000, total_events=3)
        resource_logs = build_resource_logs(telemetry, job_context={})
        assert len(resource_logs) == 1
        log_record = resource_logs[0]["scopeLogs"][0]["logRecords"][0]
        assert log_record["severityText"] == "WARN"
        assert "No flow_progress events" in log_record["body"]["stringValue"]

    def test_pipeline_level_errors_get_their_own_resource_logs_entry(self):
        telemetry = DataflowGroupTelemetry(
            dataflow_group_id="dfg_test", pipeline_id="pipe-1", window_start_ms=0, window_end_ms=5000,
            flows=[FlowMetrics(flow_id="df_a", flow_name="df_a", dataset_name="a", update_id="upd-1", status="COMPLETED")],
            pipeline_level_errors=[ErrorDetail(event_type="create_update", level="ERROR", message="cluster launch failed", fatal=True, class_name=None, stack_trace=None, timestamp_ms=50)],
        )
        resource_logs = build_resource_logs(telemetry, job_context={})
        assert len(resource_logs) == 2  # 1 flow + 1 pipeline-level-error entry


class TestValidateResourceLogs:
    def test_valid_payload_passes(self):
        telemetry = DataflowGroupTelemetry(
            dataflow_group_id="dfg_test", pipeline_id="pipe-1", window_start_ms=0, window_end_ms=5000,
            flows=[FlowMetrics(flow_id="df_a", flow_name="df_a", dataset_name="a", update_id="upd-1", status="COMPLETED")],
        )
        validate_resource_logs(build_resource_logs(telemetry, job_context={}))  # should not raise

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="non-empty list"):
            validate_resource_logs([])

    def test_missing_resource_attributes_raises(self):
        with pytest.raises(ValueError, match="resource.attributes"):
            validate_resource_logs([{"scopeLogs": [{"scope": {"name": "x"}, "logRecords": [{}]}]}])

    def test_missing_scope_name_raises(self):
        with pytest.raises(ValueError, match="scope.name"):
            validate_resource_logs([{"resource": {"attributes": []}, "scopeLogs": [{"scope": {}, "logRecords": [{"timeUnixNano": "1", "severityNumber": 9, "body": {}, "attributes": []}]}]}])

    def test_missing_log_record_field_raises(self):
        payload = [
            {
                "resource": {"attributes": []},
                "scopeLogs": [{"scope": {"name": "x"}, "logRecords": [{"timeUnixNano": "1", "severityNumber": 9}]}],
            }
        ]
        with pytest.raises(ValueError, match="missing field"):
            validate_resource_logs(payload)


def test_to_export_request_wraps_in_resource_logs_key():
    assert to_export_request([{"a": 1}]) == {"resourceLogs": [{"a": 1}]}
