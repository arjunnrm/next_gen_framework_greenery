"""Unit tests for observability/event_log_extractor.py's pure aggregation logic and
resolve_dataflow_group_id -- no Spark session, no network (a fake workspace client stands in
for the Pipelines API)."""

import json

import pytest

from flowx.lakeflow_framework.exceptions import ObservabilityConfigError
from flowx.lakeflow_framework.observability.event_log_extractor import (
    aggregate_flow_metrics,
    resolve_dataflow_group_id,
)


def _flow_progress_event(update_id, flow_id, flow_name, dataset_name, timestamp_ms, status, num_output_rows=None,
                          dropped_records=None, expectations=None, level="INFO"):
    return {
        "id": f"evt-{timestamp_ms}",
        "timestamp_ms": timestamp_ms,
        "message": f"Flow {flow_id} is {status}",
        "level": level,
        "event_type": "flow_progress",
        "origin": {"update_id": update_id, "flow_id": flow_id, "flow_name": flow_name, "dataset_name": dataset_name},
        "error": None,
        "details": json.dumps(
            {
                "flow_progress": {
                    "status": status,
                    "metrics": {"num_output_rows": num_output_rows} if num_output_rows is not None else {},
                    "data_quality": {
                        "dropped_records": dropped_records,
                        "expectations": expectations or [],
                    },
                }
            }
        ),
    }


def _error_event(update_id, flow_id, timestamp_ms, message="boom", fatal=True, class_name="RuntimeException"):
    return {
        "id": f"evt-err-{timestamp_ms}",
        "timestamp_ms": timestamp_ms,
        "message": message,
        "level": "ERROR",
        "event_type": "flow_progress",
        "origin": {"update_id": update_id, "flow_id": flow_id},
        "error": {"fatal": fatal, "exceptions": [{"class_name": class_name, "message": message, "stack_trace": "at ..."}]},
        "details": json.dumps({"flow_progress": {"status": "FAILED"}}),
    }


def _update_progress_event(update_id, timestamp_ms, state):
    return {
        "id": f"evt-up-{timestamp_ms}",
        "timestamp_ms": timestamp_ms,
        "message": f"Update {update_id} is {state}",
        "level": "INFO",
        "event_type": "update_progress",
        "origin": {"update_id": update_id},
        "error": None,
        "details": json.dumps({"update_progress": {"state": state}}),
    }


class TestAggregateFlowMetrics:
    def test_zero_events_produces_empty_telemetry(self):
        telemetry = aggregate_flow_metrics([], "dfg_test", "pipe-1", 1000, 2000)
        assert telemetry.flows == []
        assert telemetry.updates == []
        assert telemetry.total_events == 0

    def test_single_update_single_flow_lifecycle(self):
        events = [
            _update_progress_event("upd-1", 1000, "RUNNING"),
            _flow_progress_event("upd-1", "df_ingest", "df_ingest", "bronze.raw", 1100, "STARTING"),
            _flow_progress_event("upd-1", "df_ingest", "df_ingest", "bronze.raw", 1500, "COMPLETED", num_output_rows=42),
            _update_progress_event("upd-1", 1600, "COMPLETED"),
        ]
        telemetry = aggregate_flow_metrics(events, "dfg_test", "pipe-1", 1000, 2000)

        assert len(telemetry.updates) == 1
        assert telemetry.updates[0].state == "COMPLETED"
        assert telemetry.updates[0].start_time_ms == 1000
        assert telemetry.updates[0].end_time_ms == 1600

        assert len(telemetry.flows) == 1
        flow = telemetry.flows[0]
        assert flow.flow_id == "df_ingest"
        assert flow.status == "COMPLETED"
        assert flow.num_output_rows == 42
        assert flow.start_time_ms == 1100
        assert flow.end_time_ms == 1500
        assert flow.duration_ms == 400

    def test_multiple_updates_same_flow_produce_separate_flow_metrics(self):
        events = [
            _flow_progress_event("upd-1", "df_ingest", "df_ingest", "bronze.raw", 1000, "COMPLETED", num_output_rows=10),
            _flow_progress_event("upd-2", "df_ingest", "df_ingest", "bronze.raw", 5000, "COMPLETED", num_output_rows=20),
        ]
        telemetry = aggregate_flow_metrics(events, "dfg_test", "pipe-1", 0, 10000)
        assert len(telemetry.flows) == 2
        by_update = {f.update_id: f for f in telemetry.flows}
        assert by_update["upd-1"].num_output_rows == 10
        assert by_update["upd-2"].num_output_rows == 20

    def test_dq_expectations_captured_per_flow(self):
        expectations = [
            {"name": "valid_amount", "dataset": "silver.txn", "passed_records": 98, "failed_records": 2},
            {"name": "non_null_id", "dataset": "silver.txn", "passed_records": 100, "failed_records": 0},
        ]
        events = [_flow_progress_event("upd-1", "flow_a", "flow_a", "silver.txn", 1000, "COMPLETED", expectations=expectations)]
        telemetry = aggregate_flow_metrics(events, "dfg_test", "pipe-1", 0, 5000)
        assert len(telemetry.flows[0].expectations) == 2
        failing = [e for e in telemetry.flows[0].expectations if e.failed_records]
        assert failing[0].name == "valid_amount"
        assert failing[0].failed_records == 2

    def test_partial_failure_run_flags_error_on_the_failing_flow_only(self):
        events = [
            _flow_progress_event("upd-1", "flow_ok", "flow_ok", "silver.a", 1000, "COMPLETED"),
            _error_event("upd-1", "flow_bad", 1200, message="Schema mismatch"),
        ]
        telemetry = aggregate_flow_metrics(events, "dfg_test", "pipe-1", 0, 5000)
        by_id = {f.flow_id: f for f in telemetry.flows}
        assert by_id["flow_ok"].errors == []
        assert len(by_id["flow_bad"].errors) == 1
        assert by_id["flow_bad"].errors[0].message == "Schema mismatch"
        assert by_id["flow_bad"].errors[0].fatal is True
        assert by_id["flow_bad"].errors[0].class_name == "RuntimeException"

    def test_pipeline_level_error_with_no_flow_id_is_not_attached_to_any_flow(self):
        events = [
            {
                "id": "evt-pipeline-err",
                "timestamp_ms": 1000,
                "message": "Cluster launch failed",
                "level": "ERROR",
                "event_type": "create_update",
                "origin": {"update_id": "upd-1"},
                "error": {"fatal": True, "exceptions": [{"class_name": "ClusterError", "message": "Cluster launch failed", "stack_trace": ""}]},
                "details": None,
            }
        ]
        telemetry = aggregate_flow_metrics(events, "dfg_test", "pipe-1", 0, 5000)
        assert telemetry.flows == []
        assert len(telemetry.pipeline_level_errors) == 1
        assert telemetry.pipeline_level_errors[0].message == "Cluster launch failed"

    def test_warn_level_event_with_no_error_struct_still_captured(self):
        events = [
            {
                "id": "evt-warn",
                "timestamp_ms": 1000,
                "message": "Backlog growing",
                "level": "WARN",
                "event_type": "flow_progress",
                "origin": {"update_id": "upd-1", "flow_id": "flow_a"},
                "error": None,
                "details": json.dumps({"flow_progress": {"status": "RUNNING"}}),
            }
        ]
        telemetry = aggregate_flow_metrics(events, "dfg_test", "pipe-1", 0, 5000)
        assert len(telemetry.flows[0].errors) == 1
        assert telemetry.flows[0].errors[0].level == "WARN"

    def test_malformed_details_json_does_not_raise(self):
        events = [
            {
                "id": "evt-bad-json",
                "timestamp_ms": 1000,
                "message": "x",
                "level": "INFO",
                "event_type": "flow_progress",
                "origin": {"update_id": "upd-1", "flow_id": "flow_a"},
                "error": None,
                "details": "{not valid json",
            }
        ]
        telemetry = aggregate_flow_metrics(events, "dfg_test", "pipe-1", 0, 5000)
        assert len(telemetry.flows) == 1
        assert telemetry.flows[0].status is None


class TestResolveDataflowGroupId:
    class _FakePipeline:
        def __init__(self, configuration):
            self.spec = type("Spec", (), {"configuration": configuration})()

    class _FakeClient:
        def __init__(self, pipeline=None, raise_exc=None):
            self._pipeline = pipeline
            self._raise_exc = raise_exc
            self.pipelines = self

        def get(self, pipeline_id):
            if self._raise_exc:
                raise self._raise_exc
            return self._pipeline

    def test_resolves_from_pipeline_configuration(self):
        client = self._FakeClient(pipeline=self._FakePipeline({"dataflow.group.id": "dfg_finance_txn_ingest"}))
        assert resolve_dataflow_group_id(client, "pipe-1") == "dfg_finance_txn_ingest"

    def test_missing_configuration_key_raises(self):
        client = self._FakeClient(pipeline=self._FakePipeline({}))
        with pytest.raises(ObservabilityConfigError, match="no 'dataflow.group.id'"):
            resolve_dataflow_group_id(client, "pipe-1")

    def test_api_failure_raises_observability_config_error(self):
        client = self._FakeClient(raise_exc=RuntimeError("network error"))
        with pytest.raises(ObservabilityConfigError, match="Failed to fetch pipeline"):
            resolve_dataflow_group_id(client, "pipe-1")
