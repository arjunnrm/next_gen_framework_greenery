"""Unit tests for observability/config_loader.py -- pure Python, no Spark session needed.

Rows here simulate what ``onboarding/metadata_upsert.py::upsert_observability_config`` would
have written into ``observability_config`` from an onboarding spec's ``observability[]``
array -- there is no separate observability config file to translate/seed any more (see
``docs/25_dlt_observability_module.md``); ``metadata_upsert.py`` builds rows inline, and this
module only ever reads the table back.
"""

import json

import pytest

from flowx.lakeflow_framework.exceptions import ObservabilityConfigError
from flowx.lakeflow_framework.observability.config_loader import parse_config_rows


def _row(config_id="cfg1", dataflow_group_id="dfg-123", destination_id="dest-1", enabled=True, destination_type="DATABRICKS_VOLUME",
         destination_config=None, auth_config=None, retry_config=None):
    return {
        "config_id": config_id,
        "dataflow_group_id": dataflow_group_id,
        "destination_id": destination_id,
        "enabled": enabled,
        "destination_type": destination_type,
        "destination_config_json": json.dumps(destination_config or {"volume_path": "/Volumes/x/y/z/"}),
        "auth_config_json": json.dumps(auth_config) if auth_config is not None else None,
        "retry_config_json": json.dumps(retry_config) if retry_config is not None else None,
    }


class TestParseConfigRows:
    def test_resolves_enabled_group_specific_destination(self):
        rows = [_row()]
        resolved = parse_config_rows(rows, "dfg-123")
        assert len(resolved) == 1
        assert resolved[0].destination_id == "dest-1"
        assert resolved[0].destination_config == {"volume_path": "/Volumes/x/y/z/"}

    def test_disabled_destination_is_dropped(self):
        rows = [_row(enabled=False)]
        assert parse_config_rows(rows, "dfg-123") == []

    def test_rows_for_other_dataflow_group_ids_are_ignored(self):
        rows = [_row(dataflow_group_id="some-other-group")]
        assert parse_config_rows(rows, "dfg-123") == []

    def test_global_fallback_applies_when_no_group_specific_row(self):
        rows = [_row(config_id="cfg-global", dataflow_group_id="*", destination_id="dest-1")]
        resolved = parse_config_rows(rows, "dfg-123")
        assert len(resolved) == 1
        assert resolved[0].config_id == "cfg-global"
        assert resolved[0].dataflow_group_id == "*"

    def test_group_specific_row_overrides_global_row_for_same_destination_id(self):
        rows = [
            _row(config_id="cfg-global", dataflow_group_id="*", destination_id="dest-1"),
            _row(config_id="cfg-specific", dataflow_group_id="dfg-123", destination_id="dest-1"),
        ]
        resolved = parse_config_rows(rows, "dfg-123")
        assert len(resolved) == 1
        assert resolved[0].config_id == "cfg-specific"

    def test_distinct_destination_ids_from_global_and_specific_both_survive(self):
        rows = [
            _row(config_id="cfg-global", dataflow_group_id="*", destination_id="dest-global-only"),
            _row(config_id="cfg-specific", dataflow_group_id="dfg-123", destination_id="dest-specific-only"),
        ]
        resolved = parse_config_rows(rows, "dfg-123")
        assert {d.destination_id for d in resolved} == {"dest-global-only", "dest-specific-only"}

    def test_auth_and_retry_config_parsed(self):
        rows = [
            _row(
                destination_type="OTLP_CONSUMER",
                destination_config={"endpoint": "https://example.com"},
                auth_config={"type": "BEARER_TOKEN", "credentials": {"token": "env:X"}},
                retry_config={"max_attempts": 5},
            )
        ]
        resolved = parse_config_rows(rows, "dfg-123")
        assert resolved[0].auth_config == {"type": "BEARER_TOKEN", "credentials": {"token": "env:X"}}
        assert resolved[0].retry_config == {"max_attempts": 5}

    def test_null_json_fields_parse_to_empty_dict(self):
        rows = [_row(auth_config=None, retry_config=None)]
        resolved = parse_config_rows(rows, "dfg-123")
        assert resolved[0].auth_config == {}
        assert resolved[0].retry_config == {}

    def test_malformed_json_raises(self):
        rows = [_row()]
        rows[0]["destination_config_json"] = "{not valid json"
        with pytest.raises(ObservabilityConfigError, match="not valid JSON"):
            parse_config_rows(rows, "dfg-123")

    def test_non_object_json_raises(self):
        rows = [_row()]
        rows[0]["destination_config_json"] = "[1, 2, 3]"
        with pytest.raises(ObservabilityConfigError, match="must be a JSON object"):
            parse_config_rows(rows, "dfg-123")

    def test_unknown_destination_type_raises(self):
        rows = [_row(destination_type="CARRIER_PIGEON")]
        with pytest.raises(ObservabilityConfigError, match="destination_type must be one of"):
            parse_config_rows(rows, "dfg-123")

    def test_missing_destination_id_raises(self):
        rows = [_row()]
        rows[0]["destination_id"] = None
        with pytest.raises(ObservabilityConfigError, match="destination_id is required"):
            parse_config_rows(rows, "dfg-123")

    def test_no_rows_resolves_empty(self):
        assert parse_config_rows([], "dfg-123") == []

    def test_resolving_for_global_scope_itself_does_not_double_process(self):
        rows = [_row(config_id="cfg-global", dataflow_group_id="*", destination_id="dest-1")]
        resolved = parse_config_rows(rows, "*")
        assert len(resolved) == 1
