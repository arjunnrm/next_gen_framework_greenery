"""Unit tests for observability/destination_dispatcher.py -- pure Python + mocked HTTP/secrets,
no Spark session, no real network calls or Unity Catalog Volume (plain local tmp_path stands in
for the FUSE-mounted /Volumes/ path dispatch_to_volume writes to with a plain open())."""

import gzip
import json
import os

import pytest

from flowx.lakeflow_framework.exceptions import (
    ObservabilityConfigError,
    ObservabilityDispatchError,
)
from flowx.lakeflow_framework.observability.config_loader import DestinationConfig
from flowx.lakeflow_framework.observability.destination_dispatcher import (
    build_auth_headers,
    compress_payload,
    compute_backoff_delay_seconds,
    dispatch_all,
    dispatch_to_otlp,
    dispatch_to_volume,
    merge_resource_attributes,
    resolve_credential,
)

_SAMPLE_RESOURCE_LOGS = [
    {
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "dlt-observability"}}]},
        "scopeLogs": [
            {
                "scope": {"name": "x", "version": "1.0.0"},
                "logRecords": [{"timeUnixNano": "1000000000", "severityNumber": 9, "body": {"stringValue": "ok"}, "attributes": []}],
            }
        ],
    }
]


def _no_op_secret_resolver(scope, key):
    return f"resolved-{scope}-{key}"


class TestResolveCredential:
    def test_env_var_resolved(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "s3cr3t")
        assert resolve_credential("env:MY_TOKEN", _no_op_secret_resolver) == "s3cr3t"

    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(ObservabilityConfigError, match="is not set"):
            resolve_credential("env:MISSING_VAR", _no_op_secret_resolver)

    def test_secret_reference_resolved_via_resolver(self):
        assert resolve_credential("secret:my_scope:my_key", _no_op_secret_resolver) == "resolved-my_scope-my_key"

    def test_malformed_secret_reference_raises(self):
        with pytest.raises(ObservabilityConfigError, match="Malformed secret reference"):
            resolve_credential("secret:onlyscope", _no_op_secret_resolver)

    def test_literal_value_rejected(self):
        with pytest.raises(ObservabilityConfigError, match="literal secrets are not allowed"):
            resolve_credential("literal-token-value", _no_op_secret_resolver)

    def test_secret_resolver_failure_wrapped(self):
        def _raising_resolver(scope, key):
            raise RuntimeError("scope not found")

        with pytest.raises(ObservabilityConfigError, match="Failed to resolve secret"):
            resolve_credential("secret:scope:key", _raising_resolver)


class TestBuildAuthHeaders:
    def test_none_type_returns_no_headers(self):
        assert build_auth_headers({"type": "NONE"}, _no_op_secret_resolver) == {}
        assert build_auth_headers({}, _no_op_secret_resolver) == {}

    def test_bearer_token(self, monkeypatch):
        monkeypatch.setenv("TOK", "abc123")
        headers = build_auth_headers({"type": "BEARER_TOKEN", "credentials": {"token": "env:TOK"}}, _no_op_secret_resolver)
        assert headers == {"Authorization": "Bearer abc123"}

    def test_api_key_uses_custom_header_name(self, monkeypatch):
        monkeypatch.setenv("DD_API_KEY", "dd-key-value")
        headers = build_auth_headers(
            {"type": "API_KEY", "credentials": {"header_name": "DD-API-KEY", "api_key": "env:DD_API_KEY"}}, _no_op_secret_resolver
        )
        assert headers == {"DD-API-KEY": "dd-key-value"}

    def test_basic_auth_base64_encodes_user_and_password(self, monkeypatch):
        monkeypatch.setenv("USER", "alice")
        monkeypatch.setenv("PASS", "wonderland")
        headers = build_auth_headers(
            {"type": "BASIC_AUTH", "credentials": {"username": "env:USER", "password": "env:PASS"}}, _no_op_secret_resolver
        )
        import base64

        assert headers["Authorization"] == "Basic " + base64.b64encode(b"alice:wonderland").decode("ascii")

    def test_unknown_auth_type_raises(self):
        with pytest.raises(ObservabilityConfigError, match="Unknown auth_config.type"):
            build_auth_headers({"type": "CARRIER_PIGEON"}, _no_op_secret_resolver)


class TestCompressPayload:
    def test_none_compression_returns_unchanged(self):
        data, header = compress_payload(b"hello", "none")
        assert data == b"hello"
        assert header is None

    def test_empty_string_compression_returns_unchanged(self):
        data, header = compress_payload(b"hello", "")
        assert data == b"hello"
        assert header is None

    def test_none_value_compression_returns_unchanged(self):
        data, header = compress_payload(b"hello", None)
        assert data == b"hello"
        assert header is None

    def test_gzip_compresses_and_round_trips(self):
        data, header = compress_payload(b"hello world", "gzip")
        assert header == "gzip"
        assert gzip.decompress(data) == b"hello world"

    def test_gzip_case_insensitive(self):
        data, header = compress_payload(b"hello", "GZIP")
        assert header == "gzip"
        assert gzip.decompress(data) == b"hello"

    def test_unsupported_compression_raises(self):
        with pytest.raises(ObservabilityConfigError, match="Unsupported compression"):
            compress_payload(b"hello", "brotli")


class TestComputeBackoffDelay:
    def test_exponential_backoff_default_multiplier(self):
        assert compute_backoff_delay_seconds(1, {}) == pytest.approx(1.0)
        assert compute_backoff_delay_seconds(2, {}) == pytest.approx(2.0)
        assert compute_backoff_delay_seconds(3, {}) == pytest.approx(4.0)

    def test_custom_backoff_multiplier(self):
        assert compute_backoff_delay_seconds(2, {"backoff_multiplier": 3.0}) == pytest.approx(3.0)

    def test_delay_capped(self):
        assert compute_backoff_delay_seconds(20, {}) <= 30.0

    def test_retry_after_header_takes_priority(self):
        assert compute_backoff_delay_seconds(1, {"backoff_multiplier": 2.0}, retry_after_header="7") == pytest.approx(7.0)

    def test_unparseable_retry_after_falls_back_to_exponential(self):
        assert compute_backoff_delay_seconds(1, {}, retry_after_header="not-a-number") == pytest.approx(1.0)


class TestMergeResourceAttributes:
    def test_no_extra_attributes_returns_same_object(self):
        assert merge_resource_attributes(_SAMPLE_RESOURCE_LOGS, {}) is _SAMPLE_RESOURCE_LOGS

    def test_new_attribute_added(self):
        merged = merge_resource_attributes(_SAMPLE_RESOURCE_LOGS, {"ddsource": "databricks-dlt"})
        keys = {a["key"] for a in merged[0]["resource"]["attributes"]}
        assert "ddsource" in keys
        assert "service.name" in keys  # original attribute preserved

    def test_existing_attribute_overridden(self):
        merged = merge_resource_attributes(_SAMPLE_RESOURCE_LOGS, {"service.name": "overridden-name"})
        values = {a["key"]: a["value"] for a in merged[0]["resource"]["attributes"]}
        assert values["service.name"]["stringValue"] == "overridden-name"
        assert len([a for a in merged[0]["resource"]["attributes"] if a["key"] == "service.name"]) == 1

    def test_original_payload_not_mutated(self):
        import copy

        original = copy.deepcopy(_SAMPLE_RESOURCE_LOGS)
        merge_resource_attributes(_SAMPLE_RESOURCE_LOGS, {"new_key": "new_value"})
        assert _SAMPLE_RESOURCE_LOGS == original


class TestDispatchToVolume:
    def _volume_destination(self, volume_path, compression="none"):
        return DestinationConfig(
            config_id="cfg1", dataflow_group_id="*", destination_id="dest-volume", destination_type="DATABRICKS_VOLUME",
            destination_config={"volume_path": volume_path, "compression": compression, "file_format": "JSONL"},
        )

    def test_writes_jsonl_file_uncompressed(self, tmp_path):
        destination = self._volume_destination(str(tmp_path))
        result = dispatch_to_volume(_SAMPLE_RESOURCE_LOGS, destination, "dfg_test", "run-1")
        assert result.status == "SUCCESS"
        assert os.path.exists(result.file_path)
        with open(result.file_path, "r", encoding="utf-8") as f:
            line = json.loads(f.readline())
        assert line["resourceLogs"][0] == _SAMPLE_RESOURCE_LOGS[0]

    def test_file_name_is_dataflow_group_id_and_task_run_id_not_a_random_uuid(self, tmp_path):
        destination = self._volume_destination(str(tmp_path))
        result = dispatch_to_volume(_SAMPLE_RESOURCE_LOGS, destination, "dfg_test", "run-1")
        assert os.path.basename(result.file_path) == "dfg_test_run-1.jsonl"

    def test_re_dispatch_with_same_dataflow_group_id_and_run_id_overwrites_deterministically(self, tmp_path):
        destination = self._volume_destination(str(tmp_path))
        first = dispatch_to_volume(_SAMPLE_RESOURCE_LOGS, destination, "dfg_test", "run-1")
        second = dispatch_to_volume(_SAMPLE_RESOURCE_LOGS, destination, "dfg_test", "run-1")
        assert first.file_path == second.file_path

    def test_writes_gzip_compressed_file(self, tmp_path):
        destination = self._volume_destination(str(tmp_path), compression="gzip")
        result = dispatch_to_volume(_SAMPLE_RESOURCE_LOGS, destination, "dfg_test", "run-1")
        assert result.status == "SUCCESS"
        assert result.file_path.endswith(".jsonl.gz")
        with gzip.open(result.file_path, "rt", encoding="utf-8") as f:
            line = json.loads(f.readline())
        assert line["resourceLogs"][0]["resource"]["attributes"][0]["key"] == "service.name"

    def test_missing_volume_path_fails_gracefully(self):
        destination = DestinationConfig(
            config_id="cfg1", dataflow_group_id="*", destination_id="dest-volume", destination_type="DATABRICKS_VOLUME", destination_config={},
        )
        result = dispatch_to_volume(_SAMPLE_RESOURCE_LOGS, destination, "dfg_test", "run-1")
        assert result.status == "FAILED"
        assert "volume_path is required" in result.error


class TestDispatchToOtlp:
    def _otlp_destination(self, endpoint="https://example.com/v1/logs", compression="none", retry_config=None):
        return DestinationConfig(
            config_id="cfg1", dataflow_group_id="*", destination_id="dest-otlp", destination_type="OTLP_CONSUMER",
            destination_config={"endpoint": endpoint, "compression": compression},
            auth_config={"type": "NONE"},
            retry_config=retry_config or {"max_attempts": 3},
        )

    class _FakeResponse:
        def __init__(self, status_code, text="", headers=None):
            self.status_code = status_code
            self.text = text
            self.headers = headers or {}

    def test_success_on_first_attempt(self):
        calls = []

        def fake_post(url, data, headers, timeout):
            calls.append((url, headers, timeout))
            return self._FakeResponse(200)

        destination = self._otlp_destination()
        result = dispatch_to_otlp(_SAMPLE_RESOURCE_LOGS, destination, _no_op_secret_resolver, post_fn=fake_post, sleep_fn=lambda s: None)
        assert result.status == "SUCCESS"
        assert result.attempts == 1
        assert len(calls) == 1

    def test_retries_on_429_then_succeeds(self):
        responses = [self._FakeResponse(429, headers={"Retry-After": "0"}), self._FakeResponse(200)]
        sleeps = []

        def fake_post(url, data, headers, timeout):
            return responses.pop(0)

        destination = self._otlp_destination(retry_config={"max_attempts": 3})
        result = dispatch_to_otlp(
            _SAMPLE_RESOURCE_LOGS, destination, _no_op_secret_resolver, post_fn=fake_post, sleep_fn=lambda s: sleeps.append(s)
        )
        assert result.status == "SUCCESS"
        assert result.attempts == 2
        assert len(sleeps) == 1

    def test_exhausts_retries_on_persistent_500(self):
        def fake_post(url, data, headers, timeout):
            return self._FakeResponse(500, text="internal error")

        destination = self._otlp_destination(retry_config={"max_attempts": 3})
        result = dispatch_to_otlp(_SAMPLE_RESOURCE_LOGS, destination, _no_op_secret_resolver, post_fn=fake_post, sleep_fn=lambda s: None)
        assert result.status == "FAILED"
        assert result.attempts == 3
        assert result.http_status_code == 500

    def test_non_retryable_4xx_fails_immediately(self):
        calls = []

        def fake_post(url, data, headers, timeout):
            calls.append(1)
            return self._FakeResponse(400, text="bad request")

        destination = self._otlp_destination(retry_config={"max_attempts": 5})
        result = dispatch_to_otlp(_SAMPLE_RESOURCE_LOGS, destination, _no_op_secret_resolver, post_fn=fake_post, sleep_fn=lambda s: None)
        assert result.status == "FAILED"
        assert len(calls) == 1  # never retried

    def test_network_exception_retried_then_succeeds(self):
        attempts = {"n": 0}

        def fake_post(url, data, headers, timeout):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("connection reset")
            return self._FakeResponse(200)

        destination = self._otlp_destination(retry_config={"max_attempts": 3})
        result = dispatch_to_otlp(_SAMPLE_RESOURCE_LOGS, destination, _no_op_secret_resolver, post_fn=fake_post, sleep_fn=lambda s: None)
        assert result.status == "SUCCESS"
        assert result.attempts == 2

    def test_missing_endpoint_fails_without_calling_post(self):
        calls = []
        destination = DestinationConfig(
            config_id="cfg1", dataflow_group_id="*", destination_id="dest-otlp", destination_type="OTLP_CONSUMER", destination_config={},
        )
        result = dispatch_to_otlp(_SAMPLE_RESOURCE_LOGS, destination, _no_op_secret_resolver, post_fn=lambda *a, **k: calls.append(1))
        assert result.status == "FAILED"
        assert calls == []

    def test_compression_sets_content_encoding_header(self):
        captured_headers = {}

        def fake_post(url, data, headers, timeout):
            captured_headers.update(headers)
            return self._FakeResponse(200)

        destination = self._otlp_destination(compression="gzip")
        dispatch_to_otlp(_SAMPLE_RESOURCE_LOGS, destination, _no_op_secret_resolver, post_fn=fake_post, sleep_fn=lambda s: None)
        assert captured_headers["Content-Encoding"] == "gzip"


class TestDispatchAll:
    def test_raises_when_no_destinations(self):
        with pytest.raises(ObservabilityDispatchError, match="No enabled observability_config destinations"):
            dispatch_all(_SAMPLE_RESOURCE_LOGS, [], "dfg_test", "run-1", _no_op_secret_resolver)

    def test_one_destination_failure_does_not_block_others(self, tmp_path, monkeypatch):
        good_volume = DestinationConfig(
            config_id="cfg1", dataflow_group_id="*", destination_id="dest-good", destination_type="DATABRICKS_VOLUME",
            destination_config={"volume_path": str(tmp_path)},
        )
        bad_otlp = DestinationConfig(
            config_id="cfg2", dataflow_group_id="*", destination_id="dest-bad", destination_type="OTLP_CONSUMER",
            destination_config={"endpoint": "https://unreachable.example.com"},
            retry_config={"max_attempts": 1},
        )

        def failing_post(url, data, headers, timeout):
            raise ConnectionError("dns failure")

        import flowx.lakeflow_framework.observability.destination_dispatcher as dispatcher_module

        real_dispatch_to_otlp = dispatcher_module.dispatch_to_otlp

        def patched_dispatch_to_otlp(resource_logs, destination, secret_resolver, post_fn=None, sleep_fn=None):
            return real_dispatch_to_otlp(resource_logs, destination, secret_resolver, post_fn=failing_post, sleep_fn=lambda s: None)

        monkeypatch.setattr(dispatcher_module, "dispatch_to_otlp", patched_dispatch_to_otlp)

        results = dispatch_all(_SAMPLE_RESOURCE_LOGS, [good_volume, bad_otlp], "dfg_test", "run-1", _no_op_secret_resolver)
        assert len(results) == 2
        by_id = {r.destination_id: r for r in results}
        assert by_id["dest-good"].status == "SUCCESS"
        assert by_id["dest-bad"].status == "FAILED"

    def test_all_destinations_failing_raises_dispatch_error(self):
        bad_volume = DestinationConfig(
            config_id="cfg1", dataflow_group_id="*", destination_id="dest-bad", destination_type="DATABRICKS_VOLUME", destination_config={},
        )
        with pytest.raises(ObservabilityDispatchError, match="All 1 destination"):
            dispatch_all(_SAMPLE_RESOURCE_LOGS, [bad_volume], "dfg_test", "run-1", _no_op_secret_resolver)
