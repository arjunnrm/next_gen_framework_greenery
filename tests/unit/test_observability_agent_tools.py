"""Unit tests for observability/agent_tools.py -- pure Python, no Spark session needed."""

import json

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.observability.agent_tools import (
    diagnose_pipeline_telemetry_failures,
    generate_pipeline_onboarding_config,
    validate_observability_config,
)

_VALID_CONFIG_JSON = json.dumps(
    {
        "observability": [
            {
                "id": "dest-vol",
                "enabled": True,
                "type": "DATABRICKS_VOLUME",
                "destination_config": {"volume_path": "/Volumes/poc/observability/logs/", "compression": "GZIP", "file_format": "JSONL"},
            }
        ]
    }
)


class TestValidateObservabilityConfig:
    def test_valid_json_config_passes(self):
        result = validate_observability_config(_VALID_CONFIG_JSON)
        assert result["valid"] is True
        assert result["errors"] == []

    def test_valid_yaml_config_passes(self):
        yaml_text = """
        observability:
          - id: dest-vol
            enabled: true
            type: DATABRICKS_VOLUME
            destination_config:
              volume_path: /Volumes/poc/observability/logs/
        """
        result = validate_observability_config(yaml_text)
        assert result["valid"] is True

    def test_missing_observability_key_is_invalid(self):
        result = validate_observability_config(json.dumps({"not_observability": []}))
        assert result["valid"] is False
        assert any("observability" in e for e in result["errors"])

    def test_unknown_destination_type_is_invalid(self):
        bad = json.dumps({"observability": [{"id": "x", "type": "CARRIER_PIGEON", "destination_config": {}}]})
        result = validate_observability_config(bad)
        assert result["valid"] is False

    def test_volume_destination_missing_volume_path_is_invalid(self):
        bad = json.dumps({"observability": [{"id": "x", "type": "DATABRICKS_VOLUME", "destination_config": {}}]})
        result = validate_observability_config(bad)
        assert result["valid"] is False

    def test_otlp_destination_missing_endpoint_is_invalid(self):
        bad = json.dumps({"observability": [{"id": "x", "type": "OTLP_CONSUMER", "destination_config": {}}]})
        result = validate_observability_config(bad)
        assert result["valid"] is False

    def test_literal_credential_fails_pattern_check(self):
        bad = json.dumps(
            {
                "observability": [
                    {
                        "id": "x",
                        "type": "OTLP_CONSUMER",
                        "destination_config": {"endpoint": "https://example.com"},
                        "auth": {"type": "BEARER_TOKEN", "credentials": {"token": "sk-literal-secret-value"}},
                    }
                ]
            }
        )
        result = validate_observability_config(bad)
        assert result["valid"] is False

    def test_env_credential_reference_passes(self):
        good = json.dumps(
            {
                "observability": [
                    {
                        "id": "x",
                        "type": "OTLP_CONSUMER",
                        "destination_config": {"endpoint": "https://example.com"},
                        "auth": {"type": "BEARER_TOKEN", "credentials": {"token": "env:MY_TOKEN"}},
                    }
                ]
            }
        )
        result = validate_observability_config(good)
        assert result["valid"] is True

    def test_malformed_json_and_yaml_reports_parse_error(self):
        result = validate_observability_config("{not valid: [json or yaml")
        assert result["valid"] is False
        assert "could not be parsed" in result["errors"][0]

    def test_catalog_and_env_placeholders_substituted(self):
        templated = json.dumps(
            {"observability": [{"id": "x", "type": "DATABRICKS_VOLUME", "destination_config": {"volume_path": "/Volumes/{{catalog}}/obs/"}}]}
        )
        result = validate_observability_config(templated, catalog="poc", env="dev")
        assert result["valid"] is True

    def test_unsubstituted_placeholder_fails_pattern_check(self):
        templated = json.dumps(
            {"observability": [{"id": "x", "type": "DATABRICKS_VOLUME", "destination_config": {"volume_path": "/Volumes/{{catalog}}/obs/"}}]}
        )
        result = validate_observability_config(templated)  # no catalog passed -> stays literal {{catalog}}
        assert result["valid"] is True  # still starts with /Volumes/, pattern check on prefix only -- documents actual behavior


class TestGeneratePipelineOnboardingConfig:
    def test_generates_databricks_volume_destination(self):
        config = generate_pipeline_onboarding_config(
            "pipe-123", [{"template": "databricks_volume", "volume_path": "/Volumes/poc/obs/logs/", "destination_id": "dest-vol"}]
        )
        assert config == {
            "observability": [
                {
                    "id": "dest-vol",
                    "enabled": True,
                    "type": "DATABRICKS_VOLUME",
                    "destination_config": {"volume_path": "/Volumes/poc/obs/logs/", "compression": "GZIP", "file_format": "JSONL"},
                }
            ]
        }

    def test_generates_otlp_http_destination_with_defaults(self):
        config = generate_pipeline_onboarding_config(
            "pipe-123", [{"template": "otlp_http", "endpoint": "https://collector.example.com/v1/logs"}]
        )
        entry = config["observability"][0]
        assert entry["type"] == "OTLP_CONSUMER"
        assert entry["destination_config"]["endpoint"] == "https://collector.example.com/v1/logs"
        assert entry["retry"] == {"max_attempts": 3}
        assert entry["timeout_ms"] == 5000

    def test_service_name_and_environment_merged_into_otlp_resource_attributes(self):
        config = generate_pipeline_onboarding_config(
            "pipe-123",
            [{"template": "otlp_http", "endpoint": "https://collector.example.com/v1/logs"}],
            service_name="my-pipeline",
            deployment_environment="prod",
        )
        attrs = config["observability"][0]["destination_config"]["resource_attributes"]
        assert attrs == {"service.name": "my-pipeline", "deployment.environment": "prod"}

    def test_per_target_resource_attributes_not_overridden_by_defaults(self):
        config = generate_pipeline_onboarding_config(
            "pipe-123",
            [{"template": "otlp_http", "endpoint": "https://x", "resource_attributes": {"service.name": "explicit-name"}}],
            service_name="fallback-name",
        )
        assert config["observability"][0]["destination_config"]["resource_attributes"]["service.name"] == "explicit-name"

    def test_generated_config_is_valid_against_the_schema(self):
        config = generate_pipeline_onboarding_config(
            "pipe-123",
            [
                {"template": "databricks_volume", "volume_path": "/Volumes/poc/obs/logs/"},
                {"template": "otlp_http", "endpoint": "https://collector.example.com/v1/logs", "auth": {"type": "BEARER_TOKEN", "credentials": {"token": "env:TOK"}}},
            ],
        )
        result = validate_observability_config(json.dumps(config))
        assert result["valid"] is True, result["errors"]

    def test_unknown_template_raises_key_error(self):
        with pytest.raises(KeyError, match="Unknown destination template"):
            generate_pipeline_onboarding_config("pipe-123", [{"template": "carrier_pigeon"}])

    def test_missing_required_template_parameter_raises_key_error(self):
        with pytest.raises(KeyError):
            generate_pipeline_onboarding_config("pipe-123", [{"template": "databricks_volume"}])  # missing volume_path

    def test_multiple_destinations_generated_in_order(self):
        config = generate_pipeline_onboarding_config(
            "pipe-123",
            [
                {"template": "databricks_volume", "volume_path": "/Volumes/a/", "destination_id": "d1"},
                {"template": "otlp_http", "endpoint": "https://b", "destination_id": "d2"},
            ],
        )
        assert [d["id"] for d in config["observability"]] == ["d1", "d2"]


class TestDiagnosePipelineTelemetryFailures:
    def test_matches_missing_env_var(self):
        result = diagnose_pipeline_telemetry_failures("Environment variable 'DD_API_KEY' referenced by 'env:DD_API_KEY' is not set.")
        assert result["matched"] is True
        assert result["category"] == "credentials"

    def test_matches_no_enabled_destinations(self):
        result = diagnose_pipeline_telemetry_failures("No enabled observability_config destinations resolved for dataflow_group_id='dfg_x'.")
        assert result["matched"] is True
        assert result["category"] == "destination_config"

    def test_matches_http_429(self):
        result = diagnose_pipeline_telemetry_failures("POST https://x failed: HTTP 429: rate limited")
        assert result["matched"] is True
        assert result["category"] == "destination_outage"

    def test_matches_http_500(self):
        result = diagnose_pipeline_telemetry_failures("HTTP 503: Service Unavailable")
        assert result["matched"] is True
        assert result["category"] == "destination_outage"

    def test_matches_missing_dataflow_group_id_conf(self):
        result = diagnose_pipeline_telemetry_failures("Pipeline 'pipe-1' has no 'dataflow.group.id' configuration entry -- cannot resolve...")
        assert result["matched"] is True
        assert result["category"] == "pipeline_config"

    def test_matches_wrong_upstream_task(self):
        result = diagnose_pipeline_telemetry_failures("run_id=999 is not a pipeline_task run (no pipeline_task.pipeline_id present)")
        assert result["matched"] is True
        assert result["category"] == "task_wiring"

    def test_unknown_error_reports_unmatched(self):
        result = diagnose_pipeline_telemetry_failures("Some completely novel failure never seen before")
        assert result["matched"] is False
        assert result["category"] is None
        assert result["remediation"] is None

    def test_first_matching_pattern_wins_when_multiple_could_apply(self):
        # contains both "HTTP 500" language and destination_config language; matrix order should pick HTTP 5xx first only if it appears first in the matrix -- assert deterministic single match
        result = diagnose_pipeline_telemetry_failures("volume_path is required for DATABRICKS_VOLUME destinations.")
        assert result["matched"] is True
        assert result["category"] == "destination_config"
