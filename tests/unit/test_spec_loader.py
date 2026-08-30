"""Unit tests for onboarding/spec_loader.py -- pure Python, no Spark needed.

``read_raw_spec_text`` tries a plain ``open()`` before falling back to ``dbutils.fs.head``,
so passing ``dbutils=None`` is safe here: every path under test is a real local file, and
the fallback is never reached.
"""

import json
from pathlib import Path

import pytest
import yaml

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import OnboardingValidationError
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_loader import (
    load_and_template_spec,
    substitute_environment_placeholders,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_JSON = REPO_ROOT / "onboarding_templates" / "pipeline_onboarding_template.json"
TEMPLATE_YAML = REPO_ROOT / "onboarding_templates" / "pipeline_onboarding_template.yaml"


def test_substitute_environment_placeholders_replaces_both_tokens():
    raw = '{"path": "/Volumes/{{catalog}}/landing/x/", "env": "{{env}}"}'
    result = substitute_environment_placeholders(raw, "poc", "dev")
    assert result == '{"path": "/Volumes/poc/landing/x/", "env": "dev"}'


def test_json_and_yaml_templates_are_present_on_disk():
    assert TEMPLATE_JSON.exists(), f"missing {TEMPLATE_JSON}"
    assert TEMPLATE_YAML.exists(), f"missing {TEMPLATE_YAML}"


def test_json_and_yaml_templates_parse_to_the_same_structure():
    """The whole point of supporting both formats: they must be genuinely equivalent."""
    json_spec, _, _ = load_and_template_spec(None, str(TEMPLATE_JSON), "poc", "dev")
    yaml_spec, _, _ = load_and_template_spec(None, str(TEMPLATE_YAML), "poc", "dev")
    assert json_spec == yaml_spec


def test_load_and_template_spec_substitutes_catalog_placeholder():
    spec, _, _ = load_and_template_spec(None, str(TEMPLATE_JSON), "my_catalog", "staging")
    ingestion_flow = spec["ingestion_flows"][0]
    assert ingestion_flow["target_catalog"] == "my_catalog"
    assert ingestion_flow["source_config"]["path"].startswith("/Volumes/my_catalog/")


def test_spec_version_is_deterministic_for_identical_content(tmp_path):
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps({"dataflow_group_id": "dfg_x", "ingestion_flows": []}), encoding="utf-8")
    _, _, version_a = load_and_template_spec(None, str(spec_path), "poc", "dev")
    _, _, version_b = load_and_template_spec(None, str(spec_path), "poc", "dev")
    assert version_a == version_b
    assert len(version_a) == 16


def test_spec_version_changes_when_content_changes(tmp_path):
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps({"dataflow_group_id": "dfg_x", "ingestion_flows": []}), encoding="utf-8")
    _, _, version_a = load_and_template_spec(None, str(spec_path), "poc", "dev")
    spec_path.write_text(json.dumps({"dataflow_group_id": "dfg_y", "ingestion_flows": []}), encoding="utf-8")
    _, _, version_b = load_and_template_spec(None, str(spec_path), "poc", "dev")
    assert version_a != version_b


def test_malformed_json_raises_onboarding_validation_error(tmp_path):
    spec_path = tmp_path / "broken.json"
    spec_path.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(OnboardingValidationError, match="not valid JSON"):
        load_and_template_spec(None, str(spec_path), "poc", "dev")


def test_malformed_yaml_raises_onboarding_validation_error(tmp_path):
    spec_path = tmp_path / "broken.yaml"
    spec_path.write_text("dataflow_group_id: dfg_x\n  bad_indent: [1, 2\n", encoding="utf-8")
    with pytest.raises(OnboardingValidationError, match="not valid YAML"):
        load_and_template_spec(None, str(spec_path), "poc", "dev")


def test_non_object_top_level_raises_onboarding_validation_error(tmp_path):
    spec_path = tmp_path / "list.json"
    spec_path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")
    with pytest.raises(OnboardingValidationError, match="not an object"):
        load_and_template_spec(None, str(spec_path), "poc", "dev")


def test_missing_file_raises_onboarding_validation_error():
    with pytest.raises(OnboardingValidationError, match="Failed to read spec file"):
        load_and_template_spec(None, "/path/does/not/exist.json", "poc", "dev")


def test_yaml_template_is_valid_yaml_independently_of_the_loader():
    """Sanity check the fixture itself, independent of our own parser wrapper."""
    raw = TEMPLATE_YAML.read_text(encoding="utf-8")
    templated = substitute_environment_placeholders(raw, "poc", "dev")
    parsed = yaml.safe_load(templated)
    assert parsed["dataflow_group_id"] == "dfg_template_example"
    assert parsed["ingestion_flows"][0]["source_type"] == "autoloader"
