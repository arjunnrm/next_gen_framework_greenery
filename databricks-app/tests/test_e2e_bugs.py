"""
E2E tests for MetaFlow Spec Builder:
  1. All 4 tabs present (Ingestion, Transformation, Reconciliation, Observability)
  2. Canonical attributes matching pipeline_onboarding_template.json
  3. Preloaded template values
  4. Real-time JSON and YAML preview synchronization
  5. Save to Local, Databricks Workspace, and Unity Catalog Volume
  6. Path traversal security checks
"""
import os
import json
from fastapi.testclient import TestClient
import pytest

os.environ["METAFLOW_FAKE_DBX"] = "1"

from server.app import app

client = TestClient(app)


def test_spa_loads_with_canonical_schema(frontend_text):
    """Verify the frontend ships all 4 flow tabs, the theme system, and canonical attributes."""
    body = frontend_text
    assert "MetaFlow" in body or "Metaflow" in body, "Brand title missing"
    assert "data-mfl" in body, "Theme attribute missing"
    # Verify the 4 tabs
    assert "Ingestion" in body, "Ingestion tab missing"
    assert "Transformation" in body, "Transformation tab missing"
    assert "Reconciliation" in body, "Reconciliation tab missing"
    assert "Observability" in body, "Observability tab missing"
    # Verify canonical attributes from pipeline_onboarding_template.json
    assert "dataflow_group_id" in body, "dataflow_group_id missing"
    assert "source_config" in body, "source_config missing"
    assert "target_config" in body, "target_config missing"
    assert "cdc_load_strategy" in body, "cdc_load_strategy missing"
    assert "dq_config" in body, "dq_config missing"
    assert "governance_tags" in body, "governance_tags missing"


def test_config_endpoint_has_registry():
    """Verify /api/config returns registry with all flow sections."""
    res = client.get("/api/config")
    assert res.status_code == 200, f"/api/config returned {res.status_code}"
    data = res.json()
    assert "registry" in data, "registry key missing"
    reg = data["registry"]
    assert "ingestion" in reg, "ingestion registry missing"
    assert len(reg["ingestion"]) > 0, "ingestion registry is empty"


def test_render_endpoint_canonical_spec():
    """Verify /api/spec/render handles canonical pipeline_onboarding_template.json."""
    canonical_path = os.path.join(
        os.path.dirname(__file__), "..", "templates", "pipeline_onboarding_template.json"
    )
    with open(canonical_path, "r", encoding="utf-8") as f:
        template_spec = json.load(f)

    # Render as JSON
    res_json = client.post("/api/spec/render", json={"spec": template_spec, "format": "json"})
    assert res_json.status_code == 200, f"JSON render failed: {res_json.text}"
    content_json = res_json.json()["content"]
    assert "dfg_template_example" in content_json
    assert "df_template_ingest" in content_json

    # Render as YAML
    res_yaml = client.post("/api/spec/render", json={"spec": template_spec, "format": "yaml"})
    assert res_yaml.status_code == 200, f"YAML render failed: {res_yaml.text}"
    content_yaml = res_yaml.json()["content"]
    assert "dfg_template_example" in content_yaml


def test_workspace_and_volume_write_endpoints():
    """Verify write endpoint handles both workspace and volume storage roots.

    This used to accept `status_code in (200, 500)`, which is not an assertion about
    saving at all — it passed just as happily when every save failed, and did, for both
    roots. A write test has to insist on success and then read the bytes back; see
    tests/test_storage_roundtrip.py for the full round trip.
    """
    # Test Workspace
    res_ws = client.post("/api/workspace/write", json={
        "root_id": "ws_specs",
        "path": "/Workspace/Shared/metaflow/specs/onboarding_test.json",
        "content": json.dumps({"dataflow_group_id": "dfg_ws_test"}),
        "overwrite": True,
    })
    assert res_ws.status_code == 200, res_ws.text

    # Test Volume
    res_vol = client.post("/api/workspace/write", json={
        "root_id": "vol_specs",
        "path": "/Volumes/metaflow/metaflow/onboarding_specs/onboarding_test.json",
        "content": json.dumps({"dataflow_group_id": "dfg_vol_test"}),
        "overwrite": True,
    })
    assert res_vol.status_code == 200, res_vol.text


def test_workspace_write_traversal_prevention():
    """Verify path traversal is rejected with 403."""
    res = client.post("/api/workspace/write", json={
        "root_id": "ws_specs",
        "path": "/Workspace/Shared/../../../etc/shadow",
        "content": "illegal",
        "overwrite": True,
    })
    assert res.status_code == 403
    assert "traversal" in res.json().get("error", {}).get("message", "").lower()
