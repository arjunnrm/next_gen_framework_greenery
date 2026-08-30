import os
from fastapi.testclient import TestClient
import pytest
from server.app import app

os.environ["METAFLOW_FAKE_DBX"] = "1"
client = TestClient(app)


def test_health_endpoint():
    res = client.get("/api/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert data["config_ok"] is True
    assert "framework_version" in data
    assert "X-Request-Id" in res.headers


def test_config_endpoint():
    res = client.get("/api/config")
    assert res.status_code == 200
    data = res.json()
    assert "app" in data
    assert "registry" in data
    assert "phases" in data
    assert "templates" in data
    assert "theme" in data


def test_spec_validate_endpoint():
    spec_doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test_validate"}},
        "ingestion_flows": [
            {
                "v": {
                    "dataflow_id": "df_1",
                    "source_type": "autoloader",
                    "source_system": "crm_api",
                    "source_config.path": "/Volumes/metaflow/landing/incoming/",
                    "source_config.format": "csv",
                    "source_config.schema_location": "/Volumes/metaflow/landing/_schemas/crm/",
                    "target_catalog": "poc",
                    "target_schema": "dev",
                    "target_table": "raw",
                    "target_type": "streaming_table",
                    "target_config.cdc_load_strategy": "APPEND"
                }
            }
        ],
        "transformation_flows": [],
        "reconciliation_flows": []
    }
    res = client.post("/api/spec/validate", json={"spec": spec_doc})
    assert res.status_code == 200
    data = res.json()
    assert data["ok"] is True


def test_spec_render_endpoint():
    spec_doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test_render"}},
        "ingestion_flows": [],
        "transformation_flows": [],
        "reconciliation_flows": []
    }
    res = client.post("/api/spec/render", json={"spec": spec_doc, "format": "json"})
    assert res.status_code == 200
    data = res.json()
    assert "content" in data
    assert "bytes" in data
    assert data["format"] == "json"


def test_error_envelope_structure():
    # Calling non-existent template
    res = client.get("/api/templates/non_existent_template_xyz")
    assert res.status_code == 404
    data = res.json()
    assert "error" in data
    err = data["error"]
    assert err["code"] == "NOT_FOUND"
    assert "request_id" in err
    assert "message" in err
