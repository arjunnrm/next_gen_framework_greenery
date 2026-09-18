import os
from fastapi.testclient import TestClient
import pytest
from server.app import app

os.environ["METAFLOW_FAKE_DBX"] = "1"
client = TestClient(app)


def test_access_preflight_report():
    res = client.get("/api/workspace/access?root_id=vol_specs")
    assert res.status_code == 200
    data = res.json()
    assert "checks" in data
    check_ids = [c["id"] for c in data["checks"]]
    assert "workspace_reachable" in check_ids
    assert "identity" in check_ids
    assert "root_exists" in check_ids
    assert "can_read" in check_ids
    assert "can_write" in check_ids


def test_action_run_and_status():
    spec_doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test_run"}},
        "ingestion_flows": [
            {
                "v": {
                    "dataflow_id": "df_1",
                    "source_type": "autoloader",
                    "source_system": "crm_api",
                    "source_config.path": "/Volumes/flowx/landing/incoming/",
                    "source_config.format": "csv",
                    "source_config.schema_location": "/Volumes/flowx/landing/_schemas/crm/",
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
    # Run local validate action
    res = client.post("/api/actions/validate/run", json={"spec": spec_doc, "confirmed": True})
    assert res.status_code == 200
    run_data = res.json()
    assert "run_id" in run_data
    run_id = run_data["run_id"]

    # Poll status
    status_res = client.get(f"/api/actions/runs/{run_id}")
    assert status_res.status_code == 200
    status_data = status_res.json()
    assert status_data["state"] == "SUCCESS"
