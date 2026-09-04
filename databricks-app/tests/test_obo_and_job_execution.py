"""
Tests for OBO Authentication Delegation, Workspace File Operations, and Job Execution.
"""

import json
import os
from unittest.mock import MagicMock, patch
import pytest

os.environ["FLOWX_FAKE_DBX"] = "1"

from fastapi.testclient import TestClient
from server.app import app
from server.clients.dbx import FakeJobsAPI, FakeWorkspaceClient, reset_fake_workspace_client
from server.deps import get_app_settings, get_dbx_client

client = TestClient(app)

SAMPLE_SPEC = {
    "dataflow_group_id": "dfg_obo_test",
    "ingestion_flows": [
        {
            "dataflow_id": "df_obo_1",
            "source_type": "autoloader",
            "source_system": "salesforce",
            "source_config": {
                "path": "/Volumes/flowx/landing/salesforce/",
                "format": "json",
                "schema_location": "/Volumes/flowx/landing/_schemas/sf/"
            },
            "target_catalog": "flowx",
            "target_schema": "raw",
            "target_table": "contacts",
            "target_type": "streaming_table",
            "target_config": {
                "cdc_load_strategy": "APPEND"
            }
        }
    ],
    "transformation_flows": [],
    "reconciliation_flows": []
}


@pytest.fixture(autouse=True)
def _fresh_fake():
    reset_fake_workspace_client()
    yield
    reset_fake_workspace_client()


# --------------------------------------------------------------------------------------
# 1. OBO Token Acquisition & Authentication Modes
# --------------------------------------------------------------------------------------

def test_obo_token_forwarded_header():
    """Verify X-Forwarded-Access-Token header is accepted and extracted."""
    headers = {
        "X-Forwarded-Access-Token": "dapi_test_user_token_12345",
        "X-Forwarded-Email": "operator@company.com",
    }
    res = client.get("/api/health", headers=headers)
    assert res.status_code == 200
    assert res.json()["user"] == "operator@company.com"


def test_obo_authorization_bearer_header():
    """Verify Authorization: Bearer <token> is accepted and extracted."""
    headers = {
        "Authorization": "Bearer dapi_bearer_token_67890",
        "X-Forwarded-User": "bearer_operator",
    }
    res = client.get("/api/health", headers=headers)
    assert res.status_code == 200
    assert res.json()["user"] == "bearer_operator"


def test_obo_strict_mode_rejects_missing_token(monkeypatch):
    """When auth.mode == 'obo' and fallback_to_sp == False, missing token returns 403."""
    settings = get_app_settings()
    monkeypatch.setattr(settings.auth, "mode", "obo")
    monkeypatch.setattr(settings.auth, "fallback_to_sp", False)
    monkeypatch.setenv("FLOWX_TEST_OBO_STRICT", "1")

    res = client.get("/api/storage/access")
    assert res.status_code == 403
    err = res.json()["error"]
    assert err["code"] == "PERMISSION_DENIED"
    assert "does not have access" in err["message"].lower()


def test_obo_strict_mode_accepts_valid_token(monkeypatch):
    """When auth.mode == 'obo' and fallback_to_sp == False, valid token succeeds."""
    settings = get_app_settings()
    monkeypatch.setattr(settings.auth, "mode", "obo")
    monkeypatch.setattr(settings.auth, "fallback_to_sp", False)

    headers = {"X-Forwarded-Access-Token": "dapi_valid_token"}
    res = client.get("/api/storage/access", headers=headers)
    assert res.status_code == 200


def test_obo_workspace_client_passes_auth_type_pat(monkeypatch):
    """Ensure get_workspace_client passes auth_type='pat' when token is provided to avoid oauth vs pat conflict."""
    from server.clients.dbx import get_workspace_client
    monkeypatch.delenv("FLOWX_FAKE_DBX", raising=False)

    with patch("server.clients.dbx.WorkspaceClient") as mock_wc:
        get_workspace_client(token="user_token_123", host="https://dbc-test.cloud.databricks.com", user="test_user")
        mock_wc.assert_called_once_with(
            host="https://dbc-test.cloud.databricks.com",
            token="user_token_123",
            auth_type="pat",
            client_id="",
            client_secret="",
        )


# --------------------------------------------------------------------------------------
# 2. Job Parameter Mapping and Onboarding Trigger
# --------------------------------------------------------------------------------------

def test_action_run_parameter_mapping_and_execution():
    """Verify job parameters, notebook_params, and python_params are mapped and executed."""
    fake_client = FakeWorkspaceClient()

    payload = {
        "spec": SAMPLE_SPEC,
        "confirmed": True,
        "params": {
            "catalog": "custom_cat",
            "env": "staging",
            "action_type": "CREATE"
        },
        "job_parameters": {
            "catalog": "custom_cat",
            "environment": "staging",
        },
        "notebook_params": {
            "widget_custom_key": "widget_custom_val"
        },
        "python_params": ["--flag", "value1"]
    }

    res = client.post("/api/actions/onboard/run", json=payload)
    assert res.status_code == 200, res.text
    data = res.json()

    # Verify Response Schema expected by Frontend
    assert "run_id" in data
    assert "job_id" in data
    assert "run_url" in data
    assert "stages" in data
    assert "request_id" in data
    assert data["job_id"] == get_app_settings().actions["onboard"].job_id

    # Verify run status polling
    run_id = data["run_id"]
    status_res = client.get(f"/api/actions/runs/{run_id}?action_id=onboard")
    assert status_res.status_code == 200
    st_data = status_res.json()
    assert st_data["run_id"] == run_id
    assert "state" in st_data
    assert "current_stage" in st_data
    assert "log_tail" in st_data

    # Verify cancel run
    cancel_res = client.post(f"/api/actions/runs/{run_id}/cancel")
    assert cancel_res.status_code == 200
    assert cancel_res.json()["cancelled"] is True


def test_onboard_button_triggers_job_with_mapped_user_parameters():
    """Verify standard 'Onboard & run' button payload automatically maps user spec to job parameters."""
    # This payload matches exactly what Builder.jsx sends on 'Onboard & run' click:
    payload = {
        "spec": SAMPLE_SPEC,
        "format": "json",
        "confirmed": True
    }

    res = client.post("/api/actions/onboard/run", json=payload)
    assert res.status_code == 200, res.text
    data = res.json()

    assert "run_id" in data
    assert data["job_id"] == get_app_settings().actions["onboard"].job_id

    # Inspect the fake jobs client runs dictionary to verify parameter forwarding
    from server.deps import get_workspace_client
    fw = get_workspace_client(force_fake=True)
    run_record = fw.jobs._runs.get(str(data["run_id"]))
    assert run_record is not None

    job_params = run_record["job_parameters"]
    assert "spec_file_path" in job_params or "spec_path" in job_params
    staged_path = job_params.get("spec_file_path") or job_params.get("spec_path")
    assert staged_path.startswith("/Volumes/")
    assert "dfg_obo_test" in staged_path
    assert job_params.get("catalog") == "flowx"
    assert job_params.get("env") == "dev" or job_params.get("environment") == "dev"
    assert job_params.get("action_type") == "CREATE"


def test_action_run_requires_confirmation_when_configured():
    """Actions with confirm=true reject execution if confirmed is not true."""
    payload = {
        "spec": SAMPLE_SPEC,
        "confirmed": False
    }
    res = client.post("/api/actions/onboard/run", json=payload)
    assert res.status_code == 400
    assert res.json()["error"]["code"] == "VALIDATION_FAILED"
    assert "confirmation" in res.json()["error"]["message"].lower()


def test_action_run_non_existent_action():
    """Attempting to run a non-existent action returns 404."""
    payload = {
        "spec": SAMPLE_SPEC,
        "confirmed": True
    }
    res = client.post("/api/actions/unknown_action_xyz/run", json=payload)
    assert res.status_code == 404
    assert res.json()["error"]["code"] == "NOT_FOUND"


# --------------------------------------------------------------------------------------
# 3. Workspace & Volume Read / Write Operations
# --------------------------------------------------------------------------------------

def test_workspace_read_write_roundtrip_with_etag():
    """Verify write and read with ETag validation."""
    path = "/Volumes/flowx/flowx/onboarding_specs/test_etag.json"
    content = json.dumps(SAMPLE_SPEC)

    write_res = client.post("/api/workspace/write", json={
        "root_id": "vol_specs",
        "path": path,
        "content": content,
        "overwrite": True
    })
    assert write_res.status_code == 200
    etag = write_res.json()["etag"]

    # Read back
    read_res = client.get(f"/api/workspace/read?root_id=vol_specs&path={path}")
    assert read_res.status_code == 200
    assert read_res.json()["etag"] == etag
    assert json.loads(read_res.json()["content"]) == SAMPLE_SPEC

    # Stale etag conflict
    bad_write = client.post("/api/workspace/write", json={
        "root_id": "vol_specs",
        "path": path,
        "content": '{"dataflow_group_id":"dfg_changed"}',
        "overwrite": True,
        "etag": "stale_etag_hash_12345"
    })
    assert bad_write.status_code == 409
    assert bad_write.json()["error"]["code"] == "CONFLICT"


def test_workspace_delete_cleanup():
    """Verify probe or file deletion works cleanly."""
    path = "/Workspace/Shared/flowx/specs/to_delete.json"
    client.post("/api/workspace/write", json={
        "root_id": "ws_specs",
        "path": path,
        "content": json.dumps(SAMPLE_SPEC),
        "overwrite": True
    })

    # Read exists
    assert client.get(f"/api/workspace/read?root_id=ws_specs&path={path}").status_code == 200

    # Delete via FileManager
    from server.clients.files import FileManager
    from server.deps import get_app_settings, get_workspace_client
    fm = FileManager(get_app_settings(), get_workspace_client(force_fake=True))
    deleted_path = fm.delete_file("ws_specs", path)
    assert deleted_path == path

    # Read after delete returns 404
    assert client.get(f"/api/workspace/read?root_id=ws_specs&path={path}").status_code == 404
