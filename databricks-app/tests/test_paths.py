import os
from fastapi.testclient import TestClient
import pytest
from server.app import app

os.environ["METAFLOW_FAKE_DBX"] = "1"
client = TestClient(app)


def test_path_traversal_prevention_on_read():
    res = client.get("/api/workspace/read?root_id=vol_specs&path=../../../etc/passwd.json")
    assert res.status_code in (400, 403, 404)
    data = res.json()
    assert "error" in data


def test_unsupported_file_extension():
    res = client.get("/api/workspace/read?root_id=vol_specs&path=sample.exe")
    assert res.status_code in (400, 404)
    data = res.json()
    assert "error" in data
    assert data["error"]["code"] in ("UNSUPPORTED_FORMAT", "NOT_FOUND")


def test_doc_link_resolution():
    res = client.get("/api/docs/resolve?path=source_config.remove_dups")
    assert res.status_code == 200
    data = res.json()
    assert "url" in data
    assert "00_master_reference_index" in data["url"] or "attribute_reference" in data["url"]
