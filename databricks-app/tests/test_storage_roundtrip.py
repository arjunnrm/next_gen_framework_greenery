"""
Storage round-trip and SDK-contract tests for Volume and Workspace spec files.

Every test here corresponds to a defect that shipped: save and open were both broken
against a real workspace while the whole suite stayed green, because the only coverage
storage had was "the endpoint returned 200 or 500" and because the offline fake accepted
call shapes the real Databricks SDK rejects.

Two kinds of test are therefore needed and both are here:

  * round-trip tests — write bytes, then read them back and assert they are *the same
    bytes*. A status code alone cannot tell a successful save from one that base64'd the
    payload into the file or dropped it into an in-memory dict.
  * contract tests — assert the fake's call signatures and return shapes still match the
    real SDK's. Without these the fake is free to drift back into accepting what the SDK
    does not, and the round-trip tests go back to proving nothing.
"""

import base64
import dataclasses
import inspect
import io
import json
import os
import typing

import pytest

os.environ.setdefault("FLOWX_FAKE_DBX", "1")

from fastapi.testclient import TestClient  # noqa: E402

from server.app import app  # noqa: E402
from server.clients.dbx import FakeFilesAPI, FakeWorkspaceAPI, reset_fake_workspace_client  # noqa: E402

client = TestClient(app)

VOL_ROOT = "vol_specs"
WS_ROOT = "ws_specs"

SPEC = {
    "dataflow_group_id": "dfg_roundtrip",
    "ingestion_flows": [],
    "transformation_flows": [],
    "reconciliation_flows": [],
}


@pytest.fixture(autouse=True)
def _fresh_fake():
    reset_fake_workspace_client()
    yield
    reset_fake_workspace_client()


def _root_path(root_id):
    cfg = client.get("/api/config").json()
    roots = cfg["spec_storage"]["roots"]
    return next(r["path"] for r in roots if r["id"] == root_id)


# --------------------------------------------------------------------------------------
# Round-trip: what was saved is what comes back
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_write_then_read_returns_identical_content(root_id):
    """A saved spec reads back byte-for-byte.

    The workspace path used to fail this twice over: import_ was handed raw bytes (not
    JSON serializable) and export's base64 `content` was decoded as if it were UTF-8
    text, so a read that got that far returned base64 gibberish rather than the spec.
    """
    path = _root_path(root_id) + "roundtrip.json"
    body = json.dumps(SPEC, indent=2)

    res_w = client.post("/api/storage/write", json={
        "root_id": root_id, "path": path, "content": body, "overwrite": True,
    })
    assert res_w.status_code == 200, res_w.text
    assert res_w.json()["bytes"] == len(body.encode("utf-8"))

    res_r = client.get(f"/api/storage/read?root_id={root_id}&path={path}")
    assert res_r.status_code == 200, res_r.text
    got = res_r.json()

    assert got["content"] == body, "content did not survive the write/read round trip"
    assert json.loads(got["content"]) == SPEC
    assert got["format"] == "json"
    assert got["etag"] == res_w.json()["etag"]


@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_written_file_appears_in_listing(root_id):
    """A saved spec is listed by the browser the Open dialog uses."""
    path = _root_path(root_id) + "listed_me.json"
    client.post("/api/storage/write", json={
        "root_id": root_id, "path": path, "content": json.dumps(SPEC), "overwrite": True,
    })

    res = client.get(f"/api/storage/list?root_id={root_id}")
    assert res.status_code == 200, res.text
    names = [e["name"] for e in res.json()["entries"]]
    assert "listed_me.json" in names, f"saved file missing from listing: {names}"


@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_yaml_round_trip(root_id):
    """.yaml is an allowed extension, so it must survive the same round trip as .json."""
    path = _root_path(root_id) + "roundtrip.yaml"
    body = "dataflow_group_id: dfg_yaml\ningestion_flows: []\n"

    assert client.post("/api/storage/write", json={
        "root_id": root_id, "path": path, "content": body, "overwrite": True,
    }).status_code == 200

    got = client.get(f"/api/storage/read?root_id={root_id}&path={path}").json()
    assert got["content"] == body
    assert got["format"] == "yaml"


@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_unicode_content_survives_round_trip(root_id):
    """Non-ASCII must not be mangled by the base64/utf-8 hops on the workspace path."""
    path = _root_path(root_id) + "unicode.json"
    body = json.dumps({"dataflow_group_id": "dfg_ünïcodé", "note": "München · 東京"}, ensure_ascii=False)

    assert client.post("/api/storage/write", json={
        "root_id": root_id, "path": path, "content": body, "overwrite": True,
    }).status_code == 200
    assert client.get(f"/api/storage/read?root_id={root_id}&path={path}").json()["content"] == body


@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_validate_endpoint_reads_a_real_saved_spec(root_id):
    """/storage/validate must parse a file this app just wrote, not choke on base64."""
    path = _root_path(root_id) + "validate_me.json"
    client.post("/api/storage/write", json={
        "root_id": root_id, "path": path, "content": json.dumps(SPEC), "overwrite": True,
    })

    res = client.post("/api/storage/validate", json={"root_id": root_id, "path": path})
    assert res.status_code == 200, res.text
    rep = res.json()
    assert rep["stage"] == "validate", f"file did not even parse: {rep.get('parse_error')}"
    assert rep["summary"]["dataflow_group_id"] == "dfg_roundtrip"


# --------------------------------------------------------------------------------------
# Unresolved template placeholders
# --------------------------------------------------------------------------------------

def test_config_serves_no_unresolved_placeholders_in_root_paths():
    """The SPA seeds its save-target directory from these strings and posts them back.

    Serving `/Volumes/{{catalog}}/...` here is what made every save and open address a
    directory literally named `{{catalog}}`.
    """
    cfg = client.get("/api/config").json()
    for block in (cfg["spec_storage"], cfg["app"]["spec_storage"]):
        for root in block["roots"]:
            assert "{{" not in root["path"] and "}}" not in root["path"], (
                f"root {root['id']} still serves an unresolved path: {root['path']}"
            )
            assert root["template_path"], "the unsubstituted template must still be exposed"


def test_config_resolves_catalog_from_template_variables():
    cfg = client.get("/api/config").json()
    catalog = cfg["template_variables"]["catalog"]["default"]
    vol = next(r for r in cfg["spec_storage"]["roots"] if r["id"] == VOL_ROOT)
    assert vol["path"] == f"/Volumes/{catalog}/flowx/onboarding_specs/"


@pytest.mark.parametrize("verb", ["read", "write"])
def test_unresolved_placeholder_in_path_is_rejected_not_written(verb):
    """A placeholder that slips through is refused with a message naming the cause."""
    bad = "/Volumes/{{catalog}}/flowx/onboarding_specs/x.json"
    if verb == "read":
        res = client.get(f"/api/storage/read?root_id={VOL_ROOT}&path={bad}")
    else:
        res = client.post("/api/storage/write", json={
            "root_id": VOL_ROOT, "path": bad, "content": "{}", "overwrite": True,
        })
    assert res.status_code == 400, res.text
    err = res.json()["error"]
    assert err["code"] == "VALIDATION_FAILED"
    assert "placeholder" in err["message"].lower()


def test_template_vars_reach_list_and_read():
    """/list and /read accept template variables, as /write always has."""
    tv = json.dumps({"catalog": "othercat"})
    res = client.get(f"/api/storage/list?root_id={VOL_ROOT}&template_vars={tv}")
    assert res.status_code == 200, res.text
    assert res.json()["root_path"] == "/Volumes/othercat/flowx/onboarding_specs/"


# --------------------------------------------------------------------------------------
# Listing shape
# --------------------------------------------------------------------------------------

def test_workspace_listing_reports_directories_as_directories():
    """ObjectType.DIRECTORY is an Enum; `== "DIRECTORY"` is False.

    That comparison classified every workspace directory as a file, and the extension
    filter then dropped it — so the Open dialog could never show a subfolder.
    """
    root = _root_path(WS_ROOT)
    client.post("/api/storage/write", json={
        "root_id": WS_ROOT, "path": root + "nested/deep.json",
        "content": json.dumps(SPEC), "overwrite": True,
    })

    entries = client.get(f"/api/storage/list?root_id={WS_ROOT}").json()["entries"]
    dirs = [e for e in entries if e["kind"] == "dir"]
    assert any(e["name"] == "nested" for e in dirs), f"no directory in listing: {entries}"


def test_listing_excludes_disallowed_extensions():
    api = FakeFilesAPI({
        "/Volumes/flowx/flowx/onboarding_specs/keep.json": b"{}",
        "/Volumes/flowx/flowx/onboarding_specs/skip.png": b"\x89PNG",
    })
    from server.clients.files import FileManager
    from server.deps import get_app_settings

    class _C:
        files = api
    names = [e["name"] for e in FileManager(get_app_settings(), _C()).list_files(VOL_ROOT)]
    assert "keep.json" in names
    assert "skip.png" not in names


# --------------------------------------------------------------------------------------
# ETag concurrency — the parameter was accepted and then ignored
# --------------------------------------------------------------------------------------

def test_stale_etag_is_rejected_rather_than_overwriting():
    path = _root_path(VOL_ROOT) + "concurrent.json"
    client.post("/api/storage/write", json={
        "root_id": VOL_ROOT, "path": path, "content": json.dumps(SPEC), "overwrite": True,
    })

    res = client.post("/api/storage/write", json={
        "root_id": VOL_ROOT, "path": path, "content": '{"dataflow_group_id":"clobber"}',
        "overwrite": True, "etag": "0" * 32,
    })
    assert res.status_code == 409, res.text
    assert res.json()["error"]["code"] == "CONFLICT"

    # And the stored file must be untouched.
    got = client.get(f"/api/storage/read?root_id={VOL_ROOT}&path={path}").json()
    assert json.loads(got["content"])["dataflow_group_id"] == "dfg_roundtrip"


def test_matching_etag_is_accepted():
    path = _root_path(VOL_ROOT) + "concurrent_ok.json"
    first = client.post("/api/storage/write", json={
        "root_id": VOL_ROOT, "path": path, "content": json.dumps(SPEC), "overwrite": True,
    }).json()

    res = client.post("/api/storage/write", json={
        "root_id": VOL_ROOT, "path": path, "content": '{"dataflow_group_id":"dfg_v2"}',
        "overwrite": True, "etag": first["etag"],
    })
    assert res.status_code == 200, res.text


# --------------------------------------------------------------------------------------
# Errors that name the real cause
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_missing_file_reports_not_found(root_id):
    res = client.get(f"/api/storage/read?root_id={root_id}&path={_root_path(root_id)}nope.json")
    assert res.status_code == 404, res.text
    assert res.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_write_without_overwrite_conflicts_on_existing_file(root_id):
    path = _root_path(root_id) + "exists.json"
    client.post("/api/storage/write", json={
        "root_id": root_id, "path": path, "content": "{}", "overwrite": True,
    })
    res = client.post("/api/storage/write", json={
        "root_id": root_id, "path": path, "content": "{}", "overwrite": False,
    })
    assert res.status_code == 409, res.text
    assert res.json()["error"]["code"] == "CONFLICT"


# --------------------------------------------------------------------------------------
# Access preflight
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize("root_id", [VOL_ROOT, WS_ROOT])
def test_write_probe_passes_and_leaves_nothing_behind(root_id):
    """The probe used the SDK directly and only ever cleaned up Volumes.

    So the workspace probe both failed (bytes + no ImportFormat) and, had it succeeded,
    would have littered the specs folder with .flowx_access_probe_* files.
    """
    res = client.get(f"/api/storage/access?root_id={root_id}")
    assert res.status_code == 200, res.text
    write_check = next(c for c in res.json()["checks"] if c["id"] == "can_write")
    assert write_check["status"] == "ok", write_check.get("remediation")

    names = [e["name"] for e in client.get(f"/api/storage/list?root_id={root_id}").json()["entries"]]
    leftovers = [n for n in names if n.startswith(".flowx_access_probe_")]
    assert not leftovers, f"preflight left probe files behind: {leftovers}"


# --------------------------------------------------------------------------------------
# SDK contract: the fake must not drift away from the real client
# --------------------------------------------------------------------------------------

def test_fake_files_api_matches_sdk_signatures():
    from databricks.sdk.service.files import FilesAPI

    for name in ("upload", "download", "delete"):
        real = inspect.signature(getattr(FilesAPI, name))
        fake = inspect.signature(getattr(FakeFilesAPI, name))
        assert list(fake.parameters) == list(real.parameters), (
            f"FakeFilesAPI.{name}{fake} has drifted from FilesAPI.{name}{real}"
        )
        for pname, p in real.parameters.items():
            assert fake.parameters[pname].kind == p.kind, (
                f"FakeFilesAPI.{name} parameter '{pname}' must stay {p.kind}"
            )


def test_fake_workspace_api_matches_sdk_signatures():
    from databricks.sdk.service.workspace import WorkspaceAPI

    for name in ("import_", "export", "list", "mkdirs"):
        real = inspect.signature(getattr(WorkspaceAPI, name))
        fake = inspect.signature(getattr(FakeWorkspaceAPI, name))
        for pname, p in real.parameters.items():
            if pname not in fake.parameters:
                continue
            assert fake.parameters[pname].kind == p.kind, (
                f"FakeWorkspaceAPI.{name} parameter '{pname}' must stay {p.kind}"
            )
    # import_ must keep `content` keyword-only, as the SDK has it.
    assert inspect.signature(FakeWorkspaceAPI.import_).parameters["content"].kind is inspect.Parameter.KEYWORD_ONLY


def test_download_contents_is_a_stream_like_the_sdk():
    """DownloadResponse.contents is BinaryIO. Treating it as bytes broke every Volume read."""
    from databricks.sdk.service.files import DownloadResponse

    field = next(f for f in dataclasses.fields(DownloadResponse) if f.name == "contents")
    assert "BinaryIO" in str(field.type)

    api = FakeFilesAPI({"/Volumes/a/b/c.json": b"{}"})
    contents = api.download("/Volumes/a/b/c.json").contents
    assert hasattr(contents, "read"), "the fake must hand back a stream, not bytes"
    assert contents.read() == b"{}"


def test_export_content_is_base64_text_like_the_sdk():
    """ExportResponse.content is a base64 str. Decoding it as UTF-8 yields gibberish."""
    from databricks.sdk.service.workspace import ExportResponse

    field = next(f for f in dataclasses.fields(ExportResponse) if f.name == "content")
    assert "str" in str(field.type)

    api = FakeWorkspaceAPI({"/Workspace/x.json": b'{"a":1}'})
    content = api.export("/Workspace/x.json").content
    assert isinstance(content, str)
    assert base64.b64decode(content) == b'{"a":1}'


def test_workspace_import_rejects_raw_bytes():
    """The SDK JSON-serializes `content`; bytes cannot survive that, so neither may the fake."""
    from databricks.sdk.service.workspace import ImportFormat

    api = FakeWorkspaceAPI()
    with pytest.raises(TypeError):
        api.import_("/Workspace/x.json", content=b"{}", format=ImportFormat.AUTO, overwrite=True)


def test_workspace_import_requires_auto_format_for_a_spec_file():
    """format defaults to SOURCE, which needs a notebook `language` a .json file has not."""
    api = FakeWorkspaceAPI()
    with pytest.raises(ValueError):
        api.import_("/Workspace/x.json", content=base64.b64encode(b"{}").decode(), overwrite=True)


def test_workspace_list_reports_object_type_as_an_enum():
    from databricks.sdk.service.workspace import ObjectType

    api = FakeWorkspaceAPI({"/Workspace/dir/f.json": b"{}"})
    entries = api.list("/Workspace/dir")
    assert entries and isinstance(entries[0].object_type, ObjectType)
    # The trap this guards: the Enum is not equal to its own name.
    assert ObjectType.DIRECTORY != "DIRECTORY"


def test_fake_client_persists_across_requests():
    """Per-request fakes made offline save-then-open impossible and untestable."""
    from server.clients.dbx import get_workspace_client

    a = get_workspace_client(force_fake=True)
    b = get_workspace_client(force_fake=True)
    assert a is b


def test_real_client_failure_is_not_downgraded_to_the_fake(monkeypatch):
    """A broken workspace must surface, not silently become an in-memory dict.

    The old fallback made a failed save report success and a failed open return the
    built-in sample — indistinguishable from working software.
    """
    import server.clients.dbx as dbx

    monkeypatch.delenv("FLOWX_FAKE_DBX", raising=False)

    def _boom(*a, **k):
        raise RuntimeError("no credentials")

    monkeypatch.setattr(dbx, "WorkspaceClient", _boom)
    with pytest.raises(RuntimeError):
        dbx.get_workspace_client(host="https://example.cloud.databricks.com")
