import re
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
    """An attribute resolves to its own heading on a wiki page that really has it."""
    res = client.get("/api/docs/resolve?path=source_config.remove_dups")
    assert res.status_code == 200
    url = res.json()["url"]
    assert url.startswith("/docs/")

    page, _, anchor = url.partition("#")
    page_res = client.get(page if page.endswith("/") else page + "/")
    assert page_res.status_code == 200, f"{page} -> {page_res.status_code}"
    assert anchor, f"no anchor in resolved url {url}"
    assert f'id="{anchor}"' in page_res.text, f"{page} has no anchor #{anchor}"


def test_every_indexed_attribute_deep_link_resolves():
    """No attribute may link into the wiki at an anchor the wiki does not have.

    docs_index.json is generated (scripts/build_app_docs.py); this is the guard
    that it was regenerated after the wiki headings changed.
    """
    import json
    from pathlib import Path

    index_file = Path(__file__).parent.parent / "config" / "docs_index.json"
    attrs = json.loads(index_file.read_text(encoding="utf-8"))["attributes"]
    assert attrs, "docs_index.json has no attributes"

    anchors_by_page: dict[str, set[str]] = {}
    dead = []
    for path, entry in attrs.items():
        page = entry["page"]
        if page not in anchors_by_page:
            res = client.get("/docs/" + page)
            anchors_by_page[page] = (
                set(re.findall(r'id="([^"]+)"', res.text)) if res.status_code == 200 else set()
            )
        if entry["anchor"].lstrip("#") not in anchors_by_page[page]:
            dead.append(f"{path} -> {page}{entry['anchor']}")

    assert not dead, f"{len(dead)} dead wiki deep link(s): " + "; ".join(dead[:10])
