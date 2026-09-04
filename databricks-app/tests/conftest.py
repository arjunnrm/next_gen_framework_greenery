"""
Shared test fixtures.

The frontend moved from a self-contained v3 SPA — one large index.html with all
markup, styles and logic inlined — to the React/Vite build delivered by Claude
Design. Under Vite, index.html is a ~600-byte shell (`<div id="root">`) and every
string the UI actually renders lives in the hashed bundle under /assets/.

Tests that assert "the shipped frontend contains X" must therefore read the shell
*and* its referenced assets, not the shell alone. `frontend_text` does that, so
those assertions keep testing what they were written to test.
"""

import os
import re

import pytest

os.environ.setdefault("FLOWX_FAKE_DBX", "1")

from fastapi.testclient import TestClient  # noqa: E402

from server.app import app  # noqa: E402

_ASSET_REF = re.compile(r'(?:src|href)="(/assets/[^"]+)"')


@pytest.fixture(scope="session")
def client():
    return TestClient(app)


@pytest.fixture(scope="session")
def frontend_text(client):
    """The full shipped frontend: index.html plus every /assets/* file it references.

    Returned as one concatenated string so a test can assert that a label, an
    attribute path or a sample identifier is present anywhere in what the browser
    actually receives.
    """
    res = client.get("/")
    assert res.status_code == 200, f"SPA root did not serve: {res.status_code}"
    parts = [res.text]

    refs = _ASSET_REF.findall(res.text)
    assert refs, (
        "index.html referenced no /assets/* files. The frontend build is missing "
        "or was not produced by `npm run build` in databricks-app/web."
    )
    for ref in refs:
        asset = client.get(ref)
        assert asset.status_code == 200, f"asset {ref} did not serve: {asset.status_code}"
        parts.append(asset.text)

    return "\n".join(parts)
