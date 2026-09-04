import os
import re

from fastapi.testclient import TestClient

from server.app import app

os.environ["FLOWX_FAKE_DBX"] = "1"
client = TestClient(app)


def test_serve_spa_root():
    """The Vite shell is served at / and boots the React app."""
    res = client.get("/")
    assert res.status_code == 200
    assert "FlowX" in res.text or "FlowX" in res.text
    # Vite shell: a root mount point plus at least one hashed bundle reference.
    assert 'id="root"' in res.text
    assert re.search(r'(?:src|href)="/assets/[^"]+"', res.text)


def test_serve_static_assets(frontend_text):
    """Every /assets/* file index.html references resolves and is non-empty.

    Vite emits hashed filenames into dist/assets/, so these cannot be asserted by
    fixed name the way the old dist-root styles.css / app.js pair was.
    """
    res = client.get("/")
    refs = re.findall(r'(?:src|href)="(/assets/[^"]+)"', res.text)
    assert refs, "no hashed assets referenced — frontend not built"

    js = [r for r in refs if r.endswith(".js")]
    css = [r for r in refs if r.endswith(".css")]
    assert js, f"no JS bundle referenced, got {refs}"
    assert css, f"no CSS bundle referenced, got {refs}"

    for ref in refs:
        asset = client.get(ref)
        assert asset.status_code == 200, f"{ref} -> {asset.status_code}"
        assert len(asset.content) > 100, f"{ref} is suspiciously small"

    # The theme tokens still ship, now via the bundled stylesheet.
    assert "--bg" in frontend_text or "--panel" in frontend_text


def test_spa_deep_link_falls_back_to_shell():
    """Client-side routes fall through to index.html rather than 404."""
    res = client.get("/some/client/route")
    assert res.status_code == 200
    assert 'id="root"' in res.text


def test_serve_docs_site():
    """The wiki is served by MkDocs Material and carries its nav and search."""
    res_docs = client.get("/docs/")
    assert res_docs.status_code == 200
    assert "FlowX" in res_docs.text
    # Material renders one tab per top-level nav section; their presence is what
    # makes /docs browsable rather than a single page.
    for tab in ["Get started", "Architecture", "JSON reference", "Help"]:
        assert tab in res_docs.text, f"nav tab missing: {tab}"
