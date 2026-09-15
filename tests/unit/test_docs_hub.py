"""The documentation hub's navigation, generated pages and cross-links stay coherent (v1.7.13).

Offline and import-light: reads ``mkdocs.yml`` with a tag-tolerant YAML loader, checks that
every nav entry is a real file, that the generated reference pages carry their banner, and
that the anchors ``scripts/build_docs_reference.py`` emits into the console pages resolve to
headings that exist. Run ``python scripts/build_docs_reference.py`` first if the generated
pages or the staged ``docs/UC*`` trees are missing.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest
import yaml

REPO = Path(".")
DOCS = REPO / "docs"
MKDOCS = REPO / "mkdocs.yml"
GENERATOR = REPO / "scripts" / "build_docs_reference.py"
BANNER_MARK = "GENERATED FILE"


class _TolerantLoader(yaml.SafeLoader):
    """mkdocs.yml carries !!python/name tags for pymdownx; ignore them, keep the structure."""


def _ignore(loader, tag_suffix, node):  # noqa: ANN001
    return None


_TolerantLoader.add_multi_constructor("tag:yaml.org,2002:python/", _ignore)


def _config() -> dict:
    return yaml.load(MKDOCS.read_text(encoding="utf-8"), Loader=_TolerantLoader)


def _nav_paths(node) -> list:
    out: list = []
    if isinstance(node, dict):
        for v in node.values():
            out += _nav_paths(v)
    elif isinstance(node, list):
        for item in node:
            out += _nav_paths(item)
    elif isinstance(node, str):
        out.append(node)
    return out


def _slugify(text: str) -> str:
    from markdown.extensions.toc import slugify

    return slugify(text, "-")


def _heading_slugs(md: Path) -> set:
    slugs = set()
    for line in md.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^(#{1,6})\s+(.*?)\s*$", line)
        if not m:
            continue
        text = m.group(2)
        explicit = re.search(r"\{\s*#([^}\s]+)\s*\}", text)
        if explicit:
            slugs.add(explicit.group(1))
            text = re.sub(r"\{\s*#[^}]*\}\s*$", "", text).strip()
        slugs.add(_slugify(text))
    return slugs


def _generator_module():
    spec = importlib.util.spec_from_file_location("build_docs_reference", GENERATOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


# ── navigation ────────────────────────────────────────────────────────────────

def test_every_nav_entry_is_a_real_page():
    missing = [p for p in _nav_paths(_config()["nav"]) if p != "/" and not (DOCS / p).is_file()]
    assert not missing, f"mkdocs.yml nav points at pages that do not exist: {missing}"


def test_nav_has_the_five_audience_journeys():
    top = [next(iter(item)) if isinstance(item, dict) else item for item in _config()["nav"]]
    for tab in ("Home", "Get started", "Pillars", "Console", "Configuration", "Architecture", "Code reference", "Help"):
        assert tab in top, f"top-level tab {tab!r} missing from nav: {top}"


def test_hooks_and_stylesheet_are_wired():
    cfg = _config()
    for hook in cfg.get("hooks", []):
        assert (REPO / hook).is_file(), f"hook {hook} missing"
    assert cfg.get("hooks"), "mkdocs.yml must register scripts/mkdocs_hooks.py"
    for css in cfg.get("extra_css", []):
        assert (DOCS / css).is_file(), f"stylesheet {css} missing"
    fences = next((ext["pymdownx.superfences"] for ext in cfg["markdown_extensions"]
                   if isinstance(ext, dict) and "pymdownx.superfences" in ext), {})
    assert any(f.get("name") == "mermaid" for f in fences.get("custom_fences", [])), "mermaid fence not configured"


# ── generated pages ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("page", ["index.md", "tree.md", "removed.md", "root.md", "ingestion.md",
                                  "transformation.md", "reconciliation.md", "observability.md",
                                  "ingestion-transformation.md"])
def test_generated_reference_page_exists_and_is_marked_generated(page):
    path = DOCS / "reference" / "json" / page
    assert path.is_file(), f"{path} missing — run python scripts/build_docs_reference.py"
    assert BANNER_MARK in path.read_text(encoding="utf-8")[:400], f"{path} lacks the generated-file banner"


def test_tree_view_links_every_documented_attribute():
    """Each registry attribute must be reachable from the schema tree (or be a registry-only helper)."""
    import json

    kb = json.loads((REPO / "databricks-app/config/attribute_knowledge.json").read_text(encoding="utf-8"))["attributes"]
    tree = (DOCS / "reference/json/tree.md").read_text(encoding="utf-8")
    linked = set(re.findall(r'#([a-z0-9\-]+)" title="Open the attribute reference"', tree))
    mod = _generator_module()
    unlinked = sorted(p for p in kb if mod.anchor(p) not in linked)
    # The one registry helper with no parent container in the schema: the Builder's master switch.
    allowed = {"@observability_enabled"}
    unexpected = [p for p in unlinked if p not in allowed]
    assert not unexpected, f"attributes missing from the schema tree: {unexpected}"


def test_removed_page_lists_every_removed_key_from_the_validator():
    mod = _generator_module()
    removed = mod.load_removed()
    page = (DOCS / "reference/json/removed.md").read_text(encoding="utf-8")
    for group in ("REMOVED_SOURCE_CONFIG_KEYS", "REMOVED_TARGET_CONFIG_KEYS",
                  "REMOVED_RECONCILIATION_FLOW_KEYS", "REMOVED_CDC_LOAD_STRATEGIES", "UNKNOWN_KEY_ALIASES"):
        assert removed.get(group), f"generator could not read {group} from spec_validator.py"
        for key in removed[group]:
            # Keys are rendered with their container prefix, e.g. `source_config.normalize_column_names`
            # or `cdc_load_strategy = FULL_SNAPSHOT_CDC_NO_PK`, so match the key followed by its closing tick.
            assert f"{key}`" in page, f"{group}[{key}] missing from removed.md"


# ── cross-links emitted by the generator ──────────────────────────────────────

def test_generator_console_and_pillar_links_resolve():
    mod = _generator_module()
    problems = []
    for flow, links in mod.FLOW_LINKS.items():
        targets = [links["pillar"][1]] + [href for _, href in links["console"]]
        for href in targets:
            page, _, frag = href.partition("#")
            target = (DOCS / "reference" / "json" / page).resolve()
            if not target.is_file():
                problems.append(f"{flow}: {href} -> missing page")
                continue
            if frag and frag not in _heading_slugs(target):
                problems.append(f"{flow}: {href} -> no heading #{frag} in {target.name}")
    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("page", ["pillars/index.md", "pillars/ingestion.md", "pillars/transformation.md",
                                  "pillars/reconciliation.md", "pillars/observability.md",
                                  "console/index.md", "console/spec_builder.md", "console/observability_dashboard.md",
                                  "console/control_dashboard.md", "console/genie.md", "console/agent_skills.md",
                                  "reference/sync.md"])
def test_hub_page_exists_and_uses_material_features(page):
    path = DOCS / page
    assert path.is_file(), f"{page} missing"
    text = path.read_text(encoding="utf-8")
    assert text.lstrip().startswith("#"), f"{page} must start with an H1"
    if page.startswith("pillars/") and page != "pillars/index.md":
        assert "```mermaid" in text, f"{page} must carry a Mermaid diagram"
        assert '=== "' in text, f"{page} must use content tabs"
        assert "fx-badge" in text, f"{page} must use status badges"
        assert "reference/json/" in text, f"{page} must link into the attribute reference"


def test_index_is_a_topic_directory():
    text = (DOCS / "index.md").read_text(encoding="utf-8")
    assert 'class="fx-directory"' in text
    assert text.count('class="fx-topic"') >= 6
    assert text.count('class="fx-panel"') >= 3
    assert "```mermaid" in text
