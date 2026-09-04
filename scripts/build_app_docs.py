#!/usr/bin/env python
"""
Build the documentation wiki and package it inside the Databricks App.

The app's `/docs` route is served from `databricks-app/docs_site/`. Databricks Apps
upload only `source_code_path` (see resources/flowx_app/flowx_onboarding_app.yml), so the
built wiki has to live inside `databricks-app/` rather than at the repo root — the
same reason `databricks-app/web/dist/` is a committed build artifact.

Three things are produced, all derived rather than written by hand:

    docs/archive/index.md          <- landing page for the superseded archive
    databricks-app/docs_site/      <- full MkDocs site (mkdocs build, then synced)
    databricks-app/config/
        docs_index.json            <- attribute path -> wiki page + anchor

`docs_index.json` is what makes the app's per-attribute "docs" links land on the
exact attribute in the wiki. It is generated with the same `anchor()` rule and the
same `attribute_knowledge.json` source that scripts/build_docs_reference.py uses to
emit the reference pages, so the links cannot drift from the headings they target.

Usage
-----
    python scripts/build_app_docs.py
    python scripts/build_app_docs.py --check    # CI: fail if committed output is stale
"""

from __future__ import annotations

import argparse
import filecmp
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO = Path(__file__).resolve().parent.parent
APP = REPO / "databricks-app"
DOCS = REPO / "docs"
SITE = REPO / "site"
APP_DOCS_SITE = APP / "docs_site"
DOCS_INDEX = APP / "config" / "docs_index.json"

BANNER = (
    "<!-- GENERATED FILE — do not edit.\n"
    "     Produced by scripts/build_app_docs.py; edit the source it derives from. -->\n\n"
)

# Canonical page order, mirroring scripts/build_docs_reference.py. An attribute that
# appears on several reference pages is linked to the first one that claims it.
FLOW_ORDER = [
    "root",
    "ingestion",
    "transformation",
    "ingestion+transformation",
    "reconciliation",
    "observability",
    "other",
]

# The archive is kept for provenance and is reachable, but it is not living
# documentation: these two groups keep that obvious in the sidebar.
ARCHIVE_GROUPS: List[Tuple[str, str, str]] = [
    (
        "Superseded guides",
        "Design and feature notes replaced by the Architecture and JSON reference "
        "sections. Kept for provenance — where they disagree with the living docs, "
        "the living docs are correct.",
        "guides",
    ),
    (
        "Test case catalogue",
        "Point-in-time verification runbooks, one per test case, recording what was "
        "exercised and what the expected outcome was.",
        "tests",
    ),
]

TC_CATEGORIES = {
    "ing": "Ingestion",
    "cdc": "CDC / snapshot",
    "trf": "Transformation",
    "dq": "Data quality",
    "sec": "Security & crypto",
    "gov": "Governance",
    "rec": "Reconciliation",
    "snk": "Sinks & egress",
    "obs": "Observability",
    "flt": "Filtering",
    "prm": "Parameters",
}


def anchor(path: str) -> str:
    """Slug rule used by scripts/build_docs_reference.py when emitting headings."""
    return path.replace("@", "").replace(".", "").replace("[]", "").replace("_", "-").lower()


def page_slug(flow: str) -> str:
    return flow.replace("+", "-")


# ──────────────────────────────────────────────────────────────────────────────
# Archive landing page
# ──────────────────────────────────────────────────────────────────────────────

def _title_of(md: Path) -> str:
    """First H1 of a doc, falling back to a de-slugified filename."""
    for line in md.read_text(encoding="utf-8").splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return md.stem.split("_", 1)[-1].replace("_", " ").title()


def _tc_key(name: str) -> str | None:
    """'46_tc_cdc_007' -> 'cdc'; None for non-test-case docs."""
    parts = name.split("_")
    return parts[2] if len(parts) > 3 and parts[1] == "tc" else None


def build_archive_index() -> Path:
    legacy = DOCS / "archive" / "legacy_docs"
    pages = sorted(legacy.glob("*.md"))

    guides = [p for p in pages if _tc_key(p.stem) is None]
    tests = [p for p in pages if _tc_key(p.stem) is not None]

    out = [
        BANNER,
        "# Archive\n",
        "!!! warning \"Superseded material\"\n"
        "    Nothing on these pages is maintained. They are kept so the reasoning\n"
        "    behind past decisions stays available and so old links keep resolving.\n"
        "    For current behaviour use the **Architecture** and **JSON reference**\n"
        "    sections.\n",
    ]

    for title, blurb, kind in ARCHIVE_GROUPS:
        items = guides if kind == "guides" else tests
        out.append(f"\n## {title}\n")
        out.append(blurb + "\n")

        if kind == "guides":
            out.append("\n| Document | |\n|---|---|\n")
            for p in items:
                out.append(f"| [{_title_of(p)}](legacy_docs/{p.name}) | `{p.stem}` |\n")
        else:
            by_cat: Dict[str, List[Path]] = {}
            for p in items:
                by_cat.setdefault(_tc_key(p.stem) or "other", []).append(p)
            for cat in [c for c in TC_CATEGORIES if c in by_cat] + [
                c for c in by_cat if c not in TC_CATEGORIES
            ]:
                out.append(f"\n### {TC_CATEGORIES.get(cat, cat.title())}\n\n")
                links = ", ".join(
                    f"[{p.stem.split('_', 1)[1]}](legacy_docs/{p.name})" for p in by_cat[cat]
                )
                out.append(links + "\n")

    target = DOCS / "archive" / "index.md"
    target.write_text("".join(out), encoding="utf-8")
    return target


# ──────────────────────────────────────────────────────────────────────────────
# Attribute -> wiki deep-link index
# ──────────────────────────────────────────────────────────────────────────────

def build_docs_index() -> Dict[str, Any]:
    kb_path = APP / "config" / "attribute_knowledge.json"
    if not kb_path.exists():
        raise SystemExit(
            "config/attribute_knowledge.json is missing — run "
            "databricks-app/scripts/build_attribute_knowledge.py first."
        )
    kb = json.loads(kb_path.read_text(encoding="utf-8"))

    entries: Dict[str, Dict[str, str]] = {}
    for path, entry in sorted(kb.get("attributes", {}).items()):
        if path.startswith("_") or not isinstance(entry, dict):
            continue
        flows = (entry.get("_meta") or {}).get("flows") or ["other"]
        flow = next((f for f in FLOW_ORDER if f in flows), flows[0])
        entries[path] = {
            "page": f"reference/json/{page_slug(flow)}/",
            "anchor": f"#{anchor(path)}",
        }

    return {
        "_generated_by": "scripts/build_app_docs.py",
        "base_url": "/docs/",
        "attributes": entries,
    }


def verify_docs_index(index: Dict[str, Any]) -> List[str]:
    """Confirm every generated deep link resolves to a real anchor in the built site."""
    problems: List[str] = []
    cache: Dict[str, set[str]] = {}
    for path, target in index["attributes"].items():
        page = target["page"]
        if page not in cache:
            html = SITE / page / "index.html"
            if not html.exists():
                cache[page] = set()
            else:
                import re

                cache[page] = set(
                    re.findall(r'id="([^"]+)"', html.read_text(encoding="utf-8"))
                )
        if not cache[page]:
            problems.append(f"{path}: page {page} not in built site")
        elif target["anchor"].lstrip("#") not in cache[page]:
            problems.append(f"{path}: {page}{target['anchor']} has no such anchor")
    return problems


# ──────────────────────────────────────────────────────────────────────────────
# MkDocs build + sync into the app
# ──────────────────────────────────────────────────────────────────────────────

def run_mkdocs() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--clean"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit("mkdocs build failed")
    for line in (proc.stdout + proc.stderr).splitlines():
        if "WARNING" in line or "ERROR" in line:
            print("  " + line.strip())


def sync_site() -> Tuple[int, int]:
    """Mirror site/ into databricks-app/docs_site/. Returns (copied, removed)."""
    if not (SITE / "index.html").exists():
        raise SystemExit("site/ has no index.html — did mkdocs build run?")

    APP_DOCS_SITE.mkdir(parents=True, exist_ok=True)

    wanted = {p.relative_to(SITE) for p in SITE.rglob("*") if p.is_file()}
    existing = {p.relative_to(APP_DOCS_SITE) for p in APP_DOCS_SITE.rglob("*") if p.is_file()}

    copied = 0
    for rel in sorted(wanted):
        src, dst = SITE / rel, APP_DOCS_SITE / rel
        if dst.exists() and filecmp.cmp(src, dst, shallow=False):
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied += 1

    removed = 0
    for rel in sorted(existing - wanted):
        (APP_DOCS_SITE / rel).unlink()
        removed += 1

    for d in sorted((p for p in APP_DOCS_SITE.rglob("*") if p.is_dir()), reverse=True):
        if not any(d.iterdir()):
            d.rmdir()

    return copied, removed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check",
        action="store_true",
        help="fail if the committed output is stale instead of rewriting it",
    )
    args = ap.parse_args()

    print("- generating docs/archive/index.md")
    build_archive_index()

    print("- mkdocs build")
    run_mkdocs()

    print("- building config/docs_index.json")
    index = build_docs_index()
    index_text = json.dumps(index, indent=2, ensure_ascii=False) + "\n"

    problems = verify_docs_index(index)
    if problems:
        print(f"  ! {len(problems)} deep link(s) do not resolve:")
        for p in problems[:15]:
            print(f"    - {p}")
        if len(problems) > 15:
            print(f"    … and {len(problems) - 15} more")

    if args.check:
        stale: List[str] = []
        if not DOCS_INDEX.exists() or DOCS_INDEX.read_text(encoding="utf-8") != index_text:
            stale.append("databricks-app/config/docs_index.json")
        cmp_site = {p.relative_to(SITE) for p in SITE.rglob("*") if p.is_file()}
        cmp_app = {p.relative_to(APP_DOCS_SITE) for p in APP_DOCS_SITE.rglob("*") if p.is_file()}
        if cmp_site != cmp_app:
            stale.append("databricks-app/docs_site/ (file set differs)")
        if stale or problems:
            for s in stale:
                print(f"  ! stale: {s}")
            print("\nRun: python scripts/build_app_docs.py")
            return 1
        print("\nUp to date.")
        return 0

    DOCS_INDEX.write_text(index_text, encoding="utf-8")

    print("- syncing site/ -> databricks-app/docs_site/")
    copied, removed = sync_site()
    pages = len(list(APP_DOCS_SITE.rglob("index.html")))
    print(
        f"\n{pages} wiki pages packaged "
        f"({copied} file(s) written, {removed} stale file(s) removed); "
        f"{len(index['attributes'])} attribute deep links indexed."
    )
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
