#!/usr/bin/env python
"""
Generate the reference sections of the documentation wiki from source.

Two reference trees are produced, both derived rather than written by hand, so they
cannot drift from the thing they describe:

    docs/reference/json/     <- databricks-app/web/src/registry.js  (+ curated prose)
    docs/reference/code/     <- src/NextGen_Metadata_Framework/     (AST, no imports)

The JSON reference is the same data the Spec Builder renders as a form and the same
prose its attribute inspector shows, so the app and the docs always agree.

The code reference is built by parsing the source with `ast`. Nothing is imported, so
this runs anywhere — no Spark, no Databricks connection, no installed wheel.

Usage
-----
    python scripts/build_docs_reference.py
    python scripts/build_docs_reference.py --check    # CI: fail if committed output is stale
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parent.parent
APP = REPO / "databricks-app"
SRC = REPO / "src" / "NextGen_Metadata_Framework"
DOCS = REPO / "docs"
JSON_REF = DOCS / "reference" / "json"
CODE_REF = DOCS / "reference" / "code"

BANNER = (
    "<!-- GENERATED FILE — do not edit.\n"
    "     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->\n\n"
)

FLOW_TITLES = {
    "root": "Spec root",
    "ingestion": "Ingestion flows",
    "transformation": "Transformation flows",
    "reconciliation": "Reconciliation flows",
    "observability": "Observability",
    "ingestion+transformation": "CDC / load strategy",
}

FLOW_INTROS = {
    "root": "Top-level attributes of the onboarding document. Everything else hangs off these.",
    "ingestion": "One entry per `ingestion_flows[]` element — reading from a landing zone into Bronze.",
    "transformation": "One entry per `transformation_flows[]` element — SQL plus a CDC load strategy.",
    "reconciliation": "One entry per `reconciliation_flows[]` element — comparing a baseline against targets.",
    "observability": "One entry per `observability[]` element — where telemetry is exported.",
    "ingestion+transformation": (
        "Attributes under `target_config` that only apply to particular CDC load strategies. "
        "The Spec Builder shows these on the **Load strategy** step and hides the ones the "
        "selected strategy does not use."
    ),
}


# ──────────────────────────────────────────────────────────────────────────────
# JSON attribute reference
# ──────────────────────────────────────────────────────────────────────────────

def load_knowledge() -> Dict[str, Any]:
    kb = APP / "config" / "attribute_knowledge.json"
    if not kb.exists():
        raise SystemExit(
            "config/attribute_knowledge.json is missing — run "
            "databricks-app/scripts/build_attribute_knowledge.py first."
        )
    return json.loads(kb.read_text(encoding="utf-8"))


def anchor(path: str) -> str:
    return path.replace("@", "").replace(".", "").replace("[]", "").replace("_", "-").lower()


def md_escape(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ").strip()


def render_flow_page(flow: str, attrs: List[tuple[str, Dict[str, Any]]], refs: Dict[str, str]) -> str:
    title = FLOW_TITLES.get(flow, flow)
    out = [BANNER, f"# {title}\n", FLOW_INTROS.get(flow, "") + "\n"]
    out.append(
        f"\n!!! info \"{len(attrs)} attributes\"\n"
        f"    Every attribute below is also available in the Spec Builder's attribute\n"
        f"    inspector — click the **i** beside any field to see this same content\n"
        f"    without leaving the form.\n"
    )

    # Summary table first, so the page is scannable before it is readable.
    out.append("\n## Summary\n")
    out.append("| Attribute | Type | Required | Default |")
    out.append("|---|---|---|---|")
    for path, entry in attrs:
        meta = entry.get("_meta", {})
        dflt = meta.get("default", None)
        out.append(
            f"| [`{path}`](#{anchor(path)}) "
            f"| {md_escape(meta.get('type', 'string'))} "
            f"| {'**yes**' if meta.get('required') else 'no'} "
            f"| {'`' + str(dflt) + '`' if dflt not in (None, '') else '—'} |"
        )

    out.append("\n## Attributes\n")
    for path, entry in attrs:
        meta = entry.get("_meta", {})
        out.append(f"### `{path}` {{ #{anchor(path)} }}\n")
        if entry.get("purpose"):
            out.append(entry["purpose"] + "\n")
        if entry.get("why"):
            out.append(f"\n{entry['why']}\n")

        bits = [f"**Type** `{meta.get('type', 'string')}`"]
        bits.append("**Required** yes" if meta.get("required") else "**Required** no")
        if meta.get("section"):
            bits.append(f"**Section** {meta['section']}")
        out.append("\n" + " · ".join(bits) + "\n")

        for sample in entry.get("samples", [])[:1]:
            out.append(f"\n```{sample.get('language', 'json')}\n{sample.get('code', '')}\n```\n")

        tips = entry.get("tips", [])
        if tips:
            out.append('\n!!! tip "Best practice"\n')
            for t in tips:
                out.append(f"    - {t}")
            out.append("")

        errors = entry.get("errors", [])
        if errors:
            out.append('\n!!! warning "Known errors and limitations"\n')
            for e in errors:
                out.append(f"    **{e.get('symptom', '')}**  ")
                if e.get("cause"):
                    out.append(f"    *Cause:* {e['cause']}  ")
                if e.get("fix"):
                    out.append(f"    *Fix:* {e['fix']}")
                out.append("")

        links = [f"[{k.replace('_', ' ')}]({refs[k]})" for k in entry.get("refs", []) if k in refs]
        if links:
            out.append("\n**Databricks documentation:** " + " · ".join(links) + "\n")

        out.append("\n---\n")

    return "\n".join(out)


def build_json_reference() -> List[Path]:
    kb = load_knowledge()
    refs = kb.get("reference_index", {})
    attrs = kb.get("attributes", {})

    by_flow: Dict[str, List[tuple[str, Dict[str, Any]]]] = {}
    for path, entry in sorted(attrs.items()):
        for flow in entry.get("_meta", {}).get("flows", ["other"]):
            by_flow.setdefault(flow, []).append((path, entry))

    JSON_REF.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []

    order = ["root", "ingestion", "transformation", "ingestion+transformation",
             "reconciliation", "observability"]
    flows = [f for f in order if f in by_flow] + [f for f in by_flow if f not in order]

    index = [BANNER, "# JSON attribute reference\n",
             "Every attribute the framework understands, grouped by the part of the spec it "
             "belongs to. Generated from the same registry the Spec Builder renders, so this "
             "reference and the app can never disagree.\n",
             "\n## Sections\n",
             "| Section | Attributes | What it covers |", "|---|---|---|"]
    for flow in flows:
        slug = flow.replace("+", "-")
        index.append(
            f"| [{FLOW_TITLES.get(flow, flow)}]({slug}.md) | {len(by_flow[flow])} "
            f"| {md_escape(FLOW_INTROS.get(flow, ''))} |"
        )

    total = len({p for p, _ in [(p, e) for f in by_flow for p, e in by_flow[f]]})
    index.append(f"\n\n**{total} distinct attributes** across {len(flows)} sections.\n")
    index.append("\n## CDC load strategies\n")
    cdc = kb.get("cdc", [])
    if cdc:
        index.append("| Strategy | Applies to | What it does | Requires |")
        index.append("|---|---|---|---|")
        for c in cdc:
            index.append(
                f"| `{c.get('strategy','')}` | {md_escape(c.get('badge',''))} "
                f"| {md_escape(c.get('desc',''))} | {md_escape(c.get('requires',''))} |"
            )

    (JSON_REF / "index.md").write_text("\n".join(index) + "\n", encoding="utf-8")
    written.append(JSON_REF / "index.md")

    for flow in flows:
        slug = flow.replace("+", "-")
        page = JSON_REF / f"{slug}.md"
        page.write_text(render_flow_page(flow, by_flow[flow], refs), encoding="utf-8")
        written.append(page)

    return written


# ──────────────────────────────────────────────────────────────────────────────
# Code / function reference
# ──────────────────────────────────────────────────────────────────────────────

def first_sentence(doc: str | None) -> str:
    if not doc:
        return ""
    text = " ".join(doc.strip().split())
    for stop in (". ", "! ", "? "):
        if stop in text:
            return text[: text.index(stop) + 1]
    return text if len(text) < 200 else text[:197] + "…"


def signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    a = node.args
    parts: List[str] = []
    pos = list(a.posonlyargs) + list(a.args)
    defaults = [None] * (len(pos) - len(a.defaults)) + list(a.defaults)
    for arg, dflt in zip(pos, defaults):
        if arg.arg in ("self", "cls"):
            continue
        s = arg.arg
        if arg.annotation is not None:
            s += f": {ast.unparse(arg.annotation)}"
        if dflt is not None:
            s += f" = {ast.unparse(dflt)}"
        parts.append(s)
    if a.vararg:
        parts.append("*" + a.vararg.arg)
    for arg, dflt in zip(a.kwonlyargs, a.kw_defaults):
        s = arg.arg
        if arg.annotation is not None:
            s += f": {ast.unparse(arg.annotation)}"
        if dflt is not None:
            s += f" = {ast.unparse(dflt)}"
        parts.append(s)
    if a.kwarg:
        parts.append("**" + a.kwarg.arg)
    ret = f" -> {ast.unparse(node.returns)}" if node.returns is not None else ""
    return f"{node.name}({', '.join(parts)}){ret}"


def scan_module(path: Path) -> Dict[str, Any]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    mod: Dict[str, Any] = {
        "doc": first_sentence(ast.get_docstring(tree)),
        "full_doc": (ast.get_docstring(tree) or "").strip(),
        "functions": [], "classes": [],
    }
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("_"):
                continue
            mod["functions"].append({"sig": signature(node), "doc": first_sentence(ast.get_docstring(node))})
        elif isinstance(node, ast.ClassDef):
            if node.name.startswith("_"):
                continue
            methods = [
                {"sig": signature(m), "doc": first_sentence(ast.get_docstring(m))}
                for m in node.body
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and not m.name.startswith("_")
            ]
            mod["classes"].append({
                "name": node.name,
                "doc": first_sentence(ast.get_docstring(node)),
                "methods": methods,
            })
    return mod


PACKAGE_BLURBS = {
    "asn1": "BER/DER decoding of binary CDR payloads.",
    "cdc": "Change-data-capture dispatch: SCD strategies, snapshots, hashing, change metrics.",
    "control_plane": "The eight control tables — DDL, provisioning, repository access, post-deployment steps.",
    "crypto": "Column-level AES, PGP, and Unity Catalog secret resolution.",
    "dq": "Data-quality expectations and quarantine routing.",
    "engine": "Graph construction: flow and sink registration, run context, Spark configuration.",
    "governance": "Unity Catalog tagging applied after deployment.",
    "ingestion": "Source readers and the Bronze-layer transforms applied on read.",
    "observability": "Event-log extraction and OpenTelemetry export.",
    "onboarding": "Turning a spec document into control-table rows.",
    "reconciliation": "Baseline-versus-target comparison and self-healing.",
    "transformation": "Silver/Gold SQL execution and target materialisation.",
    "archive": "Superseded implementations retained for reference.",
}


def build_code_reference() -> List[Path]:
    if not SRC.exists():
        raise SystemExit(f"source tree not found: {SRC}")

    packages: Dict[str, List[tuple[Path, Dict[str, Any]]]] = {}
    for py in sorted(SRC.rglob("*.py")):
        if "__pycache__" in py.parts or py.name == "__init__.py":
            continue
        rel = py.relative_to(SRC)
        pkg = rel.parts[1] if len(rel.parts) > 2 else (rel.parts[0] if len(rel.parts) > 1 else "core")
        if pkg.endswith(".py"):
            pkg = "core"
        packages.setdefault(pkg, []).append((rel, scan_module(py)))

    CODE_REF.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []

    n_mod = sum(len(v) for v in packages.values())
    n_fn = sum(len(m["functions"]) for v in packages.values() for _, m in v)
    n_cls = sum(len(m["classes"]) for v in packages.values() for _, m in v)

    index = [BANNER, "# Code reference\n",
             f"The framework wheel, package by package — **{n_mod} modules**, "
             f"**{n_cls} classes**, **{n_fn} public functions**.\n",
             "Generated by parsing the source with `ast`: nothing is imported, so this "
             "reference builds without Spark or a Databricks connection.\n",
             "\n## Packages\n",
             "| Package | Modules | What it does |", "|---|---|---|"]
    for pkg in sorted(packages):
        index.append(f"| [`{pkg}`]({pkg}.md) | {len(packages[pkg])} | {PACKAGE_BLURBS.get(pkg, '')} |")
    (CODE_REF / "index.md").write_text("\n".join(index) + "\n", encoding="utf-8")
    written.append(CODE_REF / "index.md")

    for pkg, modules in sorted(packages.items()):
        out = [BANNER, f"# `{pkg}`\n"]
        if PACKAGE_BLURBS.get(pkg):
            out.append(PACKAGE_BLURBS[pkg] + "\n")
        out.append(f"\n{len(modules)} modules.\n")
        for rel, mod in modules:
            out.append(f"\n## `{rel.as_posix()}`\n")
            if mod["doc"]:
                out.append(mod["doc"] + "\n")
            for cls in mod["classes"]:
                out.append(f"\n### class `{cls['name']}`\n")
                if cls["doc"]:
                    out.append(cls["doc"] + "\n")
                if cls["methods"]:
                    out.append("\n| Method | Purpose |\n|---|---|")
                    for m in cls["methods"]:
                        out.append(f"| `{md_escape(m['sig'])}` | {md_escape(m['doc'])} |")
                    out.append("")
            if mod["functions"]:
                out.append("\n### Functions\n")
                out.append("| Signature | Purpose |\n|---|---|")
                for fn in mod["functions"]:
                    out.append(f"| `{md_escape(fn['sig'])}` | {md_escape(fn['doc'])} |")
                out.append("")
        page = CODE_REF / f"{pkg}.md"
        page.write_text("\n".join(out) + "\n", encoding="utf-8")
        written.append(page)

    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    if args.check:
        before = {p: p.read_text(encoding="utf-8") for p in
                  list(JSON_REF.glob("*.md")) + list(CODE_REF.glob("*.md")) if p.exists()}

    written = build_json_reference() + build_code_reference()

    if args.check:
        stale = [p for p in written if before.get(p) != p.read_text(encoding="utf-8")]
        if stale:
            print("generated docs are stale:", ", ".join(str(p.relative_to(REPO)) for p in stale), file=sys.stderr)
            return 1
        print("generated docs are up to date.")
        return 0

    print(f"wrote {len(written)} pages")
    for p in written:
        print(f"  {p.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
