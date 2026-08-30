#!/usr/bin/env python
"""
Build config/attribute_knowledge.json — the content behind the app's attribute inspector.

Why this is generated rather than hand-written
----------------------------------------------
The builder exposes ~160 distinct attributes. Hand-writing an inspector entry for each
would guarantee drift: the registry is the thing that actually renders the form, so
anything written beside it goes stale the moment a field changes. Instead this script
reads the registry itself and derives an entry for every attribute, then layers the
hand-authored entries on top.

    registry.js  ──node──▶  registry dump  ──▶  derived entries
                                                      │
    config/attribute_knowledge.curated.json ──────────┤ (wins on every field it sets)
                                                      ▼
                                    config/attribute_knowledge.json

So: every attribute always has *something* useful in the panel, and the ones that
deserve real prose get it without the other 140 sitting empty.

Usage
-----
    python databricks-app/scripts/build_attribute_knowledge.py
    python databricks-app/scripts/build_attribute_knowledge.py --check   # CI: fail if stale
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List

APP_ROOT = Path(__file__).resolve().parent.parent
WEB_ROOT = APP_ROOT / "web"
REGISTRY_JS = WEB_ROOT / "src" / "registry.js"
BUILDER_JSX = WEB_ROOT / "src" / "Builder.jsx"
CURATED = APP_ROOT / "config" / "attribute_knowledge.curated.json"
OUTPUT = APP_ROOT / "config" / "attribute_knowledge.json"

# ── Databricks documentation, keyed so entries reference a name not a raw URL ──
REFERENCE_INDEX: Dict[str, str] = {
    "auto_loader": "https://docs.databricks.com/ingestion/auto-loader/index.html",
    "auto_loader_options": "https://docs.databricks.com/ingestion/auto-loader/options.html",
    "auto_loader_schema": "https://docs.databricks.com/ingestion/auto-loader/schema.html",
    "apply_changes": "https://docs.databricks.com/delta-live-tables/cdc.html",
    "dlt_expectations": "https://docs.databricks.com/delta-live-tables/expectations.html",
    "dlt_python_ref": "https://docs.databricks.com/delta-live-tables/python-ref.html",
    "liquid_clustering": "https://docs.databricks.com/delta/clustering.html",
    "partitioning": "https://docs.databricks.com/tables/partitions.html",
    "uc_volumes": "https://docs.databricks.com/connect/unity-catalog/volumes.html",
    "uc_tags": "https://docs.databricks.com/data-governance/unity-catalog/tags.html",
    "uc_privileges": "https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html",
    "secrets": "https://docs.databricks.com/security/secrets/index.html",
    "table_properties": "https://docs.databricks.com/delta/table-properties.html",
    "uniform": "https://docs.databricks.com/delta/uniform.html",
    "streaming_tables": "https://docs.databricks.com/tables/streaming.html",
    "materialized_views": "https://docs.databricks.com/views/materialized.html",
    "sinks": "https://docs.databricks.com/delta-live-tables/sinks.html",
    "files_api": "https://docs.databricks.com/api/workspace/files",
    "workspace_api": "https://docs.databricks.com/api/workspace/workspace",
    "structured_streaming": "https://docs.databricks.com/structured-streaming/index.html",
    "spark_sql_functions": "https://docs.databricks.com/sql/language-manual/sql-ref-functions.html",
    "delta_change_data_feed": "https://docs.databricks.com/delta/delta-change-data-feed.html",
    "pipeline_settings": "https://docs.databricks.com/delta-live-tables/settings.html",
}

# First match wins, so order matters: the specific patterns come before the generic ones.
REF_RULES: List[tuple[str, List[str]]] = [
    ("secret", ["secrets", "uc_privileges"]),
    ("schema_location", ["auto_loader_schema"]),
    ("schema_evolution", ["auto_loader_schema"]),
    ("reader_options", ["auto_loader_options"]),
    ("liquid_clustering", ["liquid_clustering"]),
    ("partition", ["partitioning", "liquid_clustering"]),
    ("table_properties", ["table_properties", "uniform"]),
    ("cdc_load_strategy", ["apply_changes"]),
    ("cdc_operation", ["apply_changes", "delta_change_data_feed"]),
    ("primary_keys", ["apply_changes"]),
    ("sequence_by", ["apply_changes"]),
    ("columns_to_check", ["apply_changes"]),
    ("dq_config", ["dlt_expectations"]),
    ("governance_tags", ["uc_tags"]),
    ("sink_config", ["sinks"]),
    ("storage_format", ["uniform", "table_properties"]),
    ("target_type", ["streaming_tables", "materialized_views"]),
    ("transformation_sql", ["spark_sql_functions"]),
    ("data_standardization_sql", ["spark_sql_functions"]),
    ("filter_condition", ["spark_sql_functions"]),
    ("source_zip_handling", ["uc_volumes", "files_api"]),
    ("destination_config", ["uc_volumes"]),
    ("spark_config", ["pipeline_settings"]),
    ("pipeline_parameters", ["pipeline_settings"]),
    ("max_bytes_per_trigger", ["structured_streaming"]),
    ("starting_version", ["structured_streaming", "delta_change_data_feed"]),
    (".path", ["uc_volumes"]),
    ("target_catalog", ["uc_privileges"]),
    ("source_type", ["auto_loader"]),
    ("format", ["auto_loader_options"]),
]

WIDGET_TYPE = {
    "text": "string",
    "num": "integer",
    "select": "string (enum)",
    "bool": "boolean",
    "list": "array<string>",
    "sql": "string (SQL)",
    "kv": "object<string,string>",
    "repeat": "array<object>",
}

# Guidance that is true for a whole widget class, appended to every entry of that kind.
WIDGET_TIPS = {
    "list": ["Entered as a comma-separated list; written to the spec as a JSON array of strings."],
    "kv": ["Keys are written verbatim — a typo becomes a silently ignored option, not an error."],
    "repeat": ["Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save."],
    "sql": ["Supports {{catalog}} and {{env}} template variables, resolved at onboarding time."],
    "bool": ["Omitting the attribute is not the same as setting it false — check the default above."],
}


def _refs_for(path: str) -> List[str]:
    for needle, refs in REF_RULES:
        if needle in path:
            return refs
    return []


def _sample_value(attr: Dict[str, Any]) -> Any:
    """A plausible value for this attribute, preferring what the registry already declares."""
    ph, dflt, opts, widget = attr["placeholder"], attr["default"], attr["options"], attr["widget"]
    if widget == "bool":
        return dflt if isinstance(dflt, bool) else True
    if widget == "num":
        if isinstance(dflt, (int, float)):
            return dflt
        return int(ph) if str(ph).isdigit() else 30
    if widget == "list":
        base = ph or dflt or "column_a, column_b"
        return [p.strip() for p in str(base).split(",") if p.strip()]
    if widget == "kv":
        return {"option_name": "value"}
    if widget == "repeat":
        return [{"...": "one object per entry"}]
    if widget == "select":
        real = [o for o in (opts or []) if o]
        return dflt if dflt else (real[0] if real else "value")
    return ph or dflt or "value"


def _nest(path: str, value: Any) -> Dict[str, Any]:
    """`target_config.auto_ttl.expire_in_days` + 90 -> the nested object it becomes."""
    clean = path[1:] if path.startswith("@") else path
    clean = clean.split("[].")[-1] if "[]." in clean else clean
    parts = clean.split(".")
    out: Any = value
    for key in reversed(parts):
        out = {key: out}
    return out


def _derive(attr: Dict[str, Any]) -> Dict[str, Any]:
    path, widget = attr["path"], attr["widget"]
    desc = (attr["desc"] or "").strip()

    purpose = desc.split(". ")[0].rstrip(".") + "." if desc else f"Sets {attr['label']}."
    why = desc if desc and desc != purpose else ""

    tips: List[str] = []
    if attr["hint"]:
        tips.append(attr["hint"])
    if attr["required"]:
        tips.append("Required — onboarding rejects the flow if this is missing.")
    if attr["conditional"]:
        tips.append("Only applies to some configurations; the form hides it when it is not relevant.")
    if attr["options"]:
        real = [o for o in attr["options"] if o]
        if real:
            tips.append("Allowed values: " + ", ".join(real) + ".")
    tips.extend(WIDGET_TIPS.get(widget, []))

    sample = json.dumps(_nest(path, _sample_value(attr)), indent=2)

    return {
        "purpose": purpose,
        "why": why,
        "samples": [{"language": "json", "code": sample}],
        "tips": tips,
        "errors": [],
        "refs": _refs_for(path),
        "_meta": {
            "type": WIDGET_TYPE.get(widget, "string"),
            "required": attr["required"],
            "flows": [attr["flow"]],
            "section": attr["sectionTitle"],
            "doc": attr["sectionDoc"],
            "generated": True,
        },
    }


def dump_registry() -> Dict[str, Any]:
    """Run registry.js through node and return its attribute inventory."""
    node = shutil.which("node") or r"C:\Program Files\nodejs\node.exe"
    script = f"""
import * as R from {json.dumps(REGISTRY_JS.as_posix())};
const out=[], seen=new Set();
const push=(flow,sec,f,parent)=>{{
  if(!f||!f.p) return;
  const path=parent?`${{parent}}[].${{f.p}}`:f.p, key=flow+"|"+path;
  if(seen.has(key)) return; seen.add(key);
  out.push({{flow,section:sec.id,sectionTitle:sec.title,sectionDoc:sec.doc||"",path,
    label:f.l||f.p,widget:f.k,required:!!f.req,default:f.d===undefined?null:f.d,
    options:f.opts||null,placeholder:f.ph||"",desc:f.i||"",hint:f.hint||"",
    conditional:!!f.w}});
  (f.fields||[]).forEach(g=>push(flow,sec,g,path));
}};
[["root",R.ROOT_SECTIONS()],["ingestion",R.ING_SECTIONS()],["transformation",R.TRN_SECTIONS()],
 ["reconciliation",R.REC_SECTIONS()],["observability",R.OBS_SECTIONS()]]
  .forEach(([k,secs])=>secs.forEach(sec=>(sec.fields||[]).forEach(f=>push(k,sec,f))));
console.log(JSON.stringify({{attributes:out,
  cdc:R.CDC.map(c=>({{strategy:c[0],badge:c[1],desc:c[2],requires:c[3]}})),
  sections:[["root",R.ROOT_SECTIONS()],["ingestion",R.ING_SECTIONS()],["transformation",R.TRN_SECTIONS()],
   ["reconciliation",R.REC_SECTIONS()],["observability",R.OBS_SECTIONS()]]
   .map(([k,secs])=>({{flow:k,sections:secs.map(s=>({{id:s.id,title:s.title,sub:s.sub||"",doc:s.doc||""}}))}}))}}));
"""
    # react is bundled rather than externalised: the dump executes from a temp
    # directory, which has no node_modules to resolve a bare import against.
    # The CDC attributes are declared on the Builder component rather than in the
    # registry module, so the dump imports Builder.jsx as well and asks it directly.
    # Reaching them means transpiling JSX, hence the esbuild pass. Without this the
    # 11 CDC attributes — primary_keys and sequence_by_column among them — would get
    # no inspector entry at all.
    script += f"""
const B = (await import({json.dumps(BUILDER_JSX.as_posix())})).default;
const inst = Object.create(B.prototype);
const cdc = (inst.cdcFieldDefs ? inst.cdcFieldDefs() : []).map(f => ({{
  flow: "ingestion+transformation", section: "cdc", sectionTitle: "Load strategy",
  sectionDoc: "#8-target-config-shared-by-ingestion--transformation",
  path: f.p, label: f.l || f.p, widget: f.k, required: !!f.req,
  default: f.d === undefined ? null : f.d, options: f.opts || null,
  placeholder: f.ph || "", desc: f.i || "", hint: f.hint || "", conditional: !!f.w,
}}));
console.log("@@CDC@@" + JSON.stringify(cdc));
"""
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "dump.jsx"
        bundled = Path(td) / "dump.mjs"
        src.write_text(script, encoding="utf-8")
        esbuild = WEB_ROOT / "node_modules" / "esbuild" / "bin" / "esbuild"
        build = subprocess.run(
            [node, str(esbuild), str(src), "--bundle", "--platform=node", "--format=esm",
             "--loader:.jsx=jsx", f"--outfile={bundled}", "--log-level=error"],
            capture_output=True, text=True, encoding="utf-8", cwd=str(WEB_ROOT),
        )
        if build.returncode != 0:
            raise SystemExit(f"esbuild failed bundling the registry dump:\n{build.stderr}")
        # encoding is pinned: registry prose contains em dashes, and the Windows
        # console default would mangle them into mojibake in every derived entry.
        res = subprocess.run([node, str(bundled)], capture_output=True, text=True,
                             encoding="utf-8", cwd=str(WEB_ROOT))
    if res.returncode != 0:
        raise SystemExit(f"node failed reading the registry:\n{res.stderr}")

    head, _, cdc_line = res.stdout.partition("@@CDC@@")
    data = json.loads(head.strip())
    data["attributes"].extend(json.loads(cdc_line.strip() or "[]"))
    return data


def build() -> Dict[str, Any]:
    reg = dump_registry()

    # Collapse duplicates: the same attribute appears in several flows.
    merged: Dict[str, Dict[str, Any]] = {}
    for attr in reg["attributes"]:
        entry = merged.get(attr["path"])
        if entry is None:
            merged[attr["path"]] = _derive(attr)
        else:
            flows = entry["_meta"]["flows"]
            if attr["flow"] not in flows:
                flows.append(attr["flow"])

    # Curated prose wins field-by-field, so a hand-written entry can supply only the
    # parts worth writing and still inherit generated samples and references.
    curated = {}
    if CURATED.exists():
        curated = json.loads(CURATED.read_text(encoding="utf-8")).get("attributes", {})
    for path, hand in curated.items():
        # A curated entry with no registry match still needs a home in the docs, so
        # infer its flow from the path rather than dumping it in a catch-all bucket.
        if path.startswith("target_config."):
            fallback_flow = "ingestion+transformation"
        elif path.startswith("source_config."):
            fallback_flow = "ingestion"
        elif path.startswith("@"):
            fallback_flow = "root"
        else:
            fallback_flow = "ingestion"
        base = merged.setdefault(path, _derive({
            "path": path, "widget": "text", "desc": "", "hint": "", "label": path,
            "required": False, "conditional": False, "options": None, "placeholder": "",
            "default": None, "flow": fallback_flow, "sectionTitle": "", "sectionDoc": "",
        }))
        for key, value in hand.items():
            if value:
                base[key] = value
        base["_meta"]["generated"] = False

    return {
        "schema_version": "2.0",
        "_comment": (
            "GENERATED by databricks-app/scripts/build_attribute_knowledge.py — do not edit by hand. "
            "Hand-written prose belongs in attribute_knowledge.curated.json, which this file layers on top of."
        ),
        "reference_index": REFERENCE_INDEX,
        "defaults": {k: {"tips": v} for k, v in WIDGET_TIPS.items()},
        "cdc": reg.get("cdc", []),
        "attributes": dict(sorted(merged.items())),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="exit non-zero if the committed file is stale")
    args = ap.parse_args()

    built = build()
    rendered = json.dumps(built, indent=2, ensure_ascii=False) + "\n"

    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != rendered:
            print("attribute_knowledge.json is stale — re-run this script.", file=sys.stderr)
            return 1
        print("attribute_knowledge.json is up to date.")
        return 0

    OUTPUT.write_text(rendered, encoding="utf-8")
    hand = sum(1 for v in built["attributes"].values() if not v["_meta"]["generated"])
    print(f"wrote {OUTPUT.relative_to(APP_ROOT.parent)}")
    print(f"  {len(built['attributes'])} attributes  ({hand} curated, {len(built['attributes']) - hand} derived)")
    print(f"  {len(built['reference_index'])} documentation links")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
