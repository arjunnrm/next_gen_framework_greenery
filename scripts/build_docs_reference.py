#!/usr/bin/env python
"""
Generate the reference sections of the documentation wiki from source.

Three reference trees are produced, all derived rather than written by hand, so they
cannot drift from the thing they describe:

    docs/reference/json/     <- databricks-app/config/attribute_knowledge.json   (registry.js + curated prose)
                                databricks-app/config/attribute_faqs.json        (curated FAQs, >= 4 per attribute)
                                onboarding_templates/onboarding_spec.schema.json (types, enums, defaults, tree)
                                spec_validator.py                                 (removed keys, wrong-name aliases)
                                docs/v*_json_attribute_delta.json                 (the version an attribute arrived)
    docs/reference/code/     <- src/flowx/     (AST, no imports)
    docs/UC*/                <- BT_Usecase/<UC>/docs/

The JSON reference is the same data the Spec Builder renders as a form and the same
prose its attribute inspector shows, so the app and the docs always agree. Each attribute
page entry carries: status badges, the type/default/allowed-values table, the control-table
column the value is persisted into, a JSON sample, an offline-validation snippet, the CLI
onboarding invocation, a SQL verification query, best practice, known errors, and the FAQs.

The code reference is built by parsing the source with `ast`. Nothing is imported, so
this runs anywhere — no Spark, no Databricks connection, no installed wheel.

Every page is written only when its content changed, so `mkdocs serve` stays stable and
`git diff` shows real changes only.

Usage
-----
    python scripts/build_docs_reference.py
    python scripts/build_docs_reference.py --check    # CI: fail if committed output is stale
"""

from __future__ import annotations

import argparse
import ast
import html
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parent.parent
APP = REPO / "databricks-app"
SRC = REPO / "src" / "flowx"
DOCS = REPO / "docs"
JSON_REF = DOCS / "reference" / "json"
CODE_REF = DOCS / "reference" / "code"
SCHEMA = REPO / "onboarding_templates" / "onboarding_spec.schema.json"
VALIDATOR = SRC / "lakeflow_framework" / "onboarding" / "spec_validator.py"
SOURCE_PLANE = SRC / "lakeflow_framework" / "engine" / "source_plane.py"
FAQS = APP / "config" / "attribute_faqs.json"

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
    "other": "Other shared attributes",
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
    "other": (
        "Attributes that belong to no single flow kind — they appear nested inside a block "
        "(an `encrypted_columns[]` entry, for example) rather than at the top level of a flow."
    ),
}

FLOW_ORDER = ["root", "ingestion", "transformation", "ingestion+transformation",
              "reconciliation", "observability", "other"]

FLOW_BADGE = {
    "root": "Spec root",
    "ingestion": "Ingestion",
    "transformation": "Transformation",
    "ingestion+transformation": "Ingestion · Transformation",
    "reconciliation": "Reconciliation",
    "observability": "Observability",
    "other": "Shared",
}

# Where each flow kind's story is told — the pillar page, the console surface(s) and the
# control table. Paths are relative to docs/reference/json/.
FLOW_LINKS = {
    "root": {
        "pillar": ("Platform architecture", "../../01_platform_architecture.md"),
        "console": [("Spec Builder · Spec root", "../../console/spec_builder.md#spec-root-and-observability")],
        "table": "dataflow_group_spec",
    },
    "ingestion": {
        "pillar": ("Pillar 1 · Ingestion", "../../pillars/ingestion.md"),
        "console": [("Spec Builder · Ingestion tab", "../../console/spec_builder.md#ingestion-tab"),
                    ("Control dashboard · Ingestion Flows", "../../console/control_dashboard.md#ingestion-flows"),
                    ("Observability dashboard · Data Flow & Throughput",
                     "../../console/observability_dashboard.md#data-flow-throughput")],
        "table": "ingestion_flow_spec",
    },
    "transformation": {
        "pillar": ("Pillar 2 · Transformation", "../../pillars/transformation.md"),
        "console": [("Spec Builder · Transformation tab", "../../console/spec_builder.md#transformation-tab"),
                    ("Control dashboard · Transformation Flows",
                     "../../console/control_dashboard.md#transformation-flows"),
                    ("Observability dashboard · Framework & Lineage",
                     "../../console/observability_dashboard.md#framework-lineage")],
        "table": "transformation_flow_spec",
    },
    "ingestion+transformation": {
        "pillar": ("Pillar 2 · Transformation (load strategies)", "../../pillars/transformation.md#load-strategies"),
        "console": [("Spec Builder · Load strategy step", "../../console/spec_builder.md#ingestion-tab"),
                    ("Observability dashboard · Framework & Lineage",
                     "../../console/observability_dashboard.md#framework-lineage")],
        "table": "ingestion_flow_spec / transformation_flow_spec",
    },
    "reconciliation": {
        "pillar": ("Pillar 3 · Reconciliation", "../../pillars/reconciliation.md"),
        "console": [("Spec Builder · Reconciliation tab", "../../console/spec_builder.md#reconciliation-tab"),
                    ("Control dashboard · Reconciliation", "../../console/control_dashboard.md#reconciliation"),
                    ("Observability dashboard · Quality & Reconciliation",
                     "../../console/observability_dashboard.md#quality-reconciliation")],
        "table": "reconciliation_flow_spec",
    },
    "observability": {
        "pillar": ("Pillar 4 · Observability", "../../pillars/observability.md"),
        "console": [("Spec Builder · Observability tab", "../../console/spec_builder.md#spec-root-and-observability"),
                    ("Observability dashboard", "../../console/observability_dashboard.md"),
                    ("Control dashboard · Observability & Audit",
                     "../../console/control_dashboard.md#observability-audit")],
        "table": "observability_config",
    },
    "other": {
        "pillar": ("Pillar 2 · Transformation", "../../pillars/transformation.md"),
        "console": [("Spec Builder", "../../console/spec_builder.md")],
        "table": "ingestion_flow_spec / transformation_flow_spec",
    },
}

# Deep-dive anchors into 00_master_reference_index.md, chosen by path prefix. Resolved
# against the page's real headings at build time; an anchor that does not exist is dropped
# rather than emitted as a broken link.
MASTER_INDEX = "../../00_master_reference_index.md"
MASTER_ANCHORS: List[Tuple[str, str]] = [
    ("source_config.source_zip_handling", "source-zip-handling-source_zip_handling"),
    ("source_config.asn1", "asn1-specific-source_type-asn1"),
    ("source_config.", "3-source-config-reference"),
    ("target_config.sink_config", "4-target-config-cdc-reference"),
    ("target_config.encrypted_columns", "4-target-config-cdc-reference"),
    ("target_config.", "4-target-config-cdc-reference"),
    ("dq_config", "5-data-quality-config"),
    ("governance_tags", "6-governance-tagging"),
    ("source_inputs", "7-transformation-flow-schema"),
    ("decrypted_columns", "7-transformation-flow-schema"),
    ("transformation_sql", "7-transformation-flow-schema"),
    ("flow_step_id", "7-transformation-flow-schema"),
    ("target_configs", "83-target-only-fields"),
    ("@observability", "9-observability-config-schema"),
    ("@pipeline_parameters", "11-template-variables-parameter-substitution"),
    ("@", "1-top-level-spec-schema"),
]
MASTER_ANCHORS_BY_FLOW = {
    "ingestion": "2-ingestion-flow-schema",
    "transformation": "7-transformation-flow-schema",
    "reconciliation": "8-reconciliation-flow-schema",
    "observability": "9-observability-config-schema",
    "ingestion+transformation": "12-cdc-load-strategies-quick-reference",
    "root": "1-top-level-spec-schema",
}

FAQ_KIND_LABEL = {
    "omitted": "If omitted",
    "format": "Format gotcha",
    "performance": "Performance impact",
    "edge_case": "Edge case",
}

SPEC_VOLUME = "/Volumes/<catalog>/config/onboarding_specs"


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────

def write_if_changed(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_text(encoding="utf-8") != text:
        path.write_text(text, encoding="utf-8")
    return path


def anchor(path: str) -> str:
    return path.replace("@", "").replace(".", "").replace("[]", "").replace("_", "-").lower()


def md_escape(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ").strip()


def title_escape(text: str) -> str:
    """Admonition titles sit inside double quotes; swap any inner ones for single quotes."""
    return (text or "").replace('"', "'").replace("\n", " ").strip()


def badge(kind: str, label: str) -> str:
    return f'<span class="fx-badge fx-{kind}">{html.escape(label)}</span>'


def page_slug(flow: str) -> str:
    return flow.replace("+", "-")


def heading_slugs(md_path: Path) -> set:
    """The anchors a Markdown page will expose, using MkDocs' default slugifier."""
    try:
        from markdown.extensions.toc import slugify  # type: ignore
    except Exception:  # pragma: no cover - markdown is a mkdocs dependency
        return set()
    slugs = set()
    if not md_path.exists():
        return slugs
    for line in md_path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^(#{1,6})\s+(.*?)\s*#*\s*$", line)
        if m:
            text = re.sub(r"\{\s*#[^}]*\}\s*$", "", m.group(2)).strip()
            slugs.add(slugify(text, "-"))
            explicit = re.search(r"\{\s*#([^}\s]+)\s*\}", m.group(2))
            if explicit:
                slugs.add(explicit.group(1))
    return slugs


# ──────────────────────────────────────────────────────────────────────────────
# Sources
# ──────────────────────────────────────────────────────────────────────────────

def load_knowledge() -> Dict[str, Any]:
    kb = APP / "config" / "attribute_knowledge.json"
    if not kb.exists():
        raise SystemExit(
            "config/attribute_knowledge.json is missing — run "
            "databricks-app/scripts/build_attribute_knowledge.py first."
        )
    return json.loads(kb.read_text(encoding="utf-8"))


def load_faqs() -> Dict[str, List[Dict[str, str]]]:
    if not FAQS.exists():
        return {}
    data = json.loads(FAQS.read_text(encoding="utf-8"))
    return data.get("attributes", {}) if isinstance(data, dict) else {}


def load_schema() -> Dict[str, Any]:
    if not SCHEMA.exists():
        return {}
    return json.loads(SCHEMA.read_text(encoding="utf-8"))


class SchemaIndex:
    """Best-effort lookup of a knowledge path (relative to a flow) inside the JSON schema."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.defs = schema.get("$defs", {}) if schema else {}

    def resolve(self, node: Dict[str, Any]) -> Dict[str, Any]:
        seen = 0
        while isinstance(node, dict) and "$ref" in node and seen < 10:
            node = self.defs.get(node["$ref"].split("/")[-1], {})
            seen += 1
        if isinstance(node, dict) and "allOf" in node:
            # Keep the node's own keys (a flow def declares `properties` AND an `allOf` of
            # if/then rules), then fold in whatever the branches declare on top.
            merged: Dict[str, Any] = {k: v for k, v in node.items() if k != "allOf"}
            merged["properties"] = dict(node.get("properties", {}))
            merged["required"] = list(node.get("required", []))
            merged.setdefault("type", "object")
            for branch in node["allOf"]:
                branch = self.resolve(branch)
                merged["properties"].update(branch.get("properties", {}))
                merged["required"] = list(dict.fromkeys(merged["required"] + branch.get("required", [])))
            return merged
        return node if isinstance(node, dict) else {}

    def roots(self) -> List[Dict[str, Any]]:
        if not self.schema:
            return []
        out = [self.schema]
        for name in ("ingestionFlow", "transformationFlow", "reconciliationFlow", "observabilityDestination"):
            if name in self.defs:
                out.append(self.resolve(self.defs[name]))
        return out

    def lookup(self, path: str) -> Optional[Dict[str, Any]]:
        parts = [p for p in path.lstrip("@").replace("[]", "").split(".") if p]
        if not parts:
            return None
        for root in self.roots():
            node: Dict[str, Any] = root
            ok = True
            for part in parts:
                node = self.resolve(node)
                if node.get("type") == "array" and "items" in node:
                    node = self.resolve(node["items"])
                props = node.get("properties", {})
                if part in props:
                    node = props[part]
                else:
                    ok = False
                    break
            if ok:
                return self.resolve(node)
        return None


def _dict_literals(py_file: Path, names: set, extra_consts: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    """Extract ``NAME = {"key": "text", ...}`` assignments from a module without importing it."""
    out: Dict[str, Dict[str, str]] = {}
    if not py_file.exists():
        return out
    tree = ast.parse(py_file.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Dict):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in names:
                entries: Dict[str, str] = {}
                for k, v in zip(node.value.keys, node.value.values):
                    if not isinstance(k, ast.Constant):
                        continue
                    if isinstance(v, ast.Constant):
                        entries[str(k.value)] = str(v.value)
                    elif isinstance(v, ast.Name) and v.id in extra_consts:
                        entries[str(k.value)] = extra_consts[v.id]
                out[target.id] = entries
    return out


def _string_consts(py_file: Path, names: set) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not py_file.exists():
        return out
    tree = ast.parse(py_file.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in names:
                    out[target.id] = str(node.value.value)
    return out


def load_removed() -> Dict[str, Dict[str, str]]:
    consts = _string_consts(SOURCE_PLANE, {"MATERIALIZE_NEVER_REJECTION"})
    return _dict_literals(
        VALIDATOR,
        {
            "REMOVED_SOURCE_CONFIG_KEYS", "REMOVED_TARGET_CONFIG_KEYS", "REMOVED_RECONCILIATION_FLOW_KEYS",
            "REMOVED_CDC_LOAD_STRATEGIES", "REMOVED_MATERIALIZE_POLICIES",
            "REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE", "RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE",
            "UNKNOWN_KEY_ALIASES",
        },
        consts,
    )


_FLOW_PREFIXES = ("ingestion_flows[].", "transformation_flows[].", "reconciliation_flows[].")


def _normalise_delta_path(raw: str) -> Tuple[str, bool]:
    """Return (knowledge-style path, came_from_a_flow)."""
    p = (raw or "").strip()
    from_flow = False
    for pre in ("$.", "*."):
        if p.startswith(pre):
            p = p[len(pre):]
            from_flow = True  # `*.` means "any flow kind"
    for pre in _FLOW_PREFIXES:
        if p.startswith(pre):
            p = p[len(pre):]
            from_flow = True
    if p.endswith("[]"):
        p = p[:-2]
    if p.startswith("observability[]"):
        p = "@" + p
        from_flow = True
    return p, from_flow


def load_versions(knowledge_paths: set) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """(attribute -> version added, attribute -> versions with a behaviour change)."""
    since: Dict[str, str] = {}
    changed: Dict[str, List[str]] = {}

    def candidates(p: str, from_flow: bool) -> List[str]:
        cands = [p]
        if not from_flow and not p.startswith("@"):
            cands.insert(0, "@" + p)
        segs = p.split(".")
        for i in range(1, len(segs)):
            cands.append(".".join(segs[i:]))
        return cands

    def match(raw: str) -> Optional[str]:
        p, from_flow = _normalise_delta_path(raw)
        for c in candidates(p, from_flow):
            if c in knowledge_paths:
                return c
        return None

    def version_key(v: str) -> Tuple[int, ...]:
        return tuple(int(x) for x in re.findall(r"\d+", v))

    for delta in sorted(DOCS.glob("v*_json_attribute_delta.json")):
        try:
            d = json.loads(delta.read_text(encoding="utf-8"))
        except Exception:
            continue
        version = str(d.get("version") or d.get("framework_version_to") or "").strip()
        if not version:
            continue
        added_items: List[Dict[str, Any]] = []
        changed_items: List[Dict[str, Any]] = []
        for key in ("added", "added_attributes"):
            added_items += [x for x in d.get(key, []) if isinstance(x, dict)]
        for key in ("modified", "modified_attributes"):
            changed_items += [x for x in d.get(key, []) if isinstance(x, dict)]
        for x in d.get("attributes", []) if isinstance(d.get("attributes"), list) else []:
            if isinstance(x, dict):
                (added_items if str(x.get("change", "")).lower() == "added" else changed_items).append(x)
        for item in added_items:
            hit = match(str(item.get("path", "")))
            if hit and (hit not in since or version_key(version) < version_key(since[hit])):
                since[hit] = version
        for item in changed_items:
            hit = match(str(item.get("path", "")))
            if hit:
                changed.setdefault(hit, [])
                if version not in changed[hit]:
                    changed[hit].append(version)
    for k in changed:
        changed[k].sort(key=version_key)
    return since, changed


# ──────────────────────────────────────────────────────────────────────────────
# Per-attribute rendering
# ──────────────────────────────────────────────────────────────────────────────

def persisted_in(path: str, flow: str) -> Optional[str]:
    """The control-table column an attribute's value lands in after onboarding."""
    p = path.lstrip("@")
    table = FLOW_LINKS.get(flow, {}).get("table", "")
    if flow == "root":
        col = {"dataflow_group_id": "dataflow_group_id",
               "pipeline_parameters": "pipeline_parameters_json",
               "spark_config": "spark_config_json"}.get(p)
        return f"config.dataflow_group_spec.{col}" if col else None
    if flow == "observability":
        first = p.replace("observability[].", "").split(".")[0]
        col = {"id": "destination_id", "type": "destination_type", "mode": "mode", "enabled": "enabled",
               "destination_config": "destination_config_json", "auth": "auth_config_json",
               "retry": "retry_config_json", "timeout_ms": "retry_config_json",
               "observability": "(one row per destination)"}.get(first)
        return f"config.observability_config.{col}" if col else None
    first = p.split(".")[0].replace("[]", "")
    if flow == "reconciliation":
        col = {"reconciliation_id": "reconciliation_id", "dataflow_group_id": "dataflow_group_id",
               "source_config": "source_config_json", "target_configs": "target_configs_json",
               "match_keys": "match_keys_json", "compare_columns": "compare_columns_json",
               "transform_sql": "transform_sql", "error_handling": "error_handling_json",
               "logging_config": "logging_config_json", "two_tier_verification": "two_tier_verification",
               "execution_mode": "execution_mode", "publish_schema": "publish_schema",
               "dq_config": "dq_config_json"}.get(first)
        return f"config.reconciliation_flow_spec.{col}" if col else None
    col = {"source_config": "source_config_json", "target_config": "target_config_json",
           "dq_config": "dq_config_json", "governance_tags": "governance_tags_json",
           "source_inputs": "source_inputs_json", "decrypted_columns": "source_inputs_json",
           "transformation_sql": "transformation_sql", "source_data_type": "target_config_json",
           "dataflow_id": "dataflow_id", "flow_step_id": "flow_step_id", "source_system": "source_system",
           "source_database": "source_database", "source_table_name": "source_table_name",
           "source_description": "source_description", "source_type": "source_type",
           "target_catalog": "target_catalog", "target_schema": "target_schema",
           "target_table": "target_table", "target_type": "target_type"}.get(first)
    if not col:
        return None
    if first == "target_config" and p.startswith("target_config.cdc_load_strategy"):
        col = "cdc_load_strategy"
    return f"config.{table}.{col}"


def _sql_verify(path: str, flow: str) -> str:
    target = persisted_in(path, flow) or ""
    table = FLOW_LINKS.get(flow, {}).get("table", "ingestion_flow_spec").split(" / ")[0]
    column = target.split(".")[-1] if target else "*"
    key = {"reconciliation": "reconciliation_id", "observability": "destination_id",
           "root": "dataflow_group_id", "transformation": "flow_step_id"}.get(flow, "dataflow_id")
    if column.endswith("_json"):
        select = f"{key},\n       {column}"
        hint = f"-- {path} lives inside the {column} JSON document; inspect it with from_json / get_json_object"
    else:
        select = f"{key},\n       {column}"
        hint = f"-- {path} is persisted as its own column"
    return (
        f"{hint}\n"
        f"SELECT {select},\n       is_active, updated_at\n"
        f"FROM   <catalog>.config.{table}\n"
        f"WHERE  dataflow_group_id = '<dataflow_group_id>';"
    )


def render_type_table(meta: Dict[str, Any], node: Optional[Dict[str, Any]]) -> List[str]:
    node = node or {}
    typ = meta.get("type", node.get("type", "string"))
    default = node.get("default")
    enum = node.get("enum")
    if not enum and node.get("type") == "array":
        items = node.get("items") or {}
        if isinstance(items, dict) and items.get("enum"):
            enum = items["enum"]
    constraints: List[str] = []
    for key, label in (("pattern", "pattern"), ("minimum", "min"), ("maximum", "max"),
                       ("minLength", "min length"), ("minItems", "min items"), ("format", "format")):
        if key in node:
            constraints.append(f"{label} `{node[key]}`")
    if node.get("additionalProperties") is False:
        constraints.append("no unknown keys")
    rows = [
        "| Type | Default | Allowed values | Constraints |",
        "|---|---|---|---|",
        "| `{}` | {} | {} | {} |".format(
            md_escape(str(typ)),
            f"`{json.dumps(default)}`" if default is not None else "—",
            ", ".join(f"`{v}`" for v in enum) if enum else "—",
            ", ".join(constraints) if constraints else "—",
        ),
    ]
    return rows


def render_attribute(
    path: str,
    entry: Dict[str, Any],
    flow: str,
    refs: Dict[str, str],
    schema: SchemaIndex,
    faqs: Dict[str, List[Dict[str, str]]],
    since: Dict[str, str],
    changed: Dict[str, List[str]],
    master_slugs: set,
    tree_anchor: Optional[str],
) -> List[str]:
    meta = entry.get("_meta", {})
    out: List[str] = [f"### `{path}` {{ #{anchor(path)} }}\n"]

    badges = [badge("req", "Required") if meta.get("required") else badge("opt", "Optional")]
    for f in meta.get("flows", [flow]):
        badges.append(badge("flow", FLOW_BADGE.get(f, f)))
    if path in since:
        badges.append(badge("ver", f"v{since[path]}+"))
    if meta.get("section"):
        badges.append(badge("only", str(meta["section"])))
    out.append(" ".join(badges) + "\n")

    if entry.get("purpose"):
        out.append(entry["purpose"] + "\n")
    if entry.get("why"):
        out.append(f"\n{entry['why']}\n")

    out.append("\n**Type & constraints**\n")
    out += render_type_table(meta, schema.lookup(path))
    persisted = persisted_in(path, flow)
    meta_bits = []
    if persisted:
        meta_bits.append(f"**Persisted in** `{persisted}`")
    if path in changed:
        meta_bits.append("**Behaviour changed in** " + ", ".join(f"v{v}" for v in changed[path]))
    if meta_bits:
        out.append("\n" + " · ".join(meta_bits) + "\n")

    # Verification tabs: what you write, how you prove it offline, how you onboard it, how you
    # confirm what actually landed.
    samples = entry.get("samples", [])
    sample_code = (samples[0].get("code", "") if samples else "").rstrip()
    sample_lang = samples[0].get("language", "json") if samples else "json"
    out.append("")
    out.append('=== "JSON"')
    out.append("")
    if sample_code:
        out.append(f"    ```{sample_lang}")
        out += ["    " + line for line in sample_code.splitlines()]
        out.append("    ```")
    else:
        out.append("    _No sample recorded for this attribute yet._")
    out.append("")
    out.append('=== "Validate offline"')
    out.append("")
    out.append("    ```python")
    out.append("    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json")
    out.append("")
    out.append('    with open("dfg_orders.json", encoding="utf-8") as fh:')
    out.append('        result = validate_json(fh.read(), catalog="<catalog>", env="dev")')
    out.append("")
    out.append('    print(result["summary"])        # PASSED, or FAILED with the error count')
    out.append('    for err in result["errors"]:    # each error names the offending spec path')
    out.append("        print(err)")
    out.append("    ```")
    out.append("")
    out.append("    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.")
    out.append("")
    out.append('=== "Onboard (CLI)"')
    out.append("")
    out.append("    ```bash")
    out.append("    # 1. Dry run against the live catalog: validates, writes nothing")
    out.append("    databricks bundle run onboarding_job -t <target> \\")
    out.append(f"      --params spec_file_path={SPEC_VOLUME}/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY")
    out.append("")
    out.append("    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one")
    out.append("    databricks bundle run onboarding_job -t <target> \\")
    out.append(f"      --params spec_file_path={SPEC_VOLUME}/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE")
    out.append("    ```")
    out.append("")
    out.append("    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.")
    out.append("")
    out.append('=== "Verify (SQL)"')
    out.append("")
    out.append("    ```sql")
    out += ["    " + line for line in _sql_verify(path, flow).splitlines()]
    out.append("    ```")
    out.append("")

    tips = entry.get("tips", [])
    if tips:
        out.append('!!! tip "Best practice"\n')
        for t in tips:
            out.append(f"    - {t}")
        out.append("")

    errors = entry.get("errors", [])
    if errors:
        out.append('!!! warning "Known errors and limitations"\n')
        for e in errors:
            out.append(f"    **{e.get('symptom', '')}**  ")
            if e.get("cause"):
                out.append(f"    *Cause:* {e['cause']}  ")
            if e.get("fix"):
                out.append(f"    *Fix:* {e['fix']}")
            out.append("")

    entry_faqs = faqs.get(path, [])
    if entry_faqs:
        out.append(f"**FAQs** ({len(entry_faqs)})\n")
        for faq in entry_faqs:
            label = FAQ_KIND_LABEL.get(faq.get("kind", ""), "FAQ")
            out.append(f'??? question "{label} · {title_escape(faq.get("q", ""))}"')
            out.append("")
            for line in (faq.get("a", "") or "").splitlines() or [""]:
                out.append(f"    {line}")
            out.append("")

    # See also: recipe (pillar), schema (master index + tree), dashboard (console).
    links: List[str] = []
    pillar = FLOW_LINKS.get(flow, {}).get("pillar")
    if pillar:
        links.append(f"[{pillar[0]}]({pillar[1]})")
    master_anchor = None
    for prefix, slug in MASTER_ANCHORS:
        if path.startswith(prefix):
            master_anchor = slug
            break
    if master_anchor not in master_slugs:
        master_anchor = MASTER_ANCHORS_BY_FLOW.get(flow)
    if master_anchor in master_slugs:
        links.append(f"[Attribute dictionary]({MASTER_INDEX}#{master_anchor})")
    else:
        links.append(f"[Attribute dictionary]({MASTER_INDEX})")
    if tree_anchor:
        links.append(f"[Schema tree](tree.md#{tree_anchor})")
    for label, href in FLOW_LINKS.get(flow, {}).get("console", []):
        links.append(f"[{label}]({href})")
    links.append("[Gotchas](../../13_known_limitations_and_gotchas.md)")
    out.append("\n**See also:** " + " · ".join(links) + "\n")

    ext = [f"[{k.replace('_', ' ')}]({refs[k]})" for k in entry.get("refs", []) if k in refs]
    if ext:
        out.append("\n**Databricks documentation:** " + " · ".join(ext) + "\n")

    out.append("\n---\n")
    return out


def render_flow_page(
    flow: str,
    attrs: List[Tuple[str, Dict[str, Any]]],
    refs: Dict[str, str],
    schema: SchemaIndex,
    faqs: Dict[str, List[Dict[str, str]]],
    since: Dict[str, str],
    changed: Dict[str, List[str]],
    master_slugs: set,
    tree_anchors: Dict[str, str],
) -> str:
    title = FLOW_TITLES.get(flow, flow)
    n_faq = sum(len(faqs.get(p, [])) for p, _ in attrs)
    out = [BANNER, f"# {title}\n", FLOW_INTROS.get(flow, "") + "\n"]
    out.append(
        f"\n!!! info \"{len(attrs)} attributes · {n_faq} FAQs\"\n"
        f"    Every attribute below is also available in the Spec Builder's attribute\n"
        f"    inspector — click the **i** beside any field to see this same content\n"
        f"    without leaving the form. Badges: "
        f"{badge('req', 'Required')} {badge('opt', 'Optional')} {badge('flow', 'Flow kind')} "
        f"{badge('ver', 'vX.Y+ added in')} {badge('only', 'Spec Builder section')}.\n"
    )
    pillar = FLOW_LINKS.get(flow, {}).get("pillar")
    if pillar:
        out.append(f"\n**Recipes and deep dive:** [{pillar[0]}]({pillar[1]}) · "
                   f"[Schema tree](tree.md) · [Removed & rejected](removed.md)\n")

    out.append("\n## Summary\n")
    out.append("| Attribute | Type | Required | Default | Since |")
    out.append("|---|---|---|---|---|")
    for path, entry in attrs:
        meta = entry.get("_meta", {})
        node = schema.lookup(path) or {}
        dflt = node.get("default")
        out.append(
            f"| [`{path}`](#{anchor(path)}) "
            f"| {md_escape(meta.get('type', 'string'))} "
            f"| {'**yes**' if meta.get('required') else 'no'} "
            f"| {'`' + json.dumps(dflt) + '`' if dflt is not None else '—'} "
            f"| {('v' + since[path]) if path in since else '—'} |"
        )

    out.append("\n## Attributes\n")
    for path, entry in attrs:
        out += render_attribute(path, entry, flow, refs, schema, faqs, since, changed,
                                master_slugs, tree_anchors.get(path))
    return "\n".join(out) + "\n"


# ──────────────────────────────────────────────────────────────────────────────
# Schema tree page
# ──────────────────────────────────────────────────────────────────────────────

class TreeRenderer:
    def __init__(self, schema: SchemaIndex, attrs: Dict[str, Any]):
        self.schema = schema
        self.attrs = attrs
        self.anchors: Dict[str, str] = {}  # knowledge path -> tree anchor id
        self._ids: set = set()

    def _page_for(self, kpath: str, ctx: Optional[str]) -> Optional[str]:
        flows = self.attrs[kpath].get("_meta", {}).get("flows", ["other"])
        if ctx in flows:
            return page_slug(ctx)
        if "ingestion+transformation" in flows and ctx in ("ingestion", "transformation"):
            return "ingestion-transformation"
        return page_slug(flows[0]) if flows else None

    def _resolve_kpath(self, rel: str, ctx: Optional[str]) -> Optional[str]:
        cands = []
        if ctx == "observability":
            cands.append("@observability[]." + rel if rel else "@observability")
        elif ctx is None:
            cands.append("@" + rel)
        cands.append(rel)
        segs = rel.split(".")
        for i in range(1, len(segs)):
            cands.append(".".join(segs[i:]))
        for c in cands:
            if c in self.attrs:
                return c
        return None

    def _prefix_candidates(self, rel: str, ctx: Optional[str]) -> List[str]:
        """Knowledge-path prefixes a container at (rel, ctx) could own, most specific first."""
        cands: List[str] = []
        if ctx == "observability":
            cands.append("@observability[]." + rel if rel else "@observability")
        elif ctx is None:
            cands.append("@" + rel if rel else "")
        if rel:
            cands.append(rel)
            segs = rel.split(".")
            for i in range(1, len(segs)):
                cands.append(".".join(segs[i:]))
        return [c for c in cands if c]

    def _registry_only_children(self, rel: str, ctx: Optional[str], is_array: bool, props: Dict[str, Any]) -> List[Tuple[str, str]]:
        """Registry attributes nested directly under this container that the JSON schema does not declare.

        `destination_config.*` (free-form in the schema), Builder emission helpers such as
        `partition_mode`, and the three-box split of a reconciliation target's table name all
        live only in registry.js; they still deserve a leaf so the tree accounts for every
        documented attribute.
        """
        for prefix in self._prefix_candidates(rel, ctx):
            sep = "[]." if is_array else "."
            found: List[Tuple[str, str]] = []
            for k in self.attrs:
                if not k.startswith(prefix + sep):
                    continue
                rest = k[len(prefix) + len(sep):]
                if "." in rest or "[]" in rest or rest in props or rest in ("",):
                    continue
                found.append((rest, k))
            if found:
                return sorted(found)
        return []

    def _extra_leaf(self, name: str, kpath: str, ctx: Optional[str], out: List[str]) -> None:
        tid = self._tree_id(kpath, ctx)
        if kpath not in self.anchors:
            self.anchors[kpath] = tid
        page = self._page_for(kpath, ctx)
        link = f' <a href="../{page}/#{anchor(kpath)}" title="Open the attribute reference">reference ↗</a>' if page else ""
        out.append(f'<li id="{tid}"><code>{html.escape(name)}</code> '
                   f'<span class="fx-type">registry-only · validated by spec_validator, not in the JSON schema</span>{link}</li>')

    def _tree_id(self, rel: str, ctx: Optional[str]) -> str:
        base = "tree-" + anchor((ctx or "root") + "-" + (rel or "root"))
        tid, n = base, 2
        while tid in self._ids:
            tid, n = f"{base}-{n}", n + 1
        self._ids.add(tid)
        return tid

    def _leaf_meta(self, node: Dict[str, Any]) -> str:
        bits = []
        typ = node.get("type")
        if isinstance(typ, list):
            typ = " | ".join(str(t) for t in typ)
        if node.get("type") == "array":
            items = self.schema.resolve(node.get("items") or {})
            typ = f"array<{items.get('type', 'object') if not items.get('enum') else 'enum'}>"
            if items.get("enum"):
                bits.append(f'<span class="fx-enum">{html.escape(" | ".join(map(str, items["enum"])))}</span>')
        if typ:
            bits.insert(0, f'<span class="fx-type">{html.escape(str(typ))}</span>')
        if node.get("enum"):
            bits.append(f'<span class="fx-enum">{html.escape(" | ".join(map(str, node["enum"])))}</span>')
        if node.get("default") is not None:
            bits.append(f'<span class="fx-default">default {html.escape(json.dumps(node["default"]))}</span>')
        return " ".join(bits)

    def render(self, name: str, node: Dict[str, Any], rel: str, ctx: Optional[str],
               required: bool, depth: int, out: List[str]) -> None:
        node = self.schema.resolve(node)
        kpath = self._resolve_kpath(rel, ctx) if rel else None
        tid = self._tree_id(rel, ctx)
        if kpath and kpath not in self.anchors:
            self.anchors[kpath] = tid
        label = f"<code>{html.escape(name)}</code>" + (' <span class="fx-badge fx-req">req</span>' if required else "")
        link = ""
        if kpath:
            page = self._page_for(kpath, ctx)
            if page:
                link = f' <a href="../{page}/#{anchor(kpath)}" title="Open the attribute reference">reference ↗</a>'

        is_obj = node.get("type") == "object" or "properties" in node
        items = self.schema.resolve(node.get("items") or {}) if node.get("type") == "array" else {}
        is_obj_array = bool(items) and (items.get("type") == "object" or "properties" in items)

        if is_obj and node.get("properties"):
            req = set(node.get("required", []))
            props = node["properties"]
            extras = self._registry_only_children(rel, ctx, False, props)
            out.append(f'<details id="{tid}"{" open" if depth < 1 else ""}><summary>{label} '
                       f'<span class="fx-type">object · {len(props) + len(extras)} keys</span>{link}</summary><ul>')
            for child, cnode in props.items():
                self.render(child, cnode, f"{rel}.{child}" if rel else child, ctx, child in req, depth + 1, out)
            for name, kpath in extras:
                self._extra_leaf(name, kpath, ctx, out)
            out.append("</ul></details>")
        elif is_obj_array:
            req = set(items.get("required", []))
            props = items.get("properties", {})
            child_ctx = ctx
            if not rel or rel in ("ingestion_flows", "transformation_flows", "reconciliation_flows", "observability"):
                child_ctx = {"ingestion_flows": "ingestion", "transformation_flows": "transformation",
                             "reconciliation_flows": "reconciliation", "observability": "observability"}.get(rel, ctx)
            child_rel_base = "" if child_ctx != ctx else rel + "[]"
            extras = self._registry_only_children(rel, ctx, True, props) if child_ctx == ctx else []
            out.append(f'<details id="{tid}"{" open" if depth < 1 else ""}><summary>{label}<code>[]</code> '
                       f'<span class="fx-type">array of object · {len(props) + len(extras)} keys</span>{link}</summary><ul>')
            for child, cnode in props.items():
                child_rel = f"{child_rel_base}.{child}" if child_rel_base else child
                self.render(child, cnode, child_rel, child_ctx, child in req, depth + 1, out)
            for name, kpath in extras:
                self._extra_leaf(name, kpath, child_ctx, out)
            out.append("</ul></details>")
        else:
            # A free-form map, or a scalar the registry refines into sub-fields (e.g. the
            # schema's `delete_source_after_extract` is loosely typed; the registry knows
            # `.action` and `.days`). Either way, registry-only children turn it into a branch.
            extras = self._registry_only_children(rel, ctx, False, {})
            if is_obj:
                extra = node.get("additionalProperties")
                vtype = extra.get("type", "any") if isinstance(extra, dict) else "any"
                if isinstance(vtype, list):
                    vtype = " | ".join(map(str, vtype))
                meta = f'<span class="fx-type">object&lt;string, {html.escape(str(vtype))}&gt;</span>'
            else:
                meta = self._leaf_meta(node)
            if extras:
                out.append(f'<details id="{tid}"><summary>{label} {meta}{link}</summary><ul>')
                for name, kpath in extras:
                    self._extra_leaf(name, kpath, ctx, out)
                out.append("</ul></details>")
            else:
                out.append(f'<li id="{tid}">{label} {meta}{link}</li>')


def build_tree_page(schema: SchemaIndex, attrs: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    renderer = TreeRenderer(schema, attrs)
    body: List[str] = []
    if schema.schema:
        root = schema.schema
        req = set(root.get("required", []))
        body.append('<div class="fx-tree" markdown="0">')
        body.append('<details id="tree-root" open><summary><code>spec</code> <span class="fx-type">object · '
                    f'{len(root.get("properties", {}))} keys</span></summary><ul>')
        for name, node in root.get("properties", {}).items():
            if name == "$schema":
                continue
            renderer.render(name, node, name, None, name in req, 1, body)
        body.append("</ul></details>")
        body.append("</div>")
    n_attr = len(attrs)
    out = [
        BANNER,
        "# Spec tree view\n",
        "The whole onboarding document as one collapsible tree, generated from "
        "`onboarding_templates/onboarding_spec.schema.json` — the same schema the onboarding job "
        "enforces. Expand a node to see its keys; **reference ↗** opens the attribute's full entry "
        "(type, sample, validation, FAQs).\n",
        "\n!!! info \"How to read it\"\n"
        "    `req` marks a key the schema requires inside its parent. *italic* is the JSON type, "
        "orange text lists the allowed values, grey text the default. A key with no **reference ↗** "
        "link is structural (a container) or is documented on its parent.\n",
        f"\n**{n_attr} documented attributes** · [Master reference](index.md) · "
        "[Removed & rejected](removed.md) · [Docs ↔ code synchronisation](../sync.md)\n",
        "\n## Tree\n",
        "\n".join(body),
        "\n## Reading the shape as a document\n",
        "```mermaid\nflowchart TB\n"
        "  S[spec] --> G[dataflow_group_id]\n"
        "  S --> P[pipeline_parameters]\n"
        "  S --> C[spark_config]\n"
        "  S --> SP[source_plane]\n"
        "  S --> I[\"ingestion_flows[]\"]\n"
        "  S --> T[\"transformation_flows[]\"]\n"
        "  S --> R[\"reconciliation_flows[]\"]\n"
        "  S --> O[\"observability[]\"]\n"
        "  I --> IS[source_config] & IT[target_config] & IQ[dq_config] & IG[governance_tags]\n"
        "  T --> TI[\"source_inputs[]\"] & TS[transformation_sql] & TT[target_config] & TQ[dq_config] & TG[governance_tags]\n"
        "  R --> RS[source_config] & RT[\"target_configs[]\"] & RM[match_keys] & RC[compare_columns]\n"
        "  O --> OD[destination_config] & OA[auth] & ORt[retry]\n"
        "```\n",
    ]
    return "\n".join(out) + "\n", renderer.anchors


# ──────────────────────────────────────────────────────────────────────────────
# Removed & rejected page
# ──────────────────────────────────────────────────────────────────────────────

def _removed_table(entries: Dict[str, str], where: str, kind: str = "dep") -> List[str]:
    rows = ["| Attribute | Status | Rejection message (verbatim) |", "|---|---|---|"]
    for key, msg in entries.items():
        rows.append(f"| `{where}{key}` | {badge(kind, 'Removed' if kind == 'dep' else 'Rejected')} | {md_escape(msg)} |")
    return rows


def build_removed_page(removed: Dict[str, Dict[str, str]]) -> str:
    out = [
        BANNER,
        "# Removed & rejected attributes\n",
        "Everything the onboarding validator rejects **on presence** — a removed key, a removed enum "
        "value, a key that is legal only in another execution mode, or a name the framework never "
        "had. Generated from the dictionaries in `spec_validator.py`, so the messages below are the "
        "exact text the onboarding job prints.\n",
        "\n!!! danger \"Removals are rejected, never ignored\"\n"
        "    An ignored key still onboards, still writes its control-table row and still runs the "
        "pipeline — while quietly doing something other than what the document says. Any attribute "
        "that once switched a data-shaping behaviour ON would silently switch it OFF. So presence is "
        "the trigger: `\"flag\": false` is still a statement about a feature that no longer exists.\n",
    ]

    out.append("\n## Removed attributes\n")
    out.append("Delete the key and apply the migration the message names.\n")
    if removed.get("REMOVED_SOURCE_CONFIG_KEYS"):
        out.append("\n### `source_config.*`\n")
        out += _removed_table(removed["REMOVED_SOURCE_CONFIG_KEYS"], "source_config.")
    if removed.get("REMOVED_TARGET_CONFIG_KEYS"):
        out.append("\n### `target_config.*`\n")
        out += _removed_table(removed["REMOVED_TARGET_CONFIG_KEYS"], "target_config.")
    if removed.get("REMOVED_RECONCILIATION_FLOW_KEYS"):
        out.append("\n### `reconciliation_flows[].*`\n")
        out += _removed_table(removed["REMOVED_RECONCILIATION_FLOW_KEYS"], "reconciliation_flows[].")

    out.append("\n## Removed enum values\n")
    out.append("The attribute survives; one of its values does not.\n")
    if removed.get("REMOVED_CDC_LOAD_STRATEGIES"):
        out.append("\n### `target_config.cdc_load_strategy`\n")
        out += _removed_table(removed["REMOVED_CDC_LOAD_STRATEGIES"], "cdc_load_strategy = ")
    if removed.get("REMOVED_MATERIALIZE_POLICIES"):
        out.append("\n### `source_plane.materialize`\n")
        out += _removed_table(removed["REMOVED_MATERIALIZE_POLICIES"], "materialize = ")

    out.append("\n## Keys rejected in a particular execution mode\n")
    out.append("Legal in one reconciliation `execution_mode`, rejected on presence in another.\n")
    if removed.get("REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE"):
        out.append("\n### Rejected when `execution_mode` is `pipeline` or `pipeline_audit_only`\n")
        out += _removed_table(removed["REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE"],
                              "reconciliation_flows[].{source_config | target_configs[]}.", kind="only")
    if removed.get("RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE"):
        out.append("\n### Rejected when `execution_mode` is `job` (the default)\n")
        out += _removed_table(removed["RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE"],
                              "reconciliation_flows[].", kind="only")

    if removed.get("UNKNOWN_KEY_ALIASES"):
        out.append("\n## Names the framework never had\n")
        out.append(
            "Since v1.7.2 an unrecognised attribute is a hard error. These are the wrong names seen "
            "most often — usually a generated spec reconstructing field names from memory — and the "
            "attribute the validator points you to instead.\n"
        )
        out.append("\n| You wrote | Use instead |")
        out.append("|---|---|")
        for wrong, right in removed["UNKNOWN_KEY_ALIASES"].items():
            out.append(f"| `{wrong}` | {md_escape(right)} |")

    out.append("\n## Related\n")
    out.append("- [Onboarding restrictions & validation rules](../../14_onboarding_restrictions_and_validation_rules.md) — every rule, grouped by flow.")
    out.append("- [v1.4.0 attribute delta](../../v1.4.0_attribute_delta.md) — the release most of these removals shipped in.")
    out.append("- [Docs ↔ code synchronisation](../sync.md) — how this page is kept identical to the validator.")
    return "\n".join(out) + "\n"


# ──────────────────────────────────────────────────────────────────────────────
# Master reference index page
# ──────────────────────────────────────────────────────────────────────────────

def build_index_page(
    by_flow: Dict[str, List[Tuple[str, Dict[str, Any]]]],
    flows: List[str],
    kb: Dict[str, Any],
    faqs: Dict[str, List[Dict[str, str]]],
    since: Dict[str, str],
    removed: Dict[str, Dict[str, str]],
) -> str:
    attrs = kb.get("attributes", {})
    total = len(attrs)
    n_faq = sum(len(v) for v in faqs.values())
    n_with_faq = sum(1 for p in attrs if faqs.get(p))
    n_removed = sum(len(removed.get(k, {})) for k in
                    ("REMOVED_SOURCE_CONFIG_KEYS", "REMOVED_TARGET_CONFIG_KEYS", "REMOVED_RECONCILIATION_FLOW_KEYS"))
    n_aliases = len(removed.get("UNKNOWN_KEY_ALIASES", {}))

    out = [
        BANNER,
        "# Master configuration reference\n",
        "Every attribute the framework understands, on one searchable surface. Generated from the "
        "same registry the Spec Builder renders, the JSON schema the onboarding job enforces, and "
        "the validator's own rejection dictionaries — so this reference, the app and the gate can "
        "never disagree.\n",
        "\n<div class=\"grid cards\" markdown>\n",
        f"- :material-file-tree: **[Spec tree view](tree.md)**\n\n    The whole document as a "
        f"collapsible tree — every parent-child hierarchy, with a link from each leaf into its entry.\n",
        f"- :material-book-open-variant: **[Attributes by section](#sections)**\n\n    {total} attributes "
        f"across {len(flows)} sections, each with type, default, sample, validation and {n_faq} FAQs.\n",
        f"- :material-delete-alert: **[Removed & rejected](removed.md)**\n\n    {n_removed} removed "
        f"attributes, the removed enum values, mode-gated keys, and {n_aliases} wrong names the "
        f"validator recognises.\n",
        "- :material-sync: **[Docs ↔ code synchronisation](../sync.md)**\n\n    How registry, schema, "
        "validator and control tables map onto each other, and the hooks that keep this page honest.\n",
        "\n</div>\n",
        "\n## How to read an attribute entry\n",
        "| Element | What it tells you |",
        "|---|---|",
        f"| {badge('req', 'Required')} {badge('opt', 'Optional')} | Whether the onboarding gate rejects the spec when the key is absent. |",
        f"| {badge('flow', 'Ingestion')} | Which flow kinds accept the attribute. A `target_config` key may appear on two. |",
        f"| {badge('ver', 'v1.7.4+')} | The framework release that added the attribute (from `docs/v*_json_attribute_delta.json`). No badge means it predates the delta record. |",
        f"| {badge('only', 'Load strategy')} | The Spec Builder step that renders the field. |",
        "| **Type & constraints** | Type, default, allowed values and constraints, read from the JSON schema. |",
        "| **Persisted in** | The control-table column the value lands in after onboarding — the row the pipeline actually reads. |",
        "| **JSON · Validate offline · Onboard (CLI) · Verify (SQL)** | Write it, prove it without a cluster, onboard it, and confirm what landed. |",
        "| **FAQs** | At least four per attribute: what happens when omitted, format gotchas, performance impact, edge cases. |",
        f"\n{n_with_faq} of {total} attributes carry FAQs today. "
        "`tests/unit/test_attribute_faqs.py` fails when a registry attribute has fewer than four.\n",
        "\n## Sections\n",
        "| Section | Attributes | FAQs | What it covers |",
        "|---|---|---|---|",
    ]
    for flow in flows:
        slug = page_slug(flow)
        n_f = sum(len(faqs.get(p, [])) for p, _ in by_flow[flow])
        out.append(
            f"| [{FLOW_TITLES.get(flow, flow)}]({slug}.md) | {len(by_flow[flow])} | {n_f} "
            f"| {md_escape(FLOW_INTROS.get(flow, ''))} |"
        )
    out.append(f"\n**{total} distinct attributes** across {len(flows)} sections.\n")

    out.append("\n## Verification workflow\n")
    out.append("Every entry carries the same four tabs. Read them in this order.\n")
    out.append("```mermaid\nflowchart LR\n"
               "  A[\"Author JSON / YAML\"] --> B[\"validate_json (offline, no cluster)\"]\n"
               "  B -->|errors| A\n"
               "  B -->|clean| C[\"onboarding_job action_type=VALIDATE_ONLY\"]\n"
               "  C --> D[\"onboarding_job action_type=CREATE or UPDATE\"]\n"
               "  D --> E[\"control tables (config.*_spec)\"]\n"
               "  E --> F[\"pipeline update builds the graph\"]\n"
               "  F --> G[\"Verify (SQL) against the control row and the target table\"]\n"
               "```\n")

    out.append("\n## CDC load strategies\n")
    cdc = kb.get("cdc", [])
    if cdc:
        out.append("| Strategy | Applies to | What it does | Requires |")
        out.append("|---|---|---|---|")
        for c in cdc:
            out.append(
                f"| `{c.get('strategy','')}` | {md_escape(c.get('badge',''))} "
                f"| {md_escape(c.get('desc',''))} | {md_escape(c.get('requires',''))} |"
            )
        removed_cdc = removed.get("REMOVED_CDC_LOAD_STRATEGIES", {})
        for name in removed_cdc:
            out.append(f"| `{name}` | {badge('dep', 'Removed')} | See [Removed & rejected](removed.md#target_configcdc_load_strategy) | — |")

    if since:
        out.append("\n## Recently added attributes\n")
        out.append("| Attribute | Since |")
        out.append("|---|---|")
        for path, ver in sorted(since.items(), key=lambda kv: (tuple(int(x) for x in re.findall(r"\d+", kv[1])), kv[0]), reverse=True):
            flows_of = attrs.get(path, {}).get("_meta", {}).get("flows", ["other"])
            page = page_slug(flows_of[0]) if flows_of else "other"
            out.append(f"| [`{path}`]({page}.md#{anchor(path)}) | {badge('ver', 'v' + ver + '+')} |")

    out.append("\n## Related\n")
    out.append("- [Full attribute dictionary](../../00_master_reference_index.md) — the hand-maintained narrative dictionary, including framework-generated columns.")
    out.append("- [Onboarding restrictions & validation rules](../../14_onboarding_restrictions_and_validation_rules.md)")
    out.append("- [Known limitations & gotchas](../../13_known_limitations_and_gotchas.md) — what the validator cannot catch.")
    out.append("- Pillars: [Ingestion](../../pillars/ingestion.md) · [Transformation](../../pillars/transformation.md) · "
               "[Reconciliation](../../pillars/reconciliation.md) · [Observability](../../pillars/observability.md)")
    return "\n".join(out) + "\n"


# ──────────────────────────────────────────────────────────────────────────────
# JSON attribute reference — orchestration
# ──────────────────────────────────────────────────────────────────────────────

def build_json_reference() -> List[Path]:
    kb = load_knowledge()
    refs = kb.get("reference_index", {})
    attrs = kb.get("attributes", {})
    faqs = load_faqs()
    schema = SchemaIndex(load_schema())
    removed = load_removed()
    since, changed = load_versions(set(attrs))
    master_slugs = heading_slugs(DOCS / "00_master_reference_index.md")

    by_flow: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for path, entry in sorted(attrs.items()):
        for flow in entry.get("_meta", {}).get("flows", ["other"]):
            by_flow.setdefault(flow, []).append((path, entry))

    JSON_REF.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []

    flows = [f for f in FLOW_ORDER if f in by_flow] + [f for f in by_flow if f not in FLOW_ORDER]

    tree_md, tree_anchors = build_tree_page(schema, attrs)
    written.append(write_if_changed(JSON_REF / "tree.md", tree_md))
    written.append(write_if_changed(JSON_REF / "removed.md", build_removed_page(removed)))
    written.append(write_if_changed(JSON_REF / "index.md",
                                    build_index_page(by_flow, flows, kb, faqs, since, removed)))

    for flow in flows:
        page = JSON_REF / f"{page_slug(flow)}.md"
        written.append(write_if_changed(
            page,
            render_flow_page(flow, by_flow[flow], refs, schema, faqs, since, changed, master_slugs, tree_anchors),
        ))
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

    packages: Dict[str, List[Tuple[Path, Dict[str, Any]]]] = {}
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
             "reference builds without Spark or a Databricks connection. The first sentence of "
             "each docstring is the summary — write that sentence for a reader who will see "
             "nothing else.\n",
             "\n## Packages\n",
             "| Package | Modules | What it does |", "|---|---|---|"]
    for pkg in sorted(packages):
        index.append(f"| [`{pkg}`]({pkg}.md) | {len(packages[pkg])} | {PACKAGE_BLURBS.get(pkg, '')} |")
    written.append(write_if_changed(CODE_REF / "index.md", "\n".join(index) + "\n"))

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
        written.append(write_if_changed(CODE_REF / f"{pkg}.md", "\n".join(out) + "\n"))

    return written


def stage_usecase_docs() -> List[Path]:
    """Copy the BT_Usecase master documents into docs/ so mkdocs can build them.

    The use-case documents live under ``BT_Usecase/<UC>/docs/`` -- one folder per use
    case, next to that use case's onboarding specs and source data, so the setup script
    copies one tree per UC into the matching Volume. mkdocs, though, can only include
    files under ``docs_dir``. Rather than keep a second hand-maintained copy (which is
    exactly the drift this script exists to prevent), the pages are staged here at build
    time. BT_Usecase stays the single source of truth; docs/UC*/ is derived output.
    """
    written: List[Path] = []
    src_root = REPO / "BT_Usecase"
    if not src_root.is_dir():
        return written
    for uc_dir in sorted(src_root.iterdir()):
        docs_dir = uc_dir / "docs"
        if not uc_dir.is_dir() or not docs_dir.is_dir():
            continue
        dest_dir = DOCS / uc_dir.name
        dest_dir.mkdir(parents=True, exist_ok=True)
        for src in sorted(docs_dir.glob("*.md")):
            dest = dest_dir / src.name
            text = src.read_text(encoding="utf-8")
            if not dest.exists() or dest.read_text(encoding="utf-8") != text:
                dest.write_text(text, encoding="utf-8")
            written.append(dest)
        # Assets the master documents link to as siblings (DDL sheets, query packs).
        # They live in the use case's data/ or docs/ folder; mkdocs needs them beside
        # the page that links to them or the link 404s in the built site.
        for pattern in ("*.csv", "*.sql"):
            for src in sorted(docs_dir.glob(pattern)) + sorted((uc_dir / "data").glob(pattern)):
                dest = dest_dir / src.name
                data = src.read_bytes()
                if not dest.exists() or dest.read_bytes() != data:
                    dest.write_bytes(data)
                written.append(dest)
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    if args.check:
        before = {p: p.read_text(encoding="utf-8") for p in
                  list(JSON_REF.glob("*.md")) + list(CODE_REF.glob("*.md")) if p.exists()}

    written = build_json_reference() + build_code_reference() + stage_usecase_docs()

    if args.check:
        # Only the derived reference pages can be stale. The UC*/ pages are byte copies of
        # BT_Usecase/<UC>/docs, gitignored and re-staged on every run, so they are never "stale"
        # relative to a commit — comparing them would fail every fresh checkout.
        generated = [p for p in written if p.suffix == ".md" and (JSON_REF in p.parents or CODE_REF in p.parents)]
        stale = [p for p in generated if before.get(p) != p.read_text(encoding="utf-8")]
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
