"""
Template catalogue — discovered from the filesystem, not from a checked-in list.

Templates live under `databricks-app/templates/`. Anything dropped in that tree is
picked up on the next request: there is no build step and no registration list to
keep in sync. `index.json`, when present, is treated purely as *optional metadata
overlay* — it can give a file a nicer label, a longer description, tags, a doc
anchor or `pinned: true`, but a file needs no entry there to appear.

Scope (which flow kind a template belongs to) comes from the containing directory:

    templates/
      pipeline_onboarding_template.json     -> scope "spec"   (whole document)
      ingestion/autoloader_csv_append.json  -> scope "ingestion"
      transformation/scd2_crypto.json       -> scope "transformation"
      reconciliation/full.json              -> scope "reconciliation"

Any other directory name is reported as-is, so a team can add its own grouping
without a code change.

Everything a template says about itself — the source type, load strategy, and which
optional blocks it configures — is derived from the file's own content, so the
catalogue entry cannot drift away from the template it describes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# Directory names that map onto a flow kind. Anything else keeps its own name.
KNOWN_SCOPES = {"ingestion", "transformation", "reconciliation", "observability"}

# Files that are catalogue machinery rather than templates themselves.
RESERVED_NAMES = {"index.json"}

TEMPLATE_SUFFIXES = (".json",)


def _pretty(stem: str) -> str:
    """`autoloader_csv_append` -> `autoloader · csv · append`."""
    return " · ".join(part for part in stem.replace("-", "_").split("_") if part)


def _summarise(body: Dict[str, Any]) -> Dict[str, Any]:
    """Describe a template from its own content.

    Returns the handful of facts the UI shows as chips: what it reads, how it loads,
    and which optional blocks it switches on. Derived rather than declared, so it
    stays true when someone edits the template.
    """
    if not isinstance(body, dict):
        return {"attributes": 0, "config": {}, "features": []}

    # A whole-spec template wraps its flows; a flow template is the flow itself.
    flow: Dict[str, Any] = body
    if "ingestion_flows" in body or "transformation_flows" in body or "reconciliation_flows" in body:
        flows = (body.get("ingestion_flows") or []) + (body.get("transformation_flows") or [])
        flow = flows[0] if flows else {}

    src = flow.get("source_config") or {}
    tgt = flow.get("target_config") or {}
    zip_cfg = src.get("source_zip_handling") or {}

    config = {
        "source_type": flow.get("source_type"),
        "target_type": flow.get("target_type"),
        "strategy": tgt.get("cdc_load_strategy"),
        "format": src.get("format"),
        "sink": (tgt.get("sink_config") or {}).get("format"),
    }
    config = {k: v for k, v in config.items() if v}

    features: List[str] = []
    if zip_cfg.get("enabled"):
        features.append("ZIP extraction")
    if (zip_cfg.get("pre_extraction_decryption") or {}).get("type"):
        features.append("PGP decrypt")
    if tgt.get("encrypted_columns"):
        features.append("column encryption")
    rules = ((flow.get("dq_config") or {}).get("rules")) or []
    if rules:
        features.append(f"{len(rules)} DQ rule(s)")
    if (flow.get("dq_config") or {}).get("quarantine_table"):
        features.append("quarantine")
    gov = flow.get("governance_tags") or {}
    if gov.get("column_tags") or gov.get("table_tags"):
        features.append("governance tags")
    if tgt.get("generate_hash_columns"):
        features.append("hash columns")
    if src.get("explode_columns") is not None:
        features.append("explode columns")
    if (tgt.get("auto_ttl") or {}).get("expire_in_days"):
        features.append("auto TTL")
    if src.get("landing_retention_policy"):
        features.append("landing retention")
    if tgt.get("partition_columns"):
        features.append("partitioned")
    if tgt.get("liquid_clustering_columns"):
        features.append("liquid clustering")
    if flow.get("transformation_sql"):
        features.append("SQL transform")
    inputs = flow.get("source_inputs") or []
    if len(inputs) > 1:
        features.append(f"{len(inputs)}-way join")

    def count(node: Any) -> int:
        if isinstance(node, dict):
            return sum(count(v) for v in node.values()) or len(node)
        if isinstance(node, list):
            return sum(count(v) for v in node)
        return 1

    return {"attributes": count(flow) if flow else 0, "config": config, "features": features}


def _load_overlay(root: Path) -> Dict[str, Dict[str, Any]]:
    """Read index.json, keyed by the file path it annotates. Missing or broken is fine."""
    overlay: Dict[str, Dict[str, Any]] = {}
    index_file = root / "index.json"
    if not index_file.exists():
        return overlay
    try:
        data = json.loads(index_file.read_text(encoding="utf-8"))
    except Exception:
        return overlay
    for entry in data.get("templates", []) or []:
        rel = str(entry.get("file", "")).replace("\\", "/").strip("/")
        if rel:
            overlay[rel] = entry
    return overlay


def scan_templates(templates_root: Path) -> List[Dict[str, Any]]:
    """Discover every template under `templates_root`.

    Returns catalogue entries sorted pinned-first, then by scope, then by label.
    Unreadable files are reported with an `error` rather than omitted silently —
    a template that fails to parse is exactly what the author needs told about.
    """
    if not templates_root.exists():
        return []

    overlay = _load_overlay(templates_root)
    out: List[Dict[str, Any]] = []

    for path in sorted(templates_root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in TEMPLATE_SUFFIXES:
            continue
        if path.name in RESERVED_NAMES:
            continue

        rel = path.relative_to(templates_root).as_posix()
        parent = path.parent.name
        scope = parent if (parent in KNOWN_SCOPES and path.parent != templates_root) else "spec"

        meta = overlay.get(rel, {})
        entry: Dict[str, Any] = {
            "id": meta.get("id") or rel[: -len(path.suffix)].replace("/", "__"),
            "file": rel,
            "scope": meta.get("scope") or scope,
            "label": meta.get("label") or _pretty(path.stem),
            "description": meta.get("description") or "",
            "tags": meta.get("tags") or [],
            "doc": meta.get("doc") or "",
            "pinned": bool(meta.get("pinned")),
            "source": "index.json" if meta else "discovered",
            "bytes": path.stat().st_size,
        }

        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except Exception as ex:
            entry["error"] = f"Could not parse: {ex}"
            entry["summary"] = {"attributes": 0, "config": {}, "features": []}
            out.append(entry)
            continue

        entry["summary"] = _summarise(body)
        if not entry["description"]:
            cfg = entry["summary"]["config"]
            bits = [f"{k}: {v}" for k, v in cfg.items()]
            entry["description"] = (
                "Discovered template — " + ", ".join(bits) if bits else "Discovered template."
            )
        out.append(entry)

    out.sort(key=lambda e: (not e["pinned"], e["scope"], e["label"].lower()))
    return out


def load_template_body(templates_root: Path, rel_file: str) -> Optional[Dict[str, Any]]:
    """Read one template's JSON body, refusing any path that escapes the root."""
    candidate = (templates_root / rel_file).resolve()
    try:
        candidate.relative_to(templates_root.resolve())
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return json.loads(candidate.read_text(encoding="utf-8"))
