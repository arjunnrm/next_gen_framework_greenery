"""
v1.6.0 contract: the registries carry NO display or serialization defaults.

Every input starts empty, every toggle starts OFF, and an attribute the user never
touched must be ABSENT from the exported spec — not silently filled in by a
`"default":` key on a field definition. These tests assert the removal is real
(defaults are gone from every field definition, server-side serialization injects
nothing into an empty flow) and that the one field v1.6.0 adds —
target_config.sink_config.staged_file_format — is registered in both the server
registry and the shipped frontend bundle.
"""

import re
from pathlib import Path

from server.core.registry import RegistryManager
from server.core.serializer import SpecSerializer

APP_ROOT = Path(__file__).resolve().parent.parent


def _walk_fields(fields):
    for f in fields:
        yield f
        if isinstance(f, dict) and isinstance(f.get("fields"), list):
            yield from _walk_fields(f["fields"])


def test_no_field_definition_carries_a_default():
    """No field definition anywhere — sections, repeat children, shared CDC,
    or the injected cdc_load_strategy descriptor — may carry a 'default' key.
    A default here re-injects a value the UI never showed, breaking UI-JSON
    two-way consistency."""
    reg = RegistryManager()
    offenders = []

    for kind, secs in reg.registries.items():
        for sec in secs:
            for f in _walk_fields(sec.get("fields", [])):
                if isinstance(f, dict) and "default" in f:
                    offenders.append(f"{kind}:{f.get('path', '?')}")

    for f in _walk_fields(reg.shared_cdc.get("fields", [])):
        if isinstance(f, dict) and "default" in f:
            offenders.append(f"shared.cdc:{f.get('path', '?')}")

    assert "default" not in reg.shared_cdc, "shared.cdc.json carries a top-level default again"

    for kind, fmap in reg.field_maps.items():
        for path, f in fmap.items():
            if "default" in f:
                offenders.append(f"field_map[{kind}]:{path}")

    assert not offenders, (
        "field definitions carry 'default' keys again — v1.6.0 removed them all: "
        + ", ".join(sorted(set(offenders)))
    )


def test_empty_flow_serializes_to_nothing():
    """An untouched flow document must serialize to an empty object. In particular
    target_config.cdc_load_strategy must NOT come back as APPEND — that injection
    exported a strategy the UI never showed as selected."""
    ser = SpecSerializer(RegistryManager())
    empty = {"v": {}, "kvs": {}, "reps": {}}
    root = {"v": {}, "kvs": {}, "reps": {}}
    for kind in ("ingestion", "transformation", "reconciliation"):
        out = ser.serialize_flow(empty, kind, root)
        assert out == {}, f"empty {kind} flow serialized non-empty: {out}"


def test_staged_file_format_registered_for_both_flow_kinds():
    """v1.6.0 adds target_config.sink_config.staged_file_format: string select,
    '', json or csv, no default (absent = json behavior server-side), visible only
    for a pgp_zip sink."""
    reg = RegistryManager()
    for kind in ("ingestion", "transformation"):
        f = reg.get_field_definition(kind, "target_config.sink_config.staged_file_format")
        assert f is not None, f"staged_file_format missing from {kind} registry"
        assert f["widget"] == "select"
        assert f["type"] == "string"
        assert f["options"] == ["", "json", "csv"]
        assert "default" not in f
        vis = f.get("visible_when")
        assert vis is not None, "staged_file_format must be gated on sink_config.format == pgp_zip"
        assert '"pgp_zip"' in __import__("json").dumps(vis)


def test_staged_file_format_in_shipped_bundle(frontend_text):
    """The committed web/dist bundle must offer the new field — an un-rebuilt
    bundle keeps serving a form without it."""
    assert "staged_file_format" in frontend_text, (
        "staged_file_format is not in the shipped frontend bundle -- run `npm run build` "
        "in databricks-app/web after changing registry.js"
    )


def test_web_registry_source_has_no_display_defaults():
    """web/src/registry.js must define no `d:` display defaults. A `d:` default is
    shown by the form but skipped by the touched-keys-only export, so the UI and
    the exported JSON disagree — the exact bug v1.6.0 removed."""
    src = (APP_ROOT / "web" / "src" / "registry.js").read_text(encoding="utf-8")
    hits = re.findall(r"[({,]d:", src)
    assert not hits, f"registry.js defines d: display defaults again ({len(hits)} found)"
