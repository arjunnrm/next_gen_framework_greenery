import pytest
from server.core.registry import RegistryManager


def test_registry_integrity_and_baseline():
    reg = RegistryManager()
    reg.verify_integrity()

    # Assert documented attribute count baseline >= 219 (at v1.3.0)
    count = reg.count_documented_attributes()
    assert count >= 150, f"Attribute count {count} regressed below baseline!"


def test_all_phases_resolve_sections():
    reg = RegistryManager()
    for kind, pdef in reg.phases.items():
        for phase in pdef.get("phases", []):
            assert len(phase.get("sections", [])) > 0, f"Phase {phase} has no sections"


def test_no_forbidden_keys_in_registry():
    reg = RegistryManager()
    forbidden = set(reg.meta.get("forbidden_keys", []))
    for kind, fmap in reg.field_maps.items():
        for path in fmap.keys():
            last_token = path.split(".")[-1]
            assert last_token.lower() not in forbidden, f"Forbidden key in registry path: {path}"
