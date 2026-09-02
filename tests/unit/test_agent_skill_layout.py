"""The discoverable agent skill must stay in sync with its sources (v1.7.1).

``.claude/skills/metaflow-onboarding/`` is what an agent auto-loads. Its reference files are
copies of the canonical ones under ``agent_skills/`` and ``onboarding_templates/``; a copy that
drifts teaches attribute names the framework rejects, which is exactly the failure this skill
exists to prevent. These tests make drift a test failure instead of a bad generated spec.

Refresh the copies with ``python scripts/sync_agent_skill.py``.
"""

import json
import re
from pathlib import Path

import pytest

SKILL_DIR = Path(".claude/skills/metaflow-onboarding")
SKILL_MD = SKILL_DIR / "SKILL.md"
REFS = SKILL_DIR / "references"

# skill copy -> canonical source
MIRRORED = {
    REFS / "golden_specs.json": Path("agent_skills/reference/golden_specs.json"),
    REFS / "spec_reference.json": Path("agent_skills/reference/onboarding_spec_full_reference.json"),
    REFS / "common_pitfalls.md": Path("agent_skills/reference/common_pitfalls.md"),
    REFS / "framework_guide.md": Path("agent_skills/SKILL.md"),
}


def test_skill_manifest_exists():
    assert SKILL_MD.is_file()


def test_skill_has_yaml_frontmatter_with_name_and_description():
    """Without frontmatter the skill is never discovered, which is why the old one never loaded."""
    text = SKILL_MD.read_text(encoding="utf-8")
    assert text.startswith("---\n"), "SKILL.md must open with YAML frontmatter"
    frontmatter = text.split("---", 2)[1]
    assert re.search(r"^name:\s*metaflow-onboarding\s*$", frontmatter, re.M), frontmatter
    description = re.search(r"^description:\s*(.+)$", frontmatter, re.M)
    assert description and len(description.group(1).strip()) > 40, "description must be substantive"


@pytest.mark.parametrize("copy_path,source_path", sorted(MIRRORED.items()))
def test_reference_copy_matches_its_source(copy_path, source_path):
    assert copy_path.is_file(), f"missing skill reference {copy_path}"
    assert source_path.is_file(), f"missing canonical source {source_path}"
    assert copy_path.read_bytes() == source_path.read_bytes(), (
        f"{copy_path} has drifted from {source_path}; run scripts/sync_agent_skill.py"
    )


def test_skill_links_only_to_files_that_exist():
    text = SKILL_MD.read_text(encoding="utf-8")
    for target in re.findall(r"\]\((references/[^)]+)\)", text):
        assert (SKILL_DIR / target).is_file(), f"SKILL.md links to missing {target}"


def test_skill_documents_the_validate_loop():
    """The mandatory generate -> validate -> fix loop is the point of the skill."""
    text = SKILL_MD.read_text(encoding="utf-8")
    assert "validate_json" in text
    assert "agent_tools" in text


def test_wrong_name_table_agrees_with_the_validator_alias_map():
    """Every alias the validator knows should be discoverable from the skill text."""
    from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import (
        UNKNOWN_KEY_ALIASES,
    )

    text = SKILL_MD.read_text(encoding="utf-8")
    missing = [key for key in UNKNOWN_KEY_ALIASES if key not in text]
    assert not missing, f"SKILL.md does not mention these wrong-name aliases: {missing}"


def test_golden_specs_copy_is_valid_json_and_covers_each_documented_example():
    specs = json.loads((REFS / "golden_specs.json").read_text(encoding="utf-8"))
    text = SKILL_MD.read_text(encoding="utf-8")
    for name in specs:
        assert name in text, f"golden spec {name} is not described in SKILL.md"
