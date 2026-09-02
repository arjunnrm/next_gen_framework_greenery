"""Refresh .claude/skills/metaflow-onboarding/references/ from the canonical sources.

The discoverable skill carries copies so an agent can load them without knowing the repo
layout. Copies drift; ``tests/unit/test_agent_skill_layout.py`` fails when they do, and this
script is the fix. Run it after changing anything under ``agent_skills/``.

    python scripts/sync_agent_skill.py
"""

import shutil
import sys
from pathlib import Path

REFS = Path(".claude/skills/metaflow-onboarding/references")

# skill copy -> canonical source (must match MIRRORED in test_agent_skill_layout.py)
MIRRORED = {
    REFS / "golden_specs.json": Path("agent_skills/reference/golden_specs.json"),
    REFS / "spec_reference.json": Path("agent_skills/reference/onboarding_spec_full_reference.json"),
    REFS / "common_pitfalls.md": Path("agent_skills/reference/common_pitfalls.md"),
    REFS / "framework_guide.md": Path("agent_skills/SKILL.md"),
}


def main() -> int:
    REFS.mkdir(parents=True, exist_ok=True)
    missing = [str(src) for src in MIRRORED.values() if not src.is_file()]
    if missing:
        print("ERROR: canonical source(s) missing:", ", ".join(missing), file=sys.stderr)
        return 1

    changed = 0
    for copy_path, source_path in MIRRORED.items():
        source_bytes = source_path.read_bytes()
        if copy_path.is_file() and copy_path.read_bytes() == source_bytes:
            print(f"  unchanged  {copy_path}")
            continue
        shutil.copyfile(source_path, copy_path)
        changed += 1
        print(f"  synced     {copy_path}  <- {source_path}")

    print(f"\n{changed} file(s) updated.")
    if changed:
        print("Golden specs are generated separately: python scripts/build_golden_specs.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
