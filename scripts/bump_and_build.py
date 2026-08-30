"""Build wrapper invoked by databricks.yml's `artifacts.python_artifact.build`.

Every `databricks bundle deploy` run must produce a brand-new, uniquely-named wheel
rather than overwriting the one from the previous deploy -- otherwise a currently
running (or restarting) pipeline/job that already resolved the old wheel path risks
reading a corrupted or unexpectedly different file mid-run. See
docs/05_deployment_guide.md for the full rationale.

Steps, in order:
1. Archive (never delete) any wheel(s) already in dist/ so the subsequent `uv build`
   is the only *.whl dist/ contains -- resources/*.yml's `../dist/*.whl` glob must
   only ever match one file.
2. Stamp pyproject.toml's patch version to the current UTC epoch-milliseconds, so
   the new wheel's filename is guaranteed unique (hatch-vcs/setuptools_scm-style
   git-derived versioning isn't usable here -- this project has no .git history).
3. Run `uv build --wheel`.
"""

import re
import shutil
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
DIST = ROOT / "dist"
ARCHIVE = DIST / "archive"

VERSION_LINE_RE = re.compile(r'^(version\s*=\s*")(\d+)\.(\d+)\.(\d+)(")', re.MULTILINE)


def archive_old_wheels() -> None:
    if not DIST.exists():
        return
    ARCHIVE.mkdir(exist_ok=True)
    for wheel in DIST.glob("*.whl"):
        shutil.move(str(wheel), str(ARCHIVE / wheel.name))


def stamp_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    epoch_millis = int(time.time() * 1000)

    def replace(match: "re.Match[str]") -> str:
        prefix, major, minor, _patch, suffix = match.groups()
        return f"{prefix}{major}.{minor}.{epoch_millis}{suffix}"

    new_text, count = VERSION_LINE_RE.subn(replace, text, count=1)
    if count != 1:
        raise RuntimeError(f'Could not find a `version = "X.Y.Z"` line to stamp in {PYPROJECT}')
    PYPROJECT.write_text(new_text, encoding="utf-8")
    return VERSION_LINE_RE.search(new_text).group(0)


def build_wheel() -> None:
    uv_bin = shutil.which("uv")
    if uv_bin:
        subprocess.run([uv_bin, "build", "--wheel"], cwd=ROOT, check=True)
    else:
        # Fallback to python pip wheel
        import sys
        python_bin = sys.executable or str(ROOT / ".venv" / "Scripts" / "python.exe")
        subprocess.run([python_bin, "-m", "pip", "wheel", "--no-deps", "-w", str(DIST), "."], cwd=ROOT, check=True)


def main() -> None:
    archive_old_wheels()
    new_version_line = stamp_version()
    print(f"[bump_and_build] {new_version_line}")
    build_wheel()


if __name__ == "__main__":
    main()
