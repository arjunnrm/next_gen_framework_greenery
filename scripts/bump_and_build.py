"""Build wrapper invoked by databricks.yml's `artifacts.framework_wheel.build`.

Every `databricks bundle deploy` run must produce a brand-new, uniquely-named wheel
rather than overwriting the one from the previous deploy -- otherwise a currently
running (or restarting) pipeline/job that already resolved the old wheel path risks
reading a corrupted or unexpectedly different file mid-run. See
docs/05_deployment_guide.md for the full rationale.

Steps, in order:
1. Archive (never delete) any wheel(s) already in dist/ so the subsequent `uv build`
   is the only *.whl dist/ contains -- resources/**/*.yml's `../../dist/*.whl` glob must
   only ever match one file.
2. Leave pyproject.toml's version ALONE. The version is a real 3-part semantic version
   (``X.Y.Z``) owned by a human and bumped deliberately -- it is NOT stamped to
   epoch-milliseconds any more.

   Pre-0.0.2 this script rewrote the patch component to ``int(time.time() * 1000)``, so
   every deploy produced a filename nothing else could predict (e.g.
   ``nextgen_metadata_framework-0.0.1788350054326-py3-none-any.whl``). That bought
   filename uniqueness, but at the cost of the version meaning anything: two builds of
   identical source got different versions, and no deployed artifact could be traced back
   to a release. Uniqueness is now provided by the VERSION-SCOPED artifact_path instead
   (``/Volumes/<catalog>/config/wheels/<version>`` -- see databricks.yml), which is the
   property that actually matters: `bundle deploy` prunes superseded artifacts from
   ``<artifact_path>/.internal/``, so two releases sharing one directory means deploying
   0.0.3 deletes the wheel a running 0.0.2 pipeline is still resolving. Separate
   directories make that impossible; a unique filename never prevented it.

   To release, bump ``version`` in pyproject.toml by hand, then deploy.
3. Run `uv build --wheel`.
"""

import re
import shutil
import subprocess
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


def read_version() -> str:
    """Return the declared 3-part version, validating its shape. Never writes to pyproject.toml.

    The shape check is load-bearing, not cosmetic: ``databricks.yml``'s ``artifact_path`` is
    ``/Volumes/${var.catalog}/config/wheels/${var.framework_version}``, and that variable is set
    from this same value. A malformed or accidentally epoch-stamped version would silently
    publish into a directory nobody expects, and the next release would not prune it.
    """
    text = PYPROJECT.read_text(encoding="utf-8")
    match = VERSION_LINE_RE.search(text)
    if match is None:
        raise RuntimeError(f'Could not find a `version = "X.Y.Z"` line in {PYPROJECT}')
    _prefix, major, minor, patch, _suffix = match.groups()
    version = f"{major}.{minor}.{patch}"
    # An epoch-millis patch is what this script used to write; refuse to build one now so a
    # stale local checkout cannot resurrect the old scheme unnoticed.
    if len(patch) > 6:
        raise RuntimeError(
            f"pyproject.toml version {version!r} looks epoch-stamped (patch component "
            f"{patch!r} is {len(patch)} digits). Versions are now hand-managed 3-part semantic "
            f"versions -- set a real X.Y.Z and re-run."
        )
    return version


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
    version = read_version()
    print(f"[bump_and_build] building version {version} (hand-managed; not epoch-stamped)")
    build_wheel()


if __name__ == "__main__":
    main()
