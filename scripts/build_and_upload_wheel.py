"""Build the framework wheel and publish it to a Unity Catalog Volume.

**Why a Volume instead of the bundle's own artifact upload.**
``databricks.yml`` used to declare an ``artifacts.python_artifact`` block whose build step
stamped a fresh UTC-epoch-millis version on every deploy, and every resource referenced the
result as ``../dist/*.whl``. DABs uploads that wheel to
``<bundle root>/artifacts/.internal/`` and **prunes what is no longer current**. That is fine
for a quiet environment and actively harmful for a busy one: a ``bundle deploy`` issued while a
Lakeflow pipeline update is already running replaces the exact artifact path that update was
told to ``pip install``, and the update dies with

    [ENVIRONMENT_PIP_INSTALL_ERROR] Failed to install environment dependency:
    .../artifacts/.internal/nextgen_metadata_framework-<older version>-py3-none-any.whl

Confirmed live on 2026-08-29: ``TC-ING-004`` and ``TC-ING-005`` both died this way on
``dev_metaflow`` while an unrelated redeploy was in flight. Nothing was wrong with either test.

A UC Volume has none of that behaviour. It is ordinary managed storage: files accumulate, and
nothing removes an older wheel just because a newer one appeared. Every historical version
stays installable, so an in-flight update keeps working no matter how many times the bundle is
redeployed underneath it, and rolling a pipeline back to a prior wheel is a config edit rather
than a rebuild.

**Why this is a pre-deploy step, not an ``artifacts`` build step.** The wheel path has to be
fed to ``bundle deploy`` as a variable (``--var=framework_wheel_path=...``), and a build step
that runs *during* the deploy cannot influence that same deploy's variable resolution. Running
it first, printing the path, and passing it in is the only ordering that works.

Usage::

    python scripts/build_and_upload_wheel.py --profile dev --catalog metaflow
    # prints the resolved /Volumes/... path on the last stdout line

    # then, using that path:
    databricks bundle deploy --target dev -p dev \\
      --var="framework_wheel_path=/Volumes/metaflow/framework/wheels/nextgen_...whl"

``--skip-build`` republishes the wheel already sitting in ``dist/`` without stamping a new
version -- the right choice when redeploying unchanged code, since it keeps every running
pipeline pointed at a wheel that is still present.
"""

import argparse
import glob
import os
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST_DIR = os.path.join(REPO_ROOT, "dist")
PYPROJECT = os.path.join(REPO_ROOT, "pyproject.toml")

DEFAULT_VOLUME_SCHEMA = "framework"
DEFAULT_VOLUME_NAME = "wheels"


def _run(cmd, **kwargs):
    print(f"$ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True, **kwargs)
    if result.returncode != 0:
        sys.stderr.write(result.stdout + "\n" + result.stderr + "\n")
        raise SystemExit(f"command failed ({result.returncode}): {' '.join(cmd)}")
    return result.stdout


def stamp_version() -> str:
    """Stamp pyproject.toml's version to a fresh UTC-epoch-millis patch and return it.

    Same scheme ``scripts/bump_and_build.py`` uses, kept identical so wheels built by either
    path sort and compare the same way.
    """
    version = f"0.0.{int(time.time() * 1000)}"
    with open(PYPROJECT, "r", encoding="utf-8") as handle:
        lines = handle.readlines()
    for index, line in enumerate(lines):
        if line.startswith("version = "):
            lines[index] = f'version = "{version}"\n'
            break
    else:
        raise SystemExit("pyproject.toml has no 'version = ' line to stamp")
    with open(PYPROJECT, "w", encoding="utf-8") as handle:
        handle.writelines(lines)
    return version


def newest_wheel() -> str:
    wheels = sorted(glob.glob(os.path.join(DIST_DIR, "*.whl")), key=os.path.getmtime)
    if not wheels:
        raise SystemExit(f"no wheel found in {DIST_DIR} -- run without --skip-build first")
    return wheels[-1]


def ensure_volume(profile: str, catalog: str, schema: str, volume: str) -> None:
    """Create the wheel Volume if it does not exist. Idempotent -- a pre-existing Volume is
    left exactly as it is (never recreated, so previously published wheels survive)."""
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    check = subprocess.run(
        ["databricks", "volumes", "read", f"{catalog}.{schema}.{volume}", "-p", profile, "--output", "json"],
        cwd=REPO_ROOT, text=True, capture_output=True, env=env,
    )
    if check.returncode == 0:
        print(f"volume {catalog}.{schema}.{volume} already exists")
        return
    subprocess.run(
        ["databricks", "schemas", "create", schema, catalog, "-p", profile],
        cwd=REPO_ROOT, text=True, capture_output=True, env=env,
    )
    created = subprocess.run(
        ["databricks", "volumes", "create", catalog, schema, volume, "MANAGED", "-p", profile],
        cwd=REPO_ROOT, text=True, capture_output=True, env=env,
    )
    if created.returncode != 0:
        sys.stderr.write(created.stdout + "\n" + created.stderr + "\n")
        raise SystemExit(f"could not create volume {catalog}.{schema}.{volume}")
    print(f"created volume {catalog}.{schema}.{volume}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("--catalog", required=True, help="UC catalog holding the wheel Volume")
    parser.add_argument("--schema", default=DEFAULT_VOLUME_SCHEMA)
    parser.add_argument("--volume", default=DEFAULT_VOLUME_NAME)
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Republish the existing dist/ wheel without stamping a new version -- use when "
             "redeploying unchanged code so running pipelines keep a wheel that still exists.",
    )
    args = parser.parse_args()

    if args.skip_build:
        wheel_path = newest_wheel()
        print(f"--skip-build: reusing {os.path.basename(wheel_path)}")
    else:
        version = stamp_version()
        print(f"stamped version {version}")
        _run(["uv", "build", "--wheel"])
        wheel_path = newest_wheel()

    wheel_name = os.path.basename(wheel_path)
    ensure_volume(args.profile, args.catalog, args.schema, args.volume)

    volume_dir = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    target = f"{volume_dir}/{wheel_name}"

    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    upload = subprocess.run(
        ["databricks", "fs", "cp", wheel_path, f"dbfs:{target}", "--overwrite", "-p", args.profile],
        cwd=REPO_ROOT, text=True, capture_output=True, env=env,
    )
    if upload.returncode != 0:
        sys.stderr.write(upload.stdout + "\n" + upload.stderr + "\n")
        raise SystemExit("wheel upload failed")

    print(f"published {wheel_name} -> {target}")
    # Last line is the resolved path, so a caller can capture it with `| tail -1`.
    print(target)


if __name__ == "__main__":
    main()
