"""Copy the wheel a `bundle deploy` just published into a prune-proof archive folder.

**The problem this solves.** `databricks bundle deploy` uploads the framework wheel to
`<artifact_path>/.internal/` and PRUNES that folder down to the current deploy's wheel.
This is true on a UC Volume exactly as in the workspace -- verified 2026-08-30, and
re-verified 2026-09-01 on the `arjun` workspace, where `.internal/` held exactly one wheel
after three consecutive deploys.

Each deployed job/pipeline is pinned to a fully-resolved ABSOLUTE wheel filename, e.g.

    /Volumes/metaflow/config/wheels/.internal/nextgen_metadata_framework-0.0.<millis>-py3-none-any.whl

so the hazard is NOT overwrite -- `scripts/bump_and_build.py` stamps a unique version per
deploy, and no wheel is ever replaced in place. The hazard is DELETION: the next deploy
removes the exact file an earlier-deployed resource still points at. A pipeline that installs
that dependency afterwards fails with ENVIRONMENT_PIP_INSTALL_ERROR.

**What this script does, and what it deliberately does NOT do.** It copies each wheel out of
`.internal/` into a sibling `archive/` folder in the same Volume. DABs does not manage the
Volume root, so nothing there is ever pruned (databricks.yml's `artifacts` comment block).
That gives you a retained history for ROLLBACK and FORENSICS.

It does NOT make a running pipeline safe. Deployed resources still point into `.internal/`,
so an archived copy does not repair a pin whose target was pruned mid-install -- recovering
from that means repointing the resource at the archived path, by hand. The only PREVENTION
remains the operational rule, now mechanically enforceable with:

    databricks bundle deploy -t <target> --fail-on-active-runs

which refuses to deploy while any job or pipeline in the bundle is running. Use both: the
flag prevents the incident, this script gives you something to roll back to if one happens.

Usage::

    python scripts/archive_deployed_wheel.py --profile arjun --catalog metaflow

    # keep only the 20 most recent archived wheels
    python scripts/archive_deployed_wheel.py --profile arjun --prune-keep 20

Idempotent: a wheel already present in `archive/` is left alone and reported as `skip`.
Exit code is 0 on success, 1 if the copy failed. Intended to run immediately AFTER a
successful `bundle deploy`.
"""

import argparse
import subprocess
import sys

DEFAULT_CATALOG = "metaflow"
DEFAULT_SCHEMA = "config"
DEFAULT_VOLUME = "wheels"


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True)


def _cli(profile: str | None, *args: str) -> subprocess.CompletedProcess:
    cmd = ["databricks", *args]
    if profile:
        cmd += ["--profile", profile]
    return _run(cmd)


def list_dir(profile: str | None, path: str) -> list[str]:
    """Return bare filenames directly under `path`. Empty list if the path is absent."""
    proc = _cli(profile, "fs", "ls", f"dbfs:{path}")
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def ensure_dir(profile: str | None, path: str) -> tuple[bool, str]:
    """Create the archive folder. `fs cp` does NOT create its destination directory."""
    proc = _cli(profile, "fs", "mkdir", f"dbfs:{path}")
    if proc.returncode != 0:
        return False, (proc.stderr or proc.stdout).strip()
    return True, ""


def copy_file(profile: str | None, src: str, dst: str) -> tuple[bool, str]:
    proc = _cli(profile, "fs", "cp", f"dbfs:{src}", f"dbfs:{dst}")
    if proc.returncode != 0:
        return False, (proc.stderr or proc.stdout).strip()
    return True, ""


def wheel_sort_key(name: str) -> int:
    """Sort by the epoch-millis patch version bump_and_build.py stamps; unparseable sorts first."""
    try:
        return int(name.split("-")[1].split(".")[-1])
    except (IndexError, ValueError):
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", help="Databricks CLI profile (~/.databrickscfg)")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--volume", default=DEFAULT_VOLUME)
    parser.add_argument(
        "--prune-keep",
        type=int,
        default=0,
        help="Keep only the N most recent archived wheels (0 = keep everything, the default).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report actions without copying.")
    args = parser.parse_args()

    root = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    internal, archive = f"{root}/.internal", f"{root}/archive"

    wheels = [w for w in list_dir(args.profile, internal) if w.endswith(".whl")]
    if not wheels:
        print(f"No wheels found in {internal} -- nothing to archive.")
        print("If a deploy just succeeded, check --catalog/--schema/--volume and --profile.")
        return 0

    if not args.dry_run:
        ok, err = ensure_dir(args.profile, archive)
        if not ok:
            print(f"Could not create {archive}: {err}", file=sys.stderr)
            return 1

    archived = set(list_dir(args.profile, archive))
    failures = 0

    for wheel in sorted(wheels, key=wheel_sort_key):
        if wheel in archived:
            print(f"skip  {wheel} (already archived)")
            continue
        if args.dry_run:
            print(f"would copy {wheel} -> {archive}/")
            continue
        ok, err = copy_file(args.profile, f"{internal}/{wheel}", f"{archive}/{wheel}")
        if ok:
            print(f"copy  {wheel} -> {archive}/")
            archived.add(wheel)
        else:
            print(f"FAIL  {wheel}: {err}", file=sys.stderr)
            failures += 1

    if args.prune_keep > 0 and not args.dry_run:
        keep = args.prune_keep
        current = sorted(
            (w for w in list_dir(args.profile, archive) if w.endswith(".whl")),
            key=wheel_sort_key,
        )
        for stale in current[:-keep] if len(current) > keep else []:
            proc = _cli(args.profile, "fs", "rm", f"dbfs:{archive}/{stale}")
            verb = "prune" if proc.returncode == 0 else "FAIL-prune"
            print(f"{verb} {stale}")

    print(f"\nArchive: {archive}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
