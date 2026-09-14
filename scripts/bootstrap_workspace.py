"""Bring up a FlowX target on a brand-new workspace in one command.

**The problem this solves.** A first deploy to a workspace that has never held this bundle
fails during *config resolution*, before a single resource is created::

    Error: volume flowx.config.wheels does not exist
      at workspace.artifact_path
         resources.volumes.framework_wheels_volume

Every target points `workspace.artifact_path` into a UC Volume -- and that Volume is declared
by this same bundle (`resources/flowx_config_jobs/framework_wheels_volume.yml`). DABs refuses
the path anyway, because the pre-check runs before resource creation: the Volume that would
satisfy it has not been created yet, and cannot be, because the deploy that would create it is
the one being rejected. A genuine chicken-and-egg, and it is the FIRST thing anyone hits on a
new workspace.

**How it is broken.** `artifact_path` is composed from `${var.wheels_root}`, so the Volume can
be created by one deploy that points `wheels_root` somewhere harmless (the deployer's ordinary
workspace artifact directory), after which the normal default -- back inside the Volume -- has
something real to resolve against. Two deploys, no file ever edited.

Before v1.7.4 the same trick required commenting out each target's `artifact_path:` line by
hand, deploying, then uncommenting it: three edits to a shared, version-controlled file, easy
to half-finish and easy to commit by mistake. That is what this script and the `wheels_root`
variable replace.

**What this does NOT do.** It does not create the catalog. `${var.catalog}` is a prerequisite,
created once per workspace in the UI (**Catalog -> Create catalog -> Default storage**); a
`catalogs.*` resource cannot work on UC Default Storage accounts and was removed on 2026-09-02
(see `resources/flowx_bootstrap/README.md`). This script checks the catalog exists and stops
with instructions if it does not, rather than failing later and less clearly.

It also does not run the app. `bundle deploy` uploads the app source but leaves the running app
on its previous code -- `--run-app` adds the required `bundle run flowx_onboarding_app` step.

Usage::

    # dry run first -- prints the exact commands, changes nothing
    python scripts/bootstrap_workspace.py -t metaflow_v7 -p metaflow_v7 --dry-run

    # bootstrap + full deploy
    python scripts/bootstrap_workspace.py -t metaflow_v7 -p metaflow_v7

    # ... and start the app
    python scripts/bootstrap_workspace.py -t metaflow_v7 -p metaflow_v7 --run-app

Idempotent: on a workspace already bootstrapped, phase 1 is skipped (the Volume resolves) and
the script goes straight to the normal deploy, so it is safe to re-run.

**DO NOT PIPE THIS SCRIPT.** It signals failure through its exit code, and a pipe throws that
away: in `python scripts/bootstrap_workspace.py ... | tail -60` the shell reports *tail's*
status, which is always 0, so a failed bootstrap looks like a success. On the bt_digital_poc
bring-up (2026-09-13) that masked a phase-2 deploy failure. Run it unpiped and read the exit
code. If you need the output in a file as well, redirect rather than pipe --
`... > bootstrap.log 2>&1` keeps the script's own status -- or in bash set `PIPESTATUS`/
`set -o pipefail` before piping. The failure banner is printed in full so that it survives
a pipe even when the exit code does not.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# The pre-check this whole script exists to sidestep. Matched against `bundle validate` stderr
# to decide whether phase 1 is needed at all -- matching the message, rather than assuming a
# fresh workspace, is what makes re-runs cheap and safe.
VOLUME_PRECHECK_RE = re.compile(
    r"volume\s+\S+\s+does not exist|does not exist\s*\n\s*at workspace\.artifact_path",
    re.IGNORECASE,
)

# Deployed in phase 1, and only these: the schema the wheels Volume lives in, and the Volume
# itself. Everything else waits for phase 2, when artifact_path resolves normally.
BOOTSTRAP_SELECT = "schemas.config_schema,volumes.framework_wheels_volume"

# `databricks bundle deploy` sends `forward_user_access_token` in the app UPDATE mask, which the
# Apps API rejects (it is valid only on CREATE). So a first deploy to a fresh workspace succeeds
# and every later one fails on the app alone, while all other resources deploy fine. Recognised
# here so the script reports it as the known CLI issue it is instead of a bootstrap failure.
APP_UPDATE_MASK_RE = re.compile(r"Invalid update mask", re.IGNORECASE)

#: Newline, named so the long multi-part warning below stays readable.
NL = "\n"


def run(cmd: list[str], *, dry_run: bool = False, check: bool = True,
        capture: bool = False) -> subprocess.CompletedProcess:
    """Echo a command, then run it (unless dry_run)."""
    printable = " ".join(f'"{c}"' if " " in c else c for c in cmd)
    print(f"\n  $ {printable}\n", flush=True)
    if dry_run:
        return subprocess.CompletedProcess(cmd, 0, "", "")
    # encoding/errors are explicit: the CLI emits UTF-8, but Python on Windows decodes a
    # subprocess pipe as cp1252 by default and dies on the first em-dash in bundle output.
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        check=check,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=capture,
    )


def bundle_summary(target: str, profile: str) -> dict:
    """Return `bundle validate -o json`'s summary, or {} if validation fails.

    Used for two things a bootstrap needs before it can do anything useful: the deploying
    user (which forms the phase-1 artifact path) and whether the volume pre-check fires.
    """
    proc = run(
        ["databricks", "bundle", "validate", "-t", target, "-p", profile, "-o", "json"],
        check=False,
        capture=True,
    )
    if proc.returncode != 0 or not proc.stdout:
        return {}
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {}


def precheck_fires(target: str, profile: str) -> tuple[bool, str]:
    """Does the artifact_path volume pre-check currently reject this target?"""
    proc = run(
        ["databricks", "bundle", "validate", "-t", target, "-p", profile],
        check=False,
        capture=True,
    )
    combined = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode == 0:
        return False, combined
    return bool(VOLUME_PRECHECK_RE.search(combined)), combined


def current_user(profile: str) -> str | None:
    proc = run(
        ["databricks", "current-user", "me", "-p", profile, "-o", "json"],
        check=False,
        capture=True,
    )
    if proc.returncode != 0 or not proc.stdout:
        return None
    try:
        return json.loads(proc.stdout).get("userName")
    except json.JSONDecodeError:
        return None


def catalog_exists(catalog: str, profile: str) -> bool:
    proc = run(
        ["databricks", "catalogs", "get", catalog, "-p", profile, "-o", "json"],
        check=False,
        capture=True,
    )
    return proc.returncode == 0


def target_catalog(target: str, profile: str) -> str | None:
    """Read the target's resolved ${var.catalog}."""
    summary = bundle_summary(target, profile)
    variables = summary.get("variables") or {}
    entry = variables.get("catalog") or {}
    return entry.get("value") or entry.get("default")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap and deploy a FlowX target on a new workspace.",
    )
    parser.add_argument("-t", "--target", required=True, help="bundle target name")
    parser.add_argument("-p", "--profile", required=True, help="databricks CLI profile")
    parser.add_argument(
        "--run-app",
        action="store_true",
        help="also `bundle run flowx_onboarding_app` (deploy alone leaves the app on old code)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the commands without running the deploys",
    )
    args = parser.parse_args()

    print(f"FlowX workspace bootstrap -- target={args.target} profile={args.profile}")

    # --- Prerequisite: the catalog. Not creatable from YAML on Default Storage accounts. ---
    catalog = target_catalog(args.target, args.profile)
    if catalog and not args.dry_run:
        if not catalog_exists(catalog, args.profile):
            print(
                f"\nERROR: catalog '{catalog}' does not exist on profile "
                f"'{args.profile}'.\n\n"
                "  It is a PREREQUISITE, not a bundle resource -- these accounts use UC\n"
                "  Default Storage, where a catalogs.* resource cannot be created from YAML.\n"
                "  Create it once in the UI:  Catalog -> Create catalog -> Default storage\n"
                "  then re-run this script. See resources/flowx_bootstrap/README.md.",
                file=sys.stderr,
            )
            return 2
        print(f"  catalog '{catalog}' exists.")

    # --- Phase 1, only if the pre-check actually fires. ---
    needs_bootstrap, output = precheck_fires(args.target, args.profile)

    if needs_bootstrap:
        user = current_user(args.profile)
        if not user:
            print(
                f"ERROR: could not resolve the current user for profile "
                f"'{args.profile}'. Is it authenticated? `databricks auth profiles`",
                file=sys.stderr,
            )
            return 2

        # Anywhere outside the not-yet-created Volume works; the deployer's own bundle
        # artifact directory is the natural choice, and is what DABs would have used anyway
        # had no artifact_path been set.
        fallback = f"/Workspace/Users/{user}/.bundle/flowx/{args.target}/artifacts"

        print(
            "\n== Phase 1: artifact_path pre-check fires -- creating the wheels Volume ==\n"
            "   The Volume that artifact_path points into does not exist yet, and the check\n"
            "   runs before any resource is created. Overriding wheels_root for THIS ONE\n"
            f"   command lands the wheel at {fallback}\n"
            "   instead, so the Volume can be created. No file is edited."
        )
        run(
            [
                "databricks", "bundle", "deploy",
                "-t", args.target, "-p", args.profile,
                "--select", BOOTSTRAP_SELECT,
                f"--var=wheels_root={fallback}",
            ],
            dry_run=args.dry_run,
        )
    else:
        print(
            "\n== Phase 1: skipped -- artifact_path already resolves ==\n"
            "   The wheels Volume exists, so this workspace is already bootstrapped."
        )
        if output.strip() and "Validation OK" not in output:
            print(output.strip())

    # --- Phase 2: the ordinary full deploy, with wheels_root back at its default. ---
    print(
        "\n== Phase 2: full deploy ==\n"
        "   wheels_root is back at its default, so the wheel lands in the Volume."
    )
    deploy = run(
        ["databricks", "bundle", "deploy", "-t", args.target, "-p", args.profile],
        dry_run=args.dry_run,
        check=False,
        capture=not args.dry_run,
    )
    if not args.dry_run:
        combined = (deploy.stdout or "") + (deploy.stderr or "")
        if combined.strip():
            # flush before the warning below, which goes to stderr -- otherwise the two streams
            # interleave and the warning appears above the output it is explaining.
            print(combined.rstrip(), flush=True)
        if deploy.returncode != 0:
            if APP_UPDATE_MASK_RE.search(combined):
                # A CLI/Apps-API mismatch, not a problem with this bundle: the CLI sends
                # `forward_user_access_token` in the UPDATE mask, which the API accepts only
                # on CREATE. It therefore bites the second deploy onward, never the first,
                # and it blocks nothing else -- `bundle run` still rolls the app code forward.
                print(
                    f"{NL}WARNING: the app could not be UPDATED -- "
                    "'Invalid update mask ... forward_user_access_token'." + NL +
                    "  This is a Databricks CLI/Apps-API mismatch, not a fault in this"
                    " bundle," + NL +
                    "  and it affects only the app's definition. Every other resource"
                    " above deployed normally." + NL +
                    "  Roll the app's code forward with:" + NL +
                    f"    databricks bundle run flowx_onboarding_app -t {args.target}"
                    f" -p {args.profile}" + NL +
                    "  See docs/onboarding/05_new_workspace_bootstrap.md"
                    " (Troubleshooting).",
                    file=sys.stderr,
                )
            else:
                # The banner is deliberately loud and multi-line. This script's exit code is
                # the reliable signal, but it is EASY TO LOSE: piping the run through another
                # command (`... | tail -60`) makes the shell report the LAST command's status,
                # so a failed bootstrap looks like a clean exit 0. That happened on the
                # bt_digital_poc bring-up (2026-09-13) and very nearly hid a failed deploy.
                # A banner survives the pipe even when the exit code does not.
                print(
                    NL + "=" * 72 + NL +
                    "ERROR: the full deploy FAILED; see the output above." + NL +
                    "  Phase 1 may have succeeded -- the target is PARTIALLY deployed." + NL +
                    "  Resources created before the failure may exist in the workspace" + NL +
                    "  WITHOUT being recorded in DABs state; the next deploy will try to" + NL +
                    "  create them again and fail with ALREADY_EXISTS. Check with:" + NL +
                    f"    databricks bundle summary -t {args.target} -p {args.profile}" + NL +
                    "  and adopt any orphan with `databricks bundle deployment bind" + NL +
                    "  <bare_resource_key> <id>`. See" + NL +
                    "  docs/onboarding/06_bt_digital_poc_deployment_issues.md" + NL +
                    "NOTE: this script is exiting NON-ZERO. If your shell reported 0, you" + NL +
                    "  piped it (`| tail`) and the pipe masked the status -- trust this" + NL +
                    "  banner, not the exit code." + NL +
                    "=" * 72,
                    file=sys.stderr,
                )
                return 1

    # --- Optional: the app's mandatory second step. ---
    if args.run_app:
        print(
            "\n== Phase 3: start the app ==\n"
            "   `bundle deploy` uploads the source but leaves the running app on its\n"
            "   previous code; this is the step that actually rolls it forward."
        )
        run(
            [
                "databricks", "bundle", "run", "flowx_onboarding_app",
                "-t", args.target, "-p", args.profile,
            ],
            dry_run=args.dry_run,
        )

    if args.dry_run:
        print("\nDry run -- nothing was deployed.")
    else:
        print(f"\nDone. Target '{args.target}' is deployed.")
        if not args.run_app:
            print(
                "  NOTE: the app is still on its previous code. Roll it forward with:\n"
                f"    databricks bundle run flowx_onboarding_app -t {args.target} "
                f"-p {args.profile}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
