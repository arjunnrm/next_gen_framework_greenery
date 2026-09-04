"""Bulk (many-spec) onboarding -- one job run that onboards an entire directory of specs.

``02_onboarding_engine.py`` onboards exactly one spec per run, driven by a ``spec_file_path``
job parameter. That is the right shape for CI/CD (one spec changes, one job runs) but the
wrong shape for the two situations this module exists for:

* **Bringing up a whole environment.** A fresh catalog needs every spec onboarded before any
  pipeline can run. Doing that one-spec-per-run means N job submissions, N serverless
  cold starts, and N places for a partial failure to hide.
* **Regression-onboarding the test corpus.** ``flowx_testing/`` holds ~48 specs that must
  all be re-onboarded whenever the control-table schema or the validator changes, so that the
  *next* thing to break is a real pipeline defect and not a stale control row.

**Why fail-soft is the default.** A bulk run's value is the *report*: which specs still
validate and upsert cleanly, and which don't. Aborting the whole run on the first bad spec
throws that away and tells you about exactly one problem -- so by default every spec is
attempted, failures are captured per-spec, and the run raises at the very end only if
anything failed (``fail_fast=True`` restores abort-on-first-failure for a gating CI step).
This mirrors ``spec_validator.validate_spec``'s own "collect every error, report once"
principle, one level up: that function does it across the fields of one spec, this one does
it across the specs of one directory.

**Ordering is deterministic** (lexicographic by filename). The ``flowx_testing/`` corpus is
deliberately numbered (``010_``, ``011_``, ...), so sorted order is also authoring order, and
a bulk run's log reads in the same sequence every time -- which matters when diffing two runs'
reports to find what changed.

Every spec is onboarded through the exact same framework functions
``02_onboarding_engine.py`` uses (``load_and_template_spec`` -> ``validate_spec`` ->
``upsert_*`` -> ``write_audit_log_entry``), so a spec that onboards here behaves identically
to one onboarded singly -- this module adds discovery, iteration and reporting, and changes
no onboarding semantics whatsoever.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from flowx.lakeflow_framework.exceptions import FrameworkError, OnboardingValidationError
from flowx.lakeflow_framework.onboarding.audit_logger import write_audit_log_entry
from flowx.lakeflow_framework.onboarding.metadata_upsert import (
    upsert_dataflow_group_spec,
    upsert_ingestion_flow_spec,
    upsert_observability_config,
    upsert_reconciliation_flow_spec,
    upsert_transformation_flow_spec,
)
from flowx.lakeflow_framework.onboarding.spec_loader import load_and_template_spec

logger = logging.getLogger("flowx.lakeflow_framework.onboarding.bulk_onboarding")

# Extensions load_and_template_spec can parse (see spec_loader._YAML_EXTENSIONS). Anything
# else in the directory -- README.md, TESTING_PLAN.md, .gitkeep -- is silently skipped rather
# than attempted-and-failed, since a spec directory legitimately holds companion docs.
SPEC_FILE_EXTENSIONS = (".json", ".yaml", ".yml")

ALLOWED_ACTION_TYPES = {"CREATE", "UPDATE", "VALIDATE_ONLY"}


def discover_spec_files(
    dbutils: Any,
    spec_dir: str,
    exclude_names: Optional[List[str]] = None,
) -> List[str]:
    """List every onboarding spec file directly inside ``spec_dir``, sorted by file name.

    Non-recursive by design: this framework's spec directories are flat, and recursing would
    silently pick up ``archive/``-style subfolders of superseded specs.

    Directory listing tries a plain ``os.listdir`` first -- both UC Volumes and Workspace
    Files are FUSE-mounted on Databricks job compute, so this is the path that actually
    works for ``${workspace.file_path}/flowx_testing`` -- and falls back to
    ``dbutils.fs.ls`` for paths only reachable through the DBFS API. This is deliberately the
    same two-step strategy ``spec_loader.read_raw_spec_text`` uses for reading an individual
    file, so discovery and reading never disagree about which paths are reachable.

    Parameters
    ----------
    spec_dir:
        Directory holding the specs, e.g. ``/Workspace/.../files/flowx_testing``.
    exclude_names:
        Bare file names (not full paths) to skip, e.g. a known-bad negative-test spec that is
        *supposed* to fail validation and would otherwise pollute the report.

    Returns
    -------
    list of str
        Full paths, lexicographically sorted.

    Raises
    ------
    OnboardingValidationError
        If ``spec_dir`` cannot be listed by either method, or contains no spec files at all
        (an empty result is almost always a wrong-path typo, not a legitimately empty
        directory, so it fails loudly rather than reporting a vacuous success).
    """
    excluded = set(exclude_names or [])

    file_names: List[str] = []
    try:
        file_names = os.listdir(spec_dir)
    except (OSError, IOError) as direct_list_error:
        try:
            file_names = [entry.name.rstrip("/") for entry in dbutils.fs.ls(spec_dir)]
        except Exception as fallback_error:  # noqa: BLE001
            raise OnboardingValidationError(
                f"Failed to list spec directory '{spec_dir}' via both os.listdir() "
                f"({direct_list_error}) and dbutils.fs.ls() ({fallback_error})"
            ) from fallback_error

    spec_paths = sorted(
        f"{spec_dir.rstrip('/')}/{name}"
        for name in file_names
        if name.lower().endswith(SPEC_FILE_EXTENSIONS) and name not in excluded
    )

    if not spec_paths:
        raise OnboardingValidationError(
            f"No onboarding spec files ({', '.join(SPEC_FILE_EXTENSIONS)}) found in '{spec_dir}' "
            f"after excluding {sorted(excluded) or 'nothing'} -- check the directory path."
        )

    logger.info("Discovered %d spec file(s) in '%s'", len(spec_paths), spec_dir)
    return spec_paths


def onboard_single_spec(
    spark: Any,
    dbutils: Any,
    spec_path: str,
    catalog: str,
    environment: str,
    action_type: str,
    control_schema: str,
    client_context_json: str,
    validate_spec_fn: Any,
) -> Dict[str, Any]:
    """Onboard exactly one spec and return a structured per-spec result record.

    Never raises for a spec-level problem -- a failure is *data* here (a result row with
    ``status="FAILED"`` and the error text), because the caller's whole job is to report on
    every spec, not to stop at the first one. Only a genuinely unexpected error escapes.

    ``validate_spec_fn`` is injected rather than imported at module scope purely so the
    notebook passes in the same ``validate_spec`` it already imported, keeping one import
    surface -- there is no second implementation.

    Returns
    -------
    dict
        ``{spec_file, dataflow_group_id, status, ingestion_flows, transformation_flows,
        reconciliation_flows, observability_destinations, spec_version, error}``.
    """
    file_name = os.path.basename(spec_path)
    result: Dict[str, Any] = {
        "spec_file": file_name,
        "dataflow_group_id": None,
        "status": "FAILED",
        "ingestion_flows": 0,
        "transformation_flows": 0,
        "reconciliation_flows": 0,
        "observability_destinations": 0,
        "spec_version": None,
        "error": None,
    }

    spec: Dict[str, Any] = {}
    templated_spec_text = ""
    spec_version = None

    try:
        spec, templated_spec_text, spec_version = load_and_template_spec(dbutils, spec_path, catalog, environment)
        result["dataflow_group_id"] = spec.get("dataflow_group_id")
        result["spec_version"] = spec_version

        (
            ingestion_flows,
            transformation_flows,
            reconciliation_flows,
            observability_destinations,
            validation_errors,
        ) = validate_spec_fn(spark, spec)

        if validation_errors:
            raise OnboardingValidationError(
                f"{len(validation_errors)} validation issue(s):\n  - " + "\n  - ".join(validation_errors)
            )

        result["ingestion_flows"] = len(ingestion_flows)
        result["transformation_flows"] = len(transformation_flows)
        result["reconciliation_flows"] = len(reconciliation_flows)
        result["observability_destinations"] = len(observability_destinations)

        if action_type == "VALIDATE_ONLY":
            logger.info("%s: VALIDATE_ONLY -- spec is valid, skipping upsert.", file_name)
        else:
            group_id = spec["dataflow_group_id"]
            upsert_dataflow_group_spec(
                spark, control_schema, spec, ingestion_flows, transformation_flows, catalog, environment
            )
            upsert_ingestion_flow_spec(spark, control_schema, group_id, ingestion_flows)
            upsert_transformation_flow_spec(spark, control_schema, group_id, transformation_flows)
            upsert_reconciliation_flow_spec(spark, control_schema, group_id, reconciliation_flows)
            upsert_observability_config(spark, control_schema, group_id, observability_destinations)
            logger.info("%s: onboarded group '%s'", file_name, group_id)

        result["status"] = "SUCCESS"

    except (FrameworkError, Exception) as exc:  # noqa: BLE001 - every failure mode becomes a report row
        result["error"] = str(exc)
        logger.error("%s: onboarding FAILED -- %s", file_name, exc)

    # The audit trail records attempts, not just successes -- a spec that failed validation is
    # exactly the thing an operator later wants to find in onboarding_audit_log. Audit-write
    # failures are logged but never allowed to mask the onboarding outcome itself.
    try:
        write_audit_log_entry(
            spark=spark,
            control_schema=control_schema,
            dataflow_group_id=result["dataflow_group_id"],
            action_type=action_type,
            environment=environment,
            onboarded_by=json.loads(client_context_json).get("user_principal", "unknown"),
            spec_version=spec_version,
            client_context_json=client_context_json,
            status=result["status"],
            raw_spec_payload=templated_spec_text,
            error_message=result["error"],
        )
    except Exception as audit_exc:  # noqa: BLE001
        logger.error("%s: failed to write audit log entry: %s", file_name, audit_exc)

    return result


def summarize_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Reduce per-spec result records to a run-level summary (counts + failed file names)."""
    succeeded = [r for r in results if r["status"] == "SUCCESS"]
    failed = [r for r in results if r["status"] != "SUCCESS"]
    return {
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "failed_specs": [r["spec_file"] for r in failed],
        "total_ingestion_flows": sum(r["ingestion_flows"] for r in results),
        "total_transformation_flows": sum(r["transformation_flows"] for r in results),
        "total_reconciliation_flows": sum(r["reconciliation_flows"] for r in results),
        "total_observability_destinations": sum(r["observability_destinations"] for r in results),
    }


def format_results_table(results: List[Dict[str, Any]]) -> str:
    """Render per-spec results as a fixed-width text table for the job's driver log.

    Plain text rather than a DataFrame ``display()`` so the output is readable in the Jobs UI
    log, in ``databricks jobs get-run-output``, and in a CI console alike.
    """
    header = f"{'STATUS':<8} {'SPEC FILE':<42} {'DATAFLOW GROUP':<34} {'ING':>4} {'TRF':>4} {'REC':>4} {'OBS':>4}"
    lines = [header, "-" * len(header)]
    for r in results:
        lines.append(
            f"{r['status']:<8} {r['spec_file'][:42]:<42} {str(r['dataflow_group_id'] or '-')[:34]:<34} "
            f"{r['ingestion_flows']:>4} {r['transformation_flows']:>4} {r['reconciliation_flows']:>4} "
            f"{r['observability_destinations']:>4}"
        )
    return "\n".join(lines)
