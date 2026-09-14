# Databricks notebook source
# MAGIC %md
# MAGIC # Post-Deployment Governance
# MAGIC
# MAGIC Run this notebook as a job task chained *after* the Lakeflow pipeline's update task
# MAGIC completes. It applies Unity Catalog governance tags (column + table) for a
# MAGIC `dataflow_group_id` -- ``ALTER TABLE ... SET TAGS`` is DDL against an already
# MAGIC materialized table, which is only guaranteed to exist once the pipeline update has
# MAGIC finished.
# MAGIC
# MAGIC **Multi-group (v0.0.7):** the `dataflow_group_id` widget accepts either a single id or a
# MAGIC **comma-separated list** (`dfg_a,dfg_b,dfg_c`), so one task can tag every group in a use
# MAGIC case. A single id is simply the one-element case, so every existing single-id job
# MAGIC definition keeps working unchanged. Blank entries and duplicates are dropped.
# MAGIC
# MAGIC Each group is applied independently: a failure on one is recorded and the remaining
# MAGIC groups are still attempted, then the task fails at the end naming every group that
# MAGIC failed. Without that, one bad group would silently deny its tags to every group listed
# MAGIC after it -- making a combined task worse than one task per group.
# MAGIC
# MAGIC **Phase 7:** this notebook no longer runs `external_sink` egress. Every
# MAGIC `external_sink`/`sink` export is now a genuine `dlt.create_sink`/`@dlt.append_flow`
# MAGIC registered *inside* the pipeline's own graph (see
# MAGIC `flowx.lakeflow_framework.engine.sink_registration`), executed as
# MAGIC part of the pipeline update itself -- there is no longer a separate post-deployment
# MAGIC egress step for it to run here. See
# MAGIC `control_plane/post_deployment.py`'s module docstring for the full rationale.
# MAGIC
# MAGIC **Phase 10:** this notebook also now captures SCD/CDC insert-update-delete counts for
# MAGIC every CDC-dispatched flow in the group (`capture_all_scd_change_counts`), emitted as
# MAGIC structured JSON log events -- the same "must run after the pipeline update, since a
# MAGIC Delta table's post-update version isn't knowable from inside the update's own
# MAGIC graph-definition code" reasoning as the governance tags above. See
# MAGIC `control_plane/post_deployment.py`'s "Phase 10" docstring section.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("apply_governance_and_egress")

try:
    import flowx.lakeflow_framework  # noqa: F401
except ImportError:
    try:
        this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        dev_src_root = os.path.abspath(os.path.join(this_dir, "..", "..", "src"))
        if dev_src_root not in sys.path:
            sys.path.insert(0, dev_src_root)
        import flowx.lakeflow_framework  # noqa: F401
        logger.warning("Loaded 'flowx' from local 'src/' (dev fallback) -- not from an installed wheel.")
    except ImportError as exc:
        raise ImportError(
            "Could not import 'flowx'. In production this must be attached as a "
            "wheel library (see resources/*.yml); for local development, run from within the repo so "
            f"'../../src' resolves. Original error: {exc}"
        ) from exc

# apply_all_governance_tags comes from the GOVERNANCE module (v1.7.x) -- it was moved there from
# control_plane.post_deployment, which still re-exports it for backward compatibility. Import it
# from its real home so this notebook does not depend on that compatibility shim.
from flowx.lakeflow_framework.control_plane.post_deployment import (  # noqa: E402
    capture_all_scd_change_counts,
)
from flowx.lakeflow_framework.governance.tags import apply_all_governance_tags  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Constants

# COMMAND ----------

dbutils.widgets.text("catalog", "poc", "Target Unity Catalog")
dbutils.widgets.text("dataflow_group_id", "", "Dataflow group(s), comma-separated, to apply governance for")
dbutils.widgets.dropdown("apply_abac", "true", ["true", "false"], "Apply ABAC row filters / column masks")
dbutils.widgets.dropdown(
    "capture_cdc_change_counts", "true", ["true", "false"], "Capture SCD/CDC insert-update-delete counts (Phase 10)"
)
# NOTE: a 'run_egress_exports' job parameter may still be passed by older resources/*.yml job
# definitions (external_sink egress used to run as a post-deployment step here) -- it is now
# silently ignored: sink egress runs entirely inside the pipeline graph (Phase 7), and an
# unconsumed base_parameter/job parameter is harmless (Databricks does not require every
# passed parameter to have a matching widget).

CATALOG = dbutils.widgets.get("catalog").strip()
GROUP_ID_RAW = dbutils.widgets.get("dataflow_group_id").strip()
APPLY_ABAC = dbutils.widgets.get("apply_abac").strip().lower() == "true"
CAPTURE_CDC_CHANGE_COUNTS = dbutils.widgets.get("capture_cdc_change_counts").strip().lower() == "true"

# MULTI-GROUP (v0.0.7): `dataflow_group_id` accepts ONE id or a comma-separated LIST, so a single
# task can tag every group in a use case instead of needing one task per group. A single id is
# just the one-element case of the list -- the old single-id spelling keeps working verbatim, so
# existing resources/*.yml need no edit.
#
# Blanks are dropped and order-preserving de-duplication is applied, because a trailing comma
# ("a,b,") or a repeated id is an easy hand-edit to make in YAML and neither should fail the run
# or tag the same group twice.
GROUP_IDS = []
for _candidate in GROUP_ID_RAW.split(","):
    _candidate = _candidate.strip()
    if _candidate and _candidate not in GROUP_IDS:
        GROUP_IDS.append(_candidate)

if not GROUP_IDS:
    raise ValueError(
        "The 'dataflow_group_id' widget is required -- pass one dataflow_group_id, or several "
        "as a comma-separated list (e.g. 'dfg_a,dfg_b')."
    )

# Kept for log messages and for anything downstream that still expects a single scalar.
GROUP_ID = GROUP_IDS[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

# EVERY GROUP IS ATTEMPTED, AND FAILURES ARE RAISED TOGETHER AT THE END. Iterating with a bare
# `raise` on the first failure would make one task carrying N groups strictly WORSE than N
# separate tasks: a bad group would silently deny every group listed after it its tags, and the
# run would report a single failure that named only the first. So each group is isolated, its
# failure recorded, and the task fails at the end naming all of them.
#
# This mirrors the accumulate-then-raise contract `governance/tags.py::apply_governance_tags`
# already uses per statement within one table.
_failures = []

for _group_id in GROUP_IDS:
    try:
        if APPLY_ABAC:
            apply_all_governance_tags(spark, CATALOG, _group_id)
        else:
            logger.info("apply_abac=false: skipping governance tag application for '%s'.", _group_id)

        # Phase 10: SCD/CDC insert-update-delete count capture. Deliberately NOT inside the same
        # try/except as apply_all_governance_tags's own failure path above -- capture_all_scd_
        # change_counts already never raises on its own (a per-flow CDF/history failure is logged
        # and skipped internally, see control_plane/post_deployment.py's docstring), so it should
        # run regardless of whether ABAC tagging was even attempted this pass.
        if CAPTURE_CDC_CHANGE_COUNTS:
            capture_all_scd_change_counts(spark, CATALOG, _group_id)
        else:
            logger.info(
                "capture_cdc_change_counts=false: skipping SCD/CDC change-count capture for '%s'.", _group_id
            )

        logger.info("Governance pass complete for group '%s'", _group_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("Governance pass failed for group '%s': %s", _group_id, exc)
        _failures.append(f"{_group_id}: {exc}")

if _failures:
    raise RuntimeError(
        f"Governance pass failed for {len(_failures)} of {len(GROUP_IDS)} group(s): {_failures}"
    )

logger.info("Governance pass complete for all %d group(s): %s", len(GROUP_IDS), GROUP_IDS)
