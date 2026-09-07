# Databricks notebook source
# MAGIC %md
# MAGIC # Dataflow Group Documentation Generator
# MAGIC
# MAGIC Renders one Markdown design document per FlowX dataflow group from the control metadata
# MAGIC and the Databricks system tables, and writes them to a Unity Catalog Volume.
# MAGIC
# MAGIC **Why generate rather than hand-write.** A FlowX onboarding spec says what a pipeline
# MAGIC *should* do; the control tables say what was actually onboarded, and the system tables say
# MAGIC how it has actually been running. A hand-written document goes stale the moment any of the
# MAGIC three changes. These documents are regenerated from
# MAGIC `<catalog>.observability` (see `control_plane/observability_views.py`), so they cannot
# MAGIC drift from the deployed system.
# MAGIC
# MAGIC This is the batch counterpart to the FlowX Genie space
# MAGIC (`resources/flowx_genie/`): Genie answers "explain dataflow group X" conversationally,
# MAGIC this produces the durable artefact you can commit, attach to a ticket or hand to a
# MAGIC customer. Both read the same views, so they cannot disagree about the facts.
# MAGIC
# MAGIC All rendering lives in `observability/dataflow_documenter.py` as pure functions over
# MAGIC plain Python data -- this notebook only reads the views and writes files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap
# MAGIC
# MAGIC In production `flowx` is attached as a wheel library, so a plain import resolves from
# MAGIC site-packages. The fallback below is for local, wheel-less notebook development only.

# COMMAND ----------

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dataflow_documentation")

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
            "wheel library (see resources/*.yml); for local development, run from within the "
            f"repo so '../../src' resolves. Original error: {exc}"
        ) from exc

from flowx.lakeflow_framework.observability.dataflow_documenter import (  # noqa: E402
    render_group_document,
    render_index,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets & Constants

# COMMAND ----------

dbutils.widgets.text("catalog", "flowx", "Unity Catalog catalog")
dbutils.widgets.text("output_volume", "", "Output volume path (blank = derive)")
dbutils.widgets.text("dataflow_group_id", "", "Single group to document (blank = all)")
dbutils.widgets.text("lookback_days", "30", "Reporting window in days")

CATALOG = dbutils.widgets.get("catalog").strip()
OBSERVABILITY_SCHEMA = f"{CATALOG}.observability"
GROUP_FILTER = dbutils.widgets.get("dataflow_group_id").strip()
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days").strip() or "30")

# Default to the framework's own docs volume. Kept as a widget so a caller can redirect the
# output without editing the job -- e.g. to a per-release folder.
_out = dbutils.widgets.get("output_volume").strip()
OUTPUT_PATH = _out or f"/Volumes/{CATALOG}/config/framework_docs/dataflow_groups"

logger.info("Catalog=%s  observability=%s  output=%s  lookback=%dd  group=%s",
            CATALOG, OBSERVABILITY_SCHEMA, OUTPUT_PATH, LOOKBACK_DAYS,
            GROUP_FILTER or "<all>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the observability views
# MAGIC
# MAGIC One query per view, collected to the driver. These are control-plane and aggregate
# MAGIC datasets -- tens to low thousands of rows -- so collecting is appropriate here and keeps
# MAGIC the rendering layer free of Spark.

# COMMAND ----------


def rows(sql: str):
    """Run a query and return a list of plain dicts, so rendering needs no Spark types."""
    return [r.asDict() for r in spark.sql(sql).collect()]


_where_group = (
    f"WHERE dataflow_group_id = '{GROUP_FILTER}'" if GROUP_FILTER else ""
)
_and_group = (
    f"AND dataflow_group_id = '{GROUP_FILTER}'" if GROUP_FILTER else ""
)

groups = rows(f"""
    SELECT * FROM {OBSERVABILITY_SCHEMA}.v_dataflow_group_catalog
    {_where_group}
    ORDER BY dataflow_group_id
""")
if not groups:
    raise RuntimeError(
        f"No dataflow groups found in {OBSERVABILITY_SCHEMA}.v_dataflow_group_catalog"
        + (f" for dataflow_group_id='{GROUP_FILTER}'" if GROUP_FILTER else "")
        + ". Has the onboarding job run, and does the observability schema exist?"
    )
logger.info("Documenting %d dataflow group(s)", len(groups))

flows = rows(f"""
    SELECT * FROM {OBSERVABILITY_SCHEMA}.v_flow_inventory
    {_where_group}
    ORDER BY dataflow_group_id, flow_kind, flow_id
""")

# The run-history views depend on system table access, which the running principal may not
# hold. Each is optional: a missing one degrades that section of the document to "not observed"
# rather than failing the job, because the control-metadata half of the document is still
# worth producing.
def optional_rows(sql: str, label: str):
    try:
        return rows(sql)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read %s (%s). The corresponding document section will say "
                       "nothing was observed.", label, str(exc)[:200])
        return []


health = optional_rows(f"""
    SELECT * FROM {OBSERVABILITY_SCHEMA}.v_group_health_summary {_where_group}
""", "v_group_health_summary")

dq = optional_rows(f"""
    SELECT dataflow_group_id, dataset_name, rule_name,
           SUM(passed_records) AS passed_records,
           SUM(failed_records) AS failed_records,
           ROUND(100.0 * SUM(passed_records)
                 / NULLIF(SUM(passed_records) + SUM(failed_records), 0), 2) AS pass_rate_pct
    FROM {OBSERVABILITY_SCHEMA}.v_dq_results
    WHERE run_date >= CURRENT_DATE() - INTERVAL {LOOKBACK_DAYS} DAYS {_and_group}
    GROUP BY dataflow_group_id, dataset_name, rule_name
    ORDER BY dataflow_group_id, failed_records DESC, rule_name
""", "v_dq_results")

recon = optional_rows(f"""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY dataflow_group_id, reconciliation_id, target_id
            ORDER BY run_at DESC NULLS LAST) AS rn
        FROM {OBSERVABILITY_SCHEMA}.v_reconciliation_health
        {_where_group}
    ) WHERE rn = 1
    ORDER BY dataflow_group_id, reconciliation_id, target_id
""", "v_reconciliation_health")

lineage = optional_rows(f"""
    SELECT dataflow_group_id, source_table, target_table, entity_name, MAX(last_seen) AS last_seen
    FROM {OBSERVABILITY_SCHEMA}.v_dataflow_lineage
    WHERE source_table IS NOT NULL AND dataflow_group_id IS NOT NULL {_and_group}
    GROUP BY dataflow_group_id, source_table, target_table, entity_name
    ORDER BY dataflow_group_id, last_seen DESC
""", "v_dataflow_lineage")

logger.info("Read: %d flows, %d health rows, %d dq rows, %d recon rows, %d lineage edges",
            len(flows), len(health), len(dq), len(recon), len(lineage))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Render and write
# MAGIC
# MAGIC `dbutils.fs.put` is used rather than the local filesystem because a UC Volume path is
# MAGIC not writable through plain Python file IO on serverless compute.

# COMMAND ----------

from datetime import datetime, timezone  # noqa: E402

GENERATED_AT = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def by_group(collection, gid):
    return [r for r in collection if r.get("dataflow_group_id") == gid]


health_by_group = {str(h.get("dataflow_group_id")): h for h in health}

try:
    dbutils.fs.mkdirs(OUTPUT_PATH)
except Exception as exc:  # noqa: BLE001
    logger.warning("Could not create %s (%s); assuming it already exists.",
                   OUTPUT_PATH, str(exc)[:160])

written = []
for g in groups:
    gid = str(g.get("dataflow_group_id"))
    doc = render_group_document(
        group=g,
        flows=by_group(flows, gid),
        health=health_by_group.get(gid),
        dq=by_group(dq, gid),
        recon=by_group(recon, gid),
        lineage=by_group(lineage, gid),
        generated_at=GENERATED_AT,
    )
    path = f"{OUTPUT_PATH}/{gid}.md"
    dbutils.fs.put(path, doc, overwrite=True)
    written.append(path)
    logger.info("Wrote %s (%d chars)", path, len(doc))

index = render_index(groups, health_by_group, generated_at=GENERATED_AT)
index_path = f"{OUTPUT_PATH}/README.md"
dbutils.fs.put(index_path, index, overwrite=True)
written.append(index_path)
logger.info("Wrote %s", index_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Result

# COMMAND ----------

print(f"Generated {len(written)} document(s) under {OUTPUT_PATH}:\n")
for p in written:
    print("  ", p)

# Surface the index inline so a job run shows the result without opening the volume.
displayHTML(
    "<pre style='font-family:ui-monospace,monospace;font-size:12px;white-space:pre-wrap'>"
    + index.replace("&", "&amp;").replace("<", "&lt;")
    + "</pre>"
)

dbutils.notebook.exit(
    f"Documented {len(groups)} dataflow group(s) to {OUTPUT_PATH}"
)
