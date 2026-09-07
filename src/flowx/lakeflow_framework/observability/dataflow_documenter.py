"""Render a per-dataflow-group design document from the FlowX control metadata.

WHY THIS EXISTS. A FlowX onboarding spec is the source of truth for what a pipeline does, but a
spec is not a document: it is a dense JSON/YAML artefact that assumes the reader already knows
the framework. When a new engineer inherits a dataflow group, or a customer asks "what does this
pipeline actually do", the answer has to be assembled by hand from the spec, the control tables
and the run history. This module assembles it automatically, from the control tables rather than
from the spec file -- so the document describes the system **as onboarded and as running**, not
as some spec file on someone's laptop claims it should be.

It is the batch counterpart to the Genie space in ``resources/flowx_genie/``: Genie answers
"explain dataflow group X" conversationally, this produces the durable Markdown artefact that
can be committed, attached to a ticket, or handed to a customer. Both read the same
``<catalog>.observability`` views, so they cannot disagree about the facts.

PURE-FUNCTION BOUNDARY. Everything here takes plain Python data (lists of dicts) and returns a
string. No Spark, no I/O. The notebook
``notebooks/09_documentation/09_dataflow_documentation.py`` is the only place that reads the
views and writes files. That split keeps the rendering unit-testable without a cluster, which
matters because there is no local Spark in this repo.
"""

from typing import Any, Dict, List, Optional

# Ordering for the flow-kind sections. Ingestion first because it is where data enters the
# system, then the transformations that reshape it, then the reconciliations that verify it --
# the order a reader needs to build a mental model, not alphabetical.
FLOW_KIND_ORDER = ["INGESTION", "TRANSFORMATION", "RECONCILIATION"]

FLOW_KIND_BLURB = {
    "INGESTION": (
        "Reads an external source and lands it in Bronze. Under the Single-Read DAG mandate "
        "each source table is read exactly once per execution mode; every downstream consumer "
        "then reads the resulting base node rather than the external location."
    ),
    "TRANSFORMATION": (
        "Builds a Silver or Gold table with SQL, consuming tables already in the pipeline DAG. "
        "Filtering, renaming and join preparation happen inside the consuming table rather than "
        "in throwaway staging tables."
    ),
    "RECONCILIATION": (
        "Compares a source against one or more targets and classifies every record as matched, "
        "missing in target, missing in source, or value drift."
    ),
}

_HEALTH_BLURB = {
    "HEALTHY": "All recent updates succeeded with no data-quality or reconciliation findings.",
    "DEGRADED": (
        "Recent updates completed, but at least one failure, data-quality violation or "
        "reconciliation discrepancy was recorded in the window."
    ),
    "FAILING": "The most recent pipeline update failed.",
    "INACTIVE": "This group is soft-disabled and the engine skips it.",
    "NO_RUNS": "No pipeline updates were observed in the window.",
}


def _fmt(value: Any, dash: str = "-") -> str:
    """Render a cell value for Markdown, collapsing None/empty to a dash."""
    if value is None:
        return dash
    if isinstance(value, bool):
        return "yes" if value else "no"
    text = str(value).strip()
    if text == "" or text == "{}":
        return dash
    # A pipe inside a value would silently break the enclosing Markdown table.
    return text.replace("|", "\\|")


def _num(value: Any) -> str:
    if value is None:
        return "-"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return _fmt(value)
    return f"{int(f):,}" if f == int(f) else f"{f:,.2f}"


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    """Render a Markdown table, or a placeholder line when there are no rows."""
    if not rows:
        return ["_None configured._", ""]
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    out.append("")
    return out


def render_group_document(
    group: Dict[str, Any],
    flows: List[Dict[str, Any]],
    health: Optional[Dict[str, Any]] = None,
    dq: Optional[List[Dict[str, Any]]] = None,
    recon: Optional[List[Dict[str, Any]]] = None,
    lineage: Optional[List[Dict[str, Any]]] = None,
    generated_at: Optional[str] = None,
) -> str:
    """Render one dataflow group as a Markdown design document.

    ``group`` is a row of ``v_dataflow_group_catalog``; ``flows`` rows of ``v_flow_inventory``;
    ``health`` a row of ``v_group_health_summary``; ``dq``, ``recon`` and ``lineage`` rows of
    ``v_dq_results``, ``v_reconciliation_health`` and ``v_dataflow_lineage`` respectively. Every
    argument except ``group`` and ``flows`` is optional so the document still renders on a
    workspace with no run history or no system-table access.
    """
    gid = group.get("dataflow_group_id", "unknown")
    dq = dq or []
    recon = recon or []
    lineage = lineage or []
    L: List[str] = []

    # ---------------------------------------------------------------- header
    L += [f"# Dataflow Group: `{gid}`", ""]
    if generated_at:
        L += [f"_Generated {generated_at} from the FlowX control metadata in "
              f"`{group.get('catalog_name', 'unknown')}.config`. Do not hand-edit: rerun the "
              f"documentation job instead._", ""]
    L += ["> This document is generated from the control tables, so it describes the group **as "
          "onboarded and as running** -- not as a spec file claims it should be.", ""]

    # ---------------------------------------------------------------- summary
    L += ["## 1. Purpose and scale", ""]
    summary = group.get("feature_summary")
    if summary:
        L += [str(summary), ""]
    L += _table(["Property", "Value"], [
        ["Environment", _fmt(group.get("environment"))],
        ["Catalog", _fmt(group.get("catalog_name"))],
        ["Active", _fmt(group.get("is_active"))],
        ["Ingestion flows", _num(group.get("ingestion_flow_count"))],
        ["Transformation flows", _num(group.get("transformation_flow_count"))],
        ["Reconciliation flows", _num(group.get("reconciliation_flow_count"))],
        ["Total flows", _num(group.get("total_flow_count"))],
        ["Source connector types", _fmt(group.get("source_types"))],
        ["Target materialisations", _fmt(group.get("target_types"))],
        ["Load strategies", _fmt(group.get("cdc_load_strategies"))],
        ["Onboarded by", _fmt(group.get("onboarded_by"))],
        ["Last onboarded", _fmt(group.get("last_onboarded_at"))],
        ["Spec version", _fmt(group.get("spec_version"))],
    ])

    # ---------------------------------------------------------- capabilities
    L += ["## 2. Framework capabilities exercised", ""]
    caps = [
        ("Change data capture", group.get("has_cdc"),
         "Uses an SCD or snapshot load strategy rather than plain append."),
        ("Data quality expectations", group.get("has_dq"),
         "Declarative expectations are evaluated on write."),
        ("Quarantine", group.get("has_quarantine"),
         "Records failing an expectation are routed to a quarantine table rather than dropped."),
        ("Reconciliation", group.get("has_reconciliation"),
         "Source and target are compared and every record classified."),
        ("Governance tags", group.get("has_governance_tags"),
         "Unity Catalog tags are applied to the target objects."),
    ]
    # Only explain a capability that is actually ON. Printing "Records failing an expectation
    # are routed to a quarantine table" on a row whose Enabled column says "no" reads as a
    # contradiction, and a reader skimming the table takes the prose over the flag.
    L += _table(["Capability", "Enabled", "What it means here"],
                [[name, _fmt(flag), blurb if flag else "Not used by this group."]
                 for name, flag, blurb in caps])

    # ----------------------------------------------------------------- flows
    L += ["## 3. Flows", ""]
    by_kind: Dict[str, List[Dict[str, Any]]] = {}
    for f in flows:
        by_kind.setdefault(f.get("flow_kind", "OTHER"), []).append(f)
    ordered = [k for k in FLOW_KIND_ORDER if k in by_kind]
    ordered += sorted(k for k in by_kind if k not in FLOW_KIND_ORDER)

    if not ordered:
        L += ["_No flows are recorded for this group._", ""]
    for kind in ordered:
        rows = by_kind[kind]
        L += [f"### 3.{ordered.index(kind) + 1} {kind.title()} flows ({len(rows)})", ""]
        blurb = FLOW_KIND_BLURB.get(kind)
        if blurb:
            L += [blurb, ""]
        L += _table(
            ["Flow ID", "Source", "Target table", "Target type", "Load strategy",
             "DQ", "Quarantine", "UC tags", "Active"],
            [[
                f"`{_fmt(f.get('flow_id'))}`",
                _fmt(f.get("source_description")),
                f"`{_fmt(f.get('target_table'))}`" if f.get("target_table") else "-",
                _fmt(f.get("target_type")),
                _fmt(f.get("cdc_load_strategy")),
                _fmt(f.get("has_dq")),
                _fmt(f.get("has_quarantine")),
                _fmt(f.get("has_governance_tags")),
                _fmt(f.get("is_active")),
            ] for f in sorted(rows, key=lambda r: str(r.get("flow_id") or ""))])

    # ---------------------------------------------------------- data quality
    L += ["## 4. Data quality", ""]
    if not dq:
        if group.get("has_dq"):
            L += ["Expectations are declared for this group, but no evaluation has been "
                  "observed in the reporting window. That means the pipeline has not run since "
                  "the rules were onboarded, or its event log is not published to Unity "
                  "Catalog.", ""]
        else:
            L += ["No data-quality expectations are declared for this group.", ""]
    else:
        L += ["Each row is one expectation, with the records it evaluated in the reporting "
              "window. A non-zero failure count is a real data-quality problem, not a warning.",
              ""]
        L += _table(["Dataset", "Rule", "Passed", "Failed", "Pass rate"],
                    [[
                        f"`{_fmt(r.get('dataset_name'))}`",
                        f"`{_fmt(r.get('rule_name'))}`",
                        _num(r.get("passed_records")),
                        _num(r.get("failed_records")),
                        f"{_num(r.get('pass_rate_pct'))}%" if r.get("pass_rate_pct") is not None
                        else "-",
                    ] for r in dq])

    # -------------------------------------------------------- reconciliation
    L += ["## 5. Reconciliation", ""]
    if not recon:
        L += ["No reconciliation flows are configured for this group.", ""]
    else:
        L += ["Reconciliation compares a source against each target and classifies every "
              "record. Note the **Runs logged** column: reconciliation logging is optional, so "
              "an absence of rows is *not* evidence that the comparison was clean.", ""]
        L += _table(["Reconciliation", "Target", "Status", "Runs logged", "Source rows",
                     "Target rows", "Matched", "Missing in target", "Missing in source",
                     "Value drift", "Match rate"],
                    [[
                        f"`{_fmt(r.get('reconciliation_id'))}`",
                        _fmt(r.get("target_id")),
                        _fmt(r.get("status")),
                        _fmt(r.get("has_run_history")),
                        _num(r.get("source_record_count")),
                        _num(r.get("target_record_count")),
                        _num(r.get("matched_count")),
                        _num(r.get("missing_in_target_count")),
                        _num(r.get("missing_in_source_count")),
                        _num(r.get("value_drift_count")),
                        f"{_num(r.get('match_rate_pct'))}%"
                        if r.get("match_rate_pct") is not None else "-",
                    ] for r in recon])

    # ------------------------------------------------------------ operations
    L += ["## 6. Operational behaviour", ""]
    if not health or health.get("total_updates") in (None, 0, "0"):
        L += ["No pipeline updates were observed for this group in the reporting window.", ""]
    else:
        status = str(health.get("health_status") or "")
        L += [f"**Health: {status}** -- {_HEALTH_BLURB.get(status, 'No verdict available.')}",
              ""]
        L += _table(["Metric", "Value", "Note"], [
            ["Pipeline", f"`{_fmt(health.get('pipeline_name'))}`",
             "The Lakeflow pipeline implementing this group"],
            ["Updates", _num(health.get("total_updates")), "Executions in the window"],
            ["Succeeded", _num(health.get("successful_updates")), ""],
            ["Failed", _num(health.get("failed_updates")), ""],
            ["Automatic retries", _num(health.get("retry_updates")),
             "A high count means a retry loop, not throughput"],
            ["Success rate",
             f"{_num(health.get('success_rate_pct'))}%"
             if health.get("success_rate_pct") is not None else "-", ""],
            ["Average duration",
             f"{_num(health.get('avg_duration_minutes'))} min",
             "Full refreshes excluded -- they are legitimately slower"],
            ["P95 duration", f"{_num(health.get('p95_duration_minutes'))} min",
             "Reflects the user-visible worst case, unlike the mean"],
            ["Rows written", _num(health.get("total_rows_written")),
             "Across every flow in the window"],
            ["DQ failures", _num(health.get("dq_failed_records")), ""],
            ["Reconciliation discrepancies", _num(health.get("recon_discrepancies")), ""],
            ["Estimated cost",
             f"${_num(health.get('estimated_cost_usd_30d'))}",
             "List price: excludes negotiated discounts and commitments"],
            ["DBUs", _num(health.get("dbus_30d")), ""],
            ["Last update", _fmt(health.get("last_update_at")),
             _fmt(health.get("last_update_state"))],
        ])

    # --------------------------------------------------------------- lineage
    L += ["## 7. Observed lineage", ""]
    if not lineage:
        L += ["No table-to-table lineage has been observed for this group. Unity Catalog "
              "records lineage only once the pipeline has actually read and written tables.",
              ""]
    else:
        L += ["What the pipeline *actually* read and wrote, from Unity Catalog -- as opposed to "
              "what the spec declares. An edge here that the spec does not mention is worth "
              "investigating.", ""]
        L += _table(["Source table", "Target table", "Written by", "Last seen"],
                    [[
                        f"`{_fmt(e.get('source_table'))}`",
                        f"`{_fmt(e.get('target_table'))}`",
                        _fmt(e.get("entity_name")),
                        _fmt(e.get("last_seen")),
                    ] for e in lineage])

    # ----------------------------------------------------------- target list
    targets = group.get("target_tables")
    if targets:
        L += ["## 8. Tables this group publishes", ""]
        for t in [x.strip() for x in str(targets).split(",") if x.strip()]:
            L.append(f"- `{t}`")
        L.append("")

    L += ["---", "",
          "Generated by `observability/dataflow_documenter.py` from the "
          "`<catalog>.observability` views. Ask the FlowX Genie space the same questions "
          "conversationally.", ""]
    return "\n".join(L)


def render_index(groups: List[Dict[str, Any]],
                 health_by_group: Optional[Dict[str, Dict[str, Any]]] = None,
                 generated_at: Optional[str] = None) -> str:
    """Render the index page listing every documented dataflow group."""
    health_by_group = health_by_group or {}
    L = ["# FlowX Dataflow Group Documentation", ""]
    if generated_at:
        L += [f"_Generated {generated_at} from the FlowX control metadata._", ""]
    L += ["One document per dataflow group, generated from the control tables and the "
          "Databricks system tables. Each describes the group's purpose, its flows, the "
          "framework capabilities it exercises, its data-quality and reconciliation posture, "
          "and how it has actually been running.", ""]
    rows = []
    for g in sorted(groups, key=lambda r: str(r.get("dataflow_group_id") or "")):
        gid = str(g.get("dataflow_group_id"))
        h = health_by_group.get(gid, {})
        rows.append([
            f"[`{gid}`](./{gid}.md)",
            _fmt(g.get("environment")),
            _fmt(h.get("health_status")),
            _num(g.get("total_flow_count")),
            _fmt(g.get("cdc_load_strategies")),
            _fmt(g.get("has_dq")),
            _fmt(g.get("has_reconciliation")),
            _num(h.get("total_rows_written")),
        ])
    L += _table(["Dataflow group", "Env", "Health", "Flows", "Load strategies", "DQ",
                 "Recon", "Rows (30d)"], rows)
    return "\n".join(L)
