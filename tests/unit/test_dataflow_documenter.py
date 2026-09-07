"""Unit tests for ``observability/dataflow_documenter.py`` -- pure rendering, no Spark.

The documenter is the batch counterpart to the Genie space: it turns control metadata into a
Markdown design document a customer or a new engineer can read. Two properties matter more than
the exact wording:

* **It must never overstate confidence.** Several of the underlying signals are optional
  (reconciliation logging can be disabled, event logs may not be published, system tables may
  not be readable). A document that renders "clean" where it actually means "nothing was
  measured" is worse than no document, so the absent-data paths are tested as carefully as the
  populated ones.
* **It must render at all, on any subset of inputs.** The notebook treats every run-history read
  as optional and passes whatever it got, so each argument has to be independently omissible
  without raising.

Deliberately no Spark and no workspace: the module takes lists of plain dicts by design,
precisely so this suite runs in the repo's offline, no-local-Spark environment.
"""

import pytest

from flowx.lakeflow_framework.observability.dataflow_documenter import (
    FLOW_KIND_ORDER,
    render_group_document,
    render_index,
)

GROUP = {
    "dataflow_group_id": "dfg_uc6_ea_flood_warning",
    "environment": "metaflow_v7",
    "catalog_name": "flowx",
    "is_active": True,
    "ingestion_flow_count": 6,
    "transformation_flow_count": 10,
    "reconciliation_flow_count": 6,
    "total_flow_count": 22,
    "source_types": "autoloader",
    "target_types": "materialized_view, sink, streaming_table",
    "cdc_load_strategies": "APPEND, TRUNCATE_AND_LOAD",
    "target_tables": "flowx.bronze.uc6_css_account, flowx.gold.uc6_osapr_output",
    "has_cdc": True,
    "has_dq": True,
    "has_quarantine": False,
    "has_reconciliation": True,
    "has_governance_tags": True,
    "feature_summary": "Dataflow group dfg_uc6_ea_flood_warning defines 22 flow(s).",
    "onboarded_by": "someone@example.com",
    "last_onboarded_at": "2026-09-06T05:27:59.981Z",
    "spec_version": "3bbb5ede8e34df7b",
}

FLOWS = [
    {"dataflow_group_id": GROUP["dataflow_group_id"], "flow_kind": "INGESTION",
     "flow_id": "df_uc6_css_account", "source_description": "source_type=autoloader",
     "target_table": "flowx.bronze.uc6_css_account", "target_type": "streaming_table",
     "cdc_load_strategy": "APPEND", "has_dq": True, "has_quarantine": False,
     "has_governance_tags": True, "is_active": True},
    {"dataflow_group_id": GROUP["dataflow_group_id"], "flow_kind": "TRANSFORMATION",
     "flow_id": "step_uc6_osapr", "source_description": "inputs=[...]",
     "target_table": "flowx.gold.uc6_osapr_output", "target_type": "materialized_view",
     "cdc_load_strategy": "TRUNCATE_AND_LOAD", "has_dq": True, "has_quarantine": False,
     "has_governance_tags": True, "is_active": True},
    {"dataflow_group_id": GROUP["dataflow_group_id"], "flow_kind": "RECONCILIATION",
     "flow_id": "rf_uc6_css_address", "source_description": "compare={...}",
     "target_table": None, "target_type": "pipeline",
     "cdc_load_strategy": "reconciliation", "has_dq": False, "has_quarantine": False,
     "has_governance_tags": False, "is_active": True},
]

HEALTH = {
    "dataflow_group_id": GROUP["dataflow_group_id"],
    "health_status": "DEGRADED",
    "pipeline_name": "008_ldp_uc6_ea_flood_warning",
    "total_updates": 44,
    "successful_updates": 5,
    "failed_updates": 38,
    "retry_updates": 33,
    "success_rate_pct": 11.63,
    "avg_duration_minutes": 3.04,
    "p95_duration_minutes": 6.62,
    "total_rows_written": 1816,
    "dq_failed_records": 0,
    "recon_discrepancies": 0,
    "estimated_cost_usd_30d": 3.85,
    "dbus_30d": 11.01,
    "last_update_at": "2026-09-06T05:28:08.186Z",
    "last_update_state": "COMPLETED",
}

DQ = [{"dataflow_group_id": GROUP["dataflow_group_id"], "dataset_name": "uc6_osapr_output",
       "rule_name": "uc6_osapr_status_known", "passed_records": 7, "failed_records": 0,
       "pass_rate_pct": 100.0}]

RECON = [{"dataflow_group_id": GROUP["dataflow_group_id"],
          "reconciliation_id": "rf_uc6_css_address", "target_id": "css_acc",
          "status": "SUCCESS", "has_run_history": True, "source_record_count": 10,
          "target_record_count": 10, "matched_count": 10, "missing_in_target_count": 0,
          "missing_in_source_count": 0, "value_drift_count": 0, "match_rate_pct": 100.0}]

LINEAGE = [{"dataflow_group_id": GROUP["dataflow_group_id"],
            "source_table": "flowx.bronze.uc6_css_account",
            "target_table": "flowx.gold.uc6_osapr_output",
            "entity_name": "008_ldp_uc6_ea_flood_warning",
            "last_seen": "2026-09-06T05:30:38.478Z"}]


def full_doc():
    return render_group_document(GROUP, FLOWS, HEALTH, DQ, RECON, LINEAGE,
                                 generated_at="2026-09-07 13:45 UTC")


# ------------------------------------------------------------------------------ structure


def test_document_has_every_numbered_section_in_order():
    doc = full_doc()
    headings = ["## 1. Purpose and scale",
                "## 2. Framework capabilities exercised",
                "## 3. Flows",
                "## 4. Data quality",
                "## 5. Reconciliation",
                "## 6. Operational behaviour",
                "## 7. Observed lineage"]
    positions = [doc.index(h) for h in headings]
    assert positions == sorted(positions), "sections are out of order"


def test_document_titles_and_dates_itself():
    doc = full_doc()
    assert doc.startswith(f"# Dataflow Group: `{GROUP['dataflow_group_id']}`")
    assert "2026-09-07 13:45 UTC" in doc
    # A generated file that does not say so invites hand-edits that the next run destroys.
    assert "Do not hand-edit" in doc


def test_flow_kind_order_puts_ingestion_before_transformation_before_reconciliation():
    # The reader needs the order data actually moves in, not alphabetical.
    assert FLOW_KIND_ORDER == ["INGESTION", "TRANSFORMATION", "RECONCILIATION"]
    doc = full_doc()
    assert doc.index("Ingestion flows") < doc.index("Transformation flows")
    assert doc.index("Transformation flows") < doc.index("Reconciliation flows")


def test_every_flow_appears_with_its_target_and_strategy():
    doc = full_doc()
    for flow in FLOWS:
        assert flow["flow_id"] in doc
    assert "flowx.gold.uc6_osapr_output" in doc
    assert "TRUNCATE_AND_LOAD" in doc


def test_feature_summary_is_quoted_verbatim():
    # The Genie space is instructed to quote this same sentence, so the two surfaces agree.
    assert GROUP["feature_summary"] in full_doc()


def test_published_tables_are_listed_individually():
    doc = full_doc()
    assert "## 8. Tables this group publishes" in doc
    assert "- `flowx.bronze.uc6_css_account`" in doc
    assert "- `flowx.gold.uc6_osapr_output`" in doc


# --------------------------------------------------------------------- honest about absence


def test_capabilities_that_are_off_do_not_carry_an_explanation_of_being_on():
    """A row reading "no" beside "records are routed to a quarantine table" contradicts itself.

    A reader skimming the table takes the prose over the flag, so the prose must not describe
    behaviour the group does not have.
    """
    doc = full_doc()
    quarantine_row = [ln for ln in doc.split("\n") if ln.startswith("| Quarantine ")][0]
    assert "| no |" in quarantine_row
    assert "Not used by this group." in quarantine_row
    assert "routed to a quarantine table" not in quarantine_row


def test_declared_but_unmeasured_dq_is_reported_as_unobserved_not_as_absent():
    """has_dq is true but no evaluations came back: that is "not yet measured", not "no rules"."""
    doc = render_group_document(GROUP, FLOWS, HEALTH, dq=[], recon=RECON, lineage=LINEAGE)
    assert "no evaluation has been observed" in doc
    assert "No data-quality expectations are declared" not in doc


def test_group_with_no_dq_declared_says_so_plainly():
    group = dict(GROUP, has_dq=False)
    doc = render_group_document(group, FLOWS, HEALTH, dq=[], recon=[], lineage=[])
    assert "No data-quality expectations are declared" in doc


def test_reconciliation_section_surfaces_whether_runs_were_actually_logged():
    """Reconciliation logging is optional, so an empty log is not a clean comparison."""
    doc = full_doc()
    recon_section = doc[doc.index("## 5. Reconciliation"):doc.index("## 6.")]
    assert "Runs logged" in recon_section
    assert "not* evidence" in recon_section or "not evidence" in recon_section


def test_unrun_reconciliation_is_not_rendered_as_healthy():
    unrun = [dict(RECON[0], has_run_history=False, status=None, matched_count=None,
                  match_rate_pct=None, source_record_count=None, target_record_count=None,
                  missing_in_target_count=None, missing_in_source_count=None,
                  value_drift_count=None)]
    doc = render_group_document(GROUP, FLOWS, HEALTH, DQ, unrun, LINEAGE)
    recon_section = doc[doc.index("## 5. Reconciliation"):doc.index("## 6.")]
    assert "| no |" in recon_section
    assert "100%" not in recon_section


def test_no_run_history_reports_nothing_observed_rather_than_zero_health():
    doc = render_group_document(GROUP, FLOWS, health=None, dq=[], recon=[], lineage=[])
    assert "No pipeline updates were observed" in doc
    # Must not imply a verdict it cannot support.
    assert "**Health: HEALTHY**" not in doc


def test_zero_update_health_row_is_treated_the_same_as_no_health_row():
    doc = render_group_document(GROUP, FLOWS, health=dict(HEALTH, total_updates=0),
                                dq=[], recon=[], lineage=[])
    assert "No pipeline updates were observed" in doc


def test_absent_lineage_explains_why_rather_than_showing_an_empty_table():
    doc = render_group_document(GROUP, FLOWS, HEALTH, DQ, RECON, lineage=[])
    assert "No table-to-table lineage has been observed" in doc


# ---------------------------------------------------------------------- caveats preserved


def test_cost_is_labelled_as_an_estimate_at_list_price():
    doc = full_doc()
    assert "$3.85" in doc
    assert "List price" in doc


def test_retry_count_is_annotated_as_instability_not_throughput():
    doc = full_doc()
    retry_row = [ln for ln in doc.split("\n") if ln.startswith("| Automatic retries ")][0]
    assert "retry loop, not throughput" in retry_row


def test_duration_statistics_state_that_full_refreshes_are_excluded():
    doc = full_doc()
    assert "Full refreshes excluded" in doc
    assert "P95" in doc


def test_health_verdict_is_explained_in_words():
    doc = full_doc()
    assert "**Health: DEGRADED**" in doc
    assert "at least one failure" in doc


@pytest.mark.parametrize("status,phrase", [
    ("HEALTHY", "All recent updates succeeded"),
    ("FAILING", "most recent pipeline update failed"),
    ("INACTIVE", "soft-disabled"),
    ("NO_RUNS", "No pipeline updates were observed in the window"),
])
def test_every_health_verdict_has_a_plain_language_gloss(status, phrase):
    doc = render_group_document(GROUP, FLOWS, dict(HEALTH, health_status=status),
                                DQ, RECON, LINEAGE)
    assert phrase in doc


# ------------------------------------------------------------------------- robustness


def test_renders_with_only_the_two_required_arguments():
    # The notebook treats every run-history read as optional, so this is a real call shape.
    doc = render_group_document(GROUP, FLOWS)
    assert doc.startswith("# Dataflow Group:")
    assert "## 7. Observed lineage" in doc


def test_renders_with_no_flows_at_all():
    doc = render_group_document(dict(GROUP, total_flow_count=0), [])
    assert "No flows are recorded for this group" in doc


def test_pipe_characters_in_values_are_escaped_so_tables_do_not_break():
    """An unescaped pipe in a value silently destroys the enclosing Markdown table."""
    flows = [dict(FLOWS[0], source_description="a|b|c")]
    doc = render_group_document(GROUP, flows)
    assert r"a\|b\|c" in doc


def test_empty_json_object_renders_as_a_dash_not_as_content():
    flows = [dict(FLOWS[0], source_description="{}")]
    doc = render_group_document(GROUP, flows)
    row = [ln for ln in doc.split("\n") if FLOWS[0]["flow_id"] in ln][0]
    assert "{}" not in row


def test_large_numbers_are_thousands_separated():
    doc = render_group_document(GROUP, FLOWS,
                                dict(HEALTH, total_rows_written=526494), DQ, RECON, LINEAGE)
    assert "526,494" in doc


def test_unknown_flow_kind_is_still_rendered():
    # A future flow kind must not vanish from the document just because it is unrecognised.
    flows = FLOWS + [dict(FLOWS[0], flow_kind="EXPORT", flow_id="ex_1")]
    doc = render_group_document(GROUP, flows)
    assert "ex_1" in doc
    assert "Export flows" in doc


# ------------------------------------------------------------------------------- index


def test_index_links_every_group_to_its_own_document():
    idx = render_index([GROUP], {GROUP["dataflow_group_id"]: HEALTH},
                       generated_at="2026-09-07 13:45 UTC")
    gid = GROUP["dataflow_group_id"]
    assert f"[`{gid}`](./{gid}.md)" in idx
    assert "DEGRADED" in idx
    assert "2026-09-07 13:45 UTC" in idx


def test_index_renders_without_any_health_data():
    idx = render_index([GROUP])
    assert GROUP["dataflow_group_id"] in idx


def test_index_sorts_groups_by_id_for_a_stable_diff():
    a = dict(GROUP, dataflow_group_id="dfg_a")
    z = dict(GROUP, dataflow_group_id="dfg_z")
    idx = render_index([z, a])
    assert idx.index("dfg_a") < idx.index("dfg_z")
