"""Unit tests for ``control_plane/observability_views.py`` -- pure string building, no Spark.

These views are the semantic layer both the AI/BI observability dashboard and the Genie space
read, so a silent change in one of them desynchronises two customer-facing surfaces at once.
The assertions below fall into four groups:

1. **Structure** -- every view is emitted, in dependency order, with the schema it was asked for.
2. **The join key** -- ``configuration['dataflow.group.id']`` is the single thing that connects
   FlowX control metadata to the Databricks system tables. If it regresses, every run, cost and
   metric figure silently detaches from its dataflow group.
3. **The bugs that live data actually caught.** Four of these were real defects found by running
   the views against a live workspace, and each one produced a *plausible-looking* wrong answer
   rather than an error -- exactly the kind that survives review. They are pinned here so they
   cannot come back.
4. **The brace-escaping trap** -- ``ddl_definitions.py``'s long-standing hazard applies equally
   here: an unescaped ``{`` in an f-string DDL body is evaluated as a replacement field. A test
   that merely imports the module would catch a syntax error, but not a ``{}`` that happens to
   name a real variable, so the emitted SQL is checked directly.
"""

import pytest

from flowx.lakeflow_framework.control_plane.observability_views import (
    OBSERVABILITY_SCHEMA_SUFFIX,
    get_all_observability_view_ddls,
    get_cost_view_ddl,
    get_dataflow_group_catalog_view_ddl,
    get_dq_results_view_ddl,
    get_flow_inventory_view_ddl,
    get_flow_metrics_view_ddl,
    get_group_health_summary_view_ddl,
    get_job_runs_view_ddl,
    get_lineage_view_ddl,
    get_observability_schema_ddl,
    get_pipeline_registry_view_ddl,
    get_pipeline_updates_view_ddl,
    get_reconciliation_health_view_ddl,
    get_deployment_versions_view_ddl,
    get_installed_framework_version,
)

OBS = "poc.observability"
CTL = "poc.config"
EVENT_LOGS = ["poc.bronze.event_log_aaa_bbb", "poc.bronze.event_log_ccc_ddd"]

#: Every view the layer is contracted to create, in the order dependencies require.
EXPECTED_VIEWS = [
    "v_dataflow_group_catalog",
    "v_flow_inventory",
    "v_pipeline_registry",
    "v_pipeline_updates",
    "v_job_runs",
    "v_dataflow_cost",
    "v_flow_metrics",
    "v_dq_results",
    "v_reconciliation_health",
    "v_dataflow_lineage",
    "v_group_health_summary",
    "v_deployment_versions",
]


def all_ddls(event_log_tables=EVENT_LOGS):
    return get_all_observability_view_ddls(OBS, CTL, event_log_tables)


# --------------------------------------------------------------------------- structure


def test_schema_ddl_is_idempotent_and_targets_requested_schema():
    ddl = get_observability_schema_ddl(OBS)
    assert f"CREATE SCHEMA IF NOT EXISTS {OBS}" in ddl


def test_observability_schema_suffix_is_stable():
    # resources/flowx_bi/flowx_observability_dashboard.yml hardcodes `dataset_schema:
    # observability` and the Genie space's table identifiers spell it too, so renaming this
    # constant alone would leave both pointing at a schema that no longer exists.
    assert OBSERVABILITY_SCHEMA_SUFFIX == "observability"


def test_all_view_ddls_emits_every_expected_view():
    names = [name for name, _ in all_ddls()]
    for view in EXPECTED_VIEWS:
        assert f"create view {view}" in names, f"{view} is not emitted"
    assert len(names) == len(EXPECTED_VIEWS)


def test_all_view_ddls_are_create_or_replace_in_the_requested_schema():
    for description, ddl in all_ddls():
        view = description.replace("create view ", "")
        assert f"CREATE OR REPLACE VIEW {OBS}.{view}" in ddl, description


def test_registry_precedes_every_view_that_joins_to_it():
    # The setup notebook executes these in list order against a live warehouse, where a forward
    # reference is a hard TABLE_OR_VIEW_NOT_FOUND rather than a lazily-resolved one.
    order = [name for name, _ in all_ddls()]
    registry = order.index("create view v_pipeline_registry")
    for dependent in ("v_pipeline_updates", "v_dataflow_cost", "v_flow_metrics",
                      "v_dq_results", "v_dataflow_lineage"):
        assert order.index(f"create view {dependent}") > registry, dependent


def test_group_health_summary_precedes_only_deployment_versions():
    # v_group_health_summary is a view over five of the others, so it must come after all of
    # them. It is no longer LAST: v_deployment_versions joins v_dataflow_group_catalog and
    # v_pipeline_registry, so it is created after the summary. Both are terminal -- nothing
    # joins to either -- so their relative order is free, but each must follow its own inputs.
    order = [name for name, _ in all_ddls()]
    summary = order.index("create view v_group_health_summary")
    for upstream in ("v_pipeline_updates", "v_flow_metrics", "v_dq_results",
                     "v_dataflow_cost", "v_reconciliation_health"):
        assert order.index(f"create view {upstream}") < summary, upstream
    assert order[-1] == "create view v_deployment_versions"


def test_deployment_versions_follows_the_views_it_joins():
    order = [name for name, _ in all_ddls()]
    dv = order.index("create view v_deployment_versions")
    for upstream in ("v_dataflow_group_catalog", "v_pipeline_registry"):
        assert order.index(f"create view {upstream}") < dv, upstream


# ------------------------------------------------------------------------- the join key


def test_pipeline_registry_joins_on_the_dataflow_group_id_configuration_key():
    ddl = get_pipeline_registry_view_ddl(OBS, CTL)
    assert "configuration['dataflow.group.id']" in ddl
    assert "system.lakeflow.pipelines" in ddl
    # A deleted pipeline still has a row; including it double-counts every historical group.
    assert "delete_time IS NULL" in ddl


def test_pipeline_registry_discovers_event_log_tables_rather_than_constructing_the_name():
    """``settings`` exposes no catalog/target, so the publish location is not derivable.

    Verified against a live workspace: ``system.lakeflow.pipelines.settings`` is
    ``struct<photon, development, continuous, serverless, edition, channel>``. An earlier
    version read ``settings.catalog``/``settings.target`` and failed with FIELD_NOT_FOUND,
    taking every dependent view down with it.
    """
    ddl = get_pipeline_registry_view_ddl(OBS, CTL)
    assert "system.information_schema.tables" in ddl
    assert "event_log_" in ddl
    assert "settings.catalog" not in ddl
    assert "settings.target" not in ddl


def test_job_runs_reports_attribution_quality_instead_of_blending_it():
    """Job attribution is tag-exact or name-heuristic, and a reader must be able to tell.

    Notebook ``base_parameters`` do not surface in
    ``job_task_run_timeline.task_parameters`` (verified empty on a live workspace), so a tag is
    the only exact key. Presenting a name match as though it were exact would misattribute
    cost and run counts with no signal at all.
    """
    ddl = get_job_runs_view_ddl(OBS, CTL)
    assert "tags['dataflow_group_id']" in ddl
    assert "'tag'" in ddl and "'name_match'" in ddl
    # An unattributable run is still returned, so framework jobs predating the tag stay visible.
    assert "LEFT JOIN job_attr" in ddl


def test_cost_view_attributes_pipeline_spend_by_pipeline_id():
    ddl = get_cost_view_ddl(OBS)
    assert "usage_metadata.dlt_pipeline_id" in ddl
    assert "usage_metadata.job_id" in ddl
    # Only the price row still in effect; otherwise every historical price multiplies the spend.
    assert "price_end_time IS NULL" in ddl
    assert "pricing.effective_list.default" in ddl


# ------------------------------------------ regressions that live data actually caught


def test_has_cdc_lowercases_before_matching_strategy_names():
    """The control tables store strategies UPPERCASE (SCD1, SCD2, TRUNCATE_AND_LOAD).

    A lowercase-only pattern reported ``has_cdc = false`` for every SCD group -- a wrong answer
    that looked entirely plausible on a dashboard.
    """
    ddl = get_dataflow_group_catalog_view_ddl(OBS, CTL)
    assert "LOWER(cdc_load_strategies) RLIKE" in ddl


def test_aggregated_lists_filter_empty_strings_not_just_nulls():
    """``SPLIT('', ', ')`` returns ``['']`` -- one EMPTY element, which ARRAY_COMPACT keeps.

    ARRAY_COMPACT removes NULLs only, so the empty element survived, sorted first, and
    CONCAT_WS rendered it as a leading ``", "`` on target_types, cdc_load_strategies and
    target_tables.
    """
    ddl = get_dataflow_group_catalog_view_ddl(OBS, CTL)
    # Assert on the executable form, not the whole text: the SQL comment above the fix names
    # ARRAY_COMPACT while explaining why it is the wrong function here.
    assert "ARRAY_COMPACT(FLATTEN(" not in ddl, "ARRAY_COMPACT does not drop empty strings"
    assert ddl.count("FILTER(FLATTEN(") == 3
    assert ddl.count("x -> x IS NOT NULL AND x <> ''") == 3


@pytest.mark.parametrize(
    "ddl_factory",
    [
        lambda: get_dataflow_group_catalog_view_ddl(OBS, CTL),
        lambda: get_flow_inventory_view_ddl(OBS, CTL),
    ],
)
def test_empty_json_object_does_not_count_as_configured_dq(ddl_factory):
    """``dq_config_json`` is literally ``'{}'`` on many onboarded flows.

    Testing only for a non-empty string reported ``has_dq = true`` for every group, including
    ones with no expectations at all.
    """
    ddl = ddl_factory()
    assert "TRIM(dq_config_json) NOT IN ('', '{}')" in ddl
    # And quarantine detection must be case-insensitive for the same reason as has_cdc.
    assert "LOWER(dq_config_json) LIKE '%quarantine%'" in ddl


def test_flow_metrics_prefers_a_terminal_event_over_the_newest_one():
    """A streaming flow can emit its last progress event mid-batch while still RUNNING.

    Ordering by ``event_time DESC`` alone reported a partial ``num_output_rows`` and a
    misleading RUNNING status for an update that had in fact completed.
    """
    ddl = get_flow_metrics_view_ddl(OBS, EVENT_LOGS)
    assert "'COMPLETED', 'FAILED', 'EXCLUDED', 'SKIPPED'" in ddl
    assert "ROW_NUMBER() OVER" in ddl


@pytest.mark.parametrize(
    "ddl_factory",
    [
        lambda tables: get_flow_metrics_view_ddl(OBS, tables),
        lambda tables: get_dq_results_view_ddl(OBS, tables),
    ],
)
def test_event_log_views_deduplicate_to_one_event_per_flow_per_update(ddl_factory):
    # Event-log counts are cumulative within an update, so keeping every progress event would
    # multiply both throughput and expectation totals by however many events a flow emitted.
    ddl = ddl_factory(EVENT_LOGS)
    assert "PARTITION BY pipeline_id, update_id, flow_name" in ddl
    assert "WHERE e.rn = 1" in ddl or "rn = 1" in ddl


# ------------------------------------------------------------------- event-log plumbing


@pytest.mark.parametrize(
    "ddl_factory",
    [get_flow_metrics_view_ddl, get_dq_results_view_ddl],
)
def test_event_log_views_read_every_supplied_table(ddl_factory):
    ddl = ddl_factory(OBS, EVENT_LOGS)
    for table in EVENT_LOGS:
        assert f"FROM {table}" in ddl
    assert "UNION ALL" in ddl


@pytest.mark.parametrize(
    "ddl_factory",
    [get_flow_metrics_view_ddl, get_dq_results_view_ddl],
)
def test_event_log_views_still_resolve_with_no_published_event_logs(ddl_factory):
    """A workspace where no pipeline publishes its event log must still get usable views.

    Skipping creation instead would break the dashboard datasets and the Genie space with
    TABLE_OR_VIEW_NOT_FOUND, which is a far worse failure than an empty result.
    """
    ddl = ddl_factory(OBS, [])
    assert "CREATE OR REPLACE VIEW" in ddl
    assert "WHERE FALSE" in ddl
    assert "CAST(NULL AS" in ddl


def test_dq_results_explodes_the_expectations_array_with_the_documented_shape():
    ddl = get_dq_results_view_ddl(OBS, EVENT_LOGS)
    assert "details:flow_progress.data_quality.expectations" in ddl
    assert "passed_records:bigint" in ddl and "failed_records:bigint" in ddl
    assert "EXPLODE(" in ddl


def test_flow_metrics_reads_row_counts_from_the_documented_json_path():
    ddl = get_flow_metrics_view_ddl(OBS, EVENT_LOGS)
    # `details` is a JSON STRING in the event log, so the `details:path` operator is required;
    # struct-style dotted access silently returns nothing.
    assert "details:flow_progress.metrics.num_output_rows" in ddl
    assert "event_type = 'flow_progress'" in ddl


# ------------------------------------------------------------------------ semantics kept


def test_pipeline_updates_derives_duration_because_the_system_table_has_none():
    ddl = get_pipeline_updates_view_ddl(OBS)
    assert "TIMESTAMPDIFF(SECOND, u.period_start_time, u.period_end_time)" in ddl
    # A retry storm must be distinguishable from healthy activity.
    assert "RETRY_ON_FAILURE" in ddl
    assert "is_full_refresh" in ddl


def test_group_health_summary_excludes_full_refreshes_from_duration_statistics():
    # A full refresh is legitimately far slower; averaging it in makes a healthy pipeline look
    # like it regressed on whatever day it was rebuilt.
    ddl = get_group_health_summary_view_ddl(OBS)
    assert "CASE WHEN NOT is_full_refresh THEN duration_minutes END" in ddl
    assert "PERCENTILE_APPROX" in ddl


def test_group_health_summary_orders_health_verdicts_so_inactive_beats_failing():
    ddl = get_group_health_summary_view_ddl(OBS)
    inactive = ddl.index("'INACTIVE'")
    no_runs = ddl.index("'NO_RUNS'")
    failing = ddl.index("'FAILING'")
    degraded = ddl.index("'DEGRADED'")
    # A disabled group must not be reported as FAILING because of runs that predate disabling,
    # and "never ran" is a genuinely different state from "running badly".
    assert inactive < no_runs < failing < degraded


def test_reconciliation_health_flags_absent_run_history_rather_than_implying_health():
    """Reconciliation logging is optional, so no rows is not evidence of a clean comparison."""
    ddl = get_reconciliation_health_view_ddl(OBS, CTL)
    assert "has_run_history" in ddl
    assert "is_clean" in ddl
    assert "LEFT JOIN" in ddl, "an unrun reconciliation must still appear"


def test_flow_inventory_covers_all_three_flow_kinds():
    ddl = get_flow_inventory_view_ddl(OBS, CTL)
    for kind in ("'INGESTION'", "'TRANSFORMATION'", "'RECONCILIATION'"):
        assert kind in ddl
    for table in ("ingestion_flow_spec", "transformation_flow_spec",
                  "reconciliation_flow_spec"):
        assert f"{CTL}.{table}" in ddl
    assert ddl.count("UNION ALL") == 2


def test_lineage_view_reads_the_access_schema():
    ddl = get_lineage_view_ddl(OBS)
    assert "system.access.table_lineage" in ddl


def test_group_catalog_exposes_the_feature_summary_genie_is_told_to_quote():
    # The Genie space instructions say to quote `feature_summary` verbatim, and the generated
    # documentation uses it as the group's purpose statement. Both break if it disappears.
    ddl = get_dataflow_group_catalog_view_ddl(OBS, CTL)
    assert "AS feature_summary" in ddl
    assert "onboarding_audit_log" in ddl


# --------------------------------------------------------------- the brace-escaping trap


def test_no_unrendered_replacement_fields_survive_in_any_ddl():
    """An unescaped ``{`` in an f-string DDL body is evaluated, not emitted.

    ``ddl_definitions.py`` has been broken by exactly this before (an unescaped ``{...}`` in a
    column COMMENT), and this module hit it too while being written. A leftover ``{`` in the
    OUTPUT means a brace that should have been ``{{`` was consumed as a replacement field.
    """
    for description, ddl in all_ddls():
        for placeholder in ("{observability_schema}", "{control_schema}", "{table}",
                            "{expectation_schema}", "{union_sql}"):
            assert placeholder not in ddl, f"{description} leaked {placeholder}"


def test_empty_json_literals_render_as_actual_braces_in_the_sql():
    # The '{}' comparisons must reach the warehouse as literal braces, which means they were
    # written '{{}}' in the source. If they were written bare, Python would have raised at
    # import; if they were over-escaped, the SQL would contain '{{}}' and match nothing.
    ddl = get_dataflow_group_catalog_view_ddl(OBS, CTL)
    assert "'{}'" in ddl
    assert "'{{}}'" not in ddl


def test_ddls_are_parameterised_by_schema_and_leak_no_other_catalog():
    other = get_all_observability_view_ddls("alt.obs", "alt.config", [])
    for description, ddl in other:
        assert "poc." not in ddl, f"{description} hardcodes the poc catalog"
        assert "alt.obs" in ddl or "alt.config" in ddl or "system." in ddl

# ------------------------------------------------------- wheel drift (v_deployment_versions)


def test_deployment_versions_parses_the_wheel_version_from_the_event_log():
    # system.lakeflow exposes no pipeline library path, so the ONLY pure-SQL source for the
    # wheel is the resolved config recorded on each create_update event.
    ddl = get_deployment_versions_view_ddl(OBS, CTL, EVENT_LOGS, "0.0.4")
    assert "event_type = 'create_update'" in ddl
    assert "/wheels/([0-9]+[.][0-9]+[.][0-9]+)/" in ddl
    for table in EVENT_LOGS:
        assert table in ddl


def test_deployment_versions_baseline_is_the_installed_version_when_known():
    ddl = get_deployment_versions_view_ddl(OBS, CTL, EVENT_LOGS, "0.0.4")
    # The installed version must appear as a literal and be preferred over the best observed.
    assert "'0.0.4'" in ddl
    assert "baseline_source" in ddl
    assert "'installed'" in ddl


def test_deployment_versions_falls_back_to_best_observed_and_says_so():
    # A source checkout has no installed distribution. The view must still resolve, and must
    # NOT silently present a best-effort baseline as authoritative.
    ddl = get_deployment_versions_view_ddl(OBS, CTL, EVENT_LOGS, None)
    assert "CAST(NULL AS STRING)" in ddl
    assert "'best_observed'" in ddl


def test_deployment_versions_ranks_versions_numerically_not_as_strings():
    # A string sort puts 0.0.9 above 0.0.10, which would report a newer wheel as older.
    ddl = get_deployment_versions_view_ddl(OBS, CTL, EVENT_LOGS, "0.0.4")
    assert "CAST(split(" in ddl
    assert "AS INT)" in ddl


def test_deployment_versions_reports_untagged_jobs_rather_than_dropping_them():
    ddl = get_deployment_versions_view_ddl(OBS, CTL, EVENT_LOGS, "0.0.4")
    assert "tags['dataflow_group_id']" in ddl
    assert "'none'" in ddl
    # LEFT JOIN, so a group with no tagged job still has a row.
    assert "LEFT JOIN tagged_jobs" in ddl


def test_deployment_versions_survives_a_workspace_with_no_event_logs():
    ddl = get_deployment_versions_view_ddl(OBS, CTL, [], "0.0.4")
    assert "WHERE FALSE" in ddl
    assert f"CREATE OR REPLACE VIEW {OBS}.v_deployment_versions" in ddl


def test_installed_framework_version_is_a_dotted_version_or_none():
    v = get_installed_framework_version()
    assert v is None or v.count(".") == 2, v
