"""``onboarding/spec_pruning.py`` -- the opt-in deactivation of control rows a re-onboarded spec no
longer declares. Pure-SQL-builder tests plus an execution test over a recording fake Spark; no real
Spark session is touched."""

import pytest

from flowx.lakeflow_framework.onboarding.spec_pruning import (
    FLOW_TABLES,
    build_flow_deactivation_statements,
    deactivate_flows_absent_from_spec,
)


def test_one_statement_pair_per_flow_table():
    statements = build_flow_deactivation_statements("flowx.config", "dfg_x", ["a"], ["b"], ["c"])
    assert [s[0] for s in statements] == [table for table, _ in FLOW_TABLES]
    for _table, count_sql, update_sql in statements:
        assert count_sql.startswith("SELECT count(*) AS n FROM flowx.config.")
        assert update_sql.startswith("UPDATE flowx.config.")
        assert "SET is_active = false, updated_at = current_timestamp()" in update_sql
        assert "dataflow_group_id = 'dfg_x' AND is_active = true" in update_sql


def test_declared_ids_are_kept_and_everything_else_of_the_group_is_pruned():
    statements = dict((t, u) for t, _c, u in build_flow_deactivation_statements(
        "flowx.config", "dfg_x", ["df_keep_1", "df_keep_2"], ["ts_keep"], ["rf_keep"]
    ))
    assert "dataflow_id NOT IN ('df_keep_1', 'df_keep_2')" in statements["ingestion_flow_spec"]
    assert "flow_step_id NOT IN ('ts_keep')" in statements["transformation_flow_spec"]
    assert "reconciliation_id NOT IN ('rf_keep')" in statements["reconciliation_flow_spec"]


def test_an_empty_kind_deactivates_every_active_row_of_that_kind():
    """UC6 on 2026-09-08: reconciliation_flows removed entirely -- the six rows must all go."""
    statements = dict((t, u) for t, _c, u in build_flow_deactivation_statements(
        "flowx.config", "dfg_uc6_ea_flood_warning", ["df_1"], ["ts_1"], []
    ))
    recon = statements["reconciliation_flow_spec"]
    assert "NOT IN" not in recon
    assert recon.endswith("WHERE dataflow_group_id = 'dfg_uc6_ea_flood_warning' AND is_active = true")


def test_ids_and_group_are_sql_escaped():
    statements = build_flow_deactivation_statements("flowx.config", "o'grady", ["it's"], [], [])
    _table, _count, update_sql = statements[0]
    assert "dataflow_group_id = 'o''grady'" in update_sql
    assert "dataflow_id NOT IN ('it''s')" in update_sql


def test_unsafe_control_schema_is_rejected_before_any_sql_is_built():
    with pytest.raises(Exception):
        build_flow_deactivation_statements("flowx.config; DROP TABLE x", "dfg_x", [], [], [])
    with pytest.raises(ValueError):
        build_flow_deactivation_statements("config", "dfg_x", [], [], [])


class _FakeSpark:
    """Answers every count with the value under the table name and records the UPDATEs run."""

    def __init__(self, counts):
        self.counts = counts
        self.executed = []

    def sql(self, statement):
        self.executed.append(statement)
        if statement.startswith("SELECT count(*)"):
            table = next(t for t in self.counts if f".{t} " in statement)

            class _Result:
                def collect(_self):
                    return [(self.counts[table],)]

            return _Result()

        class _Empty:
            def collect(_self):
                return []

        return _Empty()


def test_deactivation_runs_an_update_only_where_stale_rows_exist():
    spark = _FakeSpark({"ingestion_flow_spec": 0, "transformation_flow_spec": 0, "reconciliation_flow_spec": 6})
    deactivated = deactivate_flows_absent_from_spec(
        spark,
        "flowx.config",
        "dfg_uc6_ea_flood_warning",
        ingestion_flows=[{"dataflow_id": "df_uc6_ea_request_ingest"}],
        transformation_flows=[{"flow_step_id": "ts_uc6_source_presence_gate"}],
        reconciliation_flows=[],
    )
    assert deactivated == {"ingestion_flow_spec": 0, "transformation_flow_spec": 0, "reconciliation_flow_spec": 6}
    updates = [s for s in spark.executed if s.startswith("UPDATE")]
    assert len(updates) == 1 and "reconciliation_flow_spec" in updates[0], updates
