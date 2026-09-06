"""``apply_all_governance_tags`` must skip ``target_type: "sink"`` flows.

A ``"sink"`` flow is a ``dlt.create_sink`` + ``@dlt.append_flow`` with **no persisted dataset**
(``engine/sink_registration.py`` -- "NO materialized main table is ever registered for this
target type"). ``target_table`` on such a flow names the *sink*, not a table. Tagging it issued
``ALTER TABLE flowx.gold.uc6_export_leidos_telephone SET TAGS ...`` against a table that does
not exist, raised ``TABLE_OR_VIEW_NOT_FOUND``, and -- because the group-level loop has no
per-flow isolation -- failed the whole ``apply_governance_uc6`` task, so **every other flow's
tags in the group went unapplied too**. Found live on UC6's first green pipeline update.

``"external_sink"`` is deliberately NOT skipped: it materializes a real main table first and
only additionally exports it (``register_external_sink_export``), so its tags apply as normal.
The test below pins both halves of that distinction.
"""

from types import SimpleNamespace

import pytest

from flowx.lakeflow_framework.governance import tags as tags_mod


def _row(target_table, target_type, governance_tags_json='{"table_tags": {"pii": "true"}}'):
    return SimpleNamespace(
        target_catalog="flowx",
        target_schema="gold",
        target_table=target_table,
        target_type=target_type,
        governance_tags_json=governance_tags_json,
    )


@pytest.fixture
def capture(monkeypatch):
    """Stub the control-table load and record every table the loop tries to tag."""
    tagged = []

    def fake_apply(spark, catalog, schema, table, governance_tags):  # noqa: ARG001
        tagged.append(f"{catalog}.{schema}.{table}")

    monkeypatch.setattr(tags_mod, "apply_governance_tags", fake_apply, raising=True)

    def install(ingestion_rows=(), transformation_rows=()):
        import flowx.lakeflow_framework.control_plane.repository as repo

        monkeypatch.setattr(
            repo,
            "load_active_group_metadata",
            lambda spark, catalog, group_id: SimpleNamespace(
                ingestion_rows=list(ingestion_rows),
                transformation_rows=list(transformation_rows),
            ),
            raising=True,
        )
        return tagged

    return install


def test_sink_flow_is_skipped(capture):
    tagged = capture(transformation_rows=[_row("uc6_export_leidos_telephone", "sink")])
    tags_mod.apply_all_governance_tags(spark=object(), control_catalog="flowx", group_id="g")
    assert tagged == [], "a sink has no materialized table -- nothing must be tagged"


def test_external_sink_is_still_tagged(capture):
    """The distinction that matters: external_sink DOES materialize a main table."""
    tagged = capture(transformation_rows=[_row("ref_export", "external_sink")])
    tags_mod.apply_all_governance_tags(spark=object(), control_catalog="flowx", group_id="g")
    assert tagged == ["flowx.gold.ref_export"]


@pytest.mark.parametrize("target_type", ["streaming_table", "materialized_view", "batch_table"])
def test_materialized_target_types_are_still_tagged(capture, target_type):
    """REGRESSION GUARD: the skip must be surgical. Every type that materializes a table
    keeps its pre-fix behaviour exactly."""
    tagged = capture(transformation_rows=[_row("t", target_type)])
    tags_mod.apply_all_governance_tags(spark=object(), control_catalog="flowx", group_id="g")
    assert tagged == ["flowx.gold.t"]


def test_one_sink_does_not_take_down_its_siblings(capture):
    """The actual UC6 failure mode: one sink in the group made the whole task fail, so the
    six bronze tables and two gold tables never received their tags either."""
    tagged = capture(
        ingestion_rows=[_row("uc6_ea_request", "streaming_table")],
        transformation_rows=[
            _row("uc6_telephone_output", "materialized_view"),
            _row("uc6_export_leidos_telephone", "sink"),
            _row("uc6_export_telephone", "sink"),
        ],
    )
    tags_mod.apply_all_governance_tags(spark=object(), control_catalog="flowx", group_id="g")
    assert tagged == ["flowx.gold.uc6_ea_request", "flowx.gold.uc6_telephone_output"]


def test_row_without_target_type_attribute_is_tagged_as_before(capture):
    """A control-table row from an older schema may lack target_type entirely; getattr with a
    default must fall through to the pre-fix path rather than raise."""
    row = SimpleNamespace(
        target_catalog="flowx", target_schema="gold", target_table="legacy",
        governance_tags_json='{"table_tags": {"a": "b"}}',
    )
    tagged = capture(transformation_rows=[row])
    tags_mod.apply_all_governance_tags(spark=object(), control_catalog="flowx", group_id="g")
    assert tagged == ["flowx.gold.legacy"]


def test_sink_with_no_governance_block_is_a_no_op_too(capture):
    tagged = capture(transformation_rows=[_row("s", "sink", governance_tags_json=None)])
    tags_mod.apply_all_governance_tags(spark=object(), control_catalog="flowx", group_id="g")
    assert tagged == []
