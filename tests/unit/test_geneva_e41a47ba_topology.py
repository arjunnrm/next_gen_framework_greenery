"""Regression tests for the topology of the LIVE geneva reconciliation pipeline
``e41a47ba-5ad0-4dc5-9535-5aa16cc97e65`` (``[dev arjun] FlowX_test_104_reconcilation_batch``).

Why this file exists as its own module rather than another parametrised case in
``test_source_plane_plan.py``: the rows below are not a hand-designed fixture exercising a rule in
the abstract, they are a transcription of what was actually onboarded in
``flowx.config.*`` for ``dataflow_group_id='dfg_geneva_tariffs_recon'`` (read back on
2026-08-31). Keeping them together, and naming the pipeline in the module docstring, is what makes
it obvious to the next reader that changing these values invalidates the test rather than merely
adjusting it.

The defect being pinned:

``_NON_APPEND_ONLY_CDC_STRATEGIES`` originally listed only SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC. This
pipeline's reconciliation source is its OWN ingestion target, written with
``cdc_load_strategy='TRUNCATE_AND_LOAD'`` -- a full recompute that REPLACES the table contents
every update (see ``cdc/dispatcher.py``), which Delta refuses to stream from. Because
``execution_mode='pipeline'`` requests ``want_stream=True`` for the reconciliation source, the
plane would have planned a streaming read of a non-append-only table, passed every plan-time
check, and failed only at pipeline runtime with ``DELTA_SOURCE_TABLE_IGNORE_CHANGES`` -- precisely
the class of failure the G-STREAM guard exists to convert into an onboarding-time error.
"""

import json

import pytest

from flowx.lakeflow_framework.engine.source_plane import (
    _NON_APPEND_ONLY_CDC_STRATEGIES,
    plan_source_plane,
)
from flowx.lakeflow_framework.exceptions import FrameworkConfigError

GROUP_ID = "dfg_geneva_tariffs_recon"
CATALOG = "flowx"
STG_TABLE = "flowx.geneva_admin.stg_tariffelementband"
BRONZE_TABLE = "flowx.bronze_excalibur.bronze_tariffelementband"
LANDING_TABLE = "flowx.geneva_admin.landing_tariffelementband"


class Row:
    """Minimal duck-typed stand-in for a Spark ``Row``: attribute access plus ``asDict``."""

    def __init__(self, **fields):
        self.__dict__.update(fields)

    def __getitem__(self, key):
        return self.__dict__[key]

    def asDict(self):
        return dict(self.__dict__)


def _ingestion_row():
    """The group's single active ingestion flow, verbatim from ``ingestion_flow_spec``."""
    return Row(
        dataflow_id="df_tariff_element_band_ingest",
        dataflow_group_id=GROUP_ID,
        source_type="autoloader",
        target_catalog=CATALOG,
        target_schema="geneva_admin",
        target_table="stg_tariffelementband",
        target_type="streaming_table",
        cdc_load_strategy="TRUNCATE_AND_LOAD",
        source_config_json=json.dumps(
            {
                "path": "/Volumes/flowx/geneva_admin/batch_recon/",
                "format": "csv",
                "schema_location": "/Volumes/flowx/geneva_admin/_schemas/stg_tariffelementband/",
                "file_pattern": "batch_recon_*",
            }
        ),
        target_config_json=json.dumps({"cdc_load_strategy": "TRUNCATE_AND_LOAD"}),
    )


def _reconciliation_row(execution_mode):
    """The group's single active reconciliation flow, verbatim from ``reconciliation_flow_spec``."""
    return Row(
        reconciliation_id="rec_tariff_element_band",
        dataflow_group_id=GROUP_ID,
        execution_mode=execution_mode,
        source_config_json=json.dumps({"type": "table", "table": STG_TABLE, "read_mode": "batch"}),
        target_configs_json=json.dumps(
            [
                {
                    "target_id": "bronze_target",
                    "type": "table",
                    "table": BRONZE_TABLE,
                    "append_target_table": LANDING_TABLE,
                }
            ]
        ),
    )


def _plan(execution_mode):
    return plan_source_plane(
        ingestion_rows=[_ingestion_row()],
        transformation_rows=[],
        reconciliation_rows=[_reconciliation_row(execution_mode)],
        node_catalog=CATALOG,
        node_schema="geneva_admin",
    )


def test_truncate_and_load_is_registered_as_non_append_only():
    """The constant itself, asserted directly.

    ``TRUNCATE_AND_LOAD`` dispatches to no CDC strategy at all, so it is genuinely easy to think
    of it as "just an append" and drop it from this set. It is not: its target is a full
    recompute. Asserting membership here means removing it fails a test that says why, instead of
    silently re-arming a runtime-only failure.
    """
    assert "TRUNCATE_AND_LOAD" in _NON_APPEND_ONLY_CDC_STRATEGIES


def test_full_pipeline_mode_is_rejected_at_plan_time():
    """``execution_mode='pipeline'`` must fail HERE, not at pipeline runtime."""
    with pytest.raises(FrameworkConfigError) as excinfo:
        _plan("pipeline")

    message = str(excinfo.value)
    # The message has to be actionable on its own: which consumer, which table, which producer,
    # why it is refused, and what to do instead.
    assert "rec_tariff_element_band:source" in message
    assert STG_TABLE in message
    assert "df_tariff_element_band_ingest" in message
    assert "TRUNCATE_AND_LOAD" in message
    assert "DELTA_SOURCE_TABLE_IGNORE_CHANGES" in message
    assert "pipeline_audit_only" in message, "the error must name the mode that actually works here"


def test_audit_only_mode_plans_successfully():
    """``pipeline_audit_only`` reads the source as a batch, so the guard does not fire."""
    plan = _plan("pipeline_audit_only")
    assert plan.bindings, "audit-only mode must still produce bindings"


def test_audit_only_reuses_the_in_graph_ingestion_target_rather_than_re_reading_it():
    """R2 (read-once) for this pipeline, stated as a property rather than a count.

    The reconciliation source IS the ingestion target. The whole point of the source plane is
    that this resolves to the sibling dataset already being built in this update -- not to a
    second, independent read of the same physical table.
    """
    plan = _plan("pipeline_audit_only")

    source_bindings = [
        binding
        for consumer_id, binding in plan.bindings.items()
        if consumer_id == "rec_tariff_element_band:source"
    ]
    assert len(source_bindings) == 1, f"expected exactly one source binding, got {list(plan.bindings)}"
    assert source_bindings[0].kind == "in_graph_sibling", (
        "the reconciliation source must bind to the ingestion target already in this graph; "
        f"got kind={source_bindings[0].kind!r}"
    )


def test_audit_only_reads_the_far_side_target_out_of_graph():
    """The bronze target is produced by a DIFFERENT group, so it cannot be a graph sibling."""
    plan = _plan("pipeline_audit_only")

    target_bindings = [
        binding for consumer_id, binding in plan.bindings.items() if consumer_id.endswith(":target:bronze_target")
    ]
    assert len(target_bindings) == 1, f"expected one target binding, got {list(plan.bindings)}"
    assert target_bindings[0].kind != "in_graph_sibling", (
        "bronze_tariffelementband is not produced in this dataflow group, so binding it as an "
        "in-graph sibling would mean the plane invented a graph edge to a dataset this pipeline "
        "does not build"
    )


def test_job_mode_keeps_reconciliation_out_of_the_plane_entirely():
    """Backward compatibility: the pre-v1.5.0 behaviour of this very pipeline.

    A job-mode reconciliation flow is filtered out upstream by ``load_active_group_metadata``, so
    the plane should never see it. Passing one in anyway must not create bindings for it -- that
    would mean a job-mode flow silently started participating in the pipeline graph.
    """
    plan = plan_source_plane(
        ingestion_rows=[_ingestion_row()],
        transformation_rows=[],
        reconciliation_rows=[_reconciliation_row("job")],
        node_catalog=CATALOG,
        node_schema="geneva_admin",
    )
    recon_consumers = [consumer_id for consumer_id in plan.bindings if consumer_id.startswith("rec_tariff_element_band")]
    assert recon_consumers == [], f"job-mode reconciliation must not enter the plane; got {recon_consumers}"
