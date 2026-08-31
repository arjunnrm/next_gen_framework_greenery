"""Every v1.5.0 execution_mode-conditioned reconciliation attribute must be REJECTED by
onboarding when its precondition is violated, never silently ignored -- modelled exactly on
test_removed_attributes_v140.py's pattern for the (different) class of *permanently withdrawn*
attributes.

These four are not withdrawn from the spec: each is valid under ONE ``execution_mode`` and
invalid under the other, so the rejection is *conditional*, not unconditional. The failure
mode it guards against is the same one v1.4.0's removals guard against, though: an ignored key
still onboards, still writes its control-table row, and still runs the pipeline, while quietly
doing something other than what the document says.

Presence, not truthiness, is still the trigger for the three that are pure key-presence checks
(``task_run_id_column``, ``dq_config``, ``publish_schema``) -- ``reject_mode_incompatible_keys``
tests ``key in config``, so a falsy-but-present value (an empty string, an empty dict) is
rejected exactly like a truthy one; each gets a "false is also rejected" twin below. ``read_mode``
is the one exception: its rejection is conditioned on the specific value ``"streaming"``, not on
mere presence of the ``read_mode`` key -- ``read_mode: "batch"`` is explicitly legal in pipeline
mode -- so its twin below asserts the value-specific contrast instead of a truthiness one.

Pure Python, no Spark: ``validate_spec`` only touches a session for non-empty
``transformation_sql``, and nothing here has any -- same sentinel-safety note as
test_spec_validator.py's own docstring and test_removed_attributes_v140.py's.
"""

from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import validate_spec


def _reconciliation_flow(**overrides):
    flow = {
        "reconciliation_id": "recon_test",
        "source_config": {"type": "table", "table": "poc.bronze_x.baseline"},
        "target_configs": [
            {
                "target_id": "primary",
                "type": "table",
                "table": "poc.bronze_x.product",
                "append_target_table": "poc.bronze_x.cdc",
            }
        ],
        "match_keys": ["example_id"],
    }
    flow.update(overrides)
    return flow


def _ingestion_flow(**overrides):
    """A producing ingestion flow whose target is ``poc.bronze_x.product`` -- the source table
    the positive-control pipeline-mode reconciliation flow below reads, so V-CYC-1/V-CYC-6 (a
    separate concern from the four attributes under test here) do not also fire and muddy the
    "clean flow validates" assertion."""
    flow = {
        "dataflow_id": "df_producer",
        "source_type": "autoloader",
        "target_catalog": "poc",
        "target_schema": "bronze_x",
        "target_table": "product",
        "target_type": "streaming_table",
        "source_config": {
            "path": "/Volumes/poc/landing/x/",
            "format": "csv",
            "schema_location": "/Volumes/poc/landing/_schemas/x/",
        },
        "target_config": {"cdc_load_strategy": "APPEND"},
    }
    flow.update(overrides)
    return flow


def _errors(spec):
    return validate_spec(None, spec)[4]


# --------------------------------------------------------------------- read_mode 'streaming'
def test_read_mode_streaming_is_rejected_in_pipeline_mode():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.baseline", "read_mode": "streaming"},
            )
        ],
    }
    matched = [e for e in _errors(spec) if "source_config.read_mode" in e]
    assert matched, "read_mode 'streaming' must be reported when execution_mode is 'pipeline'"
    assert "not supported when execution_mode is 'pipeline'" in matched[0]
    assert "read_mode 'batch'" in matched[0] and "execution_mode to 'job'" in matched[0], (
        "the error must name both the path (read_mode) and the replacement (batch, or job mode)"
    )


def test_read_mode_batch_explicitly_set_is_not_rejected_in_pipeline_mode():
    """The value-specific twin: the trigger is the *value* 'streaming', not mere presence of the
    read_mode key -- an explicit 'batch' (identical in effect to the omitted default) stays legal."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.baseline", "read_mode": "batch"},
            )
        ],
    }
    assert not any("read_mode" in e for e in _errors(spec))


# ---------------------------------------------------------------------- task_run_id_column
def test_task_run_id_column_is_rejected_in_pipeline_mode():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.baseline", "task_run_id_column": "run_id"},
            )
        ],
    }
    matched = [e for e in _errors(spec) if "source_config.task_run_id_column" in e]
    assert matched, "task_run_id_column must be reported when execution_mode is 'pipeline'"
    assert "not supported when execution_mode is 'pipeline'" in matched[0]
    assert "filter_condition" in matched[0] and "execution_mode to 'job'" in matched[0], (
        "the error must name both the path (task_run_id_column) and the replacement "
        "(filter_condition, or job mode)"
    )


def test_task_run_id_column_empty_string_is_also_rejected_in_pipeline_mode():
    """Presence, not truthiness. reject_mode_incompatible_keys tests ``key in config``, so an
    empty-but-present value still describes a per-run narrowing that pipeline mode cannot honour
    -- there is no stable per-update run id (engine/run_context.py::resolve_pipeline_run_id)."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.baseline", "task_run_id_column": ""},
            )
        ],
    }
    assert any("source_config.task_run_id_column" in e for e in _errors(spec))


# --------------------------------------------------------------------- dq_config in job mode
def test_dq_config_is_rejected_when_execution_mode_is_job():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _reconciliation_flow(
                dq_config={"rules": [{"rule_id": "no_value_drift", "expression": "value_drift_count = 0", "action": "fail"}]}
            )
        ],
    }
    matched = [e for e in _errors(spec) if "reconciliation_flow[recon_test].dq_config" in e and "not supported" in e]
    assert matched, "dq_config must be reported when execution_mode is 'job' (the default)"
    assert "not supported when execution_mode is 'job'" in matched[0]
    assert "execution_mode to 'pipeline'" in matched[0], "the error must name the replacement (a pipeline execution_mode)"


def test_dq_config_empty_dict_is_also_rejected_when_execution_mode_is_job():
    """Presence, not truthiness. An empty dq_config is still a statement that expectations
    attach to the one-row __metrics dataset that a 'job' execution_mode flow never produces."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_reconciliation_flow(dq_config={})],
    }
    assert any(
        "reconciliation_flow[recon_test].dq_config" in e and "not supported" in e for e in _errors(spec)
    )


# --------------------------------------------------------------- publish_schema in job mode
def test_publish_schema_is_rejected_when_execution_mode_is_job():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_reconciliation_flow(publish_schema="bronze_excalibur")],
    }
    matched = [e for e in _errors(spec) if "reconciliation_flow[recon_test].publish_schema" in e and "not supported" in e]
    assert matched, "publish_schema must be reported when execution_mode is 'job' (the default)"
    assert "not supported when execution_mode is 'job'" in matched[0]
    assert "execution_mode to 'pipeline'" in matched[0]


def test_publish_schema_empty_string_is_also_rejected_when_execution_mode_is_job():
    """Presence, not truthiness -- mirrors dq_config's twin above. An empty string is still
    present as a key, and reject_mode_incompatible_keys's ``key in config`` check does not care
    that check_string's own type validation treats an empty string as a silent no-op."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_reconciliation_flow(publish_schema="")],
    }
    assert any(
        "reconciliation_flow[recon_test].publish_schema" in e and "not supported" in e for e in _errors(spec)
    )


# --------------------------------------------------------------------------- positive control
def test_reconciliation_flow_without_any_mode_conditioned_attribute_is_valid():
    """None of the four attributes above are present; the flow must validate cleanly both in the
    default 'job' execution_mode and in 'pipeline' mode once pipeline mode's own preconditions
    (a dataflow_group_id and a source resolving to an in-spec producer) are satisfied."""
    spec = {"dataflow_group_id": "dfg_test", "reconciliation_flows": [_reconciliation_flow()]}
    assert _errors(spec) == []

    pipeline_spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_ingestion_flow()],
        "reconciliation_flows": [
            _reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.product"},
            )
        ],
    }
    assert _errors(pipeline_spec) == []
