"""The triggered observability engine's four-parameter contract (observability/runtime_params.py).

Pure Python -- no Spark, no SDK, no dbutils. That is the point of the module: the entire
parameter contract is checkable before the notebook constructs a ``WorkspaceClient``, so a
mis-wired job fails in a second instead of part-way through an export.
"""

import pytest

from flowx.lakeflow_framework.exceptions import ObservabilityConfigError
from flowx.lakeflow_framework.observability.runtime_params import (
    REQUIRED_TRIGGERED_PARAMETERS,
    assert_dataflow_group_id_matches,
    resolve_triggered_run_parameters,
)

VALID = {
    "dataflow_group_id": "dfg_zip_csv_dataload",
    "catalog": "flowx",
    "env": "dev",
    "pipeline_task_run_id": "123456789",
}


def test_all_four_present_resolves():
    resolved = resolve_triggered_run_parameters(VALID)
    assert resolved.dataflow_group_id == "dfg_zip_csv_dataload"
    assert resolved.catalog == "flowx"
    assert resolved.env == "dev"
    assert resolved.pipeline_task_run_id == "123456789"


def test_values_are_stripped():
    resolved = resolve_triggered_run_parameters({k: f"  {v}  " for k, v in VALID.items()})
    assert resolved.catalog == "flowx"
    assert resolved.pipeline_task_run_id == "123456789"


def test_extra_keys_are_ignored():
    """Callers pass their whole widget dict rather than filtering it -- service_name, obs_mode and
    the rest have nothing to do with this contract."""
    resolved = resolve_triggered_run_parameters({**VALID, "service_name": "x", "obs_mode": "triggered"})
    assert resolved.env == "dev"


@pytest.mark.parametrize("missing", REQUIRED_TRIGGERED_PARAMETERS)
def test_each_parameter_is_required(missing):
    params = {k: v for k, v in VALID.items() if k != missing}
    with pytest.raises(ObservabilityConfigError) as exc:
        resolve_triggered_run_parameters(params)
    assert missing in str(exc.value)


@pytest.mark.parametrize("blank", ["", "   ", None])
def test_blank_and_none_count_as_absent(blank):
    """A dbutils text widget with no value reads as ""; it never reads as absent. Treating blank
    as present would let an empty catalog through to a table lookup."""
    with pytest.raises(ObservabilityConfigError) as exc:
        resolve_triggered_run_parameters({**VALID, "catalog": blank})
    assert "catalog" in str(exc.value)


def test_every_missing_parameter_is_reported_at_once():
    """One run should teach an operator everything wrong with the job definition -- reporting the
    first missing parameter only turns one fix into four redeploys."""
    with pytest.raises(ObservabilityConfigError) as exc:
        resolve_triggered_run_parameters({})
    message = str(exc.value)
    for name in REQUIRED_TRIGGERED_PARAMETERS:
        assert name in message


def test_unresolved_task_value_reference_is_named_as_such():
    """The Jobs service does NOT fail on a task_key that does not exist -- it substitutes nothing
    and passes the literal template text through. Without this check that surfaces as a confusing
    "must be numeric" complaint about a value the operator never typed."""
    with pytest.raises(ObservabilityConfigError) as exc:
        resolve_triggered_run_parameters({**VALID, "pipeline_task_run_id": "{{tasks.typo_task.run_id}}"})
    message = str(exc.value)
    assert "unresolved" in message
    assert "task_key" in message


def test_non_numeric_run_id_is_rejected():
    with pytest.raises(ObservabilityConfigError) as exc:
        resolve_triggered_run_parameters({**VALID, "pipeline_task_run_id": "run-42"})
    assert "numeric" in str(exc.value)


# --------------------------------------------------------------- dataflow_group_id cross-check
def test_matching_group_ids_return_the_declared_value():
    assert assert_dataflow_group_id_matches("dfg_a", "dfg_a", "pipe-1") == "dfg_a"


def test_mismatched_group_ids_raise_and_name_both():
    """The wiring mistake this exists for: depends_on pointing at the wrong pipeline_task, or a
    copy-pasted observability block still pointing at the job it came from. A derived-only
    dataflow_group_id can never detect it -- whatever pipeline the task lands on reports its own
    group id quite happily, and the export succeeds under the wrong name."""
    with pytest.raises(ObservabilityConfigError) as exc:
        assert_dataflow_group_id_matches("dfg_expected", "dfg_actual", "pipe-1")
    message = str(exc.value)
    assert "dfg_expected" in message and "dfg_actual" in message and "pipe-1" in message


def test_a_pipeline_declaring_no_group_id_is_not_an_error():
    """Not every pipeline sets dataflow.group.id -- a pre-v1.3.0 deployment legitimately does not.
    Refusing to export because the cross-check has nothing to check against would turn an optional
    safety net into a new hard requirement on every pipeline in the estate."""
    assert assert_dataflow_group_id_matches("dfg_a", None, "pipe-1") == "dfg_a"
    assert assert_dataflow_group_id_matches("dfg_a", "", "pipe-1") == "dfg_a"
