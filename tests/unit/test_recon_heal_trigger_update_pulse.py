"""``heal_trigger: "update_pulse"`` -- the declarative in-graph reconciliation heal lane (v0.0.7).

**What this exists to prove.** Before v0.0.7 the L5 heal lane registered only under
``execution_mode: "pipeline"``, which STREAMS the reconciliation source (the same ``needs_heal``
flag was the ``want_stream`` of the L3 ``_src`` node and of the heal pulse). A MERGE-written or
fully-recomputed source therefore could not heal in-graph at all: UC3's recon source is a
``TRUNCATE_AND_LOAD`` materialized view, and the plan-time G-STREAM guard rejected it.

``heal_trigger: "update_pulse"`` decouples the TRIGGER from the PAYLOAD, exactly as
``sink_config.export_trigger: "per_update"`` already does for sink exports (v1.7.5): a
``rate-micro-batch`` pulse carrying no data drives the ``@dlt.append_flow``, and the miss set
joins in as a BATCH ``dlt.read``. Nothing streams the source, so the source's write pattern
stops mattering.

**The regression that matters most is the NEGATIVE one.** The relaxation must be opt-in: an
absent or ``"source_stream"`` ``heal_trigger`` must still be rejected for a non-append-only
source, or a genuinely streaming recon flow could silently bind an unstreamable source and fail
at pipeline runtime instead of at onboarding. Three of the tests below pin exactly that.
"""

import copy
import json

import pytest

from flowx.lakeflow_framework.engine.source_plane import plan_source_plane
from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.onboarding.spec_validator import (
    ALLOWED_HEAL_TRIGGERS,
    ALLOWED_RECONCILIATION_FLOW_KEYS,
)


class _Row(dict):
    """Control-table row stand-in: attribute access over a dict."""

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k)


CATALOG = "cat"


def _transformation_row():
    """A TRUNCATE_AND_LOAD materialized_view producer -- the non-append-only shape."""
    return _Row(
        flow_step_id="df_batch_load",
        dataflow_id="df_batch_load",
        source_inputs_json=json.dumps(
            [{"input_name": "src", "table": f"{CATALOG}.ext.connector_tbl", "is_streaming": False}]
        ),
        transformation_sql="SELECT * FROM src",
        target_catalog=CATALOG,
        target_schema="staging",
        target_table="tbl_batch",
        target_type="materialized_view",
        target_config_json=json.dumps({"cdc_load_strategy": "TRUNCATE_AND_LOAD"}),
        cdc_load_strategy="TRUNCATE_AND_LOAD",
        is_active=True,
    )


def _reconciliation_row(heal_trigger, execution_mode="pipeline"):
    row = _Row(
        reconciliation_id="rf_x",
        execution_mode=execution_mode,
        source_config_json=json.dumps({"type": "table", "table": f"{CATALOG}.staging.tbl_batch"}),
        target_configs_json=json.dumps(
            [
                {
                    "target_id": "t1",
                    "type": "table",
                    "table": f"{CATALOG}.bronze.tbl",
                    "comparison_direction": "both",
                    "append_target_table": f"{CATALOG}.staging.landing_cdc",
                }
            ]
        ),
        publish_schema="reconciliation",
        is_active=True,
    )
    if heal_trigger is not None:
        row["heal_trigger"] = heal_trigger
    return row


def _plan(heal_trigger, execution_mode="pipeline"):
    return plan_source_plane(
        ingestion_rows=[],
        transformation_rows=[_transformation_row()],
        reconciliation_rows=[_reconciliation_row(heal_trigger, execution_mode)],
    )


# --------------------------------------------------------------------------------------------
# The relaxation itself
# --------------------------------------------------------------------------------------------


def test_update_pulse_plans_against_a_truncate_and_load_materialized_view_source():
    """The whole point: the shape that was previously impossible now plans."""
    plan = _plan("update_pulse")
    assert plan is not None


def test_update_pulse_binds_the_source_as_batch_not_stream():
    """The relaxation must work by NOT streaming -- not by suppressing the guard."""
    plan = _plan("update_pulse")
    binding = plan.bindings["rf_x:source"]
    assert binding.mode == "batch"


# --------------------------------------------------------------------------------------------
# The negative regressions -- the relaxation is OPT-IN
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("heal_trigger", [None, "source_stream"])
def test_non_append_only_source_is_still_rejected_without_update_pulse(heal_trigger):
    """An absent or explicit 'source_stream' trigger must still hit the G-STREAM guard."""
    with pytest.raises(FrameworkConfigError) as exc:
        _plan(heal_trigger)
    assert "streaming read" in str(exc.value)


def test_the_guard_message_still_names_the_producer_strategy():
    """Operators rely on the message naming WHY the source is unstreamable."""
    with pytest.raises(FrameworkConfigError) as exc:
        _plan("source_stream")
    message = str(exc.value)
    assert "TRUNCATE_AND_LOAD" in message
    assert "materialized_view" in message


# --------------------------------------------------------------------------------------------
# Spec surface
# --------------------------------------------------------------------------------------------


def test_heal_trigger_is_an_allowed_reconciliation_flow_key():
    """Unknown attributes are hard-rejected since v1.7.2, so the allowlist must carry it."""
    assert "heal_trigger" in ALLOWED_RECONCILIATION_FLOW_KEYS


def test_allowed_heal_triggers_are_exactly_the_two_documented_values():
    assert ALLOWED_HEAL_TRIGGERS == {"source_stream", "update_pulse"}


def test_json_schema_and_validator_agree_on_heal_trigger():
    """The two gates drift; only the schema rejects unknown keys, so pin them together."""
    schema = json.load(open("onboarding_templates/onboarding_spec.schema.json", encoding="utf-8"))
    node = schema["$defs"]["reconciliationFlow"]["properties"]["heal_trigger"]
    assert set(node["enum"]) == ALLOWED_HEAL_TRIGGERS
    assert node["default"] == "source_stream"


# --------------------------------------------------------------------------------------------
# The UC3 spec itself
# --------------------------------------------------------------------------------------------


def test_uc3_batch_recon_spec_uses_the_pulse_driven_heal_lane():
    spec = json.load(
        open("BT_Usecase/UC3/onboarding/uc3_excalibur_batch_recon.json", encoding="utf-8")
    )
    flows = spec["reconciliation_flows"]
    assert flows, "UC3 batch recon spec must declare reconciliation flows"
    for flow in flows:
        assert flow["execution_mode"] == "pipeline"
        assert flow["heal_trigger"] == "update_pulse"
        # Healing needs a published schema for the metrics/mismatch exports.
        assert flow.get("publish_schema")


def test_uc3_customer_target_still_filters_superseded_scd2_rows():
    """bronze.customer is SCD2 and read in FULL -- without __END_AT IS NULL, superseded history
    rows would classify as VALUE_DRIFT and be healed in a loop."""
    spec = json.load(
        open("BT_Usecase/UC3/onboarding/uc3_excalibur_batch_recon.json", encoding="utf-8")
    )
    customer = next(
        f for f in spec["reconciliation_flows"] if f["reconciliation_id"].startswith("rf_uc3_customer")
    )
    assert customer["target_configs"][0]["filter_condition"] == "__END_AT IS NULL"
