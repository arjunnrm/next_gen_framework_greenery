"""v1.6.0 reconciliation logging-gate contract.

Three layers of the same rule, asserted independently:

1. **Resolution** -- ``reconciliation/appender.py::resolve_log_capture_flags``'s precedence
   (job/pipeline parameter > onboarded ``logging_config`` > ``True``) is pure and tested with
   plain dicts. (First direct coverage of this helper -- it predates v1.6.0.)
2. **Graph definition** -- ``reconciliation/graph_registration.py`` registers ``__metrics`` only
   when ``run_log_capture`` resolves true and ``__mismatch`` only when ``mismatch_log_capture``
   does; every true intermediate (``_src``/``_tgt``/``__classified``/``__missing``/pulse) is a
   pipeline-scoped ``temporary`` table under its bare name, EXCEPT a healing flow's ``_src`` and
   healing ``_tgt`` (the L5 handler reads them back via ``spark.read.table``, which resolves
   through the metastore). Contradictory configurations raise ``FrameworkConfigError`` at graph
   definition, not at runtime.
3. **Writes** -- ``reconciliation_result`` is gated by ``run_log_capture`` exactly like
   ``reconciliation_run_log`` (the pre-v1.6.0 behaviour wrote it unconditionally): both flags
   false means reconciliation persists to NOTHING but its business targets.

The onboarding-time mirror of the graph-definition guards lives in
``onboarding/spec_validator.py::_validate_logging_config`` and is asserted here too, so the
two layers cannot drift apart silently.
"""

import json
from types import SimpleNamespace

import dlt
import pytest

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import _validate_logging_config
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation import appender, graph_registration
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.appender import resolve_log_capture_flags
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.matcher import ReconciliationFingerprint


# ---------------------------------------------------------------------------------------------
# 1. resolve_log_capture_flags precedence
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "logging_config, run_override, mismatch_override, expected",
    [
        # Layer 3: both absent everywhere -> True/True.
        (None, None, None, (True, True)),
        ({}, None, None, (True, True)),
        # Layer 2: the onboarded flow metadata.
        ({"run_log_capture": False}, None, None, (False, True)),
        ({"mismatch_log_capture": False}, None, None, (True, False)),
        ({"run_log_capture": False, "mismatch_log_capture": False}, None, None, (False, False)),
        # Layer 1: the job/pipeline parameter wins over the onboarded value, in both directions.
        ({"run_log_capture": False}, True, None, (True, True)),
        ({"run_log_capture": True}, False, None, (False, True)),
        ({"mismatch_log_capture": False}, None, True, (True, True)),
        ({}, False, False, (False, False)),
    ],
)
def test_resolve_log_capture_flags_precedence(logging_config, run_override, mismatch_override, expected):
    assert resolve_log_capture_flags(logging_config, run_override, mismatch_override) == expected


# ---------------------------------------------------------------------------------------------
# Graph-definition harness: record every dlt registration, stub out everything physical.
# ---------------------------------------------------------------------------------------------


class _Registration:
    def __init__(self, kind, name, temporary):
        self.kind = kind
        self.name = name
        self.temporary = temporary

    def __repr__(self):  # pragma: no cover -- assertion output only
        return f"_Registration({self.kind!r}, {self.name!r}, temporary={self.temporary!r})"


class _Recorder:
    def __init__(self):
        self.registrations = []

    def _register(self, kind, name, temporary=False):
        def _decorator(fn):
            self.registrations.append(_Registration(kind, name or fn.__name__, temporary))
            return fn

        return _decorator

    def table(self, query_function=None, name=None, temporary=False, **_kwargs):
        decorator = self._register("table", name, temporary)
        return decorator(query_function) if query_function is not None else decorator

    def view(self, query_function=None, name=None, **_kwargs):
        decorator = self._register("view", name)
        return decorator(query_function) if query_function is not None else decorator

    def append_flow(self, name=None, **_kwargs):
        return self._register("append_flow", name)

    @property
    def names(self):
        return [registration.name for registration in self.registrations]

    def by_name(self, name):
        matches = [registration for registration in self.registrations if registration.name == name]
        assert len(matches) == 1, f"expected exactly one registration named {name!r}, got {self.names}"
        return matches[0]


@pytest.fixture()
def recorder(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(dlt, "table", rec.table)
    monkeypatch.setattr(dlt, "view", rec.view)
    monkeypatch.setattr(dlt, "append_flow", rec.append_flow)
    # Registration-shape tests never execute dataset bodies, but keep reads harmless anyway.
    monkeypatch.setattr(dlt, "read", lambda name: SimpleNamespace(name=name), raising=False)
    monkeypatch.setattr(dlt, "read_stream", lambda name: SimpleNamespace(name=name), raising=False)
    # The flow bodies (never executed here) would call bind(); the heal lane's registration-time
    # helpers ARE executed, so stub them to pure recorders.
    monkeypatch.setattr(graph_registration, "bind", lambda plan, consumer_id, want_stream: SimpleNamespace())
    monkeypatch.setattr(graph_registration, "require_streaming_source", lambda *a, **k: None)
    sinks = []
    monkeypatch.setattr(graph_registration, "register_foreach_batch_sink", lambda name, handler: sinks.append(name))
    rec.sinks = sinks
    return rec


def _recon_row(execution_mode="pipeline_audit_only", logging_config=None, dq_config=None, heal=False):
    target = {"target_id": "t_ref", "table": "metaflow.ext.orders_ref"}
    if heal:
        target["comparison_direction"] = "both"
        target["append_target_table"] = "metaflow.silver.orders"
    return SimpleNamespace(
        reconciliation_id="rec_gate",
        execution_mode=execution_mode,
        source_config_json=json.dumps({"table": "metaflow.ext.orders"}),
        target_configs_json=json.dumps([target]),
        match_keys_json=json.dumps(["order_id"]),
        compare_columns_json=json.dumps(["amount"]),
        error_handling_json=json.dumps({}),
        logging_config_json=json.dumps(logging_config or {}),
        dq_config_json=json.dumps(dq_config or {}),
        transform_sql=None,
        publish_schema=None,
        two_tier_verification=None,
    )


def _register(row, recorder, log_capture_overrides=None):
    graph_registration.register_reconciliation_flow(
        SimpleNamespace(),  # spark -- never touched at registration time
        row,
        plan=SimpleNamespace(),  # bind() is stubbed; the plan is opaque to registration
        publish_catalog="metaflow",
        publish_schema="recon",
        control_schema="metaflow.config",
        log_capture_overrides=log_capture_overrides,
    )
    return recorder


# ---------------------------------------------------------------------------------------------
# 2. Registration gating + the Intermediate Object Rule
# ---------------------------------------------------------------------------------------------


def test_defaults_register_metrics_and_mismatch_published_and_intermediates_temporary(recorder):
    _register(_recon_row(), recorder)

    metrics = recorder.by_name("metaflow.recon.recon__rec_gate__t_ref__metrics")
    mismatch = recorder.by_name("metaflow.recon.recon__rec_gate__t_ref__mismatch")
    assert metrics.temporary is False and mismatch.temporary is False

    src = recorder.by_name("_recon__rec_gate__src")
    tgt = recorder.by_name("_recon__rec_gate__t_ref__tgt")
    classified = recorder.by_name("_recon__rec_gate__t_ref__classified")
    assert src.temporary is True and tgt.temporary is True and classified.temporary is True
    # Temporary datasets keep bare, pipeline-local names -- never a qualified publish.
    for registration in (src, tgt, classified):
        assert "." not in registration.name


def test_run_log_capture_false_skips_metrics_registration(recorder):
    _register(_recon_row(logging_config={"run_log_capture": False}), recorder)

    assert not any(name.endswith("__metrics") for name in recorder.names)
    assert any(name.endswith("__mismatch") for name in recorder.names)


def test_mismatch_log_capture_false_skips_mismatch_registration(recorder):
    _register(_recon_row(logging_config={"mismatch_log_capture": False}), recorder)

    assert any(name.endswith("__metrics") for name in recorder.names)
    assert not any(name.endswith("__mismatch") for name in recorder.names)


def test_pipeline_conf_override_wins_over_onboarded_logging_config(recorder):
    _register(
        _recon_row(logging_config={"run_log_capture": False, "mismatch_log_capture": True}),
        recorder,
        log_capture_overrides={"recon_run_log_capture": True, "recon_mismatch_log": False},
    )

    assert any(name.endswith("__metrics") for name in recorder.names)
    assert not any(name.endswith("__mismatch") for name in recorder.names)


def test_audit_only_with_both_flags_false_is_rejected_at_graph_definition(recorder):
    with pytest.raises(FrameworkConfigError) as excinfo:
        _register(
            _recon_row(logging_config={"run_log_capture": False, "mismatch_log_capture": False}),
            recorder,
        )
    assert "pipeline_audit_only" in str(excinfo.value)
    assert recorder.registrations == [], "nothing may be registered when the flow is rejected"


def test_dq_rules_with_run_log_capture_false_is_rejected_at_graph_definition(recorder):
    with pytest.raises(FrameworkConfigError) as excinfo:
        _register(
            _recon_row(
                logging_config={"run_log_capture": False},
                dq_config={"rules": [{"rule_id": "r1", "expression": "value_drift_count = 0", "action": "warn"}]},
            ),
            recorder,
        )
    assert "dq_config" in str(excinfo.value)


def test_healing_flow_keeps_src_and_healing_tgt_published_for_the_handler(recorder):
    """The L5 foreach_batch_sink handler reads ``_src``/healing ``_tgt`` back via a plain
    ``spark.read.table`` (metastore resolution), so those two nodes must stay published
    qualified tables while everything else obeys the Intermediate Object Rule."""
    _register(_recon_row(execution_mode="pipeline", heal=True), recorder)

    src = recorder.by_name("metaflow.recon._recon__rec_gate__src")
    tgt = recorder.by_name("metaflow.recon._recon__rec_gate__t_ref__tgt")
    assert src.temporary is False and tgt.temporary is False

    classified = recorder.by_name("_recon__rec_gate__t_ref__classified")
    missing = recorder.by_name("_recon__rec_gate__t_ref__missing")
    pulse = recorder.by_name("_recon__rec_gate__pulse")
    assert classified.temporary is True and missing.temporary is True and pulse.temporary is True
    assert recorder.sinks == ["_recon__rec_gate__heal_sink"]


def test_healing_flow_with_both_flags_false_still_registers_the_heal_lane(recorder):
    """A healing flow with logging suppressed must still heal: no ``__metrics``/``__mismatch``,
    but the pulse + append_flow + sink are all registered (the ordering edge anchors on the
    always-registered ``__classified``, not on ``__metrics``)."""
    _register(
        _recon_row(
            execution_mode="pipeline",
            logging_config={"run_log_capture": False, "mismatch_log_capture": False},
            heal=True,
        ),
        recorder,
    )

    assert not any(name.endswith("__metrics") for name in recorder.names)
    assert not any(name.endswith("__mismatch") for name in recorder.names)
    assert "_recon__rec_gate__pulse" in recorder.names
    assert any(registration.kind == "append_flow" for registration in recorder.registrations)
    assert recorder.sinks == ["_recon__rec_gate__heal_sink"]


# ---------------------------------------------------------------------------------------------
# 3. reconciliation_result is gated by run_log_capture (Phase 1 early-out path)
# ---------------------------------------------------------------------------------------------


def _phase_1(monkeypatch, logging_config):
    written = {"run_log": 0, "result": 0}
    monkeypatch.setattr(appender, "write_run_log_entry", lambda *a, **k: written.__setitem__("run_log", written["run_log"] + 1))
    monkeypatch.setattr(
        appender, "write_reconciliation_result", lambda *a, **k: written.__setitem__("result", written["result"] + 1)
    )
    fingerprint = ReconciliationFingerprint(row_count=3, hash_key_xor="0" * 64, hash_value_xor="0" * 64)
    appender._complete_phase_1_match(
        SimpleNamespace(),
        "metaflow.config",
        "rec_gate",
        "t_ref",
        fingerprint,
        fingerprint,
        logging_config,
        None,
        None,
        None,
        0.0,
    )
    return written


def test_phase_1_writes_run_log_and_result_together_when_capture_is_on(monkeypatch):
    assert _phase_1(monkeypatch, {}) == {"run_log": 1, "result": 1}


def test_phase_1_writes_neither_when_run_log_capture_is_false(monkeypatch):
    assert _phase_1(monkeypatch, {"run_log_capture": False}) == {"run_log": 0, "result": 0}


# ---------------------------------------------------------------------------------------------
# 4. The onboarding-time mirror of the graph-definition guards
# ---------------------------------------------------------------------------------------------


def _validator_errors(logging_config, execution_mode="pipeline_audit_only", dq_config=None):
    errors = []
    _validate_logging_config(
        logging_config,
        "reconciliation_flows[0].logging_config",
        errors,
        execution_mode=execution_mode,
        dq_config=dq_config,
    )
    return errors


def test_validator_rejects_audit_only_with_both_flags_false():
    errors = _validator_errors({"run_log_capture": False, "mismatch_log_capture": False})
    assert any("pipeline_audit_only" in error for error in errors)


def test_validator_rejects_dq_rules_with_run_log_capture_false():
    errors = _validator_errors(
        {"run_log_capture": False},
        execution_mode="pipeline",
        dq_config={"rules": [{"rule_id": "r1", "expression": "value_drift_count = 0", "action": "warn"}]},
    )
    assert any("dq_config" in error for error in errors)


def test_validator_accepts_one_flag_false_and_absent_config():
    assert _validator_errors({"run_log_capture": False}) == []
    assert _validator_errors({"mismatch_log_capture": False}) == []
    assert _validator_errors(None) == []
    # Both false is fine for a plain "pipeline" healing flow -- it still appends corrections.
    assert _validator_errors({"run_log_capture": False, "mismatch_log_capture": False}, execution_mode="pipeline") == []
