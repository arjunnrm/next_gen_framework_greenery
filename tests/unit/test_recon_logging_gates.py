"""v1.7.3 reconciliation logging-gate contract.

Three layers of the same rule, asserted independently:

1. **Resolution** -- ``reconciliation/appender.py::resolve_log_capture_flags``'s precedence
   (job/pipeline parameter > onboarded ``logging_config`` > ``False``) is pure and tested with
   plain dicts. (First direct coverage of this helper -- it predates v1.6.0.)

   **v1.7.3 changed layer 3 only: the implicit fallback is now ``False``, was ``True``.**
   Reconciliation is SILENT BY DEFAULT -- a flow that states no preference at either layer
   registers NEITHER ``recon__*__metrics`` NOR ``recon__*__mismatch``, and writes no
   ``reconciliation_run_log`` / ``reconciliation_result`` / ``reconciliation_mismatch_log``
   rows. Layers 1 and 2 are untouched, so every fixture below that is about *gating* states
   its ``logging_config`` explicitly rather than leaning on whatever the default happens to
   be; the default itself is pinned by its own dedicated tests.
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

**The drift that v1.7.3 introduced is now fixed.** For a while ``_validate_logging_config``
kept its own hardcoded ``True`` fallback (and returned early on ``logging_config is None``)
after ``resolve_log_capture_flags`` had flipped layer 3 to ``False``, so a spec that simply
OMITTED ``logging_config`` passed onboarding and then raised ``FrameworkConfigError`` at graph
definition. The validator now reads both flags through a module constant
``_DEFAULT_LOG_CAPTURE = False`` and runs its cross-field rules even when no config is present.
The seam that keeps the two copies of that decision in agreement is
``test_validator_default_constant_equals_the_runtime_resolver_fallback``; the behavioural
consequences are pinned by ``test_validator_mirrors_the_new_false_default_for_an_absent_config``
and ``test_validator_mirrors_the_new_false_default_for_dq_rules``.
"""

import json
from types import SimpleNamespace

import dlt
import pytest

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.onboarding.spec_validator import _validate_logging_config
from flowx.lakeflow_framework.reconciliation import appender, graph_registration
from flowx.lakeflow_framework.reconciliation.appender import resolve_log_capture_flags
from flowx.lakeflow_framework.reconciliation.matcher import ReconciliationFingerprint


# ---------------------------------------------------------------------------------------------
# 1. resolve_log_capture_flags precedence
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "logging_config, run_override, mismatch_override, expected",
    [
        # Layer 3 (v1.7.3): both absent everywhere -> False/False. Silent by default.
        (None, None, None, (False, False)),
        ({}, None, None, (False, False)),
        # Layer 2: the onboarded flow metadata. Each flag is independent of the other, and an
        # explicit value is honoured in BOTH directions -- true opts back in, false stays off.
        ({"run_log_capture": True}, None, None, (True, False)),
        ({"mismatch_log_capture": True}, None, None, (False, True)),
        ({"run_log_capture": True, "mismatch_log_capture": True}, None, None, (True, True)),
        ({"run_log_capture": False}, None, None, (False, False)),
        ({"mismatch_log_capture": False}, None, None, (False, False)),
        ({"run_log_capture": False, "mismatch_log_capture": False}, None, None, (False, False)),
        # Layer 1: the job/pipeline parameter wins over the onboarded value, in both directions.
        ({"run_log_capture": False}, True, None, (True, False)),
        ({"run_log_capture": True}, False, None, (False, False)),
        ({"mismatch_log_capture": False}, None, True, (False, True)),
        ({"mismatch_log_capture": True}, None, False, (False, False)),
        # ...and over the layer-3 default too, in both directions.
        ({}, True, True, (True, True)),
        ({}, False, False, (False, False)),
        # An override of None is "unset", NOT "false" -- it defers to the layer below rather
        # than forcing the default. This is what makes the widgets tri-state.
        ({"run_log_capture": True, "mismatch_log_capture": True}, None, None, (True, True)),
    ],
)
def test_resolve_log_capture_flags_precedence(logging_config, run_override, mismatch_override, expected):
    assert resolve_log_capture_flags(logging_config, run_override, mismatch_override) == expected


def test_resolve_log_capture_flags_defaults_to_silent():
    """v1.7.3 pins layer 3 to ``False``. This is the BREAKING half of the change: a flow that
    omits ``logging_config`` entirely used to get both audit lanes for free and now gets
    neither. Kept as its own named test (rather than only a parametrize row) so the regression
    reads as a contract change and not as one tweaked tuple in a table."""
    assert resolve_log_capture_flags(None) == (False, False)
    assert resolve_log_capture_flags({}) == (False, False)
    # ...and nothing but an explicit statement turns it back on.
    assert resolve_log_capture_flags({"run_log_capture": True}) == (True, False)
    assert resolve_log_capture_flags({}, recon_run_log_capture=True) == (True, False)


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
    # v1.7.07: a flow with dq_config rules registers `__metrics` (temporary when nothing publishes),
    # and apply_dq_expectations wraps it in dlt.expect_* -- the pip stub raises on those locally,
    # so make them identity decorators. Registration shape is what these tests assert.
    for expectation in ("expect_all", "expect_all_or_drop", "expect_all_or_fail"):
        monkeypatch.setattr(dlt, expectation, lambda rules: (lambda fn: fn), raising=False)
    # The flow bodies (never executed here) would call bind(); the heal lane's registration-time
    # helpers ARE executed, so stub them to pure recorders.
    monkeypatch.setattr(graph_registration, "bind", lambda plan, consumer_id, want_stream: SimpleNamespace())
    monkeypatch.setattr(graph_registration, "require_streaming_source", lambda *a, **k: None)
    sinks = []
    monkeypatch.setattr(graph_registration, "register_foreach_batch_sink", lambda name, handler: sinks.append(name))
    rec.sinks = sinks
    return rec


def _recon_row(execution_mode="pipeline_audit_only", logging_config=None, dq_config=None, heal=False, publish_schema=None):
    """``publish_schema=None`` is the v1.7.07 "publish nothing" shape; tests that assert on a
    PUBLISHED three-part name pass ``publish_schema="recon"`` explicitly -- the function argument
    of the same name is no longer a fallback."""
    target = {"target_id": "t_ref", "table": "flowx.ext.orders_ref"}
    if heal:
        target["comparison_direction"] = "both"
        target["append_target_table"] = "flowx.silver.orders"
    return SimpleNamespace(
        reconciliation_id="rec_gate",
        execution_mode=execution_mode,
        source_config_json=json.dumps({"table": "flowx.ext.orders"}),
        target_configs_json=json.dumps([target]),
        match_keys_json=json.dumps(["order_id"]),
        compare_columns_json=json.dumps(["amount"]),
        error_handling_json=json.dumps({}),
        logging_config_json=json.dumps(logging_config or {}),
        dq_config_json=json.dumps(dq_config or {}),
        transform_sql=None,
        publish_schema=publish_schema,
        two_tier_verification=None,
    )


def _register(row, recorder, log_capture_overrides=None):
    graph_registration.register_reconciliation_flow(
        SimpleNamespace(),  # spark -- never touched at registration time
        row,
        plan=SimpleNamespace(),  # bind() is stubbed; the plan is opaque to registration
        publish_catalog="flowx",
        publish_schema="recon",
        control_schema="flowx.config",
        log_capture_overrides=log_capture_overrides,
    )
    return recorder


# ---------------------------------------------------------------------------------------------
# 2. Registration gating + the Intermediate Object Rule
# ---------------------------------------------------------------------------------------------


def test_both_flags_true_register_metrics_and_mismatch_published_and_intermediates_temporary(recorder):
    """The Intermediate Object Rule, asserted with BOTH capture flags explicitly on.

    Since v1.7.3 the flags must be stated: this test is about *which registrations are
    published vs temporary*, not about what the resolver falls back to, so it pins its own
    inputs instead of inheriting them. The v1.7.3 default is pinned separately by
    ``test_absent_logging_config_registers_neither_audit_dataset``.
    """
    _register(
        _recon_row(logging_config={"run_log_capture": True, "mismatch_log_capture": True}, publish_schema="recon"),
        recorder,
    )

    metrics = recorder.by_name("flowx.recon.recon__rec_gate__t_ref__metrics")
    mismatch = recorder.by_name("flowx.recon.recon__rec_gate__t_ref__mismatch")
    assert metrics.temporary is False and mismatch.temporary is False

    src = recorder.by_name("_recon__rec_gate__src")
    tgt = recorder.by_name("_recon__rec_gate__t_ref__tgt")
    classified = recorder.by_name("_recon__rec_gate__t_ref__classified")
    assert src.temporary is True and tgt.temporary is True and classified.temporary is True
    # Temporary datasets keep bare, pipeline-local names -- never a qualified publish.
    for registration in (src, tgt, classified):
        assert "." not in registration.name


def test_run_log_capture_false_skips_metrics_registration(recorder):
    """Only ``__metrics`` disappears -- the two flags gate independent datasets. The companion
    flag is stated explicitly because under v1.7.3 omitting it would suppress ``__mismatch``
    too, and the assertion below would then pass for the wrong reason."""
    _register(
        _recon_row(logging_config={"run_log_capture": False, "mismatch_log_capture": True}, publish_schema="recon"),
        recorder,
    )

    assert not any(name.endswith("__metrics") for name in recorder.names)
    assert any(name.endswith("__mismatch") for name in recorder.names)


def test_mismatch_log_capture_false_skips_mismatch_registration(recorder):
    """The mirror image of the above, and explicit for the same reason."""
    _register(
        _recon_row(logging_config={"run_log_capture": True, "mismatch_log_capture": False}, publish_schema="recon"),
        recorder,
    )

    assert any(name.endswith("__metrics") for name in recorder.names)
    assert not any(name.endswith("__mismatch") for name in recorder.names)


def test_absent_logging_config_registers_neither_audit_dataset(recorder):
    """v1.7.3 default, asserted at the graph-definition layer: a flow that says nothing about
    logging gets no ``__metrics`` and no ``__mismatch``.

    ``pipeline`` (not ``pipeline_audit_only``) because audit-only with both flags resolving
    false is a rejected configuration -- see
    ``test_audit_only_with_an_absent_logging_config_is_rejected_at_graph_definition``. The
    comparison layer still registers: silence is about the AUDIT lanes, not about the work.
    """
    _register(_recon_row(execution_mode="pipeline"), recorder)

    assert not any(name.endswith("__metrics") for name in recorder.names)
    assert not any(name.endswith("__mismatch") for name in recorder.names)
    assert "_recon__rec_gate__t_ref__classified" in recorder.names


def test_pipeline_conf_override_wins_over_onboarded_logging_config(recorder):
    _register(
        _recon_row(logging_config={"run_log_capture": False, "mismatch_log_capture": True}, publish_schema="recon"),
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


def test_audit_only_with_an_absent_logging_config_is_rejected_at_graph_definition(recorder):
    """v1.7.3 makes this rejection reachable IMPLICITLY. Pre-v1.7.3 a spec had to write two
    explicit ``false`` values to hit it; now simply omitting ``logging_config`` resolves both
    flags false, and audit-only with no output is still a contradiction. Same guard, newly
    reachable by silence -- which is the case an operator is most likely to write by accident.
    """
    with pytest.raises(FrameworkConfigError) as excinfo:
        _register(_recon_row(execution_mode="pipeline_audit_only"), recorder)

    assert "pipeline_audit_only" in str(excinfo.value)
    assert recorder.registrations == [], "nothing may be registered when the flow is rejected"


_GATE_RULE = {"rules": [{"rule_id": "r1", "expression": "source_record_count > 0", "action": "fail"}]}


def test_dq_rules_without_capture_register_a_temporary_metrics_dataset(recorder):
    """v1.7.07: a flow that declares a dq gate but captures nothing and publishes nothing gets a
    pipeline-scoped ``_recon__…__metrics`` -- bare name, temporary -- so the expectation fires
    without a materialized view ever landing in a business schema. Before v1.7.07 this shape was
    rejected ("dq_config needs run_log_capture") and, once the flag was on, the metrics view was
    published into the pipeline's own schema (UC6: six of them in bronze)."""
    _register(_recon_row(execution_mode="pipeline", dq_config=_GATE_RULE), recorder)

    metrics = recorder.by_name("_recon__rec_gate__t_ref__metrics")
    assert metrics.temporary is True
    assert not any("." in name for name in recorder.names), recorder.names
    assert not any(name.endswith("__mismatch") for name in recorder.names)


def test_audit_only_dq_gate_with_run_log_capture_false_is_accepted_and_temporary(recorder):
    """Audit-only with both flags false is still rejected when the flow would register nothing
    -- but a dq gate IS an output, so with rules present the flow is accepted and its metrics
    dataset stays pipeline-scoped."""
    _register(
        _recon_row(logging_config={"run_log_capture": False, "mismatch_log_capture": False}, dq_config=_GATE_RULE),
        recorder,
    )
    metrics = recorder.by_name("_recon__rec_gate__t_ref__metrics")
    assert metrics.temporary is True


def test_run_log_capture_without_publish_schema_is_rejected_at_graph_definition(recorder):
    """The export backstop reads the PUBLISHED metrics view; without publish_schema there is
    none, so a capture flag promises an audit row that can never be written."""
    with pytest.raises(FrameworkConfigError) as excinfo:
        _register(_recon_row(logging_config={"run_log_capture": True}), recorder)
    assert "publish_schema" in str(excinfo.value)
    assert recorder.registrations == []


def test_pipeline_conf_override_turning_capture_on_without_publish_schema_is_rejected(recorder):
    """The same guard must hold when the flag arrives from the dataflow.recon.* pipeline-conf
    override rather than the spec -- onboarding never saw that value."""
    with pytest.raises(FrameworkConfigError) as excinfo:
        _register(
            _recon_row(dq_config=_GATE_RULE),
            recorder,
            log_capture_overrides={"recon_run_log_capture": True, "recon_mismatch_log": None},
        )
    assert "publish_schema" in str(excinfo.value)


def test_healing_flow_without_publish_schema_is_rejected_at_graph_definition(recorder):
    """A healing flow's prepared source/target are read back through the metastore, so they must
    be published -- and since v1.7.07 nothing is published without publish_schema."""
    with pytest.raises(FrameworkConfigError) as excinfo:
        _register(_recon_row(execution_mode="pipeline", heal=True), recorder)
    assert "publish_schema" in str(excinfo.value)
    assert recorder.registrations == []


def test_healing_flow_keeps_src_and_healing_tgt_published_for_the_handler(recorder):
    """The L5 foreach_batch_sink handler reads ``_src``/healing ``_tgt`` back via a plain
    ``spark.read.table`` (metastore resolution), so those two nodes must stay published
    qualified tables while everything else obeys the Intermediate Object Rule.

    Capture flags stated explicitly: this test asserts node publication/temporariness, which
    must hold regardless of the logging default, so it does not inherit one."""
    _register(
        _recon_row(
            execution_mode="pipeline",
            logging_config={"run_log_capture": True, "mismatch_log_capture": True},
            heal=True,
            publish_schema="recon",
        ),
        recorder,
    )

    src = recorder.by_name("flowx.recon._recon__rec_gate__src")
    tgt = recorder.by_name("flowx.recon._recon__rec_gate__t_ref__tgt")
    assert src.temporary is False and tgt.temporary is False

    classified = recorder.by_name("_recon__rec_gate__t_ref__classified")
    missing = recorder.by_name("_recon__rec_gate__t_ref__missing")
    pulse = recorder.by_name("_recon__rec_gate__pulse")
    assert classified.temporary is True and missing.temporary is True and pulse.temporary is True
    assert recorder.sinks == ["_recon__rec_gate__heal_sink"]


def test_audit_only_does_not_register_the_missing_table_even_when_a_target_heals(recorder):
    """``__missing`` exists ONLY to feed the L5 heal lane, and L5 does not register at all under
    ``pipeline_audit_only`` (``if not needs_heal: return``). So a target that *would* heal must
    still not get a ``__missing`` table in audit-only mode.

    Regression test. The gate used to be ``_wants_heal(target_config)`` alone, which ignores
    execution_mode -- so an audit-only flow materialized a full ``left_semi`` join over the whole
    source that nothing in the update ever read: dead compute, once per update per healing
    target. The fix ANDs in ``needs_heal``, which already carries the execution-mode check.
    """
    _register(
        _recon_row(
            execution_mode="pipeline_audit_only",
            logging_config={"run_log_capture": True, "mismatch_log_capture": False},
            heal=True,
            publish_schema="recon",
        ),
        recorder,
    )

    registered = set(recorder.names)
    assert "_recon__rec_gate__t_ref__missing" not in registered

    # The heal lane itself is absent too -- the miss set had no consumer to feed.
    assert "_recon__rec_gate__pulse" not in registered
    assert recorder.sinks == []

    # ...while the comparison layer this mode DOES need is still there.
    assert "_recon__rec_gate__t_ref__classified" in registered


def test_healing_flow_with_both_flags_false_still_registers_the_heal_lane(recorder):
    """A healing flow with logging suppressed must still heal: no ``__metrics``/``__mismatch``,
    but the pulse + append_flow + sink are all registered (the ordering edge anchors on the
    always-registered ``__classified``, not on ``__metrics``)."""
    _register(
        _recon_row(
            execution_mode="pipeline",
            logging_config={"run_log_capture": False, "mismatch_log_capture": False},
            heal=True,
            publish_schema="recon",
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
        "flowx.config",
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
    """``reconciliation_result`` is gated by ``run_log_capture`` exactly like
    ``reconciliation_run_log`` -- they are written together or not at all. Capture stated
    explicitly, since v1.7.3's default would otherwise write neither."""
    assert _phase_1(monkeypatch, {"run_log_capture": True}) == {"run_log": 1, "result": 1}


def test_phase_1_writes_neither_when_run_log_capture_is_false(monkeypatch):
    assert _phase_1(monkeypatch, {"run_log_capture": False}) == {"run_log": 0, "result": 0}


@pytest.mark.parametrize("logging_config", [None, {}])
def test_phase_1_writes_neither_when_logging_config_is_absent(monkeypatch, logging_config):
    """v1.7.3 default at the write layer: silence means no ``reconciliation_run_log`` and no
    ``reconciliation_result`` row, without the flow having written ``false`` anywhere."""
    assert _phase_1(monkeypatch, logging_config) == {"run_log": 0, "result": 0}


# ---------------------------------------------------------------------------------------------
# 4. The onboarding-time mirror of the graph-definition guards
# ---------------------------------------------------------------------------------------------


def _validator_errors(
    logging_config, execution_mode="pipeline_audit_only", dq_config=None, publish_schema="recon", heals=False
):
    """``publish_schema="recon"`` by default so the pre-v1.7.07 assertions keep testing what they
    always tested (the capture-flag algebra); the v1.7.07 publish rules pass ``None`` explicitly."""
    errors = []
    _validate_logging_config(
        logging_config,
        "reconciliation_flows[0].logging_config",
        errors,
        execution_mode=execution_mode,
        dq_config=dq_config,
        publish_schema=publish_schema,
        heals=heals,
    )
    return errors


def test_validator_rejects_audit_only_with_both_flags_false():
    errors = _validator_errors({"run_log_capture": False, "mismatch_log_capture": False})
    assert any("pipeline_audit_only" in error for error in errors)


def test_validator_accepts_dq_rules_with_run_log_capture_false():
    """v1.7.07: the metrics dataset is registered (temporary) for the expectations alone, so a
    dq gate no longer needs a capture flag -- the pre-v1.7.07 rejection is gone."""
    errors = _validator_errors(
        {"run_log_capture": False},
        execution_mode="pipeline",
        dq_config={"rules": [{"rule_id": "r1", "expression": "value_drift_count = 0", "action": "warn"}]},
        publish_schema=None,
    )
    assert errors == []


def test_validator_rejects_capture_without_publish_schema():
    for logging_config in ({"run_log_capture": True}, {"mismatch_log_capture": True}):
        errors = _validator_errors(logging_config, publish_schema=None)
        assert any("publish_schema" in error for error in errors), (logging_config, errors)
    # ...and the same pair is fine once the flow says where to publish.
    assert _validator_errors({"run_log_capture": True}, publish_schema="recon") == []


def test_validator_rejects_healing_flow_without_publish_schema():
    errors = _validator_errors(
        {"run_log_capture": False, "mismatch_log_capture": False},
        execution_mode="pipeline",
        publish_schema=None,
        heals=True,
    )
    assert any("append_target_table" in error and "publish_schema" in error for error in errors), errors
    # Audit-only never heals, so a healing target there needs no publish_schema for that reason.
    assert not any(
        "append_target_table" in error
        for error in _validator_errors(
            {"run_log_capture": False, "mismatch_log_capture": False},
            execution_mode="pipeline_audit_only",
            dq_config={"rules": [{"rule_id": "r1", "expression": "source_record_count > 0", "action": "fail"}]},
            publish_schema=None,
            heals=True,
        )
    )


def test_validator_accepts_audit_only_dq_gate_with_no_capture_and_no_publish_schema():
    """The UC6-shaped presence gate after v1.7.07: six count>0 gates that publish nothing."""
    errors = _validator_errors(
        None,
        execution_mode="pipeline_audit_only",
        dq_config={"rules": [{"rule_id": "r1", "expression": "source_record_count > 0", "action": "fail"}]},
        publish_schema=None,
    )
    assert errors == []


def test_validator_audit_only_needs_exactly_one_flag_resolving_true():
    """The audit-only rule is a CROSS-FIELD rule over the resolved pair, not a rule about any
    single flag: ``pipeline_audit_only`` is rejected only when BOTH flags resolve false.

    So one explicit ``false`` is accepted when the companion flag is explicitly ``true`` -- the
    flow still has one audit lane to write to. It is rejected when the companion flag merely
    defaults, because since v1.7.3 that default is ``False`` and the pair resolves to
    ``(False, False)``: an audit-only flow that persists nothing at all.
    """
    # One lane explicitly on -- accepted, either way round.
    assert _validator_errors({"run_log_capture": False, "mismatch_log_capture": True}) == []
    assert _validator_errors({"run_log_capture": True, "mismatch_log_capture": False}) == []

    # One explicit false + the other defaulting to false = both false -> rejected.
    assert any("pipeline_audit_only" in error for error in _validator_errors({"run_log_capture": False}))
    assert any("pipeline_audit_only" in error for error in _validator_errors({"mismatch_log_capture": False}))

    # Both false is fine for a plain "pipeline" healing flow -- it still appends corrections.
    assert _validator_errors({"run_log_capture": False, "mismatch_log_capture": False}, execution_mode="pipeline") == []


@pytest.mark.parametrize("logging_config", [None, {}])
def test_validator_mirrors_the_new_false_default_for_an_absent_config(logging_config):
    """An absent/empty ``logging_config`` under ``pipeline_audit_only`` resolves to
    ``(False, False)`` at run time and IS rejected at graph definition, so onboarding rejects it
    too -- otherwise the contradiction escapes the spec review that exists to catch it and
    surfaces on the first pipeline update instead.

    Falsifiable in both halves: reinstating an early return on ``logging_config is None``, or a
    hardcoded ``True`` fallback, makes ``errors`` empty and this test fail.
    """
    errors = _validator_errors(logging_config, execution_mode="pipeline_audit_only")
    assert any("pipeline_audit_only" in error for error in errors)


def test_validator_and_graph_agree_that_dq_rules_need_no_capture_flag():
    """Both layers accept a dq gate with an absent ``logging_config`` since v1.7.07 -- the graph
    registers a temporary ``__metrics`` for it (see the graph-layer test above), so onboarding
    must not reject the pair."""
    errors = _validator_errors(
        None,
        execution_mode="pipeline",
        dq_config={"rules": [{"rule_id": "r1", "expression": "value_drift_count = 0", "action": "warn"}]},
        publish_schema=None,
    )
    assert errors == []


def test_validator_default_constant_equals_the_runtime_resolver_fallback():
    """The onboarding validator's ``_DEFAULT_LOG_CAPTURE`` MUST equal layer 3 of
    ``resolve_log_capture_flags``.

    These are two independent copies of one decision, in two packages that deliberately do not
    import each other (pulling ``reconciliation`` into onboarding-time validation would drag in
    Spark). When they disagreed -- the validator hardcoding ``True`` after the runtime flipped to
    ``False`` -- a spec that simply OMITTED ``logging_config`` passed onboarding review and then
    raised ``FrameworkConfigError`` on its first pipeline update. This test is the seam that
    catches that class of drift; it is cheaper than the incident.
    """
    from flowx.lakeflow_framework.onboarding.spec_validator import (
        _DEFAULT_LOG_CAPTURE,
    )

    resolved_run, resolved_mismatch = resolve_log_capture_flags(None)
    assert _DEFAULT_LOG_CAPTURE is resolved_run
    assert _DEFAULT_LOG_CAPTURE is resolved_mismatch


def test_validator_and_graph_agree_on_a_spec_that_omits_logging_config():
    """Onboarding-time and graph-time must reach the SAME verdict for an audit-only flow that
    never mentions logging -- the exact spec shape that used to pass review and then fail.
    """
    errors = []
    _validate_logging_config(
        None, "logging_config", errors, execution_mode="pipeline_audit_only", dq_config=None
    )
    assert errors, "validator must reject audit-only + implicitly-silent, as the graph layer does"
    assert "v1.7.3" in errors[0], "the message must explain that the DEFAULT is what rejected it"
    assert "run_log_capture: true" in errors[0], "the message must name the concrete fix"
