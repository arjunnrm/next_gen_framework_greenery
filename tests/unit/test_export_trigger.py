"""``sink_config.export_trigger`` (v1.7.5) -- the update-scoped export path.

WHY THIS EXISTS. ``@dlt.append_flow`` is streaming-only, and Delta refuses to stream from a
table that is fully recomputed each update (``DELTA_SOURCE_TABLE_IGNORE_CHANGES``). Together
those made an aggregating target -- a ``materialized_view``, or any ``TRUNCATE_AND_LOAD`` flow
-- impossible to export at all: a ``GROUP BY`` result could be published and then could not
leave the platform as a file. ``export_trigger: "per_update"`` separates the TRIGGER (an
update-scoped pulse carrying no data) from the PAYLOAD (a batch ``dlt.read``), so the append
flow is genuinely streaming while the exported rows are an aggregation.

THE PROPERTY THAT MATTERS MOST is the negative one, and it is asserted first below: a sink
that does not set ``export_trigger`` takes the byte-identical pre-v1.7.5 code path. The
feature is opt-in and default-off, so absence must be indistinguishable from before.

The registration tests use a fake ``dlt`` module. That is deliberate rather than a shortcut:
the real one only exists inside a running pipeline update (there is no local Lakeflow), so a
double is the only way to assert *which datasets get registered and how they are wired*
offline. What the double cannot prove -- that Lakeflow and Delta actually accept the shape --
was verified on the live serverless runtime with a side-by-side probe: a plain `rate` pulse
gave 1 row on a fresh checkpoint and 0 on the next incremental update (wall-clock dependent);
`rate-micro-batch` gave exactly 1 row on both. The shipped pulse is rate-micro-batch.
"""

import sys
import types

import pytest

from flowx.lakeflow_framework.exceptions import FrameworkConfigError
from flowx.lakeflow_framework.onboarding import spec_validator as sv


# --------------------------------------------------------------------------- validation


def _sink_flow(**sink_config_overrides):
    sink_config = {
        "format": "pgp_zip",
        "path": "/Volumes/c/s/v/_staging/",
        "staged_file_format": "csv",
        "post_export_archive": {"enabled": True, "output_zip_path": "/Volumes/c/s/v/out/"},
    }
    sink_config.update(sink_config_overrides)
    return {
        "flow_step_id": "ts_export",
        "dataflow_id": "df_x",
        "target_catalog": "c",
        "target_schema": "gold",
        "target_table": "t",
        "target_type": "sink",
        "source_inputs": [{"input_name": "src", "table": "c.gold.upstream", "is_streaming": True}],
        "transformation_sql": "SELECT * FROM src",
        "target_config": {"cdc_load_strategy": "APPEND", "sink_config": sink_config},
    }


def _validate_sink_config(sink_config, sink_format="pgp_zip"):
    """Drive the validator's sink-config checks directly and return its error list."""
    errors = []
    cfg = dict(sink_config)
    cfg.setdefault("format", sink_format)
    cfg.setdefault("path", "/Volumes/c/s/v/_staging/")
    if cfg.get("format") == "pgp_zip":
        # pgp_zip IS the staging-then-archive sink, so archiving is mandatory -- without it
        # the validator reports an unrelated error and these assertions test nothing.
        cfg.setdefault("post_export_archive",
                       {"enabled": True, "output_zip_path": "/Volumes/c/s/v/out/"})
    sv._validate_sink_config(cfg, "target_config.sink_config", errors, "sink")
    return errors


def test_export_trigger_is_an_allowed_sink_config_key():
    assert "export_trigger" in sv.ALLOWED_SINK_CONFIG_KEYS


def test_allowed_export_triggers_are_exactly_the_two_documented_values():
    assert sv.ALLOWED_EXPORT_TRIGGERS == {"per_micro_batch", "per_update"}


@pytest.mark.parametrize("value", ["per_micro_batch", "per_update"])
def test_valid_export_trigger_is_accepted(value):
    assert _validate_sink_config({"export_trigger": value}) == []


def test_absent_export_trigger_is_accepted():
    """The default. Every pre-v1.7.5 spec omits this key and must stay valid."""
    assert _validate_sink_config({}) == []


@pytest.mark.parametrize("value", ["per_batch", "once", "PER_UPDATE", "always"])
def test_unknown_export_trigger_is_rejected(value):
    errors = _validate_sink_config({"export_trigger": value})
    assert errors, f"{value!r} should be rejected"
    assert any("export_trigger" in e for e in errors)


@pytest.mark.parametrize("fmt", ["delta", "kafka"])
def test_export_trigger_is_presence_rejected_for_native_sink_formats(fmt):
    """Same contract as staged_file_format: a native Lakeflow sink owns its own write
    cadence, so accepting a trigger there would let a spec assert a cadence nothing honours."""
    cfg = {"format": fmt, "export_trigger": "per_update"}
    if fmt == "kafka":
        cfg["kafka_options"] = {"kafka.bootstrap.servers": "h:9092", "topic": "t"}
    errors = _validate_sink_config(cfg, sink_format=fmt)
    assert any("export_trigger" in e and "pgp_zip" in e for e in errors), errors


def test_rejection_message_names_the_attribute_and_the_fix():
    errors = _validate_sink_config({"export_trigger": "hourly"})
    joined = " ".join(errors)
    assert "export_trigger" in joined
    assert "per_update" in joined and "per_micro_batch" in joined, (
        "the message must list the legal values, not merely say the value is wrong"
    )


# ------------------------------------------------------------- both gates must agree


def test_json_schema_and_validator_agree_on_the_enum():
    """These two gates drift; only the schema rejects unknown keys, only the validator
    produces the actionable message. A mismatch means a spec passes one and fails the other."""
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "onboarding_templates" / "onboarding_spec.schema.json").read_text(encoding="utf-8"))
    defs = schema.get("$defs") or schema.get("definitions")
    props = defs["sinkConfig"]["properties"]
    assert "export_trigger" in props, "export_trigger missing from the JSON schema"
    assert set(props["export_trigger"]["enum"]) == sv.ALLOWED_EXPORT_TRIGGERS
    assert set(props) == sv.ALLOWED_SINK_CONFIG_KEYS, (
        "sinkConfig properties and ALLOWED_SINK_CONFIG_KEYS have drifted"
    )


# ------------------------------------------------------------------- registration


class _FakeDlt(types.ModuleType):
    """Records what a registration call wires up, without a live Lakeflow runtime."""

    def __init__(self):
        super().__init__("dlt")
        self.tables = {}
        self.sinks = {}
        self.append_flows = {}
        self.read_calls = []
        self.read_stream_calls = []

    def table(self, name=None, temporary=False, comment=None, **kw):
        def deco(fn):
            self.tables[name] = {"temporary": temporary, "comment": comment, "fn": fn}
            return fn
        return deco

    def create_sink(self, name=None, format=None, options=None, **kw):  # noqa: A002
        self.sinks[name] = {"format": format, "options": options}

    def append_flow(self, name=None, target=None, comment=None, **kw):
        def deco(fn):
            self.append_flows[name] = {"target": target, "comment": comment, "fn": fn}
            return fn
        return deco

    def read(self, name):
        self.read_calls.append(name)
        return _FakeDF(name)

    def read_stream(self, name):
        self.read_stream_calls.append(name)
        return _FakeDF(name)


class _FakeDF:
    def __init__(self, name, columns=None):
        self.name = name
        self.columns = list(columns or [
            "targetAreaID", "telephone", "__framework_dq_quarantine_flag",
            "__framework_dq_failed_rule_ids", "__framework_dq_failure_reasons",
            "__framework_ingestion_timestamp_utc", "__framework_pipeline_run_id",
            "__framework_record_id",
        ])
        self.dropped = []

    def withColumn(self, *a, **k):
        return self

    def join(self, *a, **k):
        return self

    def drop(self, *a, **k):
        self.dropped.extend(a)
        self.columns = [c for c in self.columns if c not in a]
        return self

    def filter(self, *a, **k):
        return self

    def select(self, *a, **k):
        return self


@pytest.fixture
def fake_dlt(monkeypatch):
    from flowx.lakeflow_framework.engine import sink_registration as sr

    fake = _FakeDlt()
    monkeypatch.setitem(sys.modules, "dlt", fake)
    monkeypatch.setattr(sr, "dlt", fake, raising=False)
    monkeypatch.setattr(sr, "_register_sink_quarantine_table_if_configured",
                        lambda *a, **k: None, raising=True)

    # _create_sink registers the pgp_zip DataSource against a live SparkSession, which does
    # not exist offline. Record the call instead -- what these tests assert is the WIRING
    # (which datasets, read how), not the DataSource registration itself.
    def _fake_create_sink(flow_label, sink_name, sink_config):
        fake.sinks[sink_name] = {"format": sink_config.get("format")}

    monkeypatch.setattr(sr, "_create_sink", _fake_create_sink, raising=True)
    # The shared pulse is registered once per PROCESS; each test is its own pipeline.
    monkeypatch.setattr(sr, "_REGISTERED_PULSES", set(), raising=True)
    return fake


SINK_CFG = {
    "format": "pgp_zip",
    "path": "/Volumes/c/gold/v/_staging/",
    "staged_file_format": "csv",
    "post_export_archive": {"enabled": True, "output_zip_path": "/Volumes/c/gold/v/out/"},
}


def _register(sr, fake, **sink_overrides):
    cfg = dict(SINK_CFG)
    cfg.update(sink_overrides)
    sr.register_sink_target(
        flow_label="ts_export",
        staged_view_name="_t_staged",
        target_table="t",
        target_catalog="c",
        target_schema="gold",
        target_config={"cdc_load_strategy": "APPEND", "sink_config": cfg},
        dq_rules=[],
        is_streaming=False,   # an aggregating payload: NOT streaming
    )


def test_per_update_accepts_a_non_streaming_payload(fake_dlt):
    """The whole point: is_streaming False would raise on the default path."""
    from flowx.lakeflow_framework.engine import sink_registration as sr

    _register(sr, fake_dlt, export_trigger="per_update")
    assert "_t_sink" in fake_dlt.sinks
    assert "t_sink_flow" in fake_dlt.append_flows


def test_default_path_still_rejects_a_non_streaming_payload(fake_dlt):
    """REGRESSION GUARD. Without export_trigger the pre-v1.7.5 guard must still fire --
    otherwise this feature would have silently weakened every existing sink."""
    from flowx.lakeflow_framework.engine import sink_registration as sr

    with pytest.raises(FrameworkConfigError) as exc:
        _register(sr, fake_dlt)
    assert "streaming" in str(exc.value).lower()


def test_explicit_per_micro_batch_behaves_exactly_like_absence(fake_dlt):
    from flowx.lakeflow_framework.engine import sink_registration as sr

    with pytest.raises(FrameworkConfigError):
        _register(sr, fake_dlt, export_trigger="per_micro_batch")


def test_default_path_registers_no_pulse_table(fake_dlt):
    """A pre-v1.7.5 spec must produce the same dataset set as before -- no extra node."""
    from flowx.lakeflow_framework.engine import sink_registration as sr

    _register(sr, fake_dlt, export_trigger=None) if False else None
    cfg = dict(SINK_CFG)
    sr.register_sink_target(
        flow_label="ts_export", staged_view_name="_t_staged", target_table="t",
        target_catalog="c", target_schema="gold",
        target_config={"cdc_load_strategy": "APPEND", "sink_config": cfg},
        dq_rules=[], is_streaming=True,
    )
    assert fake_dlt.tables == {}, "the default path must register no additional dataset"
    assert "t_sink_flow" in fake_dlt.append_flows
    fake_dlt.append_flows["t_sink_flow"]["fn"]()   # bodies are lazy until invoked
    assert fake_dlt.read_stream_calls == ["_t_staged"], (
        "the default path must stream the staged view directly, as before"
    )
    assert fake_dlt.read_calls == [], "the default path performs no batch read"


def test_per_update_registers_a_temporary_pulse_table(fake_dlt):
    from flowx.lakeflow_framework.engine import sink_registration as sr

    _register(sr, fake_dlt, export_trigger="per_update")
    assert sr.EXPORT_PULSE_TABLE == "_flowx_export_pulse"
    assert sr.EXPORT_PULSE_TABLE in fake_dlt.tables
    assert fake_dlt.tables[sr.EXPORT_PULSE_TABLE]["temporary"] is True, (
        "the pulse is plumbing, not a published dataset"
    )


def test_per_update_reads_payload_as_batch_and_pulse_as_stream(fake_dlt):
    """The core wiring: batch payload (so an aggregation is legal), streaming pulse (so the
    append flow satisfies Lakeflow). Reversing these is the bug this feature exists to avoid."""
    from flowx.lakeflow_framework.engine import sink_registration as sr

    _register(sr, fake_dlt, export_trigger="per_update")
    fake_dlt.append_flows["t_sink_flow"]["fn"]()
    assert "_t_staged" in fake_dlt.read_calls, "payload must be read with dlt.read (batch)"
    assert sr.EXPORT_PULSE_TABLE in fake_dlt.read_stream_calls, "pulse must be read as a stream"
    assert "_t_staged" not in fake_dlt.read_stream_calls, (
        "streaming the aggregating payload is exactly what fails at runtime"
    )


def test_per_update_payload_uses_dlt_read_not_spark_read_table(fake_dlt):
    """Single-Read DAG mandate rule 2: lineage goes through dlt.read, never a bare
    spark.read.table that would bypass the graph and hide the edge from the DAG."""
    from flowx.lakeflow_framework.engine import sink_registration as sr
    import inspect

    src = inspect.getsource(sr.register_per_update_sink_target)
    # Strip the docstring: it *discusses* spark.read.table to explain why it is not used, so
    # matching raw source would fail on the explanation rather than on real code.
    body = src.split('"""', 2)[-1]
    # Strip comments too: the code *explains* why spark.read.table is not used, so matching
    # raw text would fail on the explanation rather than on an actual call.
    code = " ".join(line.split("#", 1)[0] for line in body.splitlines())
    assert "spark.read.table" not in code, "the payload must not bypass the Lakeflow graph"
    assert "dlt.read(staged_view_name)" in code


def test_two_per_update_sinks_share_one_pulse(fake_dlt):
    """Four UC6 sinks used to register four identical rate-micro-batch streams. One pulse per
    pipeline is enough -- it carries no data -- and dlt would reject a duplicate table name
    anyway, so the second registration must reuse, not re-register."""
    from flowx.lakeflow_framework.engine import sink_registration as sr

    for tbl in ("t1", "t2"):
        sr.register_sink_target(
            flow_label="ts_" + tbl, staged_view_name=f"_{tbl}_staged", target_table=tbl,
            target_catalog="c", target_schema="gold",
            target_config={"cdc_load_strategy": "APPEND",
                           "sink_config": dict(SINK_CFG, export_trigger="per_update")},
            dq_rules=[], is_streaming=False,
        )
    pulses = [n for n in fake_dlt.tables if "pulse" in n]
    assert pulses == [sr.EXPORT_PULSE_TABLE], f"expected exactly one shared pulse, got {pulses}"
    assert set(fake_dlt.sinks) == {"_t1_sink", "_t2_sink"}
    for flow in ("t1_sink_flow", "t2_sink_flow"):
        fake_dlt.append_flows[flow]["fn"]()
    assert fake_dlt.read_stream_calls.count(sr.EXPORT_PULSE_TABLE) == 2, "both sinks read the one pulse"


def test_per_update_rejects_non_pgp_zip_formats(fake_dlt):
    from flowx.lakeflow_framework.engine import sink_registration as sr

    with pytest.raises(FrameworkConfigError) as exc:
        sr.register_per_update_sink_target(
            flow_label="ts_export", staged_view_name="_t_staged", target_table="t",
            target_catalog="c", target_schema="gold",
            target_config={"sink_config": {"format": "delta", "path": "/p"}},
            dq_rules=[], is_streaming=False,
        )
    assert "pgp_zip" in str(exc.value)


def test_per_update_still_filters_quarantined_rows(fake_dlt):
    """An export must never leak quarantined rows or _dq_* process columns, whatever
    triggered it."""
    from flowx.lakeflow_framework.engine import sink_registration as sr
    import inspect

    src = inspect.getsource(sr.register_per_update_sink_target)
    assert "__framework_dq_quarantine_flag" in src
    # Since the framework-column fix the process columns are dropped by prefix, together with
    # every other __framework_* column, rather than by the fixed _QUARANTINE_PROCESS_COLUMNS list.
    assert "_strip_framework_columns(" in src


def test_pulse_gate_column_is_framework_prefixed():
    """It is joined on and then dropped; a bare name could collide with a business column."""
    from flowx.lakeflow_framework.engine import sink_registration as sr

    assert sr._PULSE_GATE_COLUMN.startswith("__framework_")


def test_pulse_is_independent_of_every_business_feed():
    """The refutation that killed the earlier design: a pulse derived from a business stream
    fires per micro-batch, so an update ingesting nothing would silently produce NO export."""
    from flowx.lakeflow_framework.engine import sink_registration as sr
    import inspect

    # The pulse body lives in the shared register-once helper, not in the per-sink function.
    src = inspect.getsource(sr._register_shared_export_pulse)
    # Assert on CODE, not prose: the docstring and comments discuss `rate` and `.limit()` to
    # explain why they are NOT used, so matching raw source would fail on the explanation.
    body = src.split('"""', 2)[-1]
    src = " ".join(line.split("#", 1)[0] for line in body.splitlines())
    assert 'format("rate-micro-batch")' in src, (
        "the pulse must be rate-micro-batch: deterministic rows per batch. The plain rate "
        "source is wall-clock dependent and produced 0 rows on an incremental update live."
    )
    assert 'format("rate")' not in src, "plain rate source is a race -- must not come back"
    assert ".limit(" not in src, "no streaming LIMIT on the pulse"
    assert "bind(" not in src, "binding a business source would make the pulse data-driven"

# --------------------------------------------------------- no framework columns leak


def test_strip_framework_columns_drops_every_prefixed_column():
    from flowx.lakeflow_framework.engine import sink_registration as sr

    out = sr._strip_framework_columns(_FakeDF("x"))
    assert out.columns == ["targetAreaID", "telephone"], out.columns


def test_strip_framework_columns_is_a_no_op_without_prefixed_columns():
    from flowx.lakeflow_framework.engine import sink_registration as sr

    df = _FakeDF("x", columns=["a", "b"])
    assert sr._strip_framework_columns(df) is df and df.dropped == []


@pytest.mark.parametrize("trigger", [None, "per_update"])
def test_sink_export_carries_only_business_columns(fake_dlt, trigger):
    """Found live: UC6's first four export files had the header
    targetAreaID|telephone|__framework_ingestion_timestamp_utc|__framework_pipeline_run_id|__framework_record_id.
    A supplier file with a contractual layout must carry ONLY the projected business columns --
    on BOTH trigger paths."""
    from flowx.lakeflow_framework.engine import sink_registration as sr

    cfg = dict(SINK_CFG, **({"export_trigger": trigger} if trigger else {}))
    sr.register_sink_target(
        flow_label="ts_export", staged_view_name="_t_staged", target_table="t",
        target_catalog="c", target_schema="gold",
        target_config={"cdc_load_strategy": "APPEND", "sink_config": cfg},
        dq_rules=[], is_streaming=(trigger is None),
    )
    out = fake_dlt.append_flows["t_sink_flow"]["fn"]()
    leaked = [c for c in out.columns if c.startswith("__framework_")]
    assert leaked == [], f"framework columns leaked into the export: {leaked}"
    assert out.columns == ["targetAreaID", "telephone"]

