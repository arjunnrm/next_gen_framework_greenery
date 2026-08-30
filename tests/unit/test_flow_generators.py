"""First unit coverage of ``engine/flow_generators.py`` -- the ~130 lines of ordering-critical
ingestion/transformation logic that lived inside
``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` until v1.5.0 and that nothing could
import, and therefore nothing ever tested.

WHAT IS PROVEN HERE
-------------------
1. **The ingestion overlay chain's ORDER.** ``schema_config`` -> ``normalize_column_names`` ->
   ``attach_technical_metadata`` -> ``parse_json_string_columns`` -> ``apply_explode_columns``
   -> ``apply_stream_dedup`` -> ``apply_data_standardization_sql``. Every adjacent pair has a
   documented reason (raw-vs-normalized column names; structs must exist before explode
   consumes them; dedup must see post-explode rows and pre-standardization determinism), and
   none of those reasons is visible from the call site -- a "tidy-up" reorder would look
   harmless and break real data.
2. **``resolve_auto_flatten_all`` is fed the whole ``source_config`` dict**, not
   ``source_config.get("auto_flatten_all", False)``. The absent-vs-present-but-empty
   distinction on ``explode_columns`` is what stops silent cartesian row explosion on
   un-configured sources, and a ``.get()`` collapses the two.
3. **The transformation ``is_streaming`` rule** -- ``target_type == "streaming_table"`` **or**
   ``any(input.is_streaming)``. Dropping the second half raised a live
   ``AnalysisException: View '...' is a streaming view and must be referenced using
   readStream``.
4. **``register_staged_view(materialize=...)`` is ``True`` exactly when** the staged
   intermediate has more than one reader: ``action: "quarantine"`` DQ rules, or a
   ``sink``/``external_sink`` target -- and the generator hands ``register_flow_output`` the
   *return value* of ``register_staged_view`` (the qualified table name when materialized),
   never the bare ``_<target_table>_staged`` string.
5. **``bind()`` receives the right ``consumer_id`` for each flow kind** -- ``"<id>:source"``
   for ingestion, ``"<step>:input:<input_name>"`` for a transformation input, and
   ``"<recon>:source"`` / ``"<recon>:target:<target_id>"`` for reconciliation. ``bind`` raises
   on an unknown id rather than guessing, so a drifted id is a hard pipeline failure.

HOW IT RUNS WITHOUT A PIPELINE
------------------------------
The ``dlt``-recorder fixtures are the same ones ``tests/unit/test_read_once_wiring.py``
introduced: ``dlt.enable_local_execution()`` flips ``dlt.api.LOCAL_EXECUTION_MODE``, a plain
module-global with no reset API, so it is snapshotted through ``monkeypatch`` *before* being
enabled and restored at teardown; ``dlt.table`` / ``dlt.view`` / ``dlt.append_flow`` /
``dlt.read`` / ``dlt.read_stream`` are replaced with recorders that capture a registration's
closure without executing it (which is exactly Lakeflow's own behaviour, and is what lets each
test decide precisely which bodies to evaluate).

Stubs are the repo's duck-typed ``_StubRow``/``_StubDataFrame``/``_StubSpark`` idiom, never
``unittest.mock.MagicMock``: a MagicMock answers every attribute affirmatively, which would hide
exactly the mistakes this module hunts for -- an overlay that was never called, a keyword that
was never passed, a consumer id that never reached ``bind``.
"""

import json

import dlt
import dlt.api
import pytest

from NextGen_Metadata_Framework.lakeflow_framework.engine import flow_generators
from NextGen_Metadata_Framework.lakeflow_framework.engine import source_plane
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.json_flattening import resolve_auto_flatten_all
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation import graph_registration
from NextGen_Metadata_Framework.lakeflow_framework.transformation import inputs as transformation_inputs

# ---------------------------------------------------------------------------------------------
# The exact overlay order ``generate_ingestion_flow._build_ingestion_dataframe`` must apply.
# Written out as data rather than asserted inline so a reordering diff shows up here, on one
# reviewable line, instead of being buried in an assertion message.
# ---------------------------------------------------------------------------------------------

EXPECTED_OVERLAY_ORDER = [
    "apply_schema_config",
    "normalize_column_names",
    "attach_technical_metadata",
    "parse_json_string_columns",
    "apply_explode_columns",
    "apply_stream_dedup",
    "apply_data_standardization_sql",
]

STUB_COLUMNS = ["order_id", "amount", "status"]

QUARANTINE_RULE = {"rule_id": "amount_positive", "expression": "amount > 0", "action": "quarantine"}
WARN_RULE = {"rule_id": "status_known", "expression": "status IS NOT NULL", "action": "warn"}


# ---------------------------------------------------------------------------------------------
# Duck-typed stubs
# ---------------------------------------------------------------------------------------------


class _StubRow:
    """A control-table row: attribute access over a fixed field set, ``AttributeError`` for a
    genuinely absent field -- so a ``getattr(row, ..., default)`` in the code under test
    exercises its default branch exactly as it would against a pre-migration control table."""

    def __init__(self, **fields):
        self._fields = dict(fields)

    def __getattr__(self, name):
        try:
            return self._fields[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class _StubDataFrame:
    """A lazily-permissive DataFrame: every unknown method returns ``self``.

    ``columns`` is a real list because the code under test genuinely reads it
    (``attach_framework_ingestion_timestamp`` checks for an existing
    ``__framework_ingestion_timestamp_utc``).
    """

    def __init__(self, origin):
        self.origin = origin
        self.columns = list(STUB_COLUMNS)
        self.operations = []

    def __getattr__(self, name):
        def _lazy(*_args, **_kwargs):
            self.operations.append(name)
            return self

        return _lazy


class _StubSpark:
    """Only ``sql()`` is ever reached: the ingestion generator's read goes through ``bind``,
    and the transformation generator's only session use is ``spark.sql(resolved_sql)``."""

    def __init__(self):
        self.sql_calls = []

    def sql(self, statement):
        self.sql_calls.append(statement)
        return _StubDataFrame(f"spark.sql:{statement}")


class _Registration:
    """One recorded ``dlt.table``/``dlt.view``/``dlt.append_flow`` registration."""

    def __init__(self, kind, name, comment, fn):
        self.kind = kind
        self.name = name
        self.comment = comment
        self.fn = fn

    def __repr__(self):  # pragma: no cover -- assertion output only
        return f"_Registration(kind={self.kind!r}, name={self.name!r})"


class _DltRecorder:
    """Stand-in for the five ``dlt`` entry points this wiring uses. A registration records the
    closure but does NOT execute it -- Lakeflow's own behaviour."""

    def __init__(self):
        self.registrations = []
        self.reads = []
        self.read_streams = []

    def _register(self, kind, name, comment):
        def _decorator(fn):
            self.registrations.append(_Registration(kind, name or fn.__name__, comment, fn))
            return fn

        return _decorator

    def table(self, query_function=None, name=None, comment=None, **_kwargs):
        decorator = self._register("table", name, comment)
        return decorator(query_function) if query_function is not None else decorator

    def view(self, query_function=None, name=None, comment=None, **_kwargs):
        decorator = self._register("view", name, comment)
        return decorator(query_function) if query_function is not None else decorator

    def append_flow(self, name=None, target=None, comment=None, **_kwargs):
        return self._register("append_flow", name, comment)

    def read(self, name):
        self.reads.append(name)
        return _StubDataFrame(f"dlt.read:{name}")

    def read_stream(self, name):
        self.read_streams.append(name)
        return _StubDataFrame(f"dlt.read_stream:{name}")

    @property
    def names(self):
        return [registration.name for registration in self.registrations]

    def by_name(self, name):
        matches = [registration for registration in self.registrations if registration.name == name]
        assert len(matches) == 1, f"expected exactly one registration named {name!r}, got {matches}"
        return matches[0]


class _BindRecorder:
    """Records every ``(consumer_id, want_stream)`` pair the generators hand to
    :func:`~engine.source_plane.bind`, and answers with a stub DataFrame.

    The real ``bind`` raises ``FrameworkConfigError`` on an unknown consumer id -- it never
    guesses -- so the ids recorded here are precisely the ids that must exist in the plan built
    by ``plan_source_plane``. That is the contract ``engine/flow_generators.py``'s module
    docstring calls out as "must stay byte-identical".
    """

    def __init__(self):
        self.calls = []

    def __call__(self, _plan, consumer_id, want_stream=False):
        self.calls.append({"consumer_id": consumer_id, "want_stream": want_stream})
        return _StubDataFrame(f"bind:{consumer_id}")

    @property
    def consumer_ids(self):
        return [call["consumer_id"] for call in self.calls]


class _OverlayRecorder:
    """Records the overlay chain in call order, plus each overlay's own arguments."""

    def __init__(self):
        self.order = []
        self.calls = {}

    def install(self, monkeypatch, name, result_factory=None):
        def _overlay(df, *args, **kwargs):
            self.order.append(name)
            self.calls.setdefault(name, []).append({"df": df, "args": args, "kwargs": kwargs})
            if result_factory is not None:
                return result_factory(df, *args, **kwargs)
            return _StubDataFrame(f"after:{name}")

        monkeypatch.setattr(flow_generators, name, _overlay)


class _StagedViewCall:
    """One recorded ``register_staged_view`` invocation."""

    def __init__(self, args, kwargs, returns):
        self.args = args
        self.kwargs = kwargs
        self.returns = returns

    @property
    def staged_view_name(self):
        return self.args[0]

    @property
    def dq_rules(self):
        return self.args[2]

    @property
    def build_dataframe(self):
        return self.args[3]


# ---------------------------------------------------------------------------------------------
# Fixtures -- the monkeypatched-dlt recorder pair introduced by test_read_once_wiring.py
# ---------------------------------------------------------------------------------------------


@pytest.fixture()
def dlt_local_execution(monkeypatch):
    """Enable ``dlt``'s local-execution mode, and guarantee it is switched back off.

    ``dlt.enable_local_execution()`` sets ``dlt.api.LOCAL_EXECUTION_MODE = True`` and offers no
    way to unset it, so without the two ``monkeypatch.setattr`` snapshots below this fixture
    would permanently flip the flag for every test module that runs after it in the same pytest
    process. ``dlt/__init__.py`` does ``from .api import *``, which binds its own module-level
    copy of the flag, so both bindings are snapshotted.
    """
    monkeypatch.setattr(dlt.api, "LOCAL_EXECUTION_MODE", dlt.api.LOCAL_EXECUTION_MODE, raising=False)
    monkeypatch.setattr(dlt, "LOCAL_EXECUTION_MODE", dlt.LOCAL_EXECUTION_MODE, raising=False)
    dlt.enable_local_execution()
    assert dlt.api.LOCAL_EXECUTION_MODE is True


@pytest.fixture()
def recorder(monkeypatch, dlt_local_execution):
    """Replace the five ``dlt`` entry points with recorders and return the recorder."""
    dlt_recorder = _DltRecorder()
    monkeypatch.setattr(dlt, "table", dlt_recorder.table)
    monkeypatch.setattr(dlt, "view", dlt_recorder.view)
    monkeypatch.setattr(dlt, "append_flow", dlt_recorder.append_flow)
    monkeypatch.setattr(dlt, "read", dlt_recorder.read)
    monkeypatch.setattr(dlt, "read_stream", dlt_recorder.read_stream)
    return dlt_recorder


@pytest.fixture()
def bind_recorder(monkeypatch):
    """Replace ``bind`` in all THREE importing namespaces.

    ``engine/flow_generators.py``, ``transformation/inputs.py`` and
    ``reconciliation/graph_registration.py`` each do ``from ...source_plane import bind``, which
    binds its own module-level reference -- patching only ``source_plane.bind`` would silently
    miss every call site and the test would pass while recording nothing.
    """
    recorder = _BindRecorder()
    monkeypatch.setattr(flow_generators, "bind", recorder)
    monkeypatch.setattr(transformation_inputs, "bind", recorder)
    monkeypatch.setattr(graph_registration, "bind", recorder)
    monkeypatch.setattr(source_plane, "bind", recorder)
    return recorder


@pytest.fixture()
def flow_output_calls(monkeypatch):
    """Capture ``register_flow_output`` instead of registering real target datasets -- this
    module is about what the generators DECIDE, not about the output registrar (which
    ``tests/unit/test_column_ordering.py`` and friends already cover)."""
    calls = []

    def _register_flow_output(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return True

    monkeypatch.setattr(flow_generators, "register_flow_output", _register_flow_output)
    return calls


@pytest.fixture()
def staged_view_calls(monkeypatch):
    """Wrap (not replace) ``register_staged_view``: the real one still runs against the ``dlt``
    recorder -- so ``materialize`` is observable as a ``@dlt.table`` vs ``@dlt.view``
    registration -- while every argument it received is captured for direct assertion."""
    real_register_staged_view = flow_generators.register_staged_view
    calls = []

    def _register_staged_view(*args, **kwargs):
        returns = real_register_staged_view(*args, **kwargs)
        calls.append(_StagedViewCall(args, kwargs, returns))
        return returns

    monkeypatch.setattr(flow_generators, "register_staged_view", _register_staged_view)
    return calls


@pytest.fixture()
def overlays(monkeypatch):
    """Replace every ingestion overlay with a recorder, in ``flow_generators``' own namespace."""
    recorder = _OverlayRecorder()
    monkeypatch.setattr(
        flow_generators,
        "load_schema_config",
        lambda _dbutils, path: {"schema_config_path": path},
    )
    for name in EXPECTED_OVERLAY_ORDER:
        recorder.install(monkeypatch, name)
    return recorder


# ---------------------------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------------------------


def _ingestion_row(
    *,
    source_config=None,
    target_config=None,
    dq_config=None,
    target_type="streaming_table",
    dataflow_id="ing_orders",
):
    return _StubRow(
        dataflow_id=dataflow_id,
        source_config_json=json.dumps(source_config if source_config is not None else {}),
        target_config_json=json.dumps(target_config if target_config is not None else {}),
        dq_config_json=json.dumps(dq_config if dq_config is not None else {}),
        target_catalog="metaflow",
        target_schema="bronze",
        target_table="orders",
        target_type=target_type,
        cdc_load_strategy="APPEND",
        source_description="orders landing zone",
    )


def _transformation_row(
    *,
    source_inputs,
    target_type="table",
    dq_config=None,
    flow_step_id="tf_enrich",
):
    return _StubRow(
        flow_step_id=flow_step_id,
        source_inputs_json=json.dumps(source_inputs),
        target_config_json=json.dumps({}),
        dq_config_json=json.dumps(dq_config if dq_config is not None else {}),
        transformation_sql="SELECT * FROM orders_in",
        target_catalog="metaflow",
        target_schema="silver",
        target_table="enriched_orders",
        target_type=target_type,
        cdc_load_strategy="APPEND",
    )


def _run_ingestion(row, **kwargs):
    flow_generators.generate_ingestion_flow(
        _StubSpark(),
        object(),  # dbutils -- only load_schema_config touches it, and that is stubbed
        row,
        plan=object(),  # bind is recorded; the plan itself is never inspected by the generator
        pipeline_parameters={},
        **kwargs,
    )


def _run_transformation(row, spark=None, **kwargs):
    flow_generators.generate_transformation_flow(
        spark or _StubSpark(),
        None,  # dbutils
        row,
        plan=object(),
        pipeline_parameters={},
        **kwargs,
    )


# ---------------------------------------------------------------------------------------------
# 1. The ingestion overlay chain's order
# ---------------------------------------------------------------------------------------------


def test_ingestion_overlay_chain_runs_in_the_documented_order(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """schema_config -> normalize -> technical metadata -> parse JSON -> explode -> dedup ->
    standardization SQL. See ``engine/flow_generators.py``'s "What must not be cleaned up"
    note 1 for why each adjacent pair is load-bearing."""
    _run_ingestion(
        _ingestion_row(
            source_config={
                "path": "/Volumes/metaflow/landing/orders",
                "format": "json",
                "schema_config_path": "/Volumes/metaflow/config/orders_schema.json",
                "json_string_columns": ["payload"],
                "explode_columns": ["line_items"],
            }
        )
    )

    staged_view_calls[0].build_dataframe()
    assert overlays.order == EXPECTED_OVERLAY_ORDER


def test_schema_config_overlay_is_skipped_when_no_schema_config_path_is_configured(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """``apply_schema_config`` is conditional on ``source_config.schema_config_path``; the other
    six overlays are unconditional (each is a documented no-op on absent configuration)."""
    _run_ingestion(_ingestion_row(source_config={"path": "/Volumes/metaflow/landing/orders"}))

    staged_view_calls[0].build_dataframe()
    assert overlays.order == [name for name in EXPECTED_OVERLAY_ORDER if name != "apply_schema_config"]


def test_each_overlay_consumes_the_previous_overlays_output(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """The chain is a genuine pipeline, not seven independent calls against the bound DataFrame:
    overlay *n* must receive overlay *n-1*'s result. A copy-paste slip that re-passed the
    original ``staged_df`` would silently drop every earlier overlay's work."""
    _run_ingestion(
        _ingestion_row(
            source_config={
                "path": "/Volumes/metaflow/landing/orders",
                "schema_config_path": "/Volumes/metaflow/config/orders_schema.json",
            }
        )
    )

    staged_view_calls[0].build_dataframe()

    first = overlays.calls["apply_schema_config"][0]["df"]
    assert first.origin == "bind:ing_orders:source"
    for previous, current in zip(EXPECTED_OVERLAY_ORDER, EXPECTED_OVERLAY_ORDER[1:]):
        assert overlays.calls[current][0]["df"].origin == f"after:{previous}", (
            f"{current} did not consume {previous}'s output"
        )


def test_json_string_columns_and_standardization_sql_are_passed_through_verbatim(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """Both overlays take a *value pulled out of* ``source_config`` (unlike explode, below)."""
    _run_ingestion(
        _ingestion_row(
            source_config={
                "json_string_columns": ["payload"],
                "data_standardization_sql": "SELECT *, upper(status) AS status FROM {df}",
            }
        )
    )
    staged_view_calls[0].build_dataframe()

    assert overlays.calls["parse_json_string_columns"][0]["args"] == (["payload"],)
    assert overlays.calls["apply_data_standardization_sql"][0]["args"] == (
        "SELECT *, upper(status) AS status FROM {df}",
    )


def test_stream_dedup_receives_the_whole_source_config_dict(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """``apply_stream_dedup`` reads ``remove_dups`` (and its companions) off the dict itself."""
    source_config = {"remove_dups": True}
    _run_ingestion(_ingestion_row(source_config=source_config))
    staged_view_calls[0].build_dataframe()

    assert overlays.calls["apply_stream_dedup"][0]["args"] == (source_config,)


# ---------------------------------------------------------------------------------------------
# 2. resolve_auto_flatten_all -- present vs absent
# ---------------------------------------------------------------------------------------------


def _recorded_auto_flatten_all(overlays):
    args = overlays.calls["apply_explode_columns"][0]["args"]
    assert len(args) == 2, f"expected (explode_columns, auto_flatten_all), got {args!r}"
    return args[1]


@pytest.mark.parametrize(
    "source_config, expected",
    [
        # explode_columns ABSENT -- schema-preserving pass-through. This is the case a naive
        # `.get("explode_columns", [])` would turn into "auto-flatten everything", i.e. silent
        # cartesian row explosion on every un-configured source.
        ({}, False),
        ({"explode_columns": None}, False),  # explicit JSON null: how a scaffolded template
        # spells "not configured"
        # explode_columns PRESENT-but-empty -- an operator deliberately asking for everything.
        ({"explode_columns": []}, True),
        # An explicit auto_flatten_all wins on its own, whatever explode_columns says.
        ({"auto_flatten_all": True}, True),
        ({"auto_flatten_all": False}, False),
        # ...and a PRESENT-but-empty explode_columns still means "flatten everything" even
        # alongside an explicit `auto_flatten_all: false`: presence of the empty list is the
        # statement, exactly as the framework's "presence, not truthiness" rule requires.
        ({"explode_columns": [], "auto_flatten_all": False}, True),
    ],
)
def test_generator_resolves_auto_flatten_all_from_the_raw_source_config(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls, source_config, expected
):
    _run_ingestion(_ingestion_row(source_config=source_config))
    staged_view_calls[0].build_dataframe()

    assert _recorded_auto_flatten_all(overlays) is expected
    # The generator's value and the resolver's value must never diverge.
    assert resolve_auto_flatten_all(source_config) is expected


def test_absent_explode_columns_behaves_differently_from_a_present_empty_list(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """The single distinction the whole ``resolve_auto_flatten_all(source_config)`` signature
    exists to preserve. ``source_config.get("explode_columns")`` returns ``None`` for BOTH an
    absent key and an explicit null, and ``[]``/``None`` are equally falsy -- so only an ``in``
    test against the raw dict can tell "the operator asked for everything" from "the operator
    said nothing"."""
    _run_ingestion(_ingestion_row(source_config={}, dataflow_id="ing_absent"))
    staged_view_calls[0].build_dataframe()
    absent = _recorded_auto_flatten_all(overlays)

    overlays.order.clear()
    overlays.calls.clear()
    _run_ingestion(_ingestion_row(source_config={"explode_columns": []}, dataflow_id="ing_present"))
    staged_view_calls[1].build_dataframe()
    present_but_empty = _recorded_auto_flatten_all(overlays)

    assert absent is False
    assert present_but_empty is True
    assert absent is not present_but_empty


def test_explode_columns_list_is_passed_through_unmodified(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    _run_ingestion(_ingestion_row(source_config={"explode_columns": ["line_items", "addresses"]}))
    staged_view_calls[0].build_dataframe()

    assert overlays.calls["apply_explode_columns"][0]["args"][0] == ["line_items", "addresses"]
    assert _recorded_auto_flatten_all(overlays) is False


# ---------------------------------------------------------------------------------------------
# 3. The transformation is_streaming propagation rule
# ---------------------------------------------------------------------------------------------


def _flow_output_is_streaming(flow_output_calls):
    """``register_flow_output``'s ``is_streaming`` is positional argument index 9."""
    return flow_output_calls[0]["args"][9]


@pytest.mark.parametrize(
    "target_type, input_streaming_flags, expected",
    [
        # target_type alone.
        ("streaming_table", [False, False], True),
        # ANY streaming input propagates, whatever this flow's own target_type is -- Spark
        # propagates streaming through the whole query plan once one input is streaming.
        # Dropping this half raised a live "AnalysisException: View '...' is a streaming view
        # and must be referenced using readStream".
        ("table", [False, True], True),
        ("materialized_view", [True], True),
        ("external_sink", [True], True),
        # Neither half -- a genuinely batch transformation.
        ("table", [False, False], False),
        ("table", [], False),
    ],
)
def test_transformation_is_streaming_is_target_type_or_any_streaming_input(
    recorder, bind_recorder, staged_view_calls, flow_output_calls, target_type, input_streaming_flags, expected
):
    source_inputs = [
        {"input_name": f"in_{index}", "table": f"metaflow.silver.t{index}", "is_streaming": flag}
        for index, flag in enumerate(input_streaming_flags)
    ]
    _run_transformation(_transformation_row(source_inputs=source_inputs, target_type=target_type))

    assert _flow_output_is_streaming(flow_output_calls) is expected


def test_transformation_input_without_an_is_streaming_key_is_treated_as_batch(
    recorder, bind_recorder, staged_view_calls, flow_output_calls
):
    """``is_streaming`` is optional on a ``source_inputs`` entry; absent means batch. It must not
    be conflated with "unknown, assume streaming" -- that would make every batch transformation
    a streaming view."""
    _run_transformation(
        _transformation_row(
            source_inputs=[{"input_name": "orders_in", "table": "metaflow.bronze.orders"}],
            target_type="table",
        )
    )

    assert _flow_output_is_streaming(flow_output_calls) is False


def test_ingestion_is_streaming_comes_from_target_type_alone(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """An ingestion flow has no ``source_inputs`` to propagate from: its staged view is
    streaming exactly when ``target_type == "streaming_table"``, and that same decision is what
    it asks ``bind`` for."""
    _run_ingestion(_ingestion_row(target_type="streaming_table"))
    assert _flow_output_is_streaming(flow_output_calls) is True
    assert bind_recorder.calls == []  # nothing bound until the staged body is evaluated

    staged_view_calls[0].build_dataframe()
    assert bind_recorder.calls[0]["want_stream"] is True

    flow_output_calls.clear()
    bind_recorder.calls.clear()
    _run_ingestion(_ingestion_row(target_type="table"))
    assert _flow_output_is_streaming(flow_output_calls) is False
    staged_view_calls[1].build_dataframe()
    assert bind_recorder.calls[0]["want_stream"] is False


# ---------------------------------------------------------------------------------------------
# 4. materialize= is True exactly when the staged intermediate has a second reader
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "target_type, dq_rules, expected",
    [
        # One reader -- a @dlt.view, inlined into its single consumer.
        ("streaming_table", [], False),
        ("table", [WARN_RULE], False),  # warn/drop/fail are native expectations on the
        # existing read; they add no reader
        # Two readers -- dq/quarantine.py reads the staged intermediate once for the clean side
        # and again for the quarantine sibling.
        ("streaming_table", [QUARANTINE_RULE], True),
        ("table", [WARN_RULE, QUARANTINE_RULE], True),
        # Two readers -- engine/sink_registration.py reads it again.
        ("sink", [], True),
        ("external_sink", [], True),
        ("external_sink", [QUARANTINE_RULE], True),
    ],
)
def test_ingestion_materializes_the_staged_view_exactly_when_it_has_a_second_reader(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls, target_type, dq_rules, expected
):
    _run_ingestion(_ingestion_row(target_type=target_type, dq_config={"rules": dq_rules}))

    assert staged_view_calls[0].kwargs["materialize"] is expected


@pytest.mark.parametrize(
    "target_type, dq_rules, expected",
    [
        ("table", [], False),
        ("streaming_table", [WARN_RULE], False),
        ("table", [QUARANTINE_RULE], True),
        ("sink", [], True),
        ("external_sink", [], True),
    ],
)
def test_transformation_materializes_the_staged_view_by_the_same_rule(
    recorder, bind_recorder, staged_view_calls, flow_output_calls, target_type, dq_rules, expected
):
    _run_transformation(
        _transformation_row(
            source_inputs=[{"input_name": "orders_in", "table": "metaflow.bronze.orders"}],
            target_type=target_type,
            dq_config={"rules": dq_rules},
        )
    )

    assert staged_view_calls[0].kwargs["materialize"] is expected


def test_materialized_staged_view_is_registered_as_a_qualified_dlt_table(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    """A materialized staged intermediate is a real ``@dlt.table``, and an UNQUALIFIED name
    would silently land in the pipeline's default catalog/schema instead of this flow's target
    -- so the generator must forward ``target_catalog``/``target_schema`` too."""
    _run_ingestion(_ingestion_row(target_type="sink"))

    call = staged_view_calls[0]
    assert call.staged_view_name == "_orders_staged"
    assert call.kwargs["target_catalog"] == "metaflow"
    assert call.kwargs["target_schema"] == "bronze"
    assert call.returns == "metaflow.bronze._orders_staged"
    assert recorder.by_name("metaflow.bronze._orders_staged").kind == "table"


def test_non_materialized_staged_view_stays_a_bare_pipeline_local_dlt_view(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    _run_ingestion(_ingestion_row(target_type="streaming_table"))

    assert staged_view_calls[0].returns == "_orders_staged"
    assert recorder.by_name("_orders_staged").kind == "view"


@pytest.mark.parametrize("target_type", ["sink", "streaming_table"])
def test_register_flow_output_receives_register_staged_views_return_value(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls, target_type
):
    """The bug this guards: passing the bare ``_<target_table>_staged`` string on to
    ``register_flow_output`` while the staged intermediate was actually published under a
    qualified name, leaving every downstream ``dlt.read`` pointed at a dataset that does not
    exist."""
    _run_ingestion(_ingestion_row(target_type=target_type))

    assert flow_output_calls[0]["args"][1] == staged_view_calls[0].returns


def test_transformation_flow_output_receives_register_staged_views_return_value(
    recorder, bind_recorder, staged_view_calls, flow_output_calls
):
    _run_transformation(
        _transformation_row(
            source_inputs=[{"input_name": "orders_in", "table": "metaflow.bronze.orders"}],
            target_type="sink",
        )
    )

    assert staged_view_calls[0].returns == "metaflow.silver._enriched_orders_staged"
    assert flow_output_calls[0]["args"][1] == "metaflow.silver._enriched_orders_staged"


# ---------------------------------------------------------------------------------------------
# 5. bind()'s consumer_id per flow kind
# ---------------------------------------------------------------------------------------------


def test_ingestion_binds_under_the_dataflow_id_source_consumer_id(
    recorder, bind_recorder, overlays, staged_view_calls, flow_output_calls
):
    _run_ingestion(_ingestion_row(dataflow_id="ing_orders"))
    staged_view_calls[0].build_dataframe()

    assert bind_recorder.consumer_ids == ["ing_orders:source"]


def test_transformation_binds_one_consumer_id_per_named_input(
    recorder, bind_recorder, staged_view_calls, flow_output_calls
):
    """``input_name`` stays the SQL identifier ``transformation_sql`` references, but it is
    demoted from *being* the read to being an ALIAS over the source plane -- the cross-flow half
    of R2."""
    _run_transformation(
        _transformation_row(
            flow_step_id="tf_enrich",
            source_inputs=[
                {"input_name": "orders_in", "table": "metaflow.bronze.orders", "is_streaming": True},
                {"input_name": "events_batch", "table": "metaflow.silver.events", "is_streaming": False},
            ],
        )
    )

    assert bind_recorder.calls == []  # registration alone binds nothing
    for input_name in ("orders_in", "events_batch"):
        recorder.by_name(input_name).fn()

    assert bind_recorder.calls == [
        {"consumer_id": "tf_enrich:input:orders_in", "want_stream": True},
        {"consumer_id": "tf_enrich:input:events_batch", "want_stream": False},
    ]


def test_reconciliation_binds_source_and_target_consumer_ids(recorder, bind_recorder):
    """``reconciliation/graph_registration.py`` owns these two ids; this test pins them because
    ``engine/flow_generators.py``'s module docstring publishes them as part of the same
    consumer-id contract, and ``bind`` raises rather than guessing when one drifts."""
    row = _StubRow(
        reconciliation_id="rec_orders",
        execution_mode="pipeline_audit_only",
        source_config_json=json.dumps({"table": "metaflow.bronze.orders"}),
        target_configs_json=json.dumps([{"target_id": "t_ref", "table": "metaflow.gold.orders_ref"}]),
        match_keys_json=json.dumps(["order_id"]),
        compare_columns_json=json.dumps(["amount"]),
        error_handling_json=json.dumps({}),
        logging_config_json=json.dumps({}),
        dq_config_json=json.dumps({}),
        transform_sql=None,
        publish_schema=None,
        two_tier_verification=None,
    )

    flow_generators.generate_reconciliation_flow(
        _StubSpark(),
        row,
        plan=object(),
        publish_catalog="metaflow",
        publish_schema="recon",
        control_schema="metaflow.config",
    )

    prepare_nodes = [name for name in recorder.names if name.endswith(("__src", "__tgt"))]
    assert prepare_nodes, f"no L3 prepare datasets were registered; got {recorder.names}"
    for name in prepare_nodes:
        recorder.by_name(name).fn()

    assert "rec_orders:source" in bind_recorder.consumer_ids
    assert any(
        consumer_id.startswith("rec_orders:target:") for consumer_id in bind_recorder.consumer_ids
    ), bind_recorder.consumer_ids


def test_job_mode_reconciliation_row_registers_nothing_in_the_graph(recorder, bind_recorder):
    """R3: a ``"job"``-mode row still belongs to the standalone reconciliation job task, whose
    behaviour this redesign leaves entirely unchanged."""
    row = _StubRow(
        reconciliation_id="rec_job",
        execution_mode="job",
        source_config_json=json.dumps({"table": "metaflow.bronze.orders"}),
        target_configs_json=json.dumps([{"target_id": "t_ref", "table": "metaflow.gold.orders_ref"}]),
        match_keys_json=json.dumps(["order_id"]),
        compare_columns_json=json.dumps(["amount"]),
        error_handling_json=json.dumps({}),
        logging_config_json=json.dumps({}),
        dq_config_json=json.dumps({}),
        transform_sql=None,
        publish_schema=None,
        two_tier_verification=None,
    )

    flow_generators.generate_reconciliation_flow(
        _StubSpark(),
        row,
        plan=object(),
        publish_catalog="metaflow",
        publish_schema="recon",
        control_schema="metaflow.config",
    )

    assert recorder.registrations == []
    assert bind_recorder.calls == []


# ---------------------------------------------------------------------------------------------
# Malformed configuration is rejected, not silently defaulted
# ---------------------------------------------------------------------------------------------


def test_malformed_ingestion_json_raises_naming_the_flow(recorder, bind_recorder, overlays, flow_output_calls):
    from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

    row = _ingestion_row()
    row._fields["source_config_json"] = "{not json"

    with pytest.raises(FrameworkConfigError) as excinfo:
        _run_ingestion(row)

    assert "ing_orders" in str(excinfo.value)
    assert "malformed JSON" in str(excinfo.value)


def test_malformed_transformation_json_raises_naming_the_flow_step(recorder, bind_recorder, flow_output_calls):
    from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

    row = _transformation_row(source_inputs=[])
    row._fields["dq_config_json"] = "{not json"

    with pytest.raises(FrameworkConfigError) as excinfo:
        _run_transformation(row)

    assert "tf_enrich" in str(excinfo.value)
    assert "malformed JSON" in str(excinfo.value)


# ---------------------------------------------------------------------------------------------
# The dbutils-less transformation short form
# ---------------------------------------------------------------------------------------------


def test_transformation_accepts_the_dbutils_less_positional_short_form(
    recorder, bind_recorder, staged_view_calls, flow_output_calls
):
    """``generate_transformation_flow(spark, flow_row, plan=..., ...)`` -- the second positional
    is re-seated as ``flow_row``. A transformation flow has no ``source_config`` and therefore
    no ``schema_config_path``, so nothing in it reads ``dbutils``."""
    spark = _StubSpark()
    flow_generators.generate_transformation_flow(
        spark,
        _transformation_row(source_inputs=[{"input_name": "orders_in", "table": "metaflow.bronze.orders"}]),
        plan=object(),
        pipeline_parameters={},
    )

    assert staged_view_calls[0].staged_view_name == "_enriched_orders_staged"
    assert flow_output_calls[0]["args"][0] == "tf_enrich"


def test_transformation_staged_view_body_executes_the_resolved_sql(
    recorder, bind_recorder, staged_view_calls, flow_output_calls
):
    """The staged view's closure is ``lambda: spark.sql(resolved_sql)`` -- resolved ONCE at
    registration time (``${param}`` substitution + ``STREAM`` marking), then executed lazily."""
    spark = _StubSpark()
    _run_transformation(
        _transformation_row(source_inputs=[{"input_name": "orders_in", "table": "metaflow.bronze.orders"}]),
        spark=spark,
    )

    assert spark.sql_calls == []
    staged_view_calls[0].build_dataframe()
    assert spark.sql_calls == ["SELECT * FROM orders_in"]


# ---------------------------------------------------------------------------------------------
# Fixture hygiene -- the LOCAL_EXECUTION_MODE global must not leak into later modules
# ---------------------------------------------------------------------------------------------


def test_local_execution_mode_is_enabled_inside_the_fixture(dlt_local_execution):
    assert dlt.api.LOCAL_EXECUTION_MODE is True


def test_local_execution_mode_is_restored_after_the_fixture():
    assert dlt.api.LOCAL_EXECUTION_MODE is False


# ------------------------------------------------------------------------------------------------
# resolve_pipeline_schema -- the publish-schema resolution a reconciliation flow depends on.
#
# Regression origin: this used to be `PIPELINE_SCHEMA = _CURRENT_SCHEMA` inline in the engine
# notebook, with no fallback chain at all while PIPELINE_CATALOG had a three-step one. Live on
# 2026-08-31, pipeline be78d88d (declaring `schema: bronze_excalibur`) resolved it to None and the
# update died in graph_registration._node_name with
# "ValueError: Unsafe or malformed target_schema: None".
# ------------------------------------------------------------------------------------------------


class _StubConf:
    def __init__(self, values):
        self._values = values

    def get(self, key):
        if key not in self._values:
            raise Exception(f"conf key not set: {key}")
        return self._values[key]


class _StubCatalog:
    def __init__(self, current_database=None, raises=False):
        self._current_database = current_database
        self._raises = raises

    def currentDatabase(self):
        if self._raises:
            raise Exception("no active session")
        return self._current_database


class _StubSparkForSchema:
    def __init__(self, conf_values=None, current_database=None, catalog_raises=False):
        self.conf = _StubConf(conf_values or {})
        self.catalog = _StubCatalog(current_database, catalog_raises)


class _GroupRow:
    def __init__(self, target_schema=None):
        if target_schema is not None:
            self.target_schema = target_schema


def test_resolve_pipeline_schema_prefers_the_declared_pipelines_schema_conf():
    """The declared target schema wins over the session's current database.

    This ordering IS the fix: during graph definition the two differ, and only the first is the
    schema the pipeline actually publishes into.
    """
    spark = _StubSparkForSchema(
        conf_values={"pipelines.schema": "bronze_excalibur"},
        current_database="some_other_db",
    )
    assert flow_generators.resolve_pipeline_schema(spark, _GroupRow("from_group_row")) == "bronze_excalibur"


def test_resolve_pipeline_schema_falls_back_to_the_legacy_pipelines_target_key():
    """Pipelines created before the `schema` rename still set `pipelines.target`."""
    spark = _StubSparkForSchema(
        conf_values={"pipelines.target": "legacy_schema"},
        current_database="some_other_db",
    )
    assert flow_generators.resolve_pipeline_schema(spark, None) == "legacy_schema"


def test_resolve_pipeline_schema_falls_back_to_current_database_outside_a_pipeline():
    """Outside an update -- a plain notebook or Databricks Connect -- currentDatabase is correct."""
    spark = _StubSparkForSchema(conf_values={}, current_database="interactive_db")
    assert flow_generators.resolve_pipeline_schema(spark, None) == "interactive_db"


def test_resolve_pipeline_schema_falls_back_to_the_group_row_last():
    spark = _StubSparkForSchema(conf_values={}, catalog_raises=True)
    assert flow_generators.resolve_pipeline_schema(spark, _GroupRow("group_row_schema")) == "group_row_schema"


def test_resolve_pipeline_schema_returns_none_rather_than_raising_when_nothing_resolves():
    """Never raises: a group with no reconciliation flow never needs this value.

    The caller decides whether None is fatal -- the notebook logs a warning, and
    graph_registration raises only if it actually has to name a dataset.
    """
    spark = _StubSparkForSchema(conf_values={}, catalog_raises=True)
    assert flow_generators.resolve_pipeline_schema(spark, _GroupRow()) is None


def test_resolve_pipeline_schema_ignores_an_empty_conf_value():
    """An empty string is not a usable schema and must not short-circuit the chain."""
    spark = _StubSparkForSchema(conf_values={"pipelines.schema": ""}, current_database="real_db")
    assert flow_generators.resolve_pipeline_schema(spark, None) == "real_db"
