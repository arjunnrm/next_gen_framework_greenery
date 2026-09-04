"""The direct, executable proof of R2 ("every physical source is read EXACTLY ONCE per pipeline
update and reused across all consumers") for one whole synthetic dataflow group -- in
milliseconds, with no workspace, no Databricks Connect session and no real Spark.

WHY THIS TEST EXISTS
--------------------
``tests/unit/test_source_plane_plan.py`` proves the *plan* is right (identity grouping, per-mode
node keying, the ``materialize`` policies, guards). It stops at the plan. This test goes one step further and proves the
*wiring*: that when the plan is handed to ``register_source_plane`` and to the three flow
generators (L1 ingestion staged view, L2 transformation input views, L3/L4/L5 reconciliation
registrar), the datasets they actually register -- and the reads those dataset bodies actually
issue when Lakeflow evaluates them -- collapse to exactly one physical read per distinct
physical read identity. A plan that is correct but wired to a generator that quietly re-reads
the origin table anyway would pass every plan test and still violate R2 in production.

HOW IT RUNS WITHOUT A PIPELINE
------------------------------
``databricks-dlt`` is a *stub* package: every public entry point calls
``dlt.api.__local_execution_disabled()``, which raises outright unless
``dlt.enable_local_execution()`` has been called. That function flips
``dlt.api.LOCAL_EXECUTION_MODE``, a plain module-global with **no reset API** -- so calling it
naively leaks the flag into every test module that runs after this one in the same pytest
process, silently turning a "this raises outside a pipeline" assertion elsewhere into a
``warnings.warn``. The :func:`dlt_local_execution` fixture therefore takes ``monkeypatch``'s
snapshot of ``dlt.api.LOCAL_EXECUTION_MODE`` (and of ``dlt``'s own re-exported binding) *before*
enabling it, so pytest restores the original value at teardown.

On top of that, ``dlt.table`` / ``dlt.view`` / ``dlt.append_flow`` / ``dlt.read`` /
``dlt.read_stream`` are monkeypatched with recorders (see :class:`_DltRecorder`). Every module
under test does a plain ``import dlt`` and resolves ``dlt.<name>`` at call time on that one
module object, so patching the module object covers ``engine/source_plane.py`` (lazy import),
``engine/flow_registration.py``, ``transformation/inputs.py`` and
``reconciliation/graph_registration.py`` alike.

``source_plane._active_spark`` is monkeypatched to a :class:`_StubSpark`, which is what makes
the *physical* read observable. Under v1.7.3's default policy no binding is ``inline`` any more,
but the stub is MORE load-bearing than before, not less: every physical read now happens inside a
plane node's
``@dlt.table`` closure, where ``_execute_reader`` calls ``spark.read(Stream)`` on exactly the
identities the plan materialized. ``spark.physical_locators`` is therefore the direct evidence
for R2 -- one scan per node, and a duplicate scan of any origin would show up here as a repeated
locator. It also keeps the test offline: ``tests/conftest.py`` eagerly warms a real
``DatabricksSession`` at collection time when credentials happen to be present, and an
un-stubbed node body would otherwise issue a live remote read.

THE SYNTHETIC GROUP
-------------------
One dataflow group; one in-graph sibling plus six distinct external read identities:

Since v1.7.3 (the Single-Read architectural mandate) EVERY external identity is materialized
regardless of fanout, and nodes are keyed by ``(identity, mode)``. Six external identities
therefore yield SEVEN nodes -- ``flowx.silver.events`` is read both ways and so splits into a
``__stream`` and a ``__batch`` node -- and under the default policy no binding is ``inline`` at
all. (The legacy ``materialize="auto"`` policy is still reachable and still leaves fanout-1
identities inline; ``test_legacy_auto_policy_still_leaves_the_fanout_one_locators_inline`` plans
this same group under it, which is why the ``inline`` machinery below is still exercised.)

* ``/Volumes/flowx/landing/orders`` -- ``ing_orders:source`` (streaming). Fanout 1, ONE
  ``__stream`` node (pre-v1.7.3: inline, no node).
* ``flowx.bronze.orders`` -- IN-GRAPH (this group's own ``ing_orders`` target), consumed by
  ``tf_enrich:input:orders_in`` (streaming) and ``rec_orders_audit:source`` (batch).
  NO node; two ``dlt.read``/``dlt.read_stream`` references to the producer's qualified name.
  In-graph still wins over a node -- a node here would be a SECOND read of something the graph
  already materializes.
* ``flowx.silver.events`` -- ``tf_enrich:input:events_stream`` (streaming) and
  ``tf_daily:input:events_batch`` (batch). TWO nodes, one per mode (pre-v1.7.3: one streaming
  node read both ways).
* ``flowx.silver.lonely`` -- ``tf_daily:input:lonely_in``. Fanout 1, ONE ``__batch`` node
  (pre-v1.7.3: inline, no node).
* ``flowx.silver.customers`` -- ``rec_cust_a:source`` and ``rec_cust_b:source`` (the latter
  spelled ``flowx.Silver.customers``). ONE materialized node; casefolding is load-bearing.
* ``flowx.gold.orders_ref`` -- ``rec_orders_audit:target:t_ref``. Fanout 1, ONE ``__batch``
  node (pre-v1.7.3: inline, no node).
* ``flowx.gold.customers_ref`` -- ``rec_cust_a:target:t_ref`` and ``rec_cust_b:target:t_ref``
  (the latter spelled ``flowx.Gold.customers_ref``). ONE materialized node.

WHICH DATASET BODIES ARE EXECUTED
---------------------------------
Registering a dataset only records its closure; Lakeflow evaluates that closure once per update.
The :func:`wiring` fixture therefore invokes -- exactly once each, the way one update would --
every registered body that can touch the source plane: the L0 plane nodes, the L1 staged view,
the L2 input views and the L3 reconciliation ``__src``/``__tgt`` prepare nodes. The L4
``__classified``/``__metrics``/``__mismatch`` bodies are deliberately *not* invoked: they read
only in-graph L3 datasets by name (``dlt.read`` of a dataset this same pipeline produces) and so
cannot contribute a physical source read either way -- executing them would test
``reconciliation/matcher.py``'s join algebra against a stub DataFrame, which is
``tests/unit/test_matcher.py``'s job, not this test's.
"""

import hashlib
import json
from collections import Counter

import dlt
import dlt.api
import pytest

from flowx.lakeflow_framework.engine import source_plane
from flowx.lakeflow_framework.engine.flow_registration import register_staged_view
from flowx.lakeflow_framework.engine.identifiers import stable_node_name
from flowx.lakeflow_framework.engine.source_plane import (
    bind,
    plan_source_plane,
    register_source_plane,
)
from flowx.lakeflow_framework.reconciliation.graph_registration import (
    register_reconciliation_flow,
)
from flowx.lakeflow_framework.storage.table_properties import qualified_table_name
from flowx.lakeflow_framework.transformation.inputs import register_transformation_inputs

# ---------------------------------------------------------------------------------------------
# The synthetic group's locators. Two of them are deliberately referenced with different letter
# case by their second consumer -- casefolding is load-bearing in ReadIdentity, and a
# case-sensitive miss would silently fall through to a duplicate physical read.
# ---------------------------------------------------------------------------------------------

NODE_CATALOG = "flowx"
NODE_SCHEMA = "pipeline_src"

LANDING_PATH = "/Volumes/flowx/landing/orders"
ORDERS_TABLE = "flowx.bronze.orders"  # in-graph: produced by ing_orders in this same group
EVENTS_TABLE = "flowx.silver.events"
LONELY_TABLE = "flowx.silver.lonely"
CUSTOMERS_TABLE = "flowx.silver.customers"
CUSTOMERS_TABLE_MIXED_CASE = "flowx.Silver.customers"
ORDERS_REF_TABLE = "flowx.gold.orders_ref"
CUSTOMERS_REF_TABLE = "flowx.gold.customers_ref"
CUSTOMERS_REF_TABLE_MIXED_CASE = "flowx.Gold.customers_ref"

#: Every distinct external (non-in-graph) physical read identity the group needs, canonicalized.
#: R2 says the whole update must issue exactly this many physical reads -- no more.
EXPECTED_EXTERNAL_LOCATORS = {
    LANDING_PATH,
    EVENTS_TABLE,
    LONELY_TABLE,
    CUSTOMERS_TABLE,
    ORDERS_REF_TABLE,
    CUSTOMERS_REF_TABLE,
}

#: The ``options_fingerprint`` every plain-table :class:`ReadIdentity` carries: reading an
#: existing Delta table takes no ``source_config``-shaped base-read options, so the fingerprint is
#: ``sha256`` of canonical JSON over an EMPTY options dict. Since v1.7.3 ``stable_node_name`` is
#: called with ``discriminator=identity.options_fingerprint``, so the 8-hex digest in a node's
#: dataset name is derived from ``locator + this``, not from the locator alone. Spelled out here
#: as an independent constant rather than read back off the plan, so a node-name assertion
#: remains a real assertion instead of a restatement of whatever the engine produced.
EMPTY_OPTIONS_FINGERPRINT = hashlib.sha256(json.dumps({}, sort_keys=True).encode("utf-8")).hexdigest()

MATCH_KEYS = ["order_id"]
COMPARE_COLUMNS = ["amount"]
STUB_COLUMNS = ["order_id", "amount", "status"]


# ---------------------------------------------------------------------------------------------
# Duck-typed stubs -- the repo's _StubRow/_StubDataFrame/_StubSpark idiom
# (tests/unit/test_group_metadata_loader.py), never unittest.mock.MagicMock: a MagicMock answers
# every attribute affirmatively, which would hide exactly the mistakes this test hunts for (a
# missing column, a read that was never issued, a name that was never registered).
# ---------------------------------------------------------------------------------------------


class _StubRow:
    """A control-table row: attribute access over a fixed field set, ``AttributeError`` for a
    genuinely absent field -- so ``getattr(row, "execution_mode", None)`` exercises its default
    branch exactly as it would against a pre-migration control table."""

    def __init__(self, **fields):
        self._fields = dict(fields)

    def __getattr__(self, name):
        try:
            return self._fields[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class _StubDataFrame:
    """A lazily-permissive DataFrame: every unknown method returns ``self`` and records its name.

    ``columns`` is a real list (not a self-returning callable) because the code under test
    genuinely reads it -- ``attach_framework_ingestion_timestamp`` checks for an existing
    ``__framework_ingestion_timestamp_utc``, and ``matcher.prepare_dataset_for_matching``
    validates ``match_keys``/``compare_columns`` against it.
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


class _StubReader:
    """``spark.read`` / ``spark.readStream``: fluent option/format chaining, and a terminal
    ``load()``/``table()`` that records ONE physical read against the owning session."""

    def __init__(self, session, streaming):
        self._session = session
        self._streaming = streaming

    def format(self, *_args, **_kwargs):
        return self

    def option(self, *_args, **_kwargs):
        return self

    def options(self, *_args, **_kwargs):
        return self

    def schema(self, *_args, **_kwargs):
        return self

    def load(self, path):
        return self._session.record_physical_read("path", path, self._streaming)

    def table(self, name):
        return self._session.record_physical_read("table", name, self._streaming)


class _StubSpark:
    """The only place a *physical* read can be observed: every L0 plane node's own body resolves
    its DataFrame through this session rather than through ``dlt.read``/``dlt.read_stream``.

    Under v1.7.3's default policy every physical read is a node body's, so this session's log is
    a one-to-one record of the plan's nodes -- which is exactly what makes a duplicate scan
    visible as a repeated locator."""

    def __init__(self):
        self.physical_reads = []

    @property
    def read(self):
        return _StubReader(self, streaming=False)

    @property
    def readStream(self):  # camelCase mirrors pyspark's own attribute name
        return _StubReader(self, streaming=True)

    def record_physical_read(self, kind, locator, streaming):
        self.physical_reads.append({"kind": kind, "locator": locator, "streaming": streaming})
        return _StubDataFrame(f"physical:{kind}:{locator}")

    @property
    def physical_locators(self):
        """Every physical read's locator in the same canonical form ``ReadIdentity`` uses, so
        the two are directly comparable: a table locator is ``casefold()``-ed (UC table names
        are case-insensitive, and ``read_locator`` casefolds them), while a path locator only
        has its trailing slash stripped -- object-store paths ARE case-sensitive, and
        casefolding one here would let a genuinely distinct second path masquerade as a
        shared read."""
        return [
            read["locator"].rstrip("/") if read["kind"] == "path" else read["locator"].casefold()
            for read in self.physical_reads
        ]


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
    """Stand-in for the five ``dlt`` entry points this wiring uses.

    A registration records the closure but does NOT execute it -- that is Lakeflow's own
    behaviour, and it is what lets the :func:`wiring` fixture decide precisely which bodies to
    evaluate.
    """

    def __init__(self):
        self.registrations = []
        self.reads = []
        self.read_streams = []

    # -- registration -------------------------------------------------------------------------

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

    # -- graph-local reads --------------------------------------------------------------------

    def read(self, name):
        self.reads.append(name)
        return _StubDataFrame(f"dlt.read:{name}")

    def read_stream(self, name):
        self.read_streams.append(name)
        return _StubDataFrame(f"dlt.read_stream:{name}")

    # -- helpers ------------------------------------------------------------------------------

    @property
    def names(self):
        return [registration.name for registration in self.registrations]

    @property
    def graph_reads(self):
        """Every ``dlt.read`` + ``dlt.read_stream`` reference in one list -- neither is a
        physical read of an origin: both resolve to a dataset this same pipeline materializes."""
        return self.reads + self.read_streams

    def by_name(self, name):
        matches = [registration for registration in self.registrations if registration.name == name]
        assert len(matches) == 1, f"expected exactly one registration named {name!r}, got {matches}"
        return matches[0]


# ---------------------------------------------------------------------------------------------
# Fixtures
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
def stub_spark(monkeypatch):
    """A stub session, also installed as ``source_plane._active_spark``'s answer so a plane
    node's body never reaches a real (or absent) ``SparkSession``."""
    spark = _StubSpark()
    monkeypatch.setattr(source_plane, "_active_spark", lambda: spark)
    return spark


def _ingestion_rows():
    return [
        _StubRow(
            dataflow_id="ing_orders",
            source_type="autoloader",
            source_config_json=json.dumps(
                {
                    "path": LANDING_PATH,
                    "format": "json",
                    "schema_location": "/Volumes/flowx/landing/_schemas/orders",
                }
            ),
            target_catalog="flowx",
            target_schema="bronze",
            target_table="orders",
            target_type="table",
            cdc_load_strategy="APPEND",
        )
    ]


def _transformation_rows():
    return [
        _StubRow(
            flow_step_id="tf_enrich",
            source_inputs_json=json.dumps(
                [
                    {"input_name": "orders_in", "table": ORDERS_TABLE, "is_streaming": True},
                    {"input_name": "events_stream", "table": EVENTS_TABLE, "is_streaming": True},
                ]
            ),
            target_catalog="flowx",
            target_schema="silver",
            target_table="enriched_orders",
            target_type="table",
            cdc_load_strategy="APPEND",
        ),
        _StubRow(
            flow_step_id="tf_daily",
            source_inputs_json=json.dumps(
                [
                    {"input_name": "events_batch", "table": EVENTS_TABLE, "is_streaming": False},
                    {"input_name": "lonely_in", "table": LONELY_TABLE, "is_streaming": False},
                ]
            ),
            target_catalog="flowx",
            target_schema="gold",
            target_table="daily_orders",
            target_type="table",
            cdc_load_strategy="APPEND",
        ),
    ]


def _reconciliation_row(reconciliation_id, source_table, target_table):
    """One ``pipeline_audit_only`` reconciliation row.

    ``pipeline_audit_only`` (not ``pipeline``) throughout: it registers L3+L4 only, so the
    source side stays batch and no L5 heal lane (pulse + append_flow + foreach_batch_sink) is
    built. That keeps this test about the read-once wiring rather than about the healing lane,
    which ``tests/unit/test_recon_registration_ast.py`` and the integration test cover.
    """
    return _StubRow(
        reconciliation_id=reconciliation_id,
        execution_mode="pipeline_audit_only",
        source_config_json=json.dumps({"table": source_table}),
        target_configs_json=json.dumps([{"target_id": "t_ref", "table": target_table}]),
        match_keys_json=json.dumps(MATCH_KEYS),
        compare_columns_json=json.dumps(COMPARE_COLUMNS),
        error_handling_json=json.dumps({}),
        # run_log_capture stated EXPLICITLY since v1.7.3: the capture flags now default to FALSE
        # (reconciliation is silent by default), and pipeline_audit_only with both flags resolving
        # false is rejected outright -- that mode exists solely to produce __metrics/__mismatch, so
        # it would register compute with no output. An empty logging_config therefore no longer
        # describes a legal audit-only flow. Stating it keeps this fixture about read-once wiring
        # rather than silently depending on whatever the logging default happens to be.
        logging_config_json=json.dumps({"run_log_capture": True}),
        dq_config_json=json.dumps({}),
        transform_sql=None,
        publish_schema=None,
        two_tier_verification=None,
    )


def _reconciliation_rows():
    return [
        _reconciliation_row("rec_orders_audit", ORDERS_TABLE, ORDERS_REF_TABLE),
        _reconciliation_row("rec_cust_a", CUSTOMERS_TABLE, CUSTOMERS_REF_TABLE),
        # The same two physical tables, spelled with different capitalization -- these must NOT
        # produce a second read of either.
        _reconciliation_row("rec_cust_b", CUSTOMERS_TABLE_MIXED_CASE, CUSTOMERS_REF_TABLE_MIXED_CASE),
    ]


class _Wiring:
    """Everything one simulated pipeline update produced: the plan, the recorded registrations,
    and the stub session's physical-read log."""

    def __init__(self, plan, recorder, spark, staged_view_name):
        self.plan = plan
        self.recorder = recorder
        self.spark = spark
        self.staged_view_name = staged_view_name


@pytest.fixture()
def wiring(recorder, stub_spark):
    """Plan the source plane, register it, run the three flow generators, then evaluate every
    registered body that can touch the source plane -- exactly once each, as one update would."""
    ingestion_rows = _ingestion_rows()
    transformation_rows = _transformation_rows()
    reconciliation_rows = _reconciliation_rows()

    plan = plan_source_plane(
        ingestion_rows,
        transformation_rows,
        reconciliation_rows,
        node_catalog=NODE_CATALOG,
        node_schema=NODE_SCHEMA,
    )
    source_plane.assert_acyclic(plan)

    # L0 -- the shared nodes.
    register_source_plane(stub_spark, plan)
    plane_node_names = [node.dataset_name for node in plan.nodes.values()]

    # L1 -- the ingestion generator's staged view. ``build_dataframe`` is the one line
    # ``engine/flow_generators.py::generate_ingestion_flow`` owns: bind the flow's own source
    # consumer id, then apply the overlay chain (omitted here -- overlays are post-read shaping
    # and cannot add a read; ``tests/unit/test_flow_generators.py`` covers their order).
    staged_view_name = register_staged_view(
        staged_view_name="_orders_staged",
        comment="ingestion staged view",
        dq_rules=[],
        build_dataframe=lambda: bind(plan, "ing_orders:source", want_stream=True),
        target_config={},
    )

    # L2 -- the transformation generator's input views.
    for row in transformation_rows:
        register_transformation_inputs(
            stub_spark,
            json.loads(row.source_inputs_json),
            plan,
            row.flow_step_id,
        )

    # L3/L4 -- the reconciliation registrar.
    for row in reconciliation_rows:
        register_reconciliation_flow(
            stub_spark,
            row,
            plan=plan,
            publish_catalog="flowx",
            publish_schema="recon",
            control_schema="flowx.config",
        )

    source_touching = set(plane_node_names)
    source_touching.add(staged_view_name)
    source_touching.update(["orders_in", "events_stream", "events_batch", "lonely_in"])
    source_touching.update(name for name in recorder.names if name.endswith(("__src", "__tgt")))

    for registration in list(recorder.registrations):
        if registration.name in source_touching:
            registration.fn()

    return _Wiring(plan, recorder, stub_spark, staged_view_name)


# ---------------------------------------------------------------------------------------------
# R2: exactly one registration -- and exactly one physical read -- per distinct read identity
# ---------------------------------------------------------------------------------------------


def test_exactly_one_plane_node_registration_per_distinct_read_identity(wiring):
    plan = wiring.plan
    node_names = [node.dataset_name for node in plan.nodes.values()]

    # One PlaneNode per (identity, mode), one dataset name each, no name reused. v1.7.3: all six
    # external identities are materialized regardless of fanout, and flowx.silver.events is
    # read both ways so it splits into a __stream and a __batch node -- 7 nodes, not 3.
    assert len(plan.nodes) == 7
    assert len(node_names) == len(set(node_names)) == len(plan.nodes)

    node_name_set = set(node_names)
    registered_node_names = [name for name in wiring.recorder.names if name in node_name_set]
    assert sorted(registered_node_names) == sorted(node_names)
    assert len(registered_node_names) == len(set(registered_node_names)), "a plane node was registered twice"

    # Every shared node is a @dlt.table (materialized), never a @dlt.view: a view is inlined into
    # each consumer, so "declared once" would not be "read once".
    for name in node_names:
        assert wiring.recorder.by_name(name).kind == "table"


def test_every_external_locator_is_physically_read_once_per_execution_mode(wiring):
    """The headline R2 assertion, in its v1.7.3 form: read once per source PER EXECUTION MODE.

    Eleven consumers across three flow kinds, seven distinct locators (six external + one
    in-graph), seven physical reads. Six of the seven are a locator's only scan. The seventh is
    ``flowx.silver.events``, which is read exactly twice -- once as a stream, once as a batch
    -- because per-mode identity makes those two separate nodes.

    That second scan is a deliberate, bounded cost, not a read-once regression: a materialized
    view cannot be read with ``dlt.read_stream``, so the alternative is forcing one consumer
    onto the other's node and imposing a streaming node's checkpoint-locking and full-refresh
    semantics on a consumer that asked for neither. What R2 still forbids is a SECOND scan
    within one mode, which is what the per-mode counting below pins.
    """
    assert len(wiring.plan.bindings) == 11, sorted(wiring.plan.bindings)

    locators = wiring.spark.physical_locators
    assert set(locators) == EXPECTED_EXTERNAL_LOCATORS
    assert len(locators) == len(wiring.plan.nodes) == 7

    # Exactly one scan per locator, except the one read in both modes.
    counts = Counter(locators)
    assert counts[EVENTS_TABLE] == 2, f"the dual-mode locator must be scanned once per mode: {counts}"
    for locator in EXPECTED_EXTERNAL_LOCATORS - {EVENTS_TABLE}:
        assert counts[locator] == 1, f"{locator} was physically read more than once: {counts}"

    # The stricter statement: no (locator, mode) pair is ever scanned twice.
    per_mode = Counter((node.identity.locator, node.mode) for node in wiring.plan.nodes.values())
    assert all(count == 1 for count in per_mode.values()), per_mode


def test_shared_locator_case_variants_collapse_to_one_read(wiring):
    """``flowx.Silver.customers`` and ``flowx.silver.customers`` are one physical table; a
    case-sensitive identity would have produced two reads of it."""
    assert wiring.spark.physical_locators.count(CUSTOMERS_TABLE) == 1
    assert wiring.spark.physical_locators.count(CUSTOMERS_REF_TABLE) == 1


# ---------------------------------------------------------------------------------------------
# In-graph sibling: no plane node at all, N graph reads of the producer's qualified name
# ---------------------------------------------------------------------------------------------


def test_in_graph_locator_produces_zero_plane_nodes(wiring):
    assert ORDERS_TABLE in wiring.plan.in_graph_targets

    for node in wiring.plan.nodes.values():
        assert node.identity.locator != ORDERS_TABLE

    for mode in ("stream", "batch"):
        forbidden = qualified_table_name(NODE_CATALOG, NODE_SCHEMA, stable_node_name("_src", ORDERS_TABLE, mode))
        assert forbidden not in wiring.recorder.names

    # ...and it is never re-read from the origin either: an in-graph sibling is already
    # materialized by this same pipeline, so a physical read of it would be a SECOND read.
    assert ORDERS_TABLE not in wiring.spark.physical_locators


def test_in_graph_locator_is_read_by_name_once_per_consumer(wiring):
    """Both consumers (a streaming transformation input and a batch reconciliation source)
    resolve to a graph edge on the producer's own qualified name."""
    consumers = ["tf_enrich:input:orders_in", "rec_orders_audit:source"]
    for consumer_id in consumers:
        binding = wiring.plan.bindings[consumer_id]
        assert binding.kind == "in_graph_sibling"
        assert binding.dataset_name == ORDERS_TABLE

    assert wiring.recorder.read_streams.count(ORDERS_TABLE) == 1  # the streaming transformation input
    assert wiring.recorder.reads.count(ORDERS_TABLE) == 1  # the batch reconciliation source
    assert wiring.recorder.graph_reads.count(ORDERS_TABLE) == len(consumers)


# ---------------------------------------------------------------------------------------------
# Two-consumer external locator: exactly ONE materialized node
# ---------------------------------------------------------------------------------------------


def test_two_consumer_external_locator_produces_exactly_one_materialized_node(wiring):
    nodes = [node for node in wiring.plan.nodes.values() if node.identity.locator == CUSTOMERS_TABLE]
    assert len(nodes) == 1
    node = nodes[0]
    assert node.materialized is True
    assert node.mode == "batch"  # every consumer is batch (both flows are pipeline_audit_only)
    assert sorted(node.consumer_ids) == ["rec_cust_a:source", "rec_cust_b:source"]

    # The expected dataset name is recomputed from scratch, not read back off the node: since
    # v1.7.3 the 8-hex digest covers ``locator + options_fingerprint``, not the locator alone
    # (``stable_node_name(..., discriminator=identity.options_fingerprint)``), so the
    # discriminator has to be reproduced here too. For a plain table read no source_config-shaped
    # base-read options apply, so the fingerprint is sha256 over an EMPTY options dict -- an
    # independent constant, so this stays a real assertion rather than a restatement of the node.
    assert node.dataset_name == qualified_table_name(
        NODE_CATALOG,
        NODE_SCHEMA,
        stable_node_name("_src", CUSTOMERS_TABLE, "batch", discriminator=EMPTY_OPTIONS_FINGERPRINT),
    )
    assert wiring.recorder.names.count(node.dataset_name) == 1

    # Both consumers bind to that one node, and read it by name rather than re-reading the table.
    for consumer_id in node.consumer_ids:
        binding = wiring.plan.bindings[consumer_id]
        assert binding.kind == "shared_node"
        assert binding.dataset_name == node.dataset_name
    assert wiring.recorder.reads.count(node.dataset_name) == 2


# ---------------------------------------------------------------------------------------------
# Mixed streaming/batch external locator: ONE NODE PER MODE (v1.7.3 per-mode identity)
# ---------------------------------------------------------------------------------------------


def test_mixed_stream_and_batch_locator_produces_one_node_per_mode(wiring):
    """v1.7.3 replaced the mode-collapse rule with per-mode identity.

    Pre-v1.7.3 this locator produced ONE streaming node serving both consumers, on the (true)
    reasoning that a materialized streaming table is a legal ``dlt.read`` source as well. The
    mandate splits it instead: forcing a batch consumer onto a streaming node hands it that
    node's checkpoint-locking and full-refresh semantics, which it never asked for. The cost is
    a second scan of this one origin, which
    ``test_every_external_locator_is_physically_read_once_per_execution_mode`` accounts for
    explicitly.
    """
    nodes = {node.mode: node for node in wiring.plan.nodes.values() if node.identity.locator == EVENTS_TABLE}
    assert set(nodes) == {"stream", "batch"}

    for mode, node in nodes.items():
        assert node.materialized is True
        assert node.dataset_name == qualified_table_name(
            NODE_CATALOG,
            NODE_SCHEMA,
            # Same independent discriminator as the customers node -- another plain table read,
            # so the fingerprint is again sha256 over an empty base-read options dict. Taken from
            # the constant rather than from ``node.identity`` so the name is genuinely predicted.
            stable_node_name("_src", EVENTS_TABLE, mode, discriminator=EMPTY_OPTIONS_FINGERPRINT),
        )
        assert wiring.recorder.names.count(node.dataset_name) == 1

    # Each consumer binds to the node matching the mode it asked for, and reads it by name.
    assert wiring.plan.bindings["tf_enrich:input:events_stream"].dataset_name == nodes["stream"].dataset_name
    assert wiring.plan.bindings["tf_daily:input:events_batch"].dataset_name == nodes["batch"].dataset_name
    assert wiring.recorder.read_streams.count(nodes["stream"].dataset_name) == 1
    assert wiring.recorder.reads.count(nodes["batch"].dataset_name) == 1

    # The batch node is NOT read as a stream -- an MV cannot serve dlt.read_stream, and bind()
    # raises on that combination.
    assert nodes["batch"].dataset_name not in wiring.recorder.read_streams

    # Two physical reads of the origin, one per mode, each in its own node body.
    events_reads = [r for r in wiring.spark.physical_reads if r["locator"].casefold() == EVENTS_TABLE]
    assert len(events_reads) == 2
    assert sorted(r["streaming"] for r in events_reads) == [False, True]


# ---------------------------------------------------------------------------------------------
# Fanout-1: a materialized base node anyway (v1.7.3 Single-Read mandate)
# ---------------------------------------------------------------------------------------------


def test_fanout_one_locator_still_produces_a_materialized_node(wiring):
    """``flowx.silver.lonely`` has exactly one consumer and is materialized regardless.

    This is the exact inversion of the pre-v1.7.3 contract, which left a single-consumer read
    inline to preserve its predicate pushdown into the origin. Under the Single-Read mandate
    every external identity is a base node, so the consumer reads it by name via ``dlt.read``
    rather than issuing its own scan -- and the origin is still scanned exactly once, now from
    the node's body instead of the consumer's.
    """
    nodes = [node for node in wiring.plan.nodes.values() if node.identity.locator == LONELY_TABLE]
    assert len(nodes) == 1
    node = nodes[0]
    assert node.materialized is True
    assert node.mode == "batch"
    assert node.consumer_ids == ["tf_daily:input:lonely_in"]

    binding = wiring.plan.bindings["tf_daily:input:lonely_in"]
    assert binding.kind == "shared_node"
    assert binding.dataset_name == node.dataset_name
    # No reader_spec: the consumer must not retain a way to re-read the origin directly.
    assert binding.reader_spec is None

    assert wiring.recorder.by_name(node.dataset_name).kind == "table"
    assert wiring.recorder.names.count(node.dataset_name) == 1
    assert wiring.recorder.reads.count(node.dataset_name) == 1

    # Still exactly one physical scan of the origin -- read-once holds at fanout 1 too.
    assert wiring.spark.physical_locators.count(LONELY_TABLE) == 1
    assert LONELY_TABLE not in wiring.recorder.graph_reads


def test_fanout_one_ingestion_path_still_produces_a_materialized_node(wiring):
    """The same inversion for the ingestion flow's own raw landing path.

    Worth its own test because a path source is where a duplicated read is most expensive: two
    Auto Loader streams over one directory would share a ``cloudFiles.schemaLocation`` and race
    on it. Exactly one stream is opened over the path, now from the node body.
    """
    nodes = [node for node in wiring.plan.nodes.values() if node.identity.locator == LANDING_PATH]
    assert len(nodes) == 1
    node = nodes[0]
    assert node.materialized is True
    assert node.mode == "stream"

    binding = wiring.plan.bindings["ing_orders:source"]
    assert binding.kind == "shared_node"
    assert binding.mode == "stream"
    assert binding.dataset_name == node.dataset_name

    landing_reads = [r for r in wiring.spark.physical_reads if r["locator"].rstrip("/") == LANDING_PATH]
    assert len(landing_reads) == 1
    assert landing_reads[0]["streaming"] is True
    assert landing_reads[0]["kind"] == "path"


def test_legacy_auto_policy_still_leaves_the_fanout_one_locators_inline():
    """The pre-v1.7.3 ``"auto"`` policy is still reachable and still does the old thing.

    ``"auto"`` survives so that a group onboarded before v1.7.3 with an explicit
    ``source_plane.materialize`` keeps planning exactly as it did -- its persisted
    ``source_plane_config_json`` is fed straight back into :func:`plan_source_plane` on every
    update. Pinning it here is what makes the DEFAULT change above a real behavioural change
    rather than a rename: under ``"auto"`` this same synthetic group leaves all four fanout-1
    identities ``inline`` and materializes only the two genuinely shared ones.

    This test plans directly rather than using the :func:`wiring` fixture, because the fixture
    deliberately exercises the default policy end to end; a second full wiring run under a legacy
    policy would test ``"auto"``'s registration path, which no supported group uses.
    """
    plan = plan_source_plane(
        _ingestion_rows(),
        _transformation_rows(),
        _reconciliation_rows(),
        node_catalog=NODE_CATALOG,
        node_schema=NODE_SCHEMA,
        materialize="auto",
    )

    # Only the two fanout-2 identities become nodes; nothing splits by mode because the only
    # dual-mode locator (EVENTS_TABLE) is fanout 1 in each mode under "auto".
    assert {node.identity.locator for node in plan.nodes.values()} == {CUSTOMERS_TABLE, CUSTOMERS_REF_TABLE}

    for consumer_id in (
        "ing_orders:source",
        "tf_daily:input:lonely_in",
        "tf_enrich:input:events_stream",
        "tf_daily:input:events_batch",
        "rec_orders_audit:target:t_ref",
    ):
        binding = plan.bindings[consumer_id]
        assert binding.kind == "inline", f"{consumer_id} should stay inline under materialize='auto'"
        assert binding.dataset_name is None
        # An inline binding keeps its own reader_spec -- that is how it re-reads the origin in
        # the consumer's own closure, and it is precisely what the default policy removed.
        assert binding.reader_spec is not None

    # The in-graph sibling is unaffected by the policy: it was never a node under either.
    assert plan.bindings["rec_orders_audit:source"].kind == "in_graph_sibling"


# ---------------------------------------------------------------------------------------------
# The fixture's own contract: local execution mode does not leak out of this module
# ---------------------------------------------------------------------------------------------


def test_local_execution_mode_is_enabled_inside_the_fixture(dlt_local_execution):
    assert dlt.api.LOCAL_EXECUTION_MODE is True


def test_local_execution_mode_is_restored_after_the_fixture():
    """Runs without the fixture: if the flag leaked, this module would have silently disarmed
    every other test module's "dlt raises outside a pipeline" assertion."""
    assert dlt.api.LOCAL_EXECUTION_MODE is False
