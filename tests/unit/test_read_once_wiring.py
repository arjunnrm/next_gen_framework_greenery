"""The direct, executable proof of R2 ("every physical source is read EXACTLY ONCE per pipeline
update and reused across all consumers") for one whole synthetic dataflow group -- in
milliseconds, with no workspace, no Databricks Connect session and no real Spark.

WHY THIS TEST EXISTS
--------------------
``tests/unit/test_source_plane_plan.py`` proves the *plan* is right (identity grouping, fanout,
mode collapse, guards). It stops at the plan. This test goes one step further and proves the
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
the *physical* read observable: an ``inline`` binding is the one binding kind that does not go
through ``dlt.read``/``dlt.read_stream`` at all -- it calls ``spark.read(Stream)`` directly --
so the stub session is the only place a genuine second scan of an origin could show up. It also
keeps the test offline: ``tests/conftest.py`` eagerly warms a real ``DatabricksSession`` at
collection time when credentials happen to be present, and an un-stubbed inline bind would
otherwise issue a live remote read.

THE SYNTHETIC GROUP
-------------------
One dataflow group; one in-graph sibling plus six distinct external read identities:

* ``/Volumes/metaflow/landing/orders`` -- ``ing_orders:source``. Fanout 1 -> inline, NO node.
* ``metaflow.bronze.orders`` -- IN-GRAPH (this group's own ``ing_orders`` target), consumed by
  ``tf_enrich:input:orders_in`` (streaming) and ``rec_orders_audit:source`` (batch).
  NO node; two ``dlt.read``/``dlt.read_stream`` references to the producer's qualified name.
* ``metaflow.silver.events`` -- ``tf_enrich:input:events_stream`` (streaming) and
  ``tf_daily:input:events_batch`` (batch). ONE **streaming** node, read both ways.
* ``metaflow.silver.lonely`` -- ``tf_daily:input:lonely_in``. Fanout 1 -> inline, NO node.
* ``metaflow.silver.customers`` -- ``rec_cust_a:source`` and ``rec_cust_b:source`` (the latter
  spelled ``metaflow.Silver.customers``). ONE materialized node; casefolding is load-bearing.
* ``metaflow.gold.orders_ref`` -- ``rec_orders_audit:target:t_ref``. Fanout 1 -> inline, NO node.
* ``metaflow.gold.customers_ref`` -- ``rec_cust_a:target:t_ref`` and ``rec_cust_b:target:t_ref``
  (the latter spelled ``metaflow.Gold.customers_ref``). ONE materialized node.

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

import json

import dlt
import dlt.api
import pytest

from NextGen_Metadata_Framework.lakeflow_framework.engine import source_plane
from NextGen_Metadata_Framework.lakeflow_framework.engine.flow_registration import register_staged_view
from NextGen_Metadata_Framework.lakeflow_framework.engine.identifiers import stable_node_name
from NextGen_Metadata_Framework.lakeflow_framework.engine.source_plane import (
    bind,
    plan_source_plane,
    register_source_plane,
)
from NextGen_Metadata_Framework.lakeflow_framework.reconciliation.graph_registration import (
    register_reconciliation_flow,
)
from NextGen_Metadata_Framework.lakeflow_framework.storage.table_properties import qualified_table_name
from NextGen_Metadata_Framework.lakeflow_framework.transformation.inputs import register_transformation_inputs

# ---------------------------------------------------------------------------------------------
# The synthetic group's locators. Two of them are deliberately referenced with different letter
# case by their second consumer -- casefolding is load-bearing in ReadIdentity, and a
# case-sensitive miss would silently fall through to a duplicate physical read.
# ---------------------------------------------------------------------------------------------

NODE_CATALOG = "metaflow"
NODE_SCHEMA = "pipeline_src"

LANDING_PATH = "/Volumes/metaflow/landing/orders"
ORDERS_TABLE = "metaflow.bronze.orders"  # in-graph: produced by ing_orders in this same group
EVENTS_TABLE = "metaflow.silver.events"
LONELY_TABLE = "metaflow.silver.lonely"
CUSTOMERS_TABLE = "metaflow.silver.customers"
CUSTOMERS_TABLE_MIXED_CASE = "metaflow.Silver.customers"
ORDERS_REF_TABLE = "metaflow.gold.orders_ref"
CUSTOMERS_REF_TABLE = "metaflow.gold.customers_ref"
CUSTOMERS_REF_TABLE_MIXED_CASE = "metaflow.Gold.customers_ref"

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
    """The only place a *physical* read can be observed: an ``inline`` binding, and every L0
    plane node's own body, resolve their DataFrame through this session rather than through
    ``dlt.read``/``dlt.read_stream``."""

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
    """A stub session, also installed as ``source_plane._active_spark``'s answer so an
    ``inline`` binding never reaches a real (or absent) ``SparkSession``."""
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
                    "schema_location": "/Volumes/metaflow/landing/_schemas/orders",
                }
            ),
            target_catalog="metaflow",
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
            target_catalog="metaflow",
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
            target_catalog="metaflow",
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
        logging_config_json=json.dumps({}),
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
            publish_catalog="metaflow",
            publish_schema="recon",
            control_schema="metaflow.config",
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

    # One PlaneNode per shared identity, one dataset name each, no name reused. Three of the six
    # external identities are shared (events, customers, customers_ref); the other three are
    # fanout-1 and stay inline.
    assert len(plan.nodes) == 3
    assert len(node_names) == len(set(node_names)) == len(plan.nodes)

    node_name_set = set(node_names)
    registered_node_names = [name for name in wiring.recorder.names if name in node_name_set]
    assert sorted(registered_node_names) == sorted(node_names)
    assert len(registered_node_names) == len(set(registered_node_names)), "a plane node was registered twice"

    # Every shared node is a @dlt.table (materialized), never a @dlt.view: a view is inlined into
    # each consumer, so "declared once" would not be "read once".
    for name in node_names:
        assert wiring.recorder.by_name(name).kind == "table"


def test_every_external_locator_is_physically_read_exactly_once_per_update(wiring):
    """The headline R2 assertion: eleven consumers across three flow kinds, seven distinct
    locators (six external + one in-graph), six physical reads -- no locator scanned twice."""
    assert len(wiring.plan.bindings) == 11, sorted(wiring.plan.bindings)

    locators = wiring.spark.physical_locators
    assert len(locators) == len(EXPECTED_EXTERNAL_LOCATORS)
    assert sorted(locators) == sorted(EXPECTED_EXTERNAL_LOCATORS)
    assert len(locators) == len(set(locators)), f"a locator was physically read more than once: {locators}"


def test_shared_locator_case_variants_collapse_to_one_read(wiring):
    """``metaflow.Silver.customers`` and ``metaflow.silver.customers`` are one physical table; a
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

    assert node.dataset_name == qualified_table_name(
        NODE_CATALOG, NODE_SCHEMA, stable_node_name("_src", CUSTOMERS_TABLE, "batch")
    )
    assert wiring.recorder.names.count(node.dataset_name) == 1

    # Both consumers bind to that one node, and read it by name rather than re-reading the table.
    for consumer_id in node.consumer_ids:
        binding = wiring.plan.bindings[consumer_id]
        assert binding.kind == "shared_node"
        assert binding.dataset_name == node.dataset_name
    assert wiring.recorder.reads.count(node.dataset_name) == 2


# ---------------------------------------------------------------------------------------------
# Mixed streaming/batch external locator: ONE streaming node, read both ways
# ---------------------------------------------------------------------------------------------


def test_mixed_stream_and_batch_locator_produces_one_streaming_node_read_both_ways(wiring):
    nodes = [node for node in wiring.plan.nodes.values() if node.identity.locator == EVENTS_TABLE]
    assert len(nodes) == 1
    node = nodes[0]

    # Collapsed to a streaming table, never split into a stream node plus a batch node: a
    # materialized streaming table is legally readable by dlt.read_stream AND dlt.read, while a
    # materialized view can serve neither pair.
    assert node.mode == "stream"
    assert node.materialized is True
    assert node.dataset_name == qualified_table_name(
        NODE_CATALOG, NODE_SCHEMA, stable_node_name("_src", EVENTS_TABLE, "stream")
    )
    assert wiring.recorder.names.count(node.dataset_name) == 1

    assert wiring.recorder.read_streams.count(node.dataset_name) == 1  # tf_enrich:input:events_stream
    assert wiring.recorder.reads.count(node.dataset_name) == 1  # tf_daily:input:events_batch

    # One physical read of the origin -- issued by the node's own body, streaming.
    events_reads = [r for r in wiring.spark.physical_reads if r["locator"].casefold() == EVENTS_TABLE]
    assert len(events_reads) == 1
    assert events_reads[0]["streaming"] is True


# ---------------------------------------------------------------------------------------------
# Fanout-1: no node at all
# ---------------------------------------------------------------------------------------------


def test_fanout_one_locator_produces_no_node(wiring):
    """``metaflow.silver.lonely`` has exactly one consumer, so materializing it would buy a full
    physical copy and cost that consumer's predicate pushdown -- it stays inline."""
    for node in wiring.plan.nodes.values():
        assert node.identity.locator != LONELY_TABLE

    binding = wiring.plan.bindings["tf_daily:input:lonely_in"]
    assert binding.kind == "inline"
    assert binding.dataset_name is None

    for mode in ("stream", "batch"):
        forbidden = qualified_table_name(NODE_CATALOG, NODE_SCHEMA, stable_node_name("_src", LONELY_TABLE, mode))
        assert forbidden not in wiring.recorder.names

    # Inline means the read happens in the consumer's own closure, through the session --
    # exactly once, because there is exactly one consumer.
    assert wiring.spark.physical_locators.count(LONELY_TABLE) == 1
    assert LONELY_TABLE not in wiring.recorder.graph_reads


def test_fanout_one_ingestion_path_produces_no_node(wiring):
    """The same rule for the ingestion flow's own raw landing path: one ingestion flow reads it,
    so there is no node -- and exactly one Auto Loader stream is opened over it."""
    for node in wiring.plan.nodes.values():
        assert node.identity.locator != LANDING_PATH

    binding = wiring.plan.bindings["ing_orders:source"]
    assert binding.kind == "inline"
    assert binding.mode == "stream"

    landing_reads = [r for r in wiring.spark.physical_reads if r["locator"].rstrip("/") == LANDING_PATH]
    assert len(landing_reads) == 1
    assert landing_reads[0]["streaming"] is True
    assert landing_reads[0]["kind"] == "path"


# ---------------------------------------------------------------------------------------------
# The fixture's own contract: local execution mode does not leak out of this module
# ---------------------------------------------------------------------------------------------


def test_local_execution_mode_is_enabled_inside_the_fixture(dlt_local_execution):
    assert dlt.api.LOCAL_EXECUTION_MODE is True


def test_local_execution_mode_is_restored_after_the_fixture():
    """Runs without the fixture: if the flag leaked, this module would have silently disarmed
    every other test module's "dlt raises outside a pipeline" assertion."""
    assert dlt.api.LOCAL_EXECUTION_MODE is False
