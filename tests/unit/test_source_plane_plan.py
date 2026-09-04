"""Unit tests for the L0 source plane's PURE plan pass -- ``engine/source_plane.py`` and
``engine/identifiers.py``.

No Spark, no ``dlt`` graph, no workspace. ``plan_source_plane`` / ``assert_acyclic`` /
``stable_node_name`` are deliberately pure functions over plain attribute-bearing row objects
(a ``pyspark.sql.Row`` in production, ``_StubRow`` here), so the read-once contract's rules can
be pinned down without paying for a Databricks Connect session -- see
``tests/unit/test_group_metadata_loader.py`` for the same duck-typed-stub idiom, and
``tests/conftest.py`` for why an eager session would otherwise be created at collection time.

This file is the local proof of **R2** at plan time (``tests/unit/test_read_once_wiring.py``
proves it again at registration time, against a locally-executing ``dlt``). Covered here:

  * IDENTITY EQUALITY across overlay-only differences -- two consumers differing only in
    ``filter_condition`` / ``watermark`` / ``decrypted_columns`` collapse to ONE read;
  * IDENTITY INEQUALITY on each BASE-READ option in turn (``format``, ``schema_location``,
    ``file_pattern``, ``reader_options``, ``starting_version``) -- these change which bytes are
    scanned, so they must NOT be shared;
  * CASEFOLD matching of a table locator (``flowx.Excalibur_usecase.x`` vs its lowercase
    twin) -- casefolding is load-bearing, not cosmetic: a case-sensitive miss here silently
    falls through to a second, duplicate physical read;
  * fanout counting; the any-consumer-streams MODE COLLAPSE; ``in_graph_sibling`` winning over
    ``shared_node``; the v1.7.3 Single-Read mandate under which the DEFAULT ``materialize``
    (``"always"``) materializes a node at ANY fanout, fanout 1 included, while stream and
    batch of one locator stay two DISTINCT nodes; the legacy ``"auto"`` policy still falling
    back to ``inline`` at fanout 1; and ``materialize="never"`` being REJECTED outright;
  * the :func:`stable_node_name` COLLISION MATRIX -- ``"flowx.bronze.a_b"`` and
    ``"flowx.bronze_a.b"`` sanitize identically and MUST still get different node names,
    or Lakeflow fails the whole update with "Cannot redefine dataset";
  * the G-STREAM and G-SIDE plan-time guards, asserted by message substring;
  * :func:`assert_acyclic` ACCEPTING the shipped geneva shape
    (``flowx_testing/051_geneva_tariffs_recon.json``) and REJECTING the
    ``dfg_rec_003_precomputed_hash`` shape (``flowx_testing/038_rec_003_precomputed_hash.json``,
    whose reconciliation target IS its ``append_target_table`` IS an SCD1 ingestion target of
    the same group) with both members of the ring named;
  * :func:`bind` on an unknown ``consumer_id`` raising a ``FrameworkConfigError`` that lists the
    known ids (so a mis-spelled consumer id fails loudly at registration rather than silently
    binding nothing).
"""

import json
import os

import pytest

from flowx.lakeflow_framework.engine.identifiers import (
    sanitize_identifier,
    stable_node_name,
)
from flowx.lakeflow_framework.engine.source_plane import (
    FrameworkGraphCycleError,
    SourcePlanePlan,
    assert_acyclic,
    bind,
    describe_plan,
    plan_source_plane,
)
from flowx.lakeflow_framework.exceptions import FrameworkConfigError

CATALOG = "flowx"
NODE_CATALOG = "flowx"
NODE_SCHEMA = "plane"

_SPEC_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "flowx_testing")


class _StubRow:
    """A duck-typed control-table row: attribute access over a fixed field set.

    ``source_plane._row_get`` is ``getattr(row, name, default)``, so a genuinely absent field
    must raise ``AttributeError`` (exactly what a real ``pyspark.sql.Row`` built from a table
    lacking that column does) rather than resolve to a present ``None`` -- hence a plain
    ``__getattr__`` over a dict rather than ``types.SimpleNamespace``.
    """

    def __init__(self, **fields):
        self._fields = dict(fields)

    def __getattr__(self, name):
        try:
            return self.__dict__["_fields"][name]
        except KeyError:
            raise AttributeError(name) from None


# ---------------------------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------------------------


def _ingestion_row(
    dataflow_id,
    source_config,
    target_table,
    source_type="autoloader",
    cdc_load_strategy="APPEND",
    target_type="streaming_table",
    target_schema="bronze",
    target_catalog=CATALOG,
):
    return _StubRow(
        dataflow_id=dataflow_id,
        source_type=source_type,
        source_config_json=json.dumps(source_config),
        cdc_load_strategy=cdc_load_strategy,
        target_type=target_type,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table,
    )


def _transformation_row(
    flow_step_id,
    inputs,
    target_table,
    target_schema="silver",
    target_catalog=CATALOG,
    cdc_load_strategy="APPEND",
    target_type="streaming_table",
):
    return _StubRow(
        flow_step_id=flow_step_id,
        source_inputs_json=json.dumps(inputs),
        cdc_load_strategy=cdc_load_strategy,
        target_type=target_type,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table,
    )


def _reconciliation_row(reconciliation_id, source_config, target_configs, execution_mode="pipeline"):
    return _StubRow(
        reconciliation_id=reconciliation_id,
        execution_mode=execution_mode,
        source_config_json=json.dumps(source_config),
        target_configs_json=json.dumps(target_configs),
    )


def _autoloader_config(path="/Volumes/flowx/land/incoming", **overrides):
    config = {
        "path": path,
        "format": "csv",
        "schema_location": "/Volumes/flowx/land/_schemas/a",
        "reader_options": {"header": "true"},
    }
    config.update(overrides)
    return config


def _plan(ingestion=(), transformation=(), reconciliation=(), **kwargs):
    kwargs.setdefault("node_catalog", NODE_CATALOG)
    kwargs.setdefault("node_schema", NODE_SCHEMA)
    return plan_source_plane(list(ingestion), list(transformation), list(reconciliation), **kwargs)


def _load_spec(file_name):
    """Load a shipped onboarding spec and apply the one substitution the control tables would
    already carry: ``{{catalog}}`` -> a concrete catalog. Everything downstream of
    ``onboarding/spec_loader.py`` (and therefore ``plan_source_plane``) only ever sees
    substituted values, and ``qualified_table_name`` rejects ``{{catalog}}`` outright as an
    unsafe identifier, so replaying a shipped spec here requires exactly this one step.
    """
    with open(os.path.join(_SPEC_DIR, file_name), encoding="utf-8") as handle:
        raw = handle.read()
    return json.loads(raw.replace("{{catalog}}", CATALOG))


def _rows_from_spec(spec, execution_mode_override=None):
    """The minimal spec -> control-row projection ``plan_source_plane`` actually consults.

    Deliberately NOT a re-implementation of ``onboarding/metadata_upsert.py``: only the handful
    of columns the plan pass reads are mapped, so this helper cannot silently drift into a
    second, competing definition of the control-table schema.
    """
    ingestion_rows = [
        _ingestion_row(
            flow["dataflow_id"],
            flow["source_config"],
            flow["target_table"],
            source_type=flow["source_type"],
            cdc_load_strategy=flow.get("target_config", {}).get("cdc_load_strategy", "APPEND"),
            target_type=flow["target_type"],
            target_schema=flow["target_schema"],
            target_catalog=flow["target_catalog"],
        )
        for flow in spec.get("ingestion_flows", [])
    ]
    reconciliation_rows = [
        _reconciliation_row(
            flow["reconciliation_id"],
            flow["source_config"],
            flow["target_configs"],
            execution_mode=execution_mode_override or flow.get("execution_mode", "job"),
        )
        for flow in spec.get("reconciliation_flows", [])
    ]
    return ingestion_rows, reconciliation_rows


# ---------------------------------------------------------------------------------------------
# Identity: equality across overlay-only differences
# ---------------------------------------------------------------------------------------------

_OVERLAY_ONLY_DIFFERENCES = [
    ("filter_condition", "amount > 0", "amount > 100"),
    ("watermark", {"column": "ts", "delay": "10 minutes"}, {"column": "ts", "delay": "1 hour"}),
    ("decrypted_columns", ["ssn"], ["ssn", "email"]),
]


@pytest.mark.parametrize("key,left,right", _OVERLAY_ONLY_DIFFERENCES, ids=[c[0] for c in _OVERLAY_ONLY_DIFFERENCES])
def test_overlay_only_difference_shares_one_read(key, left, right):
    """Two ingestion consumers of the identical path differing ONLY in an OVERLAY attribute
    collapse to ONE ``ReadIdentity`` -- one physical read, one shared node, two bindings.

    This is the whole point of the BASE-READ / OVERLAY split: ``filter_condition``,
    ``watermark`` and ``decrypted_columns`` are all applied *after* the read, so making them
    part of the identity would buy nothing and cost a duplicate scan.
    """
    rows = [
        _ingestion_row("df_left", _autoloader_config(**{key: left}), "left_target"),
        _ingestion_row("df_right", _autoloader_config(**{key: right}), "right_target"),
    ]
    plan = _plan(ingestion=rows)

    assert len(plan.nodes) == 1
    node = next(iter(plan.nodes.values()))
    assert node.consumer_ids == ["df_left:source", "df_right:source"]
    assert plan.bindings["df_left:source"].kind == "shared_node"
    assert plan.bindings["df_right:source"].kind == "shared_node"
    assert plan.bindings["df_left:source"].dataset_name == plan.bindings["df_right:source"].dataset_name


# ---------------------------------------------------------------------------------------------
# Identity: inequality on each BASE-READ option in turn
# ---------------------------------------------------------------------------------------------

_BASE_READ_DIFFERENCES = [
    ("format", "csv", "json"),
    ("schema_location", "/Volumes/flowx/land/_schemas/a", "/Volumes/flowx/land/_schemas/b"),
    ("file_pattern", "*.csv", "*.dat"),
    ("reader_options", {"header": "true"}, {"header": "false"}),
    ("starting_version", 1, 7),
]


@pytest.mark.parametrize("key,left,right", _BASE_READ_DIFFERENCES, ids=[c[0] for c in _BASE_READ_DIFFERENCES])
def test_base_read_option_difference_is_not_shared(key, left, right):
    """Each BASE-READ option changes WHICH BYTES ARE SCANNED, so two consumers differing in one
    of them must NOT collapse into a single shared node.

    Under the v1.7.3 Single-Read mandate the non-collapse shows up as TWO nodes rather than
    zero: every external identity is materialized regardless of fanout, so what this test
    guards is that the two requests are two identities -- if the differing option were dropped
    from :class:`ReadIdentity`, they would fold into ONE node and one consumer would silently
    be served bytes read under the other's options.
    """
    rows = [
        _ingestion_row("df_left", _autoloader_config(**{key: left}), "left_target"),
        _ingestion_row("df_right", _autoloader_config(**{key: right}), "right_target"),
    ]
    plan = _plan(ingestion=rows)

    assert len(plan.nodes) == 2
    left_binding = plan.bindings["df_left:source"]
    right_binding = plan.bindings["df_right:source"]
    assert left_binding.kind == "shared_node"
    assert right_binding.kind == "shared_node"
    assert left_binding.dataset_name != right_binding.dataset_name


# ---------------------------------------------------------------------------------------------
# Casefold matching
# ---------------------------------------------------------------------------------------------


def test_table_locator_matching_is_casefolded():
    """``flowx.Excalibur_usecase.zerobus_source_bus`` and its lowercase twin are ONE physical
    table, so they must resolve to ONE identity and ONE shared node.

    Modelled on ``flowx_testing/003_autoload_recon_append.json``, which really does write a
    capitalized schema name -- a case-sensitive comparison here would silently fall through to
    a second, duplicate full-table read of the same object.
    """
    mixed = f"{CATALOG}.Excalibur_usecase.zerobus_source_bus"
    lower = mixed.casefold()
    rows = [
        _transformation_row("fs_a", [{"input_name": "src", "table": mixed, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "src", "table": lower, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(transformation=rows)

    assert len(plan.nodes) == 1
    node = next(iter(plan.nodes.values()))
    assert node.identity.locator == lower
    assert sorted(node.consumer_ids) == ["fs_a:input:src", "fs_b:input:src"]


# ---------------------------------------------------------------------------------------------
# Fanout, mode collapse, precedence, materialize
# ---------------------------------------------------------------------------------------------


def test_fanout_counts_every_consumer_of_one_identity():
    """Three distinct consumers of one external table produce ONE node whose ``consumer_ids``
    names all three -- the fanout the read-once contract's sharing rule is computed from.
    """
    external = f"{CATALOG}.external.shared_dim"
    rows = [
        _transformation_row("fs_a", [{"input_name": "d", "table": external, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
        _transformation_row("fs_c", [{"input_name": "d", "table": external, "is_streaming": False}], "out_c"),
    ]
    plan = _plan(transformation=rows)

    assert len(plan.nodes) == 1
    node = next(iter(plan.nodes.values()))
    assert len(node.consumer_ids) == 3
    assert sorted(node.consumer_ids) == ["fs_a:input:d", "fs_b:input:d", "fs_c:input:d"]

    node_rows = [r for r in describe_plan(plan) if r["row_type"] == "node"]
    assert [r["fanout"] for r in node_rows] == [3]


def test_streaming_consumer_no_longer_collapses_a_batch_consumer_onto_its_node():
    """The v1.7.3 replacement for the old mode-collapse rule -- asserted as its inverse.

    Until v1.7.3 a mixed-mode locator produced ONE node, in stream mode, on the reasoning that a
    materialized streaming table legally serves ``dlt.read`` as well as ``dlt.read_stream``.
    That is true as far as it goes, but it silently imposed a streaming node's
    checkpoint-locking and full-refresh semantics on a consumer that only ever wanted a batch
    read. Under "read once per source PER EXECUTION MODE" the node key is ``(identity, mode)``,
    so each consumer gets a node in the mode it actually asked for.

    Kept as its own test (rather than folded into
    ``test_stream_and_batch_of_one_locator_stay_two_distinct_nodes``) because this is the
    specific REGRESSION direction: a future "optimisation" that reintroduces the collapse would
    still satisfy a bare two-node count if it merged the other way, but it cannot satisfy the
    per-binding mode assertions below.
    """
    external = f"{CATALOG}.external.shared_events"
    rows = [
        _transformation_row("fs_stream", [{"input_name": "e", "table": external, "is_streaming": True}], "out_s"),
        _transformation_row("fs_batch", [{"input_name": "e", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(transformation=rows)

    assert {node.mode for node in plan.nodes.values()} == {"stream", "batch"}
    assert plan.bindings["fs_stream:input:e"].mode == "stream"
    assert plan.bindings["fs_batch:input:e"].mode == "batch"
    # Each node lists ONLY the consumers that asked for its mode -- the collapse used to show up
    # here as a stream node carrying the batch consumer's id.
    by_mode = {node.mode: node for node in plan.nodes.values()}
    assert by_mode["stream"].consumer_ids == ["fs_stream:input:e"]
    assert by_mode["batch"].consumer_ids == ["fs_batch:input:e"]


def test_in_graph_sibling_wins_over_shared_node():
    """A locator this same group PRODUCES is bound as an ``in_graph_sibling``, never as a plane
    node -- a node here would be a SECOND read of something the graph already materializes.
    """
    produced = f"{CATALOG}.bronze.events"
    rows_ing = [_ingestion_row("df_events", _autoloader_config(), "events")]
    rows_trf = [
        _transformation_row("fs_a", [{"input_name": "e", "table": produced, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "e", "table": produced, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(ingestion=rows_ing, transformation=rows_trf)

    assert plan.bindings["fs_a:input:e"].kind == "in_graph_sibling"
    assert plan.bindings["fs_b:input:e"].kind == "in_graph_sibling"
    assert plan.bindings["fs_a:input:e"].dataset_name == produced
    # Only the (fanout-1, external) ingestion source could ever have become a node; it did not.
    assert all(node.identity.locator != produced.casefold() for node in plan.nodes.values())
    assert produced.casefold() in plan.in_graph_targets
    assert (produced.casefold(), f"{CATALOG}.silver.out_a".casefold()) in plan.edges


def test_default_policy_materializes_a_node_at_fanout_one():
    """v1.7.3 Single-Read mandate: the DEFAULT policy materializes a base node for a
    single-consumer external read, with no ``materialize`` argument passed at all.

    This is the inversion of the pre-v1.7.3 contract, where fanout 1 stayed ``inline`` to keep
    predicate pushdown into the origin. Asserting it via the signature default (rather than an
    explicit ``materialize="always"``) is the point of the test: the default is the thing that
    changed, and a regression to ``"auto"`` would be invisible to a test that passes the value.
    """
    inputs = [{"input_name": "d", "table": f"{CATALOG}.ext.dim", "is_streaming": False}]
    plan = _plan(transformation=[_transformation_row("fs_only", inputs, "out")])

    assert len(plan.nodes) == 1
    binding = plan.bindings["fs_only:input:d"]
    assert binding.kind == "shared_node"
    assert binding.dataset_name is not None
    # A consumer of a materialized node reads it by name via dlt.read/dlt.read_stream; it must
    # NOT also carry a reader_spec, which is what would let it re-read the origin directly.
    assert binding.reader_spec is None


def test_n_source_identities_yield_n_base_nodes():
    """The mandate stated as a count: N distinct external identities => N base ingestion nodes,
    every one of them materialized, at fanout 1 apiece.
    """
    rows = [
        _transformation_row("fs_a", [{"input_name": "d", "table": f"{CATALOG}.ext.dim_a", "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "d", "table": f"{CATALOG}.ext.dim_b", "is_streaming": False}], "out_b"),
        _transformation_row("fs_c", [{"input_name": "d", "table": f"{CATALOG}.ext.dim_c", "is_streaming": False}], "out_c"),
    ]
    plan = _plan(transformation=rows)

    assert len(plan.nodes) == 3
    assert {b.kind for b in plan.bindings.values()} == {"shared_node"}
    assert len({b.dataset_name for b in plan.bindings.values()}) == 3


def test_materialize_always_shares_even_at_fanout_one():
    """``materialize="always"`` passed EXPLICITLY behaves identically to the default -- it is
    now the default, so this pins that passing it is a no-op rather than a different path.
    """
    inputs = [{"input_name": "d", "table": f"{CATALOG}.ext.dim", "is_streaming": False}]
    plan = _plan(transformation=[_transformation_row("fs_only", inputs, "out")], materialize="always")

    assert len(plan.nodes) == 1
    assert plan.bindings["fs_only:input:d"].kind == "shared_node"


def test_stream_and_batch_of_one_locator_stay_two_distinct_nodes():
    """"Read once" means read once per source PER EXECUTION MODE.

    One locator wanted as a stream by one consumer and as a batch by another is NOT collapsed
    into a single node: nodes are keyed by ``(identity, mode)``, so this yields two nodes with
    the ``__stream`` and ``__batch`` suffixes. Collapsing them would be a real defect, not an
    optimisation -- a materialized view cannot be read with ``dlt.read_stream``, :func:`bind`
    raises when a ``mode="batch"`` binding is asked for a streaming read, and forcing either
    binding onto the other's node drags in checkpoint-locking and full-refresh side effects.
    """
    external = f"{CATALOG}.ext.dim"
    rows = [
        _transformation_row("fs_stream", [{"input_name": "d", "table": external, "is_streaming": True}], "out_s"),
        _transformation_row("fs_batch", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(transformation=rows)

    assert len(plan.nodes) == 2
    stream_binding = plan.bindings["fs_stream:input:d"]
    batch_binding = plan.bindings["fs_batch:input:d"]
    assert stream_binding.kind == batch_binding.kind == "shared_node"
    assert stream_binding.dataset_name != batch_binding.dataset_name
    assert stream_binding.dataset_name.endswith("__stream")
    assert batch_binding.dataset_name.endswith("__batch")
    assert {node.mode for node in plan.nodes.values()} == {"stream", "batch"}
    assert stream_binding.mode == "stream"
    assert batch_binding.mode == "batch"


def test_materialize_never_is_rejected_at_plan_time():
    """``materialize="never"`` is prohibited under the Single-Read mandate and raises HERE, not
    only at onboarding.

    The runtime raise is the half of the prohibition that covers existing groups: onboarding
    validation runs once, so a group onboarded before v1.7.3 keeps its persisted
    ``source_plane_config_json`` and feeds it straight into this function on every pipeline
    update. Without this raise, those groups would keep running the prohibited policy silently
    -- a validator-only rejection would make the prohibition true for new specs only.
    """
    external = f"{CATALOG}.ext.dim"
    rows = [
        _transformation_row("fs_a", [{"input_name": "d", "table": external, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
    ]

    with pytest.raises(FrameworkConfigError) as excinfo:
        _plan(transformation=rows, materialize="never")

    assert str(excinfo.value) == (
        "source_plane.materialize: materialize='never' is deprecated and prohibited under the "
        "Single-Read architectural mandate. Remove this setting to default to 'always', "
        "ensuring base tables are read once and reused via dlt.read()."
    )


def test_materialize_auto_is_a_distinct_legacy_policy_not_an_alias_for_always():
    """``"auto"`` is a genuinely DIFFERENT policy from the default, not an accepted synonym.

    It materializes only at fanout >= 2 and leaves a single-consumer identity ``inline`` -- the
    pre-v1.7.3 shape. It is retained so a control-table row onboarded before v1.7.3 keeps the DAG
    topology it was onboarded with, instead of silently gaining base nodes the moment its group is
    re-onboarded. It is a strictly WEAKER guarantee than ``"always"`` (a fanout-1 identity is
    re-read inside each consumer's own closure) and is not recommended for new specs.

    This test is the seam that keeps the two policies from quietly converging: if ``"auto"`` were
    ever collapsed back into ``"always"``, the fanout-1 assertions below would flip to
    ``shared_node`` and fail here rather than being discovered as an unexplained topology change
    in a deployed pipeline.
    """
    inputs = [{"input_name": "d", "table": f"{CATALOG}.ext.dim", "is_streaming": False}]
    row = _transformation_row("fs_only", inputs, "out")

    explicit_auto = _plan(transformation=[row], materialize="auto")
    default = _plan(transformation=[row])

    # Fanout 1 under "auto": no node, the consumer re-reads the origin inline.
    assert explicit_auto.nodes == {}
    auto_binding = explicit_auto.bindings["fs_only:input:d"]
    assert auto_binding.kind == "inline"
    assert auto_binding.dataset_name is None
    assert auto_binding.reader_spec is not None

    # ...whereas the DEFAULT policy materializes that same fanout-1 identity.
    assert len(default.nodes) == 1
    assert default.bindings["fs_only:input:d"].kind == "shared_node"


def test_materialize_auto_still_shares_at_fanout_two():
    """The other half of the legacy contract: ``"auto"`` DOES materialize once two distinct
    consumers request the same identity in the same mode -- that is the threshold it is named for.
    """
    external = f"{CATALOG}.ext.dim"
    rows = [
        _transformation_row("fs_a", [{"input_name": "d", "table": external, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(transformation=rows, materialize="auto")

    assert len(plan.nodes) == 1
    assert plan.bindings["fs_a:input:d"].kind == "shared_node"
    assert plan.bindings["fs_b:input:d"].kind == "shared_node"
    assert plan.bindings["fs_a:input:d"].dataset_name == plan.bindings["fs_b:input:d"].dataset_name


def test_materialize_auto_counts_fanout_per_mode_not_per_locator():
    """Fanout under ``"auto"`` is counted per (identity, MODE), matching the node key.

    One locator consumed once as a stream and once as a batch is fanout 1 in EACH mode, not
    fanout 2 -- those two consumers can never share a node (a materialized view cannot be read
    with ``dlt.read_stream``), so counting them as "shared" would materialize a node on behalf of
    a sharing that cannot happen.
    """
    external = f"{CATALOG}.ext.dim"
    rows = [
        _transformation_row("fs_s", [{"input_name": "d", "table": external, "is_streaming": True}], "out_s"),
        _transformation_row("fs_b", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(transformation=rows, materialize="auto")

    assert plan.nodes == {}
    assert plan.bindings["fs_s:input:d"].kind == "inline"
    assert plan.bindings["fs_b:input:d"].kind == "inline"


def test_shared_node_without_node_catalog_is_an_unpublished_temporary_node():
    """v1.6.0 Intermediate Object Rule: a plan that needs a node but was given no explicit
    publish location (``source_plane.catalog``/``schema`` unset) creates the node anyway --
    bare-named and ``published=False``, which ``register_source_plane`` registers as a
    pipeline-scoped ``@dlt.table(temporary=True)``. (Pre-v1.6.0 this raised
    ``FrameworkConfigError``; the notebook then papered over it by falling back to the
    pipeline's own catalog/schema, publishing every L0 node.)
    """
    external = f"{CATALOG}.ext.dim"
    rows = [
        _transformation_row("fs_a", [{"input_name": "d", "table": external, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = plan_source_plane([], rows, [], node_catalog=None, node_schema=None)

    assert len(plan.nodes) == 1
    node = next(iter(plan.nodes.values()))
    assert node.published is False
    assert "." not in node.dataset_name, "an unpublished node must keep its bare, pipeline-local name"
    assert plan.bindings["fs_a:input:d"].kind == "shared_node"
    assert plan.bindings["fs_a:input:d"].dataset_name == node.dataset_name


def test_shared_node_with_explicit_catalog_and_schema_is_published_qualified():
    """When the spec explicitly sets ``source_plane.catalog``+``schema``, the node is published
    there, exactly as pre-v1.6.0."""
    external = f"{CATALOG}.ext.dim"
    rows = [
        _transformation_row("fs_a", [{"input_name": "d", "table": external, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = plan_source_plane([], rows, [], node_catalog=CATALOG, node_schema="plane")

    node = next(iter(plan.nodes.values()))
    assert node.published is True
    assert node.dataset_name.startswith(f"{CATALOG}.plane.")


# ---------------------------------------------------------------------------------------------
# stable_node_name collision matrix
# ---------------------------------------------------------------------------------------------


def test_sanitize_collapses_two_distinct_locators_to_one_string():
    """The premise of the collision matrix below: sanitization IS lossy."""
    assert sanitize_identifier("flowx.bronze.a_b") == sanitize_identifier("flowx.bronze_a.b")
    assert sanitize_identifier("flowx.bronze.a_b") == "flowx_bronze_a_b"


def test_stable_node_name_distinguishes_locators_that_sanitize_identically():
    """``"flowx.bronze.a_b"`` and ``"flowx.bronze_a.b"`` sanitize to the SAME string, so
    without the always-appended ``sha256(locator)[:8]`` digest they would register the same
    Lakeflow dataset name and fail the whole update with "Cannot redefine dataset".
    """
    left = stable_node_name("_src", "flowx.bronze.a_b", "batch")
    right = stable_node_name("_src", "flowx.bronze_a.b", "batch")

    assert left != right
    assert left.startswith("_src__flowx_bronze_a_b__")
    assert right.startswith("_src__flowx_bronze_a_b__")
    assert left.endswith("__batch") and right.endswith("__batch")


@pytest.mark.parametrize(
    "locator",
    [
        "flowx.bronze.a_b",
        "flowx.bronze_a.b",
        "/volumes/flowx/land/incoming",
        "flowx.excalibur_usecase.zerobus_source_bus",
    ],
)
def test_stable_node_name_is_deterministic_and_digest_is_unconditional(locator):
    """Same locator -> same name across calls (a node name must not depend on which consumer
    triggered registration first), and the 8-hex digest is present even for a short locator
    that was never truncated.
    """
    first = stable_node_name("_src", locator, "stream")
    assert first == stable_node_name("_src", locator, "stream")

    parts = first.split("__")
    assert parts[0] == "_src"
    assert parts[-1] == "stream"
    digest = parts[-2]
    assert len(digest) == 8
    assert all(c in "0123456789abcdef" for c in digest)


def test_stable_node_names_are_pairwise_distinct_across_the_collision_matrix():
    matrix = [
        "flowx.bronze.a_b",
        "flowx.bronze_a.b",
        "flowx.bronze.a.b",
        "flowx-bronze.a_b",
        "/volumes/flowx/land/incoming",
        "/volumes/flowx/land_incoming",
    ]
    names = [stable_node_name("_src", locator, "batch") for locator in matrix]
    assert len(set(names)) == len(matrix)


# ---------------------------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("cdc_load_strategy", ["SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"])
def test_g_stream_rejects_streaming_read_of_a_merge_written_sibling(cdc_load_strategy):
    """G-STREAM: streaming an in-graph target written by MERGE/snapshot semantics would raise
    Delta's ``DELTA_SOURCE_TABLE_IGNORE_CHANGES`` at execution time, and the only fix Delta
    offers (``skipChangeCommits``) silently DROPS changed rows -- so the plan fails instead.
    """
    produced = f"{CATALOG}.silver.orders_src"
    ingestion = [
        _ingestion_row(
            "df_orders_src",
            _autoloader_config(),
            "orders_src",
            cdc_load_strategy=cdc_load_strategy,
            target_schema="silver",
        )
    ]
    transformation = [
        _transformation_row("fs_stream", [{"input_name": "o", "table": produced, "is_streaming": True}], "out")
    ]

    with pytest.raises(FrameworkConfigError) as excinfo:
        _plan(ingestion=ingestion, transformation=transformation)

    message = str(excinfo.value)
    assert "fs_stream:input:o" in message
    assert produced.casefold() in message
    assert "df_orders_src" in message
    assert "DELTA_SOURCE_TABLE_IGNORE_CHANGES" in message
    assert "skipChangeCommits is refused" in message


def test_g_stream_rejects_streaming_read_of_a_materialized_view_sibling():
    """A ``materialized_view`` sibling is rejected for the same reason, independent of the
    ``cdc_load_strategy`` (an MV is fully rewritten each update).
    """
    produced = f"{CATALOG}.silver.dim_customer"
    ingestion = [
        _ingestion_row(
            "df_dim",
            _autoloader_config(),
            "dim_customer",
            cdc_load_strategy="APPEND",
            target_type="materialized_view",
            target_schema="silver",
        )
    ]
    transformation = [
        _transformation_row("fs_stream", [{"input_name": "c", "table": produced, "is_streaming": True}], "out")
    ]

    with pytest.raises(FrameworkConfigError) as excinfo:
        _plan(ingestion=ingestion, transformation=transformation)
    assert "target_type='materialized_view'" in str(excinfo.value)


def test_g_stream_allows_streaming_read_of_an_append_only_sibling():
    """Positive control: an APPEND streaming-table sibling is a legal streaming source."""
    produced = f"{CATALOG}.bronze.events"
    ingestion = [_ingestion_row("df_events", _autoloader_config(), "events")]
    transformation = [
        _transformation_row("fs_stream", [{"input_name": "e", "table": produced, "is_streaming": True}], "out")
    ]
    plan = _plan(ingestion=ingestion, transformation=transformation)
    assert plan.bindings["fs_stream:input:e"].kind == "in_graph_sibling"
    assert plan.bindings["fs_stream:input:e"].mode == "stream"


@pytest.mark.parametrize(
    "left_overrides,right_overrides",
    [
        ({"landing_retention_policy": {"mode": "move"}}, {"landing_retention_policy": {"mode": "delete"}}),
        ({}, {"source_zip_handling": {"enabled": True}}),
    ],
    ids=["landing_retention_policy", "source_zip_handling"],
)
def test_g_side_rejects_two_lifecycle_regimes_on_one_landing_path(left_overrides, right_overrides):
    """G-SIDE: two ingestion flows reading ONE landing path with different
    ``landing_retention_policy`` / ``source_zip_handling`` is a latent DATA-LOSS bug (two
    competing file lifecycle regimes -- ``cloudFiles.cleanSource`` moves/deletes committed
    files, ``source_zip_handling`` PGP-decrypts and unzips in place), not a legitimate
    difference in what is read. Rejected regardless of whether the two happen to collapse to
    the same ``ReadIdentity``.
    """
    path = "/Volumes/flowx/land/shared_incoming"
    rows = [
        _ingestion_row("df_left", _autoloader_config(path=path, **left_overrides), "left_target"),
        _ingestion_row("df_right", _autoloader_config(path=path, **right_overrides), "right_target"),
    ]
    with pytest.raises(FrameworkConfigError) as excinfo:
        _plan(ingestion=rows)

    message = str(excinfo.value)
    assert path in message
    assert "df_left:source" in message and "df_right:source" in message
    assert "landing_retention_policy" in message and "source_zip_handling" in message
    assert "latent data-loss bug" in message


def test_g_side_allows_identical_lifecycle_configuration_on_one_path():
    """Positive control: identical lifecycle configuration on one path is exactly the case
    sharing the read FIXES -- the side effects then run precisely once.
    """
    path = "/Volumes/flowx/land/shared_incoming"
    policy = {"mode": "move", "archive_path": "/Volumes/flowx/land/archive"}
    rows = [
        _ingestion_row("df_left", _autoloader_config(path=path, landing_retention_policy=policy), "left_target"),
        _ingestion_row("df_right", _autoloader_config(path=path, landing_retention_policy=policy), "right_target"),
    ]
    plan = _plan(ingestion=rows)
    assert len(plan.nodes) == 1


def test_full_snapshot_cdc_ingestion_source_never_enters_the_plane():
    """A ``FULL_SNAPSHOT_CDC`` flow's own source is consumed by
    ``apply_changes_from_snapshot``'s PATH-based lambda (which may never name a pipeline
    dataset), so it produces no ``ConsumerRequest`` at all.
    """
    rows = [
        _ingestion_row("df_snap", _autoloader_config(), "snap_target", cdc_load_strategy="FULL_SNAPSHOT_CDC")
    ]
    plan = _plan(ingestion=rows)
    assert "df_snap:source" not in plan.bindings
    assert plan.nodes == {}


def test_job_mode_reconciliation_rows_are_skipped():
    """``execution_mode`` absent/``"job"`` belongs to the standalone job-task engine and never
    enters the pipeline's source plane.
    """
    recon = [
        _reconciliation_row(
            "recon_job",
            {"table": f"{CATALOG}.ext.src"},
            [{"target_id": "t1", "table": f"{CATALOG}.ext.tgt"}],
            execution_mode="job",
        )
    ]
    plan = _plan(reconciliation=recon)
    assert plan.bindings == {}


# ---------------------------------------------------------------------------------------------
# assert_acyclic
# ---------------------------------------------------------------------------------------------


def test_assert_acyclic_accepts_the_geneva_shape():
    """``flowx_testing/051_geneva_tariffs_recon.json``: the reconciliation NEAR side reads
    this group's own APPEND ingestion target (one edge), the FAR side is produced by a
    different pipeline entirely, and ``append_target_table`` is a landing/bus table nothing in
    THIS group produces. Acyclic -- and the near-side edge really is present, so the test would
    notice if the shape stopped being a graph at all.
    """
    spec = _load_spec("051_geneva_tariffs_recon.json")
    ingestion_rows, reconciliation_rows = _rows_from_spec(spec)
    plan = _plan(ingestion=ingestion_rows, reconciliation=reconciliation_rows)

    near = f"{CATALOG}.geneva_admin.tariffelementband_near"
    recon_owner = "__reconciliation__recon_geneva_tariffelementband"
    assert plan.bindings["recon_geneva_tariffelementband:source"].kind == "in_graph_sibling"
    assert (near.casefold(), recon_owner) in plan.edges
    # The far side is genuinely external to this group -- so under the v1.7.3 Single-Read
    # mandate it gets its own materialized base node despite being read by only one consumer.
    # Being a NODE rather than an inline read is what keeps it out of the dependency graph:
    # an external node has no in-graph producer, so it contributes no edge and cannot close a
    # cycle -- which is why the shape stays acyclic under the new policy too.
    far_binding = plan.bindings["recon_geneva_tariffelementband:target:bronze_tariffelementband_far"]
    assert far_binding.kind == "shared_node"

    assert_acyclic(plan)  # must not raise


def test_assert_acyclic_rejects_the_rec_003_precomputed_hash_ring():
    """``flowx_testing/038_rec_003_precomputed_hash.json``: the reconciliation TARGET
    (``silver_sales.orders_tgt``) IS its own ``append_target_table`` IS an SCD1 ingestion
    target of the same group. Reading it makes the reconciliation owner depend on the
    ingestion target; the L5 append lane writing back to it makes the ingestion target depend
    on the reconciliation owner. That is a ring, and a ring inside one Lakeflow update is a
    hard "Graph is not topologically sorted" failure.

    ``plan_source_plane`` contributes only the READ edges; the write-back edge is contributed
    by the L5 append lane's registrar, and is added here explicitly to model the complete
    graph ``assert_acyclic`` is asked to certify.

    The spec ships without ``execution_mode`` (i.e. job mode). It is replayed here as
    ``pipeline_audit_only`` rather than ``pipeline`` because the source side of a full
    ``pipeline``-mode flow wants a STREAMING read, which this shape's SCD1 source target trips
    on G-STREAM first (asserted separately below) -- the cycle would then never be reached.
    """
    spec = _load_spec("038_rec_003_precomputed_hash.json")
    ingestion_rows, reconciliation_rows = _rows_from_spec(spec, execution_mode_override="pipeline_audit_only")
    plan = _plan(ingestion=ingestion_rows, reconciliation=reconciliation_rows)

    target = f"{CATALOG}.silver_sales.orders_tgt".casefold()
    recon_owner = "__reconciliation__recon_rec_003_orders_precomputed_hash"
    assert (target, recon_owner) in plan.edges

    plan.edges.append((recon_owner, target))  # the L5 append lane's write-back to append_target_table

    with pytest.raises(FrameworkGraphCycleError) as excinfo:
        assert_acyclic(plan)

    message = str(excinfo.value)
    assert "cyclic in-graph-sibling dependency" in message
    assert target in message
    assert recon_owner in message
    assert " -> " in message


def test_rec_003_shape_in_full_pipeline_mode_trips_g_stream_first():
    """The same shape in ``execution_mode="pipeline"``: the L5 heal lane needs a STREAMING read
    of the source, and the source is an SCD1 (MERGE-written) in-graph target -- G-STREAM
    rejects it at plan time, before any cycle question arises.
    """
    spec = _load_spec("038_rec_003_precomputed_hash.json")
    ingestion_rows, reconciliation_rows = _rows_from_spec(spec, execution_mode_override="pipeline")

    with pytest.raises(FrameworkConfigError) as excinfo:
        _plan(ingestion=ingestion_rows, reconciliation=reconciliation_rows)
    assert "DELTA_SOURCE_TABLE_IGNORE_CHANGES" in str(excinfo.value)


def test_assert_acyclic_accepts_a_plan_with_no_edges():
    assert_acyclic(SourcePlanePlan(nodes={}, bindings={}, in_graph_targets=set(), edges=[]))


def test_assert_acyclic_names_a_longer_ring():
    """A three-node ring is reported with every member named, not just "a cycle exists"."""
    plan = SourcePlanePlan(
        nodes={},
        bindings={},
        in_graph_targets=set(),
        edges=[("a", "b"), ("b", "c"), ("c", "a")],
    )
    with pytest.raises(FrameworkGraphCycleError) as excinfo:
        assert_acyclic(plan)
    message = str(excinfo.value)
    assert "a" in message and "b" in message and "c" in message


# ---------------------------------------------------------------------------------------------
# bind()
# ---------------------------------------------------------------------------------------------


def test_bind_unknown_consumer_id_lists_the_known_ids():
    """A mis-spelled ``consumer_id`` must fail loudly, naming the ids that DO exist -- silently
    returning nothing would leave a dataset body reading an empty frame.
    """
    inputs = [{"input_name": "d", "table": f"{CATALOG}.ext.dim", "is_streaming": False}]
    plan = _plan(transformation=[_transformation_row("fs_a", inputs, "out")])

    with pytest.raises(FrameworkConfigError) as excinfo:
        bind(plan, "fs_a:input:typo", want_stream=False)

    message = str(excinfo.value)
    assert "unknown consumer_id 'fs_a:input:typo'" in message
    assert "fs_a:input:d" in message
