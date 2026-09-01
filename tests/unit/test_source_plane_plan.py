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
  * CASEFOLD matching of a table locator (``metaflow.Excalibur_usecase.x`` vs its lowercase
    twin) -- casefolding is load-bearing, not cosmetic: a case-sensitive miss here silently
    falls through to a second, duplicate physical read;
  * fanout counting; the any-consumer-streams MODE COLLAPSE; ``in_graph_sibling`` winning over
    ``shared_node``; fanout == 1 falling back to ``inline``; ``materialize="always"`` /
    ``"never"``;
  * the :func:`stable_node_name` COLLISION MATRIX -- ``"metaflow.bronze.a_b"`` and
    ``"metaflow.bronze_a.b"`` sanitize identically and MUST still get different node names,
    or Lakeflow fails the whole update with "Cannot redefine dataset";
  * the G-STREAM and G-SIDE plan-time guards, asserted by message substring;
  * :func:`assert_acyclic` ACCEPTING the shipped geneva shape
    (``metaflow_testing/051_geneva_tariffs_recon.json``) and REJECTING the
    ``dfg_rec_003_precomputed_hash`` shape (``metaflow_testing/038_rec_003_precomputed_hash.json``,
    whose reconciliation target IS its ``append_target_table`` IS an SCD1 ingestion target of
    the same group) with both members of the ring named;
  * :func:`bind` on an unknown ``consumer_id`` raising a ``FrameworkConfigError`` that lists the
    known ids (so a mis-spelled consumer id fails loudly at registration rather than silently
    binding nothing).
"""

import json
import os

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.engine.identifiers import (
    sanitize_identifier,
    stable_node_name,
)
from NextGen_Metadata_Framework.lakeflow_framework.engine.source_plane import (
    FrameworkGraphCycleError,
    SourcePlanePlan,
    assert_acyclic,
    bind,
    describe_plan,
    plan_source_plane,
)
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

CATALOG = "metaflow"
NODE_CATALOG = "metaflow"
NODE_SCHEMA = "plane"

_SPEC_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "metaflow_testing")


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


def _autoloader_config(path="/Volumes/metaflow/land/incoming", **overrides):
    config = {
        "path": path,
        "format": "csv",
        "schema_location": "/Volumes/metaflow/land/_schemas/a",
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
    ("schema_location", "/Volumes/metaflow/land/_schemas/a", "/Volumes/metaflow/land/_schemas/b"),
    ("file_pattern", "*.csv", "*.dat"),
    ("reader_options", {"header": "true"}, {"header": "false"}),
    ("starting_version", 1, 7),
]


@pytest.mark.parametrize("key,left,right", _BASE_READ_DIFFERENCES, ids=[c[0] for c in _BASE_READ_DIFFERENCES])
def test_base_read_option_difference_is_not_shared(key, left, right):
    """Each BASE-READ option changes WHICH BYTES ARE SCANNED, so two consumers differing in one
    of them must NOT collapse: no shared node, and each falls back to its own ``inline`` read.
    """
    rows = [
        _ingestion_row("df_left", _autoloader_config(**{key: left}), "left_target"),
        _ingestion_row("df_right", _autoloader_config(**{key: right}), "right_target"),
    ]
    plan = _plan(ingestion=rows)

    assert plan.nodes == {}
    assert plan.bindings["df_left:source"].kind == "inline"
    assert plan.bindings["df_right:source"].kind == "inline"


# ---------------------------------------------------------------------------------------------
# Casefold matching
# ---------------------------------------------------------------------------------------------


def test_table_locator_matching_is_casefolded():
    """``metaflow.Excalibur_usecase.zerobus_source_bus`` and its lowercase twin are ONE physical
    table, so they must resolve to ONE identity and ONE shared node.

    Modelled on ``metaflow_testing/003_autoload_recon_append.json``, which really does write a
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


def test_any_streaming_consumer_collapses_the_node_to_stream():
    """Mode collapse: ANY consumer wanting a stream makes the shared node a STREAMING table.

    Never the reverse -- a materialized streaming table legally serves both ``dlt.read_stream``
    and ``dlt.read`` in one update, whereas a materialized view serves neither pair.
    """
    external = f"{CATALOG}.external.shared_events"
    rows = [
        _transformation_row("fs_stream", [{"input_name": "e", "table": external, "is_streaming": True}], "out_s"),
        _transformation_row("fs_batch", [{"input_name": "e", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(transformation=rows)

    node = next(iter(plan.nodes.values()))
    assert node.mode == "stream"
    assert node.dataset_name.endswith("__stream")
    assert plan.bindings["fs_stream:input:e"].mode == "stream"
    assert plan.bindings["fs_batch:input:e"].mode == "stream"


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


def test_fanout_one_external_read_stays_inline():
    """A single-consumer external read is left INLINE -- today's exact code path, preserving
    predicate pushdown into the origin, with nothing gained by materializing it.
    """
    inputs = [{"input_name": "d", "table": f"{CATALOG}.ext.dim", "is_streaming": False}]
    plan = _plan(transformation=[_transformation_row("fs_only", inputs, "out")])

    assert plan.nodes == {}
    binding = plan.bindings["fs_only:input:d"]
    assert binding.kind == "inline"
    assert binding.dataset_name is None
    assert binding.reader_spec == {"origin": "table", "table": f"{CATALOG}.ext.dim"}


def test_materialize_always_shares_even_at_fanout_one():
    """``materialize="always"`` forces a node per external identity even at fanout 1."""
    inputs = [{"input_name": "d", "table": f"{CATALOG}.ext.dim", "is_streaming": False}]
    plan = _plan(transformation=[_transformation_row("fs_only", inputs, "out")], materialize="always")

    assert len(plan.nodes) == 1
    assert plan.bindings["fs_only:input:d"].kind == "shared_node"


def test_materialize_never_stays_inline_even_at_fanout_two():
    """``materialize="never"`` is the explicit opt-out of sharing: N physical reads, no node,
    even when the fanout would otherwise qualify.
    """
    external = f"{CATALOG}.ext.dim"
    rows = [
        _transformation_row("fs_a", [{"input_name": "d", "table": external, "is_streaming": False}], "out_a"),
        _transformation_row("fs_b", [{"input_name": "d", "table": external, "is_streaming": False}], "out_b"),
    ]
    plan = _plan(transformation=rows, materialize="never")

    assert plan.nodes == {}
    assert plan.bindings["fs_a:input:d"].kind == "inline"
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
    assert sanitize_identifier("metaflow.bronze.a_b") == sanitize_identifier("metaflow.bronze_a.b")
    assert sanitize_identifier("metaflow.bronze.a_b") == "metaflow_bronze_a_b"


def test_stable_node_name_distinguishes_locators_that_sanitize_identically():
    """``"metaflow.bronze.a_b"`` and ``"metaflow.bronze_a.b"`` sanitize to the SAME string, so
    without the always-appended ``sha256(locator)[:8]`` digest they would register the same
    Lakeflow dataset name and fail the whole update with "Cannot redefine dataset".
    """
    left = stable_node_name("_src", "metaflow.bronze.a_b", "batch")
    right = stable_node_name("_src", "metaflow.bronze_a.b", "batch")

    assert left != right
    assert left.startswith("_src__metaflow_bronze_a_b__")
    assert right.startswith("_src__metaflow_bronze_a_b__")
    assert left.endswith("__batch") and right.endswith("__batch")


@pytest.mark.parametrize(
    "locator",
    [
        "metaflow.bronze.a_b",
        "metaflow.bronze_a.b",
        "/volumes/metaflow/land/incoming",
        "metaflow.excalibur_usecase.zerobus_source_bus",
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
        "metaflow.bronze.a_b",
        "metaflow.bronze_a.b",
        "metaflow.bronze.a.b",
        "metaflow-bronze.a_b",
        "/volumes/metaflow/land/incoming",
        "/volumes/metaflow/land_incoming",
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
    path = "/Volumes/metaflow/land/shared_incoming"
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
    path = "/Volumes/metaflow/land/shared_incoming"
    policy = {"mode": "move", "archive_path": "/Volumes/metaflow/land/archive"}
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
    """``metaflow_testing/051_geneva_tariffs_recon.json``: the reconciliation NEAR side reads
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
    # The far side is genuinely external to this group -- fanout 1, so inline.
    far_binding = plan.bindings["recon_geneva_tariffelementband:target:bronze_tariffelementband_far"]
    assert far_binding.kind == "inline"

    assert_acyclic(plan)  # must not raise


def test_assert_acyclic_rejects_the_rec_003_precomputed_hash_ring():
    """``metaflow_testing/038_rec_003_precomputed_hash.json``: the reconciliation TARGET
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
