"""Post-deployment verification for TC-DAG-001 -- "Read-Once Source Plane Across Three Flow
Kinds" (``flowx_testing/049_dag_001_unified_three_flow.json``, dataflow group
``dfg_dag_001_unified_three_flow``, pipeline
``resources/feature_tests/flowx_test_dag_001_unified_pipeline.yml``).

Like ``tests/integration/test_lakeflow_sink_dag.py``, every assertion here is made against
artifacts a real ``databricks bundle run flowx_test_dag_001_unified_job`` pass has ALREADY
materialized -- this module never triggers the pipeline itself (Lakeflow Declarative Pipelines
cannot run locally; see the note in ``tests/conftest.py``). Run that job first, then run these.

What is being proved
--------------------
**R1 -- ingestion + transformation + reconciliation all run inside ONE Lakeflow DAG.** The
reconciliation comparison is no longer a post-deployment plain-Spark job task: its L3/L4
datasets must appear in this pipeline's OWN event log as real graph nodes
(``flow_definition``/``dataset_definition``) and must actually have executed
(``flow_progress``). The R1 *edge* is asserted separately: the L3 source node
``_recon__<reconciliation_id>__src`` binds ``source_config.table`` --
``flowx.bronze_dag_001.shared_source_bronze``, a table THIS group's own ingestion flow
produces -- as an ``in_graph_sibling``, so that table must show up in the L3 flow's
``input_datasets``. That is a producer -> consumer edge inside one update, which is exactly
what "one DAG" means and what a separate job task could never express.

**R2 -- every physical source table is read exactly once per update.** One external locator,
``flowx.dag_001_usecase.shared_source_bus``, has fanout 3 in this spec (the zerobus
ingestion flow, the ``shared_source_direct`` transformation input, and the reconciliation
flow's ``target_configs[0]``), and one of those consumers streams -- so
``engine/source_plane.py`` must register exactly ONE materialized L0 node, the streaming table
``_src__flowx_dag_001_usecase_shared_source_bus__d7cda736__stream``, and bind all three
consumers to it. The event-log proof is that exactly one flow in the whole update carries a
read of that raw locator, and it is the plane node's own flow.

**R3 -- observability stays a normal job task.** Nothing here asserts anything about the
observability export; it remains ``run_observability_task`` in
``resources/feature_tests/flowx_test_dag_001_unified_job.yml``, outside the pipeline. This module
deliberately makes no event-log assertion about it.

Why the event log, and why these fixtures
-----------------------------------------
The graph shape is only observable from the pipeline's own event log. The
``event_log('<pipeline_id>')`` table-valued function is already how this framework reads it
(``observability/event_log_extractor.py``, which binds ``pipeline_id`` as a named SQL
parameter because ``args=`` accepts plain literals only) -- the same TVF and the same binding
style are reused here. ``origin`` is projected alongside ``event_type``/``details`` for one
reason: a ``flow_progress`` event carries its flow's identity in ``origin.flow_name`` and
nowhere in ``details``, so "these flows actually executed" is not answerable from ``details``
alone.

Post-update *table* state (does ``recon__...__classified`` exist as a queryable Unity Catalog
table?) goes through the ``table_exists`` fixture from ``tests/conftest.py``, NOT
``spark.table(...)``: this project's session-scoped Spark Connect session was confirmed live to
return stale catalog answers, and that fixture asks the control plane directly over REST.
(``volume_exists`` is the same idea for ``/Volumes`` paths; TC-DAG-001 publishes no Volume
artifacts, so it is not used here.)

Known limitation carried over from the resource files: no seed notebook exists yet for
``flowx.dag_001_usecase.shared_source_bus``, so the source table must be populated out of
band before the job runs. Every test below skips (rather than fails) when the pipeline or its
event log is not there at all, so a bundle that has never run this scenario does not produce
false failures.
"""

import json

import pytest

from flowx.lakeflow_framework.engine.identifiers import stable_node_name

CATALOG = "flowx"
SOURCE_SCHEMA = "dag_001_usecase"
BRONZE_SCHEMA = "bronze_dag_001"

PIPELINE_NAME_PREFIX = "flowx_test_dag_001_unified_pipeline"

#: The ONE external physical locator this whole test case is about. Canonical (casefolded,
#: fully-qualified) form -- the same string ``engine/source_plane.py`` keys its ``ReadIdentity``
#: on, so ``stable_node_name`` below reproduces the plane node name byte-for-byte.
SHARED_LOCATOR = f"{CATALOG}.{SOURCE_SCHEMA}.shared_source_bus"

#: The single L0 source-plane node the three consumers must share: a streaming table, because
#: at least one consumer (zerobus ingestion) streams. Never a view -- a view is inlined into
#: each consumer and would re-open the read three times.
PLANE_NODE_BARE = stable_node_name("_src", SHARED_LOCATOR, "stream")

#: ingestion_flows[0]'s target -- also reconciliation_flows[0]'s ``source_config.table``, which
#: is what makes the L3 source node an in-graph sibling (V-CYC-1) instead of a second read.
INGESTION_TARGET = f"{CATALOG}.{BRONZE_SCHEMA}.shared_source_bronze"

RECONCILIATION_ID = "recon_dag_001_bronze_vs_raw_bus"
TARGET_ID = "raw_source_bus_target"

# 049 sets no publish_schema, so the reconciliation datasets publish into the pipeline
# resource's own catalog/schema (flowx.bronze_dag_001) per the documented default.
PUBLISH_SCHEMA = f"{CATALOG}.{BRONZE_SCHEMA}"

RECON_SRC_BARE = f"_recon__{RECONCILIATION_ID}__src"
RECON_TGT_BARE = f"_recon__{RECONCILIATION_ID}__{TARGET_ID}__tgt"
RECON_CLASSIFIED_BARE = f"recon__{RECONCILIATION_ID}__{TARGET_ID}__classified"
RECON_METRICS_BARE = f"recon__{RECONCILIATION_ID}__{TARGET_ID}__metrics"
RECON_MISMATCH_BARE = f"recon__{RECONCILIATION_ID}__{TARGET_ID}__mismatch"
RECON_HEAL_SINK_BARE = f"_recon__{RECONCILIATION_ID}__heal_sink"

#: The three PUBLISHED L4 comparison datasets -- the ones whose presence in this pipeline's
#: event log is the direct evidence for R1.
PUBLISHED_L4_BARE_NAMES = (RECON_CLASSIFIED_BARE, RECON_METRICS_BARE, RECON_MISMATCH_BARE)

#: Exactly the query named in the design for this step (plus ``origin`` -- see the module
#: docstring). ``pipeline_id`` is bound as a named parameter rather than f-string-interpolated,
#: matching ``observability/event_log_extractor.py``'s use of the same TVF.
EVENT_LOG_QUERY = (
    "SELECT event_type, details, origin FROM event_log(:pipeline_id) "
    "WHERE event_type IN ('flow_definition', 'dataset_definition', 'sink_definition', 'flow_progress')"
)


# ---------------------------------------------------------------------------
# Fixtures -- resolve the deployed pipeline, then read its event log once.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dag_001_pipeline_id() -> str:
    """Resolve the deployed ``flowx_test_dag_001_unified_pipeline_<target>`` pipeline id.

    Resolved by NAME rather than hard-coded, because the bundle appends the deployment target
    to the pipeline name (``name: ..._${bundle.target}``) and the id itself is generated at
    deploy time. Skips -- never fails -- when the pipeline is not deployed in this workspace:
    an un-deployed scenario is "not tested here", not "broken".
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    matches = [
        pipeline
        for pipeline in client.pipelines.list_pipelines()
        if (pipeline.name or "").startswith(PIPELINE_NAME_PREFIX)
    ]
    if not matches:
        pytest.skip(
            f"no pipeline whose name starts with '{PIPELINE_NAME_PREFIX}' is deployed in this "
            "workspace -- run `databricks bundle deploy` and `databricks bundle run "
            "flowx_test_dag_001_unified_job` first"
        )
    return matches[0].pipeline_id


@pytest.fixture(scope="module")
def event_rows(spark, dag_001_pipeline_id):
    """All ``flow_definition``/``dataset_definition``/``sink_definition``/``flow_progress``
    rows for the pipeline, read once per module.

    ``event_log(...)`` raises if the pipeline has never run an update (or the caller lacks
    CAN_VIEW), which is the same "has the job run yet?" condition every other post-deployment
    test in this directory treats as a skip.
    """
    try:
        rows = spark.sql(EVENT_LOG_QUERY, args={"pipeline_id": dag_001_pipeline_id}).collect()
    except Exception as exc:  # noqa: BLE001 - never-run / no-permission both mean "nothing to assert on"
        pytest.skip(f"could not read event_log('{dag_001_pipeline_id}'): {exc}")
    if not rows:
        pytest.skip(
            f"event_log('{dag_001_pipeline_id}') returned no flow/dataset/sink events -- "
            "has flowx_test_dag_001_unified_job run yet?"
        )
    return rows


# ---------------------------------------------------------------------------
# Event-log helpers
# ---------------------------------------------------------------------------


def _details(row) -> dict:
    """Parse an event row's ``details`` JSON string into a dict (``{}`` when absent/unparseable)."""
    raw = row["details"]
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _raw_details(row) -> str:
    return row["details"] or ""


def _origin_field(row, field: str):
    """Read one field out of the event's ``origin`` struct, tolerating its absence.

    ``origin.flow_name`` is where a ``flow_progress`` event carries the flow it belongs to --
    ``details`` alone cannot identify it, which is why ``origin`` is projected alongside the two
    columns named in this step's design.
    """
    origin = row["origin"]
    if origin is None:
        return None
    try:
        return origin[field]
    except (KeyError, ValueError, TypeError):
        return getattr(origin, field, None)


def _rows_of_type(rows, event_type: str) -> list:
    return [row for row in rows if row["event_type"] == event_type]


def _names_match(candidate, bare_name: str) -> bool:
    """True when ``candidate`` is ``bare_name``, or a qualified name ending in it.

    Lakeflow reports a dataset sometimes bare and sometimes fully qualified depending on the
    event and the publishing mode; both spellings mean the same node, so matching on the
    qualified suffix keeps these assertions about the GRAPH rather than about name formatting.
    """
    if not isinstance(candidate, str):
        return False
    return candidate == bare_name or candidate.endswith(f".{bare_name}")


def _flow_definition_for(rows, bare_name: str):
    """The ``flow_definition`` event whose ``output_dataset`` is ``bare_name``, or ``None``."""
    for row in _rows_of_type(rows, "flow_definition"):
        definition = _details(row).get("flow_definition", {})
        if _names_match(definition.get("output_dataset"), bare_name):
            return definition
    return None


def _defines_dataset(rows, bare_name: str) -> bool:
    """True when any ``flow_definition``/``dataset_definition`` event declares ``bare_name`` as a
    graph node."""
    if _flow_definition_for(rows, bare_name) is not None:
        return True
    for row in _rows_of_type(rows, "dataset_definition"):
        definition = _details(row).get("dataset_definition", {})
        for key in ("dataset_name", "name", "output_dataset"):
            if _names_match(definition.get(key), bare_name):
                return True
        # Some event-log shapes name the dataset only on the origin struct.
        if _names_match(_origin_field(row, "dataset_name"), bare_name):
            return True
    return False


def _flow_progress_names(rows) -> list:
    """Every distinct flow name that produced a ``flow_progress`` event."""
    names = []
    for row in _rows_of_type(rows, "flow_progress"):
        name = _origin_field(row, "flow_name")
        if isinstance(name, str) and name not in names:
            names.append(name)
    return names


def _executed(rows, bare_name: str) -> bool:
    return any(_names_match(name, bare_name) for name in _flow_progress_names(rows))


def _flow_name_of(row):
    """Best-effort flow identity for an event row: ``origin.flow_name``, else the
    ``flow_definition``'s own ``output_dataset``."""
    name = _origin_field(row, "flow_name")
    if isinstance(name, str):
        return name
    definition = _details(row).get("flow_definition", {})
    output_dataset = definition.get("output_dataset")
    return output_dataset if isinstance(output_dataset, str) else None


def _flows_reading_shared_locator(rows) -> set:
    """Flows whose events show a physical read of ``SHARED_LOCATOR`` -- the R2 measurement.

    A row counts when its ``details`` payload mentions the raw dotted locator, or names a
    ``DeltaSource`` over the ``shared_source_bus`` table. The plane node's OWN name is stripped
    from the text first: it is derived from the locator (dots sanitized to underscores) and
    contains the substring ``shared_source_bus``, so every downstream consumer of the plane node
    would otherwise look like a reader of the raw locator -- which is precisely the thing under
    test and must not be allowed to pass by accident.
    """
    readers = set()
    for row in rows:
        text = _raw_details(row).replace(PLANE_NODE_BARE, "")
        mentions_locator = SHARED_LOCATOR in text.casefold()
        mentions_delta_source = "DeltaSource" in text and "shared_source_bus" in text
        if not (mentions_locator or mentions_delta_source):
            continue
        flow_name = _flow_name_of(row)
        if flow_name:
            readers.add(flow_name)
    return readers


# ---------------------------------------------------------------------------
# R1 -- the reconciliation comparison is a real node in THIS pipeline's graph
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bare_name",
    PUBLISHED_L4_BARE_NAMES,
    ids=["classified", "metrics", "mismatch"],
)
def test_recon_l4_dataset_is_declared_in_the_pipeline_graph(event_rows, bare_name):
    """R1: each published L4 comparison dataset must be DECLARED as a node of this pipeline.

    Before this redesign the comparison ran in ``05_reconciliation_engine.py`` as a separate
    job task and left no trace whatsoever in any pipeline event log -- so a
    ``flow_definition``/``dataset_definition`` row bearing these names is not a formality, it is
    the difference between "reconciliation runs somewhere" and "reconciliation is inside the
    DAG". See reconciliation/graph_registration.py's L4 RECON COMPARE section for where these
    names are minted.
    """
    assert _defines_dataset(event_rows, bare_name), (
        f"no flow_definition/dataset_definition event names '{bare_name}' -- the L4 "
        "reconciliation comparison is not a node of this pipeline's graph (R1). Flows that "
        f"reported progress: {sorted(_flow_progress_names(event_rows))}"
    )


@pytest.mark.parametrize(
    "bare_name",
    PUBLISHED_L4_BARE_NAMES,
    ids=["classified", "metrics", "mismatch"],
)
def test_recon_l4_dataset_actually_executed(event_rows, bare_name):
    """R1, second half: declared is not the same as run. ``flow_progress`` events for the same
    three names prove the comparison flows really executed in the update, rather than being
    registered and then pruned/skipped."""
    assert _executed(event_rows, bare_name), (
        f"no flow_progress event for '{bare_name}' -- the flow was declared but never ran. "
        f"Flows that did run: {sorted(_flow_progress_names(event_rows))}"
    )


def test_recon_l3_prepare_datasets_are_in_the_graph(event_rows):
    """The L3 PREPARE pair must be nodes too: without them the L4 comparison would be reading
    something outside the update, and the "one hash-prepared read shared by every target"
    property (the reason L3 exists at all) would be unverifiable."""
    for bare_name in (RECON_SRC_BARE, RECON_TGT_BARE):
        assert _defines_dataset(event_rows, bare_name), (
            f"L3 reconciliation dataset '{bare_name}' is missing from the pipeline graph"
        )


def test_recon_source_node_reads_this_groups_own_ingestion_target(event_rows):
    """R1's actual EDGE: the L3 source node's ``input_datasets`` must name
    ``flowx.bronze_dag_001.shared_source_bronze``.

    ``source_config.table`` resolves to a table this same group's ingestion flow publishes, so
    ``source_plane.bind`` returns an ``in_graph_sibling`` binding -- ``dlt.read(...)`` of the
    sibling dataset, no second physical read -- and Lakeflow therefore records a producer ->
    consumer dependency. That dependency is what makes the reconciliation see THIS update's
    freshly-ingested rows; a job-task reconciliation could only ever see whatever was committed
    by the time it happened to start.
    """
    definition = _flow_definition_for(event_rows, RECON_SRC_BARE)
    assert definition is not None, (
        f"no flow_definition event with output_dataset '{RECON_SRC_BARE}' -- cannot verify the "
        "ingestion -> reconciliation edge"
    )
    input_datasets = definition.get("input_datasets") or []
    assert any(_names_match(name, "shared_source_bronze") for name in input_datasets), (
        f"'{RECON_SRC_BARE}' does not list the ingestion target '{INGESTION_TARGET}' among its "
        f"input_datasets ({input_datasets}) -- the reconciliation source is not bound as an "
        "in-graph sibling, so ingestion and reconciliation are not one DAG (R1)"
    )


def test_no_heal_sink_is_registered_for_this_group(event_rows):
    """Negative control on ``sink_definition``: 049's only target is ``target_to_source`` with no
    ``append_target_table``, so ``_wants_heal`` is False and the L5 pulse/``foreach_batch`` heal
    sink must NOT be registered. A sink appearing here would mean the registrar builds the
    healing path unconditionally -- which would append corrections nobody asked for."""
    sink_names = []
    for row in _rows_of_type(event_rows, "sink_definition"):
        definition = _details(row).get("sink_definition", {})
        for key in ("sink_name", "name"):
            value = definition.get(key)
            if isinstance(value, str):
                sink_names.append(value)
    assert not any(RECON_HEAL_SINK_BARE in name for name in sink_names), (
        f"a heal sink '{RECON_HEAL_SINK_BARE}' was registered even though no target_config in "
        f"049 sets append_target_table; sinks seen: {sink_names}"
    )


# ---------------------------------------------------------------------------
# R2 -- one physical read of the shared locator per update
# ---------------------------------------------------------------------------


def test_shared_source_plane_node_is_a_graph_node(event_rows):
    """The L0 node itself must exist, under exactly the ``stable_node_name`` spelling
    ``engine/source_plane.py`` mints (prefix ``_src``, sanitized locator, an ALWAYS-present
    8-hex sha256 of the locator, and the ``stream`` suffix chosen because one of the three
    consumers streams). If this name drifts, every consumer silently falls back to its own
    read and R2 is lost without any error."""
    assert _defines_dataset(event_rows, PLANE_NODE_BARE), (
        f"the shared source-plane node '{PLANE_NODE_BARE}' is not in the pipeline graph -- "
        f"the three consumers of {SHARED_LOCATOR} are not sharing one read (R2)"
    )


def test_exactly_one_flow_reads_the_shared_physical_locator(event_rows):
    """R2, stated as bluntly as the event log allows: across the WHOLE update, exactly one flow
    performs a physical read of ``flowx.dag_001_usecase.shared_source_bus``.

    Three consumers declare that locator in 049 (zerobus ingestion, the
    ``shared_source_direct`` transformation input, and the reconciliation target). Two or three
    readers here means the source plane did not dedup them -- the exact regression this whole
    module exists to catch. Zero readers means the plane node is not reading the raw table at
    all, which is just as wrong.
    """
    readers = _flows_reading_shared_locator(event_rows)
    assert len(readers) == 1, (
        f"expected EXACTLY one flow reading {SHARED_LOCATOR} per update (R2), found "
        f"{len(readers)}: {sorted(readers)}"
    )


def test_the_single_locator_reader_is_the_source_plane_node(event_rows):
    """...and that one reader must be the L0 plane node, not (say) the ingestion flow having won
    a race to read the table directly while the other two consumers each opened their own."""
    readers = _flows_reading_shared_locator(event_rows)
    assert readers, f"no flow reads {SHARED_LOCATOR} at all"
    assert all(_names_match(name, PLANE_NODE_BARE) for name in readers), (
        f"the raw locator {SHARED_LOCATOR} is read by {sorted(readers)}, not by the shared "
        f"source-plane node '{PLANE_NODE_BARE}'"
    )


def test_shared_source_plane_node_executed(event_rows):
    """The plane node must have run: it is the single upstream all three consumers depend on, so
    a declared-but-never-executed node would starve the whole update."""
    assert _executed(event_rows, PLANE_NODE_BARE), (
        f"no flow_progress event for the source-plane node '{PLANE_NODE_BARE}'"
    )


# ---------------------------------------------------------------------------
# Post-update table state -- via the REST-backed conftest fixture, not spark
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bare_name",
    PUBLISHED_L4_BARE_NAMES,
    ids=["classified", "metrics", "mismatch"],
)
def test_published_l4_tables_exist_in_unity_catalog(dag_001_pipeline_id, table_exists, bare_name):
    """The three L4 datasets are PUBLISHED (not internal), so after a successful update they are
    real, queryable Unity Catalog tables under the reconciliation flow's publish schema -- 049
    sets no ``publish_schema``, so that is the pipeline's own ``flowx.bronze_dag_001``.

    Uses ``table_exists`` (a plain Unity Catalog REST call) rather than ``spark.table(...)``:
    see that fixture's docstring in tests/conftest.py for the live-reproduced evidence that this
    project's session-scoped Spark Connect session answers catalog-existence questions from a
    stale cache.
    """
    qualified = f"{PUBLISH_SCHEMA}.{bare_name}"
    assert table_exists(qualified), (
        f"'{qualified}' does not exist -- the L4 reconciliation dataset was not published by "
        "the pipeline update"
    )


def test_ingestion_target_exists(dag_001_pipeline_id, table_exists):
    """Sanity anchor for the R1 edge asserted above: the ingestion half of the group really did
    publish the table the reconciliation source binds to."""
    assert table_exists(INGESTION_TARGET), (
        f"'{INGESTION_TARGET}' does not exist -- ingestion did not run, so nothing above about "
        "the ingestion -> reconciliation edge can be trusted"
    )
