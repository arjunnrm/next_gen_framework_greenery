"""L0 source plane: plan and register exactly-once external reads for one dataflow-group pipeline.

**Why this module exists.** In "pipeline" execution mode, ingestion + transformation +
reconciliation all run inside ONE Lakeflow Declarative Pipeline update for a dataflow group.
Naively, every ingestion flow's raw source, every transformation input, and every
reconciliation source/target would re-issue its own ``spark.read``/``spark.readStream`` --
even when two or more of them point at the identical physical locator (the same landing path,
the same Delta table). A ``@dlt.view`` does not fix this: a view is inlined into *each*
consumer, so two consumers of one view still open two independent physical reads (two
``cloudFiles`` streams sharing one ``cloudFiles.schemaLocation``, or two full-table scans).

This module is the read-once contract's enforcement point (see ``docs/13`` "CANONICAL
IDENTITY" / "SHARING RULES" / "COLLISION HANDLING" sections). It runs in two disjoint phases:

1. **Plan** (:func:`plan_source_plane`) -- pure, no Spark action, no ``dlt`` import at module
   scope. Given the group's active ingestion/transformation/(pipeline-mode) reconciliation
   control-table rows, it derives one :class:`ConsumerRequest` per external read a dataset
   body will need, groups them by :class:`ReadIdentity` (the read-once identity key), and
   decides -- per the SHARING RULES -- whether each request is served by an in-graph sibling
   dataset this same pipeline already produces (``in_graph_sibling``) or by a materialized
   base node of its own (``shared_node``). It also runs the plan-time guards (G-STREAM,
   G-SIDE) and builds the producer -> consumer dependency edges :func:`assert_acyclic`
   checks.

   **Sharing rule (v1.7.3, the Single-Read architectural mandate).** Every external read
   identity that is not already produced in-graph gets its own materialized ``shared_node``,
   *unconditionally* -- fanout is no longer part of the decision. N distinct source
   identities therefore yield N base ingestion nodes, and each consumer binds via
   ``dlt.read``/``dlt.read_stream`` rather than re-reading the origin. This replaces the
   pre-v1.7.3 fanout-threshold policy, under which a single-consumer read stayed ``inline``
   (a per-consumer physical read) to preserve predicate pushdown into the origin. The
   ``inline`` :class:`Binding` kind is retained in the code but is no longer reachable for an
   external identity; it is kept as a declared kind so ``describe_plan`` output and the
   binding vocabulary stay stable.

   **Per-mode identity is retained.** "Read once" means read once per source *per execution
   mode*. Nodes are keyed by ``(identity, mode)``, so one locator read as both a stream and a
   batch is TWO nodes carrying the ``__stream`` and ``__batch`` suffixes, never one. They are
   deliberately not collapsed: a materialized view cannot be read with ``dlt.read_stream``,
   :func:`bind` raises when a ``mode="batch"`` binding is asked for a streaming read, and
   forcing either binding onto the other's node drags in checkpoint-locking and full-refresh
   side effects.
2. **Register / bind** (:func:`register_source_plane`, :func:`bind`) -- the only functions in
   this module that import ``dlt`` (lazily, inside the function body, never at module scope,
   so this module stays importable and unit-testable outside a Lakeflow pipeline runtime).
   :func:`register_source_plane` declares one ``@dlt.table`` per :class:`PlaneNode`.
   :func:`bind` is what every ingestion/transformation/reconciliation dataset body calls
   instead of reading its source directly -- it resolves a caller's ``consumer_id`` to its
   planned :class:`Binding` and returns the right ``DataFrame``.

**What is, and is not, part of the identity key.** Only the BASE-READ options that change
*which bytes are scanned* participate in :class:`ReadIdentity` (see
:func:`NextGen_Metadata_Framework.lakeflow_framework.ingestion.readers.base_read_options` and
its ``_BASE_READ_KEYS``). Everything downstream of the read -- ``schema_config``,
``column_normalization``, technical metadata, JSON flattening, dedup, standardization SQL,
decryption, watermarking, ``filter_condition``, DQ/quarantine columns -- is an OVERLAY,
applied by each consumer on top of the shared (or inline) DataFrame :func:`bind` returns.
``read_mode`` (streaming vs batch) is excluded from :class:`ReadIdentity` itself but IS part of
the node key, which is the pair ``(identity, mode)``. Two consumers differing only in read mode
therefore get two nodes, not one -- see "Per-mode identity is retained" above. (Before v1.7.3
they shared a single node under an "any consumer streams" rule, on the theory that a
materialized streaming table serves ``dlt.read`` too; the Single-Read mandate replaced that with
the stricter per-mode split, which avoids imposing a streaming node's checkpoint-locking and
full-refresh semantics on a consumer that only ever wanted a batch read.)
"""

import hashlib
import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from NextGen_Metadata_Framework.lakeflow_framework.engine.identifiers import stable_node_name
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError, FrameworkError
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.readers import (
    base_read_options,
    read_ingestion_source,
    read_locator,
)
from NextGen_Metadata_Framework.lakeflow_framework.storage.table_properties import qualified_table_name

logger = logging.getLogger("common.engine.source_plane")

#: The verbatim rejection for ``materialize="never"``, shared with
#: ``onboarding/spec_validator.py`` so the onboarding-time error and the pipeline-runtime error
#: are the same sentence. Onboarding validation runs once, at onboarding; a group onboarded
#: before v1.7.3 keeps its persisted ``source_plane_config_json`` and re-reads it on every
#: pipeline update, so a validator-only rejection would leave existing groups quietly running the
#: prohibited policy. This constant is the second half of that enforcement.
MATERIALIZE_NEVER_REJECTION = (
    "materialize='never' is deprecated and prohibited under the Single-Read architectural "
    "mandate. Remove this setting to default to 'always', ensuring base tables are read once "
    "and reused via dlt.read()."
)

#: ``cdc_load_strategy`` values under which an in-graph target is written by MERGE (SCD1/SCD2)
#: or is not a plain append (SCD3's history table, FULL_SNAPSHOT_CDC's ``apply_changes_from_snapshot``,
#: TRUNCATE_AND_LOAD's full recompute) -- a streaming read of any of these hits Delta's
#: ``DELTA_SOURCE_TABLE_IGNORE_CHANGES`` at execution time. Kept local to this module (rather than
#: imported from ``storage/table_properties.py``'s private ``_CDC_DISPATCHED_STRATEGIES``) so this
#: module's plan-time guard does not depend on another module's private implementation detail.
#:
#: ``TRUNCATE_AND_LOAD`` belongs here even though it dispatches to no CDC strategy at all: its
#: target is registered as a ``@dlt.table`` fed by a full recompute of its upstream (see
#: ``cdc/dispatcher.py``), so every update REPLACES the table's contents rather than appending to
#: them -- exactly the non-append-only shape Delta refuses to stream from. Omitting it was a real
#: gap, not a judgement call: the geneva reconciliation pipeline
#: (e41a47ba-5ad0-4dc5-9535-5aa16cc97e65) reconciles against ``stg_tariffelementband``, an in-graph
#: TRUNCATE_AND_LOAD ingestion target, so in ``execution_mode: "pipeline"`` the plane would have
#: planned a streaming read of it, passed every plan-time check, and then failed at pipeline
#: runtime -- the failure mode this guard exists to convert into an onboarding-time error.
_NON_APPEND_ONLY_CDC_STRATEGIES = frozenset({"SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC", "TRUNCATE_AND_LOAD"})

#: A ``FULL_SNAPSHOT_CDC`` ingestion/transformation flow feeds Lakeflow's
#: ``apply_changes_from_snapshot`` API, whose snapshot lambda may reference only a PATH, never
#: a pipeline dataset name (hard rule 3). Such a flow's own "source" read therefore never goes
#: through the source plane at all -- it happens inside the snapshot function, on the
#: framework's existing, unchanged snapshot-loading code path.
_SNAPSHOT_EXCLUDED_STRATEGY = "FULL_SNAPSHOT_CDC"

#: Public alias of the strategy above. engine/flow_generators.py must branch on exactly the same
#: value this module excludes -- if the two ever disagree, a flow is planned by one side and not
#: the other and the mismatch surfaces only at pipeline runtime as
#: "source_plane.bind: unknown consumer_id". One spelling, imported, keeps that impossible.
SNAPSHOT_EXCLUDED_FROM_SOURCE_PLANE = _SNAPSHOT_EXCLUDED_STRATEGY


class FrameworkGraphCycleError(FrameworkError):
    """Raised by :func:`assert_acyclic` when the source plane's producer -> consumer edges
    contain a cycle.

    Deliberately defined here rather than in ``exceptions.py``: this step's authorized file is
    ``engine/source_plane.py`` alone, and this error type is new (no other module raises or
    catches it yet). A later step that wires a shared exception module may re-home it; until
    then it is importable from this module exactly like every other framework exception.
    """


# ---------------------------------------------------------------------------------------------
# Plan-time data model
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ReadIdentity:
    """The read-once identity key for one external physical locator.

    INTERNAL to this module -- never constructed by, or exposed in the public signature of,
    any caller outside ``source_plane.py``. Callers address the plane exclusively by
    ``consumer_id`` (see :func:`plan_source_plane` / :func:`bind`); only the plan pass ever
    computes a :class:`ReadIdentity`, so a plan and a bind can never disagree about identity.

    ``locator`` is already ``casefold()``-ed and (for a path) trailing-slash-stripped by
    :func:`NextGen_Metadata_Framework.lakeflow_framework.ingestion.readers.read_locator` /
    this module's own table-locator normalization -- casefolding is load-bearing, not
    cosmetic: a case-sensitive miss here silently falls through to a duplicate physical read.
    """

    locator_kind: str  # "table" | "path" | "zerobus"
    locator: str
    options_fingerprint: str


@dataclass(frozen=True)
class ConsumerRequest:
    """One dataset body's declared need for a read of ``identity``.

    ``side_effecting`` marks a request whose underlying read also performs a filesystem-level
    side effect (Auto Loader ``cloudFiles.cleanSource*`` / ``source_zip_handling`` PGP-decrypt
    and unzip) -- true only for ``autoloader``/``asn1`` ingestion sources. It drives the
    G-SIDE guard: two requests with the same raw path locator but incompatible side-effect
    configuration are a latent data-loss bug (two competing lifecycle regimes on one
    directory), never a legitimate "these are different reads".
    """

    consumer_id: str
    identity: ReadIdentity
    want_stream: bool
    side_effecting: bool = False


@dataclass(frozen=True)
class PlaneNode:
    """One materialized L0 source-plane node: the single physical read of one external identity,
    backing every consumer of that identity.

    Since v1.7.3 (the Single-Read architectural mandate) a node exists for EVERY external read
    identity, at any fanout -- N distinct source identities produce N base ingestion nodes, and
    every downstream consumer binds to one via ``dlt.read``/``dlt.read_stream``. Fanout no longer
    decides whether a node is created; it only decides how many consumer ids the node lists.

    ``mode`` is part of the NODE KEY, not a property derived after the fact: nodes are keyed by
    ``(identity, mode)``, so one locator read as both a stream and a batch is two nodes, and
    every consumer of a given node requested that node's mode. The ``dataset_name`` carries the
    matching ``__stream``/``__batch`` suffix.

    This replaced (v1.7.3) an "any consumer streams" collapse rule, under which one node in
    stream mode served batch consumers too -- legal in the narrow sense that a materialized
    streaming table is a valid ``dlt.read`` source, but it imposed a streaming node's
    checkpoint-locking and full-refresh semantics on consumers that only ever wanted a batch
    read. Splitting by mode is what "read once per source PER EXECUTION MODE" means.

    ``published`` (v1.6.0): ``True`` only when the spec explicitly supplied
    ``source_plane.catalog``/``source_plane.schema`` -- the node is then a published
    ``catalog.schema.table``. ``False`` (the default when the spec is silent): the node is
    registered as ``@dlt.table(temporary=True)`` under its bare, pipeline-local
    ``dataset_name`` -- still materialized (read-once holds), never visible in Unity
    Catalog. The Intermediate Object Rule: an L0 node is plumbing, not a deliverable.
    """

    identity: ReadIdentity
    dataset_name: str
    mode: str  # "stream" | "batch"
    materialized: bool
    consumer_ids: List[str]
    published: bool = False


@dataclass(frozen=True)
class Binding:
    """How one ``consumer_id`` resolves at bind time."""

    kind: str  # "in_graph_sibling" | "shared_node" | "inline"
    dataset_name: Optional[str]
    reader_spec: Optional[Dict[str, Any]]
    locator: str
    mode: str  # "stream" | "batch"


@dataclass
class SourcePlanePlan:
    """The full plan output of :func:`plan_source_plane`.

    ``node_reader_specs`` is an implementation-necessary addition beyond the four fields the
    read-once contract names: :func:`register_source_plane` needs to know *how* to physically
    read each ``shared_node`` identity (which :class:`PlaneNode` alone does not carry), and
    this is the one place in the plan that can hold it without smuggling a fifth field onto
    the otherwise-exact :class:`PlaneNode` shape. Never consulted by :func:`bind` -- a
    consumer only ever needs ``bindings``.
    """

    nodes: Dict[Tuple[ReadIdentity, str], PlaneNode]
    bindings: Dict[str, Binding]
    in_graph_targets: Set[str]
    edges: List[Tuple[str, str]]
    node_reader_specs: Dict[Tuple[ReadIdentity, str], Dict[str, Any]] = field(default_factory=dict)


# ---------------------------------------------------------------------------------------------
# Identity helpers
# ---------------------------------------------------------------------------------------------


def _fingerprint(base_options: Dict[str, Any]) -> str:
    """``sha256`` of canonical (sorted-key) JSON over a BASE-READ options dict.

    ``default=str`` tolerates any non-JSON-native value that slipped into ``source_config``
    (e.g. a ``Decimal``) without raising -- a fingerprint mismatch on such a value would only
    cost an extra, harmless physical read, whereas raising here would fail the whole plan pass.
    """
    return hashlib.sha256(json.dumps(base_options, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _json_loads(raw: Optional[str], default: Any) -> Any:
    """``json.loads`` a control-table JSON-string column, tolerating ``None``/empty."""
    if not raw:
        return default
    return json.loads(raw)


def _row_get(row: Any, name: str, default: Any = None) -> Any:
    """Attribute-style field access that works for a Spark ``Row`` and any row-like stand-in
    (e.g. a test double) that sets plain attributes."""
    return getattr(row, name, default)


def _table_identity(qualified_table: str) -> ReadIdentity:
    """:class:`ReadIdentity` for a plain Delta-table read (a transformation input, or a
    reconciliation source/target) -- no ``source_config``-shaped base-read options apply to
    reading an existing table, so the fingerprint is taken over an empty options dict. Every
    consumer of the same table therefore shares one identity regardless of the per-consumer
    overlay (``filter_condition``, ``data_standardization_sql``, hashing, etc.) applied
    downstream of the shared read.
    """
    return ReadIdentity("table", qualified_table.casefold(), _fingerprint({}))


# ---------------------------------------------------------------------------------------------
# Plan pass
# ---------------------------------------------------------------------------------------------


def _ingestion_target_locator(row: Any) -> str:
    return qualified_table_name(
        _row_get(row, "target_catalog"), _row_get(row, "target_schema"), _row_get(row, "target_table")
    )


def _transformation_target_locator(row: Any) -> str:
    return qualified_table_name(
        _row_get(row, "target_catalog"), _row_get(row, "target_schema"), _row_get(row, "target_table")
    )


def _collect_in_graph_targets(
    ingestion_rows: List[Any], transformation_rows: List[Any]
) -> Tuple[Set[str], Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Build ``(in_graph_targets, producer_meta_by_locator, qualified_name_by_locator)``.

    ``in_graph_targets`` and ``producer_meta_by_locator`` are keyed by the ``casefold()``-ed
    qualified target name, per the read-once contract ("built from
    ``qualified_table_name(r.target_catalog, r.target_schema, r.target_table)`` over
    ingestion+transformation rows"). Reconciliation targets are deliberately excluded --
    nothing may read a reconciliation flow's published comparison datasets as an in-graph
    sibling source.
    """
    in_graph_targets: Set[str] = set()
    producer_meta_by_locator: Dict[str, Dict[str, Any]] = {}
    qualified_name_by_locator: Dict[str, str] = {}

    for row in ingestion_rows:
        qualified = _ingestion_target_locator(row)
        casefolded = qualified.casefold()
        in_graph_targets.add(casefolded)
        qualified_name_by_locator[casefolded] = qualified
        producer_meta_by_locator[casefolded] = {
            "cdc_load_strategy": _row_get(row, "cdc_load_strategy"),
            "target_type": _row_get(row, "target_type"),
            "flow_id": _row_get(row, "dataflow_id"),
            "owner_node": casefolded,
        }

    for row in transformation_rows:
        qualified = _transformation_target_locator(row)
        casefolded = qualified.casefold()
        in_graph_targets.add(casefolded)
        qualified_name_by_locator[casefolded] = qualified
        producer_meta_by_locator[casefolded] = {
            "cdc_load_strategy": _row_get(row, "cdc_load_strategy"),
            "target_type": _row_get(row, "target_type"),
            "flow_id": _row_get(row, "flow_step_id"),
            "owner_node": casefolded,
        }

    return in_graph_targets, producer_meta_by_locator, qualified_name_by_locator


def _requests_from_ingestion_rows(
    ingestion_rows: List[Any],
) -> Tuple[List[ConsumerRequest], Dict[str, str], Dict[ReadIdentity, Dict[str, Any]], Dict[str, str]]:
    """Derive one :class:`ConsumerRequest` per non-snapshot ingestion row's raw source read.

    Returns ``(requests, owner_by_consumer_id, reader_spec_by_identity, side_effect_key_by_consumer_id)``.

    A row whose ``cdc_load_strategy`` is ``FULL_SNAPSHOT_CDC`` is skipped entirely (hard rule
    3 / the contract's explicit snapshot exclusion): its "source" is consumed by
    ``apply_changes_from_snapshot``'s own path-based lambda, never through the plane.

    Every remaining ingestion source request has ``want_stream=True`` -- every registered
    ingestion reader (``autoloader``, ``zerobus``, ``asn1``) is unconditionally implemented as
    a ``spark.readStream`` (see ``ingestion/readers.py``); there is no batch ingestion reader
    to fall back to. This is also why an ingestion-origin :class:`PlaneNode` can never end up
    ``mode="batch"``: at least one of its consumers (an ingestion row's own source request)
    always wants a stream.
    """
    requests: List[ConsumerRequest] = []
    owner_by_consumer_id: Dict[str, str] = {}
    reader_spec_by_identity: Dict[ReadIdentity, Dict[str, Any]] = {}
    side_effect_key_by_consumer_id: Dict[str, str] = {}

    for row in ingestion_rows:
        cdc_load_strategy = _row_get(row, "cdc_load_strategy")
        if cdc_load_strategy == _SNAPSHOT_EXCLUDED_STRATEGY:
            continue

        dataflow_id = _row_get(row, "dataflow_id")
        source_type = _row_get(row, "source_type")
        source_config = _json_loads(_row_get(row, "source_config_json"), {})

        locator_kind, locator = read_locator(source_config, source_type)
        options_fingerprint = _fingerprint(base_read_options(source_type, source_config))
        identity = ReadIdentity(locator_kind, locator, options_fingerprint)

        consumer_id = f"{dataflow_id}:source"
        side_effecting = source_type in ("autoloader", "asn1")

        requests.append(ConsumerRequest(consumer_id, identity, want_stream=True, side_effecting=side_effecting))
        owner_by_consumer_id[consumer_id] = _ingestion_target_locator(row).casefold()
        reader_spec_by_identity.setdefault(
            identity, {"origin": "ingestion", "source_type": source_type, "source_config": source_config}
        )
        if side_effecting:
            side_effect_key_by_consumer_id[consumer_id] = json.dumps(
                {
                    "landing_retention_policy": source_config.get("landing_retention_policy"),
                    "source_zip_handling": source_config.get("source_zip_handling"),
                },
                sort_keys=True,
                default=str,
            )

    return requests, owner_by_consumer_id, reader_spec_by_identity, side_effect_key_by_consumer_id


def _requests_from_transformation_rows(
    transformation_rows: List[Any],
) -> Tuple[List[ConsumerRequest], Dict[str, str], Dict[ReadIdentity, Dict[str, Any]]]:
    """Derive one :class:`ConsumerRequest` per transformation-flow input.

    ``consumer_id`` is ``"<flow_step_id>:input:<input_name>"`` -- ``input_name`` stays the SQL
    identifier a flow's ``transformation_sql`` references, but is demoted from *being* the
    read to being an alias over the plane (see the read-once contract's L2 node entry). Each
    input's own ``is_streaming`` flag (already present on every ``source_inputs_json`` entry)
    is honoured verbatim as ``want_stream`` -- the plane does not second-guess it.
    """
    requests: List[ConsumerRequest] = []
    owner_by_consumer_id: Dict[str, str] = {}
    reader_spec_by_identity: Dict[ReadIdentity, Dict[str, Any]] = {}

    for row in transformation_rows:
        flow_step_id = _row_get(row, "flow_step_id")
        owner_locator = _transformation_target_locator(row).casefold()
        inputs = _json_loads(_row_get(row, "source_inputs_json"), [])

        for input_spec in inputs:
            input_name = input_spec["input_name"]
            table = input_spec["table"]
            want_stream = bool(input_spec.get("is_streaming", False))

            identity = _table_identity(table)
            consumer_id = f"{flow_step_id}:input:{input_name}"

            requests.append(ConsumerRequest(consumer_id, identity, want_stream=want_stream, side_effecting=False))
            owner_by_consumer_id[consumer_id] = owner_locator
            reader_spec_by_identity.setdefault(identity, {"origin": "table", "table": table})

    return requests, owner_by_consumer_id, reader_spec_by_identity


#: Reconciliation flows whose ``execution_mode`` resolves to this value are the standalone
#: job-task engine's rows -- they never enter the pipeline's source plane at all.
_JOB_EXECUTION_MODE = "job"


def _requests_from_reconciliation_rows(
    reconciliation_rows: List[Any],
) -> Tuple[List[ConsumerRequest], Dict[str, str], Dict[ReadIdentity, Dict[str, Any]]]:
    """Derive the ``"<reconciliation_id>:source"`` and ``"<reconciliation_id>:target:<target_id>"``
    :class:`ConsumerRequest`\\ s for every pipeline-mode reconciliation row.

    A row with ``execution_mode`` absent/``None``/``"job"`` is skipped -- it belongs to the
    standalone job-task reconciliation path (see ``control_plane/repository.py``'s own
    identical filter), which this pipeline-mode plan never touches.

    The source side's ``want_stream`` is ``True`` only for ``execution_mode == "pipeline"``:
    that is the only mode with an L5 heal lane, whose ``_recon__<id>__pulse`` node needs a
    streaming read of the source (hard rule -- ``dlt.create_sink``/``dlt.foreach_batch_sink``
    accept streaming queries only). ``"pipeline_audit_only"`` registers L3+L4 only, so its
    source read stays batch. Every target side is always ``want_stream=False`` -- the far-side
    ``_tgt`` node is always a materialized view (see the read-once contract's L3 node entry).
    """
    requests: List[ConsumerRequest] = []
    owner_by_consumer_id: Dict[str, str] = {}
    reader_spec_by_identity: Dict[ReadIdentity, Dict[str, Any]] = {}

    for row in reconciliation_rows:
        execution_mode = _row_get(row, "execution_mode") or _JOB_EXECUTION_MODE
        if execution_mode == _JOB_EXECUTION_MODE:
            continue

        reconciliation_id = _row_get(row, "reconciliation_id")
        owner_id = f"__reconciliation__{reconciliation_id}"

        source_config = _json_loads(_row_get(row, "source_config_json"), {})
        source_table = source_config["table"]
        source_identity = _table_identity(source_table)
        source_consumer_id = f"{reconciliation_id}:source"
        requests.append(
            ConsumerRequest(
                source_consumer_id,
                source_identity,
                want_stream=(execution_mode == "pipeline"),
                side_effecting=False,
            )
        )
        owner_by_consumer_id[source_consumer_id] = owner_id
        reader_spec_by_identity.setdefault(source_identity, {"origin": "table", "table": source_table})

        target_configs = _json_loads(_row_get(row, "target_configs_json"), [])
        for target_config in target_configs:
            target_id = target_config["target_id"]
            target_table = target_config["table"]
            target_identity = _table_identity(target_table)
            target_consumer_id = f"{reconciliation_id}:target:{target_id}"
            requests.append(
                ConsumerRequest(target_consumer_id, target_identity, want_stream=False, side_effecting=False)
            )
            owner_by_consumer_id[target_consumer_id] = owner_id
            reader_spec_by_identity.setdefault(target_identity, {"origin": "table", "table": target_table})

    return requests, owner_by_consumer_id, reader_spec_by_identity


def _apply_guards(
    requests: List[ConsumerRequest],
    in_graph_targets: Set[str],
    producer_meta_by_locator: Dict[str, Dict[str, Any]],
    side_effect_key_by_consumer_id: Dict[str, str],
) -> None:
    """Run the plan-time guards. Raises :class:`FrameworkConfigError` on the first violation.

    G-STREAM: a streaming request against an in-graph sibling whose producer is written by
    MERGE/snapshot semantics (SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC) or published as a
    ``materialized_view`` would hit Delta's ``DELTA_SOURCE_TABLE_IGNORE_CHANGES`` at execution
    time. The only "fix" Delta itself offers -- ``skipChangeCommits`` -- is refused outright
    (hard rule 6): it silently drops every changed row rather than failing loudly, which is a
    strictly worse outcome than failing the pipeline update at plan time.

    G-SIDE: two or more ingestion requests over the identical raw path locator with
    incompatible ``landing_retention_policy``/``source_zip_handling`` configuration is a
    latent data-loss bug (two competing file lifecycle regimes on one landing directory), not
    a legitimate difference in what is being read -- it is rejected regardless of whether the
    two requests happen to collapse to the same :class:`ReadIdentity`.
    """
    for request in requests:
        if not request.want_stream:
            continue
        locator = request.identity.locator
        if locator not in in_graph_targets:
            continue
        producer = producer_meta_by_locator[locator]
        cdc_load_strategy = producer.get("cdc_load_strategy")
        target_type = producer.get("target_type")
        if cdc_load_strategy in _NON_APPEND_ONLY_CDC_STRATEGIES or target_type == "materialized_view":
            raise FrameworkConfigError(
                f"Consumer '{request.consumer_id}' requested a streaming read of '{locator}', which is "
                f"produced in this same pipeline by flow '{producer.get('flow_id')}' with "
                f"cdc_load_strategy={cdc_load_strategy!r} / target_type={target_type!r}. A streaming read "
                f"of a MERGE-written, snapshot-applied, fully-recomputed (TRUNCATE_AND_LOAD) or "
                f"materialized-view target raises Delta's DELTA_SOURCE_TABLE_IGNORE_CHANGES at execution "
                f"time. skipChangeCommits is refused as a workaround because it silently drops changed "
                f"rows rather than failing loudly -- read '{locator}' as a batch (dlt.read) consumer "
                f"instead. For a reconciliation flow, that means execution_mode 'pipeline_audit_only' "
                f"(which reads its source as a batch) rather than 'pipeline'."
            )

    side_effect_keys_by_locator: Dict[str, Set[str]] = defaultdict(set)
    consumer_ids_by_locator: Dict[str, List[str]] = defaultdict(list)
    for request in requests:
        if not request.side_effecting:
            continue
        key = side_effect_key_by_consumer_id.get(request.consumer_id)
        if key is None:
            continue
        side_effect_keys_by_locator[request.identity.locator].add(key)
        consumer_ids_by_locator[request.identity.locator].append(request.consumer_id)

    for locator, keys in side_effect_keys_by_locator.items():
        if len(keys) > 1:
            raise FrameworkConfigError(
                f"Path '{locator}' is read by {len(consumer_ids_by_locator[locator])} ingestion flow(s) "
                f"({', '.join(sorted(consumer_ids_by_locator[locator]))}) with more than one distinct "
                f"landing_retention_policy / source_zip_handling configuration. Two competing file "
                f"lifecycle regimes (cloudFiles.cleanSource move/delete, PGP-decrypt-then-unzip) on one "
                f"landing directory is a latent data-loss bug -- configure identical "
                f"landing_retention_policy / source_zip_handling for every flow reading this path."
            )


def plan_source_plane(
    ingestion_rows: List[Any],
    transformation_rows: List[Any],
    reconciliation_rows: List[Any],
    pipeline_parameters: Optional[Dict[str, Any]] = None,
    materialize: str = "always",
    node_catalog: Optional[str] = None,
    node_schema: Optional[str] = None,
) -> SourcePlanePlan:
    """Plan the L0 source plane for one dataflow-group pipeline update.

    Pure: no Spark action, no ``dlt`` import. Safe to call, and to unit test, with no active
    Spark session at all -- every row is a plain attribute-bearing object (a Spark ``Row`` in
    production; anything duck-typed the same way in a test).

    Parameters
    ----------
    ingestion_rows, transformation_rows, reconciliation_rows:
        The group's active control-table rows, exactly as returned by
        ``control_plane/repository.py``'s ``load_active_group_metadata`` (``reconciliation_rows``
        already pre-filtered to pipeline-mode rows there; a stray ``"job"``-mode row reaching
        this function is additionally skipped here, defensively).
    pipeline_parameters:
        The group's ``${param}`` substitution map. Accepted for forward compatibility (a future
        guard or node-naming rule may need it) -- unused today, since every row's JSON columns
        have already had ``${param}``/``{{catalog}}`` substitution applied by
        ``onboarding/spec_loader.py`` before reaching the control tables.
    materialize:
        ``"always"`` (default since v1.7.3) -- materialize EVERY external read identity into
        its own base node, at any fanout, so N source identities yield N base ingestion nodes
        and every consumer binds via ``dlt.read``/``dlt.read_stream``. This is the Single-Read
        architectural mandate and the only policy the framework is designed around.

        ``"auto"`` is the retained LEGACY policy and is genuinely distinct, not an alias: it
        materializes an identity only at fanout >= 2 and leaves a single-consumer identity
        ``inline``, re-read inside that consumer's own closure. Fanout is counted per
        ``(identity, mode)`` -- the same key the node dict uses -- so a locator consumed once as
        a stream and once as a batch is fanout 1 in EACH mode, not fanout 2; those two consumers
        can never share one node anyway (a materialized view cannot be read with
        ``dlt.read_stream``). It exists so a control-table row onboarded before v1.7.3 keeps the
        DAG topology it was onboarded with rather than silently gaining base nodes on
        re-onboarding. It is a strictly WEAKER guarantee than ``"always"`` and is not recommended
        for new specs.

        ``"never"`` is PROHIBITED. It is rejected at onboarding time by
        ``onboarding/spec_validator.py`` and raised on here as well, because onboarding
        validation only ever runs once: a group onboarded before v1.7.3 keeps its persisted
        ``source_plane_config_json`` forever, and that row is read straight into this function
        by ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``. Rejecting it in both
        places is what makes the prohibition true for existing groups rather than only for new
        ones.
    node_catalog, node_schema:
        Where a ``shared_node`` :class:`PlaneNode` is *published*, when the spec explicitly
        asks for publication (``source_plane.catalog`` + ``source_plane.schema`` both set).
        Since v1.6.0 these are genuinely optional: when either is absent, a shared node is
        registered as a pipeline-scoped ``@dlt.table(temporary=True)`` under its bare name
        instead -- still materialized (read-once holds), never published to Unity Catalog
        (the Intermediate Object Rule). Callers must pass the spec's raw values, NOT a
        fallback to the pipeline's own catalog/schema -- the fallback is what used to make
        every L0 node a published table.

    Returns
    -------
    SourcePlanePlan

    Raises
    ------
    FrameworkConfigError
        On a G-STREAM or G-SIDE guard violation.
    """
    if materialize == "never":
        raise FrameworkConfigError(f"source_plane.materialize: {MATERIALIZE_NEVER_REJECTION}")

    in_graph_targets, producer_meta_by_locator, qualified_name_by_locator = _collect_in_graph_targets(
        ingestion_rows, transformation_rows
    )

    ingestion_requests, ingestion_owners, ingestion_reader_specs, side_effect_keys = _requests_from_ingestion_rows(
        ingestion_rows
    )
    transformation_requests, transformation_owners, transformation_reader_specs = _requests_from_transformation_rows(
        transformation_rows
    )
    reconciliation_requests, reconciliation_owners, reconciliation_reader_specs = _requests_from_reconciliation_rows(
        reconciliation_rows
    )

    all_requests = ingestion_requests + transformation_requests + reconciliation_requests
    owner_by_consumer_id: Dict[str, str] = {}
    owner_by_consumer_id.update(ingestion_owners)
    owner_by_consumer_id.update(transformation_owners)
    owner_by_consumer_id.update(reconciliation_owners)

    reader_spec_by_identity: Dict[ReadIdentity, Dict[str, Any]] = {}
    reader_spec_by_identity.update(ingestion_reader_specs)
    reader_spec_by_identity.update(transformation_reader_specs)
    reader_spec_by_identity.update(reconciliation_reader_specs)

    _apply_guards(all_requests, in_graph_targets, producer_meta_by_locator, side_effect_keys)

    # Group the external (non-in-graph) requests by identity to compute fanout.
    requests_by_identity: Dict[ReadIdentity, List[ConsumerRequest]] = defaultdict(list)
    for request in all_requests:
        if request.identity.locator in in_graph_targets:
            continue
        requests_by_identity[(request.identity, "stream" if request.want_stream else "batch")].append(request)

    # Keyed by (identity, mode): per-mode identity means one locator read as BOTH a stream and a
    # batch is two distinct nodes, so the mode has to be part of the key (see the mandate rule 1).
    nodes: Dict[Tuple[ReadIdentity, str], PlaneNode] = {}
    node_reader_specs: Dict[Tuple[ReadIdentity, str], Dict[str, Any]] = {}
    bindings: Dict[str, Binding] = {}
    edges: List[Tuple[str, str]] = []

    for request in all_requests:
        locator = request.identity.locator

        if locator in in_graph_targets:
            producer = producer_meta_by_locator[locator]
            dataset_name = qualified_name_by_locator[locator]
            mode = "stream" if request.want_stream else "batch"
            bindings[request.consumer_id] = Binding(
                kind="in_graph_sibling", dataset_name=dataset_name, reader_spec=None, locator=locator, mode=mode
            )
            consumer_owner = owner_by_consumer_id.get(request.consumer_id)
            if consumer_owner is not None:
                edges.append((producer["owner_node"], consumer_owner))
            continue

        request_mode = "stream" if request.want_stream else "batch"
        node_key = (request.identity, request_mode)
        sibling_requests = requests_by_identity[node_key]
        # v1.7.3 Single-Read mandate: under the DEFAULT policy ("always") every external identity
        # gets its own materialized base node regardless of fanout -- N source tables produce N
        # base ingestion nodes, and `fanout` does not participate in the decision.
        #
        # "auto" is retained as a genuinely DISTINCT legacy policy, not an alias: it materializes
        # only at fanout >= 2 and leaves a single-consumer identity `inline`, which is the
        # pre-v1.7.3 shape. It exists so a control-table row onboarded before v1.7.3 keeps the DAG
        # topology it was onboarded with instead of silently gaining nodes on re-onboarding. It is
        # a strictly weaker guarantee than "always" (a fanout-1 identity is re-read in each
        # consumer's own closure) and is not recommended for new specs.
        #
        # Fanout is counted PER (identity, mode) -- the same key the node dict uses -- so a locator
        # consumed once as a stream and once as a batch is fanout 1 in each mode, not fanout 2.
        # Counting it as 2 would materialize a node "because it is shared" for two consumers that
        # never actually share one.
        #
        # "never" cannot reach here: it is rejected above.
        fanout = len(sibling_requests)
        share = materialize != "auto" or fanout >= 2

        # Retained as dead-code defence only: no external identity reaches this branch now. (A
        # request whose locator is in `in_graph_targets` short-circuits to `in_graph_sibling`
        # long before this point, so `inline` is not how those are served either.)
        if not share:  # pragma: no cover - unreachable under the Single-Read mandate
            bindings[request.consumer_id] = Binding(
                kind="inline",
                dataset_name=None,
                reader_spec=reader_spec_by_identity[request.identity],
                locator=locator,
                mode="stream" if request.want_stream else "batch",
            )
            continue

        node = nodes.get(node_key)
        if node is None:
            # Per-mode identity (AGENTS.md Single-Read mandate, rule 1): stream and batch of one
            # locator are TWO nodes and are never collapsed. A materialized view cannot be read
            # with dlt.read_stream, and forcing one binding onto the other introduces checkpoint
            # locking / full-refresh side effects -- so the mode comes from THIS request, not
            # from "does any sibling stream".
            node_mode = request_mode
            suffix = node_mode
            # The digest must cover the FULL identity, not just the locator: two reads of one
            # locator under different base-read options are two identities and therefore two
            # nodes, and two nodes sharing a dataset name fail the Lakeflow update outright
            # ("Cannot redefine dataset"). Unreachable before v1.7.3 -- such a pair was fanout 1
            # apiece and stayed inline, so neither was ever named.
            node_table_name = stable_node_name(
                "_src", locator, suffix, discriminator=request.identity.options_fingerprint
            )
            # v1.6.0 Intermediate Object Rule: a shared node is published to UC only when the
            # spec explicitly asked for it (source_plane.catalog + source_plane.schema both
            # set). Otherwise it stays a pipeline-scoped temporary table under its bare name
            # -- materialized (read-once still holds) but never visible outside the pipeline.
            published = bool(node_catalog and node_schema)
            node_dataset_name = (
                qualified_table_name(node_catalog, node_schema, node_table_name) if published else node_table_name
            )
            node = PlaneNode(
                identity=request.identity,
                dataset_name=node_dataset_name,
                mode=node_mode,
                materialized=True,
                consumer_ids=[r.consumer_id for r in sibling_requests],
                published=published,
            )
            nodes[node_key] = node
            node_reader_specs[node_key] = reader_spec_by_identity[request.identity]

        bindings[request.consumer_id] = Binding(
            kind="shared_node",
            dataset_name=node.dataset_name,
            reader_spec=None,
            locator=locator,
            mode=node.mode,
        )

    return SourcePlanePlan(
        nodes=nodes,
        bindings=bindings,
        in_graph_targets=in_graph_targets,
        edges=edges,
        node_reader_specs=node_reader_specs,
    )


# ---------------------------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------------------------


def assert_acyclic(plan: SourcePlanePlan) -> None:
    """Kahn's algorithm over ``plan.edges`` (producer-owner -> consumer-owner). Raises
    :class:`FrameworkGraphCycleError` listing the ring if the source plane's in-graph-sibling
    dependencies are not a DAG.

    Only ``in_graph_sibling`` relationships contribute edges -- a ``shared_node``/``inline``
    binding always reads something external to this group's own targets, so it can never
    participate in a cycle among this group's own flows.
    """
    nodes: Set[str] = set()
    adjacency: Dict[str, List[str]] = defaultdict(list)
    in_degree: Dict[str, int] = defaultdict(int)

    for producer, consumer in plan.edges:
        nodes.add(producer)
        nodes.add(consumer)
        adjacency[producer].append(consumer)
        in_degree[consumer] += 1
        in_degree.setdefault(producer, in_degree.get(producer, 0))

    queue = deque(n for n in nodes if in_degree.get(n, 0) == 0)
    visited_count = 0
    while queue:
        current = queue.popleft()
        visited_count += 1
        for neighbor in adjacency.get(current, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if visited_count == len(nodes):
        return

    # A cycle exists among the unvisited (in_degree never reached 0) nodes. Walk it out via DFS
    # for a human-readable ring in the error message.
    remaining = {n for n in nodes if in_degree.get(n, 0) > 0}
    start = next(iter(remaining))
    path = [start]
    visited_on_path = {start}
    current = start
    while True:
        next_node = next((n for n in adjacency.get(current, []) if n in remaining), None)
        if next_node is None:
            break
        if next_node in visited_on_path:
            cycle_start_index = path.index(next_node)
            ring = path[cycle_start_index:] + [next_node]
            raise FrameworkGraphCycleError(
                f"Source plane has a cyclic in-graph-sibling dependency: {' -> '.join(ring)}"
            )
        path.append(next_node)
        visited_on_path.add(next_node)
        current = next_node

    raise FrameworkGraphCycleError(
        f"Source plane has a cyclic in-graph-sibling dependency among: {', '.join(sorted(remaining))}"
    )


# ---------------------------------------------------------------------------------------------
# Register / bind (dlt-touching -- lazy import only)
# ---------------------------------------------------------------------------------------------


def _active_spark():
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise FrameworkConfigError(
            "source_plane: no active SparkSession -- register_source_plane/bind must run inside a "
            "Lakeflow pipeline's dataset-registration context."
        )
    return spark


def _execute_reader(spark, reader_spec: Dict[str, Any], streaming: bool):
    """Physically perform the read described by ``reader_spec``.

    ``reader_spec["origin"] == "ingestion"`` dispatches to
    :func:`NextGen_Metadata_Framework.lakeflow_framework.ingestion.readers.read_ingestion_source`,
    which is unconditionally a ``spark.readStream`` for every registered source type -- there is
    no batch ingestion reader, so ``streaming=False`` here is a plan-time inconsistency (an
    ingestion-origin identity's :class:`ConsumerRequest` always has ``want_stream=True`` -- see
    :func:`_requests_from_ingestion_rows`) and is rejected rather than silently mis-reading.
    ``reader_spec["origin"] == "table"`` reads an existing Delta table, batch or streaming per
    ``streaming``.
    """
    origin = reader_spec["origin"]
    if origin == "ingestion":
        if not streaming:
            raise FrameworkConfigError(
                f"source_plane: ingestion source '{reader_spec.get('source_type')}' has no batch reader -- "
                f"a batch (want_stream=False) bind of this identity is not supported."
            )
        return read_ingestion_source(spark, reader_spec["source_type"], reader_spec["source_config"])

    if origin == "table":
        table = reader_spec["table"]
        return spark.readStream.table(table) if streaming else spark.read.table(table)

    raise FrameworkConfigError(f"source_plane: unknown reader_spec origin {origin!r}")


def register_source_plane(spark, plan: SourcePlanePlan) -> None:
    """Register one ``@dlt.table`` per :class:`PlaneNode` in ``plan.nodes``.

    Must be called exactly once per pipeline update, before any dataset body calls
    :func:`bind` for a ``shared_node`` consumer -- Lakeflow resolves ``dlt.read``/
    ``dlt.read_stream`` by dataset name at graph-build time, so the node's ``@dlt.table``
    registration must already have run.
    """
    import dlt  # noqa: F401  -- lazy: this module must stay importable outside a DLT runtime.

    for node_key, node in plan.nodes.items():
        reader_spec = plan.node_reader_specs[node_key]
        streaming = node.mode == "stream"

        def _make_source_plane_node(_spark=spark, _reader_spec=reader_spec, _streaming=streaming):
            return _execute_reader(_spark, _reader_spec, _streaming)

        dlt.table(
            name=node.dataset_name,
            temporary=not node.published,
            comment=(
                f"L0 source-plane node -- one shared read of '{node.identity.locator}' "
                f"({'streaming table' if streaming else 'materialized view'}"
                f"{'' if node.published else ', pipeline-scoped temporary'}), consumed by "
                f"{len(node.consumer_ids)} downstream reader(s): {', '.join(sorted(node.consumer_ids))}."
            ),
        )(_make_source_plane_node)


def bind(plan: SourcePlanePlan, consumer_id: str, want_stream: bool):
    """Resolve ``consumer_id`` to a ``DataFrame`` per its planned :class:`Binding`.

    Raises
    ------
    FrameworkConfigError
        If ``consumer_id`` is not a known binding, or if a batch (``mode="batch"``) binding is
        asked for a streaming read (an MV cannot be read as a stream).
    """
    import dlt

    binding = plan.bindings.get(consumer_id)
    if binding is None:
        raise FrameworkConfigError(
            f"source_plane.bind: unknown consumer_id {consumer_id!r}. Known consumer ids: "
            f"{sorted(plan.bindings)}"
        )

    if binding.kind in ("in_graph_sibling", "shared_node"):
        if want_stream and binding.mode == "batch":
            raise FrameworkConfigError(
                f"source_plane.bind: consumer '{consumer_id}' requested a streaming read of "
                f"'{binding.dataset_name}', but it is bound as a batch dataset (materialized view) -- "
                f"a materialized view cannot be read with dlt.read_stream."
            )
        return dlt.read_stream(binding.dataset_name) if want_stream else dlt.read(binding.dataset_name)

    # inline
    spark = _active_spark()
    return _execute_reader(spark, binding.reader_spec, want_stream)


def describe_plan(plan: SourcePlanePlan) -> List[Dict[str, Any]]:
    """A flat, JSON-serializable description of the plan for logging/observability/tests.

    One row per ``consumer_id`` binding, plus one trailing row per :class:`PlaneNode`
    summarizing its fanout -- deliberately not a single nested structure, so a caller can
    ``spark.createDataFrame`` or ``json.dumps`` it directly.
    """
    rows: List[Dict[str, Any]] = []
    for consumer_id, binding in sorted(plan.bindings.items()):
        rows.append(
            {
                "row_type": "binding",
                "consumer_id": consumer_id,
                "kind": binding.kind,
                "locator": binding.locator,
                "dataset_name": binding.dataset_name,
                "mode": binding.mode,
            }
        )
    for (identity, _node_mode), node in plan.nodes.items():
        rows.append(
            {
                "row_type": "node",
                "consumer_id": None,
                "kind": "shared_node",
                "locator": identity.locator,
                "dataset_name": node.dataset_name,
                "mode": node.mode,
                "fanout": len(node.consumer_ids),
                "consumer_ids": sorted(node.consumer_ids),
            }
        )
    return rows
