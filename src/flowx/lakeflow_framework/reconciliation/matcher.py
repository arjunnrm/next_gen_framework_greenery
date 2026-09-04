"""Hash-first matching between a reconciliation flow's source and one of its targets.

**Why hash-first.** Every CDC-dispatched target table this framework materializes already
carries ``__framework_hash_key``/``__framework_hash_value`` (see ``cdc/hashing.py`` --
"precomputing once, here, at CDC-materialization time, makes hash-based reconciliation joins
cheap at any scale"). This module is that promise cashed in: when a dataset's
``hash_precomputed`` flag is ``True`` (``source_config``/``target_configs[]`` each carry their
own), the two SHA-256 columns are consumed as-is; when it's ``False``, they're computed inline
here via ``cdc/hashing.py::compute_hash_columns`` -- either way, the join between source and
target runs on a single ``__framework_hash_key`` equality, never a multi-column ``match_keys``
join, and drift detection is one ``__framework_hash_value`` comparison instead of comparing
every ``compare_columns`` entry individually. This is what lets a reconciliation flow with a
dozen ``compare_columns`` cost the same at join time as one with a single column.

**Duplicate-key safety (the one correctness property this module cannot compromise on).** A
target dataset is frequently an append-only CDC/Zerobus-style bus and may legitimately hold
more than one historical row for the same logical key (e.g. a prior reconciliation run's own
correction, appended *alongside* -- not replacing -- the stale drifted row it corrects). The
pre-redesign matcher discovered this the hard way: joining naively and classifying *per joined
row* let the same source record appear as both matched (via its new corrected counterpart) and
unmatched (via the stale row still sitting alongside it) at once, so the self-healing append
never converged and re-appended a fresh duplicate "correction" on every subsequent run. This
module keeps that lesson: after the hash-key join, every group of rows sharing the same
``__framework_hash_key`` is collapsed to one representative outcome via ``F.max_by`` ordered by
an explicit MATCHED > VALUE_DRIFT > MISSING_* priority -- a key counts as matched as soon as
*any* target-side row for it satisfies the match condition, exactly as before, just expressed
as a single-pass groupBy/``max_by`` aggregation (no window-function sort) instead of the
original per-key ``groupBy(...).agg(F.max(...))`` + re-join.

**One join serves every ``comparison_direction``.** A full outer join on ``__framework_hash_key``
simultaneously classifies every record as MATCHED, MISSING_IN_TARGET, MISSING_IN_SOURCE, or
VALUE_DRIFT in a single pass -- ``comparison_direction: "both"`` is therefore *not* two
separate join passes; it is this one join, with the caller (``reconciliation/appender.py``)
simply choosing which of the four categories to act on / log, based on the target's configured
direction. ``target_to_source`` (MISSING_IN_SOURCE) is classified here like any other outcome,
but this module has no write path at all -- it is the caller's responsibility (and this
framework's explicit design constraint) to never feed a MISSING_IN_SOURCE record into anything
that deletes or modifies the target.

**Two-tier verification (v1.3.0): Phase 1 is an early-out, never a replacement.** Everything
described above -- the hash-key join, the ``max_by`` per-key collapse, the four-way
classification -- is *Phase 2*, and it is unchanged. What v1.3.0 adds in front of it is *Phase 1*:
:func:`compute_side_fingerprint`, a single shuffle-free aggregate per side
(``count(1)`` plus an order-independent bitwise-XOR fold of ``__framework_hash_key`` and of
``__framework_hash_value``, via ``cdc/hashing.py::xor_fold_hex_digest``). When both sides'
fingerprints agree (:func:`fingerprints_match`), ``appender.py`` short-circuits the whole
comparison and never joins at all; when they disagree -- or when the flow sets
``two_tier_verification: false`` -- Phase 2 runs exactly as it always did, duplicate-key safety
and all. **Phase 1 is deliberately incapable of *deciding* that two datasets differ**: it can
only decide that they are (almost certainly) identical, and every "not identical" answer is
merely a decision to go and do the real work. That asymmetry is what makes it safe to bolt in
front of logic whose correctness this module cannot compromise on.

*The accepted probabilistic property.* A bitwise XOR fold cancels in pairs (``x XOR x == 0``),
so two datasets that differ by an even number of *identical duplicate* rows can fold to the same
digest. Folding ``row_count`` into the fingerprint alongside the two digests catches every case
where the cardinalities differ at all -- which is every duplicate-count drift -- so what remains
is the narrow same-cardinality, even-multiplicity permutation case. That is the same collision
class ``appender.py::compute_batch_fingerprint``'s restartability fingerprint has always
accepted. Concretely: Phase 1 can produce a false "equal", and can therefore skip reporting a
genuine difference; it can **never** produce a false "different", so it can never invent a
mismatch that is not there. A flow that cannot tolerate the false-equal case sets
``two_tier_verification: false`` and pays for Phase 2 on every run.

**Decision 11 (v1.5.0: the algebra is split; the graph wiring is not implemented here).**
Today ``05_reconciliation_engine.py`` runs as a standalone job task, independent of any
Lakeflow Declarative Pipeline graph. A future optimization could register a reconciliation
flow's comparison as a node *inside* the same pipeline graph that already materializes its
source/target (skipping a redundant table read) whenever ``source_config.table`` or a
``target_configs[].table`` matches a flow already registered under the reconciliation flow's
own ``dataflow_group_id`` (i.e. an ``ingestion_flow_spec``/``transformation_flow_spec`` row
with the same ``target_table`` and ``dataflow_group_id``) -- at that point this module's
``prepare_dataset_for_matching``/``classify_reconciliation_target`` functions could be called
directly from within that pipeline's own flow registration instead of from this standalone
notebook, since both already operate on plain DataFrames with no notebook-specific state
(:func:`classify_reconciliation_target` in place of ``match_reconciliation_target`` precisely
*because* it stays lazy -- see its own docstring -- which a pipeline-graph call site requires
and a notebook call site never needed). This module now offers that lazy entry point; it does
not implement the lookup or the graph wiring that would call it -- the standalone job-task path
via :func:`match_reconciliation_target` remains the must-work deliverable, and this paragraph
exists so the hook-in point is discoverable for the pass that does the wiring.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flowx.lakeflow_framework.cdc.comparison_columns import resolve_comparison_columns
from flowx.lakeflow_framework.cdc.hashing import (
    HASH_KEY_COLUMN,
    HASH_VALUE_COLUMN,
    assemble_xor_folded_digest,
    compute_hash_columns,
    xor_fold_hex_digest,
)
from flowx.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("flowx.lakeflow_framework.reconciliation.matcher")

# Namespaced identically to this framework's __framework_* technical columns (see
# cdc/hashing.py) -- a target's own raw columns are prefixed with this before joining so they
# can never collide with a like-named source column in the joined/classified output.
TARGET_COLUMN_PREFIX = "__recon_target__"

MISMATCH_TYPE_COLUMN = "__recon_mismatch_type"
MISMATCH_TYPE_MATCHED = "MATCHED"
MISMATCH_TYPE_MISSING_IN_TARGET = "MISSING_IN_TARGET"
MISMATCH_TYPE_MISSING_IN_SOURCE = "MISSING_IN_SOURCE"
MISMATCH_TYPE_VALUE_DRIFT = "VALUE_DRIFT"

# reconciliation_mismatch_log's own allowed values (see ddl_definitions.py) -- MATCHED never
# appears there, it exists only inside this module's classification.
MISMATCH_LOG_TYPES = (MISMATCH_TYPE_MISSING_IN_TARGET, MISMATCH_TYPE_MISSING_IN_SOURCE, MISMATCH_TYPE_VALUE_DRIFT)

# Phase names carried on the "reconciliation_match" structured log event (appender.py) so an
# operator reading the event stream can tell a cheap Phase 1 early-out apart from a run that
# actually did the join -- otherwise a run that skipped the join is indistinguishable from a
# run that did one and found everything matched, which is exactly the thing a two-tier
# optimization must stay auditable about.
PHASE_1_MATCH = "PHASE_1_MATCH"
PHASE_2_DETAIL = "PHASE_2_DETAIL"

# Aggregate aliases for compute_side_fingerprint's single pass. The two XOR folds are
# re-aliased off cdc/hashing.py's own FINGERPRINT_LANE_ALIASES because both folds run inside
# ONE .agg(...) here: reusing the shared lane aliases for both would produce two sets of
# identically-named output columns on the same aggregate row, which is ambiguous to read back
# by name.
_FINGERPRINT_ROW_COUNT_ALIAS = "__framework_fingerprint_row_count"
_FINGERPRINT_KEY_LANE_ALIAS = "__framework_fingerprint_key_lane_{index}"
_FINGERPRINT_VALUE_LANE_ALIAS = "__framework_fingerprint_value_lane_{index}"


def target_prefixed_column(column: str) -> str:
    """Name of ``column`` as it appears target-side in :func:`match_reconciliation_target`'s
    output (``classified_df``/``mismatch_detail_df``) -- shared with ``mismatch_logging.py``
    so both modules agree on the naming convention without hardcoding the prefix twice."""
    return f"{TARGET_COLUMN_PREFIX}{column}"


def prepare_dataset_for_matching(
    df: DataFrame,
    match_keys: List[str],
    compare_columns: Optional[List[str]],
    hash_precomputed: bool,
) -> DataFrame:
    """Ensure ``df`` carries ``__framework_hash_key``/``__framework_hash_value``, computing
    them inline when ``hash_precomputed`` is ``False``. Call once per side (source, and each
    target independently) before :func:`match_reconciliation_target`.

    **v1.4.0 signature change (breaking):** the trailing ``generate_surrogate_key`` parameter is
    gone along with the surrogate-key engine. A reconciliation flow matches on the ``match_keys``
    it declares, and both sides must actually carry those columns -- there is no longer a mode in
    which the framework hashes each side's whole payload to invent a key for it. Callers that
    passed the flag positionally must drop the argument.

    Parameters
    ----------
    df:
        One side of the comparison (already read + filtered + standardized by
        ``dataset_reader.py::read_reconciliation_dataset``).
    match_keys:
        Flow-level column names identifying the same logical record across datasets. Hashed
        into ``__framework_hash_key`` when ``hash_precomputed`` is ``False``.
    compare_columns:
        Flow-level column names compared for drift. ``None``/empty means key-presence-only
        matching -- deliberately **not** passed through as ``resolve_comparison_columns``'s
        own ``columns_to_check=None`` (which means "compare every applicable column", the
        correct default for CDC change-detection but the wrong one here): an explicitly empty
        ``compare_columns`` always resolves to zero comparison columns, so
        ``compute_hash_columns`` produces a constant ``__framework_hash_value`` and every
        co-present key counts as matched regardless of its other column values.
    hash_precomputed:
        **Consumes pre-built hashes; it never precomputes anything.** The name reads like an
        instruction ("precompute the hashes") and is in fact an assertion about the dataset
        ("this dataset's hashes were already computed, upstream"). See this module's docstring
        and ``docs/07_reconciliation_engine.md`` for the full mechanics. Concretely:

        * ``False`` (the default) -- this function computes both columns *here, now*, from raw
          columns: ``__framework_hash_key`` over ``match_keys`` in the declared order, and
          ``__framework_hash_value`` over the resolved ``compare_columns``, via
          ``cdc/hashing.py::compute_hash_columns``. Whatever the dataset already carried under
          those two names is overwritten.
        * ``True`` -- ``df`` must ALREADY carry both columns, put there by a CDC-dispatched
          framework flow with ``generate_hash_columns`` enabled (``dq/quarantine.py``). They are
          trusted verbatim and neither ``match_keys`` nor ``compare_columns`` is hashed here at
          all. Missing either column is a ``FrameworkConfigError``, not a silent recompute.

        The trust in ``True`` is real and has one precondition worth stating plainly: the
        upstream flow's ``primary_keys`` must be this flow's ``match_keys``, and its resolved
        comparison columns this flow's ``compare_columns``. They are hashed with the same
        canonical construction, so when they agree the two sides are directly comparable and the
        join gets very cheap; when they disagree nothing errors -- the hashes simply never match
        and every row reports as drifted.

    Raises
    ------
    FrameworkConfigError
        If ``hash_precomputed=True`` but either hash column is actually absent (defense in
        depth -- ``dataset_reader.py`` already rejects ``hash_precomputed=True`` for a
        non-``"table"`` dataset type, but that doesn't guarantee the table itself was ever
        actually hash-computed), or ``match_keys``/``compare_columns`` reference a column
        missing from ``df``.
    """
    if hash_precomputed:
        missing_hash_columns = {HASH_KEY_COLUMN, HASH_VALUE_COLUMN}.difference(df.columns)
        if missing_hash_columns:
            raise FrameworkConfigError(
                f"hash_precomputed=True but dataset is missing {sorted(missing_hash_columns)} -- this dataset was "
                "not actually produced by a CDC-dispatched flow with generate_hash_columns enabled (see cdc/hashing.py), "
                "so its __framework_hash_key/__framework_hash_value cannot be trusted for reconciliation matching"
            )
        return df

    missing_keys = set(match_keys).difference(df.columns)
    if missing_keys:
        raise FrameworkConfigError(f"Reconciliation match_keys {match_keys} not found on dataset: {sorted(missing_keys)}")

    if compare_columns:
        missing_compare = set(compare_columns).difference(df.columns)
        if missing_compare:
            raise FrameworkConfigError(
                f"Reconciliation compare_columns {compare_columns} not found on dataset: {sorted(missing_compare)}"
            )
        resolved_compare_columns = resolve_comparison_columns(df.columns, match_keys, compare_columns, None)
    else:
        resolved_compare_columns = []

    return compute_hash_columns(df, match_keys, resolved_compare_columns)


@dataclass(frozen=True)
class ReconciliationFingerprint:
    """One side's cheap Phase 1 fingerprint (see this module's docstring).

    ``hash_key_xor``/``hash_value_xor`` are 64-character lowercase hex digests produced by an
    order-independent bitwise-XOR fold (``cdc/hashing.py::xor_fold_hex_digest``); a side with no
    rows -- and, equally, a side whose ``__framework_hash_value`` is NULL throughout because the
    flow configured no ``compare_columns`` -- folds to ``"0" * 64``. That NULL case folds
    identically on *both* sides, so a key-presence-only flow's Phase 1 comparison correctly
    reduces to "same keys, same cardinality", which is exactly what Phase 2 would have concluded
    for it (``match_reconciliation_target`` forces ``hash_equal = True`` when ``compare_columns``
    is empty).
    """

    row_count: int
    hash_key_xor: str
    hash_value_xor: str


def compute_side_fingerprint(df: DataFrame) -> ReconciliationFingerprint:
    """Phase 1: one single-pass aggregate over ``df`` -- ``count(1)`` plus both hash columns' folds.

    No join, no ``groupBy``, no ``orderBy``: a single ``.agg(...)`` producing exactly one row, so
    the cost is one scan of the side's hash columns and nothing else. Both XOR folds run inside
    that *same* aggregate rather than in two passes -- the scan is the expensive part, and
    running it twice would eat most of the saving the early-out exists to produce.

    ``df`` must already carry ``__framework_hash_key``/``__framework_hash_value`` -- call
    :func:`prepare_dataset_for_matching` on it first. This function deliberately does not hash
    anything itself: Phase 1 and Phase 2 must fingerprint and join *the same* hash columns, and
    the only way to guarantee that is for neither of them to compute one.

    Parameters
    ----------
    df:
        One prepared side of the comparison (source, or one target).

    Returns
    -------
    ReconciliationFingerprint
        This side's ``(row_count, hash_key_xor, hash_value_xor)``.

    Raises
    ------
    FrameworkConfigError
        If either hash column is missing (i.e. :func:`prepare_dataset_for_matching` was skipped),
        or the aggregate itself fails (e.g. an unreadable underlying table).
    """
    missing_hash_columns = {HASH_KEY_COLUMN, HASH_VALUE_COLUMN}.difference(df.columns)
    if missing_hash_columns:
        raise FrameworkConfigError(
            f"compute_side_fingerprint requires the DataFrame to already carry {sorted(missing_hash_columns)} -- "
            "call prepare_dataset_for_matching(...) on it first"
        )

    key_lane_aliases = []
    key_lanes = []
    for index, lane in enumerate(xor_fold_hex_digest(F.col(HASH_KEY_COLUMN))):
        alias = _FINGERPRINT_KEY_LANE_ALIAS.format(index=index)
        key_lane_aliases.append(alias)
        key_lanes.append(lane.alias(alias))

    value_lane_aliases = []
    value_lanes = []
    for index, lane in enumerate(xor_fold_hex_digest(F.col(HASH_VALUE_COLUMN))):
        alias = _FINGERPRINT_VALUE_LANE_ALIAS.format(index=index)
        value_lane_aliases.append(alias)
        value_lanes.append(lane.alias(alias))

    try:
        aggregate_row = df.agg(
            F.count(F.lit(1)).alias(_FINGERPRINT_ROW_COUNT_ALIAS), *key_lanes, *value_lanes
        ).collect()[0]
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to compute reconciliation Phase 1 fingerprint: {exc}") from exc

    return ReconciliationFingerprint(
        row_count=int(aggregate_row[_FINGERPRINT_ROW_COUNT_ALIAS] or 0),
        hash_key_xor=assemble_xor_folded_digest([aggregate_row[alias] for alias in key_lane_aliases]),
        hash_value_xor=assemble_xor_folded_digest([aggregate_row[alias] for alias in value_lane_aliases]),
    )


def fingerprints_match(source: ReconciliationFingerprint, target: ReconciliationFingerprint) -> bool:
    """``True`` only when ``row_count`` **and** both XOR digests are equal on both sides.

    See this module's docstring for the XOR fold's documented pair-cancellation property: this
    can return ``True`` for two genuinely different datasets of equal cardinality whose
    differences all have even multiplicity, but it can **never** return ``False`` for two
    identical datasets -- so it is only ever used as an early-out for the equal case, never as
    evidence of a difference. A caller that treated a ``False`` here as a reportable mismatch
    would be inventing a finding Phase 1 is not entitled to make; ``appender.py`` instead treats
    it purely as "go run Phase 2".
    """
    return (
        source.row_count == target.row_count
        and source.hash_key_xor == target.hash_key_xor
        and source.hash_value_xor == target.hash_value_xor
    )


@dataclass(frozen=True)
class ReconciliationMatchResult:
    """Per-target output of :func:`match_reconciliation_target`.

    ``missing_in_target_df`` and ``mismatch_detail_df`` are both derived from ``deduped_df``,
    the per-key aggregation described below. **Not cached**: Lakeflow Jobs in this framework
    run on serverless compute, which rejects ``DataFrame.cache()``/``.persist()`` outright
    (``[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute``,
    confirmed empirically against this project's own ``dev`` profile) -- so each consumer of
    ``deduped_df`` (the counts aggregation, the append-set key list, ``mismatch_detail_df``)
    independently re-executes the hash-key join and groupBy that produced it in the standalone
    job-task path. This is a deliberate, environment-forced trade-off (correctness and
    portability over avoiding redundant shuffles), not an oversight, for that path -- and the
    storage-backed alternative this note used to merely wish for now exists for an
    ``execution_mode: "pipeline"`` flow: there, ``source_df``/``target_df`` (see
    :func:`classify_reconciliation_target`) are themselves the
    ``_recon__<reconciliation_id>__src``/``__tgt`` Lakeflow datasets, each already materialized
    exactly once per update by the graph before this module ever sees them -- so the
    redundant-shuffle cost described above is specifically a job-mode cost, not one pipeline
    mode still pays.
    """

    deduped_df: DataFrame
    """One row per distinct ``__framework_hash_key`` with its winning classification (see
    this module's docstring) -- every other field on this result is derived from this same
    DataFrame's lineage. Exposed mainly for introspection/testing; not usually consumed
    directly by callers."""
    matched_count: int
    missing_in_target_df: DataFrame
    """Source-shaped rows (``source_df``'s own columns, minus matcher-computed hash columns
    when ``source_hash_precomputed=False`` -- see :func:`match_reconciliation_target`) for
    every record that is either entirely absent from the target or present-but-drifted. This
    is the set ``appender.py`` appends into a target's ``append_target_table`` for the
    ``source_to_target``/``both`` directions; a drifted record is re-appended as a correction
    rather than mutating the target in place, consistent with the append-only CDC/Zerobus bus
    model this framework's targets are typically built on."""
    missing_in_target_count: int
    """Union of MISSING_IN_TARGET + VALUE_DRIFT -- matches
    ``reconciliation_run_log.missing_in_target_count``'s own documented semantic exactly."""
    value_drift_count: int
    """Subset of ``missing_in_target_count`` -- records present on both sides but differing in
    at least one compared column."""
    missing_in_source_count: int
    """Records present in this target, absent from source -- ``target_to_source``, audit-only,
    reported here purely as a count. Never remediated by this module or its callers."""
    mismatch_detail_df: DataFrame
    """One row per non-MATCHED record (``__recon_mismatch_type`` populated with one of
    :data:`MISMATCH_LOG_TYPES`), carrying both sides' raw ``match_keys``/``compare_columns``
    values (target-side columns named via :func:`target_prefixed_column`) plus both sides'
    ``__framework_hash_value`` -- everything ``mismatch_logging.py`` needs to build one
    ``reconciliation_mismatch_log`` row per record, including a full column-by-column
    ``differing_columns_json`` for VALUE_DRIFT."""


@dataclass(frozen=True)
class ReconciliationClassification:
    """Lazy, pipeline-safe output of :func:`classify_reconciliation_target` -- the same
    hash-key join / priority collapse :class:`ReconciliationMatchResult` has always produced,
    minus the one eager ``.collect()`` that makes it illegal to call from inside a Lakeflow
    Declarative Pipeline ``@dlt.table`` closure reachable from a streaming read (Lakeflow rule
    2). Every field on this dataclass is an unevaluated ``DataFrame``; nothing here triggers a
    job.

    ``counts_df`` replaces :class:`ReconciliationMatchResult`'s four scalar ``*_count`` fields
    with a single, un-collected one-row aggregate carrying the same four columns
    (``matched_count``, ``missing_in_target_count``, ``value_drift_count``,
    ``missing_in_source_count``). In pipeline mode this DataFrame *is* the
    ``recon__<reconciliation_id>__<target_id>__metrics`` published dataset (a one-row
    ``@dlt.table`` carrying the flow's ``dq_config`` expectations); in job mode
    :func:`match_reconciliation_target` collects it itself, exactly as it always collected this
    same aggregate before this split.
    """

    deduped_df: DataFrame
    """Same as :attr:`ReconciliationMatchResult.deduped_df` -- one row per distinct
    ``__framework_hash_key`` with its winning classification."""
    counts_df: DataFrame
    """One-row, lazy aggregate over ``deduped_df`` -- see above. Never collected inside this
    function; collecting it is the caller's decision (the one action
    :func:`match_reconciliation_target` still performs, for job mode)."""
    missing_in_target_df: DataFrame
    """Same as :attr:`ReconciliationMatchResult.missing_in_target_df`."""
    mismatch_detail_df: DataFrame
    """Same as :attr:`ReconciliationMatchResult.mismatch_detail_df`."""


def classify_reconciliation_target(
    source_df: DataFrame,
    target_df: DataFrame,
    match_keys: List[str],
    compare_columns: Optional[List[str]] = None,
    source_hash_precomputed: bool = False,
    target_hash_precomputed: bool = False,
) -> ReconciliationClassification:
    """Fully lazy hash-key classification of ``source_df`` against ``target_df``.

    This is Phase 2's actual algebra -- the full outer join on ``__framework_hash_key``, the
    four-way MATCHED/VALUE_DRIFT/MISSING_IN_TARGET/MISSING_IN_SOURCE ``when``-chain, and the
    ``groupBy`` + ``F.max_by(priority)`` per-key collapse described in this module's docstring
    -- with every eager action removed, so it is legal to call from inside a Lakeflow
    Declarative Pipeline ``@dlt.table`` closure even when ``source_df``/``target_df`` are
    themselves reachable from a streaming read (Lakeflow rule 2: no eager action --
    ``.collect()``/``.count()``/``.first()``/``.take()``/``.isEmpty()`` -- on such a plan; a
    lazy aggregate like ``counts_df`` below is fine).
    :func:`match_reconciliation_target` is now a thin wrapper around this function that
    performs the one collect job mode still needs; its signature, behaviour and return type are
    unchanged by this split.

    Parameters
    ----------
    source_df, target_df:
        Prepared DataFrames -- see :func:`match_reconciliation_target`'s own docstring for the
        duplicate-key-safety property both sides preserve.
    match_keys, compare_columns:
        Same flow-level lists passed to :func:`prepare_dataset_for_matching` for both sides.
    source_hash_precomputed:
        Controls whether ``__framework_hash_value`` is stripped from ``missing_in_target_df`` --
        see :func:`match_reconciliation_target`'s own docstring; unchanged here.
    target_hash_precomputed:
        Accepted for signature symmetry with ``source_hash_precomputed`` and reserved for a
        future pipeline-mode caller (e.g. the
        ``recon__<reconciliation_id>__<target_id>__classified`` node's own call site) -- the
        classification algebra below is target-hash-agnostic today, exactly as
        :func:`match_reconciliation_target` always was (it only ever accepted
        ``source_hash_precomputed``), so this parameter currently has no effect on the output.

    Returns
    -------
    ReconciliationClassification
        ``deduped_df``, ``counts_df``, ``missing_in_target_df``, ``mismatch_detail_df`` -- all
        lazy. Nothing is collected.

    Raises
    ------
    FrameworkConfigError
        If ``match_keys``/``compare_columns`` are missing from either side, or either side is
        missing its hash columns (i.e. :func:`prepare_dataset_for_matching` was skipped).
    """
    compare_columns = compare_columns or []

    for label, df in (("source", source_df), ("target", target_df)):
        missing_keys = set(match_keys).difference(df.columns)
        missing_compare = set(compare_columns).difference(df.columns)
        if missing_keys or missing_compare:
            raise FrameworkConfigError(
                f"Reconciliation match_keys/compare_columns not found on {label} dataset -- "
                f"missing match_keys: {sorted(missing_keys)}, missing compare_columns: {sorted(missing_compare)}"
            )
        missing_hash_columns = {HASH_KEY_COLUMN, HASH_VALUE_COLUMN}.difference(df.columns)
        if missing_hash_columns:
            raise FrameworkConfigError(
                f"match_reconciliation_target requires the {label} DataFrame to already carry "
                f"{sorted(missing_hash_columns)} -- call prepare_dataset_for_matching(...) on it first"
            )

    # __framework_hash_key is always kept (a deterministic function of match_keys, harmless
    # and useful downstream -- see the source_hash_precomputed docstring above);
    # __framework_hash_value is a by-product of *this join* when not already real, so it's
    # dropped unless it was genuinely part of the source table's own schema.
    append_columns = [c for c in source_df.columns if source_hash_precomputed or c != HASH_VALUE_COLUMN]

    comparison_columns_needed = list(dict.fromkeys([*match_keys, *compare_columns, HASH_VALUE_COLUMN]))

    source_narrow = source_df.select(F.col(HASH_KEY_COLUMN), *comparison_columns_needed).withColumn(
        "__recon_src_present", F.lit(True)
    )
    target_narrow = target_df.select(
        F.col(HASH_KEY_COLUMN),
        *[F.col(c).alias(target_prefixed_column(c)) for c in comparison_columns_needed],
    ).withColumn("__recon_tgt_present", F.lit(True))

    joined = source_narrow.join(target_narrow, on=HASH_KEY_COLUMN, how="full_outer")

    src_present = F.col("__recon_src_present").isNotNull()
    tgt_present = F.col("__recon_tgt_present").isNotNull()
    if compare_columns:
        hash_equal = F.col(HASH_VALUE_COLUMN).eqNullSafe(F.col(target_prefixed_column(HASH_VALUE_COLUMN)))
    else:
        # DDL semantics: null/empty compare_columns means key-presence-only matching -- forced
        # to True regardless of the actual hash values, since a hash_precomputed target may
        # legitimately carry a real (non-null) __framework_hash_value computed from *its own*
        # upstream flow's comparison columns, which this reconciliation flow explicitly asked
        # to ignore.
        hash_equal = F.lit(True)

    mismatch_type = (
        F.when(src_present & tgt_present & hash_equal, F.lit(MISMATCH_TYPE_MATCHED))
        .when(src_present & tgt_present, F.lit(MISMATCH_TYPE_VALUE_DRIFT))
        .when(src_present, F.lit(MISMATCH_TYPE_MISSING_IN_TARGET))
        .otherwise(F.lit(MISMATCH_TYPE_MISSING_IN_SOURCE))
    )
    joined = joined.withColumn(MISMATCH_TYPE_COLUMN, mismatch_type)

    # Collapse every row sharing the same __framework_hash_key to one representative outcome:
    # MATCHED beats VALUE_DRIFT beats MISSING_* (a key counts as matched as soon as *any*
    # target-side row for it matches) -- see this module's docstring for why this is not
    # optional. F.max_by picks each output column's value from whichever row achieved the
    # winning priority, all in a single groupBy/aggregate pass (no window-function sort).
    priority = (
        F.when(F.col(MISMATCH_TYPE_COLUMN) == MISMATCH_TYPE_MATCHED, F.lit(3))
        .when(F.col(MISMATCH_TYPE_COLUMN) == MISMATCH_TYPE_VALUE_DRIFT, F.lit(2))
        .otherwise(F.lit(1))
    )
    target_side_columns = [target_prefixed_column(c) for c in comparison_columns_needed]
    # Not cached (serverless compute rejects DataFrame.cache()/.persist() -- see
    # ReconciliationMatchResult's docstring): each of this function's consumers below
    # (the counts aggregate, the append-set key list, mismatch_detail_df) independently
    # re-executes this groupBy/aggregate from deduped's lineage.
    deduped = joined.groupBy(HASH_KEY_COLUMN).agg(
        F.max_by(F.col(MISMATCH_TYPE_COLUMN), priority).alias(MISMATCH_TYPE_COLUMN),
        *[F.max_by(F.col(c), priority).alias(c) for c in comparison_columns_needed],
        *[F.max_by(F.col(c), priority).alias(c) for c in target_side_columns],
    )

    # Lazy -- deliberately never collected here. Collecting it would be illegal inside a
    # @dlt.table closure reachable from a streaming read (Lakeflow rule 2); in pipeline mode
    # this DataFrame IS the recon__<reconciliation_id>__<target_id>__metrics published
    # dataset, and Lakeflow itself materializes it. match_reconciliation_target collects it
    # below, for job mode -- the one action this split moved out of this function.
    counts_df = deduped.agg(
        F.sum(F.when(F.col(MISMATCH_TYPE_COLUMN) == MISMATCH_TYPE_MATCHED, 1).otherwise(0)).alias("matched_count"),
        F.sum(
            F.when(F.col(MISMATCH_TYPE_COLUMN).isin(MISMATCH_TYPE_MISSING_IN_TARGET, MISMATCH_TYPE_VALUE_DRIFT), 1).otherwise(0)
        ).alias("missing_in_target_count"),
        F.sum(F.when(F.col(MISMATCH_TYPE_COLUMN) == MISMATCH_TYPE_VALUE_DRIFT, 1).otherwise(0)).alias("value_drift_count"),
        F.sum(F.when(F.col(MISMATCH_TYPE_COLUMN) == MISMATCH_TYPE_MISSING_IN_SOURCE, 1).otherwise(0)).alias(
            "missing_in_source_count"
        ),
    )

    missing_or_drifted_keys = deduped.filter(
        F.col(MISMATCH_TYPE_COLUMN).isin(MISMATCH_TYPE_MISSING_IN_TARGET, MISMATCH_TYPE_VALUE_DRIFT)
    ).select(HASH_KEY_COLUMN)
    # left_semi against the ORIGINAL (un-deduped) source_df: every physical source row whose
    # key falls in the miss/drift set is appended, even if that key had more than one source
    # row -- deduplication above is purely a classification-decision safeguard, never a
    # excuse to silently drop genuine duplicate source rows from the append set.
    missing_in_target_df = source_df.join(missing_or_drifted_keys, on=HASH_KEY_COLUMN, how="left_semi").select(
        *append_columns
    )

    mismatch_detail_df = deduped.filter(F.col(MISMATCH_TYPE_COLUMN) != MISMATCH_TYPE_MATCHED)

    return ReconciliationClassification(
        deduped_df=deduped,
        counts_df=counts_df,
        missing_in_target_df=missing_in_target_df,
        mismatch_detail_df=mismatch_detail_df,
    )


def match_reconciliation_target(
    source_df: DataFrame,
    target_df: DataFrame,
    match_keys: List[str],
    compare_columns: Optional[List[str]] = None,
    source_hash_precomputed: bool = False,
) -> ReconciliationMatchResult:
    """Compare one prepared source DataFrame against one prepared target DataFrame.

    Both inputs must already carry ``__framework_hash_key``/``__framework_hash_value`` (call
    :func:`prepare_dataset_for_matching` on each side first) -- this function only joins and
    classifies; it never computes a hash itself, keeping the (potentially wide, multi-column)
    hashing step and the (narrow, single-hash-key) join step independently reusable.

    Parameters
    ----------
    source_df, target_df:
        Prepared DataFrames (see above). Row multiplicity is preserved and safely handled on
        both sides -- see this module's docstring for why duplicate keys on either side cannot
        be pre-filtered away.
    match_keys, compare_columns:
        Same flow-level lists passed to :func:`prepare_dataset_for_matching` for both sides
        (must be identical -- passing a different list here than was used to compute the hash
        columns produces a join/comparison that doesn't correspond to what was actually
        hashed).
    source_hash_precomputed:
        When ``False`` (the common case), ``__framework_hash_key``/``__framework_hash_value``
        were computed by :func:`prepare_dataset_for_matching` purely for this join and are
        stripped from ``missing_in_target_df`` so they never leak into an ``append_target_table``
        that was never designed to carry them. ``__framework_hash_key`` specifically is the
        one exception: it is *always* kept (see :func:`prepare_dataset_for_matching` and
        ``appender.py``'s liquid-clustering note) because it is a pure, deterministic function
        of ``match_keys`` -- exactly the same kind of framework technical column every
        CDC-dispatched target already carries by convention -- so a first-time-created
        ``append_target_table`` has something meaningful to be liquid-clustered on.
        ``__framework_hash_value``, by contrast, is ``compare_columns``-dependent and
        genuinely specific to *this* reconciliation flow's own comparison, so it is dropped
        whenever it wasn't already a real column on the source table.

    Performance note
    ----------------
    ``source_df`` is read twice within this function (once for the narrow join projection,
    once again -- via ``left_semi`` -- to build ``missing_in_target_df`` with its full column
    set), and once more per target when one flow's ``source_config`` is shared across several
    ``target_configs[]`` entries. In the standalone job-task path this cannot be mitigated with
    ``.cache()``/``.persist()`` -- this framework's Lakeflow Jobs run on serverless compute,
    which rejects both outright (see :class:`ReconciliationMatchResult`'s docstring) -- so this
    cost is accepted rather than avoided there: each re-read benefits from Delta's own file
    skipping on ``filter_condition`` and any partition pruning, which keeps it well short of a
    full duplicate table scan in practice. An ``execution_mode: "pipeline"`` flow does not pay
    this cost at all: ``source_df``/``target_df`` are there the already-materialized
    ``_recon__<reconciliation_id>__src``/``__tgt`` Lakeflow datasets (see
    :func:`classify_reconciliation_target`), so every re-read this note describes resolves to a
    read of that one materialized dataset rather than a re-scan of the original physical
    source/target.

    Raises
    ------
    FrameworkConfigError
        If ``match_keys``/``compare_columns`` are missing from either side, or either side is
        missing its hash columns (i.e. :func:`prepare_dataset_for_matching` was skipped).
    """
    compare_columns = compare_columns or []

    classification = classify_reconciliation_target(
        source_df, target_df, match_keys, compare_columns, source_hash_precomputed
    )
    # The one action this split moved OUT of the classification algebra: a lazy counts_df is
    # legal inside a @dlt.table closure reachable from a streaming read (Lakeflow rule 2), but
    # collecting it is not -- so job mode, which is not subject to that rule, still collects it
    # right here, exactly as it always did before classify_reconciliation_target existed.
    counts = classification.counts_df.collect()[0]

    logger.info(
        "Reconciliation match: match_keys=%s, compare_columns=%s -- matched=%d, missing_in_target=%d "
        "(value_drift=%d), missing_in_source=%d",
        match_keys,
        compare_columns,
        counts["matched_count"],
        counts["missing_in_target_count"],
        counts["value_drift_count"],
        counts["missing_in_source_count"],
    )

    return ReconciliationMatchResult(
        deduped_df=classification.deduped_df,
        matched_count=counts["matched_count"],
        missing_in_target_df=classification.missing_in_target_df,
        missing_in_target_count=counts["missing_in_target_count"],
        value_drift_count=counts["value_drift_count"],
        missing_in_source_count=counts["missing_in_source_count"],
        mismatch_detail_df=classification.mismatch_detail_df,
    )
