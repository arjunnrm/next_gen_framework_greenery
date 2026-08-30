"""The framework's ONE canonical hashing standard -- ``__framework_hash_key`` /
``__framework_hash_value``. As of v1.4.0 these are the only two hash columns the framework
produces: ``__framework_surrogate_key`` and the ``crypto/hashing.py`` module that built it were
removed with the rest of the surrogate-key engine (see ``cdc/snapshot.py``).

Every CDC-dispatched flow (SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC) gets two SHA-256 columns
added to its target table, gated by ``target_config.generate_hash_columns`` (default
``True`` for every CDC-dispatched strategy; never added for ``APPEND``/``TRUNCATE_AND_LOAD``,
which have no CDC comparison concept):

* ``__framework_hash_key`` -- hash of the ordered primary key columns. Identifies "the same
  logical row" across datasets without needing every key column individually.
* ``__framework_hash_value`` -- hash of the resolved comparison-column set (see
  ``cdc/comparison_columns.py``). Identifies "has this row changed" in one column.

Reconciliation (``reconciliation/matcher.py``) consumes both directly instead of
recomputing them from raw columns whenever a dataset's ``hash_precomputed`` flag is
``true`` -- this is the entire point: precomputing once, here, at CDC-materialization time,
makes hash-based reconciliation joins cheap at any scale, especially once the target table
is liquid-clustered on ``__framework_hash_key`` (see ``storage/table_properties.py``).

Read ``hash_precomputed`` as an assertion, not an instruction. It does not ask reconciliation to
precompute anything; it declares that THIS module already did, upstream, when the table was
materialized. ``true`` means "trust the two columns already on this dataset"; ``false`` means
"compute them now, from ``match_keys``/``compare_columns``". A ``true`` on a dataset that never
went through a CDC-dispatched framework flow is a ``FrameworkConfigError``, not a silent
recompute -- see ``reconciliation/matcher.py::prepare_dataset_for_matching``.

Exactly one implementation, on purpose
--------------------------------------
Before v1.3.0 this construction existed three times over -- here, in the since-deleted
``crypto/hashing.py::generate_surrogate_key_hash``, and inline inside
``reconciliation/appender.py::compute_batch_fingerprint`` -- and the three copies had already
drifted apart in their exclusion sets. Drift between them is silently wrong rather than loudly
broken: two sides of a reconciliation would disagree about what "the same row" hashes to, and
nothing would report an error. So there is now exactly ONE expression,
:func:`deterministic_hash_expression`, defined here and *imported* by
``reconciliation/appender.py``. **Never rebuild it locally.** This module deliberately imports
nothing from the framework itself (only ``typing`` and ``pyspark``) so that every hashing
consumer can import it without creating a cycle.

The canonical construction
--------------------------
::

    sha2( concat_ws('||', coalesce(trim(lower(cast(c1 as string))), '__NULL__'), ...), 256 )

* **Per-column normalization is ``trim(lower(cast(col as string)))``**, applied identically in
  ingestion, transformation, CDC, and reconciliation. Without it ``"ACME "`` and ``"acme"``
  hash differently, so trivially-equivalent source and target rows were reported as
  ``VALUE_DRIFT`` -- a whole class of false positives that this removes. The flip side is the
  intended semantic and must be understood before relying on anything built on top of it: two
  values differing *only* by letter case or edge whitespace are now the SAME row to this
  framework.
* **The NULL sentinel is the literal ``__NULL__``, coalesced AFTER the normalization.** Both
  halves of that sentence are load-bearing:

  * *Why after:* ``trim(lower(cast(NULL as string)))`` is ``NULL`` in Spark -- null propagates
    through all three functions -- so coalescing afterwards is the only ordering that catches
    NULLs at all. Coalescing first would leave the NULL row's value NULL all the way into
    ``concat_ws``, which drops NULL arguments outright.
  * *Why it cannot collide:* every real value reaching the ``coalesce`` has already been
    lowercased and trimmed, so no real value can ever be uppercase or carry edge whitespace.
    ``__NULL__`` is uppercase, and is therefore unreachable by any real normalized value.
    Coalescing *before* the normalization would instead have produced
    ``trim(lower('__NULL__')) == '__null__'``, which a source value of ``"__NULL__"``,
    ``" __null__ "`` or ``"__Null__"`` collides with exactly. (The pre-v1.3.0 sentinel was
    ``" NULL "`` -- a perfectly legal string value, and therefore collidable by construction.)
    The double-underscore form also matches this framework's own ``__framework_*`` reserved
    namespace convention.
* **The separator is ``||``** via ``concat_ws``, and the digest is ``sha2(..., 256)`` -- a
  64-character lowercase hex string.

Column ordering is the caller's responsibility
----------------------------------------------
:func:`deterministic_hash_expression` hashes its columns in **exactly the order given** and
never sorts. That is deliberate: the framework's three hashes want three different orders, and
a function that quietly sorted would make ``__framework_hash_key`` stop distinguishing
``(customer_id, region)`` from ``(region, customer_id)``.

* ``__framework_hash_key`` -- the caller's ``primary_keys`` order, **not** sorted; the spec
  author's declared key order is the stable order.
* ``__framework_hash_value`` -- ``cdc/comparison_columns.py::resolve_comparison_columns``
  already returns an alphabetically sorted list, so this is deterministic regardless of the
  source DataFrame's physical column ordering without this function sorting anything.
* ``reconciliation/appender.py::compute_batch_fingerprint`` -- the caller's ``match_keys``
  order, then XOR-folded across rows (see :func:`xor_fold_hex_digest`), which is
  order-independent across rows by construction.

Analyst-reproducible Spark SQL
------------------------------
Anything this module computes can be reproduced by hand in a SQL cell, which is the point of
having one written-down construction::

    -- __framework_hash_key for primary_keys = (customer_id, region), in that exact order
    SELECT
      sha2(
        concat_ws('||',
          coalesce(trim(lower(cast(`customer_id` AS STRING))), '__NULL__'),
          coalesce(trim(lower(cast(`region`      AS STRING))), '__NULL__')
        ),
        256
      ) AS __framework_hash_key
    FROM my_catalog.my_schema.my_table;

    -- __framework_hash_value for the resolved comparison columns, ALPHABETICALLY SORTED
    SELECT
      sha2(
        concat_ws('||',
          coalesce(trim(lower(cast(`email`  AS STRING))), '__NULL__'),
          coalesce(trim(lower(cast(`status` AS STRING))), '__NULL__'),
          coalesce(trim(lower(cast(`tier`   AS STRING))), '__NULL__')
        ),
        256
      ) AS __framework_hash_value
    FROM my_catalog.my_schema.my_table;

BREAKING CHANGE (v1.3.0) -- READ BEFORE DEPLOYING
-------------------------------------------------
**Every digest this framework has ever materialized changes value the moment the new wheel is
deployed.** The construction changed (normalization added, sentinel changed, technical-column
exclusion set widened); the column names did not. There is deliberately **no opt-in flag and no
legacy mode** -- a per-flow "old hash" switch would guarantee that two sides of a reconciliation
eventually end up on different constructions, which is precisely the failure mode this
consolidation exists to eliminate. Migration consequences, all one-time:

* **Deployed CDC targets keep their old digests until they are full-refreshed.** A stored
  ``__framework_hash_key`` / ``__framework_hash_value`` is a
  materialized value, not a view expression, so existing rows carry the old digest until the
  row is rewritten.
* **SCD1/SCD2/SCD3:** the first update after the upgrade sees every row as changed (its newly
  computed ``__framework_hash_value`` differs from the stored one) and emits a full set of
  updates. For SCD2 that is one extra version of every row.
* **FULL_SNAPSHOT_CDC_NO_PK:** the diff key itself changes, so the first post-upgrade snapshot
  looks like a complete delete-and-reinsert.
* **Reconciliation with ``hash_precomputed: true``:** the target's stored hashes were computed
  with the old construction while the source is hashed inline with the new one, so *everything*
  reports as drift. Either re-materialize the target through its own pipeline before the first
  reconciliation run, or temporarily set ``hash_precomputed: false`` on that side.
* **``reconciliation_run_log.source_batch_fingerprint``:** historical batch fingerprints change
  once, so the first post-upgrade run cannot short-circuit to ``SKIPPED_ALREADY_PROCESSED``. It
  re-evaluates instead; if the miss set is genuinely empty it appends nothing, so the practical
  cost is one extra full comparison, not duplicate appends.
"""

from typing import Iterable, List, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

HASH_KEY_COLUMN = "__framework_hash_key"
HASH_VALUE_COLUMN = "__framework_hash_value"

#: The one separator between normalized column values inside a framework hash. ``concat_ws``
#: skips NULL arguments entirely, which is a second reason every value is coalesced to
#: :data:`HASH_NULL_SENTINEL` first -- otherwise a NULL column would silently shift every
#: following value one separator to the left and make ``(a, NULL, b)`` hash like ``(a, b)``.
HASH_SEPARATOR = "||"

#: The one NULL sentinel. Applied AFTER ``trim(lower(cast(...)))`` -- see this module's
#: docstring for why that order is the only collision-proof one, and why an uppercase sentinel
#: is unreachable by any real (already lowercased, already trimmed) value.
HASH_NULL_SENTINEL = "__NULL__"

#: Lane layout for :func:`xor_fold_hex_digest`: each tuple is ``(start offset, width)`` into a
#: 64-hex-char ``sha2(..., 256)`` digest, in hex characters. Widths are capped at 15 hex chars
#: (60 bits, not 16 / 64 bits) specifically so ``conv(..., 16, 10)`` never has to represent a
#: value >= 2**63 -- side-stepping any ambiguity in how Spark's ``conv`` treats a hex value
#: whose top bit is set -- while the five lanes together still cover every one of the digest's
#: 64 hex characters exactly once.
#:
#: Moved here from ``reconciliation/appender.py`` in v1.3.0 so ``matcher.py`` and ``appender.py``
#: can both fold digests without importing each other (``appender`` already imports ``matcher``;
#: leaving the fold in ``appender`` would have forced the reverse import as well).
FINGERPRINT_LANES = ((0, 15), (15, 15), (30, 15), (45, 15), (60, 4))

#: Aliases :func:`xor_fold_hex_digest` gives its lane columns, in lane order. Callers normally
#: read the collected aggregate row positionally (``assemble_xor_folded_digest(list(row))``);
#: these names exist so the row can also be read by name, and so the lane columns are visibly
#: framework-generated in any explain plan.
FINGERPRINT_LANE_ALIASES = tuple(f"__framework_fingerprint_lane_{index}" for index in range(len(FINGERPRINT_LANES)))


def normalized_hash_input(column: str) -> Column:
    """One column's canonical, normalized hash input.

    ``coalesce(trim(lower(cast(col as string))), '__NULL__')`` -- the single per-column building
    block every framework hash is assembled from. Exposed (rather than kept private) because
    ``reconciliation`` needs the exact same per-column expression when it hashes one side of a
    comparison inline; re-deriving it there is exactly what let the three pre-v1.3.0 copies of
    this construction drift apart.

    See this module's docstring for why the ``coalesce`` must come *after* the
    ``trim(lower(cast(...)))`` rather than before it -- that ordering is the only collision-proof
    one, and reversing it silently reintroduces a sentinel a real source value can spell.

    Parameters
    ----------
    column:
        A column name on the DataFrame the resulting expression will be evaluated against.

    Returns
    -------
    Column
        The normalized, null-safe string expression for that one column.
    """
    return F.coalesce(F.trim(F.lower(F.col(column).cast("string"))), F.lit(HASH_NULL_SENTINEL))


def deterministic_hash_expression(columns: Sequence[str]) -> Column:
    """The framework's ONE canonical hash expression.

    ``sha2(concat_ws('||', <normalized_hash_input per column>), 256)``.

    Hashes ``columns`` in **exactly the order given** -- it never sorts. Callers that need a
    stable order regardless of the source schema's physical column ordering must sort before
    calling (``cdc/comparison_columns.py::resolve_comparison_columns`` does). See this module's
    docstring for which framework hash uses which ordering, and why.

    Parameters
    ----------
    columns:
        Column names to hash, in the order they should be concatenated.

    Returns
    -------
    Column
        A 64-character lowercase hex SHA-256 digest expression.

    Raises
    ------
    ValueError
        If ``columns`` is empty -- a zero-column hash is a row-independent constant (the SHA-256
        of the empty string), identical for every row, and is never what a caller meant. Failing
        loudly here beats materializing a column that silently identifies nothing.
    """
    column_list = list(columns)
    if not column_list:
        raise ValueError(
            "deterministic_hash_expression requires at least one column: a zero-column hash is a "
            "row-independent constant, never a row identity."
        )
    normalized = [normalized_hash_input(column) for column in column_list]
    return F.sha2(F.concat_ws(HASH_SEPARATOR, *normalized), 256)


def compute_hash_columns(
    df: DataFrame,
    primary_keys: Iterable[str],
    comparison_columns: Iterable[str],
) -> DataFrame:
    """Add ``__framework_hash_key``/``__framework_hash_value`` to ``df``.

    Both are computed with :func:`deterministic_hash_expression`, the framework's single
    canonical construction -- the same one ``reconciliation/appender.py::compute_batch_fingerprint``
    imports rather than rebuilds, so a value hashed on the ingestion side and the same value
    hashed on the reconciliation side are guaranteed to agree.

    Parameters
    ----------
    df:
        Input DataFrame.
    primary_keys:
        Column names hashed into ``__framework_hash_key``, **in the order given** -- the
        caller's declared key order is the stable order (``dq/quarantine.py`` passes
        ``target_config.primary_keys``; ``reconciliation/matcher.py`` passes the dataset's
        configured ``match_keys``).
    comparison_columns:
        Column names hashed into ``__framework_hash_value`` (see
        ``cdc/comparison_columns.py::resolve_comparison_columns``, which returns them sorted).
        An empty list produces a ``NULL`` hash value rather than a hash of nothing -- not an
        error, since some flows genuinely have nothing to compare beyond key presence, and
        ``reconciliation/matcher.py`` already forces ``hash_equal = True`` for that case.

    Returns
    -------
    DataFrame
        ``df`` with both hash columns appended.

    Raises
    ------
    ValueError
        If ``primary_keys`` is empty -- propagated from
        :func:`deterministic_hash_expression`. Callers already gate on this
        (``dq/quarantine.py`` skips hash generation entirely, with a WARNING, when a flow has
        no ``primary_keys``).
    """
    result_df = df.withColumn(HASH_KEY_COLUMN, deterministic_hash_expression(list(primary_keys)))
    comparison_list = list(comparison_columns)
    if comparison_list:
        result_df = result_df.withColumn(HASH_VALUE_COLUMN, deterministic_hash_expression(comparison_list))
    else:
        result_df = result_df.withColumn(HASH_VALUE_COLUMN, F.lit(None).cast("string"))
    return result_df


def xor_fold_hex_digest(digest_column: Column) -> List[Column]:
    """Build the per-lane ``bit_xor`` aggregates that fold arbitrarily many digests into one.

    Folds a column of 64-hex-char ``sha2(..., 256)`` digests -- one per row -- into a single
    fixed-size 64-hex-char digest, entirely on the workers. XOR is commutative and associative,
    so the result does not depend on row or partition order (no ``orderBy`` needed), and
    ``F.bit_xor`` never materializes more than one aggregate row on any executor or the driver
    regardless of how many rows are folded. That bounded-memory property is the whole reason
    this exists: the construction it replaced pulled every distinct key combination into the
    driver's Python heap via ``.distinct().orderBy(...).collect()``, which is a real OOM at
    500K-5M+ keys.

    The digest is split across :data:`FINGERPRINT_LANES` because one lane would have to carry
    256 bits while Spark's ``bit_xor`` aggregates a ``bigint``.

    Usage -- one ``.agg(...)``, then reassemble on the driver::

        lane_row = digests_df.agg(*xor_fold_hex_digest(F.col("_row_hash"))).collect()[0]
        fingerprint = assemble_xor_folded_digest(list(lane_row))

    Parameters
    ----------
    digest_column:
        A column expression yielding one 64-character hex digest per row (i.e. the output of
        :func:`deterministic_hash_expression`).

    Returns
    -------
    list[Column]
        One aggregate expression per lane, aliased per :data:`FINGERPRINT_LANE_ALIASES`, to be
        splatted into a single ``.agg(...)``. Each lane already coalesces an empty aggregate to
        zero, so an input with no rows yields all-zero lanes rather than ``NULL`` ones.
    """
    return [
        F.lower(
            F.lpad(
                F.hex(
                    F.coalesce(
                        F.bit_xor(F.conv(F.substring(digest_column, start + 1, width), 16, 10).cast("bigint")),
                        F.lit(0).cast("bigint"),
                    )
                ),
                width,
                "0",
            )
        ).alias(FINGERPRINT_LANE_ALIASES[index])
        for index, (start, width) in enumerate(FINGERPRINT_LANES)
    ]


def assemble_xor_folded_digest(lane_values: Sequence[Optional[str]]) -> str:
    """Concatenate collected per-lane hex values back into one 64-character digest.

    The driver-side other half of :func:`xor_fold_hex_digest`. Kept separate from the Spark
    expression so the reassembly is a pure, unit-testable string operation with no session
    involved, and so the caller decides when -- and on which collected row -- the fold
    materializes.

    Never raises. A missing, ``None`` or empty lane contributes that lane's width in zeros, and
    an entirely empty input folds to ``"0" * 64`` -- the well-defined "nothing was fingerprinted"
    value ``reconciliation/appender.py`` compares against. A fingerprint that raised or returned
    ``None`` on an empty batch would turn a legitimately empty miss set into a run failure.

    Parameters
    ----------
    lane_values:
        The collected lane values in lane order, e.g. ``list(row)`` from the aggregate row
        produced by :func:`xor_fold_hex_digest`.

    Returns
    -------
    str
        A 64-character lowercase hex digest.
    """
    values = list(lane_values)
    parts = []
    for index, (_, width) in enumerate(FINGERPRINT_LANES):
        value = values[index] if index < len(values) else None
        parts.append(str(value).rjust(width, "0") if value else "0" * width)
    return "".join(parts)
