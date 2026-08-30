"""Opt-in stream-level **full-row** deduplication for ingestion flows
(``source_config.remove_dups``).

Off by default. When enabled, every column that can legitimately define a row's identity is used
as the dedup subset -- which is deliberately *not* the same as "every column on the DataFrame".
The ``__framework_*`` technical columns are per-run/per-file audit metadata
(``__framework_source_file_name``, ``__framework_ingestion_timestamp_utc``,
``__framework_source_file_size``, ...), so the same logical row redelivered in a second file, or
re-read on a later micro-batch, carries *different* values in them. Including those columns in
the subset would make every row trivially unique and defeat full-row dedup entirely -- the
feature would silently do nothing, which is far worse than not offering it. The same applies to
the non-``__framework_``-prefixed technical columns Auto Loader and the ASN.1 decoder attach
(:data:`DEDUP_EXCLUDED_COLUMNS`).

**Ordering matters.** This runs *after* ``explode_columns``/auto-flatten and *before*
``data_standardization_sql``. After explode because a source row delivered twice becomes 2xM rows
once an array is ``explode_outer``ed, and only a post-explode dedup collapses that correctly; a
pre-explode dedup would also miss duplicates that differ only inside the nesting being flattened
away. Before standardization because standardization expressions should be evaluated once per
*surviving* row rather than once per duplicate -- and because a standardization expression
deriving a value from ``current_timestamp()`` or any other non-deterministic function would
otherwise make every duplicate look distinct and defeat the dedup entirely. See
``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` for the exact call order.

**Operational caveat -- unbounded streaming state.** ``dropDuplicates`` on a streaming DataFrame
keeps **unbounded** state -- every distinct row key seen since the stream started is retained in
the state store forever. On a long-running streaming table this grows without limit and will
eventually degrade or fail the pipeline. Configure ``dedup_watermark`` for any stream expected to
run continuously; the same warning is emitted at runtime (once per flow) whenever dedup runs on a
streaming DataFrame with no watermark configured, so it is visible in the pipeline event log and
not only here.

``dropDuplicates`` is nevertheless the **default** operator, in preference to
``dropDuplicatesWithinWatermark``: full-row dedup must be *exact* by default. Bounding state by a
watermark trades exactness -- a late-arriving duplicate falling outside the watermark survives --
for bounded memory, and that trade has to be an explicit operator decision (by configuring
``dedup_watermark``), never an implicit one the framework makes on the operator's behalf.

**Which row survives is arbitrary.** Spark gives no guarantee about *which* member of a duplicate
group is kept, so the surviving row's ``__framework_source_file_name`` /
``__framework_ingestion_timestamp_utc`` are an arbitrary pick from among the duplicates. This is
documented and accepted: those columns are excluded from row identity precisely because they are
not part of it, and a flow that needs a deterministic "keep the earliest/latest" rule wants an
SCD strategy or a windowed ``transformation_sql``, not full-row dedup.
"""

import logging
from typing import Any, Dict, List, Sequence, Tuple

from pyspark.sql import DataFrame

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.ingestion.dedup")

#: Every framework-generated column carries this prefix (double underscore -- see the framework
#: column conventions). Matching on the prefix rather than an explicit list means a technical
#: column added in a future release is excluded from row identity automatically, instead of
#: silently starting to defeat dedup on every flow that had enabled it.
FRAMEWORK_COLUMN_PREFIX = "__framework_"

#: Non-``__framework_``-prefixed technical columns that must never define row identity. These are
#: native Auto Loader / ASN.1 columns the framework does not own the naming of (which is why they
#: carry a single underscore and cannot be caught by the prefix rule above): ``_rescued_data``
#: holds whatever did not fit the read schema, ``_metadata``/``_object_metadata`` are per-file
#: hidden pseudo-columns, and ``_asn1_decode_error`` is a per-row decode diagnostic.
DEDUP_EXCLUDED_COLUMNS = ("_rescued_data", "_metadata", "_object_metadata", "_asn1_decode_error")


def resolve_dedup_columns(columns: Sequence[str]) -> List[str]:
    """The full-row dedup subset for ``columns``: everything except the technical columns.

    Excludes any name starting with :data:`FRAMEWORK_COLUMN_PREFIX` and any name in
    :data:`DEDUP_EXCLUDED_COLUMNS`; see this module's docstring for why those columns cannot
    participate in row identity.

    Input order is preserved, so the subset handed to ``dropDuplicates`` mirrors the DataFrame's
    own column order and the resulting plan is stable across runs of the same flow.

    Pure -- it takes and returns plain names and needs no Spark session, so the exclusion rules
    are unit-testable on their own.
    """
    return [
        column
        for column in columns
        if not column.startswith(FRAMEWORK_COLUMN_PREFIX) and column not in DEDUP_EXCLUDED_COLUMNS
    ]


def apply_stream_dedup(df: DataFrame, source_config: Dict[str, Any]) -> DataFrame:
    """Full-row deduplication of ``df``, gated by ``source_config.remove_dups``.

    Default ``False`` -- a no-op otherwise, returning ``df`` unchanged. This runs on every
    ingestion flow, so a flow that has not opted in must be byte-for-byte unaffected, right down
    to not adding a node to the plan.

    When enabled, the subset is :func:`resolve_dedup_columns` over ``df.columns`` *at this point
    in the chain* (i.e. post-explode), and the operator is:

    * ``df.dropDuplicates(subset)`` -- the default, exact everywhere, but **unbounded** in
      streaming state (see this module's docstring; a ``WARNING`` naming the fix is logged once
      per flow whenever this combination occurs on a streaming DataFrame);
    * ``df.withWatermark(event_time_column, delay_threshold).dropDuplicatesWithinWatermark(subset)``
      -- when ``source_config.dedup_watermark`` is configured **and** ``df`` is streaming.

    ``dedup_watermark`` is ignored (at ``INFO``) on a non-streaming DataFrame: there is no state
    store to bound there, ``dropDuplicates`` is a plain shuffle, and applying a watermark would
    only introduce the late-data exactness trade-off with none of the memory benefit that
    justifies it.

    Parameters
    ----------
    df:
        The staged ingestion DataFrame, after explode/auto-flatten.
    source_config:
        The flow's raw ``source_config``; ``remove_dups`` (bool) and the optional
        ``dedup_watermark`` object (``{"event_time_column": ..., "delay_threshold": ...}``) are
        read from it.

    Raises
    ------
    FrameworkConfigError
        If every column is excluded (nothing is left to define row identity), if
        ``dedup_watermark`` is present but malformed, or if its ``event_time_column`` is not a
        column on ``df``.
    """
    if not source_config.get("remove_dups", False):
        return df

    subset = resolve_dedup_columns(df.columns)
    if not subset:
        raise FrameworkConfigError(
            "remove_dups: no columns remain to deduplicate on after excluding framework technical columns -- "
            "this DataFrame carries nothing but __framework_*/_rescued_data columns, which cannot define row "
            "identity"
        )

    dedup_watermark = source_config.get("dedup_watermark")
    if dedup_watermark and df.isStreaming:
        event_time_column, delay_threshold = _resolve_dedup_watermark(dedup_watermark)
        if event_time_column not in df.columns:
            raise FrameworkConfigError(
                f"dedup_watermark.event_time_column '{event_time_column}' is not a column on this DataFrame "
                f"(columns: {df.columns})"
            )
        logger.info(
            "remove_dups: watermarked full-row dedup on %d column(s), bounded by %s <= %s.",
            len(subset),
            event_time_column,
            delay_threshold,
        )
        return df.withWatermark(event_time_column, delay_threshold).dropDuplicatesWithinWatermark(subset)

    if dedup_watermark:
        logger.info(
            "remove_dups: dedup_watermark is configured but this flow's DataFrame is not streaming -- ignoring "
            "it and using an exact dropDuplicates (a batch shuffle keeps no state store to bound)."
        )
    elif df.isStreaming:
        logger.warning(
            "remove_dups is enabled on a STREAMING DataFrame with no dedup_watermark -- dropDuplicates will keep "
            "UNBOUNDED state (every distinct row seen since the stream started is retained forever). Configure "
            "source_config.dedup_watermark for any continuously-running stream."
        )

    logger.info("remove_dups: exact full-row dedup on %d column(s).", len(subset))
    return df.dropDuplicates(subset)


def _resolve_dedup_watermark(dedup_watermark: Any) -> Tuple[str, str]:
    """Pull ``(event_time_column, delay_threshold)`` out of a ``dedup_watermark`` object.

    The onboarding validator already requires both keys, so a malformed object here means a
    hand-edited control-table row. It raises rather than quietly falling back to the unwatermarked
    path: that fallback would hand the operator exactly the unbounded-state behaviour they had
    configured ``dedup_watermark`` to avoid, and it would do so silently.
    """
    if not isinstance(dedup_watermark, dict):
        raise FrameworkConfigError(
            f"source_config.dedup_watermark: expected an object with 'event_time_column' and 'delay_threshold', "
            f"got {dedup_watermark!r}"
        )
    event_time_column = dedup_watermark.get("event_time_column")
    delay_threshold = dedup_watermark.get("delay_threshold")
    if not isinstance(event_time_column, str) or not event_time_column:
        raise FrameworkConfigError(
            f"source_config.dedup_watermark.event_time_column: expected a non-empty string, got "
            f"{event_time_column!r}"
        )
    if not isinstance(delay_threshold, str) or not delay_threshold:
        raise FrameworkConfigError(
            f"source_config.dedup_watermark.delay_threshold: expected a non-empty Spark interval string "
            f"(e.g. '2 hours'), got {delay_threshold!r}"
        )
    return event_time_column, delay_threshold
