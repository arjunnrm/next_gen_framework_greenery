"""Opt-in Bronze/raw column-name normalization: trim, case-fold, replace whitespace and
special characters with underscores.

Off by default (``source_config.column_normalization.enabled`` must be explicitly set to
``true``) so onboarding an existing flow never silently renames its columns underneath
already-written ``data_standardization_sql``/``explode_columns``/``dq_config.rules[].expression``
references.

**Ordering matters.** When enabled, this must run immediately after the raw source read --
before :mod:`ingestion.schema_config`'s renames (if also configured), before
``explode_columns``, before ``data_standardization_sql``, before technical-metadata attachment
-- so every one of those config fields is written against the *normalized* column names, not
the source's raw ones. See ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` for the
exact call order.

**Configurable case, non-configurable everything else.** ``column_normalization.case`` selects
one of ``"lower"`` (the default and the pre-v1.3.0 behaviour), ``"preserve"``, or ``"upper"``.
Only the *case-fold* step is configurable; the character normalization -- trim, replace every
character outside ``[A-Za-z0-9_]`` with ``_``, collapse runs of ``_``, strip edge ``_``, fall
back to ``"_"`` -- is applied unconditionally and identically for all three, so a given source
name always produces the same target name apart from its letters' casing. Two guarantees follow
from that split and neither is negotiable:

1. The invalid-character class is **case-independent** (``[^A-Za-z0-9_]``). It was
   ``[^a-z0-9_]`` while this module always lowercased first, which was safe only because of that
   ordering; under ``preserve``/``upper`` a lowercase-only class would shred every capital letter
   into an underscore (``"CustomerID"`` -> ``"_"``). For ``case="lower"`` the widening is a
   bit-for-bit no-op, since the substitution still runs over an already-lowercased string.
2. **Collision detection is always performed on the lowercased projection** of the produced
   names, whatever ``case`` was requested. Unity Catalog and Spark resolve column references
   case-insensitively (``spark.sql.caseSensitive`` is ``false`` by default), so two output names
   differing only by case are an unusable table rather than a valid one. Folding for the
   duplicate check only is what keeps ``preserve``/``upper`` deterministic *and* safe: whatever
   casing reaches the target table, the framework has already proven the deterministic lowercase
   projection of those names is unique.

``column_normalization`` is the ONLY switch (v1.4.0)
---------------------------------------------------
The legacy boolean ``source_config.normalize_column_names`` is **removed**. It was a second way
to say the one thing ``column_normalization.enabled`` already says, and carrying both meant this
module owned a three-level precedence ladder, a present-vs-truthy distinction on ``enabled``, and
a contradiction warning -- roughly forty lines of adjudication whose only job was to decide which
of two synonyms won. ``column_normalization.enabled`` now answers that question on its own, with
``.get("enabled", False)``: absent means off, exactly as it reads.

**BREAKING.** A spec that enabled normalization *only* through ``normalize_column_names: true``
no longer normalizes anything. ``onboarding/spec_validator.py`` rejects the key outright rather
than ignoring it, so this surfaces at onboarding time with a migration message instead of as
silently un-renamed columns at the next pipeline run. Migration is mechanical:
``{"normalize_column_names": true}`` becomes ``{"column_normalization": {"enabled": true}}``,
and a spec that already carried the object needs no change at all -- ``case`` keeps its
``"lower"`` default, so the resolved ``(enabled, case)`` is bit-identical.
"""

import logging
import re
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame

from flowx.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("flowx.lakeflow_framework.ingestion.column_normalization")

DEFAULT_NORMALIZATION_CASE = "lower"
ALLOWED_NORMALIZATION_CASES = ("lower", "preserve", "upper")

# Case-independent by design -- see guarantee (1) in this module's docstring. Widening this from
# [^a-z0-9_] is what makes case="preserve"/"upper" possible at all, and is a no-op for "lower".
_INVALID_CHARS_PATTERN = re.compile(r"[^A-Za-z0-9_]")
_REPEATED_UNDERSCORES_PATTERN = re.compile(r"_+")


def resolve_column_normalization(source_config: Dict[str, Any]) -> Tuple[bool, str]:
    """Resolve ``(enabled, case)`` for one ingestion flow from its raw ``source_config``.

    One switch, read plainly:

    * ``enabled`` -- ``column_normalization.enabled``, defaulting to ``False``. Absent object,
      absent key and explicit ``false`` are all "off", because they all mean the same thing now
      that there is no second declaration to adjudicate against. (Before v1.4.0 this had to be a
      ``"enabled" in ...`` presence test, so that an object supplying only ``case`` could leave
      enablement to the legacy ``normalize_column_names`` boolean. That boolean is gone, so the
      distinction it existed to preserve is gone with it.)
    * ``case`` -- ``column_normalization.case``, defaulting to ``"lower"``, the pre-v1.3.0
      behaviour. Validated by :func:`normalize_column_name`, not here.

    A ``column_normalization`` value that is not a dict at all resolves to ``(False, "lower")``
    rather than raising. The onboarding validator already rejects that shape, so reaching it means
    a hand-edited control-table row; for a *renaming* step, declining to rename is the
    conservative outcome, and a hard failure here would take down a pipeline mid-run over a
    malformed optional field.

    Pure (no Spark), so the resolution is unit-testable against a plain dict.
    """
    column_normalization = source_config.get("column_normalization")
    if not isinstance(column_normalization, dict):
        return False, DEFAULT_NORMALIZATION_CASE

    enabled = bool(column_normalization.get("enabled", False))
    case = column_normalization.get("case") or DEFAULT_NORMALIZATION_CASE
    return enabled, case


def normalize_column_name(name: str, case: str = DEFAULT_NORMALIZATION_CASE) -> str:
    """Trim, case-fold per ``case``, and replace whitespace/special characters with a single
    underscore.

    ``"  Customer ID#1  "`` -> ``"customer_id_1"``; leading/trailing underscores produced by
    a leading/trailing special character are stripped, and a name that normalizes to nothing
    at all (e.g. ``"###"``) falls back to a literal ``"_"`` rather than an empty string, which
    Spark rejects as a column name outright.

    Every step except the case fold is unconditional, so the output is deterministic and
    independent of the source name's own casing -- see this module's docstring.

    Parameters
    ----------
    name:
        The raw source column name.
    case:
        One of :data:`ALLOWED_NORMALIZATION_CASES`. ``"lower"`` lowercases, ``"upper"``
        uppercases, ``"preserve"`` leaves the letters exactly as the source spelled them. This is
        a **new trailing keyword argument** whose default reproduces the previous behaviour
        exactly, so every existing single-argument call -- including
        ``tests/unit/test_column_normalization.py``'s -- keeps its exact previous output.

    Raises
    ------
    FrameworkConfigError
        If ``case`` is not one of :data:`ALLOWED_NORMALIZATION_CASES`. Silently falling back to
        ``"lower"`` on a typo would rename every column in a way the spec did not ask for, and
        the resulting table would look correct enough that nobody would go looking for the typo.
    """
    if case not in ALLOWED_NORMALIZATION_CASES:
        raise FrameworkConfigError(
            f"column_normalization.case '{case}' is not supported -- expected one of "
            f"{', '.join(ALLOWED_NORMALIZATION_CASES)}."
        )

    normalized = name.strip()
    if case == "lower":
        normalized = normalized.lower()
    elif case == "upper":
        normalized = normalized.upper()
    normalized = _INVALID_CHARS_PATTERN.sub("_", normalized)
    normalized = _REPEATED_UNDERSCORES_PATTERN.sub("_", normalized)
    normalized = normalized.strip("_")
    return normalized or "_"


def normalize_column_names(df: DataFrame, source_config: Dict[str, Any]) -> DataFrame:
    """Rename every column on ``df`` per :func:`normalize_column_name`, when column normalization
    is enabled for this flow (default off -- a no-op otherwise, returning ``df`` unchanged).

    Enablement *and* the case fold are resolved by :func:`resolve_column_normalization` from
    ``source_config.column_normalization``, the only switch as of v1.4.0. The signature is
    unchanged: the whole resolution happens from the ``source_config`` this already receives, so
    no call site moves.

    Collision detection runs over the **lowercased projection** of the produced names regardless
    of the requested ``case`` -- see guarantee (2) in this module's docstring. For
    ``case="lower"`` the comparison key and the produced name are the same string, which is why
    this is a no-op for every pre-v1.3.0 spec; for ``preserve``/``upper`` it is what stops the
    framework from producing a pair like ``"Order_ID"``/``"order_id"`` that Unity Catalog cannot
    tell apart. The reported message still names the *produced* spelling, not the comparison key,
    so the operator sees what would actually have been written to the table.

    Raises
    ------
    FrameworkConfigError
        If two or more source columns normalize to the same target name (e.g. ``"Customer ID"``
        and ``"customer_id"`` both becoming ``"customer_id"``) -- silently dropping one of them
        via an implicit overwrite would be a silent data-loss bug, so this is a hard error
        naming the exact colliding source columns; rename one of them at the source, or use
        :mod:`ingestion.schema_config` for explicit, collision-free per-column renaming instead.
        Also raised, via :func:`normalize_column_name`, for an unsupported ``case``.
    """
    enabled, case = resolve_column_normalization(source_config)
    if not enabled:
        return df

    rename_map = {original: normalize_column_name(original, case) for original in df.columns}

    # `seen` is keyed on the lowercased comparison key -- which is what catches `preserve`/`upper`
    # collisions -- but stores the *source* column name, so the message can name both raw columns
    # the operator has to go rename; the produced spelling that would actually have landed in the
    # table is reported separately at the end of each entry.
    seen: Dict[str, str] = {}
    collisions: List[str] = []
    for original, normalized in rename_map.items():
        comparison_key = normalized.lower()
        if comparison_key in seen:
            collisions.append(f"'{seen[comparison_key]}' and '{original}' both normalize to '{normalized}'")
        else:
            seen[comparison_key] = original
    if collisions:
        raise FrameworkConfigError(
            "column_normalization: normalization produced duplicate column names -- "
            + "; ".join(collisions)
            + ". Rename one of the source columns, or use schema_config_path for explicit renaming instead."
        )

    # Single vectorized select (one Project node) instead of one withColumnRenamed() per
    # column -- with 50-100+ columns, a rename-per-call loop stacks that many separate
    # Project/Alias nodes and materially bloats Catalyst plan-compile time. ``rename_map``
    # is built from ``df.columns`` above, so iterating ``df.columns`` here (rather than
    # ``rename_map`` directly) both preserves the original column order and is guaranteed
    # to hit every key. ``df[original]`` (not ``F.col(original)``) resolves by the column's
    # exact literal name -- matching ``withColumnRenamed``'s semantics -- since ``F.col``
    # would otherwise parse a raw source column name containing a ``.`` as nested-field
    # access instead of a literal identifier.
    result_df = df.select(*[df[original].alias(rename_map[original]) for original in df.columns])
    changed = {original: normalized for original, normalized in rename_map.items() if original != normalized}

    if changed:
        logger.info(
            "column_normalization (case=%s): renamed %d column(s): %s", case, len(changed), changed
        )
    return result_df
