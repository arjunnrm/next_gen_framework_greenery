"""Shared Lakeflow dataset/sink identifier helpers.

**Why this module exists.** Two independent parts of the framework need to turn an arbitrary
configured name (a fully-qualified table name, a ``${param}``-substituted path, a control-table
``destination_id``) into a valid, collision-safe Lakeflow dataset/sink identifier fragment:

* ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py`` -- registers
  one ``@dlt.view``/sink per configured event-log table / continuous destination.
* the pipeline-mode "source plane" (Phase 8+) -- registers one materialized node per shared
  external read locator, keyed by :func:`stable_node_name`.

:func:`sanitize_identifier` is lifted verbatim from notebook 06's former private
``_sanitize_identifier`` (same regex, same behaviour: every character outside
``[0-9a-zA-Z_]`` becomes ``_``). Notebook 06 now imports it from here instead of defining its
own copy, so its behaviour is byte-identical to before this module existed.

**Why :func:`stable_node_name` always appends a hash, not only on truncation.** Sanitizing
collapses punctuation to ``_``, which is lossy: ``"metaflow.bronze.a_b"`` and
``"metaflow.bronze_a.b"`` both sanitize to ``"metaflow_bronze_a_b"``. If the hash suffix were
only added when ``locator`` exceeds ``max_core``, these two distinct locators would register
the same short name and Lakeflow would fail the whole update with "Cannot redefine dataset".
Appending ``sha256(locator)[:8]`` unconditionally makes every name collision-safe regardless of
length, while the human-readable sanitized prefix (truncated to ``max_core`` characters) keeps
the identifier legible for debugging.
"""

import hashlib
import re

_NON_IDENTIFIER_CHARS = re.compile(r"[^0-9a-zA-Z_]")


def sanitize_identifier(value: str) -> str:
    """Turn an arbitrary configured name into a valid Lakeflow dataset/sink identifier fragment.

    Used for both a fully-qualified table name (dots, and potentially other punctuation), e.g.
    'observability.event_logs.pipeline_1' -> 'observability_event_logs_pipeline_1', and for a
    control-table `destination_id` (which an onboarding spec author is free to write with
    hyphens, e.g. 'dest-continuous-volume') when naming that destination's Volume sink. Both
    need the same treatment for the same reason: Lakeflow identifiers are Spark identifiers,
    and an unsanitized dot or hyphen in one either fails graph resolution or silently reads as a
    qualified name.
    """
    return _NON_IDENTIFIER_CHARS.sub("_", value)


def stable_node_name(prefix: str, locator: str, suffix: str, max_core: int = 80) -> str:
    """Build a collision-safe Lakeflow dataset name fragment for a shared source-plane node.

    Returns ``f"{prefix}__{sanitize_identifier(locator)[:max_core]}__{digest}__{suffix}"``,
    where ``digest`` is the first 8 hex characters of ``sha256(locator)``. The digest is
    **always** appended, never only when ``locator`` is longer than ``max_core`` -- see the
    module docstring for why omitting it on the non-truncated path is unsafe (two distinct
    locators can sanitize to the identical string).

    ``locator`` should be the same canonical, casefolded string used as the source-plane
    identity key (see ``ReadIdentity`` in ``engine/source_plane.py``), so that the same external
    read always produces the same node name regardless of which consumer triggers registration
    first.
    """
    sanitized_core = sanitize_identifier(locator)[:max_core]
    digest = hashlib.sha256(locator.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}__{sanitized_core}__{digest}__{suffix}"
