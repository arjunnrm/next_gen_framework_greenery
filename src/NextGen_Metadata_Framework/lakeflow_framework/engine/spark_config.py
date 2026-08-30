"""Hierarchical Spark session configuration for a Lakeflow Declarative Pipeline update.

Before v1.3.0 this framework set **no** Spark configuration at all: the pipeline notebook read
exactly two ``configuration:`` keys (``dataflow.group.id``, ``dataflow.control.catalog``) and
every tuning knob was whatever the runtime happened to default to. That left an operator with no
metadata-driven way to say "this group shuffles wide, give it different partitioning" short of
hand-editing every pipeline resource, and no way at all to express a default the whole framework
should start from.

Three layers, resolved **lowest -> highest** by :func:`resolve_spark_conf`:

1. :data:`FRAMEWORK_SPARK_DEFAULTS` -- framework built-ins. A starting point, never an opinion
   that should win over a human's.
2. The onboarded, group-scoped ``spark_config`` object, persisted to
   ``dataflow_group_spec.spark_config_json`` and read off the control row by the notebook.
3. The pipeline resource's own ``configuration:`` entry :data:`PIPELINE_SPARK_CONF_KEY`
   (``dataflow.spark.conf``), whose value is a JSON **object encoded as a string** -- the same
   convention ``dataflow.otel_streaming.event_log_tables`` already uses in
   ``resources/observability_otel_streaming_pipeline.yml``, because a bundle ``configuration:``
   block can only carry flat string values.

**Why the bundle YAML is highest, not lowest.** The bundle is the deployment-time, per-target
artifact (dev/staging/prod) an operator edits; it must be able to override metadata that was
onboarded *once* and is shared across every environment, without forcing a re-onboarding cycle.
Onboarded metadata in turn beats the framework built-ins because a spec author knows their
workload better than this package does.

**Why an explicit framework-owned key rather than "whatever the session already has".** An
implicit "only set it if nobody else did" test is not implementable: ``spark.conf.get(
"spark.sql.shuffle.partitions")`` returns Spark's own default (``200``) whether or not a human
ever set it, so "already set" and "never set" are indistinguishable at read time. Reading one
dedicated key that only this framework writes is what makes the precedence decidable at all.

**Nothing in this module may ever fail a pipeline update.** Every failure mode -- a malformed
``dataflow.spark.conf`` JSON string, a key Spark refuses because it is a static SQL configuration
(``AnalysisException: Cannot modify the value of a static config``), a key that does not exist on
this runtime -- is logged at WARNING and skipped. This is tuning metadata; losing a shuffle-
partition hint is a performance regression, while raising would take down an otherwise-correct
ingestion graph.

The resolver (:func:`resolve_spark_conf`) is deliberately **pure** -- no ``SparkSession``, no
I/O -- so the precedence chain is unit-testable with plain dicts and no Spark fixture, and the
thin applier (:func:`apply_spark_conf`) is the only thing that touches a live session.
"""

import json
import logging
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.engine.spark_config")

#: The pipeline ``configuration:`` key holding a JSON object of Spark settings -- the HIGHEST
#: precedence layer. Same JSON-encoded-string convention as
#: ``dataflow.otel_streaming.event_log_tables`` (see
#: ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py``): a bundle
#: ``configuration:`` block can only hold flat strings, so a nested mapping has to arrive as
#: JSON text and be decoded here.
PIPELINE_SPARK_CONF_KEY = "dataflow.spark.conf"

#: Framework built-in defaults -- the LOWEST precedence layer. Anything listed here is a starting
#: point that a group's onboarded ``spark_config``, or the pipeline resource itself, is *expected*
#: to override. ``spark.sql.shuffle.partitions = "200"`` is Spark's own default restated
#: explicitly: stating it costs nothing behaviourally, but it makes the value show up in the
#: applied-configuration log, which is what turns "why is this pipeline shuffling into 200 files"
#: into a one-line answer instead of a runtime archaeology exercise.
FRAMEWORK_SPARK_DEFAULTS: Dict[str, str] = {
    "spark.sql.shuffle.partitions": "200",
}


def coerce_spark_conf_value(value: Any) -> str:
    """Render one configuration value the way ``spark.conf.set`` expects it.

    ``str(value)`` for everything, **except** Python booleans, which become lowercase
    ``"true"``/``"false"`` -- Spark's own spelling. This special case is not cosmetic:
    ``str(True)`` produces ``"True"``, which Spark's boolean configuration parser rejects for
    several keys (it compares against the lowercase literals), so a spec author writing the
    natural JSON ``true`` would otherwise get a silently-unset or outright-failing key. JSON
    numbers therefore render as ``"200"``, not ``200`` -- ``spark.conf.set`` wants strings.

    The ``bool`` check must come first: in Python ``bool`` is a subclass of ``int``, so an
    ``isinstance(value, int)`` branch placed above it would swallow ``True``/``False``.

    Parameters
    ----------
    value:
        A raw JSON-decoded configuration value (string, number, or boolean).

    Returns
    -------
    str
        The string form ``spark.conf.set`` accepts.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _merge_conf_layer(merged: Dict[str, str], layer: Optional[Dict[str, Any]], layer_label: str) -> None:
    """Fold one precedence layer into ``merged`` in place, coercing every value.

    This is a **merge, never a replace**: a layer that does not mention a key leaves the lower
    layer's value intact, which is the whole point of a hierarchy (a pipeline resource that wants
    to retune one key must not have to restate every other key the group onboarded).

    Non-mapping layers and non-string keys are skipped with a WARNING rather than raised on --
    ``onboarding/spec_validator.py::_validate_spark_config`` already rejects both shapes at
    onboarding time, so reaching here means a hand-edited control-table row, and this module's
    contract is that no tuning metadata can fail a pipeline update.
    """
    if layer is None:
        return
    if not isinstance(layer, dict):
        logger.warning(
            "Ignoring the %s Spark configuration layer: expected a JSON object of {key: value} pairs, got %r.",
            layer_label,
            layer,
        )
        return
    for key, value in layer.items():
        if not isinstance(key, str) or not key:
            logger.warning(
                "Ignoring a %s Spark configuration entry with a non-string/empty key %r "
                "(keys must be Spark configuration names such as 'spark.sql.shuffle.partitions').",
                layer_label,
                key,
            )
            continue
        merged[key] = coerce_spark_conf_value(value)


def resolve_spark_conf(
    framework_defaults: Optional[Dict[str, Any]] = None,
    spec_spark_config: Optional[Dict[str, Any]] = None,
    pipeline_spark_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Merge the three precedence layers into one flat ``{key: str-value}`` mapping.

    PURE -- no ``SparkSession``, no I/O -- so the precedence chain can be unit-tested with plain
    dicts and no Spark fixture. That separation is deliberate: the ordering rule is the part most
    likely to be got wrong or silently regressed, and it is exactly the part a Spark-free test
    can pin down.

    Later layers strictly override earlier ones, key by key; a layer that does not mention a key
    leaves the lower layer's value intact (a merge, never a replace). Every surviving value is
    passed through :func:`coerce_spark_conf_value`.

    Parameters
    ----------
    framework_defaults:
        The lowest layer. ``None`` (the default) means :data:`FRAMEWORK_SPARK_DEFAULTS`; pass an
        explicit ``{}`` to resolve against no built-ins at all (tests do this to assert a layer
        in isolation). Note the distinction: ``None`` is "use the framework's defaults", ``{}``
        is "there are no defaults".
    spec_spark_config:
        The middle layer -- the group's onboarded ``spark_config`` object, JSON-decoded from
        ``dataflow_group_spec.spark_config_json``.
    pipeline_spark_config:
        The highest layer -- the JSON object decoded out of the pipeline resource's own
        ``dataflow.spark.conf`` entry, normally via :func:`read_pipeline_spark_config`.

    Returns
    -------
    Dict[str, str]
        The resolved configuration, values already rendered as strings. The canonical worked
        example: ``FRAMEWORK_SPARK_DEFAULTS`` carries
        ``{"spark.sql.shuffle.partitions": "200"}``; a pipeline resource carrying
        ``dataflow.spark.conf: '{"spark.sql.shuffle.partitions": "auto"}'`` resolves to
        ``"auto"``.
    """
    merged: Dict[str, str] = {}
    _merge_conf_layer(
        merged,
        FRAMEWORK_SPARK_DEFAULTS if framework_defaults is None else framework_defaults,
        "framework-default",
    )
    _merge_conf_layer(merged, spec_spark_config, "onboarded spark_config")
    _merge_conf_layer(merged, pipeline_spark_config, f"pipeline '{PIPELINE_SPARK_CONF_KEY}'")
    return merged


def read_pipeline_spark_config(spark: SparkSession) -> Dict[str, Any]:
    """Read and JSON-decode the pipeline resource's own :data:`PIPELINE_SPARK_CONF_KEY` entry.

    Returns ``{}`` when the key is absent, empty, not valid JSON, or does not decode to a JSON
    *object* -- **never raises**. A malformed value here is a deployment typo inside a tuning
    block; failing an entire pipeline update over one is a strictly worse outcome than running
    with the two lower-precedence layers and a loud WARNING that names the problem.

    The ``spark.conf.get(key, None)`` two-argument form is used rather than a bare ``get``
    because an unset key raises on some runtimes instead of returning ``None``; the surrounding
    ``try`` covers the runtimes where even that form raises.

    Parameters
    ----------
    spark:
        Active ``SparkSession`` (the pipeline's own).

    Returns
    -------
    Dict[str, Any]
        The decoded JSON object, or ``{}``. Values are *not* coerced here -- that happens in
        :func:`resolve_spark_conf`, so the raw JSON types stay visible to the resolver's own
        coercion rules (notably booleans).
    """
    try:
        raw = spark.conf.get(PIPELINE_SPARK_CONF_KEY, None)
    except Exception:  # noqa: BLE001 - key genuinely absent in this runtime/context
        return {}
    if not raw:
        return {}

    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError) as exc:
        logger.warning(
            "Pipeline configuration 'dataflow.spark.conf' is not a valid JSON object (%s) -- ignoring it "
            "and falling back to the onboarded spark_config and the framework defaults.",
            exc,
        )
        return {}

    if not isinstance(decoded, dict):
        logger.warning(
            "Pipeline configuration 'dataflow.spark.conf' is not a valid JSON object (%s) -- ignoring it "
            "and falling back to the onboarded spark_config and the framework defaults.",
            f"decoded to a JSON {type(decoded).__name__}, not an object",
        )
        return {}
    return decoded


def apply_spark_conf(spark: SparkSession, resolved_conf: Dict[str, str]) -> Dict[str, str]:
    """Thin applier: ``spark.conf.set`` each resolved key, returning the subset actually applied.

    A key Spark refuses -- a **static** SQL configuration
    (``AnalysisException: Cannot modify the value of a static config``), or a key that simply
    does not exist on this runtime -- is logged at WARNING and skipped, never raised. Tuning
    metadata must never be able to fail a pipeline update, and the static-config case is not even
    a user error: a perfectly reasonable key can be settable on one runtime and frozen at session
    start on the next.

    Keys are applied in sorted order purely so the WARNING sequence and the returned mapping are
    deterministic across updates -- an applied-configuration log that reorders itself run to run
    is far harder to diff when diagnosing a performance change.

    Parameters
    ----------
    spark:
        Active ``SparkSession`` (the pipeline's own). Configuration is applied to the session
        **before any flow is registered**, so every ``@dlt.table``/``@dlt.view`` closure the
        notebook goes on to define executes under it.
    resolved_conf:
        The output of :func:`resolve_spark_conf` -- values already rendered as strings.

    Returns
    -------
    Dict[str, str]
        Only the keys Spark accepted. The caller logs the summary (it is the one place a
        ``dataflow_group_id`` is in scope); returning the applied subset rather than the
        requested one keeps that log honest about what actually took effect.
    """
    applied: Dict[str, str] = {}
    for key, value in sorted(resolved_conf.items()):
        try:
            spark.conf.set(key, value)
        except Exception as exc:  # noqa: BLE001 - static/unknown conf keys must never fail an update
            logger.warning(
                "Could not apply Spark configuration '%s' = %r: %s -- this is typically a static SQL "
                "configuration that cannot be changed after the session starts. Continuing without it.",
                key,
                value,
                exc,
            )
            continue
        applied[key] = value
    return applied
