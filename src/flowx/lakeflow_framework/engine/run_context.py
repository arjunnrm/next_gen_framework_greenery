"""Best-effort pipeline/update run-identifier resolution, for quarantine-row traceability.

Called once from ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``, right after
``control_plane/repository.py::load_active_group_metadata`` resolves the active group, and the
result is threaded through to ``dq/quarantine.py::add_quarantine_columns`` as
``pipeline_run_id`` -- stamped onto every row's ``__framework_pipeline_run_id`` column so a quarantined
record can be traced back to the specific pipeline update that produced it.

Lakeflow does not expose one single, documented, version-stable Spark conf key for "the current
pipeline update ID" -- the candidate keys in ``_CANDIDATE_SPARK_CONF_KEYS`` are what's been
observed to work across runtime versions, tried in order. Never raises: an absent/renamed conf
key on some future runtime falls back to ``group_id`` (always available, always deterministic)
rather than failing the whole pipeline update over metadata that only ever helps post-hoc
diagnosis.

Ordering note (v1.3.0): this runs *before* ``engine/spark_config.py::apply_spark_conf`` sets the
group's hierarchical Spark configuration, and deliberately so. The keys read here are
**platform-provided** identifiers Lakeflow/Jobs put on the session, not framework tuning keys, so
reading them first keeps the two concerns cleanly separated: nothing this framework applies can
influence, mask, or be mistaken for the resolved run identifier -- and a mis-typed key in a
group's ``spark_config`` block can never change which run id ends up stamped on a quarantined
row.
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger("flowx.lakeflow_framework.engine.run_context")

_CANDIDATE_SPARK_CONF_KEYS = (
    "pipelines.id",
    "pipeline.id",
    "spark.databricks.job.runId",
)


def resolve_pipeline_run_id(spark: SparkSession, group_id: str) -> str:
    """Best-effort resolution of a run/update identifier to attach to quarantine metadata.

    Lakeflow does not expose a single documented, stable Spark conf key for "the current
    pipeline update ID" across every runtime version, so this tries a short list of known
    candidate keys and falls back to ``group_id`` (always available, always deterministic)
    rather than raising -- a slightly less specific run identifier is preferable to a failed
    pipeline update over metadata that only helps with post-hoc diagnosis.

    Parameters
    ----------
    spark:
        Active SparkSession.
    group_id:
        The resolved ``dataflow_group_id`` -- used as the fallback identifier.

    Returns
    -------
    str
        The first resolvable candidate value, or ``group_id`` if none are set.
    """
    for conf_key in _CANDIDATE_SPARK_CONF_KEYS:
        try:
            value = spark.conf.get(conf_key)
        except Exception:  # noqa: BLE001 - conf key genuinely absent in this runtime/context
            continue
        if value:
            return value

    logger.info(
        "No pipeline/update run-id Spark conf found among %s; falling back to dataflow_group_id '%s'.",
        _CANDIDATE_SPARK_CONF_KEYS,
        group_id,
    )
    return group_id
