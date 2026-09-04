"""Runtime parameter contract for the **triggered** observability engine.

``notebooks/08_observability/08_dlt_observability_engine.py`` runs as an ordinary Workflow task,
downstream of the ``pipeline_task`` whose update it exports. Four values reach it as task
parameters, and all four are **required** (v1.4.0):

=========================  ==============================================================
``dataflow_group_id``      Which dataflow group this export is about. Also the
                           ``observability_config`` lookup key.
``catalog``                Control catalog -- ``observability_config`` lives in
                           ``<catalog>.config``.
``env``                    Deployment environment (``dev``/``uat``/``prod``). Becomes the
                           OTel ``deployment.environment`` resource attribute, so a
                           consumer can tell one environment's telemetry from another's.
``pipeline_task_run_id``   The upstream ``pipeline_task``'s own task run id, supplied as
                           ``{{tasks.<task_key>.run_id}}``. Everything else about the run
                           -- pipeline id, start/end wall clock, result state -- is
                           resolved from it via the Jobs API
                           (``task_context_resolver.py``).
=========================  ==============================================================

Why all four are required, when three were previously optional or derived
------------------------------------------------------------------------
``dataflow_group_id`` used to be *derived*: ``event_log_extractor.py::resolve_dataflow_group_id``
reads it back off the pipeline's own ``dataflow.group.id`` configuration. That still runs, but as
a **cross-check** rather than as the source of truth. A derived value cannot detect the single
most likely wiring mistake -- ``depends_on`` pointing at the wrong ``pipeline_task``, or a
copy-pasted observability task block left pointing at the job it was copied from -- because
whatever pipeline it lands on will happily report *its* group id, and the export succeeds while
describing the wrong dataflow. Declaring the expected value turns that into a loud failure. When
the two disagree, :func:`assert_dataflow_group_id_matches` raises and names both.

``env`` used to be an optional ``deployment_environment`` widget. Optional environment labelling
is worse than none: telemetry that omits it is silently merged with every other environment's in
the consumer, and nobody notices until a prod alert fires on dev data.

Why ``pipeline_task_run_id`` is NOT declared in ``pipeline_parameters``
----------------------------------------------------------------------
It is a **task parameter of the observability notebook task**, not a pipeline parameter, and it
must not be declared as one. See :func:`resolve_triggered_run_parameters` for the mechanics and
``docs/08_observability_and_telemetry.md`` for the full write-up. In short: ``pipeline_parameters``
(the ``configuration:`` block on a pipeline resource) is resolved when the *pipeline* is updated
and is static for the life of that deployment; ``{{tasks.<task_key>.run_id}}`` is resolved by the
Jobs service per *job run*, at the moment the downstream task is dispatched. Declaring a dynamic
task value in a static pipeline block cannot work -- there is no run in scope to resolve it
against -- and would at best pin every run to a stale literal.

Every check here is pure Python over a plain mapping -- no Spark, no SDK, no ``dbutils`` -- so the
whole parameter contract is unit-testable, and the notebook stays thin orchestration per this
repo's own convention (``AGENTS.md``).
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from flowx.lakeflow_framework.exceptions import ObservabilityConfigError

logger = logging.getLogger("flowx.lakeflow_framework.observability.runtime_params")

#: The four task parameters a triggered observability run cannot start without, in the order they
#: are reported when missing. Ordered "what is this about" -> "where is its config" ->
#: "which environment" -> "which run", which is the order an operator reasons about them in.
REQUIRED_TRIGGERED_PARAMETERS = ("dataflow_group_id", "catalog", "env", "pipeline_task_run_id")

#: An unresolved Databricks parameter reference, e.g. ``{{tasks.run_pipeline_update.run_id}}``.
#: The Jobs service substitutes these at dispatch time; when the referenced task key does not
#: exist in the job, it does NOT fail -- it passes the literal text through. That literal is the
#: single most common symptom of a mis-wired observability task, so it gets its own check and its
#: own message rather than being left to fail later as "not numeric".
_UNRESOLVED_REFERENCE_PATTERN = re.compile(r"\{\{.*\}\}")


@dataclass(frozen=True)
class TriggeredRunParameters:
    """The validated four. Frozen -- these are the run's identity, not working state."""

    dataflow_group_id: str
    catalog: str
    env: str
    pipeline_task_run_id: str


def resolve_triggered_run_parameters(raw_parameters: Mapping[str, Any]) -> TriggeredRunParameters:
    """Validate and normalize the four required triggered-mode task parameters.

    Parameters
    ----------
    raw_parameters:
        ``{name: value}`` exactly as read from the task's widgets -- values may be ``None``,
        empty, or whitespace, all of which count as absent. Extra keys are ignored, so a caller
        can pass its whole widget dict without filtering.

    Returns
    -------
    TriggeredRunParameters
        All four, stripped.

    Raises
    ------
    ObservabilityConfigError
        If any are missing/blank (**all** of them are reported in one message -- an operator
        fixing a job definition should learn about every missing parameter in one run, not
        discover them one redeploy at a time), if ``pipeline_task_run_id`` is still an unresolved
        ``{{...}}`` reference, or if it is not numeric.

    Notes
    -----
    ``pipeline_task_run_id`` gets two checks the other three do not, because it is the only one
    whose value is produced by the platform rather than typed by a human:

    1. **Still-templated.** ``{{tasks.<task_key>.run_id}}`` resolves only when ``<task_key>``
       names a task in the *same job run*. Misname it and the Jobs service substitutes nothing
       and passes the literal string through, so the notebook receives ``"{{tasks.foo.run_id}}"``
       as its run id. Left unchecked that becomes a confusing "must be numeric" complaint about a
       value the operator never wrote; checked, it names the actual defect -- the task key does
       not exist -- and the fix.
    2. **Numeric.** A resolved task run id is an integer. This mirrors the check
       ``task_context_resolver.resolve_task_context`` makes before its Jobs API call, on purpose:
       failing here means failing before any API call, any table read, and any dispatch, so a
       mis-wired job costs one second instead of a partial export.
    """
    missing = []
    values = {}
    for name in REQUIRED_TRIGGERED_PARAMETERS:
        value = raw_parameters.get(name)
        text = "" if value is None else str(value).strip()
        if not text:
            missing.append(name)
        values[name] = text

    if missing:
        raise ObservabilityConfigError(
            f"Triggered observability run is missing required task parameter(s): {missing}. "
            "All of "
            f"{list(REQUIRED_TRIGGERED_PARAMETERS)} must be supplied as base_parameters on the "
            "observability task -- see resources/observability/dlt_observability_job.yml. "
            'pipeline_task_run_id is a dynamic task value: pass "{{tasks.<pipeline_task_key>.run_id}}", '
            "where <pipeline_task_key> is the task_key of the pipeline_task this task depends_on."
        )

    run_id = values["pipeline_task_run_id"]
    if _UNRESOLVED_REFERENCE_PATTERN.search(run_id):
        raise ObservabilityConfigError(
            f"pipeline_task_run_id is still an unresolved parameter reference ({run_id!r}). The Jobs service "
            "substitutes {{tasks.<task_key>.run_id}} only when <task_key> names a task in this job; when it does "
            "not, the literal text is passed through instead of failing. Fix: correct the task_key to match the "
            "pipeline_task this observability task depends_on."
        )
    if not run_id.isdigit():
        raise ObservabilityConfigError(
            f"pipeline_task_run_id must be a numeric task run id, got {run_id!r}. Expected the resolved value of "
            "{{tasks.<pipeline_task_key>.run_id}} -- note run_id, not job_id and not {{job.run_id}} (the parent "
            "job run, which is not the pipeline task's own run)."
        )

    resolved = TriggeredRunParameters(
        dataflow_group_id=values["dataflow_group_id"],
        catalog=values["catalog"],
        env=values["env"],
        pipeline_task_run_id=run_id,
    )
    logger.info(
        "Triggered observability parameters resolved: dataflow_group_id=%s, catalog=%s, env=%s, "
        "pipeline_task_run_id=%s",
        resolved.dataflow_group_id,
        resolved.catalog,
        resolved.env,
        resolved.pipeline_task_run_id,
    )
    return resolved


def assert_dataflow_group_id_matches(declared: str, resolved_from_pipeline: Optional[str], pipeline_id: str) -> str:
    """Cross-check the declared ``dataflow_group_id`` against the upstream pipeline's own.

    Parameters
    ----------
    declared:
        ``TriggeredRunParameters.dataflow_group_id`` -- what the job says this export is about.
    resolved_from_pipeline:
        ``event_log_extractor.resolve_dataflow_group_id``'s answer -- the ``dataflow.group.id``
        configured on the pipeline the upstream task actually ran. ``None`` when the pipeline
        does not declare one.
    pipeline_id:
        Named in the error, so an operator can go straight to the pipeline that disagreed.

    Returns
    -------
    str
        ``declared``, always. The declaration is authoritative when the two agree, and when they
        disagree this raises rather than returning either -- so there is no path on which a
        caller silently exports under a group id nobody asked for.

    Raises
    ------
    ObservabilityConfigError
        If both are present and differ.

    Notes
    -----
    A ``None`` ``resolved_from_pipeline`` is **not** an error. Not every pipeline sets
    ``dataflow.group.id`` in its ``configuration:`` block -- a pre-v1.3.0 deployment legitimately
    does not -- and refusing to export because the cross-check has nothing to check against would
    turn an optional safety net into a new hard requirement on every pipeline in the estate. The
    declared value stands, and the absence is logged at INFO so it is visible without being
    alarming.
    """
    if not resolved_from_pipeline:
        logger.info(
            "Pipeline '%s' declares no dataflow.group.id -- cannot cross-check the declared "
            "dataflow_group_id='%s'. Proceeding on the declared value.",
            pipeline_id,
            declared,
        )
        return declared
    if resolved_from_pipeline != declared:
        raise ObservabilityConfigError(
            f"dataflow_group_id mismatch: this task declares {declared!r}, but the upstream pipeline "
            f"'{pipeline_id}' it just observed is configured for {resolved_from_pipeline!r}. Exporting anyway "
            "would file one dataflow group's telemetry under another's name. Fix: either point this task's "
            "depends_on at the pipeline_task for "
            f"{declared!r}, or correct the dataflow_group_id parameter to {resolved_from_pipeline!r}."
        )
    return declared
