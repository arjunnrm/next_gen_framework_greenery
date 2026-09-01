"""Resolves the upstream ``pipeline_task`` run into a concrete execution window.

This module exists because the observability engine runs as a *separate* Workflow task,
downstream of the task that actually ran the DLT pipeline update (``run_pipeline_update`` by
convention -- see ``resources/observability/dlt_observability_job.yml``). All this module is ever handed is
that upstream task's own ``run_id``, arriving as the ``pipeline_task_run_id`` task parameter
whose value is ``{{tasks.<pipeline_task_key>.run_id}}`` -- a Databricks Jobs *dynamic value
reference*, substituted by the Jobs service into the downstream task's parameters at dispatch
time. It is the pipeline task's own run id, **not** the parent job's (``{{job.run_id}}``), and
it is a *task* parameter: it is never declared in a pipeline's ``pipeline_parameters``, which
are resolved per pipeline update and have no job run in scope to resolve a task value against.
See ``runtime_params.py`` for the full contract and the validation that enforces it.

Everything else -- which pipeline ran, and the wall-clock window its update(s) executed in --
is resolved from the Jobs API here, before the event log can be queried.

``dataflow_group_id`` is deliberately **not** resolved here even though it is knowable from the
pipeline's own ``dataflow.group.id`` configuration (this framework's one-pipeline-per-group
convention -- see ``AGENTS.md``/``SKILL.md`` §2). That lookup lives in
``event_log_extractor.py::resolve_dataflow_group_id`` instead, next to the event-log queries it
exists to label, keeping this module's one job to "resolve run_id -> (pipeline_id, time window)"
and nothing else. As of v1.4.0 the group id is also *declared* as a required task parameter and
the derived value is used to cross-check it (``runtime_params.assert_dataflow_group_id_matches``)
-- deriving alone could never catch a task wired to the wrong pipeline.

The same boundary holds for the v1.3.0 ``update_id`` narrowing: the window this module returns is
the upstream *task run*'s wall clock, and a pipeline may legitimately have run more than one
update inside it. Working out *which* of those updates this task produced is another Pipelines
API lookup, and it lives beside the query it narrows, as
``event_log_extractor.py::resolve_update_ids_for_window`` -- not here. The window this module
returns therefore stays the honest, unnarrowed task boundary, and narrowing stays an optional
refinement layered on top of it by the caller (see
``notebooks/08_observability/08_dlt_observability_engine.py``).
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import ObservabilityConfigError

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.observability.task_context_resolver")


@dataclass
class TaskExecutionContext:
    """The resolved identity + time window of the upstream ``pipeline_task`` run."""

    upstream_task_run_id: str
    job_id: Optional[str]
    pipeline_id: str
    start_time_ms: int
    end_time_ms: int
    task_state: Optional[str]


def resolve_task_context(workspace_client: Any, upstream_task_run_id: str) -> TaskExecutionContext:
    """Resolve the upstream ``pipeline_task`` run into a :class:`TaskExecutionContext`.

    Calls the Databricks Jobs API (``/api/2.1/jobs/runs/get``, via the SDK's
    ``jobs.get_run``) on the task's own ``run_id`` -- each task execution inside a multi-task
    job run has its own distinct ``run_id``, and querying it directly returns that task's own
    ``start_time``/``end_time``/``state`` plus its task-type-specific settings (here,
    ``pipeline_task.pipeline_id``) at the top level of the response.

    Parameters
    ----------
    workspace_client:
        A ``databricks.sdk.WorkspaceClient`` (or any object exposing the same
        ``jobs.get_run(run_id=...)`` surface) -- authenticated implicitly from the notebook's
        own run context when constructed with no arguments inside a Databricks job/pipeline.
    upstream_task_run_id:
        The validated ``pipeline_task_run_id`` task parameter -- a numeric run_id as a string.
        ``runtime_params.resolve_triggered_run_parameters`` has already rejected a blank, an
        unresolved ``{{...}}`` reference and a non-numeric value by the time this is called; the
        re-checks below are defense in depth for a direct caller that skipped it.

    Returns
    -------
    TaskExecutionContext

    Raises
    ------
    ObservabilityConfigError
        If ``upstream_task_run_id`` isn't a valid integer, the run cannot be fetched, the run
        was not a ``pipeline_task`` (nothing to observe), or its ``end_time`` isn't yet
        populated (the run hasn't actually finished -- since this task is meant to be wired as
        a downstream dependency of ``run_pipeline_update``, that should not happen in normal
        operation, and is treated as a configuration/wiring error rather than something to
        silently work around).
    """
    if not upstream_task_run_id or not str(upstream_task_run_id).strip():
        raise ObservabilityConfigError(
            "upstream_task_run_id is required (the pipeline_task_run_id task parameter, "
            "{{tasks.<pipeline_task_key>.run_id}})."
        )

    try:
        run_id_int = int(str(upstream_task_run_id).strip())
    except ValueError as exc:
        raise ObservabilityConfigError(f"upstream_task_run_id must be numeric, got {upstream_task_run_id!r}: {exc}") from exc

    try:
        run = workspace_client.jobs.get_run(run_id=run_id_int)
    except Exception as exc:  # noqa: BLE001
        raise ObservabilityConfigError(f"Failed to fetch run details for run_id={run_id_int}: {exc}") from exc

    # Querying by a task's own run_id returns a Run whose *task-type-specific* fields (e.g.
    # pipeline_task) live on run.tasks[0] -- the single-entry RunTask list representing that
    # task itself -- not on the top-level Run object directly (confirmed live: the SDK reports
    # run_type=JOB_RUN for this query shape, wrapping the one task as a synthetic job run).
    # run.start_time/end_time/state/job_id, by contrast, ARE already correct at the top level
    # (confirmed live to mirror the wrapped task's own values exactly) and are read from there
    # below, unchanged.
    run_tasks = getattr(run, "tasks", None) or []
    if not run_tasks:
        raise ObservabilityConfigError(
            f"run_id={run_id_int} has no task details (Run.tasks is empty) -- expected exactly one entry, "
            "the task itself, when queried by its own run_id."
        )
    pipeline_task = getattr(run_tasks[0], "pipeline_task", None)
    pipeline_id = getattr(pipeline_task, "pipeline_id", None)
    if not pipeline_id:
        raise ObservabilityConfigError(
            f"run_id={run_id_int} is not a pipeline_task run (no pipeline_task.pipeline_id present) -- "
            "the observability engine must be wired downstream of the pipeline_task, not another task type."
        )

    start_time_ms = getattr(run, "start_time", None)
    end_time_ms = getattr(run, "end_time", None)
    if not start_time_ms:
        raise ObservabilityConfigError(f"run_id={run_id_int} has no start_time -- cannot resolve an execution window.")
    if not end_time_ms:
        state = getattr(run, "state", None)
        raise ObservabilityConfigError(
            f"run_id={run_id_int} has no end_time yet (state={getattr(state, 'life_cycle_state', state)!r}) -- "
            "the observability task must depends_on the pipeline_task whose run_id it was given, so it only "
            "starts after that pipeline update has actually completed."
        )

    task_state = None
    state = getattr(run, "state", None)
    if state is not None:
        result_state = getattr(state, "result_state", None)
        task_state = str(result_state) if result_state is not None else None

    context = TaskExecutionContext(
        upstream_task_run_id=str(run_id_int),
        job_id=str(getattr(run, "job_id", "")) or None,
        pipeline_id=pipeline_id,
        start_time_ms=int(start_time_ms),
        end_time_ms=int(end_time_ms),
        task_state=task_state,
    )
    logger.info(
        "Resolved run_id=%s -> pipeline_id=%s, window=[%d, %d] (%d ms), state=%s",
        context.upstream_task_run_id,
        context.pipeline_id,
        context.start_time_ms,
        context.end_time_ms,
        context.end_time_ms - context.start_time_ms,
        context.task_state,
    )
    return context
