"""Unit tests for observability/task_context_resolver.py -- a fake WorkspaceClient stands in
for the Jobs API, so these are pure Python with no live workspace call.

The fake `_Run`/`_RunTask` shape mirrors what `jobs.get_run(run_id=<a task's own run_id>)`
actually returns (confirmed live against a real workspace): task-type-specific fields like
`pipeline_task` live on `run.tasks[0]` (a single-entry list wrapping the task itself), not on
the top-level `Run` object -- `run.start_time`/`end_time`/`state`/`job_id` ARE correct at the
top level, though, and mirror the wrapped task's own values exactly.
"""

import pytest

from flowx.lakeflow_framework.exceptions import ObservabilityConfigError
from flowx.lakeflow_framework.observability.task_context_resolver import resolve_task_context


class _State:
    def __init__(self, result_state="SUCCESS"):
        self.life_cycle_state = "TERMINATED"
        self.result_state = result_state


class _PipelineTask:
    def __init__(self, pipeline_id):
        self.pipeline_id = pipeline_id


class _RunTask:
    def __init__(self, pipeline_id, has_pipeline_task=True):
        self.pipeline_task = _PipelineTask(pipeline_id) if has_pipeline_task else None


class _Run:
    def __init__(self, pipeline_id="pipe-abc", start_time=1_700_000_000_000, end_time=1_700_000_060_000, job_id=42, state=None, has_pipeline_task=True, no_tasks=False):
        self.tasks = [] if no_tasks else [_RunTask(pipeline_id, has_pipeline_task=has_pipeline_task)]
        self.start_time = start_time
        self.end_time = end_time
        self.job_id = job_id
        self.state = state or _State()


class _FakeJobsClient:
    def __init__(self, run=None, raise_exc=None):
        self._run = run
        self._raise_exc = raise_exc

    def get_run(self, run_id):
        if self._raise_exc:
            raise self._raise_exc
        return self._run


class _FakeWorkspaceClient:
    def __init__(self, run=None, raise_exc=None):
        self.jobs = _FakeJobsClient(run=run, raise_exc=raise_exc)


class TestResolveTaskContext:
    def test_resolves_full_context(self):
        client = _FakeWorkspaceClient(run=_Run())
        context = resolve_task_context(client, "999")
        assert context.pipeline_id == "pipe-abc"
        assert context.start_time_ms == 1_700_000_000_000
        assert context.end_time_ms == 1_700_000_060_000
        assert context.job_id == "42"
        assert context.task_state == "SUCCESS"
        assert context.upstream_task_run_id == "999"

    def test_empty_run_id_raises(self):
        client = _FakeWorkspaceClient(run=_Run())
        with pytest.raises(ObservabilityConfigError, match="upstream_task_run_id is required"):
            resolve_task_context(client, "")

    def test_non_numeric_run_id_raises(self):
        client = _FakeWorkspaceClient(run=_Run())
        with pytest.raises(ObservabilityConfigError, match="must be numeric"):
            resolve_task_context(client, "not-a-number")

    def test_jobs_api_failure_raises(self):
        client = _FakeWorkspaceClient(raise_exc=RuntimeError("permission denied"))
        with pytest.raises(ObservabilityConfigError, match="Failed to fetch run details"):
            resolve_task_context(client, "999")

    def test_no_tasks_on_run_raises(self):
        client = _FakeWorkspaceClient(run=_Run(no_tasks=True))
        with pytest.raises(ObservabilityConfigError, match="has no task details"):
            resolve_task_context(client, "999")

    def test_non_pipeline_task_run_raises(self):
        client = _FakeWorkspaceClient(run=_Run(has_pipeline_task=False))
        with pytest.raises(ObservabilityConfigError, match="not a pipeline_task run"):
            resolve_task_context(client, "999")

    def test_missing_start_time_raises(self):
        client = _FakeWorkspaceClient(run=_Run(start_time=None))
        with pytest.raises(ObservabilityConfigError, match="no start_time"):
            resolve_task_context(client, "999")

    def test_missing_end_time_raises_wiring_hint(self):
        client = _FakeWorkspaceClient(run=_Run(end_time=None))
        with pytest.raises(ObservabilityConfigError, match="depends_on"):
            resolve_task_context(client, "999")
