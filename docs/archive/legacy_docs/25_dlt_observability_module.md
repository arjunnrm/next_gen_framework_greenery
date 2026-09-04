# DLT Observability Module

> See also: [README.md](README.md) — the full FlowX documentation set.

## Purpose

FlowX's pipeline graph (`03_lakeflow_declarative_pipeline.py`) already gets Lakeflow's own
native event log — `flow_progress`/`dataset_definition`/`update_progress`/... events, covering
generic per-flow row counts and native (`warn`/`drop`/`fail`) expectation pass/fail counts, for
free. What it does not get is a **vendor-neutral, exportable** telemetry format any external
observability stack (Datadog, an OTel Collector, a plain audit Volume) can ingest without a
Databricks-specific connector, and it does not carry this framework's own business identifiers
(`dataflow_group_id`, `dataflow_id`/`step_id`) since those live in this framework's own control
tables, not in Lakeflow's event log at all.

This module closes that gap: it runs **after** a pipeline update completes, as a separate
Databricks Workflow task, reads that update's slice of the DLT event log, enriches it with this
framework's own identifiers, and re-exports it as strict [OpenTelemetry (OTel) Logs Data
Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/) JSON to one or more configured
destinations.

It is deliberately **not** a rewrite of `observability/structured_logger.py` (Phase 10's
in-pipeline business-event JSON logging — flow read/write/reject counts emitted live, from
inside `engine/flow_registration.py` et al., landing in driver/cluster logs). The two are
complementary: `structured_logger.py` is real-time, in-process, Databricks-log-only telemetry;
this module is a batch, post-hoc, vendor-neutral re-export of Lakeflow's own native event log.
Neither depends on the other, though this module's own dispatch phase reuses
`structured_logger.py`'s `log_flow_event`/`logged_operation` for its own operational logging
(see [Step-by-step logging](#step-by-step-logging) below) — that's the one place they touch.

## Architecture overview

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│ Databricks Workflow (one job)                                                        │
│                                                                                        │
│  [Task: run_pipeline_update]  (pipeline_task)                                         │
│   Executes the DLT/Lakeflow pipeline update → writes to that pipeline's own event log  │
│                                                                                        │
│  [Task: observability_export]  (notebook_task, depends_on: run_pipeline_update)        │
│   base_parameters: run_pipeline_update_run_id = "{{tasks.run_pipeline_update.run_id}}" │
│                                                                                        │
│   1. Context Resolution   → task_context_resolver.py + event_log_extractor.py         │
│   2. Extraction           → event_log_extractor.py::extract_raw_events                │
│   3. Transformation       → event_log_extractor.py::aggregate_flow_metrics            │
│                              + otel_payload_builder.py::build_resource_logs           │
│   4. Dispatch             → destination_dispatcher.py::dispatch_all                   │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

See `resources/dlt_observability_job.yml` for a complete, runnable example of this two-task
wiring, and `notebooks/08_observability/08_dlt_observability_engine.py` for the entrypoint
notebook that executes the four numbered phases above in order.

## Component breakdown

All business logic lives under
`src/flowx/lakeflow_framework/observability/` — the entrypoint notebook is
deliberately thin orchestration, per this repo's own convention (`AGENTS.md`/`agent_skills/SKILL.md`).

| Module | Responsibility |
|---|---|
| `config_loader.py` | Reads/parses `observability_config`; resolves enabled destinations for a `dataflow_group_id` (group-specific rows override a same-`destination_id` `"*"` global-fallback row). Read-only -- rows are populated by `onboarding/metadata_upsert.py::upsert_observability_config` from the onboarding spec's own `observability[]` array, not a separate config file (see [Configuration](#configuration-embedded-in-the-onboarding-spec) below). |
| `task_context_resolver.py` | Turns the upstream task's own `run_id` (`{{tasks.run_pipeline_update.run_id}}`) into `(pipeline_id, start_time_ms, end_time_ms)` via the Jobs API (`/api/2.1/jobs/runs/get`, via `WorkspaceClient().jobs.get_run`). |
| `event_log_extractor.py` | `resolve_dataflow_group_id` (Pipelines API); `extract_raw_events` (queries `event_log(:pipeline_id)` for the resolved window, Spark-touching); `aggregate_flow_metrics` (pure, no Spark — groups raw events per `(update_id, flow_id)` into `FlowMetrics`/`UpdateSummary`/`ErrorDetail`). |
| `otel_payload_builder.py` | Pure mapping from `DataflowGroupTelemetry` to strict OTLP/HTTP JSON `ResourceLogs` — one `Resource` per flow; `validate_resource_logs` asserts every mandatory OTel field is present. |
| `destination_dispatcher.py` | `DATABRICKS_VOLUME` (plain `open()` write to the Volume's FUSE mount) / `OTLP_CONSUMER` (HTTP POST with resolved auth headers, gzip, exponential-backoff retry on `429`/`5xx`) dispatch, isolating one destination's failure from the others. |
| `agent_tools.py` | The 3 pure-Python functions backing the AI Agent tools in `agent_skills/dlt_observability_tools.json` (Deliverable 6 — see [docs/27](27_dlt_observability_onboarding_reference.md) for the attribute dictionary `validate_observability_config` enforces). |
| `control_plane/ddl_definitions.py::get_observability_config_ddl` | The `observability_config` control table's DDL — auto-provisioned by `01_setup_control_tables.py` alongside the other 7 control tables. |
| `exceptions.py::ObservabilityConfigError` / `ObservabilityDispatchError` | Typed failures for config/context-resolution problems and all-destinations-failed dispatch, respectively (see [Error Handling Matrix](#error-handling-matrix--troubleshooting) below). |

### Configuration: embedded in the onboarding spec

There is **no separate observability config file or seed step**. Telemetry destinations are
declared in a top-level `observability[]` array inside the *same* onboarding spec as
`ingestion_flows`/`transformation_flows`/`reconciliation_flows` (see
[docs/01_control_metadata_schema.md](01_control_metadata_schema.md) and
[docs/27](27_dlt_observability_onboarding_reference.md)):

```json
{
  "dataflow_group_id": "dfg_finance_txn_ingest",
  "ingestion_flows": [ /* ... */ ],
  "observability": [
    {"id": "dest-vol", "type": "DATABRICKS_VOLUME", "destination_config": {"volume_path": "/Volumes/{{catalog}}/observability/logs/"}}
  ]
}
```

`onboarding/spec_validator.py::_validate_observability_destinations` validates this array
alongside every other flow (via `onboarding_templates/onboarding_spec.schema.json`'s
`observability` property/`$defs` too, for agent/Genie-facing structural checks);
`onboarding/metadata_upsert.py::upsert_observability_config` `MERGE`s it into
`observability_config`, keyed by a `config_id` deterministically derived from
`(dataflow_group_id, destination_id)` so re-onboarding the same spec is idempotent. Both run as
part of the ordinary `02_onboarding_engine.py` CREATE/UPDATE flow — there is nothing
observability-specific to run separately. `observability[]` deliberately does **not** count
toward the "at least one of ingestion/transformation/reconciliation flows must be non-empty"
requirement — telemetry with nothing to observe is meaningless.

`observability_config` is keyed by **`dataflow_group_id`**, not the real DLT `pipeline_id` —
`dataflow_group_id` is known at onboarding time (it's the spec's own top-level field), while
`pipeline_id` only exists once the pipeline resource is actually deployed. At export time,
`event_log_extractor.py::resolve_dataflow_group_id` bridges the two: it resolves the real
`pipeline_id` (from the Jobs API, via `task_context_resolver.py`) back to the
`dataflow_group_id` `config_loader.py` reads destinations by (see the next section).

### Why `dataflow_group_id` isn't read off the event log

`dataflow_group_id` is **not** a column (or a `details`/`origin` field) anywhere in the DLT
event log — this is a FlowX-specific business identifier, not a Databricks platform concept.
FlowX configures exactly one `dataflow.group.id` Spark conf per pipeline (see
`agent_skills/SKILL.md` §2 and any `resources/*_pipeline.yml`'s `configuration:` block), so
`event_log_extractor.py::resolve_dataflow_group_id` resolves it once, up front, via the
Pipelines API (`GET /api/2.0/pipelines/{pipeline_id}` → `spec.configuration["dataflow.group.id"]`)
rather than expecting it to appear in event rows. A pipeline with no such conf entry — i.e. one
never deployed through this framework's engine notebook — fails fast with
`ObservabilityConfigError` rather than silently labeling telemetry with a guessed value.

### Why one OTel `Resource` per flow, not one per pipeline run

An OTel `Resource` describes "the entity producing the telemetry." This module's own resource
attribute contract (below) puts flow-scoped values — `databricks.dataflow_id`,
`databricks.step_id`, `pipeline.update_id` — directly into the *resource* attribute set, not
just onto individual log records, because a resource attribute is what most log backends index
and let operators filter/group by. Since those values differ per flow within one pipeline run,
`otel_payload_builder.py::build_resource_logs` emits one `ResourceLogs` entry per flow, sharing
the same `databricks.job_id`/`databricks.pipeline_id`/`databricks.dataflow_group_id` values
across all of them. A pipeline update that produced zero `flow_progress` events in the resolved
window still gets exactly one `ResourceLogs` entry with a single diagnostic `WARN` `LogRecord` —
"no telemetry" and "extraction genuinely broke" must never look identical downstream.

### Handling single vs. continuous/multiple updates in one window

`run_pipeline_update`'s resolved `[start_time_ms, end_time_ms]` window may contain more than one
pipeline update — a continuous pipeline, or a job retried mid-window. `aggregate_flow_metrics`
does not assume a single `update_id`: it keys `FlowMetrics` by `(update_id, flow_id)`, so a flow
that ran across two updates in the window gets two independent `FlowMetrics` entries (row counts
reset per update, they never accumulate across update boundaries) and therefore two independent
`ResourceLogs` entries, each carrying its own `pipeline.update_id`. `UpdateSummary` rolls up one
entry per distinct `update_id` seen, driven by `update_progress` events' `details.update_progress.state`.

## Event log → OpenTelemetry field mapping reference

Event log schema per [Databricks' own reference](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
— `origin`/`error` are native structs (dot-accessible); `details` is a `STRING` column holding a
JSON payload whose shape depends on `event_type` (hence JSON-typed rather than a fixed struct).

### Resource attributes (one `Resource` per flow)

| OTel resource attribute | Source | Notes |
|---|---|---|
| `service.name` | `observability_config` destination's `resource_attributes.service.name`, else the notebook's `service_name` widget (default `"dlt-observability"`) | Destination-specific value merged in by `destination_dispatcher.py::merge_resource_attributes` immediately before dispatch — the canonical payload built by `otel_payload_builder.py` uses the widget default. |
| `deployment.environment` | `observability_config` destination's `resource_attributes.deployment.environment`, else the notebook's `deployment_environment` widget | Same merge rule as `service.name`. |
| `databricks.job_id` | Jobs API `run.job_id` (via `task_context_resolver.py`) | |
| `databricks.task_run_id` | The `run_pipeline_update` task's own `run_id` (the widget input itself) | |
| `databricks.pipeline_id` | `task_context_resolver.py::resolve_task_context` → `run.pipeline_task.pipeline_id` | |
| `databricks.dataflow_group_id` | `event_log_extractor.py::resolve_dataflow_group_id` (Pipelines API, see above) | Constant across every `ResourceLogs` entry in one run. |
| `databricks.dataflow_id` / `databricks.step_id` | Event log `origin.flow_id` | Both attributes carry the same value — `dataflow_id` (ingestion) and `flow_step_id` (transformation) are FlowX's own two names for the same underlying Lakeflow `flow_id`; both are populated so a consumer can filter by either vocabulary. |
| `pipeline.update_id` | Event log `origin.update_id` | |
| `pipeline.config.*` | `job_context["pipeline_config"]` (currently unpopulated by the entrypoint notebook — a documented extension point, see [Extending](#extending-this-module)) | One resource attribute per key. |

### Log records (per flow, inside one `ScopeLogs`, `scope.name = "flowx.lakeflow_framework.observability.dlt_observability"`)

| LogRecord | Built from | `body` | `severityNumber` | Key attributes |
|---|---|---|---|---|
| Flow performance summary (always exactly 1 per flow) | Latest `flow_progress` event for `(update_id, flow_id)` | `"Flow '<flow_id>' status=<status> in update '<update_id>'"` | `FAILED`→17 (ERROR), `STOPPED`/`SKIPPED`/`EXCLUDED`→13 (WARN), else 9 (INFO) | `flow.status`, `flow.start_time_ms`/`end_time_ms`/`duration_ms`, `flow.num_output_rows`/`num_upserted_rows`/`num_deleted_rows`, `flow.dropped_records`, `flow.backlog_bytes`/`backlog_files` — from `details.flow_progress.metrics`/`.data_quality` |
| Data quality expectation (0..N per flow) | `details.flow_progress.data_quality.expectations[]` | `"Expectation '<name>' on dataset '<dataset>': <passed> passed, <failed> failed"` | any `failed_records > 0` → 13 (WARN), else 9 (INFO) | `expectation.name`, `expectation.dataset`, `expectation.passed_records`, `expectation.failed_records` |
| Pipeline/flow error (0..N per flow, plus a pipeline-level `ResourceLogs` entry for errors with no `origin.flow_id`) | `error` struct (`fatal`, `exceptions[].class_name`/`message`/`stack_trace`), or any `ERROR`/`WARN`-level event with no structured `error` | The exception `message` (or event `message`) | `fatal=true`→21 (FATAL), else 17 (ERROR); a bare `WARN`-level event with no `error` struct is emitted at its own level, not upgraded | `error.event_type`, `error.level`, `error.fatal`, `error.class_name`, `error.stack_trace` |
| No-events diagnostic (only when a flow-less window produces zero flows) | Synthetic — no source event | `"No flow_progress events found for dataflow_group_id='...' in window [...] (N total event(s))."` | 13 (WARN) | `event.name = "no_flow_events_in_window"`, `window.total_events` |

`severityNumber` values follow the OTel Logs Data Model's base-of-range convention: TRACE=1,
DEBUG=5, INFO=9, WARN=13, ERROR=17, FATAL=21.

## Destination dispatch

| `destination_type` | Mechanism | Auth | Compression |
|---|---|---|---|
| `DATABRICKS_VOLUME` | `open(file_path, "wb")` against the Volume's FUSE mount — no `dbutils`/token needed, governed entirely by native Unity Catalog Volume permissions on the job's run-as identity. One JSONL line per `ResourceLogs` entry, path `<volume_path>/<dataflow_group_id>/<yyyy-mm-dd>/<dataflow_group_id>_<task_run_id>.jsonl[.gz]` — the file name itself is deterministic (`dataflow_group_id` + the observability task's own `run_id`), not a random UUID, so a file identifies its own run without being opened. | None | `GZIP`/`gzip` or `none`/`""` (case-insensitive) |
| `OTLP_CONSUMER` | `requests.post(endpoint, ...)`, body = `{"resourceLogs": [...]}` for every flow's payload in this run, batched into one POST per destination. Retries on `429`/`500`/`502`/`503`/`504` with exponential backoff (`retry_config.backoff_multiplier`, default 2.0, capped at 30s; honors a `Retry-After` header on `429`), up to `retry_config.max_attempts` (default 3). Any other 4xx fails immediately, no retry. | `BEARER_TOKEN` / `API_KEY` / `BASIC_AUTH` / `NONE`, credentials always `env:<VAR_NAME>` or `secret:<scope>:<key>` references (see [docs/27](27_dlt_observability_onboarding_reference.md)) — never a literal value. | Same as above; sets `Content-Encoding: gzip` when compressed. |

**One destination's failure never blocks the others** — `destination_dispatcher.py::dispatch_all`
catches every exception per-destination and continues; `ObservabilityDispatchError` is raised
only when *every* enabled destination failed (or none were configured at all), which the
entrypoint notebook's `fail_task_on_dispatch_error` widget (default `true`) then turns into a
failed Workflow task run.

## Step-by-step logging

Every phase logs through Python's standard `logging` module (`logger.info`/`logger.warning`/
`logger.error`, landing in the task's driver/cluster logs), plus a `logged_operation` /
`log_flow_event` call (reused from `observability/structured_logger.py`, `flow_id` =
`dataflow_group_id`) bracketing each of the four numbered phases and each individual
destination's dispatch:

1. **Context Resolution** — upstream `run_id`, resolved `start_time` → `end_time`,
   `pipeline_id`, `dataflow_group_id`, and the count + IDs of active destinations loaded.
2. **Extraction** — the `event_log(:pipeline_id)` query, total rows pulled, query duration (ms).
3. **Transformation** — record counts per flow, DQ expectation count evaluated, errors
   intercepted, total `ResourceLogs`/`LogRecord` entities generated.
4. **Dispatch** (per destination) — destination ID/type, endpoint or volume path, compression,
   uncompressed/compressed byte sizes, HTTP status + latency + attempt number (OTLP) or written
   file path + size + duration (Volume).
5. **Failure handling** — the failing phase, target destination (when applicable), exception
   message; credentials are never logged (only `env:`/`secret:` *references* ever reach this
   module's code — the resolved value itself is used solely to build an HTTP header and is
   never passed to any logging call).

## Error Handling Matrix / Troubleshooting

This table is the human-readable form of `observability/agent_tools.py::diagnose_pipeline_telemetry_failures`'s
`_FAILURE_MATRIX` — keep the two in sync when either changes.

| Symptom / exception text | Category | Likely cause | Remediation |
|---|---|---|---|
| `... is not a pipeline_task run (no pipeline_task.pipeline_id present)` | Task wiring | `run_pipeline_update_run_id` widget was pointed at a task that isn't the `pipeline_task`. | Confirm `base_parameters.run_pipeline_update_run_id = "{{tasks.<pipeline_task_key>.run_id}}"` names the actual `pipeline_task`'s `task_key`. |
| `... has no start_time` / `... has no end_time yet` | Task wiring | Observability task ran before/independently of `run_pipeline_update` finishing. | Add `depends_on: [{task_key: run_pipeline_update}]`. |
| `Pipeline '...' has no 'dataflow.group.id' configuration entry` | Pipeline config | Target pipeline wasn't deployed through this framework's engine notebook, or the conf was removed. | Set `configuration: {dataflow.group.id: <id>}` in the pipeline resource YAML and redeploy. |
| `No enabled observability_config destinations resolved for dataflow_group_id='...'` | Destination config | This `dataflow_group_id`'s onboarding spec has no `observability[]` array (or `"*"` has none either), or every matching row is `enabled=false`. | Add an `observability[]` array to the onboarding spec and re-run onboarding (CREATE/UPDATE); flip `enabled=true`. |
| `Environment variable '...' referenced by 'env:...' is not set` | Credentials | `auth_config` env var reference not injected into this task's environment. | Add the env var as a secret-backed environment variable on the job cluster/serverless environment. |
| `Malformed secret reference` / `Failed to resolve secret` | Credentials | `secret:<scope>:<key>` malformed, or the scope/key doesn't exist / isn't readable. | Verify with `databricks secrets list-secrets <scope>`; grant READ to the job's run-as identity. |
| `volume_path is required for DATABRICKS_VOLUME destinations` | Destination config | Missing `destination_config.volume_path`. | Add it (must start with `/Volumes/`). |
| `destination_config.endpoint is required for OTLP_CONSUMER destinations` | Destination config | Missing `destination_config.endpoint`. | Add a full `https://` URL. |
| `HTTP 429: ...` (after exhausting retries) | Destination outage | Destination is rate-limiting this pipeline's telemetry volume. | Raise `retry_config.max_attempts`/`backoff_multiplier`, or reduce telemetry volume. |
| `HTTP 5xx: ...` (after exhausting retries) | Destination outage | The destination itself is erroring on every attempt. | Check the destination's own status/logs — usually not a FlowX-side problem. |
| `Unsupported compression '...'` | Destination config | `compression` isn't `gzip`/`GZIP`/`none`/`""`. | Fix the value — see [docs/27](27_dlt_observability_onboarding_reference.md). |
| `... literal secrets are not allowed` | Credentials | `auth_config.credentials` contains a literal secret string instead of an `env:`/`secret:` reference. | Replace with a reference; store the real value in an env var or secret scope. |
| `Failed to query event_log(...)` | Event log access | Run-as identity lacks `CAN_VIEW`/`CAN_MANAGE` on the pipeline, or it has never run an update. | Grant pipeline permissions; confirm at least one completed update exists. |
| `All N destination(s) failed for dataflow_group_id='...'` | Aggregate failure | Every individually-diagnosable failure above happened simultaneously across all destinations. | Diagnose each destination's own error in the `ObservabilityDispatchError` message using the rows above. |

## Extending this module

- **A new destination type**: add a branch in `destination_dispatcher.py::dispatch_all` (and a
  new `dispatch_to_<type>` function alongside `dispatch_to_volume`/`dispatch_to_otlp`), add the
  literal to `ALLOWED_DESTINATION_TYPES` in `config_loader.py` and
  `ALLOWED_OBSERVABILITY_DESTINATION_TYPES` in `onboarding/spec_validator.py`, and to the
  `enum` in `onboarding_templates/onboarding_spec.schema.json`'s `observabilityDestination` def.
- **OTLP gRPC / OTLP HTTP Protobuf**: `destination_config.protocol` already accepts
  `OTLP_HTTP_PROTO`/`OTLP_GRPC` values structurally (see [docs/27](27_dlt_observability_onboarding_reference.md)),
  but `destination_dispatcher.py::dispatch_to_otlp` only implements `OTLP_HTTP_JSON` today — a
  protocol-dispatch branch there is the extension point.
- **`pipeline.config.*` resource attributes**: currently unpopulated —
  `08_dlt_observability_engine.py` passes `job_context={"pipeline_config": {}}`. Populate it from
  `dataflow_group_spec.pipeline_parameters_json` (via `control_plane/repository.py`) if per-run
  pipeline parameters should be visible as OTel resource attributes.
