# 📊 Metaflow — Observability & OpenTelemetry Engine

> **Audience**: Site Reliability Engineers (SREs), DevOps teams, and platform architects responsible for monitoring Lakeflow pipeline health, SLA tracking, and OpenTelemetry integration.

---

## 1. Observability Architecture Overview

Metaflow includes an enterprise telemetry engine that captures pipeline lifecycle metrics, data quality statistics, and operational event logs from the native Databricks Lakeflow Event Log and formats them into OpenTelemetry (OTel) standard payloads.

As of **v1.3.0** there are two independent export engines, and every destination is served by exactly one of them — never both, and there is no single entrypoint that switches between them. A bounded downstream job task and an always-on `continuous: true` pipeline have fundamentally different lifecycles, so which engine owns a destination is decided by the destination's own `mode` field, resolved once at read time:

| `mode` | Engine (notebook) | Lifecycle | Table source |
|---|---|---|---|
| `"triggered"` (default) | `notebooks/08_observability/08_dlt_observability_engine.py` | Bounded — runs once, as a Workflow task chained after one pipeline's update finishes | `event_log(:pipeline_id)` for the one pipeline resolved from the upstream task run |
| `"continuous"` | `notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py` | Always-on — a `continuous: true` Lakeflow pipeline with no terminal moment | An array of fully-qualified `catalog.schema.event_log_table` names, streamed and unioned |

### 1.1 Triggered mode

```
┌─────────────────────────────────────────────────────────────┐
│                 LAKEFLOW DECLARATIVE PIPELINE                │
│ • Emits events, state transitions, and DQ metrics            │
└──────────────────────────────┬──────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                 DATABRICKS DLT EVENT LOG                     │
│      event_log(pipeline_id) system Delta table               │
└──────────────────────────────┬──────────────────────────────┘
                                │
                 ▼ Workflow task: observability_export
┌─────────────────────────────────────────────────────────────┐
│      08_dlt_observability_engine.py  (mode = triggered)      │
│ • Task Context Resolver: run_id -> pipeline_id + time window │
│ • Event Log Extractor: windowed (+ update_id-narrowed) query │
│ • OTel Payload Builder: converts events into OTel ResourceLogs│
│ • dispatch_all(): fans out to every "triggered" destination  │
└──────────────────────────────┬──────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                ▼                                ▼
 ┌─────────────────────────────┐  ┌─────────────────────────────┐
 │    DATABRICKS_VOLUME sink    │  │       OTLP_CONSUMER sink     │
 │ Writes one JSONL/GZIP file   │  │ POSTs OTel ResourceLogs to   │
 │ per task run to a Volume     │  │ Dynatrace/Datadog/Splunk/    │
 │ path, native UC permissions  │  │ an OTel Collector            │
 └─────────────────────────────┘  └─────────────────────────────┘
```

### 1.1.1 Triggered-mode run parameters (v1.4.0)

The triggered engine is an ordinary Workflow task, and it takes **four required task parameters**.
All four are validated together by `observability/runtime_params.py` **before** a
`WorkspaceClient` is constructed and before any API call, table read or dispatch — so a mis-wired
job fails in about a second, with every problem reported at once, rather than part-way through an
export.

| Parameter | Required | What it is |
|---|---|---|
| `dataflow_group_id` | ✅ | Which dataflow group this export is about; also the `observability_config` lookup key. |
| `catalog` | ✅ | Control catalog — `observability_config` lives in `<catalog>.config`. |
| `env` | ✅ | Deployment environment (`dev`/`uat`/`prod`). Becomes the OTel `deployment.environment` resource attribute. |
| `pipeline_task_run_id` | ✅ | The upstream `pipeline_task`'s own run id, supplied as `{{tasks.<pipeline_task_key>.run_id}}`. |
| `service_name` | — | OTel `service.name`. Default `dlt-observability`. |
| `fail_task_on_dispatch_error` | — | Default `true`. |
| `narrow_to_task_updates` | — | Default `true`. See §5. |

```yaml
- task_key: observability_export
  depends_on:
    - task_key: run_pipeline_update          # the pipeline_task
  notebook_task:
    notebook_path: ../notebooks/08_observability/08_dlt_observability_engine.py
    base_parameters:
      dataflow_group_id: dfg_zip_csv_dataload
      catalog: flowx
      env: dev
      pipeline_task_run_id: "{{tasks.run_pipeline_update.run_id}}"
```

**Two of these changed in v1.4.0**, and both changes are about removing a silent failure:

- **`dataflow_group_id` was derived; it is now declared.** `event_log_extractor.py::resolve_dataflow_group_id`
  still reads it back off the upstream pipeline's own `dataflow.group.id` configuration, but that
  value is now a **cross-check**, not the source of truth. A derived value cannot detect the most
  likely wiring mistake there is — `depends_on` pointing at the wrong `pipeline_task`, or a
  copy-pasted observability block still pointing at the job it was copied from — because whatever
  pipeline the task lands on reports *its* group id quite happily, and the export succeeds while
  describing the wrong dataflow. A disagreement now raises and names both. A pipeline that declares
  no `dataflow.group.id` at all is **not** an error: the cross-check simply has nothing to check
  against, logs that at INFO, and the declared value stands.
- **`env` replaces the optional `deployment_environment` widget.** Optional environment labelling is
  worse than none — telemetry that omits it is silently merged with every other environment's in the
  consumer, and nobody notices until a prod alert fires on dev data.

- **`run_pipeline_update_run_id` is renamed to `pipeline_task_run_id`.** The old name baked one
  convention — a task literally called `run_pipeline_update` — into the parameter name, so it read
  as a lie in every job whose pipeline task is called something else. The value is the run id of
  whichever `pipeline_task` this task `depends_on`.

#### Does `pipeline_task_run_id` need to be declared in `pipeline_parameters`?

**No. It must not be.** It is a *task* parameter of the observability notebook task, and nothing
about the pipeline resource changes to support it.

The two are resolved by different services, at different times, against different scopes:

| | `{{tasks.<key>.run_id}}` (a Jobs **dynamic value reference**) | `pipeline_parameters` / a pipeline's `configuration:` block |
|---|---|---|
| Resolved by | The Jobs service | The Pipelines service |
| Resolved when | Per **job run**, at the moment the downstream task is dispatched | Per **pipeline update** |
| Scope | The job run that contains both tasks | The pipeline deployment |
| Value | Different on every run | Static until the bundle is redeployed |

A pipeline update has no job run in scope, so there is nothing for a task-value reference to resolve
*against*. Declaring one in `pipeline_parameters` cannot work: at best it pins every run to a stale
literal, and Spark configuration values are plain strings that are never re-evaluated. The general
rule, and the cleanest pattern:

> **Dynamic, per-run values travel as task parameters. Static, per-deployment values travel as
> pipeline configuration.** `pipeline_task_run_id`, `{{job.run_id}}` and `{{job.parameters.*}}` are
> the first kind. `dataflow.group.id`, `dataflow.control.catalog` and the continuous engine's
> event-log table list are the second.

`dataflow_group_id`, `catalog` and `env` are all in the second category *by nature* — they are static
per deployment — but they are still passed as **task parameters** here, not read from pipeline
configuration, for one reason: this task is not the pipeline. It runs a notebook, on its own compute,
in its own task context; it can read the pipeline's configuration only by asking the Pipelines API
about a pipeline it has to identify first. Keeping all four in one place, validated by one call,
beats splitting the contract across two services.

**A mistyped task key does not fail loudly on its own.** When `<key>` in `{{tasks.<key>.run_id}}`
does not name a task in the job, the Jobs service substitutes nothing and passes the literal template
text through as the parameter value. `runtime_params.py` checks for exactly that and says so:

```
pipeline_task_run_id is still an unresolved parameter reference ('{{tasks.typo_task.run_id}}'). The Jobs service substitutes {{tasks.<task_key>.run_id}} only when <task_key> names a task in this job; when it does not, the literal text is passed through instead of failing. Fix: correct the task_key to match the pipeline_task this observability task depends_on.
```

Note also `run_id`, not `job_id`, and not `{{job.run_id}}` — the latter is the *parent job's* run,
which is not the pipeline task's own run and resolves to a different `pipeline_id` (or to none).

### 1.2 Continuous mode (new in v1.3.0)

```
┌───────────────────────────────────────────────────────────────────────┐
│  observability_config rows with mode = "continuous"                    │
│  (destination_config.event_log_tables), scoped to this pipeline's      │
│  dataflow.group.id -- falls back to the pipeline's own                 │
│  dataflow.otel_streaming.event_log_tables configuration when empty     │
└──────────────────────────────┬──────────────────────────────────────────┘
                                │ N fully-qualified event_log tables
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  06_event_log_otel_streaming_pipeline.py   (continuous: true)         │
│  One @dlt.view per table (spark.readStream.table(...)), tagged        │
│  source_pipeline, unioned by unionByName into unified_event_log       │
└──────────────────────────────┬──────────────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                ▼                                ▼
 ┌─────────────────────────────┐  ┌─────────────────────────────────┐
 │ dlt.create_sink(format=json)  │  │ dlt.create_sink(format=          │
 │ per continuous DATABRICKS_    │  │ otel_streaming) -- gated by      │
 │ VOLUME destination, fed by an │  │ otel_export_enabled -- fed by an │
 │ @dlt.append_flow. Writes raw  │  │ @dlt.append_flow. Exports OTel   │
 │ event-log JSON (NOT OTel-     │  │ ResourceLogs-shaped payloads to  │
 │ shaped) -- see §6.2           │  │ an OTLP/HTTP endpoint            │
 └─────────────────────────────┘  └─────────────────────────────────┘
```

Both sinks read the *same* `unified_event_log` streaming table — a table is read once regardless of how many continuous destinations reference it (see §6.1).

---

## 2. Observability Configuration Schema

Observability is configured via the root `observability[]` array in the onboarding specification — the same spec as every other flow, upserted by `onboarding/metadata_upsert.py::upsert_observability_config` into the `observability_config` control table. There is no separate observability config file.

> **Field names below are verified against `onboarding/spec_validator.py::_validate_observability_destinations`, not against any prior version of this document.** The onboarding spec uses short field names (`id`, `type`, `auth`, `retry`) that are renamed on write into the control table's columns (`destination_id`, `destination_type`, `auth_config_json`, `retry_config_json`) — do not confuse the two.

### 2.1 Destination object — top-level fields

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `id` | string | yes | — | Unique within the spec. Becomes `destination_id` in `observability_config`. Re-onboarding the same `id` for the same dataflow group upserts (MERGEs) the same row rather than duplicating it. |
| `type` | string | yes | — | `DATABRICKS_VOLUME` \| `OTLP_CONSUMER` |
| `mode` | string | no | `"triggered"` | `"triggered"` \| `"continuous"` — **v1.3.0**. See §1. |
| `enabled` | boolean | no | `true` | A disabled row is dropped entirely by `config_loader.py` — callers never see it, so there is no "enabled" flag to remember to check downstream. |
| `destination_config` | object | yes | — | Shape depends on `type` and `mode` — see §2.2. |
| `auth` | object | no | none | See §2.3. Ignored for `DATABRICKS_VOLUME` (native Unity Catalog permissions; no auth is read). |
| `retry` | object | no | see §2.4 defaults | `OTLP_CONSUMER` only. |
| `timeout_ms` | integer ≥ 1 | no | `5000` | Sits at the destination's top level in the spec (a sibling of `retry`, not nested inside it) but is folded into `retry_config_json` for storage by `upsert_observability_config`. |

### 2.2 `destination_config` by `type`

**`DATABRICKS_VOLUME`:**

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `volume_path` | string | yes | — | Must start with `/Volumes/`. |
| `file_format` | string | no | `"JSONL"` | `"JSONL"` \| `"JSON"`. |
| `compression` | string | no | `"none"` | Allowed values are `{"GZIP", "gzip", "none", ""}` **exactly as written** — `"GZIP"`/`"gzip"` both enable gzip; only lowercase `"none"` or an empty string disable it. `"NONE"` (uppercase) is rejected. |

**`OTLP_CONSUMER`:**

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `endpoint` | string | yes | — | Must be a full `http://` or `https://` URL. |
| `protocol` | string | no | — | `"OTLP_HTTP_JSON"` \| `"OTLP_HTTP_PROTO"` \| `"OTLP_GRPC"`. |
| `resource_attributes` | object (string → string) | no | `{}` | Merged into every `ResourceLogs` entry's `resource.attributes`, overriding a same-named key the framework already built. |
| `compression` | string | no | `"none"` | Same allowed-values set and case sensitivity as `DATABRICKS_VOLUME` above. |

**Both types, `mode: "continuous"` only, and required there:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_log_tables` | array of string | yes (when `mode` is `"continuous"`) | Non-empty. Each entry must be a fully-qualified `catalog.schema.event_log_table` name (exactly three dot-separated, non-empty parts) — a real Unity Catalog Delta table one *source* pipeline publishes its own event log to. **Rejected outright on a `"triggered"` destination** — the triggered engine resolves its pipeline from the upstream task run, so a table list there would be silently meaningless, which is why the validator reports it instead of tolerating it. |

### 2.3 `auth` object

| Field | Type | Required | Notes |
|---|---|---|---|
| `type` | string | yes (when `auth` present) | `"BEARER_TOKEN"` \| `"API_KEY"` \| `"BASIC_AUTH"` \| `"NONE"`. |
| `credentials` | object | yes (when `type` needs one) | Shape depends on `type`: `BEARER_TOKEN` → `{"token": <ref>}`; `API_KEY` → `{"header_name": <string>, "api_key": <ref>}`; `BASIC_AUTH` → `{"username": <ref>, "password": <ref>}`. |

Every `<ref>` value must match `'env:<VAR_NAME>'` or `'secret:<scope>:<key>'` — a **literal secret value in the spec is always a validation error**, never accepted. `env:` reads an environment variable at dispatch time (e.g. a job-cluster/serverless environment secret); `secret:<scope>:<key>` resolves via `dbutils.secrets.get(scope, key)`.

### 2.4 `retry` object (`OTLP_CONSUMER` only)

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `max_attempts` | integer ≥ 1 | no | `3` | |
| `backoff_multiplier` | number > 1 | no | `2.0` | Exponential backoff, capped at 30s per attempt; a `Retry-After` response header overrides the computed delay when present. Retried status codes: `429, 500, 502, 503, 504`. |

### 2.5 Copy-pasteable example — all three real combinations

```json
{
  "observability": [
    {
      "id": "dest-post-update-volume",
      "type": "DATABRICKS_VOLUME",
      "mode": "triggered",
      "enabled": true,
      "destination_config": {
        "volume_path": "/Volumes/poc/observability/telemetry/",
        "file_format": "JSONL",
        "compression": "GZIP"
      }
    },
    {
      "id": "dest-continuous-otlp",
      "type": "OTLP_CONSUMER",
      "mode": "continuous",
      "enabled": true,
      "destination_config": {
        "endpoint": "https://otel-collector.example.com/v1/logs",
        "protocol": "OTLP_HTTP_JSON",
        "event_log_tables": [
          "observability.event_logs.orders_pipeline",
          "observability.event_logs.customers_pipeline"
        ]
      }
    },
    {
      "id": "dest-continuous-volume",
      "type": "DATABRICKS_VOLUME",
      "mode": "continuous",
      "enabled": true,
      "destination_config": {
        "volume_path": "/Volumes/poc/observability/streaming/",
        "event_log_tables": ["observability.event_logs.orders_pipeline"]
      }
    }
  ]
}
```

`dest-post-update-volume` is exported once per pipeline update by the triggered engine; `dest-continuous-otlp` and `dest-continuous-volume` are exported continuously by the streaming pipeline, and both name `observability.event_logs.orders_pipeline` — `resolve_event_log_tables` de-duplicates the union across destinations, so that table is still read only **once** into one `@dlt.view`, not once per destination that references it.

> [!NOTE]
> `mode` is nullable in the `observability_config` control table and every reader defaults an absent/`NULL` value to `"triggered"` (`row.get("mode") or "triggered"`). A control table provisioned before v1.3.0 — `01_setup` only ever runs `CREATE TABLE IF NOT EXISTS`, never a migration — needs no migration step, and every pre-v1.3.0 destination keeps being served by the triggered engine with zero configuration change. The one real **behavior change**: the triggered engine now dispatches only to destinations whose resolved `mode` is `"triggered"` (previously it dispatched to everything, because `mode` didn't exist). Since every pre-v1.3.0 row resolves to `"triggered"`, no existing destination is dropped by this change.

---

## 3. OpenTelemetry Event Mapping

The engine translates native Databricks Lakeflow event JSON into OpenTelemetry log and metric records:

| Lakeflow Event Log Field | OpenTelemetry Log Record Field | Description |
|---|---|---|
| `timestamp` | `Timestamp` (UNIX epoch nanoseconds) | Event occurrence timestamp. |
| `level` (`INFO`, `WARN`, `ERROR`) | `SeverityText` / `SeverityNumber` | Log severity level. |
| `message` | `Body.stringValue` | Human-readable event description. |
| *(record kind)* | `Attributes["event.name"]` | One of `flow_performance_summary`, `data_quality_expectation`, `pipeline_error` — identifies which curated record this is. |
| `details.flow_progress.status` | `Attributes["flow.status"]` | Flow status for the summary record. |
| `details.flow_progress.metrics.num_output_rows` | `Attributes["flow.num_output_rows"]` | Rows written. Companions: `flow.num_upserted_rows`, `flow.num_deleted_rows`, `flow.dropped_records`, `flow.duration_ms`, `flow.start_time_ms`, `flow.end_time_ms`, `flow.backlog_bytes`, `flow.backlog_files`. |
| `details.flow_progress.data_quality.expectations[]` | `Attributes["expectation.passed_records"]` | Per-expectation record. Companions: `expectation.name`, `expectation.dataset`, `expectation.failed_records`. |
| *(error events)* | `Attributes["error.event_type"]` | Per-error record. Companions: `error.level`, `error.fatal`, `error.class_name`, `error.stack_trace`. |
| *(run context)* | `Resource.Attributes["databricks.*"]` | `databricks.pipeline_id`, `databricks.job_id`, `databricks.task_run_id`, `databricks.dataflow_group_id`, `databricks.dataflow_id`, `databricks.step_id`, plus `service.name` / `deployment.environment`. |

> **Note — `origin.flow_name` is not emitted as an attribute.** It is captured into `FlowMetrics.flow_name` and used for keying/correlation, but no attribute carries it.

This mapping describes the **triggered** engine's `ResourceLogs` output only (`otel_payload_builder.py::build_resource_logs`). It does **not** describe the **continuous** engine, whose OTLP sink uses a different builder — `build_resource_logs_from_event_rows` — that emits one flat `databricks.event_log.{column}` attribute per raw event-log row column plus `service.name`: a wholesale row dump rather than this curated per-field mapping. The continuous Volume sink likewise writes raw, un-shaped event-log rows — see §6.2.

---

## 4. In-Pipeline Structured JSON Logging

For custom operational steps, use the framework's structured logger (`flowx.lakeflow_framework.observability.structured_logger`):

```python
from flowx.lakeflow_framework.observability.structured_logger import (
    log_flow_event,
    logged_operation,
)

# Example: Structured JSON log emission
# operation, flow_id and status are REQUIRED positional-or-keyword parameters.
# Anything else you pass is swept into **extra and merged into the JSON payload.
log_flow_event(
    operation="SCHEMA_DRIFT_DETECTED",
    flow_id="df_orders_bronze",
    status="WARN",
    records_read=12_000,
    records_written=12_000,
    new_columns=["loyalty_tier"],
    action="rescued",
)
```

---

## 5. Triggered-Mode Update Narrowing

`08_dlt_observability_engine.py`'s Extraction step reads the DLT `event_log(:pipeline_id)` table-valued function bounded by the upstream task run's own wall clock (`start_time_ms`/`end_time_ms`, from `task_context_resolver.py::resolve_task_context`). That window-only query was always correct for the common case, but a pipeline can legitimately run more than one update inside a single task's wall-clock window — a retry, or a manually-started update racing the scheduled one — and a pure timestamp filter would then export **both** updates' telemetry from a task that is only responsible for one.

`observability/event_log_extractor.py::extract_raw_events` gains a new trailing keyword argument:

```python
def extract_raw_events(
    spark: Any,
    pipeline_id: str,
    start_time_ms: int,
    end_time_ms: int,
    update_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    ...
```

- `update_ids=None` (the default, and what every pre-v1.3.0 caller passes implicitly) preserves the timestamp-window-only query exactly.
- When a list is supplied, the query is additionally narrowed with `origin.update_id IN (...)`, so the export contains only the update(s) this task actually produced.

The notebook resolves that list itself, by default, via `observability/event_log_extractor.py::resolve_update_ids_for_window`, which asks the Pipelines API which updates were *created* inside the task's window:

```python
update_ids = (
    resolve_update_ids_for_window(workspace_client, task_context.pipeline_id, task_context.start_time_ms, task_context.end_time_ms)
    if NARROW_TO_TASK_UPDATES
    else None
)
```

Two things make this safe to leave on by default:

1. **It is best-effort and never raises.** If the Pipelines API call fails, the SDK in use is too old to expose `pipelines.list_updates`, or zero updates are reported as created inside the window (more likely a lookup problem than a genuinely empty result), the function logs a `WARNING` and returns `None` — which `extract_raw_events` treats exactly like an explicit `update_ids=None`, i.e. today's full timestamp-window query. Narrowing can only ever *tighten* the export; it is never a prerequisite for one.
2. **An operator can disable it entirely.** The notebook's `narrow_to_task_updates` widget (dropdown `true`/`false`, default `"true"`) controls whether the lookup is attempted at all — set it to `"false"` to restore the unconditional pre-v1.3.0 timestamp-window-only behavior with no code change.

---

## 6. Continuous Mode

`06_event_log_otel_streaming_pipeline.py` is a genuinely `continuous: true` Lakeflow pipeline — a standing export observing *other* pipelines' event logs, not a bounded per-update job task. It has no upstream task to resolve a pipeline id from, so every continuous destination must instead be told, explicitly, which event-log tables to stream via `destination_config.event_log_tables`.

### 6.1 Where the event-log table list belongs, and why

**Canonical location: `destination_config.event_log_tables` on the `mode: "continuous"`
observability row** — part of the onboarding spec, upserted into `observability_config` like every
other flow. Not a pipeline setting.

That is a design decision, not a convention, and it follows from what a continuous export actually
is. Three reasons:

1. **The list belongs to the destination set, not to the pipeline.** One continuous pipeline fans a
   single streaming read out to N destinations, and two destinations covering different subsets of
   the estate is a normal shape — one team's collector taking three pipelines' event logs, another
   taking twelve. A pipeline-level list has exactly one value and cannot express that.
2. **It has to change without a redeploy.** Adding a newly-onboarded pipeline's event log to an
   existing export is an onboarding-spec change (`observability[]`), which is a control-table upsert
   the standing pipeline picks up on its next restart. As a pipeline `configuration:` key it would
   be a bundle deploy — and this repo's own operational rule is *never deploy while a pipeline is
   running*, which is precisely the state a continuous export is always in.
3. **One row, one place to look.** The mode, the destination type, the credentials, the retry policy
   and the source tables are all attributes of the same destination. Splitting the source tables out
   into a second system means two places to check when an export is missing data.

`dataflow.otel_streaming.event_log_tables` on the pipeline resource remains as a **bootstrap
fallback**, consulted only when the control-table lookup yields nothing. It exists for the deployment
that has not onboarded an observability row yet — including every pre-v1.3.0 one — and for standing
the streaming pipeline up before the control plane is populated. Two sources, one of them explicitly
subordinate: the control table wins whenever it has anything to say.

The two pipeline `configuration:` keys that **do** belong on the pipeline resource are
`dataflow.group.id` and `dataflow.control.catalog` — they are what let the pipeline find its own
rows, so they cannot themselves live in those rows. They are static per deployment, which is exactly
the category pipeline configuration is for (see §1.1.1).

### 6.1.1 Resolution order

The notebook tries two sources, in this order, and uses the first one that yields anything:

1. **`observability_config` rows with `mode == "continuous"`**, scoped to this pipeline's configured `dataflow.group.id`, read from `<dataflow.control.catalog>.config`. Their `destination_config.event_log_tables` arrays are unioned — de-duplicated, order-preserving — by `config_loader.py::resolve_event_log_tables`. This source requires **both** new pipeline `configuration:` keys, `dataflow.group.id` and `dataflow.control.catalog`, to be set; when either is absent this source is silently skipped (not an error) — a pre-v1.3.0 deployment legitimately has neither.
2. **`dataflow.otel_streaming.event_log_tables`** — the original mechanism, a JSON-encoded array set directly on the pipeline resource's own `configuration:` block (see `resources/observability/observability_otel_streaming_pipeline.yml`). This fallback is **permanent and fully supported**, not a deprecation path — a deployment that never adopts the two new configuration keys keeps working exactly as it did before v1.3.0.

If neither source yields a table, the pipeline fails at graph-definition time with this exact message:

```
No continuous observability destinations found in observability_config for dataflow_group_id='%s' and no
'dataflow.otel_streaming.event_log_tables' pipeline configuration was set -- there is nothing to stream.
Onboard a destination with mode: 'continuous' and destination_config.event_log_tables, or set the pipeline
configuration key.
```

A malformed value in source 2 (invalid JSON, or JSON that doesn't decode to a non-empty array) still raises — only *absence* falls through to the other source; a typo'd JSON array must not be silently treated as "nothing configured."

Each configured table becomes one `@dlt.view` (`spark.readStream.table(table_name)`, tagged with a `source_pipeline` column), and every view is unioned via `unionByName` (never positional `.union()`, and never a nonexistent `union_all`) into a single `unified_event_log` streaming table. A schema mismatch across source event-log tables surfaces as a clear `FrameworkConfigError` naming every configured view, rather than a bare Spark `AnalysisException`.

### 6.2 Two sinks, fed from the same stream

Both sinks below read `unified_event_log` — reading once and fanning out is exactly why `resolve_event_log_tables` de-duplicates the table union in the first place.

**OTLP sink** (unchanged from pre-v1.3.0) — gated by `dataflow.otel_streaming.otel_export_enabled`. When enabled, registers a custom `otel_streaming` Lakeflow sink Data Source and a `@dlt.append_flow` into it, exporting to `dataflow.otel_streaming.otel_endpoint`. Payloads here are OTel `ResourceLogs`-shaped, matching §3's mapping.

**Volume sink** (new in v1.3.0) — one native `dlt.create_sink(format="json", options={"path": <volume_path>})` per continuous `DATABRICKS_VOLUME` destination, each fed by its own `@dlt.append_flow` reading `unified_event_log`. This uses Lakeflow's own file sink rather than the triggered engine's `destination_dispatcher.dispatch_to_volume` (which opens a file with `open()` and writes one deterministically-named `<dataflow_group_id>_<task_run_id>` file — coherent only for a single bounded task run, and inapplicable to a stream with no task_run_id and no terminal moment to name a file after). Consequently:

> [!IMPORTANT]
> A continuous `DATABRICKS_VOLUME` destination writes **raw event-log JSON** — the native `unified_event_log` schema, `source_pipeline` column included — **not** the OTel `ResourceLogs` envelope §3 documents. That shaping exists to satisfy an OTLP collector's wire contract; a Volume archive meant to be read back with Spark is more useful with the native event-log columns intact than wrapped in an OTLP envelope. If you need OTel-shaped output from a Volume, there is no such destination as of v1.3.0 — only `OTLP_CONSUMER` produces `ResourceLogs`.

A continuous `DATABRICKS_VOLUME` destination missing `volume_path` is skipped with a `WARNING` (no sink registered for it) rather than failing the whole pipeline update — one misconfigured destination must never take down an always-on export serving the others.

### 6.3 Enabling it

Add both new pipeline `configuration:` keys to `resources/observability/observability_otel_streaming_pipeline.yml` (or your own copy of it) so the pipeline can reach `observability_config`:

```yaml
configuration:
  dataflow.group.id: dfg_orders_cdc
  dataflow.control.catalog: flowx
  # existing dataflow.otel_streaming.* keys remain a fully-supported fallback
```

Then onboard one or more `observability[]` destinations with `"mode": "continuous"` and a populated `destination_config.event_log_tables`, as shown in §2.5.

---

## 7. Telemetry Error Handling Matrix

| Failure Category | Likely Cause | Diagnostic Symptom | Remediation Action |
|---|---|---|---|
| **`task_wiring`** | Missing dependency on pipeline run task | `ObservabilityConfigError` from `task_context_resolver.py` ("... is not a pipeline_task run ..." / "... has no start_time/end_time yet ...") | Ensure `observability_export` task has `depends_on: run_pipeline_update`. |
| **`pipeline_config`** | Empty or invalid `observability[]` array | Validation error in onboarding | Run `validate_observability_config` tool to verify JSON structure. |
| **`destination_config`** | Volume path does not start with `/Volumes/` | Onboarding-time validator error (`...volume_path: must start with '/Volumes/'...`), or at runtime a non-raising `DispatchResult(error="destination_config.volume_path is required for DATABRICKS_VOLUME destinations.")` | Ensure `volume_path` starts with `/Volumes/<catalog>/<schema>/<volume>`. |
| **`credentials`** | Invalid or expired bearer token / API key | `401 Unauthorized` / `403 Forbidden` | Verify secret in Databricks CLI (`databricks secrets get-secret ...`). |
| **`destination_outage`** | OTLP collector network timeout | `HTTP 503` / `ConnectionTimeout` | Check collector health and increase `timeout_ms` / `retry.max_attempts`. |
| **`event_log_access`** | Insufficient UC permissions on event log | `PermissionDenied` on system table | Grant `SELECT` privileges on event log schema to the service principal. |
| **`mode_mismatch` (v1.3.0)** | The triggered engine's `obs_mode` widget was passed something other than `"triggered"` | Notebook raises `ObservabilityConfigError` before any API call: `The 'obs_mode' widget must be 'triggered' for this notebook -- continuous-mode destinations are served by notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py, which has a fundamentally different (always-on) lifecycle.` | Never override `obs_mode` on the triggered job — continuous destinations run only under the streaming pipeline. |
| **`missing_event_log_tables` (v1.3.0)** | A destination has `"mode": "continuous"` but no `destination_config.event_log_tables` | Onboarding validation error: `<path>.destination_config.event_log_tables: is required when mode is 'continuous' -- the continuous observability pipeline has no upstream task to resolve a pipeline from, so it must be told which event-log tables to stream` | Add a non-empty array of fully-qualified `catalog.schema.table` names to the destination's `destination_config`. |
| **`nothing_to_stream` (v1.3.0)** | The continuous pipeline found neither `observability_config` continuous rows for its `dataflow.group.id` nor a `dataflow.otel_streaming.event_log_tables` pipeline configuration | Pipeline fails at graph-definition time: `No continuous observability destinations found in observability_config for dataflow_group_id='%s' and no 'dataflow.otel_streaming.event_log_tables' pipeline configuration was set -- there is nothing to stream. Onboard a destination with mode: 'continuous' and destination_config.event_log_tables, or set the pipeline configuration key.` | Onboard at least one `mode: "continuous"` destination with `event_log_tables`, or set the `dataflow.otel_streaming.event_log_tables` configuration key directly on the pipeline resource. |

---

## 8. Export-out telemetry vs. store-and-query observability

Everything above this section is **export-out** telemetry: the framework measures itself and then
ships the measurements *off the platform* — OTel `ResourceLogs` to an OTLP collector, JSONL/GZIP
files to a Unity Catalog Volume, structured JSON into the driver log. That is the right shape for
alerting, for paging, and for a customer's existing observability estate.

It is the wrong shape for a **question**, because none of it lands in a queryable Delta table. A
per-flow `num_output_rows` that left as an OTLP attribute cannot be `SUM`ed; an expectation's
`failed_records` written into a Volume file has to be read back with Spark before anyone can ask
which rule fails most often; and **cost is not in the export at all**.

That second half is [`17_framework_observability_and_genie.md`](17_framework_observability_and_genie.md):
11 views in `<catalog>.observability` joining the Metaflow control tables to `system.lakeflow`,
`system.billing` and `system.access`, consumed by an AI/BI dashboard, a Genie space and a
documentation job.

| | **Doc 08** — this page | **Doc 17** — observability semantic layer |
|---|---|---|
| Direction | **Out** of the platform | **Stays** in the platform |
| Mechanism | `observability[]` destinations: OTLP HTTP, Volume files, driver logs | Views over the Databricks system tables + UC event logs |
| Grain | Per event, per pipeline update | Per group, per update, per flow, per rule, per day |
| Consumers | Dynatrace / Datadog / Splunk / an OTel collector | AI/BI dashboard, Genie space, Markdown design docs, ad-hoc SQL |
| Configured by | The onboarding spec (§2 above) | Nothing — `01_setup` provisions it; **no spec attribute** |
| Cost data | Absent | First-class (`v_dataflow_cost`, and an `AI_FORECAST` page) |
| Typical question | "Page me when a pipeline fails" | "What did this group cost, and is its DQ passing?" |

The two are complementary and neither replaces the other. A customer running Dynatrace still wants
the OTLP export configured here; they *also* want to answer a question in SQL without opening a
Volume. The one genuine overlap is the source data — both read the Lakeflow event log — but they
read it for different reasons: this page to reshape and forward individual events, doc 17 to
aggregate them into `v_flow_metrics` and `v_dq_results`. Doc 17's views need the event log
**published to Unity Catalog**; this page's triggered engine reads it through the
`event_log(:pipeline_id)` table-valued function instead, so a pipeline can serve one and not the
other.

---

**See also:** [`docs/00_master_reference_index.md`](00_master_reference_index.md) §9 (Observability Config Schema) for the **control-table** column names (`destination_id`, `destination_type`, `auth_config_json`, `retry_config_json`) — note these are the persisted column names, which deliberately differ from the **onboarding-spec** field names used in §2 above (`id`, `type`, `auth`, `retry`, `mode`), [`docs/11_hashing_and_determinism.md`](11_hashing_and_determinism.md) if you are correlating observability telemetry with `__framework_hash_key`/`__framework_hash_value` drift surfaced by reconciliation, and [`docs/17_framework_observability_and_genie.md`](17_framework_observability_and_genie.md) for the queryable semantic layer, the AI/BI dashboard, the Genie space and the documentation generator.
