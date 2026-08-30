# Test Pipeline: Continuous Event-Log → OTel Streaming Export

> See also: [Documentation index](README.md), [metaflow_testing/TESTING_PLAN.md](../metaflow_testing/TESTING_PLAN.md) (Module 9: Observability & Telemetry Framework).

## Purpose

Prove the new **Design 1** capability: a genuinely continuous Lakeflow Declarative Pipeline that
unions N *other* pipelines' published event-log tables into one streaming table (tagged with
`source_pipeline`) and exports it continuously to an OpenTelemetry OTLP/HTTP endpoint via a
custom `dlt.create_sink`/`@dlt.append_flow` pair — gated on/off by config, never touching the
pre-existing batch observability engine (`notebooks/08_observability/08_dlt_observability_engine.py`).

This is new infrastructure, not a metadata-driven onboarded flow — there is no onboarding spec
for this scenario; it is configured entirely through its own pipeline resource's
`configuration:` block.

## Responsibilities

* Read every table listed in `dataflow.otel_streaming.event_log_tables` (a JSON array of
  fully-qualified Unity Catalog table names) as a Structured Streaming source.
* Tag every row with `source_pipeline` (the source table's own fully-qualified name).
* Materialize the tagged union as one real streaming table, `unified_event_log`.
* When `dataflow.otel_streaming.otel_export_enabled` is `"true"`: register a custom
  `otel_streaming` sink Data Source and stream `unified_event_log` into it via
  `@dlt.append_flow`, which POSTs one OTLP `ResourceLogs` JSON payload per micro-batch to
  `dataflow.otel_streaming.otel_endpoint`. When `"false"` (the checked-in default): the sink/flow
  are never created at all — `unified_event_log` still materializes, but nothing is exported.

## Inputs

* **This scenario has no ingestion/onboarding fixtures of its own.** It reads whichever tables
  you list in `event_log_tables` — normally the *published event log tables* of other pipelines
  already deployed in this project (e.g. `metaflow_test_002_zerobus_pipeline`,
  `metaflow_test_003_autoload_recon_pipeline`). A source pipeline only has a queryable event-log
  **table** (as opposed to only the `event_log(pipeline_id)` TVF every pipeline already has) once
  you explicitly turn on Pipeline settings → Advanced → "Event log" → a `catalog.schema.table`
  destination for it — this is a one-time manual step per source pipeline, not something this
  new pipeline or its job can do for you.
* `resources/observability_otel_streaming_pipeline.yml`'s `configuration:` block ships with two
  **placeholder** table names (`observability.event_logs.pipeline_1`/`pipeline_2`) — replace
  these with your real published event-log table names before deploying if you want a live run;
  left as-is, `databricks bundle deploy` still succeeds and the pipeline starts, it just streams
  from tables that don't exist yet (which will fail at run time, not at deploy time — this is a
  config placeholder, not a working default).

## Outputs

* `{catalog}.observability_streaming.unified_event_log` — the unified, `source_pipeline`-tagged
  streaming table. Always materializes, regardless of `otel_export_enabled`.
* **Output file / external artifact**: when export is enabled, one OTLP `ResourceLogs` JSON HTTP
  POST body per micro-batch, sent to `otel_endpoint` — this is the "file" this scenario produces,
  except it's a request body delivered to an external collector, not a file written to a Volume
  (unlike the batch observability engine's `.jsonl.gz` Volume export — see
  `docs/25_dlt_observability_module.md`). There is no local artifact to inspect after a run
  beyond the collector's own ingestion record, since this design intentionally has no Volume
  fallback (that's the batch engine's job, untouched by this one).

## Configuration

`resources/observability_otel_streaming_pipeline.yml`:

```yaml
configuration:
  dataflow.otel_streaming.event_log_tables: '["observability.event_logs.pipeline_1", "observability.event_logs.pipeline_2"]'
  dataflow.otel_streaming.otel_endpoint: https://otel-collector.example.com/v1/logs
  dataflow.otel_streaming.otel_export_enabled: "false"
```

`continuous: true` on the pipeline resource itself is what makes this genuinely always-on —
every other pipeline resource in this project sets `continuous: false` (triggered/on-demand).

## Main execution flow

There is no job wrapper for this scenario (see the resource YAML's own header comment for why —
a `continuous: true` pipeline's lifecycle is Databricks' to manage once started, not a job's):

```bash
databricks bundle deploy --target dev
databricks bundle run observability_otel_streaming_pipeline --target dev
```

Starting it launches an update that keeps running indefinitely — new files/rows arriving in any
configured event-log table get picked up and (if enabled) exported continuously, with no
`--full-refresh`/re-trigger needed between arrivals. Stop it via the Pipelines UI or
`databricks pipelines stop`.

## Expected results — simulation notes (this pass was verified structurally, not live)

This environment has no live external OTLP collector and no already-published event-log source
tables to point at, so this scenario was **not run end-to-end against a real workspace** as part
of building it. What was actually verified:

* `ast.parse`/import checks on every new/changed file (`otel_streaming_sink.py`,
  `otel_payload_builder.py`'s new function, the new engine notebook) — all pass.
* An offline unit test of `build_resource_logs_from_event_rows` against synthetic row dicts,
  confirming correct per-`source_pipeline` grouping, timestamp conversion, and graceful handling
  of a row with a null `timestamp`/`level`.
* An offline end-to-end wiring test: synthetic rows → `build_resource_logs_from_event_rows` →
  a minimal `DestinationConfig` → `dispatch_to_otlp` with a fake `post_fn`, confirming a
  `SUCCESS` result and that the (deliberately-stubbed, should-never-fire) secret resolver is
  never actually invoked.
* `databricks bundle validate` passes with this pipeline resource included.

**Not verified**: a real streaming run against live published event-log tables; whether the
restricted "python streaming data source runtime" worker process that runs `commit()` genuinely
permits outbound HTTPS via `requests` (a documented, reasoned assumption in
`otel_streaming_sink.py`'s module docstring, not a re-confirmed fact — the only restriction this
project has hit live in that runtime is against spawning a `dbutils` gateway subprocess, not
network calls in general). To actually validate this scenario: publish at least one real
pipeline's event log to a table, point `event_log_tables` at it, set `otel_export_enabled: "true"`
and a real reachable `otel_endpoint` (or a local test HTTP listener), deploy, run, and watch
`unified_event_log` populate plus the sink's own driver logs for `"committed microbatch"` lines.
