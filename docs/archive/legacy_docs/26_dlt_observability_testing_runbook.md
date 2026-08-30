# DLT Observability: Testing & Validation Runbook

> See also: [README.md](README.md) — the full Metaflow documentation set, and
> [25_dlt_observability_module.md](25_dlt_observability_module.md) for the architecture this
> runbook exercises.

## Test strategy

Following this repo's `tests/{unit,integration}/` split (`unit` = pure Python, no Spark
session; `integration` = live Databricks Connect — see `agent_skills/SKILL.md` §15): every
module in `observability/` is built with a **pure/impure split** specifically so its
business-logic branches are unit-testable with synthetic data, and only the genuinely
Spark/API/network-touching edges need a live workspace:

| Module | Pure (unit-tested, no Spark/network) | Impure (needs live Spark / Jobs / Pipelines API / HTTP) |
|---|---|---|
| `config_loader.py` | `parse_config_rows` | `load_destination_configs` |
| `onboarding/spec_validator.py::_validate_observability_destinations` | Everything (same no-Spark unit tests as every other flow validator, `tests/unit/test_spec_validator.py`) | — |
| `onboarding/metadata_upsert.py::upsert_observability_config` | — | The whole function (Delta `MERGE`, same as every other `upsert_*` function — no direct unit test, exercised via onboarding integration tests) |
| `task_context_resolver.py` | — (Jobs API call mocked via a fake `WorkspaceClient` in unit tests) | `resolve_task_context` |
| `event_log_extractor.py` | `aggregate_flow_metrics` | `extract_raw_events`, `resolve_dataflow_group_id` |
| `otel_payload_builder.py` | Everything — the whole module | — |
| `destination_dispatcher.py` | `resolve_credential`, `build_auth_headers`, `compress_payload`, `compute_backoff_delay_seconds`, `merge_resource_attributes`; `dispatch_to_otlp`/`dispatch_all` with an injected fake `post_fn` | `dispatch_to_otlp`/`dispatch_to_volume` against a real endpoint/Volume |
| `agent_tools.py` | Everything — the whole module | — |

120+ unit tests currently cover this module (`tests/unit/test_observability_*.py`, plus the
`observability[]` cases inside `tests/unit/test_spec_validator.py`); run them with:

```bash
uv run pytest tests/unit/test_observability_*.py -v
```

> `tests/conftest.py` eagerly opens a live `DatabricksSession` at collection time (its
> `pytest_configure` hook — see that file's own docstring), which every test in `tests/`
> inherits regardless of whether the test itself needs Spark. If you're iterating locally
> without wanting to authenticate against a workspace yet, the observability unit tests (being
> genuinely Spark-free) can be run bypassing that conftest with
> `--confcutdir=tests/unit`; drop that flag for a normal full-suite run once a profile is
> configured (`databricks auth profiles` — see the `databricks-core` skill).

## Test matrix

### Happy paths

| Scenario | Covered by |
|---|---|
| Single update, single flow, `COMPLETED` status, non-zero output rows | `test_observability_event_log_extractor.py::test_single_update_single_flow_lifecycle` |
| DQ expectations with both passing and failing records | `test_observability_event_log_extractor.py::test_dq_expectations_captured_per_flow`, `test_observability_otel_payload_builder.py::test_expectations_and_errors_become_additional_log_records` |
| Full OTel compliance (every mandatory field present) | `test_observability_otel_payload_builder.py::TestValidateResourceLogs` |
| Volume dispatch, uncompressed and gzip | `test_observability_destination_dispatcher.py::TestDispatchToVolume` |
| OTLP dispatch, first-attempt `200 OK` | `test_observability_destination_dispatcher.py::test_success_on_first_attempt` |
| Global (`"*"`) destination fallback + pipeline-specific override | `test_observability_config_loader.py::TestParseConfigRows` |
| End-to-end config generation → validation round trip | `test_observability_agent_tools.py::test_generated_config_is_valid_against_the_schema` |

### Edge cases

| Scenario | Why it matters | Covered by |
|---|---|---|
| **Zero event log rows** in the resolved window | A silently-empty run and a genuinely-broken extraction must not look identical downstream. | `test_observability_event_log_extractor.py::test_zero_events_produces_empty_telemetry`, `test_observability_otel_payload_builder.py::test_zero_flows_emits_single_diagnostic_resource_logs_entry` |
| **Continuous / multiple updates** in one window | Row counts reset per update; a flow spanning 2 updates must produce 2 independent `FlowMetrics`, not one double-counted entry. | `test_observability_event_log_extractor.py::test_multiple_updates_same_flow_produce_separate_flow_metrics` |
| **Upstream task not yet finished** (`end_time` missing) | The observability task must never run ahead of `run_pipeline_update` — this is a wiring bug (missing `depends_on`), not a retryable condition. | `test_observability_task_context_resolver.py::test_missing_end_time_raises_wiring_hint` |
| **Wrong upstream task type** (not a `pipeline_task`) | Catches a misconfigured `run_pipeline_update_run_id` widget pointed at the wrong task. | `test_observability_task_context_resolver.py::test_non_pipeline_task_run_raises` |
| **Pipeline missing `dataflow.group.id` conf** | A pipeline never deployed through this framework's engine notebook has no group identity to label telemetry with. | `test_observability_event_log_extractor.py::test_missing_configuration_key_raises` |
| **Malformed `details` JSON** on one event | One corrupt row must not abort aggregation of the rest of the window. | `test_observability_event_log_extractor.py::test_malformed_details_json_does_not_raise` |
| **Partial failure** — one flow fails, others succeed | Errors must attach only to the failing flow, never bleed into unrelated flows' `ResourceLogs`. | `test_observability_event_log_extractor.py::test_partial_failure_run_flags_error_on_the_failing_flow_only` |
| **Pipeline-level error with no `flow_id`** | Must not be silently dropped, and must not be misattached to an arbitrary flow. | `test_observability_event_log_extractor.py::test_pipeline_level_error_with_no_flow_id_is_not_attached_to_any_flow` |

### Failure modes

| Scenario | Covered by |
|---|---|
| `429` rate limit → retried with backoff → eventual success | `test_observability_destination_dispatcher.py::test_retries_on_429_then_succeeds` |
| `500`/`5xx` persisted across every retry attempt | `test_observability_destination_dispatcher.py::test_exhausts_retries_on_persistent_500` |
| Non-retryable `4xx` (e.g. `400`) — fails immediately, no wasted retries | `test_observability_destination_dispatcher.py::test_non_retryable_4xx_fails_immediately` |
| Network exception (e.g. connection reset) mid-attempt, then recovers | `test_observability_destination_dispatcher.py::test_network_exception_retried_then_succeeds` |
| Missing/invalid credential reference (`env:`/`secret:`) | `test_observability_destination_dispatcher.py::TestResolveCredential`, `TestBuildAuthHeaders` |
| One destination fails, others still succeed | `test_observability_destination_dispatcher.py::TestDispatchAll::test_one_destination_failure_does_not_block_others` |
| **Every** destination fails → `ObservabilityDispatchError` | `test_observability_destination_dispatcher.py::TestDispatchAll::test_all_destinations_failing_raises_dispatch_error` |
| Zero destinations configured/enabled | `test_observability_destination_dispatcher.py::test_raises_when_no_destinations` |
| Malformed `observability_config` JSON columns | `test_observability_config_loader.py::test_malformed_json_raises` |

## End-to-end testing inside Databricks Workflows

1. **Deploy.** `databricks bundle deploy --target dev --profile <your-profile>` — this builds
   the wheel (picking up `observability/` and the three new deps in `pyproject.toml`) and
   registers `dlt_observability_job.yml`'s job + reuses the existing
   `metaflow_test_100_zipcsv_pipeline`.
2. **Configure at least one destination.** There is no separate seed step — edit the
   `"observability": [...]` array directly inside
   `metaflow_testing/100_zipcsv_onbaording.json` (the onboarding spec `dlt_observability_job.yml`'s
   `onboard_100` task onboards) with a real Volume path / OTLP endpoint reachable from your
   workspace. For a first smoke test with zero external dependencies, keep only the
   `DATABRICKS_VOLUME` entry — it needs no credentials and no reachable external endpoint.
   Validate the edited array first with `observability/agent_tools.py::validate_observability_config`.
3. **Run the full job.** `databricks bundle run dlt_observability_job --target dev --profile <your-profile>`
   — this chains `setup_control_tables → onboard_100 → run_pipeline_update →
   observability_export` in one job run; `onboard_100` upserts both the ingestion flows and the
   `observability[]` array from the same spec in the same step.
4. **Inspect the `observability_export` task's logs** in the Jobs UI (or
   `databricks jobs get-run-output --run-id <task_run_id> --profile <your-profile>`) for the
   five phase log lines described in [docs/25 §Step-by-step logging](25_dlt_observability_module.md#step-by-step-logging).
5. **Inspect the dispatched payload** — see [Sample payload inspection queries](#sample-payload-inspection-queries) below.
6. **Exercise the continuous-mode / multiple-update edge case**: set the pipeline resource's
   `continuous: true` temporarily (or trigger two manual updates before the observability task
   runs), then confirm the extraction log line reports more than one distinct `update_id` and
   that flows repeated across updates appear as separate `ResourceLogs` entries (grep the
   dispatched JSONL/HTTP payload for `pipeline.update_id`).
7. **Exercise the zero-event-log edge case**: point `run_pipeline_update_run_id` at a task run
   whose pipeline update genuinely produced no `flow_progress` events in its window (e.g. a
   pipeline with `continuous: false` and no active flows) and confirm exactly one
   `ResourceLogs` entry with the `"no_flow_events_in_window"` diagnostic `LogRecord` is
   dispatched — not zero, not an exception.
8. **Exercise a failure destination on purpose** — add an `OTLP_CONSUMER` destination with an
   unreachable `endpoint` or a `secret:` reference to a nonexistent scope, re-onboard, re-run,
   and confirm
   (a) the task fails with `ObservabilityDispatchError` when `fail_task_on_dispatch_error=true`
   (the default), and (b) the error message matches one of the
   [Error Handling Matrix](25_dlt_observability_module.md#error-handling-matrix--troubleshooting)
   rows — feed it to `observability/agent_tools.py::diagnose_pipeline_telemetry_failures` to
   confirm the match.

## Validation checklist

- [ ] `observability_config` table exists in `<catalog>.config` with the expected columns
      (`SHOW COLUMNS IN <catalog>.config.observability_config`).
- [ ] At least one `enabled = true` row resolves for the target `dataflow_group_id` (or `"*"`).
- [ ] `observability_export` task's `state.result_state` is `SUCCESS` for a normal run.
- [ ] Every dispatched `LogRecord` has non-empty `timeUnixNano`, an integer `severityNumber`,
      a `body`, and an `attributes` array (`otel_payload_builder.py::validate_resource_logs`
      already asserts this inline before dispatch — this checkbox is for validating the
      *delivered* payload independently, past any destination-side transformation).
- [ ] `databricks.dataflow_group_id` is identical across every `ResourceLogs` entry from the
      same run; `databricks.dataflow_id`/`pipeline.update_id` vary per flow/update as expected.
- [ ] No credential value (only `env:`/`secret:` *references*) appears anywhere in the task's
      driver logs.
- [ ] Re-onboarding the same spec (CREATE/UPDATE) does not create duplicate `observability_config`
      rows (idempotent `MERGE` on a `config_id` deterministically derived from
      `(dataflow_group_id, destination_id)`).

## Sample payload inspection queries

### Databricks Volume destination (JSONL)

List written files for a run:

```python
# Databricks notebook / Python cell
dbutils.fs.ls(f"/Volumes/poc/observability/app_logs/{dataflow_group_id}/")
```

Read and pretty-print one payload line:

```python
import gzip, json

path = "/Volumes/poc/observability/app_logs/dfg_zip_csv_dataload/2026-08-28/dfg_zip_csv_dataload_12345.jsonl.gz"
with gzip.open(path, "rt", encoding="utf-8") as f:
    for line in f:
        payload = json.loads(line)
        resource_attrs = {a["key"]: a["value"] for a in payload["resourceLogs"][0]["resource"]["attributes"]}
        print(resource_attrs.get("databricks.dataflow_id"), resource_attrs.get("pipeline.update_id"))
```

Query every dispatched flow's status directly with Spark SQL, without leaving the lakehouse
(treats the JSONL Volume path as an ad hoc JSON source):

```sql
SELECT
  filter(resourceLogs[0].resource.attributes, a -> a.key = 'databricks.dataflow_id')[0].value.stringValue AS dataflow_id,
  filter(resourceLogs[0].scopeLogs[0].logRecords, r ->
    filter(r.attributes, a -> a.key = 'event.name')[0].value.stringValue = 'flow_performance_summary'
  )[0].severityText AS severity
FROM json.`/Volumes/poc/observability/app_logs/dfg_zip_csv_dataload/*/*.jsonl`
```

> Adjust the glob/compression handling if `compression: GZIP` was used — Spark's `json.`
> data source auto-detects `.gz` by file extension when reading a directory of mixed files.

### OTLP consumer destination

For a self-hosted OTel Collector configured with a `file` or `debug` exporter, tail its own
output; for a SaaS backend (Datadog, etc.), use that backend's own log explorer filtered on
`service.name:dlt-observability` (or whatever `service_name` was set to) and
`databricks.dataflow_group_id:<your group id>`.

To confirm a payload is being **sent** (independent of the receiving side accepting it),
temporarily point an `OTLP_CONSUMER` destination's `endpoint` at
[https://webhook.site](https://webhook.site) (or a local `nc -l`/`python -m http.server`
listener) and inspect the raw POST body — the top-level shape is exactly
`{"resourceLogs": [...]}`, matching `otel_payload_builder.py::to_export_request`.

## Mock integration tests (no live workspace/network required)

`tests/unit/test_observability_destination_dispatcher.py` already covers `200`/`429`-then-retry/
`500`-exhausted HTTP responses and Volume writes to a local `tmp_path`, entirely via
`unittest`-style dependency injection (`post_fn`, `sleep_fn`) — no real network call or Unity
Catalog Volume is touched. If you need to exercise the same scenarios interactively:

```python
from NextGen_Metadata_Framework.lakeflow_framework.observability.destination_dispatcher import dispatch_to_otlp
from NextGen_Metadata_Framework.lakeflow_framework.observability.config_loader import DestinationConfig

class _FakeResponse:
    def __init__(self, status_code): self.status_code, self.text, self.headers = status_code, "", {}

destination = DestinationConfig(
    config_id="c1", dataflow_group_id="*", destination_id="d1", destination_type="OTLP_CONSUMER",
    destination_config={"endpoint": "https://example.com"}, retry_config={"max_attempts": 3},
)
result = dispatch_to_otlp(
    [...],  # a ResourceLogs list, e.g. from otel_payload_builder.build_resource_logs
    destination,
    secret_resolver=lambda scope, key: "unused",
    post_fn=lambda *a, **k: _FakeResponse(429),
    sleep_fn=lambda s: None,  # skip real sleeps
)
print(result.status, result.attempts, result.http_status_code)
```
