# DLT Observability: Onboarding Template & Attribute Reference

> See also: [README.md](README.md) — the full FlowX documentation set, and
> [25_dlt_observability_module.md](25_dlt_observability_module.md) for the architecture these
> attributes configure.

## Ready-to-use onboarding template

**There is no separate observability config file.** The `observability[]` array lives directly
inside the standard "kitchen sink" onboarding template — same JSON/YAML parity convention as
every other section (see [09_onboarding_yaml_json.md](09_onboarding_yaml_json.md)):

- [`onboarding_templates/pipeline_onboarding_template.json`](../onboarding_templates/pipeline_onboarding_template.json)
- [`onboarding_templates/pipeline_onboarding_template.yaml`](../onboarding_templates/pipeline_onboarding_template.yaml)

Both carry a two-destination `observability` example (a `DATABRICKS_VOLUME` destination and a
generic OTel Collector `OTLP_CONSUMER` destination with `BEARER_TOKEN` auth) alongside their
`ingestion_flows`/`transformation_flows`/`reconciliation_flows` sections. Copy the template,
edit its `observability[]` array (and everything else you need) in place, then onboard it the
same way as any other spec:

```bash
databricks bundle run onboarding_job --target dev \
  --params spec_file_path=<path-to-your-edited-spec>.json,catalog=poc,env=dev,action_type=CREATE
```

`action_type=VALIDATE_ONLY` runs the exact same validation (`onboarding/spec_validator.py::
validate_spec`, including `observability[]`) without upserting anything — use it to check a
spec before committing to `CREATE`/`UPDATE`. For validating just an `observability[]` fragment
in isolation (e.g. from an AI agent generating one before merging it into a full spec), use
`observability/agent_tools.py::validate_observability_config` instead.

The `observability` property (and its `$defs`) live in
[`onboarding_templates/onboarding_spec.schema.json`](../onboarding_templates/onboarding_spec.schema.json)
— the same schema every other section of the onboarding spec is validated against, both by
`onboarding/spec_validator.py` at onboarding time and by the Genie/MCP-callable
`preflight_check_onboarding_spec` Unity Catalog function (see
[01_control_metadata_schema.md](01_control_metadata_schema.md)).

## Complete attribute dictionary

### Top level (`{"observability": [...]}`)

| Attribute | Type | Required | Allowed / possible values | Default | Example |
|---|---|---|---|---|---|
| `observability` | array of destination objects | **Yes** | 1 or more entries | — | see templates above |

### Destination object (each entry in `observability[]`)

| Attribute | Type | Required | Allowed / possible values | Default | Example |
|---|---|---|---|---|---|
| `id` | string | **Yes** | any non-empty string, unique within the file | — | `"dest-dbx-prod-volume"` |
| `enabled` | boolean | No | `true` / `false` | `true` | `false` (temporarily disable without deleting the row) |
| `type` | string | **Yes** | `"DATABRICKS_VOLUME"`, `"OTLP_CONSUMER"` | — | `"OTLP_CONSUMER"` |
| `destination_config` | object | **Yes** | shape depends on `type` — see below | — | — |
| `auth` | object | No (`OTLP_CONSUMER` only; ignored/invalid for `DATABRICKS_VOLUME`) | see [Authentication](#authentication-strategies--credential-sources) below | none (`{"type": "NONE"}` behavior) | — |
| `retry` | object | No (`OTLP_CONSUMER` only) | see [Retry & backoff parameters](#retry--backoff-parameters-retry-otlp_consumer-only) below | `{max_attempts: 3, backoff_multiplier: 2.0}` | `{"max_attempts": 5}` |
| `timeout_ms` | integer | No (`OTLP_CONSUMER` only) | any positive integer | `5000` | `5000` |

### `destination_config` — `type: "DATABRICKS_VOLUME"`

| Attribute | Type | Required | Allowed / possible values | Default | Example |
|---|---|---|---|---|---|
| `volume_path` | string | **Yes** | must start with `/Volumes/` | — | `"/Volumes/prod_catalog/observability/app_logs/"` |
| `compression` | string | No | `"GZIP"`, `"gzip"`, `"none"`, `""` (case-insensitive; `""`/omitted/`"none"` all mean uncompressed) | `"none"` | `"GZIP"` |
| `file_format` | string | No | `"JSONL"`, `"JSON"` | `"JSONL"` | `"JSONL"` |

No `auth` is read for this destination type — access is governed entirely by the job's run-as
identity's native Unity Catalog Volume permissions.

### `destination_config` — `type: "OTLP_CONSUMER"`

| Attribute | Type | Required | Allowed / possible values | Default | Example |
|---|---|---|---|---|---|
| `endpoint` | string | **Yes** | a full `http://`/`https://` URL | — | `"https://http-intake.logs.datadoghq.com/api/v2/logs"` |
| `protocol` | string | No | `"OTLP_HTTP_JSON"` (implemented), `"OTLP_HTTP_PROTO"`, `"OTLP_GRPC"` (schema-valid, **not yet implemented** by `destination_dispatcher.py` — see [docs/25 §Extending](25_dlt_observability_module.md#extending-this-module)) | `"OTLP_HTTP_JSON"` | `"OTLP_HTTP_JSON"` |
| `compression` | string | No | same as `DATABRICKS_VOLUME` above | `"none"` | `"gzip"` |
| `resource_attributes` | object of string→string | No | any custom key-value pairs; merged over the canonical framework-built attributes immediately before dispatch (this destination's values win on key collision) | `{}` | `{"service.name": "dlt-order-pipeline", "deployment.environment": "production", "ddsource": "databricks-dlt"}` |

### Authentication strategies & credential sources

| `auth.type` | Required `auth.credentials` keys | Resulting HTTP header |
|---|---|---|
| `"NONE"` (or `auth` omitted entirely) | — | none |
| `"BEARER_TOKEN"` | `token` | `Authorization: Bearer <resolved token>` |
| `"API_KEY"` | `header_name`, `api_key` | `<header_name>: <resolved api_key>` |
| `"BASIC_AUTH"` | `username`, `password` | `Authorization: Basic <base64(username:password)>` |

**Every credential value must be a reference, never a literal secret.** Two forms, both
enforced by `onboarding_spec.schema.json`'s `observabilityCredentialRef` pattern at onboarding
time (`onboarding/spec_validator.py::check_credential_ref`), and rejected again at dispatch
time as defense-in-depth for any row written some other way
(`ObservabilityConfigError: ... literal secrets are not allowed`):

| Form | Resolution | Example | Notes |
|---|---|---|---|
| `env:<VAR_NAME>` | `os.environ["<VAR_NAME>"]` | `"env:DD_API_KEY"` | The variable must be injected into the job cluster's/serverless environment's environment variables — typically itself backed by a secret at the compute layer, not stored as a literal in `observability_config`. |
| `secret:<scope>:<key>` | `dbutils.secrets.get(scope="<scope>", key="<key>")` | `"secret:my_scope:dd_api_key"` | A **classic** Databricks secret scope/key (not this framework's usual Unity Catalog 3-level secret dict shape — see the note below). |

> **Why this differs from the rest of FlowX's secret convention.** Every other secret
> reference in this framework (`crypto/secrets.py`) is a 3-level Unity Catalog secret dict,
> `{"secret_catalog": ..., "secret_schema": ..., "secret_key": ...}`. This module's
> `auth.credentials` values are short, single-line strings instead (`env:X` / `secret:scope:key`)
> because they need to fit inside a compact per-field JSON value rather than a nested object,
> and because `env:` — the recommended default — has no Unity Catalog equivalent at all. Prefer
> `env:` in production; `secret:` is provided for a classic workspace-scope secret an
> organization already has provisioned.

### Retry & backoff parameters (`retry`, `OTLP_CONSUMER` only)

| Attribute | Type | Required | Allowed / possible values | Default | Example |
|---|---|---|---|---|---|
| `max_attempts` | integer | No | 1–10 | `3` | `5` |
| `backoff_multiplier` | number | No | any number `> 1` | `2.0` | `3.0` |
| `timeout_ms` (sibling of `retry`, not inside it) | integer | No | any positive integer | `5000` | `5000` |

Retry only triggers on HTTP `429`/`500`/`502`/`503`/`504` or a network-level exception (DNS
failure, connection reset, timeout) — any other 4xx status fails immediately with no retry.
Delay before the next attempt = `min(30, backoff_multiplier^(attempt-1))` seconds, except a
`429` response's `Retry-After` header (seconds) takes priority over the computed value when
present.

### Custom resource attribute mappings

`destination_config.resource_attributes` (OTLP_CONSUMER only) accepts **any** string key —
common ones an operator typically sets:

| Key | Purpose | Example |
|---|---|---|
| `service.name` | OTel-standard service identity; most backends group/alert by this. | `"dlt-order-pipeline"` |
| `deployment.environment` | OTel-standard environment tag. | `"production"` |
| `ddsource` | Datadog-specific log-source tag (unrecognized by non-Datadog backends, harmless to include). | `"databricks-dlt"` |
| any other custom key | Passed through verbatim as an OTel resource attribute. | `"team": "data-platform"` |

These **override** (not merge under) the framework-canonical `service.name`/
`deployment.environment` values for the same key — see
[docs/25's Resource attributes table](25_dlt_observability_module.md#resource-attributes-one-resource-per-flow)
for the full precedence chain and every framework-populated attribute
(`databricks.job_id`, `databricks.pipeline_id`, `databricks.dataflow_group_id`,
`databricks.dataflow_id`/`step_id`, `pipeline.update_id`, `pipeline.config.*`).

## `observability_config` control table columns

For reference, the flat Delta table shape `onboarding/metadata_upsert.py::
upsert_observability_config` translates `observability[]` into (full DDL:
`control_plane/ddl_definitions.py::get_observability_config_ddl`):

| Column | Type | Notes |
|---|---|---|
| `config_id` | `STRING` (PK) | Deterministically derived from `(dataflow_group_id, destination_id)` — re-onboarding the same spec is idempotent, never creates duplicates. |
| `dataflow_group_id` | `STRING` | The onboarding spec's own top-level `dataflow_group_id`, or literal `"*"` for the global fallback (same scoping as every other control table). |
| `destination_id` | `STRING` | From `observability[].id`. |
| `enabled` | `BOOLEAN` | From `observability[].enabled`. |
| `destination_type` | `STRING` | From `observability[].type`. |
| `destination_config_json` | `STRING` | JSON-serialized `observability[].destination_config`. |
| `auth_config_json` | `STRING`, nullable | JSON-serialized `observability[].auth`; `NULL` when omitted. |
| `retry_config_json` | `STRING`, nullable | JSON-serialized `observability[].retry` merged with `timeout_ms`; `NULL` when both omitted. |
| `created_at` / `updated_at` | `TIMESTAMP` | Set by `upsert_observability_config`'s `MERGE`. |

## Onboarding a new pipeline — step by step

1. Start from `pipeline_onboarding_template.json`/`.yaml` (or an existing spec) and add/edit its
   `observability[]` array with your destination(s)' `id`, `type`, and `destination_config`. Use
   `dataflow_group_id: "*"` semantics by authoring a spec whose own top-level
   `dataflow_group_id` is literally `"*"` if these destinations should apply globally
   (uncommon); otherwise they're automatically scoped to that spec's own
   `dataflow_group_id` — no separate scoping decision to make.
2. Validate: run the onboarding job with `action_type=VALIDATE_ONLY`, or pass just the
   `observability[]` fragment to `observability/agent_tools.py::validate_observability_config`
   for a quicker isolated check — fix every reported error before proceeding.
3. Onboard for real: re-run with `action_type=CREATE` (or `UPDATE` for an existing
   `dataflow_group_id`) — this upserts `observability_config` alongside every other flow in the
   same spec, via `onboarding/metadata_upsert.py::upsert_observability_config`.
4. Wire the Workflow task: add an `observability_export` task to the pipeline's job (see
   `resources/dlt_observability_job.yml` for the full pattern) with `depends_on:
   [{task_key: run_pipeline_update}]` and `base_parameters.run_pipeline_update_run_id:
   "{{tasks.run_pipeline_update.run_id}}"`.
5. Deploy and run — see
   [docs/26's end-to-end testing instructions](26_dlt_observability_testing_runbook.md#end-to-end-testing-inside-databricks-workflows).
