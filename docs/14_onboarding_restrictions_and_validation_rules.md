# 🚦 Onboarding Restrictions & Validation Rules

> **Audience**: Spec authors, reviewers, and AI agents generating onboarding specs — anyone who
> needs to know, before submitting a spec, exactly what the onboarding validator will accept,
> reject, and why.
>
> **Version & maintenance**: this page describes **v1.6.0** and is **hand-maintained against
> `src/flowx/lakeflow_framework/onboarding/spec_validator.py`** — the
> single source of truth for every rule below. When this page and the validator disagree, the
> validator wins; fix the page. Companion pages:
> [`00_master_reference_index.md`](00_master_reference_index.md) (what each field *means*),
> [`12_module_permutation_matrix.md`](12_module_permutation_matrix.md) (what is *legal
> together*), and [`13_known_limitations_and_gotchas.md`](13_known_limitations_and_gotchas.md)
> (what is legal, meaningful, and still wrong — the traps the validator *cannot* catch).

---

## 1. Where validation runs, and what an "error" is

Every onboarding spec passes through one function —
`onboarding/spec_validator.py::validate_spec` — before any control-table row is written. It
runs in three places:

| Entry point | What runs | When to use it |
|---|---|---|
| **Single-spec onboarding job** | `notebooks/02_onboarding/02_onboarding_engine.py` (widgets: `spec_file_path`, `catalog`, `env`, `action_type` = `CREATE` / `UPDATE` / `VALIDATE_ONLY`, and since v1.7.07 `prune_missing_flows` = `false` / `true` — opt-in soft-disable of this group's control rows for flows the spec no longer declares, `onboarding/spec_pruning.py`) | The normal CI/CD path — one spec changes, one job runs. Without `prune_missing_flows: true` a flow deleted from the spec keeps its active row and keeps running. |
| **Bulk directory onboarding** | `notebooks/02_onboarding/02b_bulk_config_onboarding_engine.py` → `onboarding/bulk_onboarding.py` (parameters: `spec_dir`, `action_type`, fail-soft by default) | Bringing up a whole environment or regression-onboarding the `flowx_testing/` corpus. Every spec is attempted; failures are reported per spec at the end. |
| **Spec Builder app** | `POST /api/spec/validate` (`databricks-app/server/routers/spec_router.py` → `server/core/validator.py::SpecValidator`) | Interactive feedback while authoring in the app. This is a registry-driven, in-app **approximation** of the framework validator (Layer 1 structural + Layer 2 rule checks, no Spark session) — a spec that passes the app can still fail the framework validator, never the reverse direction you want. Treat the framework validator as authoritative. |

Four properties of the framework validator worth internalizing:

- **It collects *every* problem, not the first one.** A single validation pass returns a
  complete report, so you fix the whole spec once instead of looping fix-one-rerun. Each
  finding is one human-readable string of the form `"<json_path>: <what's wrong>"`, e.g.
  `ingestion_flow[df_raw_txn].source_config.capture_technical_metadata: expected a boolean
  (true/false), got 'abc' (str)`.
- **Errors block onboarding; warnings do not.** Everything appended to the errors list makes
  the onboarding engine raise before any `MERGE` runs. A handful of findings are deliberately
  demoted to `logger.warning` instead: the graph-cycle rules under `execution_mode: "job"`
  (§4.4), a cross-group append-loop advisory, and `transformation_sql` references that cannot
  resolve yet because the referenced views only exist once the pipeline runs
  (`_validate_sql_syntax`). Warnings appear in the job run log only.
- **The validator is authoritative over the JSON Schema.** The editor-facing schema
  (`onboarding_templates/onboarding_spec.schema.json`) exists for IDE autocomplete and the
  Spec Builder app; it mirrors the validator but enforces nothing at onboarding time. Where
  the two drift, the validator is what actually runs.
- **It validates the shape of what is present.** Container objects such as
  `source_config` and `target_config` are not themselves presence-checked — a flow that omits
  `target_config` entirely produces no onboarding error (and fails later, at pipeline
  graph-definition time). Every "required" below therefore means *required by the validator*:
  either always, or whenever its enclosing container is present. The tables say which.

---

## 2. Mandatory fields per flow type

Extracted from the validator's `check_string(..., required=True)` / `check_dict(...,
required=True)` / `check_list_of_str(..., required=True)` calls, not from memory.

### 2.1 Spec root (the dataflow group)

| Field | Type | Constraint |
|---|---|---|
| `dataflow_group_id` | string | **Required**, non-empty. |
| `ingestion_flows` / `transformation_flows` / `reconciliation_flows` | arrays | Each optional individually, but **at least one of the three must be non-empty**. |
| `observability` | array | Optional. Deliberately does **not** count toward the "at least one non-empty" rule — telemetry with nothing to observe is meaningless. |
| `pipeline_parameters` | object | Optional. Drives `${param}` substitution in SQL and paths. |
| `spark_config` | object | Optional. Every key must start with `spark.`; every value must be a string, number, or boolean (never a nested object/array). See `_validate_spark_config`. |

### 2.2 `ingestion_flows[]`

Always required on every entry:

| Field | Type | Constraint |
|---|---|---|
| `dataflow_id` | string | Required, non-empty (the flow's primary key). |
| `source_type` | string | Required, one of `ALLOWED_SOURCE_TYPES` (§3.1). |
| `target_catalog` / `target_schema` / `target_table` | string | Required, non-empty. |
| `target_type` | string | Required, one of `ALLOWED_TARGET_TYPES` (§3.1). |

Conditionally required, per `source_type` (inside `source_config`, when that block is present):

| `source_type` | Required `source_config` fields |
|---|---|
| `autoloader` | `path`, `format`, `schema_location` — `schema_location` is auto-derived to `/Volumes/<target_catalog>/landing/_schemas/<target_table>/` when omitted and the target coordinates are present (the derived value is persisted). |
| `zerobus` | `source_catalog`, `source_schema`, `source_table`. |
| `asn1` | `path`, `schema_location` (same auto-derivation), `asn1_schema_path`, `asn1_codec` (`ber`/`der`), `asn1_pdu_name`. |

Conditionally required elsewhere in an ingestion (or transformation) flow:

| Trigger | Then required |
|---|---|
| `target_config` present | `target_config.cdc_load_strategy`. |
| `cdc_load_strategy` in `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` | `target_config.primary_keys` (list of strings). |
| `target_config.cdc_operation_column` or `cdc_operation_mapping` present | Both `cdc_operation_column` and `cdc_operation_mapping.delete_values`. |
| `target_type` is `sink`/`external_sink` | `target_config.sink_config` with `format`; then `path` (for `delta`/`pgp_zip`) or `kafka_options` (for `kafka`); `pgp_zip` additionally requires `post_export_archive` (§4.3). |
| `source_zip_handling` present | `enabled` (boolean); when `true`: `source_zip_path`, `zip_file_pattern`, `target_volume_path`. |
| `source_zip_handling.pre_extraction_decryption.type: "pgp"` | `private_key_secret` (a secret reference, §3.2). |
| `encrypted_columns[]` entry present | `column_name` and `secret` per entry. |
| `dq_config.rules[]` entry present | `rule_id`, `expression`, `action` per rule. |
| `governance_tags.column_tags[]` entry present | `column` and `tags` (string→string object) per entry. |
| `source_config.dedup_watermark` present | Both `event_time_column` and `delay_threshold` — plus `remove_dups: true` on the same source (§4.3). |

### 2.3 `transformation_flows[]`

| Field | Type | Constraint |
|---|---|---|
| `flow_step_id` | string | Required, non-empty (the flow's primary key). |
| `dataflow_id` | string | Required, non-empty. |
| `target_catalog` / `target_schema` / `target_table` | string | Required, non-empty. |
| `target_type` | string | Required, one of `ALLOWED_TARGET_TYPES`. |
| `transformation_sql` | string | Required, non-empty. Parse-validated via `EXPLAIN` after `${param}` substitution (§3.6). |
| `source_inputs[].input_name` | string | Required per entry — and **unique across every transformation flow in the spec**, because each registers a `@dlt.view` in the same pipeline graph (`_validate_no_duplicate_input_names`). |
| `source_inputs[].table` | string | Required per entry. |
| `source_inputs[].watermark` | object | Optional; when present, both `event_time_column` and `delay_threshold` are required. |
| `source_inputs[].decrypted_columns[]` | objects | Per entry: `column_name`, `cast_to_type`, and `secret` are all required — decryption may change the physical type, so `cast_to_type` is never optional. |

`target_config` and its CDC rules behave exactly as in §2.2, with one widening: transformation
flows may also use `cdc_load_strategy: "SCD3"` (§3.1).

### 2.4 `reconciliation_flows[]`

| Field | Type | Constraint |
|---|---|---|
| `reconciliation_id` | string | Required, non-empty. |
| `source_config` | object | **Required** (`check_dict(..., required=True)`); its `table` is required (the only supported `type` is `"table"`, which is also the default). |
| `target_configs` | array | Required, non-empty. |
| `target_configs[].target_id` | string | Required per entry, unique within the flow. |
| `target_configs[].table` | string | Required per entry. |
| `target_configs[].append_target_table` | string | Required when the entry's `comparison_direction` is `source_to_target` or `both` (and `both` is the default). |
| `match_keys` | list of strings | Required. |
| `dataflow_group_id` | string | Required **when `execution_mode` is `pipeline` or `pipeline_audit_only`** — a group-less flow has no Lakeflow pipeline to be registered into (rule V-CYC-6, §4.4). Optional in `job` mode. |

Everything else — `execution_mode`, `compare_columns`, `transform_sql`, `two_tier_verification`,
`error_handling`, `logging_config`, `publish_schema`, `dq_config` — is optional, but several of
them are only *legal* under a specific `execution_mode` (§4.2).

### 2.5 `observability[]`

| Field | Type | Constraint |
|---|---|---|
| `id` | string | Required per destination, unique within the spec. |
| `type` | string | Required, one of `ALLOWED_OBSERVABILITY_DESTINATION_TYPES` (§3.1). |
| `destination_config` | object | **Required.** |
| `destination_config.volume_path` | string | Required for `type: DATABRICKS_VOLUME`; must start with `/Volumes/` (§3.3). |
| `destination_config.endpoint` | string | Required for `type: OTLP_CONSUMER`; must be a full `http://` or `https://` URL. |
| `destination_config.event_log_tables` | list of strings | Required when `mode: "continuous"`; **rejected** when `mode` is `triggered` (the default); every entry must be a fully-qualified three-part `catalog.schema.table` name. See `_validate_observability_event_log_tables`. |
| `auth.type` | string | Required whenever `auth` is present. |
| `auth.credentials` | object | Required for `BEARER_TOKEN` (`token`), `API_KEY` (`header_name` + `api_key`), `BASIC_AUTH` (`username` + `password`) — each credential value a `env:`/`secret:` reference, never a literal (§3.2). |

---

## 3. Enumerations and data-type constraints

### 3.1 Allowed-value registries

All defined as module-level constants at the top of `onboarding/spec_validator.py`. An invalid
value produces `"<path>: has invalid value '<x>' -- allowed values are [...]"`.

| Attribute path | Constant | Allowed values | Notes |
|---|---|---|---|
| `ingestion_flows[].source_type` | `ALLOWED_SOURCE_TYPES` | `autoloader`, `zerobus`, `asn1` | `gcs_autoloader` was renamed to `autoloader` in v2. |
| `*.target_type` | `ALLOWED_TARGET_TYPES` | `streaming_table`, `materialized_view`, `batch_table`, `external_sink`, `sink` | |
| `target_config.cdc_load_strategy` (ingestion) | `ALLOWED_INGESTION_CDC_STRATEGIES` | `APPEND`, `TRUNCATE_AND_LOAD`, `SCD1`, `SCD2`, `FULL_SNAPSHOT_CDC` | `SCD3` on an ingestion flow is rejected with a dedicated message (§4.3). |
| `target_config.cdc_load_strategy` (transformation) | `ALLOWED_TRANSFORMATION_CDC_STRATEGIES` | the above + `SCD3` | |
| `dq_config.rules[].action` | `ALLOWED_DQ_ACTIONS` | `warn`, `drop`, `fail`, `quarantine` | `quarantine` is additionally rejected on a reconciliation flow's `dq_config` (§4.3). |
| `target_config.encrypted_columns[].mode` | `ALLOWED_AES_MODES` | `GCM`, `CBC`, `ECB` | |
| `source_config.landing_retention_policy.clean_source` | `ALLOWED_CLEAN_SOURCE_MODES` | `archive`, `delete`, `off` | `archive_path` is deliberately never required — an `archive` policy with no path degrades to a documented no-op. |
| `source_zip_handling.delete_source_after_extract.action` | `ALLOWED_ZIP_DELETE_ACTIONS` | `delete_now`, `delete_after_x_days` | `"never"` has deliberately **no spec spelling** — it exists only as the normalized form of the legacy boolean `false`. |
| `source_config.column_normalization.case` | `ALLOWED_COLUMN_NORMALIZATION_CASES` | `lower`, `preserve`, `upper` | Only the case fold is configurable; character normalization and the lowercased collision check are fixed. |
| `source_config.schema_evolution_mode` | `ALLOWED_SCHEMA_EVOLUTION_MODES` | `addNewColumns`, `addNewColumnsWithTypeWidening`, `rescue`, `failOnNewColumns`, `none` | |
| `target_config.storage_format` | `ALLOWED_STORAGE_FORMATS` | `delta`, `iceberg` | `iceberg` only for `target_type: "batch_table"` (§4.3). |
| `target_config.sink_config.format` | `ALLOWED_SINK_FORMATS` | `delta`, `kafka`, `pgp_zip` | |
| `target_config.sink_config.staged_file_format` | `ALLOWED_STAGED_FILE_FORMATS` | `json`, `csv` | **New in v1.6.0.** The staged per-partition file format inside a `pgp_zip` archive; absent means `json` (JSON-Lines, the only pre-v1.6.0 behaviour). Presence-rejected for `delta`/`kafka` (§4.3). |
| `source_zip_handling.member_format` | `ALLOWED_SOURCE_MEMBER_FORMATS` | `zip`, `gzip` | **New in v1.7.4.** The landing archive's *container*, orthogonal to any decryption layer. Absent means `zip` (the only pre-v1.7.4 behaviour). Use `gzip` only for an **encrypted** `.gz` — an unencrypted one needs no ZIP handling at all, because Spark decompresses it natively on read. |
| `post_export_archive.archive_format` | `ALLOWED_ARCHIVE_FORMATS` | `zip`, `gzip` | **New in v1.7.4.** The finished export's container. Absent means `zip`. `gzip` concatenates the micro-batch's staged files into one single-member stream named `<stem>.csv.gz` (`.csv.gz.gpg` when encrypted), and has no archive password — `post_export_archive.secret` does not apply. |
| `staged_file_options.line_terminator` | `ALLOWED_STAGED_LINE_TERMINATORS` | `crlf`, `lf` | **New in v1.7.4.** Absent means `crlf` (RFC-4180). Spelled as a name because JSON cannot carry a bare control character. Only valid alongside `staged_file_format: "csv"`. |
| `sink_config.export_trigger` | `ALLOWED_EXPORT_TRIGGERS` | `per_micro_batch`, `per_update` | **New in v1.7.5.** WHAT drives a `pgp_zip` export. Absent means `per_micro_batch` (one archive per micro-batch of an append-only stream — the only pre-v1.7.5 behaviour). `per_update` drives the sink from an update-scoped pulse and reads the payload as a batch, giving exactly one archive per pipeline update. It is what makes an **aggregating** target (a `materialized_view`, or any `TRUNCATE_AND_LOAD` flow) exportable at all — such a target is fully recomputed each update, which Delta refuses to stream from, so before v1.7.5 it had no sink path. See [`06_egress_and_lakeflow_sinks.md`](06_egress_and_lakeflow_sinks.md). |
| `reconciliation_flows[].error_handling.on_failure` | `ALLOWED_RECONCILIATION_FAILURE_MODES` | `fail`, `warn` | |
| `reconciliation_flows[].execution_mode` | `ALLOWED_RECONCILIATION_EXECUTION_MODES` | `job`, `pipeline`, `pipeline_audit_only` | Defaults to `job`; an invalid value still resolves to `job` for the mode-conditional checks, so nothing is silently skipped. |
| `reconciliation` dataset `type` | `ALLOWED_RECON_DATASET_TYPES` | `table` | Delta tables only since v1.3.0 — `file`/`sink` produce a migration-naming error. |
| `target_configs[].comparison_direction` | `ALLOWED_COMPARISON_DIRECTIONS` | `source_to_target`, `target_to_source`, `both` | Default `both`. |
| `source_config`/`target_configs[]` `read_mode` | `ALLOWED_READ_MODES` | `batch`, `streaming` | `streaming` rejected in pipeline execution modes (§4.2). |
| `source_config.asn1_codec` | `ALLOWED_ASN1_CODECS` | `ber`, `der` | |
| `pre_extraction_decryption.type` | `ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES` | `pgp`, `pgp_symmetric` | `pgp_symmetric` is **new in v1.7.4**: a passphrase-encrypted message (SKESK) rather than one encrypted to a recipient keypair (PKESK). The two take different secrets and neither opens the other's messages — see [`05_security_and_cryptography.md` §4.1](05_security_and_cryptography.md). Adding an algorithm later means a new handler + a new name here, never a schema restructure. |
| `observability[].type` | `ALLOWED_OBSERVABILITY_DESTINATION_TYPES` | `DATABRICKS_VOLUME`, `OTLP_CONSUMER` | |
| `observability[].mode` | `ALLOWED_OBSERVABILITY_MODES` | `triggered`, `continuous` | Absent resolves to `triggered`. `mode` is what stops one destination being served — and double-exported — by both observability engines. |
| `observability[].auth.type` | `ALLOWED_OBSERVABILITY_AUTH_TYPES` | `BEARER_TOKEN`, `API_KEY`, `BASIC_AUTH`, `NONE` | |
| `observability[].destination_config.compression` | `ALLOWED_OBSERVABILITY_COMPRESSION` | `GZIP`, `gzip`, `none`, `""` | |
| `destination_config.file_format` (volume) | inline | `JSONL`, `JSON` | |
| `destination_config.protocol` (OTLP) | inline | `OTLP_HTTP_JSON`, `OTLP_HTTP_PROTO`, `OTLP_GRPC` | |

One constant is defined but not currently consumed by any check: `ALLOWED_SINK_WRITE_MODES`
(`overwrite`, `append`). Do not document a sink `write_mode` restriction as enforced — today it
is not.

### 3.2 Secret and credential reference shapes

Two deliberately different shapes exist, and they are not interchangeable:

- **Unity Catalog three-level secret reference** — used *everywhere* in the data plane
  (encryption keys, PGP keys, ZIP passwords, Kafka secret options). Validated by
  `check_secret_ref`; resolved by `crypto/secrets.py::resolve_secret_value` via
  `dbutils.secrets.get(catalog=, schema=, key=)`, never a classic workspace scope:

  ```json
  {"secret_catalog": "poc", "secret_schema": "security", "secret_key": "pii_encryption_key"}
  ```

  All three keys are required whenever the reference itself is required or present.

- **Observability credential reference** — the `observability[].auth.credentials` values are
  short strings matching `env:<VAR_NAME>` or `secret:<scope>:<key>`
  (`check_credential_ref`, pattern `_CREDENTIAL_REF_PATTERN`). A literal secret value is
  rejected with a message saying so. See
  `observability/destination_dispatcher.py::resolve_credential` for why this module's shape
  differs.

### 3.3 Path rules

| Rule | Where enforced |
|---|---|
| `observability[].destination_config.volume_path` must start with `/Volumes/`. | `_validate_observability_destination_config`. |
| `observability[].destination_config.endpoint` must start with `http://` or `https://`. | Same function. |
| `event_log_tables[]` entries must be three-part `catalog.schema.table`. | `_validate_observability_event_log_tables`. |
| An undefined `${param}` in any `source_config`/`target_config` path field is an onboarding error, not a pipeline-run failure. | `_validate_path_parameters`, mirroring the engine notebooks' own substitution. |
| `schema_location`, when omitted on an `autoloader`/`asn1` flow, is derived to the repo-wide `/Volumes/<catalog>/landing/_schemas/<table>/` convention rather than rejected. | `_validate_ingestion_source_config`. |

Other path fields (`source_config.path`, `sink_config.path`, `source_zip_handling.*`) are
validated as non-empty strings only — the validator has no filesystem access, so existence is a
runtime concern.

### 3.4 Identifier safety

`crypto/secrets.py::assert_safe_identifier` enforces `^[A-Za-z_][A-Za-z0-9_]*$` on every
identifier that gets spliced into framework-assembled SQL — catalog/schema/table/column names
and the three secret-reference parts. It is deliberately stricter than what Unity Catalog
itself permits (no `.` or `-`), because its job is closing off SQL injection via a compromised
or malformed control-table row.

Where it bites at validation time: the cross-flow graph rules (§4.4) build comparison keys
through `storage/table_properties.py::qualified_table_name`, which applies
`assert_safe_identifier` to each part — a name that fails simply drops out of cross-flow
comparison (the per-field type error has already been reported). Where it bites at runtime:
encryption, decryption, governance-tag DDL, and every qualified table name. A spec can
technically onboard with an exotic identifier and then fail at graph-definition time, so treat
the regex as the practical naming rule for all framework-touched identifiers.

### 3.5 Type strictness and numeric bounds

| Rule | Detail |
|---|---|
| Booleans must be JSON `true`/`false`. | `check_bool` rejects `"true"` (string) with a message telling you to unquote it. |
| Integers must be integers. | `check_int` rejects booleans explicitly, even though Python's `bool` subclasses `int`. |
| `target_config.liquid_clustering_columns` | At most `MAX_LIQUID_CLUSTERING_COLUMNS` = **3** columns (Delta Liquid Clustering's own hard limit, re-asserted at runtime by `storage/table_properties.py`). An empty list is valid and means "no clustering". |
| `target_config.partition_columns: []` | Valid; means "no partitioning", logged distinctly from an absent field. |
| `landing_retention_policy.retention_days` | Integer ≥ **0**. Zero means "no age threshold"; *omitting* the field means 7 days (`ingestion/readers.py::DEFAULT_LANDING_RETENTION_DAYS`), not zero. |
| `delete_source_after_extract.days` | Integer ≥ 0; required for `delete_after_x_days`, rejected for `delete_now` (§4.3). |
| `auto_ttl.expire_in_days` | Integer ≥ 1 when present. |
| `observability[].retry.max_attempts` | Integer ≥ 1. |
| `observability[].retry.backoff_multiplier` | A number strictly > 1. |
| `observability[].timeout_ms` | Integer ≥ 1. |

### 3.6 Restricted SQL grammars

Two different SQL surfaces, two different rules:

- **`data_standardization_sql`** (ingestion sources and reconciliation dataset sides) is a
  **column-expression allowlist, never a full statement**. Any bare occurrence of
  `SELECT`/`FROM`/`JOIN`/`UNION`/`WHERE`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`/`DROP`/`ALTER`/
  `CREATE`/`GRANT`/`REVOKE` (case-insensitive, word-boundary matched) is rejected outright, as
  is any `;`. Exactly one column expression per entry, e.g.
  `"trim(customer_name) AS customer_name"`. Deliberately conservative: better to reject a
  legitimate edge case than silently accept a disguised full statement. **Since v1.7.07 every
  entry must also end with `AS <column_name>`** (`_STANDARDIZATION_ALIAS_PATTERN`, the same
  regex the runtime uses): `ingestion/standardization_sql.py` writes the expression to exactly
  that column via `withColumn` -- replacing an existing column of that name in place, adding it
  otherwise, never dropping anything -- and raises without the alias, so an entry like
  `"trim(customer_name)"` used to onboard cleanly and die at pipeline graph definition.
- **`transformation_sql`** (and reconciliation `transform_sql`) legitimately needs full
  `SELECT`/`FROM`, so it is validated for *parse-ability and structural planning* instead, via
  an `EXPLAIN` of the post-`${param}`-substitution text (`_validate_sql_syntax`). A
  `ParseException` is always a hard error. Of the planning failures, only the genuinely
  structural codes `NUM_COLUMNS_MISMATCH` and `INCOMPATIBLE_COLUMN_TYPE` (e.g. a `UNION` whose
  branches don't line up) are hard errors — an unresolved table/column reference is expected
  pre-deployment (the referenced `source_inputs[]` views only exist once the pipeline runs) and
  is logged as a warning, never an error.

---

## 4. Forbidden and rejected configurations

### 4.0 Unrecognised attributes — rejected on presence (v1.7.1)

The rule in §4.1 below applies to attributes that *were* real and have been removed. Since
v1.7.1 the same rule covers attributes that were **never** real: any key the framework does not
read is a hard validation error, not a silent no-op.

The motivation is identical, and the observed failures were worse — because an invented key
looks plausible and nothing anywhere contradicts it:

| Written in the spec | The real key | What actually happened before v1.7.1 |
|---|---|---|
| `data_quality` | `dq_config` | No data-quality rule ever ran |
| `cdc_config: {keys: [...]}` | `target_config.cdc_load_strategy` (+ `primary_keys`) | CDC keys ignored |
| `partition_by` | `target_config.partition_columns` | Table not partitioned |
| `primary_key: "id"` | `target_config.primary_keys: ["id"]` | Key ignored; SCD strategies failed elsewhere or merged wrongly |
| `infer_schema` | — (delete it) | Nothing; never a real key |

A spec carrying six such keys previously validated with `valid: true` and `error_count: 0`.

**Enforcement.** `spec_validator.py::reject_unknown_keys()` checks each authored container
against its allowlist (`ALLOWED_ROOT_KEYS`, `ALLOWED_INGESTION_FLOW_KEYS`,
`ALLOWED_TRANSFORMATION_FLOW_KEYS`, `ALLOWED_INGESTION_SOURCE_CONFIG_KEYS`,
`ALLOWED_TARGET_CONFIG_KEYS`, `ALLOWED_DQ_CONFIG_KEYS`, `ALLOWED_GOVERNANCE_TAGS_KEYS`,
`ALLOWED_SOURCE_INPUT_KEYS`, `ALLOWED_SINK_CONFIG_KEYS`, `ALLOWED_RECONCILIATION_FLOW_KEYS`).
`onboarding_spec.schema.json` now also sets `additionalProperties: false` on every authored
container, so an editor flags the same mistake before onboarding runs. The two are asserted
equal by `tests/unit/test_unknown_key_rejection.py::test_allowed_key_sets_match_json_schema` —
adding an attribute to one and not the other fails the build.

**Message.** The error names the replacement wherever one is known, via `UNKNOWN_KEY_ALIASES`:

```
ingestion_flow[df_x].source_config.file_format: not a recognised attribute -- the framework
never reads it, so leaving it in place silently does nothing. Use source_config.format.
```

Otherwise it offers the closest real key (`Did you mean 'target_table'?`) or lists the allowed
keys for that container.

**Exempt.** Any key matching `^_` is an author comment — JSON has no comment syntax, and specs
in `flowx_testing/` use `_scenario`, `_provenance` and `_test_case_note` extensively. `$schema`
is exempt as an editor hint.

**Migrating.** 56 of the 57 specs shipped in this repo were already clean. The one exception
carried `environment` and `catalog_name` at the spec root; both were inert (a group's
`catalog_name` comes from the onboarding job's `--catalog` parameter, never the spec) and have
been deleted. `depends_on_dataflow_group_ids`, previously accepted-and-unread, is now rejected
with a message pointing at Lakeflow Jobs `depends_on`.

### 4.1 Removed attributes — rejected on presence, never silently ignored

The design rule, quoting `reject_removed_keys`' own rationale:

> Presence alone is the trigger — not truthiness. `generate_surrogate_key: false` is still a
> statement about a feature that no longer exists, and leaving it in a spec means the next
> person to read it believes the framework still has the knob. Reporting it costs the author
> one deletion and buys a document that describes what actually runs.

An ignored key is the worst possible behaviour: the spec still onboards, the control table
still gets a row, the pipeline still runs — and it quietly does something other than what the
document says. For any attribute that switched a data-shaping behaviour ON, ignoring it flips
that behaviour OFF with no signal at all. Hence four registries, each key mapping to its own
migration message (paraphrased below; the validator emits the full text verbatim):

| Registry | Removed key | Migration (summary of the emitted message) |
|---|---|---|
| `REMOVED_SOURCE_CONFIG_KEYS` | `source_config.normalize_column_names` | Removed in v1.4.0 — `column_normalization` is now the only switch. Replace `normalize_column_names: true` with `column_normalization: {enabled: true}` (add a `case` key if you relied on something other than the default `lower`); delete the key outright if it was `false`. |
| `REMOVED_TARGET_CONFIG_KEYS` | `target_config.generate_surrogate_key` | Removed in v1.4.0 — the surrogate-key engine is gone; `__framework_surrogate_key` is no longer generated for any flow. Declare real `primary_keys` (`SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` all take them), or use `TRUNCATE_AND_LOAD` if the source has no key. |
| `REMOVED_TARGET_CONFIG_KEYS` | `target_config.surrogate_key_columns` | Removed with the surrogate-key engine — it scoped a column no longer generated. Row identity: `primary_keys`; change comparison: `columns_to_check`/`columns_to_exclude`. |
| `REMOVED_TARGET_CONFIG_KEYS` | `target_config.surrogate_key_exclude_columns` | Same as above. |
| `REMOVED_RECONCILIATION_FLOW_KEYS` | `reconciliation_flows[].recon_mode` | Removed in v1.4.0 — reconciliation is triggered-only. Every run is a bounded job task (`trigger(availableNow=True)` for a streaming side). Delete the key; for continuous coverage, schedule the job, or set `execution_mode` to `pipeline`/`pipeline_audit_only` to run the comparison inside the group's own Lakeflow update. |
| `REMOVED_RECONCILIATION_FLOW_KEYS` | `reconciliation_flows[].generate_surrogate_key` | Removed with the surrogate-key engine. A reconciliation flow matches on its declared `match_keys`; both sides must carry those columns. |
| `REMOVED_CDC_LOAD_STRATEGIES` | `cdc_load_strategy: "FULL_SNAPSHOT_CDC_NO_PK"` | Removed in v1.4.0 — it existed only to consume the surrogate-key engine, hashing every payload column of every row on every run. Use `FULL_SNAPSHOT_CDC` with `target_config.primary_keys` (the Databricks-native `apply_changes_from_snapshot` pattern), or `TRUNCATE_AND_LOAD` if the source genuinely has no key. Checked *before* the allowed-values test, so authors see the migration message, not a generic "not one of [...]" list. |

### 4.2 Mode-incompatible keys (reconciliation `execution_mode`)

Same presence-not-truthiness convention as §4.1 (`reject_mode_incompatible_keys`), but nothing
here is gone from the spec forever — each key is perfectly valid under a *different*
`execution_mode`. The error always names the mode actually in force.

| Key | Rejected when `execution_mode` is | Why / migration |
|---|---|---|
| `source_config.task_run_id_column` / `target_configs[].task_run_id_column` | `pipeline`, `pipeline_audit_only` (`REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE`) | `engine/run_context.py::resolve_pipeline_run_id` has no stable per-update key — `pipelines.id` is constant across every update, so narrowing by it would silently match everything. Use `filter_condition`, or keep `execution_mode: "job"`. |
| `reconciliation_flows[].publish_schema` | `job` (`RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE`) | It names where the flow's `recon__<id>__<target>__classified`/`__metrics`/`__mismatch` datasets are published inside the hosting pipeline — a job-mode flow has no such datasets. |
| `reconciliation_flows[].dq_config` | `job` (same registry) | Its expectations attach to the one-row `__metrics` dataset, which a job task never produces. |
| `read_mode: "streaming"` on either dataset side | `pipeline`, `pipeline_audit_only` | The in-pipeline comparison is a whole-snapshot batch classification; a stream-static join cannot express `MISSING_IN_SOURCE`. Use `batch` (the default) or `execution_mode: "job"`. |
| *Missing* `dataflow_group_id` on the flow | `pipeline`, `pipeline_audit_only` | Required — see §2.4 and V-CYC-6 (§4.4). |

### 4.3 Cross-field rejections

Rules that only fire when two or more fields are considered together. The two `logging_config`
rules are **new in v1.6.0** and mirror graph-time guards in
`reconciliation/graph_registration.py`, so the contradiction surfaces at onboarding instead of
on the first pipeline update.

**v1.7.3 breaking change — silent by default.** `run_log_capture` and `mismatch_log_capture` now default to **`false`** (they defaulted to `true` through v1.7.2), so both rules below now fire for a flow that simply omits `logging_config`, not only for one that writes an explicit `false`. They are still rejections rather than silent downgrades, but each message names whether the flag was written `false`, left unset (and therefore defaulted false by v1.7.3), or forced false by a `dataflow.recon.*` pipeline-conf override — and tells the author to set `run_log_capture: true`. The v1.6.0 contract behind them: `logging_config.run_log_capture`
now gates `reconciliation_run_log` **and** `reconciliation_result` **and** (in pipeline mode)
whether the `recon__*__metrics` dataset is registered at all; `mismatch_log_capture` gates
`reconciliation_mismatch_log` and the `recon__*__mismatch` dataset; both `false` means the flow
persists only to its business targets. Both flags default to `false` since v1.7.3 and the rules
evaluate the *defaulted* values. **v1.7.07:** `publish_schema` is the only thing that publishes —
without it the audit datasets are pipeline-scoped, so a capture flag needs `publish_schema` (rows
1a/1b), and a `dq_config` gate no longer needs a capture flag (the old rule 1 is gone).

| # | Rejected configuration | Rule (see `spec_validator.py` function) |
|---|---|---|
| 1 | ~~`logging_config.run_log_capture: false` together with `dq_config.rules`~~ | **Removed in v1.7.07.** The `__metrics` dataset is now registered whenever `dq_config.rules` exist (pipeline-scoped if nothing publishes), so a gate needs no capture flag. |
| 1a | `run_log_capture: true` or `mismatch_log_capture: true` on a pipeline-mode flow with **no** `publish_schema` | **New in v1.7.07.** The control-table rows are exported from the *published* `recon__*__metrics`/`__mismatch` datasets; without `publish_schema` those are pipeline-scoped temporary tables and the row could never be written. Set `publish_schema`, or set both flags false. (`_validate_logging_config`) |
| 1b | `execution_mode: "pipeline"` with any `target_configs[].append_target_table` and **no** `publish_schema` | **New in v1.7.07.** The heal handler reads the prepared source/target back through the metastore, so they must be published. Set `publish_schema`, or use `pipeline_audit_only`. (`_validate_logging_config`) |
| 2 | `execution_mode: "pipeline_audit_only"` with **both** `run_log_capture: false` and `mismatch_log_capture: false` **and no `dq_config.rules`** | **v1.6.0, amended v1.7.07.** With no capture, no publish and no gate the flow registers compute with no output at all. Enable a flag (with `publish_schema`), declare rules, or use `job`/`pipeline`. (`_validate_logging_config`) |
| 3 | `dq_config.rules[].action: "quarantine"` on a reconciliation flow | There is nothing to quarantine on the one-row `__metrics` dataset. Use `warn`/`drop`/`fail`. (`_validate_reconciliation_flows`) |
| 4 | `sink_config.staged_file_format` with `format: "delta"` or `"kafka"` | **v1.6.0, presence-rejected.** The native sink formats have no framework staging step; accepting it would let a spec assert a file shape nothing ever produces. Only `pgp_zip` stages files. (`_validate_sink_config`) |
| 5 | `format: "pgp_zip"` without `post_export_archive`, or with `post_export_archive.enabled: false` | Archiving *is* what this sink format does; use `delta`/`kafka` for a sink with no archiving step. When enabled, `output_zip_path` is also required. (`_validate_sink_config`) |
| 6 | `post_export_archive.pgp_encryption.enabled: true` with **neither** `recipient_public_key_secret` **nor** `passphrase_secret` | **Amended v1.7.4.** Encryption needs a key or a passphrase. Before v1.7.4 the recipient key was unconditionally required; it is now required only when `passphrase_secret` is absent. Exactly one of the two. |
| 7 | `pgp_encryption.sign_passphrase_secret` without `sign_with_private_key_secret` | A passphrase is only meaningful alongside a signing key. **v1.7.4**: neither is valid alongside `passphrase_secret` — signing needs a sender keypair, which symmetric encryption does not have. |
| 8 | `format: "kafka"` without `kafka_options["kafka.bootstrap.servers"]` or without `kafka_options["topic"]` | The same minimum options a Spark Structured Streaming Kafka writer requires. Every `kafka_secret_options` value must be a UC secret reference. |
| 9 | `storage_format: "iceberg"` with any `target_type` other than `batch_table` | Use `delta` (optionally with `table_properties.enable_iceberg_read_uniformity`). (`_validate_target_config`) |
| 10 | `cdc_load_strategy: "SCD3"` on an ingestion flow | SCD3 pivots current/previous state via an internal history table — transformation flows only. (`validate_spec`) |
| 10a | `cdc_load_strategy: "TRUNCATE_AND_LOAD"` on a `streaming_table` (ingestion or transformation) | **New in v1.7.07.** Both strategies are no-ops in `engine/flow_registration.py::_NO_OP_CDC_STRATEGIES`, so on a streaming target TRUNCATE_AND_LOAD is realised as `dlt.read_stream` → plain append: identical to APPEND at runtime, the table is never truncated, and the spec claims a full reload it never performs. Use `materialized_view` / `batch_table` (fully recomputed each update) or declare `APPEND`. (`_reject_incompatible_target_type_strategy`) |
| 11 | `target_config.columns_to_exclude` outside `SCD1`/`SCD2`/`SCD3` | Only those strategies have a comparison-column concept to exclude from. |
| 12 | `target_config.cdc_operation_column`/`cdc_operation_mapping` outside `SCD1`/`SCD2`/`FULL_SNAPSHOT_CDC` | Only those strategies have a delete-marker path. |
| 13 | `target_config.empty_target_if_source_empty` outside `TRUNCATE_AND_LOAD` | Every other strategy appends or diffs — there is no truncation to guard, so the value would be silently inert. |
| 14 | `target_config.auto_ttl` (both sub-fields set) outside `APPEND`/`TRUNCATE_AND_LOAD` | The only strategies the engine threads the `auto_ttl` decorator kwarg through. (`_validate_auto_ttl`) |
| 15 | `source_config.dedup_watermark` without `remove_dups: true` | On its own it configures nothing; accepting it silently would let an author believe dedup was enabled. |
| 16 | `source_config.landing_retention_policy` on `source_type: "zerobus"` | Maps to `cloudFiles.cleanSource`, which only exists on an Auto Loader file read — `autoloader`/`asn1` only. |
| 17 | `liquid_clustering_columns` with more than 3 entries | Delta Liquid Clustering's own hard limit — caught here so it fails at onboarding, not mid-pipeline-update. |
| 18 | `hash_precomputed: true` on a reconciliation dataset whose `type` is not `table` | Only a framework-managed table can carry pre-built `__framework_hash_key`/`__framework_hash_value`. |
| 19 | `destination_config.event_log_tables` present with `mode: "triggered"`, or absent with `mode: "continuous"` | The continuous pipeline must be told what to stream; the triggered engine resolves its pipeline from the upstream task and would silently ignore a list. |
| 20 | `delete_source_after_extract.days` with `action: "delete_now"`, or missing with `action: "delete_after_x_days"` | A sweep with no threshold is `delete_now` spelled confusingly; a `days` on `delete_now` would be silently inert. |
| 21 | A column listed twice in `source_config.json_string_columns` | The second entry would silently overwrite the first's parse. |
| 22 | `source_inputs[].input_name` reused across transformation flows | All flows in one spec share one pipeline graph and one view namespace. (`_validate_no_duplicate_input_names`) |
| 23 | Duplicate `target_configs[].target_id` within a reconciliation flow, or duplicate `observability[].id` within a spec | Both are upsert keys. |
| 24 | A `spark_config` key not starting with `spark.`, or a non-scalar value | Far more likely a misfiled `pipeline_parameters` entry; a nested object has no meaningful `str()` rendering for `spark.conf.set`. |
| 25 | An undefined `${param}` in SQL or in any path field | `_validate_sql_syntax` / `_validate_path_parameters` — checked against `pipeline_parameters` at onboarding time. |
| 26 | `post_export_archive.pgp_encryption` with **both** `recipient_public_key_secret` and `passphrase_secret` | **v1.7.4.** Asymmetric and symmetric encryption are mutually exclusive — set exactly one. Rejected rather than resolved by precedence, because either resolution order would silently encrypt to something the author did not choose. (`_validate_sink_config`) |
| 27 | `source_zip_handling.pre_extraction_decryption.private_key_secret` with `type: "pgp_symmetric"` | **v1.7.4.** A passphrase-encrypted OpenPGP message carries a SKESK packet and has no recipient keypair. Use `type: "pgp"` for a key-encrypted message. |
| 28 | `type: "pgp_symmetric"` without `passphrase_secret` | **v1.7.4.** The passphrase *is* the decryption key here — there is nothing else to try. |
| 29 | `source_zip_handling.pre_extraction_decryption.secret_passphrase` with `member_format: "gzip"` | **v1.7.4.** `secret_passphrase` is the AES password on a **ZIP archive**; a gzip stream has no archive password, so the value would be silently inert. (Note the two similarly-named fields: `passphrase_secret` is PGP, `secret_passphrase` is the ZIP password.) |
| 30 | `source_zip_handling.member_format` outside `{"zip", "gzip"}`, or `post_export_archive.archive_format` outside `{"zip", "gzip"}` | **v1.7.4.** Both default to `"zip"` when absent. No other container is implemented — `tar` and friends are rejected rather than silently treated as ZIP. |
| 31 | `sink_config.staged_file_options` with `staged_file_format` other than `"csv"` | **v1.7.4.** A JSON-Lines export has no delimiter, header row or configurable record separator, so the object would assert a file shape nothing produces — the same reasoning as row 4. |
| 32 | `staged_file_options.delimiter` longer than one character, or `line_terminator` outside `{"crlf", "lf"}` | **v1.7.4.** Python's `csv` writer cannot emit a multi-character delimiter. `line_terminator` is spelled as a name because JSON cannot carry a bare control character. |
| 33 | `sink_config.export_trigger` with `format` other than `"pgp_zip"` | **v1.7.5.** `delta` and `kafka` are native Lakeflow sinks whose write cadence Lakeflow itself owns; there is no framework-driven export step for a trigger to schedule, so the value would be silently inert — the same reasoning as row 4. |

### 4.4 Graph-cycle and placement rules (V-CYC-1 … V-CYC-8)

Cross-array checks in `_validate_reconciliation_pipeline_placement` and
`_validate_landing_side_effect_collisions` that only make sense once every flow array in the
spec is known. All table references are compared as casefolded, fully-qualified
`catalog.schema.table` strings via `storage/table_properties.py::qualified_table_name`.

Severity depends on `execution_mode` (`_append_cycle_finding`): in `pipeline`/
`pipeline_audit_only` a cycle finding is a **hard error** (the append would race a read inside
the same Lakeflow update); in `job` mode the same finding is demoted to a **warning**, because
the standalone reconciliation task runs after the update finishes — the hazard is real but
there is no in-graph cycle, and job-mode specs that have always onboarded must keep onboarding.

| Rule | What it rejects (or warns about) |
|---|---|
| V-CYC-1 | Pipeline mode, same group: `source_config.table` does not resolve to any target this dataflow group actually produces — the in-pipeline source must be a dataset *this* update publishes, not an external always-one-update-stale read. Error. |
| V-CYC-2 | `append_target_table` is one of this group's own ingestion/transformation targets — Lakeflow owns that table's transaction log, and a reconciliation append would corrupt its declared write contract. Error in pipeline mode, warning in job mode. |
| V-CYC-3 | `append_target_table` is the raw `zerobus` ingestion source a flow in this same group reads — appending corrections back into it races the next update's own read. Error in pipeline mode, warning in job mode. |
| V-CYC-4 | Same as V-CYC-3 but the reconciliation flow is placed in a *different* `dataflow_group_id` — always a warning naming both groups, since neither pipeline's own graph can see the cross-pipeline loop. |
| V-CYC-5 | `append_target_table` equals the flow's own `source_config.table`, its own compared `target_configs[].table`, or another table this same flow reconciles against — every future run re-arms against its own output. Error in pipeline mode, warning in job mode. |
| V-CYC-6 | Pipeline mode with no `dataflow_group_id` on the flow — no pipeline to register into. Always an error. |
| V-CYC-7 | `execution_mode: "pipeline"` where the reconciliation source is produced by a merge-writing strategy (`SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`) or by `TRUNCATE_AND_LOAD` into a `materialized_view`. That mode streams the source to drive the L5 heal pulse, so it requires an append-only producer. Always an error under `"pipeline"`. **Since v1.7.11 it does not fire under `"pipeline_audit_only"`**, which binds the same source with `want_stream=False` and registers no heal lane: that is the in-pipeline fix, and the rejection message names it. `execution_mode: "job"` remains the alternative if you also want the comparison out of the update. Audit-only does not heal, so add one `05_reconciliation_engine.py` task per `reconciliation_id`. |
| V-CYC-8 | Two ingestion flows sharing one Auto Loader landing `path` while declaring *different* `landing_retention_policy` or `source_zip_handling` blocks — `cloudFiles.cleanSource` moves/deletes committed files and zip handling decrypts/extracts/marks them, so two competing lifecycle regimes on one directory corrupt whichever runs second. Always an error, independent of any reconciliation flow. |

---

## 5. Testing a spec without onboarding it

Four ways to run the full validator (or an approximation) with zero writes. **Option 0 is the
one to reach for when generating a spec** — it needs no workspace, no cluster and no bundle, so
it is fast enough to loop on:

0. **`agent_tools.validate_json` — offline, no Spark.** The real `validate_spec` behind a
   text-in/dict-out wrapper that also accepts YAML, substitutes `{{catalog}}`/`{{env}}` and adds
   governance warnings:

   ```python
   import sys; sys.path.insert(0, "src")
   from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

   result = validate_json(open("my_spec.json", encoding="utf-8").read())
   print(result["summary"])
   for error in result["errors"]:
       print(" ERROR:", error)
   ```

   Every flow type is supported as of v1.7.1. The only check that needs a live session is the
   `EXPLAIN`-based structural validation of `transformation_sql`/`transform_sql`; without one it
   is skipped (parameter substitution is still checked) and runs at onboarding time on the
   cluster. Before v1.7.1 that call raised on `spark=None`, so transformation and reconciliation
   flows could not be linted offline at all.

1. **`action_type: "VALIDATE_ONLY"`** — run the generic onboarding job
   (`resources/flowx_config_jobs/onboarding_job.yml`, backing notebook
   `notebooks/02_onboarding/02_onboarding_engine.py`) with `action_type=VALIDATE_ONLY`. The
   exact same `validate_spec` runs, every error across the whole spec is reported in one pass,
   and the control-table upsert is skipped. The bulk engine
   (`02b_bulk_config_onboarding_engine.py`, job
   `resources/flowx_config_jobs/framework_config_onboarding_job.yml`) accepts the same
   `action_type` to dry-run an entire `spec_dir`, reporting per-spec results fail-soft.
2. **The Spec Builder app's validate endpoint** — `POST /api/spec/validate` with
   `{"spec": {...}}` returns the app's Layer 1 + Layer 2 findings interactively. Remember it is
   an in-app approximation (§1); finish with a `VALIDATE_ONLY` run before trusting a spec.
3. **pytest** — `tests/unit/test_spec_validator.py` exercises the validator directly (add a
   case here for any new rule), and `tests/unit/test_spec_loader.py` asserts the shipped
   templates stay loadable and that `pipeline_onboarding_template.json` and `.yaml` remain
   byte-for-byte equivalent — which means the templates themselves are validated on every
   `pytest tests/unit` run, with no Databricks workspace involved.

When a spec fails, read the error text literally: it names the exact `json_path` and the fix.
The validator's messages are generated to be actionable on their own — do not guess.
