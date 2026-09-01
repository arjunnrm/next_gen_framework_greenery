# Onboarding Spec — Exhaustive Field Reference

> Companion to [`onboarding_spec_full_reference.json`](onboarding_spec_full_reference.json) —
> read this doc side by side with that file. Every field named below is copied verbatim from
> that JSON so you can search for the literal string and see it in context.

## Purpose, and how this differs from the existing template docs

This repo already has two onboarding references, and this doc does not replace either of
them:

* [`pipeline_onboarding_template.json`](pipeline_onboarding_template.json) /
  [`.yaml`](pipeline_onboarding_template.yaml), documented field-by-field in
  [`17_onboarding_template_reference.md`](../docs/17_onboarding_template_reference.md) — a
  **good-defaults "kitchen sink"**: every `source_type`, `target_type`, and
  `cdc_load_strategy` appears at least once, but only the fields a realistic spec for that
  flow would actually set.
* [`onboarding_spec_full_reference.json`](onboarding_spec_full_reference.json) (this doc's
  subject) — **maximal, not realistic**: every single field
  `spec_validator.py` recognizes appears at least once, *including* every optional field,
  every enum value not otherwise exercised, and combinations no real pipeline would
  actually ship (a `batch_table` transformation flow with `storage_format: "iceberg"`
  right next to a Kafka sink and a PGP-signed, passphrase-protected export ZIP). Treat it
  as a field catalog to search, not a design to copy wholesale.
* [`onboarding_spec.schema.json`](onboarding_spec.schema.json) — the same shape as a
  machine-readable JSON Schema (Draft 2020-12), enforceable by any standard validator. This
  full-reference JSON validates cleanly against it (0 errors, confirmed with the Python
  `jsonschema` package — see the bottom of this doc).
* [`spec_validator.py`](../src/NextGen_Metadata_Framework/lakeflow_framework/onboarding/spec_validator.py)
  — the single source of truth all three documents above (and this one) are derived from.
  Where anything here and that file ever disagree, the code wins.

Every attribute/type/required/allowed-values cell below mirrors `spec_validator.py`
exactly, cross-checked function by function; narrative "why"/mechanics detail is
deliberately kept light here since [`docs/17`](../docs/17_onboarding_template_reference.md)
and [`docs/01`](../docs/01_control_metadata_schema.md) already cover it in depth and are
linked throughout instead of repeated.

---

## 1. Top-level shape

| Attribute | Type | Required | Sample value (from this file) |
|---|---|---|---|
| `dataflow_group_id` | string | **yes** | `"dfg_full_reference_example"` |
| `pipeline_parameters` | object | no | `{"filter_country": "US", "min_amount": 0, "run_date": "2026-01-01", "enable_strict_mode": true}` |
| `spark_config` | object | no | `{"spark.sql.shuffle.partitions": "auto", "spark.databricks.delta.optimizeWrite.enabled": "true"}` |
| `source_plane` | object | no | `{"materialize": "always", "catalog": null, "schema": "ref_source_plane"}` |
| `ingestion_flows` | array | see [17 §1](../docs/17_onboarding_template_reference.md#1-top-level-spec-shape) | 3 objects, §2 below |
| `transformation_flows` | array | — | 13 objects, §3 below |
| `reconciliation_flows` | array | — | 4 objects, §4 below |
| `observability` | array | no | 2 objects (`dest_ref_triggered_volume`, `dest_ref_continuous_otlp`) |

`pipeline_parameters` deliberately mixes value types here (`"US"` a string, `0` an int,
`"2026-01-01"` a string date, `true` a bool) to make the point explicit: `check_dict` only
validates the container is an object — every value is accepted as-is and rendered as a SQL
literal via `str()` at substitution time (strings single-quoted; everything else passed
through bare). The baseline template only ever used string values; this file shows a
non-string value is equally valid.

`source_plane` is **new in v1.5.0** — the read-once threshold policy for this dataflow
group's Lakeflow pipeline (see the CANONICAL IDENTITY / SHARING RULES sections of
[`docs/13`](../docs/13_known_limitations_and_gotchas.md)). This file sets `materialize: "always"`
(forcing a materialized L0 node for every external locator, not just the fanout-`>=`-2
default) and a dedicated `schema` for those nodes, to show a non-default configuration; the
baseline template shows the all-default `{"materialize": "auto", "catalog": null, "schema":
null}` shape instead. Persisted to the new nullable
`dataflow_group_spec.source_plane_config_json` column.

| Attribute | Type | Required | Default, and what an absent key / SQL `NULL` means | Lands in |
|---|---|---|---|---|
| `source_plane` | object | no | Absent ⇒ the column is written as `{}` (`json.dumps(spec.get("source_plane", {}))`), and a row or table predating the column reads back as `{}` via `getattr(GROUP_ROW, "source_plane_config_json", None) or "{}"` — either way, all defaults | `dataflow_group_spec.source_plane_config_json` (nullable `STRING`) |
| `source_plane.materialize` | string enum `auto`\|`always`\|`never` | no | `"auto"` — a shared L0 node is materialized only at fanout `>=` 2, so a single-consumer read keeps the pre-v1.5.0 inline path and its predicate pushdown | same column, `materialize` key |
| `source_plane.catalog` | string \| null | no | `null` ⇒ the hosting pipeline's own catalog (`spark.catalog.currentCatalog()`, falling back to `dataflow_group_spec.catalog_name`) | same column, `catalog` key |
| `source_plane.schema` | string \| null | no | `null` ⇒ the hosting pipeline's own schema (`resolve_pipeline_schema`: `pipelines.schema` → `pipelines.target` → current database → the group row's `target_schema`) | same column, `schema` key |

**Honest caveat — `source_plane` is the one v1.5.0 block `spec_validator.py` does *not*
check.** The string `source_plane` does not appear anywhere in `spec_validator.py`; the block
is persisted verbatim by `metadata_upsert.py` and only `onboarding_spec.schema.json` (which
`02_onboarding_engine.py` does not run) constrains it. The practical consequence is that a
misspelled key, or an unrecognized `materialize` value, is **not rejected at onboarding**: the
runtime decision is `share = materialize == "always" or (materialize != "never" and fanout >= 2)`
in `engine/source_plane.py`, so anything that is neither `"always"` nor `"never"` silently
behaves as `"auto"`. Validate this block against the JSON Schema yourself if you rely on it.

---

## 2. `ingestion_flows[]` — 3 flows, one per `source_type`

### 2.1 `df_ref_autoloader_full` (`source_type: "autoloader"`)

The richest flow in this file — carries every `source_config` field common to all source
types, every `autoloader`-specific field, and a `target_config` built around
`cdc_load_strategy: "SCD2"` so every SCD-adjacent field has somewhere to live.

**Flow-level fields** — `dataflow_id`, `source_system`, `source_database`,
`source_table_name`, `source_description` (all free text, unvalidated — see
[17 §2.1](../docs/17_onboarding_template_reference.md#21-fields-common-to-every-ingestion-flow)),
`source_type: "autoloader"`, `target_catalog`/`target_schema`/`target_table:
"{{catalog}}"`/`"ref_bronze"`/`"ref_autoloader_full"`, `target_type: "streaming_table"`.

**`source_config`** — every common field plus every `autoloader`-specific field:

| Attribute | Sample value in this flow | Notes |
|---|---|---|
| `path` | `"/Volumes/{{catalog}}/landing/ref_autoloader_zone/incoming/"` | required |
| `format` | `"csv"` | required, not enum-restricted |
| `schema_location` | `"/Volumes/{{catalog}}/landing/_schemas/ref_autoloader_full/"` | required (explicit here rather than relying on auto-derivation) |
| `schema_evolution_mode` | `"addNewColumnsWithTypeWidening"` | a different enum value than the baseline template's `"rescue"` — see `ALLOWED_SCHEMA_EVOLUTION_MODES` for the remaining two (`"failOnNewColumns"`, `"none"`) |
| `file_pattern` | `"customer_*.csv"` | — |
| `reader_options` | `{"header": "true", "delimiter": "\|", "cloudFiles.inferColumnTypes": "true"}` | passthrough `.option()` calls |
| `explode_columns` | `["event_payload"]` | scoped flatten/explode |
| `data_standardization_sql` | `["trim(region) AS region", "upper(country_code) AS country_code"]` | restricted column-expression grammar |
| `landing_retention_policy.clean_source` | `"archive"` | |
| `landing_retention_policy.archive_path` | `"/Volumes/{{catalog}}/landing/_archive/ref_autoloader_zone/"` | required because `clean_source == "archive"` |
| `landing_retention_policy.retention_days` | `14` | |
| `capture_technical_metadata` | `true` | |

`source_zip_handling` — demonstrated here on `autoloader` (the baseline template only
demoed it on `asn1`; v2 made it valid for `autoloader` too — see
[17 §2.3](../docs/17_onboarding_template_reference.md#23-source_type--autoloader)):

| Attribute | Sample value |
|---|---|
| `enabled` | `true` |
| `source_zip_path` | `"/Volumes/{{catalog}}/landing/ref_autoloader_zone/{{env}}/incoming_zips/"` |
| `zip_file_pattern` | `"ref_customer_batch_*.zip"` |
| `target_volume_path` | `"/Volumes/{{catalog}}/landing/ref_autoloader_zone/incoming/"` |
| `delete_source_after_extract` | `false` (the baseline template only ever showed `true`) |
| `pre_extraction_decryption.type` | `"pgp"` |
| `pre_extraction_decryption.private_key_secret` | secret ref |
| `pre_extraction_decryption.passphrase_secret` | secret ref — **new vs. the baseline template**, which never set this optional field on either of its `pre_extraction_decryption` blocks |
| `pre_extraction_decryption.secret_passphrase` | `{"secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "ref_zip_passphrase"}` — AES password on the ZIP archive itself, independent of `type` (moved here from the old top-level `source_zip_handling.secret` field) |

**`target_config`** — built on `cdc_load_strategy: "SCD2"`:

| Attribute | Sample value | Notes |
|---|---|---|
| `storage_format` | `"delta"` | (`"iceberg"` is demonstrated on the `asn1` flow below, §2.3 — the only `target_type` it's valid for is `batch_table`) |
| `partition_columns` | `["region"]` | only takes effect for `APPEND`/`TRUNCATE_AND_LOAD` — inert here on SCD2, kept to show the field parses regardless |
| `liquid_clustering_columns` | `["customer_id"]` | same caveat |
| `table_properties.log_retention_duration` | `"interval 30 days"` | |
| `table_properties.deleted_file_retention_duration` | `"interval 30 days"` | |
| `table_properties.enable_iceberg_read_uniformity` | `true` | **new vs. baseline template** — valid on any `target_type`, unlike `storage_format: "iceberg"` |
| `cdc_load_strategy` | `"SCD2"` | |
| `primary_keys` | `["customer_id"]` | required for SCD2 |
| `sequence_by_column` | `"updated_at"` | |
| `columns_to_check` | `["status_code", "amount"]` | |
| `columns_to_exclude` | `["batch_load_ts", "source_extract_filename", "etl_run_id"]` | only meaningful for SCD1/SCD2/SCD3 |
| `cdc_operation_column` | `"op"` | |
| `cdc_operation_mapping.delete_values` | `["D"]` | |
| `generate_hash_columns` | `true` | |
| `encrypted_columns[0]` | `{"column_name": "pii_column", "output_column": "pii_column", "mode": "GCM", "source_data_type": "string", "secret": {...}}` | output-column encryption |
| `encrypted_columns[0].source_data_type` | `"string"` | **new in v1.4.0**, optional — the column's original Spark type *before* encryption (`"string"`, `"decimal(18,2)"`, …). Omitted, the framework falls back to whatever type Spark observes at encryption time; declared, a silent source type change fails loudly at encryption time instead of being absorbed |

**`dq_config.rules`** — all four `ALLOWED_DQ_ACTIONS` values in one flow (the baseline
template spreads these across different flows; here they're together):
`dq_ref_customer_id_not_null` (`drop`), `dq_ref_amount_non_negative` (`quarantine`),
`dq_ref_status_known_value` (`fail`), `dq_ref_region_populated` (`warn`) — plus
`quarantine_table: "ref_autoloader_full_quarantine"` and `record_id_column: "customer_id"`.

**`governance_tags`** — two `column_tags` entries (`pii_column`, `account_number`) and a
`table_tags` object with three keys, showing the "multiple tags per column/table" shape
from [17 §4.8](../docs/17_onboarding_template_reference.md#48-governance_tags).

### 2.2 `df_ref_zerobus_full` (`source_type: "zerobus"`)

Exercises every `zerobus`-specific `source_config` field and pairs `cdc_load_strategy:
"APPEND"` with `auto_ttl` — the one `target_config` field pairing that only "counts" (per
`_validate_auto_ttl`) on `APPEND`/`TRUNCATE_AND_LOAD` targets.

| Attribute | Sample value |
|---|---|
| `source_config.source_catalog` | `"reference_source_catalog"` |
| `source_config.source_schema` | `"reference_source_schema"` |
| `source_config.source_table` | `"reference_source_zerobus_table"` |
| `source_config.starting_version` | `0` |
| `source_config.max_bytes_per_trigger` | `"1g"` |
| `target_config.cdc_load_strategy` | `"APPEND"` |
| `target_config.partition_columns` | `["event_type"]` |
| `target_config.liquid_clustering_columns` | `["event_id"]` |
| `target_config.auto_ttl.timestamp_column` | `"event_ts"` |
| `target_config.auto_ttl.expire_in_days` | `90` |
| `target_config.generate_hash_columns` | `false` (APPEND never gets hash columns regardless — see [17 §4.5](../docs/17_onboarding_template_reference.md#45-__framework_hash_key--__framework_hash_value--and-the-ingestion-timestamp)) |

`starting_version`/`max_bytes_per_trigger` are worth flagging explicitly: `spec_validator.py`
never calls `check_int`/`check_string` on either one for `source_type == "zerobus"` — they
pass through completely unvalidated at onboarding time (any JSON value is accepted). This
file still uses the documented types (`0` as a real int, `"1g"` as a string) since that's
what the runtime reader option actually expects.

### 2.3 `df_ref_asn1_full` (`source_type: "asn1"`)

Exercises every `asn1`-specific field with `asn1_codec: "der"` (the baseline template used
`"ber"` — `ALLOWED_ASN1_CODECS` only has these two values, so between the two files both are
now covered), plus `target_type: "batch_table"` with `storage_format: "iceberg"` — **the
only `target_type` where `"iceberg"` is a legal `storage_format`** (`_validate_target_config`
hard-errors otherwise; see [17 §4.2](../docs/17_onboarding_template_reference.md#42-target_config--full-field-table)).

| Attribute | Sample value |
|---|---|
| `source_config.path` | `"/Volumes/{{catalog}}/landing/ref_asn1_zone/extracted/"` |
| `source_config.schema_location` | `"/Volumes/{{catalog}}/landing/_schemas/ref_asn1_full/"` |
| `source_config.asn1_schema_path` | `"/Volumes/{{catalog}}/landing/_asn1_schemas/ref_cdr.asn"` |
| `source_config.asn1_codec` | `"der"` |
| `source_config.asn1_pdu_name` | `"ReferenceCallDetailRecord"` |
| `source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret` | secret ref — same optional field demonstrated again here |
| `target_config.storage_format` | `"iceberg"` |
| `target_config.table_properties.enable_iceberg_read_uniformity` | `true` |
| `target_config.cdc_load_strategy` | `"FULL_SNAPSHOT_CDC"` |
| `target_config.primary_keys` | `["callReferenceId"]` — **required** as of v1.4.0: snapshot CDC is `dlt.apply_changes_from_snapshot`, which has no keyless mode. A source with no usable key belongs on `TRUNCATE_AND_LOAD` instead |
| `target_config.cdc_operation_column` | `"recordStatus"` |
| `target_config.cdc_operation_mapping.delete_values` | `["DELETED", "PURGED"]` — 2 values, vs. the baseline template's 1 |
| `target_config.generate_hash_columns` | `true` |

---

## 3. `transformation_flows[]` — 13 flows

All 13 share `dataflow_id: "df_ref_autoloader_full"` or `"df_ref_asn1_full"` (a
dependency-ordering label only — see
[17 §3.1](../docs/17_onboarding_template_reference.md#31-fields-common-to-every-transformation-flow))
and read from the `ingestion_flows` tables above. Each row below names the flow's
distinguishing field(s) — see the JSON itself for the full object.

| `flow_step_id` | `target_type` | `cdc_load_strategy` | What it uniquely demonstrates |
|---|---|---|---|
| `ts_ref_append_full` | `streaming_table` | `APPEND` | 2 `source_inputs` (both streaming, both with `watermark` — a stream-stream range join); first input's `decrypted_columns` (mode `GCM`, `cast_to_type: "string"`); `target_config.auto_ttl` (valid — `APPEND`); full `table_properties`; `encrypted_columns`; **`target_config.capture_technical_metadata: true`** — the transformation-flow-only equivalent of `source_config.capture_technical_metadata`, absent from the baseline template entirely; all 4 `dq_config` actions again; `governance_tags` |
| `ts_ref_truncate_and_load` | `materialized_view` | `TRUNCATE_AND_LOAD` | `partition_columns`/`liquid_clustering_columns` shown again on `TRUNCATE_AND_LOAD` (the other strategy, besides `APPEND`, where they actually take effect) |
| `ts_ref_batch_table_iceberg` | `batch_table` | `TRUNCATE_AND_LOAD` | `storage_format: "iceberg"` on a **transformation** flow (§2.3 showed it on ingestion) — confirms the `batch_table`-only constraint is enforced identically for both flow kinds |
| `ts_ref_scd1` | `streaming_table` | `SCD1` | `primary_keys`, `sequence_by_column`, `cdc_operation_column`/`cdc_operation_mapping`, `columns_to_exclude`, `generate_hash_columns` |
| `ts_ref_scd1_no_sequence` | `streaming_table` | `SCD1` | `primary_keys` only — `sequence_by_column` omitted entirely, falling back to `__framework_ingestion_timestamp_utc` (see [17 §4.5](../docs/17_onboarding_template_reference.md#45-__framework_hash_key--__framework_hash_value--and-the-ingestion-timestamp)) |
| `ts_ref_scd2` | `streaming_table` | `SCD2` | `source_inputs[0].decrypted_columns[0].mode: "ECB"` and `target_config.encrypted_columns[0].mode: "CBC"` — the two `ALLOWED_AES_MODES` values neither the baseline template nor any other flow in this file uses (both default elsewhere to `"GCM"`); `columns_to_check` + `columns_to_exclude` together |
| `ts_ref_scd3` | `streaming_table` | `SCD3` | Transformation-only strategy (ingestion hard-rejects it — §2 never uses it); `primary_keys`, `sequence_by_column`, `columns_to_check` |
| `ts_ref_full_snapshot_cdc` | `streaming_table` | `FULL_SNAPSHOT_CDC` | `primary_keys: ["customer_id"]` + `cdc_operation_column`/`cdc_operation_mapping` (2 delete values) |
| `ts_ref_full_snapshot_cdc_minimal` | `streaming_table` | `FULL_SNAPSHOT_CDC` | The minimal snapshot shape — `primary_keys: ["region"]` and nothing else; no `cdc_operation_column`, so deletes are inferred purely from rows missing from the next snapshot |
| `ts_ref_external_sink_delta` | `external_sink` | `SCD1` | `sink_config.format: "delta"` + `write_mode` (dead field, see below) + `post_export_archive.enabled: false` — the structurally-accepted-but-inert path for a non-`pgp_zip` format |
| `ts_ref_external_sink_kafka` | `external_sink` | `APPEND` | `sink_config.format: "kafka"` with `kafka_options` (`kafka.bootstrap.servers`, `topic`, `databricks.serviceCredential`) and `kafka_secret_options` (`kafka.sasl.jaas.config` → secret ref) — **the baseline template has no Kafka example at all** |
| `ts_ref_pure_sink_pgp_zip` | `sink` | `APPEND` | `sink_config.format: "pgp_zip"` with `staged_file_format: "csv"` (v1.6.0 — RFC-4180 staged files with a header row; absent means the JSON-Lines default) and **every** `post_export_archive` field: `output_zip_path`, `export_file_name_format` (a `str.format()` template — new vs. baseline), `secret` (AES ZIP password), and `pgp_encryption` with all three of `recipient_public_key_secret`, `sign_with_private_key_secret`, **and** `sign_passphrase_secret` together (new — the baseline template signs without a passphrase) |
| `ts_ref_union_all` | `streaming_table` | `APPEND` | `UNION ALL` across two streaming `source_inputs` |

A few of these fields are worth calling out individually since they don't fit neatly into
the table:

* **`sink_config.write_mode`** (`ts_ref_external_sink_delta`, value `"append"`) — accepted by
  `spec_validator.py` (`ALLOWED_SINK_WRITE_MODES = {"overwrite", "append"}` exists) but
  **never read** by the engine's `"delta"` sink branch; `@dlt.append_flow` is always
  append-only. Included here purely for field-catalog completeness — omit it in a real spec.
* **The two snapshot flows** — v1.4.0 removed `FULL_SNAPSHOT_CDC_NO_PK`, so the flow that
  demonstrated it became an ordinary `FULL_SNAPSHOT_CDC` flow. Both are kept, because they are
  still two distinct permutations worth cataloguing: `ts_ref_full_snapshot_cdc` is the full form
  (declared key **plus** a `cdc_operation_column` delete marker), and
  `ts_ref_full_snapshot_cdc_minimal` is the minimal one (declared key and nothing else). The
  second was renamed rather than deleted, since collapsing it onto the first's `flow_step_id`,
  `target_table` and `source_inputs[0].input_name` would have made the file invalid —
  `input_name` must be unique across the **whole** spec, not merely within one flow, and that
  is one of the cross-array-element constraints JSON Schema cannot express (see the schema's own
  `$comment`, gap 1).
* **`target_config.capture_technical_metadata`** (`ts_ref_append_full`) — the one
  `target_config` field the baseline template never demonstrates at all. Conventionally
  used only by transformation flows (ingestion flows host the equivalent on
  `source_config` instead, since they have no other place to put it), but nothing in
  `_validate_target_config` actually restricts it to transformation flows only.

---

## 4. `reconciliation_flows[]` — 4 flows

> **v1.3.0:** reconciliation is restricted to Delta **tables** only. All three
> `target_configs[]` entries now use `type: "table"`; the previous `file` and `sink`
> examples were converted to read-back tables, which is the documented migration path.
>
> **v1.4.0:** `recon_mode` is gone (both `"triggered"` and `"continuous"`). Reconciliation
> is triggered-only: every side is read as a batch, a streaming side runs under
> `trigger(availableNow=True)`, and each run drains what is there and stops. To reconcile
> more often, schedule the job more often — there is no standing-stream mode to opt into.
>
> **v1.5.0:** `execution_mode` (default `"job"`, unchanged standalone engine) gains
> `"pipeline"` and `"pipeline_audit_only"`, which register this flow's comparison (L3+L4
> published `classified`/`metrics`/`mismatch` datasets, and — for `"pipeline"` only — the L5
> heal/append-back lane) inside this flow's own `dataflow_group_id`'s Lakeflow pipeline
> update instead of a standalone job task. `publish_schema` and `dq_config` are only
> meaningful (and only accepted) in those two modes; a flow-level `dataflow_group_id`
> (independent of this spec's own top-level one) becomes required in those two modes. See
> §4.3–§4.4 below, the attribute-by-attribute contract in §4.5, and the V-CYC-1…V-CYC-8
> cross-array placement rules in
> `spec_validator.py::_validate_reconciliation_pipeline_placement` — whose append-loop rules
> are hard errors in a pipeline mode but only warnings in `"job"` mode (§4.5).

Full narrative: [`docs/07_reconciliation_engine.md`](../docs/07_reconciliation_engine.md). Field
reference: [17 §5](../docs/17_onboarding_template_reference.md#5-reconciliation_flows).

### 4.1 `recon_ref_full_reference`

`execution_mode: "job"` is set explicitly here (the default — the standalone
`05_reconciliation_engine.py` job task, unchanged from pre-v1.5.0 behaviour).

**`source_config`** (`type: "table"`, the default, set explicitly here):

| Attribute | Sample value |
|---|---|
| `table` | `"{{catalog}}.ref_bronze.ref_reconciliation_baseline"` |
| `read_mode` | `"batch"` |
| `task_run_id_column` | `"__framework_pipeline_run_id"` — narrows this side's read to the rows written by the `task_run_id` job parameter's run, applied *before* `filter_condition`. Legal here only because this flow is `execution_mode: "job"`: it is **rejected on presence** in `"pipeline"`/`"pipeline_audit_only"`, where `pipelines.id` is constant across every update and the narrowing would match every row the pipeline ever wrote |
| `filter_condition` | `"load_date = ${run_date}"` — **no quotes around `${run_date}`**, matching the same substitution pitfall documented for `transformation_sql` in [17 §3.4](../docs/17_onboarding_template_reference.md#34-transformation_sql-parameters-union-and-the-stream-keyword): substitution already supplies the quotes for a string parameter |
| `data_standardization_sql` | `["trim(status_code) AS status_code"]` |
| `hash_precomputed` | `false` |

**`target_configs[]`** — 3 entries, all `type: "table"` (the only remaining
`ALLOWED_RECON_DATASET_TYPES` value), differing in every *other* per-target field, which is
something neither the baseline template nor any single flow needs to do since one flow's
`target_configs` is a list of independently-shaped targets:

| `target_id` | `read_mode` | Distinguishing fields |
|---|---|---|
| `ref_table_target` | `"batch"` | `hash_precomputed: true` (reuses `__framework_hash_key`/`__framework_hash_value` instead of recomputing — only legal when `type == "table"`), `comparison_direction: "both"`, `append_target_table` |
| `ref_file_readback_target` | `"streaming"` | The migration shape for what used to be a `type: "file"` target — a `target_type: "sink"`/file export read back into a Delta table. Its own `filter_condition`/`data_standardization_sql`, `comparison_direction: "source_to_target"`, `append_target_table`. `read_mode: "streaming"` still runs under `trigger(availableNow=True)` and stops (see the v1.4.0 note above) |
| `ref_sink_readback_target` | `"batch"` | Reads back into a table what a `target_type: "sink"` flow previously wrote (see [17 §5.2](../docs/17_onboarding_template_reference.md#52-source_config--each-target_configs-entry--shared-dataset-shape)); `comparison_direction: "target_to_source"` — the one direction where `append_target_table` is correctly **omitted**, since it's only required when the direction is `"source_to_target"` or `"both"` |

Flow-level fields: `match_keys: ["customer_id"]`, `compare_columns: ["amount",
"status_code"]`, `transform_sql` (reads `FROM
_reconciliation_unmatched_records`, per [17 §5.4](../docs/17_onboarding_template_reference.md#54-transform_sql)),
`error_handling.on_failure: "fail"`.

### 4.2 `recon_ref_warn_minimal`

The minimal legal shape — `source_config` omits `type` entirely (defaults to `"table"`,
per `_validate_reconciliation_dataset_config`'s `config.get("type", "table")`), a single
`target_configs[]` entry with `comparison_direction: "source_to_target"`, and
`error_handling.on_failure: "warn"` (the other `ALLOWED_RECONCILIATION_FAILURE_MODES`
value — `"fail"` is exercised by the flow above). `execution_mode` is omitted entirely
here too, also defaulting to `"job"` — between §4.1 and §4.2, `execution_mode: "job"` is
shown both explicit and implicit.

### 4.3 `recon_ref_pipeline_mode` — `execution_mode: "pipeline"`

New in v1.5.0. Demonstrates the full in-pipeline comparison-and-heal placement:

| Attribute | Sample value | Why |
|---|---|---|
| `execution_mode` | `"pipeline"` | Registers L3+L4 (`recon__recon_ref_pipeline_mode__ref_zerobus_external_baseline__classified`/`__metrics`/`__mismatch`) **and** the L5 heal/append-back lane inside this flow's `dataflow_group_id`'s Lakeflow pipeline update. |
| `dataflow_group_id` | `"dfg_full_reference_example"` | **Required** in pipeline/`pipeline_audit_only` modes (V-CYC-6) — a group-less flow has no pipeline to be registered into. Equal to this spec's own top-level `dataflow_group_id`, so this flow is placed in the *same* group's pipeline. |
| `publish_schema` | `"ref_bronze_recon"` | Schema (within the hosting pipeline's own catalog) where the three published recon datasets land. **Rejected on presence** when `execution_mode` is `"job"`. |
| `source_config.table` | `"{{catalog}}.ref_bronze.ref_zerobus_full"` | **Must resolve to a target this same dataflow group actually produces** (V-CYC-1) — here, `df_ref_zerobus_full`'s ingestion target — and that target's `cdc_load_strategy` must be append-only (V-CYC-7): `ref_zerobus_full` uses `APPEND`, which passes; a `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` producer, or `TRUNCATE_AND_LOAD` into a `materialized_view`, would be rejected. |
| `target_configs[].append_target_table` | `"{{catalog}}.ref_bronze.ref_zerobus_full_corrections"` | Must not equal any in-spec target (V-CYC-2), any in-spec zerobus ingestion source in this group (V-CYC-3), this flow's own `source_config.table` (V-CYC-5), or any of its own other `target_configs[].table` (V-CYC-5) — this value is a table no other flow in this spec references at all. |
| `dq_config` | `{"rules": [{"rule_id": "no_value_drift", "expression": "value_drift_count = 0", "action": "fail"}]}` | Expectations attached to the one-row `__metrics` dataset — the first declarative way a reconciliation threshold can fail a pipeline update. `action: "quarantine"` would be rejected here (nothing to quarantine on a one-row table); additive to, and independent of, `error_handling.on_failure`. |

### 4.4 `recon_ref_pipeline_audit_only_mode` — `execution_mode: "pipeline_audit_only"`

New in v1.5.0. Same L3+L4 in-pipeline comparison/reporting as §4.3, but healing stays on
the standalone job engine — the landing zone for a source whose Lakeflow-mode healing
can't (yet) run, or for a comparison whose source is static/low-change (see the
`known_limitations` note on source-change-triggered healing in
[`docs/13`](../docs/13_known_limitations_and_gotchas.md)). Its `source_config.table`
(`"{{catalog}}.ref_silver.ref_append_full"`) resolves to transformation flow
`ts_ref_append_full`'s `APPEND` target, satisfying V-CYC-1/V-CYC-7 the same way §4.3 does.
Its single `target_configs[]` entry uses `comparison_direction: "target_to_source"`, so
`append_target_table` is correctly **omitted** (only required for `"source_to_target"`/
`"both"`) — there is nothing for V-CYC-2/3/5 to check here. `dq_config` uses `action:
"warn"` this time, showing the two non-`"quarantine"` actions a reconciliation `dq_config`
rule can take (the third, `"drop"`, is equally valid but not separately exercised here).

### 4.5 The v1.5.0 reconciliation attributes — type, default, storage, rejection

The four sections above show these attributes *in use*. This table is the contract itself,
read straight out of `spec_validator.py` (what is accepted), `metadata_upsert.py` (what is
persisted, and to which column) and `control_plane/ddl_definitions.py` (what each column's
`NULL` means). Where the three disagreed, the code wins and the disagreement is stated.

| Attribute | Type | Required | Default, and what an absent key / SQL `NULL` means | Control-table column | Cross-field rule |
|---|---|---|---|---|---|
| `execution_mode` | string, one of `"job"` / `"pipeline"` / `"pipeline_audit_only"` (`ALLOWED_RECONCILIATION_EXECUTION_MODES`) | no | Absent ⇒ `"job"`. Written through as-is, with `None` left as **SQL `NULL`** rather than the literal `"job"`, so the DDL-documented default stays the one authority. Every reader resolves it as `(getattr(row, "execution_mode", None) or "job")` | `reconciliation_flow_spec.execution_mode` (nullable `STRING`) | An *invalid* value is reported as an error **and** falls back to `"job"` for every mode-conditional check that follows, so one typo cannot silently skip the rest of the block |
| `publish_schema` | string | no | Absent/`NULL` ⇒ the hosting pipeline's own schema (`_row_get(flow_row, "publish_schema", None) or publish_schema` in `reconciliation/graph_registration.py`) | `reconciliation_flow_spec.publish_schema` (nullable `STRING`) | **Rejected on presence — not truthiness — when `execution_mode` is `"job"`** (`RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE` via `reject_mode_incompatible_keys`). A job-mode flow publishes none of the datasets this names |
| `dq_config` | object, the same `#/$defs/dqConfig` ingestion and transformation use | no | Absent ⇒ no expectations. Persisted as `json.dumps(flow["dq_config"]) if flow.get("dq_config") else None`, so an **empty** `dq_config: {}` stores as SQL `NULL`, not `"{}"` | `reconciliation_flow_spec.dq_config_json` (nullable `STRING`) | Same presence-not-truthiness rejection in `"job"` mode — `dq_config: {}` is rejected even though it would persist as `NULL`. Additionally `action: "quarantine"` is rejected per rule index (nothing to quarantine on a one-row `__metrics` dataset); `warn`/`drop`/`fail` are allowed |
| `dataflow_group_id` (flow-level) | string | **conditionally** | Absent ⇒ this spec's own top-level `dataflow_group_id` | `reconciliation_flow_spec.dataflow_group_id` — but see the caveat below | **Required** when `execution_mode` is `"pipeline"`/`"pipeline_audit_only"` (V-CYC-6); optional in `"job"` mode, and deliberately not made globally required |
| `two_tier_verification` | boolean | no | Absent/`NULL` ⇒ `true` (Phase 1 fingerprint early-out enabled) | `reconciliation_flow_spec.two_tier_verification` (nullable `BOOLEAN`) | None. Valid in every `execution_mode` |
| `source_config.read_mode` / `target_configs[].read_mode` | string, `"batch"` / `"streaming"` | no | Absent ⇒ `"batch"` | inside `source_config_json` / `target_configs_json` | `"streaming"` is **rejected** when the owning flow is in a pipeline mode — the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join cannot express `MISSING_IN_SOURCE` |
| `source_config.task_run_id_column` / `target_configs[].task_run_id_column` | string | no | Absent ⇒ `task_run_id` stays correlation-only, no read narrowing | inside `source_config_json` / `target_configs_json` | **Rejected on presence** when the owning flow is in a pipeline mode (`REMOVED_RECONCILIATION_DATASET_KEYS_PIPELINE`) — a pipeline has no stable per-update key to narrow by, so it would be a silent no-op. Use `filter_condition` instead |

**Caveat on the flow-level `dataflow_group_id`: the validator reads it, the upsert does
not persist it.** `upsert_reconciliation_flow_spec(spark, control_schema, group_id, flows)`
sets `dataflow_group_id=group_id` on every row, and both call sites
(`02_onboarding_engine.py` and `onboarding/bulk_onboarding.py`) pass the spec's own
top-level `spec["dataflow_group_id"]`. So a recon flow that declares a *different* group
satisfies V-CYC-6 and changes how V-CYC-3/V-CYC-4 classify the flow, but its control-table
row still lands under the spec's own group — which is the group whose pipeline will
register it. Declaring another group's id is therefore a validation-time statement today,
not a routing instruction. Keep it equal to the spec's own `dataflow_group_id` unless you
have read `_validate_reconciliation_pipeline_placement` and want exactly its warning
behaviour.

**Severity of the V-CYC append-loop rules depends on `execution_mode`.** V-CYC-2, the
same-group half of V-CYC-3, V-CYC-5 and the `target_configs[]` self/cross-append checks all
describe a *Lakeflow graph* cycle. That graph exists only in a pipeline mode, so
`_append_cycle_finding` records them as **hard errors** under `"pipeline"` /
`"pipeline_audit_only"` and as **logger warnings** under `"job"`, where the standalone
`05_reconciliation_engine.py` task runs after the update has already finished. Reporting
them unconditionally was a backward-compatibility break: the shipped, purely job-mode
`metaflow_testing/038_rec_003_precomputed_hash.json` stopped validating and so could no
longer be onboarded at all.

| Rule | `execution_mode: "job"` | `"pipeline"` / `"pipeline_audit_only"` |
|---|---|---|
| V-CYC-1 (source must be a target this group produces) | not evaluated | error |
| V-CYC-2 (`append_target_table` is an in-spec target) | warning | error |
| V-CYC-3 (`append_target_table` is an in-spec zerobus ingestion source, same group) | warning | error |
| V-CYC-4 (same, but the flow declares a different group) | warning | warning — the loop is real but cross-pipeline and legal |
| V-CYC-5 (`append_target_table` equals this flow's own `source_config.table`, its own `target_configs[i].table`, or another `target_configs[j].table`) | warning | error |
| V-CYC-6 (`dataflow_group_id` required) | not evaluated | error |
| V-CYC-7 (producer must be append-only) | not evaluated | error |
| V-CYC-8 (conflicting `landing_retention_policy`/`source_zip_handling` on one landing path) | error | error — a pure ingestion rule, independent of any reconciliation flow |

**Operational note on the three new columns.** `execution_mode`, `publish_schema` and
`dq_config_json` are in the `CREATE TABLE IF NOT EXISTS` DDL, so a **new** workspace gets
them for free. An existing `reconciliation_flow_spec` gets them only from the additive
migration (`ADDITIVE_CONTROL_TABLE_COLUMNS` / `get_add_column_ddl` /
`ensure_control_table_columns`), and that migration runs only when the
`setup_control_tables` task actually **runs** — `databricks bundle deploy` does not apply
it. Onboarding a pipeline-mode flow against a pre-v1.5.0 control table fails with
`UNRESOLVED_COLUMN`.

---

## 5. Field-to-location index

Every field name below is a **direct child key** somewhere in
`onboarding_spec_full_reference.json` (nesting omitted for brevity — see the sections
above, or search the JSON for the literal name). Use this table as the fast "does this
file actually cover field X" lookup; every row was cross-checked line-by-line against
`spec_validator.py`.

| Field | Demonstrated in |
|---|---|
| `dataflow_group_id`, `pipeline_parameters` | top level |
| `source_plane.{materialize,catalog,schema}` (**new in v1.5.0** — the read-once threshold policy; `materialize` default `"auto"`, shown here as `"always"`) | top level |
| `source_zip_handling` on `autoloader` | `df_ref_autoloader_full` |
| `source_zip_handling` on `asn1` | `df_ref_asn1_full` |
| `pre_extraction_decryption.passphrase_secret` | both zip-handling blocks above |
| `starting_version`, `max_bytes_per_trigger` | `df_ref_zerobus_full` |
| `storage_format: "iceberg"` + `enable_iceberg_read_uniformity` | `df_ref_asn1_full` (ingestion), `ts_ref_batch_table_iceberg` (transformation) |
| `auto_ttl` (both sub-fields, on `APPEND`) | `df_ref_zerobus_full`, `ts_ref_append_full` |
| `encrypted_columns`/`decrypted_columns` mode `GCM` | `df_ref_autoloader_full`, `ts_ref_append_full` |
| `encrypted_columns`/`decrypted_columns` mode `CBC`/`ECB` | `ts_ref_scd2` |
| `encrypted_columns[].source_data_type` (**new in v1.4.0**, optional) | every `encrypted_columns[]` entry in this file: `df_ref_autoloader_full`, `ts_ref_append_full`, `ts_ref_scd2` |
| `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`/`APPEND`/`TRUNCATE_AND_LOAD` | every `cdc_load_strategy` value appears at least once across §2–§3 (`FULL_SNAPSHOT_CDC_NO_PK` was removed in v1.4.0 — keyless snapshot sources go to `TRUNCATE_AND_LOAD`) |
| `target_config.primary_keys` — **required** by `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` as of v1.4.0 | every CDC flow in §2–§3 |
| `target_config.capture_technical_metadata` | `ts_ref_append_full` |
| `sink_config.format: "delta"` | `ts_ref_external_sink_delta` |
| `sink_config.format: "kafka"` + `kafka_options` + `kafka_secret_options` | `ts_ref_external_sink_kafka` |
| `sink_config.format: "pgp_zip"` + `staged_file_format` + `export_file_name_format` + `sign_passphrase_secret` | `ts_ref_pure_sink_pgp_zip` |
| `sink_config.write_mode` (dead field) | `ts_ref_external_sink_delta` |
| `dq_config.rules[].action` — all 4 values | `df_ref_autoloader_full`, `ts_ref_append_full` |
| `governance_tags.column_tags`/`table_tags` | `df_ref_autoloader_full`, `ts_ref_append_full` |
| `watermark` (stream-stream join) | `ts_ref_append_full` |
| `UNION ALL` | `ts_ref_union_all` |
| `reconciliation` `type: "table"` (**the only allowed value as of v1.3.0** — `file`/`sink` were removed; read such output into a Delta table first) | `recon_ref_full_reference.target_configs[]` (all 3 targets are now `table`) |
| `reconciliation` `comparison_direction` — all 3 values | `recon_ref_full_reference` (`both`, `source_to_target`, `target_to_source`), `recon_ref_warn_minimal` (`source_to_target`) |
| `reconciliation` `error_handling.on_failure` — both values | `recon_ref_full_reference` (`fail`), `recon_ref_warn_minimal` (`warn`) |
| `reconciliation` `hash_precomputed` | `recon_ref_full_reference` (both `true` and `false`) |
| `reconciliation` `execution_mode` — all 3 values (**new in v1.5.0**) | `recon_ref_full_reference` (`"job"`, explicit), `recon_ref_warn_minimal` (`"job"`, implicit — key omitted), `recon_ref_pipeline_mode` (`"pipeline"`), `recon_ref_pipeline_audit_only_mode` (`"pipeline_audit_only"`) |
| `reconciliation` `publish_schema` (**new in v1.5.0** — rejected on presence when `execution_mode` is `"job"`) | `recon_ref_pipeline_mode`, `recon_ref_pipeline_audit_only_mode` |
| `reconciliation` `dq_config` (**new in v1.5.0** — `$ref` to the same `dqConfig` as ingestion/transformation, attached to the one-row `__metrics` dataset; `action: "quarantine"` is rejected here specifically; rejected on presence when `execution_mode` is `"job"`) | `recon_ref_pipeline_mode` (`action: "fail"`), `recon_ref_pipeline_audit_only_mode` (`action: "warn"`) |
| `reconciliation` flow-level `dataflow_group_id` (**new in v1.5.0**, independent of the spec's own top-level `dataflow_group_id`; required only in `"pipeline"`/`"pipeline_audit_only"` modes, V-CYC-6 — but validator-only: the upsert writes the spec's own group id to the column regardless, see §4.5) | `recon_ref_pipeline_mode`, `recon_ref_pipeline_audit_only_mode` |
| `reconciliation` `task_run_id_column` (optional; **rejected on presence** when the owning flow is in a pipeline mode) | `recon_ref_full_reference.source_config` |
| **v1.3.0 →** `spark_config` (E11, pipeline-level Spark overrides) | top level |
| `source_config.json_string_columns` (E03 — JSON-string → struct before flatten) | `df_ref_autoloader_full` |
| `source_config.explode_columns: []` present-but-empty = auto-flatten all (E03) | see §2.1 note; absent key stays pass-through |
| `source_config.remove_dups` + `dedup_watermark` (E04) | `df_ref_autoloader_full` |
| `source_config.column_normalization.{enabled,case}` (E05 — as of v1.4.0 the **only** normalization switch; `enabled` defaults to `false`, and the legacy `source_config.normalize_column_names` boolean is rejected at onboarding) | `df_ref_autoloader_full` |
| `source_config.landing_retention_policy.{clean_source,archive_path,retention_days}` (E01) | `df_ref_autoloader_full` |
| `source_zip_handling.delete_source_after_extract` nested object (E02) | both zip-handling blocks |
| `target_config.partition_columns: []` = explicitly unpartitioned (E06) | §2–§3 target configs |
| `target_config.liquid_clustering_columns` (max 3 — E07) | §2–§3 target configs |
| `target_config.empty_target_if_source_empty` (E09 — `TRUNCATE_AND_LOAD` only) | the `TRUNCATE_AND_LOAD` transformation flow |
| `reconciliation` `two_tier_verification` — both values (E12) | `recon_ref_full_reference` (`true`), `recon_ref_warn_minimal` (`false`) |
| `reconciliation` `logging_config.{run_log_capture,mismatch_log_capture}` (E12) | all four reconciliation flows (`recon_ref_warn_minimal` is the one setting `mismatch_log_capture: false`) |
| `observability[]` `mode` — both values (E13) | `dest_ref_triggered_volume` (`triggered`), `dest_ref_continuous_otlp` (`continuous`) |
| `observability[]` `destination_config.event_log_tables` (E13 continuous) | `dest_ref_continuous_otlp` |
| `observability[]` `auth.{type,credentials}` + `retry` + `timeout_ms` | `dest_ref_continuous_otlp` |

---

## 6. Validity

`onboarding_spec_full_reference.json` parses as JSON (`python -c "import json;
json.load(open(...))"`) and validates with **zero errors** against
`onboarding_spec.schema.json` under the standard Python `jsonschema` package
(`Draft202012Validator`). It has not been run through `spec_validator.py` itself (that
needs a live `SparkSession` for the `EXPLAIN`-based SQL checks), so treat the `EXPLAIN`
based checks on `transformation_sql`/`transform_sql` as unverified — every other field on
this document has been checked directly against the validator's source.

The two pipeline-mode flows added in v1.5.0 were hand-checked against the cross-array
placement rules, **not executed**: `recon_ref_pipeline_mode.source_config.table`
(`ref_bronze.ref_zerobus_full`) is ingestion flow `df_ref_zerobus_full`'s target and its
`cdc_load_strategy` is `APPEND`, and `recon_ref_pipeline_audit_only_mode.source_config.table`
(`ref_silver.ref_append_full`) is transformation flow `ts_ref_append_full`'s target, also
`APPEND` — so both satisfy V-CYC-1 and V-CYC-7. Neither flow's `append_target_table` (the
audit-only flow omits it) collides with any in-spec target, ingestion source, or sibling
`target_configs[].table`, so no V-CYC-2/3/5 finding fires at either severity. That is a
source-reading argument, not a run: this file has never been onboarded.
