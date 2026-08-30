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
| `ingestion_flows` | array | see [17 §1](../docs/17_onboarding_template_reference.md#1-top-level-spec-shape) | 3 objects, §2 below |
| `transformation_flows` | array | — | 13 objects, §3 below |
| `reconciliation_flows` | array | — | 2 objects, §4 below |

`pipeline_parameters` deliberately mixes value types here (`"US"` a string, `0` an int,
`"2026-01-01"` a string date, `true` a bool) to make the point explicit: `check_dict` only
validates the container is an object — every value is accepted as-is and rendered as a SQL
literal via `str()` at substitution time (strings single-quoted; everything else passed
through bare). The baseline template only ever used string values; this file shows a
non-string value is equally valid.

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
| `ts_ref_pure_sink_pgp_zip` | `sink` | `APPEND` | `sink_config.format: "pgp_zip"` with **every** `post_export_archive` field: `output_zip_path`, `export_file_name_format` (a `str.format()` template — new vs. baseline), `secret` (AES ZIP password), and `pgp_encryption` with all three of `recipient_public_key_secret`, `sign_with_private_key_secret`, **and** `sign_passphrase_secret` together (new — the baseline template signs without a passphrase) |
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

## 4. `reconciliation_flows[]` — 2 flows

> **v1.3.0:** reconciliation is restricted to Delta **tables** only. All three
> `target_configs[]` entries now use `type: "table"`; the previous `file` and `sink`
> examples were converted to read-back tables, which is the documented migration path.
>
> **v1.4.0:** `recon_mode` is gone (both `"triggered"` and `"continuous"`). Reconciliation
> is triggered-only: every side is read as a batch, a streaming side runs under
> `trigger(availableNow=True)`, and each run drains what is there and stops. To reconcile
> more often, schedule the job more often — there is no standing-stream mode to opt into.

Full narrative: [`docs/07_reconciliation.md`](../docs/07_reconciliation.md). Field
reference: [17 §5](../docs/17_onboarding_template_reference.md#5-reconciliation_flows).

### 4.1 `recon_ref_full_reference`

**`source_config`** (`type: "table"`, the default, set explicitly here):

| Attribute | Sample value |
|---|---|
| `table` | `"{{catalog}}.ref_bronze.ref_reconciliation_baseline"` |
| `read_mode` | `"batch"` |
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
value — `"fail"` is exercised by the flow above).

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
| `sink_config.format: "pgp_zip"` + `export_file_name_format` + `sign_passphrase_secret` | `ts_ref_pure_sink_pgp_zip` |
| `sink_config.write_mode` (dead field) | `ts_ref_external_sink_delta` |
| `dq_config.rules[].action` — all 4 values | `df_ref_autoloader_full`, `ts_ref_append_full` |
| `governance_tags.column_tags`/`table_tags` | `df_ref_autoloader_full`, `ts_ref_append_full` |
| `watermark` (stream-stream join) | `ts_ref_append_full` |
| `UNION ALL` | `ts_ref_union_all` |
| `reconciliation` `type: "table"` (**the only allowed value as of v1.3.0** — `file`/`sink` were removed; read such output into a Delta table first) | `recon_ref_full_reference.target_configs[]` (all 3 targets are now `table`) |
| `reconciliation` `comparison_direction` — all 3 values | `recon_ref_full_reference` (`both`, `source_to_target`, `target_to_source`), `recon_ref_warn_minimal` (`source_to_target`) |
| `reconciliation` `error_handling.on_failure` — both values | `recon_ref_full_reference` (`fail`), `recon_ref_warn_minimal` (`warn`) |
| `reconciliation` `hash_precomputed` | `recon_ref_full_reference` (both `true` and `false`) |
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
| `reconciliation` `logging_config.{run_log_capture,mismatch_log_capture}` (E12) | both reconciliation flows |
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
