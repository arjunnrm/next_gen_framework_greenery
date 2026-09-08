# 📖 FlowX — Master Reference Index & Attribute Dictionary

> **Purpose**: Single authoritative lookup dictionary indexing every configuration attribute, CDC strategy, target type, technical concept, error code, and framework-generated column across FlowX.
>
> **Navigation**: Use `Ctrl+F` to search for any attribute or concept keyword.

---

## 📑 Master Section Navigation

1. [Top-Level Spec Schema](#1-top-level-spec-schema)
2. [Ingestion Flow Schema (`ingestion_flows[]`)](#2-ingestion-flow-schema)
3. [Source Config Reference (`source_config`)](#3-source-config-reference)
4. [Target Config & CDC Reference (`target_config`)](#4-target-config--cdc-reference)
5. [Data Quality Config (`dq_config`)](#5-data-quality-config)
6. [Governance & Tagging (`governance_tags`)](#6-governance--tagging)
7. [Transformation Flow Schema (`transformation_flows[]`)](#7-transformation-flow-schema)
8. [Reconciliation Flow Schema (`reconciliation_flows[]`)](#8-reconciliation-flow-schema)
9. [Observability Config Schema (`observability[]`)](#9-observability-config-schema)
10. [Framework-Generated Columns](#10-framework-generated-columns)
11. [Template Variables & Parameter Substitution](#11-template-variables--parameter-substitution)
12. [CDC Load Strategies Quick Reference](#12-cdc-load-strategies-quick-reference)
13. [Target Types Quick Reference](#13-target-types-quick-reference)
14. [Official Databricks Documentation Index](#14-official-databricks-documentation-index)
15. [**🔐 Deterministic Hashing & Determinism (v1.3.0)**](11_hashing_and_determinism.md) — the one canonical `__framework_hash_key`/`__framework_hash_value` construction, a reproducible Spark SQL snippet, and the breaking-change migration checklist.
16. [**🧩 Module Permutation Matrix (v1.3.0)**](12_module_permutation_matrix.md) — which source types, CDC strategies, reconciliation scopes, and observability modes legally combine, plus a consolidated list of unsupported combinations.
17. [**🔭 Framework Observability, AI/BI & Genie**](17_framework_observability_and_genie.md) — the `<catalog>.observability` semantic layer: 11 views joining FlowX control metadata to the Databricks system tables, the `configuration['dataflow.group.id']` join key and the `dataflow_group_id` job tag, the 9-page AI/BI dashboard, the four `AI_FORECAST` rules, the Genie space, and the per-group documentation generator. **No spec attribute** — nothing here is configured through an onboarding spec.

---

## 1. Top-Level Spec Schema

The root structure of an onboarding JSON/YAML file:

| Attribute | Type | Required | Default | Allowed Values | Description | Official Reference |
|---|---|---|---|---|---|---|
| `dataflow_group_id` | `string` | **Yes** | — | Non-empty alphanumeric string + underscores (e.g. `"dfg_finance_txn_ingest"`) | Unique identifier for this pipeline group. All control-table rows in this spec are upserted under this ID. | — |
| `pipeline_parameters` | `object` | No | `{}` | Key-value map (e.g. `{"filter_country": "US", "min_amount": "0"}`) | Runtime parameters substituted into `${param}` placeholders in SQL queries, conditions **and file paths** (`source_config.path`, `target_config`), resolved fresh on every pipeline update. | [Parameters Guide](03_transformation_and_cdc.md#5-parameter-substitution-param-catalog-env) |
| `spark_config` | `object` | No | `{}` | Keys **must** start with `"spark."`; values `string`\|`number`\|`boolean` (e.g. `{"spark.sql.shuffle.partitions": "auto"}`) | **New in v1.3.0.** Group-scoped Spark session configuration, a sibling of `pipeline_parameters` (never nested inside a flow) — distinct from it: this calls `spark.conf.set(...)`, `pipeline_parameters` does `${param}` string substitution. Sits in the middle of a three-layer precedence chain: `engine/spark_config.py`'s `FRAMEWORK_SPARK_DEFAULTS` < this block < the pipeline resource's own `configuration: dataflow.spark.conf` (JSON-encoded string). A malformed key/value or a static Spark config is logged at `WARNING` and skipped, never fails the update. **Known gap:** `onboarding/metadata_upsert.py` does not yet persist this block to `dataflow_group_spec.spark_config_json`, so a validated `spark_config` currently has no effect through the standard onboarding flow — only the framework-default and pipeline-resource layers are live. | [Hierarchical Spark Configuration](01_platform_architecture.md#5-hierarchical-spark-configuration) |
| `ingestion_flows` | `array` | At least 1 flow array must be non-empty | `[]` | Array of ingestion flow objects | Bronze-layer ingestion definitions. See [§2](#2-ingestion-flow-schema). | [Auto Loader](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html) |
| `transformation_flows` | `array` | At least 1 flow array must be non-empty | `[]` | Array of transformation flow objects | Silver/Gold transformation definitions. See [§7](#7-transformation-flow-schema). | [apply_changes](https://docs.databricks.com/en/delta-live-tables/cdc.html) |
| `reconciliation_flows` | `array` | At least 1 flow array must be non-empty | `[]` | Array of reconciliation flow objects | Cross-dataset comparison and self-healing definitions. See [§8](#8-reconciliation-flow-schema). | [Reconciliation Guide](07_reconciliation_engine.md) |
| `source_plane` | `object` | No | `{"materialize": "always", "catalog": null, "schema": null}` | `materialize`: `"always"`\|`"auto"`; `catalog`/`schema`: string or `null` | **New in v1.5.0; default changed in v1.7.3.** Read-once policy for the whole group. Every physical source — ingestion source, transformation input, both reconciliation sides — is routed through the source plane so one locator is read once per update. Under v1.7.3's **Single-Read architectural mandate** `"always"` is the default and the only behaviour: every external identity is materialized into its own base node regardless of fan-out (N sources → N base nodes). `"auto"` is still accepted but resolves to `"always"` (it formerly meant "a node only at fan-out ≥ 2"). `"never"` was **removed** and is rejected at onboarding time with a migration message, and again at plan time for control-table rows written before the mandate. v1.6.0: `null` catalog/schema mean the shared node is **not published** — it becomes a pipeline-scoped temporary table (set both to publish it; pre-v1.6.0 `null` meant the pipeline's own schema). Persisted to `dataflow_group_spec.source_plane_config_json`. | [Read-once source plane](01_platform_architecture.md#7-the-read-once-source-plane) |
| `observability` | `array` | No | `[]` | Array of observability destination objects | Telemetry export destinations (OTel, Databricks Volumes). Does not count toward the non-empty flow requirement. | [Observability Guide](08_observability_and_telemetry.md) |

---

## 2. Ingestion Flow Schema

Objects inside `ingestion_flows[]`:

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `dataflow_id` | `string` | **Yes** | — | Unique flow identifier (e.g. `"df_orders_bronze"`) | Unique ID for this ingestion flow. Referenced by transformation flows. |
| `source_system` | `string` | No | — | Free text (e.g. `"SAP_ERP"`) | Descriptive metadata stored in control tables. |
| `source_database` | `string` | No | — | Free text | Descriptive metadata stored in control tables. |
| `source_table_name` | `string` | No | — | Free text | Descriptive metadata stored in control tables. |
| `source_description` | `string` | No | — | Free text | Stored as the target Delta table's `COMMENT`. Supports `{{catalog}}` placeholders. |
| `source_type` | `string` | **Yes** | — | `"autoloader"`, `"zerobus"`, `"asn1"` | Ingestion reader engine used to consume data. |
| `target_catalog` | `string` | **Yes** | — | e.g. `"{{catalog}}"`, `"poc"` | Unity Catalog catalog for the target table. |
| `target_schema` | `string` | **Yes** | — | e.g. `"bronze_finance"` | Target schema/database name. |
| `target_table` | `string` | **Yes** | — | e.g. `"orders_raw"` | Target Delta table name. |
| `target_type` | `string` | **Yes** | — | `"streaming_table"`, `"materialized_view"`, `"batch_table"`, `"external_sink"`, `"sink"` | Type of Lakeflow dataset or egress sink to register. |
| `source_config` | `object` | No | `{}` | See [§3](#3-source-config-reference) | Source reader configuration. |
| `target_config` | `object` | No | `{}` | See [§4](#4-target-config--cdc-reference) | Target storage, CDC, encryption, and sink configuration. |
| `dq_config` | `object` | No | `{}` | See [§5](#5-data-quality-config) | Data quality rules and quarantine settings. |
| `governance_tags` | `object` | No | `{}` | See [§6](#6-governance--tagging) | Table and column governance tags. |

---

## 3. Source Config Reference

Fields within `source_config`:

### Common Fields (All `source_type`s)
| Attribute | Type | Required | Default | Allowed Values | Description | Databricks Link |
|---|---|---|---|---|---|---|
| `capture_technical_metadata` | `boolean` | No | `true` | `true`, `false` | When `true`, automatically injects source file metadata (`__framework_source_file_name`, `__framework_source_file_size`, `__framework_source_file_modification_time`) and `__framework_ingestion_timestamp_utc`. | — |
| `explode_columns` | `array<string>` | No | *(see Description — absent and `[]` are NOT equivalent)* | List of column names, or an explicit empty list | **ABSENT** (or explicit JSON `null`) is a schema-preserving pass-through — nothing exploded. **PRESENT-BUT-EMPTY (`[]`)** auto-flattens every nested struct and explodes every array (v1.3.0; equivalent to `auto_flatten_all: true`). A populated list scopes flattening to exactly those columns. This absent-vs-empty distinction is **load-bearing**: it is what prevents silent cartesian row explosion on an unconfigured source — see [§6](02_ingestion_and_sources.md#6-json-explode-auto-flatten--json-string-column-parsing). | — |
| `json_string_columns` | `array<string \| object>` | No | `[]` | `"col_name"` or `{"column": "col_name", "schema_ddl": "struct<...>"}` | **New in v1.3.0.** STRING columns holding a JSON document, parsed via `from_json` into a struct immediately before `explode_columns`/auto-flatten runs — gives Parquet/CSV/Delta/Zerobus sources the same struct-flatten/array-explode treatment native JSON sources get. `schema_ddl` is the recommended form (works batch and streaming); the no-`schema_ddl` shorthand needs Databricks' inferring `from_json` and only works on a **streaming** DataFrame with a checkpoint — it raises `FrameworkConfigError` on a batch/materialized source. | [§6](02_ingestion_and_sources.md#json-held-in-a-string-column-json_string_columns) |
| `auto_flatten_all` | `boolean` | No | `false` | `true`, `false` | When `true`, recursively flattens all nested structs and explodes all arrays, regardless of `explode_columns`. Same effect as `explode_columns` being present-but-empty (see above). | — |
| `remove_dups` | `boolean` | No | `false` | `true`, `false` | **New in v1.3.0.** Full-row `dropDuplicates` over every column except `__framework_*`-prefixed columns and `_rescued_data`/`_metadata`/`_object_metadata`/`_asn1_decode_error`. Runs after explode/auto-flatten, before `data_standardization_sql`. **On a streaming source with no `dedup_watermark`, this keeps unbounded state** — see [§7](02_ingestion_and_sources.md#7-full-row-streaming-deduplication-remove_dups). Ignored (INFO-logged) concern-free on a batch source. | — |
| `dedup_watermark.event_time_column` | `string` | Only meaningful with `remove_dups: true` | — | A `TIMESTAMP` column on the ingested DataFrame | **New in v1.3.0.** Bounds dedup state on a streaming source via `withWatermark(...).dropDuplicatesWithinWatermark(...)`. Configuring this without `remove_dups: true` is a validation error. | — |
| `dedup_watermark.delay_threshold` | `string` | Only meaningful with `remove_dups: true` | — | Spark interval string, e.g. `"2 hours"` | **New in v1.3.0.** Paired with `event_time_column` above. | — |
| `data_standardization_sql` | `array<string>` | No | `[]` | SQL expressions ending in `AS <col>` | Restricted SQL expressions for per-column standardization (e.g. `"trim(code) AS code"`). Forbidden keywords: `SELECT, FROM, JOIN, WHERE`. | — |
| `column_normalization.enabled` | `boolean` | No | `false` | `true`, `false` | The **only** switch for column-name normalization. **v1.4.0:** the legacy boolean `normalize_column_names` is REMOVED and is rejected by onboarding with a migration message — absent object, absent key and explicit `false` all mean off. | [Column Normalization](02_ingestion_and_sources.md#column-normalization) |
| `column_normalization.case` | `string` | No | `"lower"` | `"lower"`, `"preserve"`, `"upper"` | **New in v1.3.0.** Case-fold step only. Character normalization (trim → replace `[^A-Za-z0-9_]` with `_` → collapse repeated `_` → strip edge `_`) is **always** applied regardless of `case`; duplicate-name collision detection **always** runs on the lowercased projection of the produced names (Unity Catalog/Spark resolve columns case-insensitively), so `preserve`/`upper` remain safe. | [§8](02_ingestion_and_sources.md#column-normalization) |
| `schema_config_path` | `string` | No | — | Volume file or directory path | External JSON/YAML defining explicit column types, nullability, comments, and renames. Resolves directories to latest file. | [Schema Config Guide](02_ingestion_and_sources.md#explicit-schema-configuration-schema_config_path) |

### Auto Loader Specific (`source_type: "autoloader"`)
| Attribute | Type | Required | Default | Allowed Values | Description | Databricks Link |
|---|---|---|---|---|---|---|
| `path` | `string` | **Yes** | — | `/Volumes/...` or `s3://...`, `abfss://...` | Source storage location. | [Auto Loader Paths](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html) |
| `format` | `string` | No | `"json"` | `"json"`, `"csv"`, `"parquet"`, `"avro"`, `"text"`, `"binaryFile"` | Data file format. | [Cloud Files Formats](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html) |
| `schema_location` | `string` | No | Derived | Volume path | Schema checkpoint location for Auto Loader inference. | [Schema Inference](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/schema-detection-evolution.html) |
| `schema_evolution_mode` | `string` | No | `"addNewColumns"` | `"addNewColumns"`, `"addNewColumnsWithTypeWidening"`, `"rescue"`, `"failOnNewColumns"`, `"none"` | Controls schema evolution behavior. Maps to `cloudFiles.schemaEvolutionMode`. | [Schema Evolution](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html) |
| `file_pattern` | `string` | No | — | Glob or regex | File filter pattern (e.g. `"*.csv"`). | [File Name Pattern](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html) |

#### Landing Retention Policy (`landing_retention_policy`, Auto Loader / ASN.1 only)

Maps to Auto Loader's own `cloudFiles.cleanSource*` options. **Never applied to `source_zip_handling` or the raw ZIP pre-extraction path** — configuring it on a `zerobus` source is a validation error (no landing zone to clean). Full behavior, defaults, and the degrade-to-off rationale: [§2](02_ingestion_and_sources.md#landing-retention-policy-landing_retention_policy).

| Attribute | Type | Required | Default | Allowed Values | Description | Databricks Link |
|---|---|---|---|---|---|---|
| `landing_retention_policy.clean_source` | `string` | No | `"off"` | `"archive"`, `"delete"`, `"off"` | Post-ingestion file cleanup. **v1.3.0:** `"archive"` with an empty/absent `archive_path` **degrades to `"off"`** with a `WARNING` — a documented no-op, not a validation error. `"delete"` ignores `archive_path` entirely (not an error). | [Clean Source Options](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html) |
| `landing_retention_policy.archive_path` | `string` | No (never required, even for `"archive"`) | — | Volume path | Destination for archived files. Meaningful only for `clean_source: "archive"`. | — |
| `landing_retention_policy.retention_days` | `integer` | No | **`7`** (v1.3.0; was previously "whatever Auto Loader's own default is") | `>= 0` | Age threshold before a file becomes archive/delete-eligible. `0` is valid and means "eligible as soon as Auto Loader commits the file." | — |

### ASN.1 Specific (`source_type: "asn1"`)
| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `asn1_schema_path` | `string` | **Yes** | — | `/Volumes/...` | Path to ASN.1 `.asn` or `.asn1` schema definition. |
| `codec` | `string` | No | `"ber"` | `"ber"`, `"der"` | ASN.1 binary encoding rule. |
| `top_level_type` | `string` | **Yes** | — | Type name | Target ASN.1 PDU type name to decode. |

### Source ZIP Handling (`source_zip_handling`)

> **Field-name correction:** the correct spellings are `source_zip_path`, `zip_file_pattern`, and `target_volume_path`. `file_pattern` is a *different*, top-level `source_config` field (§2 above, mapping to Spark's generic file-source `pathGlobFilter`) — it has no effect inside `source_zip_handling` and naming it there silently matches nothing.

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `enabled` | `boolean` | No | `false` | `true`, `false` | Enables in-flight ZIP extraction before reading. |
| `source_zip_path` | `string` | Yes, when `enabled` | — | Volume directory | Landing **directory** containing archives — never a single file. |
| `zip_file_pattern` | `string` | Yes, when `enabled` | — | Glob, e.g. `"orders_*.zip"` | Selects which archive(s) in `source_zip_path` this update processes (case-sensitive `fnmatchcase`). |
| `target_volume_path` | `string` | Yes, when `enabled` | — | Volume path | Extraction destination. |
| `member_format` | `string` | No | `"zip"` | `"zip"`, `"gzip"` | **v1.7.4.** The archive's *container*, orthogonal to any decryption layer. `"zip"` is a real archive with N named members (`pyzipper`); `"gzip"` is a single compressed stream with no member table, which `pyzipper` cannot open. An **unencrypted** `.gz` needs no `source_zip_handling` at all — Spark decompresses it natively on read — so use `"gzip"` only for an **encrypted** one, e.g. `.csv.gz.gpg`. |
| `pre_extraction_decryption` | `object` | No | — | PGP decryption config | Optional PGP decryption applied to an encrypted landing file before extraction. `type` is `"pgp"` (encrypted to a recipient keypair — takes `private_key_secret`, plus `passphrase_secret` if that key is itself protected) or, **v1.7.4**, `"pgp_symmetric"` (encrypted with a shared passphrase — takes `passphrase_secret` only, and **rejects** `private_key_secret`). Neither type can open the other's messages. A third field, `secret_passphrase`, is **not** PGP at all: it is the AES password on the ZIP archive itself, and is rejected when `member_format` is `"gzip"`. See [§5 §4.1](05_security_and_cryptography.md). |
| `delete_source_after_extract` | `boolean \| object` | No | `{"action": "delete_now"}` | `true`, `false`, `{"action": "delete_now"}`, `{"action": "delete_after_x_days", "days": <int >= 0>}` | **v1.3.0:** accepts a nested action object in addition to the legacy boolean (`true` == `delete_now`, `false` == never delete). `delete_after_x_days` does **not** delete this run's own just-extracted archive — it sweeps `source_zip_path` at extraction time for archives matching `zip_file_pattern` older than `days` days, excluding only archives that failed extraction this run. See [§5](02_ingestion_and_sources.md#deleting-the-source-archive-after-extraction-delete_source_after_extract). |

---

## 4. Target Config & CDC Reference

Fields inside `target_config`:

> **Field-name correction (drift predating v1.3.0, fixed here):** the correct spellings are `partition_columns` and `liquid_clustering_columns` — `partition_by` and `liquid_clustering` are **not** recognized field names, in this release or any prior one. `compute_hash_key`/`compute_hash_value` also do not exist as booleans anywhere in `onboarding/spec_validator.py` or `onboarding_spec.schema.json`; the one real switch controlling both `__framework_hash_key` and `__framework_hash_value` together is `generate_hash_columns` (below).

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `cdc_load_strategy` | `string` | **Yes** | — | `"APPEND"`, `"TRUNCATE_AND_LOAD"`, `"SCD1"`, `"SCD2"`, `"SCD3"`, `"FULL_SNAPSHOT_CDC"` | CDC merge strategy. **v1.4.0:** `"FULL_SNAPSHOT_CDC_NO_PK"` is REMOVED and rejected by name. See [§12](#12-cdc-load-strategies-quick-reference). |
| `primary_keys` | `array<string>` | **Yes for SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC** | `[]` | Column name list | Business key columns for merge matching. **v1.4.0:** required by every CDC-dispatched strategy — there is no framework-generated substitute since the surrogate-key engine was removed. A source with genuinely no key uses `TRUNCATE_AND_LOAD`. |
| `sequence_by_column` | `string` | No | Falls back to `__framework_ingestion_timestamp_utc` | Column name | Ordering column (e.g. `updated_at`) to resolve out-of-order records. |
| `columns_to_check` | `array<string>` | No | `[]` (compare all applicable columns) | Column name list | Subset of columns evaluated for value changes in SCD1/SCD2/SCD3/FULL_SNAPSHOT. |
| `columns_to_exclude` | `array<string>` | No | `[]` | Column name list | **Comparison-only** — excludes columns from change-detection hashing; never drops them from the target table. Only meaningful for `cdc_load_strategy` in `{SCD1, SCD2, SCD3}`. |
| `generate_hash_columns` | `boolean` | No | `true` | `true`, `false` | Injects `__framework_hash_key` (SHA-256 of `primary_keys`, in order) and `__framework_hash_value` (SHA-256 of the resolved comparison columns, sorted). Both are SHA-256 only — see [11_hashing_and_determinism.md](11_hashing_and_determinism.md) for the canonical construction. |
| `storage_format` | `string` | No | `"delta"` | `"delta"`, `"iceberg"` | Storage format. `"iceberg"` only valid when `target_type == "batch_table"` (activates UniForm read-compatibility). |
| `partition_columns` | `array<string>` | No | `[]` | Column name list, **explicitly empty is legal** | Physical table partitioning columns. **An explicitly empty array (`[]`) configures the target with NO partitioning and is never an error** — behaviorally identical to omitting the field, but diagnostically distinct (v1.3.0: logs an `INFO` recording the deliberate opt-out). Only takes effect for `cdc_load_strategy` in `{APPEND, TRUNCATE_AND_LOAD}` — inert (not an error) on every other strategy. |
| `liquid_clustering_columns` | `array<string>` | No | `[]` | Column name list, **`maxItems: 3`** | Enables Delta Liquid Clustering. **v1.3.0: at most 3 columns** (Delta Liquid Clustering's own limit) — more is a hard validation error at onboarding time, re-enforced at runtime as defense in depth. Same APPEND/TRUNCATE_AND_LOAD-only scope as `partition_columns`. |
| `empty_target_if_source_empty` | `boolean` | No | `false` | `true`, `false` | **Accepted but NOT enforced (withdrawn 2026-08-29).** `TRUNCATE_AND_LOAD` only (a validation error on any other strategy). It was intended so a zero-record source would leave the target's contents untouched, but preserving them makes the target read itself, which Lakeflow rejects as a graph cycle — every `TRUNCATE_AND_LOAD` pipeline failed graph construction. The field stays valid in specs so existing onboarding files keep working; it currently changes nothing at runtime. See `docs/03_transformation_and_cdc.md`. |
| `encrypted_columns` | `array<object>` | No | `[]` | Array of crypto objects | Encrypts named columns with AES. Each entry: `column_name` (required), `secret` (required), `output_column` (default: `column_name`), `mode` (`GCM` default, `CBC`, `ECB`), and **`source_data_type`** (optional, new in v1.4.0). See [Security Guide](05_security_and_cryptography.md). |
| `encrypted_columns[].source_data_type` | `string` | No | The type Spark observes at encryption time | A Spark type string, e.g. `"string"`, `"decimal(18,2)"`, `"timestamp"` | **New in v1.4.0.** The column's original data type, before encryption replaces it with ciphertext binary. Becomes the Unity Catalog `original_data_type` column tag, which is what a downstream `source_inputs[].decrypted_columns[].cast_to_type` is validated against. **Omitting it is safe** — the observed type is used, exactly as before v1.4.0, so no existing spec needs editing. Declaring it makes a silent source type change fail loudly at encryption time (declared vs observed compared case-insensitively; a mismatch raises `CryptoError`) instead of silently re-tagging and breaking the decrypt side later. |
| `sink_config` | `object` | **Yes for sinks** | — | Object | Destination configuration for `target_type: "sink"` / `"external_sink"`. |

---

## 5. Data Quality Config

Fields inside `dq_config`:

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `rules` | `array<object>` | **Yes** | `[]` | List of rule objects | Expectation definitions (`name`, `sql_condition`, `action`). |
| `rules[].action` | `string` | **Yes** | — | `"warn"`, `"drop"`, `"fail"`, `"quarantine"` | Action taken on expectation violation. |
| `quarantine_table` | `string` | **Yes if quarantine used** | — | Table name (e.g. `"orders_quarantine"`) | Destination Delta table for quarantined records. |
| `record_id_column` | `string` | No | — | Column name | Source record identifier propagated to quarantine diagnostics. |

---

## 6. Governance & Tagging

Fields inside `governance_tags`:

| Attribute | Type | Required | Default | Description |
|---|---|---|---|---|
| `table_tags` | `object` | No | `{}` | String-to-string dictionary applied to target Delta table via `ALTER TABLE ... SET TAGS`. |
| `column_tags` | `object` | No | `{}` | Map of `column_name -> {tag_key: tag_value}` applied to specific table columns. |

---

## 7. Transformation Flow Schema

Objects inside `transformation_flows[]`:

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `dataflow_id` | `string` | **Yes** | — | Unique identifier | Unique ID for this transformation flow. |
| `flow_step_id` | `string` | **Yes** | — | Unique step identifier | Identifier for this specific pipeline DAG step. |
| `source_inputs` | `array<object>` | **Yes** | `[]` | List of inputs | Upstream tables or views consumed by this transformation. |
| `source_inputs[].table` | `string` | **Yes** | — | `catalog.schema.table` or view name | Name of input table/view. |
| `source_inputs[].alias` | `string` | No | — | SQL identifier | Alias used in `transformation_sql`. |
| `source_inputs[].decrypted_columns` | `array<object>` | No | `[]` | Decryption configs | Columns to decrypt on input before running transformation SQL. |
| `transformation_sql` | `string` | **Yes** | — | Valid Spark SQL | SQL query defining the transformation logic. |
| `target_catalog` | `string` | **Yes** | — | e.g. `"{{catalog}}"` | Unity Catalog catalog for target table. |
| `target_schema` | `string` | **Yes** | — | Target schema name | Schema for target table. |
| `target_table` | `string` | **Yes** | — | Target table name | Target table name. |
| `target_type` | `string` | **Yes** | — | `"streaming_table"`, `"materialized_view"`, `"batch_table"`, `"sink"`, `"external_sink"` | Output dataset type. |
| `target_config` | `object` | No | `{}` | See [§4](#4-target-config--cdc-reference) | CDC, storage, and sink configuration. |
| `dq_config` | `object` | No | `{}` | See [§5](#5-data-quality-config) | Data quality rules and quarantine settings. |
| `governance_tags` | `object` | No | `{}` | See [§6](#6-governance--tagging) | Governance tags applied to output table. |

---

## 8. Reconciliation Flow Schema

> **This table is rebuilt wholesale from `onboarding/spec_validator.py::_validate_reconciliation_flows` / `_validate_reconciliation_dataset_config` / `_validate_reconciliation_target_configs` / `_validate_logging_config`.** A prior version of this document used field names (`source_dataset`, `target_datasets`, `dataset_type`, `table_name`, `primary_keys` at flow level, `enable_self_healing`, `failure_mode`) that never matched the actual validator — do not carry them forward. Full narrative treatment, architecture diagram, and the two-tier verification mechanism: [`07_reconciliation_engine.md`](07_reconciliation_engine.md).

Objects inside `reconciliation_flows[]`:

```json
{
  "reconciliation_id": "recon_orders_daily",
  "two_tier_verification": true,
  "logging_config": { "run_log_capture": true, "mismatch_log_capture": true },
  "source_config": { "type": "table", "table": "poc.bronze_sales.orders_raw", "read_mode": "batch", "task_run_id_column": "__framework_pipeline_run_id", "hash_precomputed": false },
  "target_configs": [
    { "target_id": "orders_silver", "type": "table", "table": "poc.silver_sales.orders", "read_mode": "batch", "hash_precomputed": true, "comparison_direction": "both", "append_target_table": "poc.bronze_sales.orders_raw" }
  ],
  "match_keys": ["order_id"],
  "compare_columns": ["status", "total"],
  "error_handling": { "on_failure": "fail" }
}
```

### 8.1 Flow-level fields

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `reconciliation_id` | `string` | **Yes** | — | Unique ID | Unique identifier for this reconciliation flow. |
| `execution_mode` | `string` | No | `"job"` | `"job"`, `"pipeline"`, `"pipeline_audit_only"` | **New in v1.5.0.** `"job"` runs the flow as today's `05_reconciliation_engine.py` job task. `"pipeline"` registers it as a third flow type inside the owning dataflow group's Lakeflow pipeline update — published `__classified`/`__metrics`/`__mismatch` datasets plus the in-graph heal lane. `"pipeline_audit_only"` registers the comparison and expectations in-pipeline while healing stays in job mode. Requires `dataflow_group_id` and `read_mode: "batch"` in the two pipeline modes. See [`07_reconciliation_engine.md` §11](07_reconciliation_engine.md#11-execution-modes-job-pipeline-pipeline_audit_only). |
| `publish_schema` | `string` | No | none — absent means **publish nothing** (v1.7.07) | Schema name | **New in v1.5.0; semantics changed in v1.7.07.** Where the published reconciliation datasets (`recon__*__metrics`/`__mismatch`, a healing flow's `_src`/`_tgt`) land, inside the pipeline's own catalog. Absent ⇒ those datasets are pipeline-scoped temporary tables and nothing is exported. Required when a capture flag is true or a `pipeline`-mode flow heals. Persisted to `reconciliation_flow_spec.publish_schema`. **Rejected on presence when `execution_mode` is `"job"`.** |
| `dq_config` | `object` | No | SQL `NULL` | Same shape as an ingestion/transformation `dq_config` (see [§5](#5-data-quality-config)) | **New in v1.5.0.** Expectations attached to the one-row `__metrics` dataset — the first declarative way a reconciliation threshold can fail a pipeline update, e.g. `{"rules": [{"name": "no_value_drift", "expr": "value_drift_count = 0", "action": "fail"}]}`. Additive: it does not repurpose `error_handling.on_failure`. `action: "quarantine"` is rejected, and the block is **rejected on presence when `execution_mode` is `"job"`**. |
| `two_tier_verification` | `boolean` | No | `true` | `true`, `false` | **New in v1.3.0.** A cheap Phase 1 per-side fingerprint (`row_count` + XOR-fold of `__framework_hash_key`/`__framework_hash_value`) short-circuits the whole comparison when both sides match; Phase 2 (full hash-key join + column-level drift) runs only on a Phase 1 mismatch. `false` always runs Phase 2. |
| `logging_config.run_log_capture` / `.mismatch_log_capture` | `boolean` | No | `true` / `true` | `true`, `false` | Per-flow log-write gates, overridable at runtime by the `recon_run_log_capture`/`recon_mismatch_log` job parameters (highest precedence). v1.6.0: `run_log_capture` also gates `reconciliation_result` and (pipeline mode) the `recon__*__metrics` dataset registration; `mismatch_log_capture` gates the `__mismatch` dataset. Both `false` == the flow persists only to its business targets. |
| `source_config` | `object` | **Yes** | — | Dataset config | Baseline source dataset. See [§8.2](#82-per-side-dataset-fields-source_config--each-target_configs-entry). |
| `target_configs` | `array<object>` (non-empty) | **Yes** | — | Dataset configs | One or more comparison targets. See [§8.2](#82-per-side-dataset-fields-source_config--each-target_configs-entry) + [§8.3](#83-target-only-fields). |
| `match_keys` | `array<string>` | **Yes** | — | Column list | Join keys for row-level matching (renamed from the never-real `primary_keys`). |
| `compare_columns` | `array<string>` | No | None (key-presence-only matching) | Column list | Columns evaluated for value drift once matched by key. |
| `error_handling.on_failure` | `string` | No | `"fail"` | `"fail"`, `"warn"` | Evaluated per target. `"fail"` stops the run on the first target failure; `"warn"` logs it and continues to the next target. **v1.5.0:** semantics unchanged, blast radius larger under `execution_mode: "pipeline"` — re-raising fails the whole **pipeline update**, not just a job task. |

### 8.2 Per-side dataset fields (`source_config` + each `target_configs[]` entry)

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `type` | `string` | No | `"table"` | **`"table"` only, as of v1.3.0** | **Breaking:** `"file"`/`"sink"` are rejected at both onboarding and runtime — reconciliation is Delta-tables-only. See [§8.4](#84-breaking-delta-tables-only-scope). |
| `table` | `string` | **Yes** | — | `catalog.schema.table` | Fully-qualified table name. Must resolve to a Delta table. |
| `read_mode` | `string` | No | `"batch"` | `"batch"`, `"streaming"` | At most one side of a given target's comparison may be streaming. **v1.5.0: `"streaming"` is rejected on presence when `execution_mode` is `"pipeline"`/`"pipeline_audit_only"`** — the in-pipeline comparison is a whole-snapshot batch classification. |
| `task_run_id_column` | `string` | No | — | Column name | **New in v1.3.0.** When the `task_run_id` job parameter is set, narrows this side's read to `task_run_id_column == task_run_id`, applied before `filter_condition`. Reconciliation is triggered-only as of v1.4.0, so the narrowing is unconditional whenever both are configured. Recommended value: `__framework_pipeline_run_id` (not hardcoded — a third-party table's own run-id column works the same way). **v1.5.0: rejected on presence when `execution_mode` is `"pipeline"`/`"pipeline_audit_only"`** — a Lakeflow update exposes no stable per-update run id, so the narrowing would be a silent no-op. |
| `hash_precomputed` | `boolean` | No | `false` | `true`, `false` | **An assertion, not an instruction.** `true` declares that this side ALREADY carries `__framework_hash_key`/`__framework_hash_value` from an upstream CDC-dispatched flow, and they are trusted verbatim; `false` computes both here and now, from `match_keys` (declared order) and `compare_columns` (sorted). It never precomputes anything. Only valid when `type == "table"`; a `true` on a dataset lacking the columns is a hard error. Full mechanics + the precondition that makes the trust safe: [`07_reconciliation_engine.md` §9](07_reconciliation_engine.md). |

### 8.3 Target-only fields

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `target_id` | `string` | **Yes** | — | Unique within the flow | Identifies this comparison target. |
| `comparison_direction` | `string` | No | `"both"` | `"source_to_target"`, `"target_to_source"`, `"both"` | Gates self-healing *action*, not classification (all four drift categories are always computed). |
| `append_target_table` | `string` | **Yes when `comparison_direction` is `"source_to_target"` or `"both"`** | — | `catalog.schema.table` | Where missing/drifted records are appended for self-healing. |

### 8.4 (Breaking) Delta-tables-only scope

`type` narrows from `{"table", "file", "sink"}` to `{"table"}` only. `file`/`sink` datasets are rejected both at onboarding and at runtime — read the file/sink output into a Delta table first, then reconcile against that table. Full narrative and verbatim error strings: [`07_reconciliation_engine.md` §7](07_reconciliation_engine.md#7-delta-tables-only-scope-breaking-change-in-v130).

---

## 9. Observability Config Schema

> **Field-name correction (drift predating v1.3.0, fixed here):** the onboarding spec uses **short** field names — `id`, `type`, `auth`, `retry` — verified against `onboarding/spec_validator.py::_validate_observability_destinations`. These are renamed on write into the `observability_config` control table's columns (`destination_id`, `destination_type`, `auth_config_json`, `retry_config_json`) — the spec-level names below are what a practitioner writes in the onboarding JSON; do not confuse the two. Full field-by-`type` breakdown and the triggered/continuous engine split: [`08_observability_and_telemetry.md` §2](08_observability_and_telemetry.md#2-observability-configuration-schema).

> **Two different things are called "observability" in this framework, and only one of them is a spec attribute.** This section — and `observability[]` — is **export-out telemetry**: OTLP payloads and Volume archives leaving the platform ([doc 08](08_observability_and_telemetry.md)). The **store-and-query** semantic layer in `<catalog>.observability` — 11 views over the Databricks system tables, the AI/BI dashboard, the Genie space and the documentation generator — has **no spec attribute at all**; it is provisioned by `01_setup` and documented in [doc 17](17_framework_observability_and_genie.md). Do not look for a spec key to turn it on.

Objects inside `observability[]`:

| Attribute | Type | Required | Default | Allowed Values | Description |
|---|---|---|---|---|---|
| `id` | `string` | **Yes** | — | Unique within the spec | Destination identifier (e.g. `"dest-dbx-prod-volume"`). Becomes `destination_id` in the control table; re-onboarding the same `id` for the same group upserts the same row. |
| `type` | `string` | **Yes** | — | `"DATABRICKS_VOLUME"`, `"OTLP_CONSUMER"` | Telemetry egress target. |
| `mode` | `string` | No | `"triggered"` | `"triggered"`, `"continuous"` | **New in v1.3.0.** `"triggered"`: served by the bounded post-update engine (`08_dlt_observability_engine.py`). `"continuous"`: served by the always-on streaming pipeline (`06_event_log_otel_streaming_pipeline.py`). A destination is served by exactly one engine — nullable in the control table, defaults to `"triggered"` for zero-migration compatibility with a pre-v1.3.0 table. |
| `enabled` | `boolean` | No | `true` | `true`, `false` | A disabled destination is dropped entirely by `config_loader.py` — callers never see it. |
| `destination_config` | `object` | **Yes** | — | Config object | Shape depends on `type` (`volume_path`/`file_format` for `DATABRICKS_VOLUME`; `endpoint`/`protocol`/`resource_attributes` for `OTLP_CONSUMER`; `compression` on both). |
| `destination_config.event_log_tables` | `array<string>` | **Yes when `mode` is `"continuous"`** (rejected on `"triggered"`) | — | Non-empty list of `catalog.schema.event_log_table` | **New in v1.3.0.** Fully-qualified event-log Delta tables the continuous pipeline streams and unions. |
| `auth` | `object` | No | none | `{"type": "BEARER_TOKEN"\|"API_KEY"\|"BASIC_AUTH"\|"NONE", "credentials": {...}}` | Credential values must be `env:<VAR>` or `secret:<scope>:<key>` references — a literal secret is a validation error. Ignored for `DATABRICKS_VOLUME`. |
| `retry` | `object` | No | `max_attempts: 3`, `backoff_multiplier: 2.0` | `{"max_attempts": int>=1, "backoff_multiplier": number>1}` | `OTLP_CONSUMER` only. |

---

## 10. Framework-Generated Columns

The framework automatically manages and injects the following technical metadata columns:

| Column Name | Injected When | Type | Description |
|---|---|---|---|
| `__framework_source_file_name` | Ingestion (`capture_technical_metadata: true`) | `STRING` | Origin file path (`_metadata.file_name`). |
| `__framework_source_file_size` | Ingestion (`capture_technical_metadata: true`) | `BIGINT` | File size in bytes (`_metadata.file_size`). |
| `__framework_source_file_modification_time` | Ingestion (`capture_technical_metadata: true`) | `TIMESTAMP` | Source file modification timestamp (`_metadata.file_modification_time`). |
| `__framework_ingestion_timestamp_utc` | Ingestion (`capture_technical_metadata: true`) | `TIMESTAMP` | UTC timestamp when record was ingested into Bronze. |
| `__framework_hash_key` | CDC (`generate_hash_columns: true`, default) | `STRING` | Deterministic **SHA-256** hash of `primary_keys` values, in the order given (never sorted). Full construction, normalization, and null-sentinel rules: [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md). |
| `__framework_hash_value` | CDC (`generate_hash_columns: true`, default) | `STRING` | Deterministic **SHA-256** hash of the resolved comparison columns (alphabetically sorted). See [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md). |
| `__framework_quarantine_validated_at` | DQ Quarantine routing | `TIMESTAMP` | UTC processing timestamp stamped when the record was routed into the quarantine companion table. |
| `__framework_dq_failed_rule_ids` | DQ Quarantine routing | `ARRAY<STRING>` | Array of `rule_id` values whose expression evaluated to false for this record. |
| `__framework_dq_failure_reasons` | DQ Quarantine routing | `ARRAY<STRING>` | Array of human-readable `"<rule_id>: failed expression ..."` strings, one per failed rule. |
| `__framework_dq_quarantine_flag` | DQ Quarantine routing | `BOOLEAN` | `true` when at least one quarantine-action rule failed; drives the split between the main and `_quarantine` tables. |
| `detected_at` | Reconciliation Mismatch Log | `TIMESTAMP` | UTC timestamp this mismatch was detected. Note: a `reconciliation_mismatch_log` table column, **not** a `__framework_`-prefixed column attached to a data table. |

---

## 11. Template Variables & Parameter Substitution

| Variable / Placeholder | Syntax | Where Allowed | Description |
|---|---|---|---|
| `{{catalog}}` | `{{catalog}}` | `target_catalog`, `path`, descriptions | Substituted with the deployment target catalog at onboarding time. |
| `{{env}}` | `{{env}}` | Paths, descriptions, tags | Substituted with the target environment (`dev`, `stage`, `prod`). |
| `${param}` | `${param_name}` | `transformation_sql`, `filter_condition`, **`source_config` (incl. `path`) and `target_config`** | Runtime parameter replaced with values from `pipeline_parameters`, resolved **fresh on every pipeline update** (not at onboarding) — so paths can be retargeted without re-onboarding. Applied by `flow_generators.py`, `source_plane.py` and `reconciliation/graph_registration.py`. `dq_config` is deliberately excluded (its expressions are SQL predicates, not paths). A missing parameter raises at planning time. |

---

## 12. CDC Load Strategies Quick Reference

| Strategy | Target Requirement | Updates | Deletes | History | Underlying Mechanism |
|---|---|---|---|---|---|
| `APPEND` | `streaming_table`, `batch_table` | No | No | Append-only | Direct Spark Streaming/Batch Append |
| `TRUNCATE_AND_LOAD` | `materialized_view`, `batch_table` | Full reload | Full reload | None | Atomic Table Overwrite (`CREATE OR REPLACE`) |
| `SCD1` | `streaming_table` | In-place | Soft delete if mapped | Current state only | `dlt.apply_changes(stored_as_scd_type="1")` |
| `SCD2` | `streaming_table` | Historical | Tracked (`__end_at`) | Full timeline | `dlt.apply_changes(stored_as_scd_type="2")` |
| `SCD3` | `materialized_view` | Previous + Current | No | 1 prior version (`current_*`, `previous_*`) | Custom SQL window/lag join (Transformation only) |
| `FULL_SNAPSHOT_CDC` | `streaming_table` | Delta diffing | Hard deletes detected | Current state | Diff against prior snapshot using primary keys |

---

## 13. Target Types Quick Reference

| `target_type` | Lakeflow Mapping | Supported Ingestion Types | Supported CDC Strategies |
|---|---|---|---|
| `"streaming_table"` | `@dlt.table` (streaming) | `autoloader`, `zerobus`, `asn1` | `APPEND`, `SCD1`, `SCD2`, `FULL_SNAPSHOT_CDC` |
| `"materialized_view"` | `@dlt.table` (batch / MV) | `autoloader` (batch) | `TRUNCATE_AND_LOAD`, `SCD3` |
| `"batch_table"` | `@dlt.table` (batch) | `autoloader` (batch) | `TRUNCATE_AND_LOAD`, `APPEND` |
| `"sink"` / `"external_sink"` | `dlt.create_sink` + `@dlt.append_flow` | `autoloader`, `zerobus` | Direct Egress (`delta`, `kafka`, `pgp_zip`) |

---

## 14. Official Databricks Documentation Index

- [Databricks Lakeflow Declarative Pipelines Documentation](https://docs.databricks.com/aws/en/dlt/)
- [Auto Loader Cloud Files Guide](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html)
- [Change Data Capture with Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [Unity Catalog Volumes Overview](https://docs.databricks.com/aws/en/connect/unity-catalog/volumes)
- [Managing Secrets in Unity Catalog](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets)
- [Unity Catalog Row Filters & Column Masks](https://docs.databricks.com/data-governance/unity-catalog/row-and-column-filters)
- [Using Delta Lake Change Data Feed (CDF)](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [Databricks Asset Bundles (DABs) Guide](https://docs.databricks.com/dev-tools/bundles/index.html)
