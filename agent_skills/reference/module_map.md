# Module Map — `lakeflow_framework/*`

One paragraph per subpackage under
`src/NextGen_Metadata_Framework/lakeflow_framework/`, its responsibility, and its key public
functions — so an agent can quickly answer "where does X happen?" without re-reading the
whole tree. File paths below are relative to
`src/NextGen_Metadata_Framework/lakeflow_framework/`. See `SKILL.md` for how these subpackages
compose into the end-to-end architecture.

---

## `crypto/`

Column-level AES encryption/decryption at rest, Unity Catalog secret resolution, and PGP
encrypt/decrypt/sign. Every secret anywhere in the framework flows through this package's
`secrets.py`. Key functions: `secrets.py::resolve_secret_value(spark, secret_catalog,
secret_schema, secret_key)` / `resolve_secret_ref(spark, secret_ref_dict)` (the
`dbutils.secrets.get()`-based resolution every other secret consumer calls — never the SQL
`secret()` function), `secrets.py::assert_safe_identifier` (shared SQL-injection guard for any
catalog/schema/table/column/secret name spliced into DDL text);
`column_crypto.py::apply_aes_column_encryption` / `apply_aes_column_decryption` (the
`target_config.encrypted_columns[]` / `source_inputs[].decrypted_columns[]` engines — an
`encrypted_columns[]` entry may declare `source_data_type` to pin the column's pre-encryption
type, which is otherwise taken from what Spark observes at encryption time);
(`crypto/hashing.py` was DELETED in v1.4.0 with the surrogate-key engine — the framework's only
remaining hash construction is `cdc/hashing.py`);
`pgp.py::pgp_encrypt` / `pgp_decrypt` / `pgp_verify` (pure-Python `PGPy`-based, no external
`gpg` binary — required for serverless compute; secrets are always resolved by the caller
before these functions ever see key material).

## `dq/`

Data-quality enforcement: native Lakeflow expectations plus this framework's own quarantine
extension. `expectations.py::apply_dq_expectations(dq_rules)` is a decorator factory wrapping
`dlt.expect_all`/`dlt.expect_all_or_drop`/`dlt.expect_all_or_fail` for `warn`/`drop`/`fail`
rules. `quarantine.py::add_quarantine_columns` derives `__framework_dq_quarantine_flag` /
`__framework_dq_failed_rule_ids` / `__framework_dq_failure_reasons` / `__framework_pipeline_run_id` / `__framework_record_id` for
`action: "quarantine"` rules (Lakeflow has no native "route to a different table"
expectation); `quarantine.py::register_main_and_quarantine_tables` is the function that
actually registers the main/clean table (or an internal clean view, when a CDC strategy will
own the real target-table name — see `cdc/dispatcher.py`) and its sibling `_quarantine` table,
and is where `__framework_hash_key`/`__framework_hash_value` get computed on the clean upstream
(`_apply_hash_columns`) before any CDC strategy ever sees it.

## `cdc/`

Every `cdc_load_strategy` implementation, dispatched by `dispatcher.py::register_cdc_strategy`.
`scd.py::register_scd1` / `register_scd2` (+ `register_scd2_reporting_view`) / `register_scd3`
implement SCD1/2/3 (SCD1/2 via native `dlt.apply_changes`; SCD3 as a derived window-function
pivot over a hidden internal SCD2 history table — Lakeflow's `apply_changes` has no native
SCD3). `snapshot.py::register_full_snapshot_cdc` implements `FULL_SNAPSHOT_CDC` via
`dlt.apply_changes_from_snapshot`, keyed on `target_config.primary_keys` (required). `comparison_columns.py::resolve_comparison_columns` is the
single source of truth for "which columns count as a change" (shared by SCD2's
`track_history_column_list` and this package's own hash computation). `hashing.py::
compute_hash_columns` computes `__framework_hash_key`/`__framework_hash_value`
(`HASH_KEY_COLUMN`/`HASH_VALUE_COLUMN`) — consumed directly by `reconciliation/matcher.py`
when `hash_precomputed: true`. `change_metrics.py::capture_scd_change_counts` queries Delta
Change Data Feed (`table_changes(...)`) for exact insert/update/delete counts per pipeline
update, called post-deployment from `control_plane/post_deployment.py`.

## `ingestion/`

Everything that turns a raw landing-zone source into a staged DataFrame for an ingestion
flow. `readers.py::read_ingestion_source(spark, source_type, source_config)` dispatches to
`read_autoloader_source` / `read_zerobus_source` / `read_asn1_source` (the 3 `source_type`
values — see `SKILL.md` §4); `_apply_source_zip_handling` (private, called from within the
`autoloader`/`asn1` readers) decrypts (optionally, via a type-dispatched
`pre_extraction_decryption` registry — `"pgp"` today) and unzips a landing directory before
Auto Loader ever reads it. `json_flattening.py::apply_explode_columns` flattens
struct/explodes array columns for JSON sources. `standardization_sql.py::
apply_data_standardization_sql` applies the restricted, single-column-expression
`data_standardization_sql` allowlist. `technical_metadata.py::attach_technical_metadata` /
`attach_framework_ingestion_timestamp` add `__framework_source_file_name`/`_rescued_data`/etc. and the
framework-wide `__framework_ingestion_timestamp_utc` column (also the default CDC sequencer
when `sequence_by_column` is unset). `column_normalization.py::normalize_column_names` is
opt-in (`source_config.column_normalization.enabled`) trim/case-fold/replace-special-characters
Bronze column-name cleanup. Note the asymmetry: the *function* is plural, the *config key* is
`column_normalization` — the legacy `normalize_column_names` config key was removed in v1.4.0. `schema_config.py::load_schema_config` / `apply_schema_config` load
an external JSON/YAML file (`source_config.schema_config_path` — an exact file, or a directory
resolved to its latest-modified file) declaring explicit type casts, Unity Catalog column
comments, and source-to-target renames — see `docs/28_ingestion_schema_config.md` for the full
guide and the exact ordering against column normalization.

## `transformation/`

The Silver/Gold multi-input SQL engine. `inputs.py::register_transformation_inputs` registers
one watermarked `@dlt.view` per `source_inputs[]` entry (applying `decrypted_columns` and
`withWatermark`, casting a string event-time column to `timestamp` first);
`mark_streaming_references` rewrites `FROM <name>`/`JOIN <name>` to `FROM STREAM <name>`/
`JOIN STREAM <name>` for every input whose `is_streaming: true` — required because plain
`spark.sql(...)` resolves a bare reference to a streaming view as batch otherwise.
`parameters.py::substitute_dynamic_parameters` resolves `${param}` placeholders in
`transformation_sql`/`filter_condition`/`transform_sql` against `pipeline_parameters` (quotes
string values itself — never wrap a placeholder in your own quotes in the SQL text).

## `engine/`

The shared convergence point both the ingestion and transformation engines funnel into (see
`notebooks/03_engine/03_lakeflow_declarative_pipeline.py`, which only differs per-engine in
*how* it builds the staged DataFrame). `flow_registration.py::register_staged_view` registers
the `@dlt.view` staged intermediate (DQ expectations decorator, ingestion timestamp,
output-column encryption, quarantine columns); `register_flow_output` dispatches on
`target_type` to either a genuine sink (`"sink"`) or the main/quarantine-table + CDC-dispatch
path (everything else), and additionally triggers the `"external_sink"` export.
`sink_registration.py::register_sink_target` / `register_external_sink_export` build the
actual `dlt.create_sink`/`@dlt.append_flow` pairs for `"sink"`/`"external_sink"` respectively
(see `SKILL.md` §7) — every secret referenced in `sink_config` is resolved here, eagerly, at
graph-definition time, never inside the sink's own `write()`/`commit()`. `run_context.py::
resolve_pipeline_run_id` best-effort resolves a run/update identifier for quarantine-row
traceability.

## `onboarding/`

Spec ingestion end to end. `spec_loader.py::load_and_template_spec` reads a JSON/YAML spec,
substitutes `{{catalog}}`/`{{env}}` placeholders, and returns `(spec_dict, templated_text,
spec_version)`. `spec_validator.py::validate_spec` is the **authoritative** structural/type/
allowed-value/SQL-syntax validator — every `ALLOWED_*` constant near its top is ground truth
for what a spec may contain; it collects every problem before returning
`(ingestion_flows, transformation_flows, reconciliation_flows, observability_destinations,
errors)` rather than failing fast. `metadata_upsert.py::upsert_dataflow_group_spec` /
`upsert_ingestion_flow_spec` / `upsert_transformation_flow_spec` /
`upsert_reconciliation_flow_spec` / `upsert_observability_config` `MERGE`-upsert validated
flows into the five control tables — `observability[]` (telemetry destinations for the DLT
observability engine, §16 in `SKILL.md`) is validated and upserted from this exact same spec,
there is no separate observability config file. `audit_logger.py::write_audit_log_entry` and
`client_context.py::build_client_context_json` write the perception/audit trail
(`onboarding_audit_log`), on both success and failure.

## `governance/`

Tags-only Unity Catalog governance. `tags.py::apply_governance_tags(spark, catalog, schema,
table, governance_tags)` applies `ALTER TABLE ... SET TAGS` / `ALTER TABLE ... ALTER COLUMN
... SET TAGS` for `governance_tags.table_tags`/`column_tags[]`. The framework applies tags
only — it does not create or administer the Unity Catalog masking/row-filter policy a tag
activates. Must be called post-deployment (see `control_plane/post_deployment.py`), never
from inside pipeline graph-definition code, since it's DDL against an already-materialized
table.

## `reconciliation/`

The standalone (non-pipeline-graph) source-vs-target(s) comparison engine — see `SKILL.md` §8
for the full flow. `dataset_reader.py::read_reconciliation_dataset` reads one side
(`table`/`file`/`sink` type, batch or streaming). `matcher.py::prepare_dataset_for_matching` /
`match_reconciliation_target` do the hash-first full-outer-join classification
(`MATCHED`/`MISSING_IN_TARGET`/`MISSING_IN_SOURCE`/`VALUE_DRIFT`), collapsing duplicate keys
via an `F.max_by` priority aggregation. `appender.py::run_target_reconciliation` is the single
per-target entrypoint composing match → append (`append_missing_records`, with
fingerprint-based idempotency via `compute_batch_fingerprint` /
`is_target_batch_already_processed`) → run-log write (`write_run_log_entry`).
`mismatch_logging.py::write_mismatch_log_rows` projects per-record mismatch detail (including
a column-by-column `differing_columns_json` for `VALUE_DRIFT`) entirely via native Spark
column expressions (never a driver-side collect+loop). `streaming.py::
run_streaming_target_reconciliation` is the `foreachBatch`-driven incremental counterpart,
reusing every one of the above functions per micro-batch instead of once per whole table.
`metrics.py::ReconciliationMetrics` is the shared counts dataclass both logging paths write
from.

## `archive/`

ZIP/PGP archive handling, for both ingestion (unzip a landing drop) and egress (a genuine
Lakeflow sink format). `zip_utils.py::extract_encrypted_zip` (optionally AES-password-
protected ZIP extraction into a UC Volume, via `pyzipper`) and `compress_and_encrypt_sink`
(bundles files into a ZIP built in an in-memory `BytesIO` buffer — required because Volumes'
FUSE mount doesn't support seek-on-write). `pgp_zip_sink.py::PgpZipDataSource` /
`_PgpZipStreamWriter` is the genuine custom Lakeflow sink backing `sink_config.format:
"pgp_zip"` (a real `pyspark.sql.datasource.DataSource`, registered via
`spark.dataSource.register`) — stages rows as JSON-Lines per-partition on `write()`
(executor-side), zips+optionally-PGP-encrypts exactly that micro-batch's files on `commit()`
(driver-side); every secret it uses was already resolved upstream by
`engine/sink_registration.py`. `zip_ingestion_pipeline.py::validate_zip_batch` /
`ingest_zip_batch` is the multi-ZIP batch orchestration (validate → extract → load → join →
re-archive) backing `notebooks/06_zip_ingestion/`.

## `asn1/`

Distributed ASN.1 BER/DER binary decoding for telecom CDR-style sources.
`decoder.py::derive_asn1_field_defs(schema_path, pdu_name)` introspects a real `.asn` module
file (via `asn1tools.parse_files`) to derive the Spark output schema — never a hand-authored
field list; `decode_asn1_binary_stream` is the entrypoint `ingestion/readers.py::
read_asn1_source` calls, running `make_partition_decoder`'s generator via `DataFrame.
mapInPandas` (the ASN.1 module is compiled **once per partition**, not once per row — the fix
for an original per-row-UDF implementation that recompiled it on every row). Fully distributed
end to end — no driver-side collection at any point, matching the framework-wide constraint
that file discovery/reading and decoding both run entirely on executors.

## `observability/`

Structured JSON business-event logging, plus the standalone **DLT observability engine**
(`dlt_observability` — see `docs/25_dlt_observability_module.md` for the full architecture).

`structured_logger.py::log_flow_event(operation, flow_id, status, ...)` emits one JSON line via
Python's standard `logging` module (landing in driver/cluster logs) and **never raises** — a
logging failure must never mask the real processing error. `logged_operation(operation,
flow_id, **extra)` is a context manager wrapping "time this block, log SUCCESS/FAILED with
whatever counts were set on `ctx`, re-raise the original exception unchanged" — the pattern
used at every call site in `engine/flow_registration.py`, `engine/sink_registration.py`,
`dq/quarantine.py`, `reconciliation/appender.py`, `reconciliation/mismatch_logging.py`, and
(the observability engine's own dispatch phase) `destination_dispatcher.py` below.

The DLT observability engine runs as a **downstream Workflow task**, chained after a pipeline's
`run_pipeline_update` task (see `resources/dlt_observability_job.yml`,
`notebooks/08_observability/08_dlt_observability_engine.py`), and is completely independent of
the pipeline's own graph-definition code:
- `config_loader.py::load_destination_configs(spark, control_catalog, dataflow_group_id)`
  resolves enabled `observability_config` destinations for a dataflow group (group-specific
  rows override a same-`destination_id` `"*"` global-fallback row). Rows are populated by
  `onboarding/metadata_upsert.py::upsert_observability_config` from the **same onboarding
  spec** as every other flow (its top-level `observability[]` array, validated by
  `onboarding/spec_validator.py` alongside `ingestion_flows`/`transformation_flows`) — there is
  no separate observability config file or seed step; this module only ever reads the table.
- `task_context_resolver.py::resolve_task_context(workspace_client, upstream_task_run_id)`
  turns `{{tasks.run_pipeline_update.run_id}}` into `(pipeline_id, start_time_ms, end_time_ms)`
  via the Jobs API.
- `event_log_extractor.py::resolve_dataflow_group_id` reads a pipeline's `dataflow.group.id`
  Spark conf via the Pipelines API (this framework's one-pipeline-per-group convention —
  `dataflow_group_id` isn't a native event-log field, and is a different scoping key from the
  DLT `pipeline_id` the Jobs API returns above) to bridge from the resolved `pipeline_id` to
  the `dataflow_group_id` `config_loader.py` reads destinations by; `extract_raw_events`
  queries `event_log(:pipeline_id)` for the resolved window; `aggregate_flow_metrics` (pure, no
  Spark) groups events per `(update_id, flow_id)` into `FlowMetrics`/`UpdateSummary`/`ErrorDetail`.
- `otel_payload_builder.py::build_resource_logs` maps that aggregation into strict OTLP/HTTP
  JSON `ResourceLogs` — one `Resource` per flow (carrying `databricks.dataflow_id`/
  `databricks.step_id`/`pipeline.update_id`), each with flow-summary/DQ-expectation/error
  `LogRecord`s; `validate_resource_logs` asserts every mandatory OTel field is present.
- `destination_dispatcher.py::dispatch_all` sends the built payload to every resolved
  destination — `DATABRICKS_VOLUME` via a plain `open()` write to the Volume's FUSE mount,
  `OTLP_CONSUMER` via HTTP POST with `env:`/`secret:`-resolved auth headers and exponential
  backoff on `429`/`5xx`. One destination's failure never blocks the others
  (`ObservabilityDispatchError` only raises when *every* destination fails).
- `agent_tools.py` — the 3 pure-Python functions backing Deliverable 6's AI Agent tools
  (`validate_observability_config`, `generate_pipeline_onboarding_config`,
  `diagnose_pipeline_telemetry_failures`); declarative tool specs in
  `agent_skills/dlt_observability_tools.json`.

New exception types: `ObservabilityConfigError` (config/context resolution failures),
`ObservabilityDispatchError` (all-destinations-failed).

## `control_plane/`

Control-table DDL, read access, provisioning, and post-deployment steps.
`ddl_definitions.py::get_all_control_table_ddls` (pure string-building, no execution — this is
the DDL ground truth for every control table's columns). `repository.py::
load_active_group_metadata(spark, control_catalog, group_id)` is what the engine notebook
calls to resolve the active group + its active ingestion/transformation rows.
`schema_provisioner.py::ensure_control_schema_exists` idempotently creates the `config` schema
+ all control tables (shared by `01_setup_control_tables.py` and the onboarding engine's own
self-provisioning). `post_deployment.py::apply_all_governance_tags` /
`capture_all_scd_change_counts` are the two functions
`notebooks/04_governance/04_apply_governance_and_egress.py` calls after a pipeline update
completes — both are genuinely *post*-deployment because they need state (a materialized
table to tag; a committed Delta version range) that doesn't exist yet at graph-definition
time.

## `storage/`

Delta/Lakeflow physical-storage concerns shared across every table this framework
materializes. `table_properties.py::qualified_table_name(catalog, schema, table)` — **use
this, always**, for any `@dlt.table`/`@dlt.view` `name=`; a bare `target_table` resolves
against the *pipeline's own* default catalog/schema, not the flow's configured target.
`build_table_properties` translates `target_config` into Delta/Lakeflow table properties
(UniForm Iceberg read-compat, Change Data Feed for CDC-dispatched strategies, log/deleted-file
retention). `build_auto_ttl_kwarg` builds the **separate** `auto_ttl={...}` keyword argument a
`@dlt.table`/`dlt.create_streaming_table` call needs (Auto TTL is not a generic
`delta.*`-prefixed table property — see `reference/common_pitfalls.md`).
`column_ordering.py::reorder_columns_for_delta_stats` moves key/clustering/hash columns to the
front of a target's schema so they fall within Delta's default 32-column data-skipping stats
window — applied once, on every flow's clean/quarantine output, right before it becomes a
materialized table.

## Top level: `exceptions.py`

The framework's typed exception hierarchy, all deriving from `FrameworkError`:
`FrameworkConfigError` (malformed/missing control metadata — the most common one you'll see),
`SecretResolutionError`, `CryptoError`, `ArchiveError`, `Asn1DecodeError` (schema/module-load
failures only — a per-row binary decode failure is captured inline as
`_asn1_decode_error`, never raised), `AbacApplicationError` (governance tag failures),
`CdcStrategyError`, `OnboardingValidationError`, `OnboardingUpsertError`,
`ObservabilityConfigError` (observability_config/task-context/event-log resolution failures),
`ObservabilityDispatchError` (every configured telemetry destination failed for a run). Catch
the specific type you actually care about — that's the entire point of this hierarchy existing
instead of bare `Exception`/`ValueError` everywhere.
