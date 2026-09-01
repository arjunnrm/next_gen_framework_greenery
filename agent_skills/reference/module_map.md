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

`table_errors.py` is the odd one out in this package and the reason is import safety, not data
quality: it holds `is_table_not_found` / `TABLE_NOT_FOUND_CONDITIONS` and **imports nothing at
all**. `quarantine.py` does a module-level `import dlt`, which is fatal in a plain job notebook
task (`NoSuchElementException: None.get`), so any module reachable from a job — today
`observability/reconciliation_export.py` and `reconciliation/appender.py` — imports the helper
from `table_errors.py`, never from `quarantine.py`. `quarantine.py` keeps `_is_table_not_found`
as a backwards-compatible alias for pipeline-side callers; importing *that* from job context
reintroduces the bug. See `common_pitfalls.md` **39** and
`tests/unit/test_job_context_has_no_dlt_import.py`.

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
the staged intermediate (DQ expectations decorator, ingestion timestamp, output-column
encryption, quarantine columns) — a `@dlt.view` for a single-reader flow, or (v1.6.0, the
Intermediate Object Rule) a pipeline-scoped `@dlt.table(temporary=True)` under the same bare
name when it has more than one reader (quarantine rules, `sink`/`external_sink` targets):
materialized once for read-once, never published to Unity Catalog (pre-v1.6.0 this case
published a qualified `catalog.schema` table); `register_flow_output` dispatches on
`target_type` to either a genuine sink (`"sink"`) or the main/quarantine-table + CDC-dispatch
path (everything else), and additionally triggers the `"external_sink"` export.
`sink_registration.py::register_sink_target` / `register_external_sink_export` build the
actual `dlt.create_sink`/`@dlt.append_flow` pairs for `"sink"`/`"external_sink"` respectively
(see `SKILL.md` §7) — every secret referenced in `sink_config` is resolved here, eagerly, at
graph-definition time, never inside the sink's own `write()`/`commit()`. `run_context.py::
resolve_pipeline_run_id` best-effort resolves a run/update identifier for quarantine-row
traceability.

Three modules added in **v1.5.0**, when reconciliation moved inside the pipeline DAG:

- `identifiers.py::sanitize_identifier(value)` maps every non-`[0-9a-zA-Z_]` character to `_`;
  `stable_node_name(prefix, locator, suffix, max_core=80)` builds a collision-free dataset name by
  **always** appending `sha256(locator)[:8]` — never only on truncation, because sanitizing alone
  would collide `metaflow.bronze.a_b` with `metaflow.bronze_a.b` and raise `Cannot redefine dataset`
  for the whole update.
- `source_plane.py` — the **read-once** plane (requirement R2: every physical source table/path is
  read exactly once per pipeline update and reused by every consumer). Three-phase, deliberately
  pure-first: `plan_source_plane(...)` (no `dlt`, no Spark action, unit-testable with zero
  workspace) → `register_source_plane(spark, plan)` → `bind(plan, consumer_id, want_stream)`.
  `bind` is keyed on a **consumer id string**, never on a caller-recomputed identity, so plan and
  bind cannot disagree; it returns one of three binding kinds — `in_graph_sibling` (`dlt.read`/
  `read_stream` of a table this same group publishes), `shared_node` (one materialized node, a
  streaming table if *any* consumer streams, an MV otherwise — since v1.6.0 registered as a
  bare-named `@dlt.table(temporary=True)`, published qualified only when the spec sets both
  `source_plane.catalog` + `source_plane.schema`; `PlaneNode.published` carries the decision), or
  `inline` (today's exact code path, preserving predicate pushdown into the origin). Internal types: `ReadIdentity`
  (`locator_kind`/`locator`/`options_fingerprint`, locator casefolded), `ConsumerRequest`,
  `PlaneNode`, `Binding`, `SourcePlanePlan`. `assert_acyclic(plan)` runs a Kahn topological sort
  over the whole edge set and raises `FrameworkGraphCycleError` naming the ring;
  `describe_plan(plan)` emits one structured `source_plane_node` event per node so "was my table
  actually read once" is answerable from the log stream. Guards `G-STREAM` (a `want_stream`
  consumer on a MERGE-written/overwritten locator) and `G-SIDE` (two competing
  `landing_retention_policy`/`source_zip_handling` lifecycle regimes on one path — a latent
  data-loss bug that sharing the read also fixes) reject at plan time. `G-STREAM`'s membership test
  is the module-level `_NON_APPEND_ONLY_CDC_STRATEGIES` frozenset, which as of v1.5.0 is
  `{SCD1, SCD2, SCD3, FULL_SNAPSHOT_CDC, TRUNCATE_AND_LOAD}` — `TRUNCATE_AND_LOAD` belongs there
  because its target is a `@dlt.table` fed by a full recompute, so Delta refuses to stream from it
  (`DELTA_SOURCE_TABLE_IGNORE_CHANGES`); `APPEND` is the only strategy that is genuinely
  append-only. The guard's message names `execution_mode: "pipeline_audit_only"` as the fix. See
  `common_pitfalls.md` entry 31.
- `flow_generators.py::generate_ingestion_flow` / `generate_transformation_flow` /
  `generate_reconciliation_flow` — the per-flow registration bodies, lifted verbatim out of
  `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` so the notebook is the thin
  registration loop its own header always claimed to be, and so they are importable by tests.
  Each takes the flow's control-table row plus the `SourcePlanePlan`, calls `source_plane.bind`
  rather than reading a source itself, and funnels into `flow_registration.py`'s shared tail.
  `_needs_materialized_staged_view(dq_rules, target_type)` is the private helper deciding whether a
  flow's staged intermediate must be a real table rather than a `@dlt.view` (quarantine rules or a
  sink target force materialization — `common_pitfalls.md` entry 25).
  `generate_reconciliation_flow` is the third first-class flow type, delegating to
  `reconciliation/graph_registration.py`; the engine notebook calls it only for rows whose
  `execution_mode` is `"pipeline"`/`"pipeline_audit_only"`, so a `"job"` row (including a row whose
  `execution_mode` is NULL) is filtered out of the DAG and left to notebook 05.

`spark_config.py::resolve_spark_conf` / `read_pipeline_spark_config` / `apply_spark_conf` resolve
and apply the layered `spark_config` Spark-conf overrides for a group.

## `onboarding/`

Spec ingestion end to end. `spec_loader.py::load_and_template_spec` reads a JSON/YAML spec,
substitutes `{{catalog}}`/`{{env}}` placeholders, and returns `(spec_dict, templated_text,
spec_version)`. `spec_validator.py::validate_spec` is the **authoritative** structural/type/
allowed-value/SQL-syntax validator — every `ALLOWED_*` constant near its top is ground truth
for what a spec may contain; it collects every problem before returning
`(ingestion_flows, transformation_flows, reconciliation_flows, observability_destinations,
errors)` rather than failing fast. Two v1.5.0 helpers worth knowing:
`_append_cycle_finding(pipeline_mode, execution_mode, message, errors)` is the single funnel for
every V-CYC append-loop finding — an **ERROR** under `execution_mode` `"pipeline"`/
`"pipeline_audit_only"`, a **WARNING** under `"job"`, because a Lakeflow graph cycle simply does not
exist when the standalone engine runs after the update; and
`_validate_landing_side_effect_collisions` (V-CYC-8) is a pure *ingestion* rule called from
`validate_spec` directly, not from any reconciliation-scoped helper. `metadata_upsert.py::upsert_dataflow_group_spec` /
`upsert_ingestion_flow_spec` / `upsert_transformation_flow_spec` /
`upsert_reconciliation_flow_spec` / `upsert_observability_config` `MERGE`-upsert validated
flows into the five control tables — `observability[]` (telemetry destinations for the DLT
observability engine, §16 in `SKILL.md`) is validated and upserted from this exact same spec,
there is no separate observability config file. Each upsert describes its row **twice** — a
module-level `StructType` (`_RECONCILIATION_FLOW_SPEC_SCHEMA` and siblings) and the `Row(...)`
literal actually constructed — and a new attribute must be added to both or it silently writes
NULL forever (`common_pitfalls.md` entry 28). `audit_logger.py::write_audit_log_entry` and
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

The source-vs-target(s) comparison engine — see `SKILL.md` §8 for the full flow. Since **v1.5.0**
it runs in either of two hosts, chosen per flow by `reconciliation_flows[].execution_mode`: the
standalone job task (`"job"`, the default, unchanged) or **inside the dataflow group's own Lakeflow
pipeline DAG** (`"pipeline"` / `"pipeline_audit_only"`). `dataset_reader.py::read_reconciliation_dataset` reads one side
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
from. `matcher.py::classify_reconciliation_target` is the lazy entry point the in-graph path
calls — the same full-outer join, when-chain and `F.max_by` per-key collapse as
`match_reconciliation_target`, with no eager action, so it is legal inside a `@dlt.table` closure.
`mismatch_logging.py::build_mismatch_rows` and `dataset_reader.py::apply_reconciliation_overlays`
(plus `read_reconciliation_dataset`'s `in_graph` flag) are the other pure halves both hosts share.

`graph_registration.py::register_reconciliation_flow(...)` is the in-pipeline registrar (layers
L3/L4/L5 of the DAG). L3 materializes the prepared source and prepared target — so the
read/filter/standardize/hash chain is paid **once** per update instead of once per target, and
`target_record_count` is structurally pinned to the pre-append instant; since v1.6.0 these (and
`__classified`/`__missing`/the pulse) are pipeline-scoped **temporary** tables, published qualified
only for a healing flow's `_src`/healing `_tgt` (the L5 handler reads them back via
`spark.read.table`). L4's published audit datasets, `recon__<rid>__<tid>__metrics` /
`__mismatch`, are registered **only when their `logging_config` capture flag resolves true**
(resolved at graph-definition time via `resolve_log_capture_flags`, pipeline-conf overrides
included), land in `publish_schema`, and carry `dq_config` expectations on the one-row `__metrics`
dataset (the first declarative way a reconciliation threshold can fail a pipeline update). Two
contradictions raise `FrameworkConfigError` at graph definition (mirrored by the validator):
`dq_config.rules` with `run_log_capture` false, and `pipeline_audit_only` with both flags false.
L5 is the heal lane: the fingerprint-ledger-guarded append plus the control-table writes
(`reconciliation_result` gated by `run_log_capture` since v1.6.0), re-hosted verbatim inside
**one** `dlt.foreach_batch_sink` handler per flow, keeping notebook 05's sequential per-target
loop, its `try/except` and its `break` — so `error_handling.on_failure: "fail"` still stops the
remaining targets of that `reconciliation_id`; the heal flow's ordering edge joins a one-row
aggregate of `__classified` (v1.6.0 — was `__metrics`, now conditional). `pipeline_audit_only`
registers L3+L4 and leaves healing in job mode. `_counts_query` / `_mismatch_query` /
`_wants_heal` are its helpers.
The fingerprint ledger (`compute_batch_fingerprint` / `is_target_batch_already_processed`) is
**kept verbatim and is more load-bearing here, not less**: a full refresh re-runs an
`@dlt.append_flow` with no cleanup of prior writes, and `foreach_batch_sink`'s `batch_id` restarts
at 0 while the target table is untouched.

## `archive/`

ZIP/PGP archive handling, for both ingestion (unzip a landing drop) and egress (a genuine
Lakeflow sink format). `zip_utils.py::extract_encrypted_zip` (optionally AES-password-
protected ZIP extraction into a UC Volume, via `pyzipper`) and `compress_and_encrypt_sink`
(bundles files into a ZIP built in an in-memory `BytesIO` buffer — required because Volumes'
FUSE mount doesn't support seek-on-write). `pgp_zip_sink.py::PgpZipDataSource` /
`_PgpZipStreamWriter` is the genuine custom Lakeflow sink backing `sink_config.format:
"pgp_zip"` (a real `pyspark.sql.datasource.DataSource`, registered via
`spark.dataSource.register`) — stages rows per-partition on `write()` (executor-side) as
JSON-Lines, or as RFC-4180 CSV with a header row when `sink_config.staged_file_format: "csv"`
(v1.6.0; header order follows the sink's write schema, nested values stage as JSON text),
zips+optionally-PGP-encrypts exactly that micro-batch's files on `commit()` (driver-side); every
secret it uses was already resolved upstream by `engine/sink_registration.py`. `zip_ingestion_pipeline.py::validate_zip_batch` /
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
`run_pipeline_update` task (see `resources/observability/dlt_observability_job.yml`,
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

`reconciliation_export.py::export_reconciliation_control_rows(spark, control_catalog, group_id,
pipeline_update_id) -> int` (**v1.5.0**) is the observability-side counterpart of in-pipeline
reconciliation: it reads every `recon__*__metrics` / `__mismatch` dataset a pipeline update
published and writes the corresponding `reconciliation_run_log` / `reconciliation_mismatch_log` /
`reconciliation_result` rows, de-duplicated per `(pipeline_update_id, target)`, so
`pipeline_audit_only` flows still land in the control tables. **Flag-aware since v1.6.0**: it
probes for `__metrics` when `run_log_capture` resolves true (else `__mismatch`), skips a flow
whose two capture flags are both false (such a flow registers no audit datasets and writes no
control rows anywhere), and gates its `reconciliation_result` writes under `run_log_capture` —
the same contract as the L5 handler. Like every other function in this package it runs as a **downstream job
task**, never inside the pipeline graph — requirement R3, observability stays a normal Lakeflow job
task, unchanged.

New exception types: `ObservabilityConfigError` (config/context resolution failures),
`ObservabilityDispatchError` (all-destinations-failed).

## `control_plane/`

Control-table DDL, read access, provisioning, and post-deployment steps.
`ddl_definitions.py::get_all_control_table_ddls` (pure string-building, no execution — this is
the DDL ground truth for every control table's columns; remember that literal braces inside those
f-strings must be doubled as `{{ }}`). `repository.py::
load_active_group_metadata(spark, control_catalog, group_id) -> GroupMetadata` is what the engine
notebook calls to resolve the active group + its active flow rows. **Signature changed in v1.5.0:**
it now returns a 4-field `GroupMetadata` NamedTuple — `(group_row, ingestion_rows,
transformation_rows, reconciliation_rows)` — where it previously returned three values. The fourth
field is what lets the engine notebook register reconciliation as a third flow type; unpack by name
(`md.reconciliation_rows`), and read the newer nullable group columns defensively via
`getattr(md.group_row, "source_plane_config_json", None)`, since `01_setup` only ever runs
`CREATE TABLE IF NOT EXISTS` and an already-provisioned control table will not have them.
`schema_provisioner.py::ensure_control_schema_exists` idempotently creates the `config` schema
+ all control tables (shared by `01_setup_control_tables.py` and the onboarding engine's own
self-provisioning).

**Additive column migration, added in v1.5.0.** Every statement in `get_all_control_table_ddls` is
`CREATE TABLE IF NOT EXISTS`, a no-op against an existing table — so a column added to a CREATE
statement reaches **new installations only**. `ddl_definitions.py::ADDITIVE_CONTROL_TABLE_COLUMNS`
(bare table name → list of `(column_name, sql_type, comment)`) plus
`ddl_definitions.py::get_add_column_ddl(control_schema, table_name, column_name, sql_type, comment)`
close that gap for existing ones, driven by
`schema_provisioner.py::ensure_control_table_columns(spark, control_catalog)`, now called at the
end of `ensure_control_schema_exists`. It currently carries
`reconciliation_flow_spec`'s `two_tier_verification`/`execution_mode`/`publish_schema`/
`dq_config_json` and `dataflow_group_spec`'s `source_plane_config_json`. Four things to know before
touching it:

- **A new control-table column must be added in BOTH places** — the `CREATE TABLE` DDL *and*
  `ADDITIVE_CONTROL_TABLE_COLUMNS` — so a fresh install and a migrated one converge.
- **Strictly additive**: only `ALTER TABLE ... ADD COLUMNS`, never a drop and never a retype.
  Existing rows get NULL, which is each column's documented default.
- **Idempotence is caller-side**, because Databricks SQL rejects `ADD COLUMNS IF NOT EXISTS` with
  `PARSE_SYNTAX_ERROR` (verified live 2026-08-31). `ensure_control_table_columns` skips columns
  already present and swallows only the narrow duplicate-column race
  (`_DUPLICATE_COLUMN_CONDITIONS` / `_is_duplicate_column_race`, sibling to the pre-existing
  `_ALREADY_EXISTS_CONDITIONS` / `is_already_exists_race`).
- **`databricks bundle deploy` does not apply it.** Only *running* the `setup_control_tables` task
  does. Verified live: `metaflow.config.reconciliation_flow_spec` had none of the three new
  columns, and pipeline-mode onboarding failed with `UNRESOLVED_COLUMN` until the setup task ran.
  On a brand-new workspace the migration is a harmless no-op. See `common_pitfalls.md` 26 and 27.

`post_deployment.py::apply_all_governance_tags` /
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
