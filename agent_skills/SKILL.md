# Metaflow Framework — Agent Skill

**Read this first.** This document orients any LLM-based coding agent (Claude, Databricks
Genie, or otherwise) to this repository so it can answer questions about the framework and
generate correct onboarding specs / framework code on the first attempt. It is a map, not a
copy — every section links to the real file that is authoritative. When in doubt, open the
cited file; do not guess at a function signature or a field name.

> Project brand name: **Metaflow**. Repo name: `NextGen_Metadata_Framework`. Python package
> root: `NextGen_Metadata_Framework.lakeflow_framework` under `src/NextGen_Metadata_Framework/lakeflow_framework/`.

## Table of contents

1. [What this framework is](#1-what-this-framework-is)
2. [Architecture: control tables → onboarding → engine notebook → DLT graph](#2-architecture)
3. [The onboarding spec shape](#3-the-onboarding-spec-shape)
4. [`source_type` — the 3 ingestion sources](#4-source_type--the-3-ingestion-sources)
5. [`target_type` — the 5 output destinations](#5-target_type--the-5-output-destinations)
6. [`cdc_load_strategy` — every load/CDC strategy](#6-cdc_load_strategy--every-loadcdc-strategy)
7. [How sinks work (`sink` / `external_sink`)](#7-how-sinks-work)
8. [How reconciliation works](#8-how-reconciliation-works)
9. [Secrets: Unity Catalog 3-level namespace](#9-secrets-unity-catalog-3-level-namespace)
10. [Governance tags](#10-governance-tags)
11. [Data quality: expectations vs. quarantine](#11-data-quality-expectations-vs-quarantine)
12. [Walkthrough: onboarding a new flow, step by step](#12-walkthrough-onboarding-a-new-flow-step-by-step)
13. [Where to look for X](#13-where-to-look-for-x)
14. [Deep-dive doc index (`docs/*.md`)](#14-deep-dive-doc-index)
15. [Repo layout cheat sheet](#15-repo-layout-cheat-sheet)
16. [The DLT observability module](#16-the-dlt-observability-module)

Companion files in this same skill folder:

- **`governance/SKILL.md`** — Dedicated Governance Skill enforcing standardized pipeline/flow naming, mandatory metadata tagging, and observability signatures.
- **`tool_specifications.json`** — Master Declarative AI Agent Tool Specifications (`validate_json`, `onboard_entity`, `get_catalog_schema_parameters`, `validate_observability_config`, `generate_pipeline_onboarding_config`, `diagnose_pipeline_telemetry_failures`).
- **`SKILL_GAP_ANALYSIS.md`** — Systematic audit checklist itemizing skill coverage, behavioral updates, and net-new capabilities.
- **`README.md`** — Executive Skill & Tool Catalog Summary with LangChain / Semantic Kernel / OpenAI integration patterns.
- **`reference/module_map.md`** — one paragraph per subpackage under `lakeflow_framework/`, its responsibility, and key public functions. Covers the v1.5.0 additions: `engine/source_plane.py` (the read-once plane + its `G-STREAM`/`G-SIDE` guards), `engine/flow_generators.py`, `engine/identifiers.py`, `reconciliation/graph_registration.py` (the in-pipeline L3/L4/L5 registrar), the additive control-table column migration in `control_plane/`, `repository.py`'s now-4-field `GroupMetadata`, and `observability/reconciliation_export.py`.
- **`reference/common_pitfalls.md`** — real bugs hit and fixed in this codebase. Read before editing! Entries 26–33 are the v1.5.0 batch: the `CREATE TABLE IF NOT EXISTS` migration gap, `ADD COLUMNS IF NOT EXISTS` being a parse error, an upsert `StructType` field never written into the `Row(...)`, a validation rule stranded behind an early return, a graph-cycle rule wrongly applied to job mode, streaming from a `TRUNCATE_AND_LOAD` target, `currentDatabase()` not being the pipeline's schema, and inlining onboarding into a new job. Entry **34** covers the grouped `resources/` tree: why every resource path is now `../../`, and why scoping a deploy by commenting out an `include:` line DELETES the resources it declared (use `--select`).
- **`reference/onboarding_spec_full_reference.json`** — the complete, machine-readable attribute dictionary for the onboarding spec.

---

## 1. What this framework is

Metaflow is a **metadata-driven** Databricks Lakeflow Declarative Pipelines (formerly Delta
Live Tables / DLT) framework. Instead of writing a new notebook per data source, you write a
JSON or YAML **onboarding spec** describing a flow declaratively (source, target, CDC
strategy, DQ rules, governance tags), submit it once through an onboarding job, and one
generic engine notebook reads the resulting control-table rows and dynamically builds the
`@dlt.table` / `@dlt.view` / `dlt.create_sink` graph at runtime. No per-source Python code is
required for the common cases; framework Python is only needed to add a genuinely new
capability (a new `source_type`, a new `cdc_load_strategy`, a new sink `format`, ...).

Every sample/test pipeline in this repo (see `metaflow_testing/*.json`,
`onboarding_templates/pipeline_onboarding_template.{json,yaml}`) is onboarded through
configuration alone — that is the framework's own proof that it is genuinely config-driven,
and the standard an agent should hold new work to as well.

Core design constraints worth internalizing up front:

- **Business logic lives in `src/NextGen_Metadata_Framework/lakeflow_framework/`.** Notebooks
  under `notebooks/` are deliberately thin orchestration — they resolve control-table rows and
  wire them to framework functions. If you find yourself writing real logic inside a notebook
  cell, that logic almost certainly belongs in a framework module instead, so it's unit
  testable and reusable.
- **Everything is a Delta table underneath.** `storage_format: "iceberg"` and
  `table_properties.enable_iceberg_read_uniformity` both mean "turn on UniForm Iceberg
  read-compatibility on a physically-Delta table" — there is no separate native-Iceberg engine.
- **Secrets are always Unity Catalog 3-level secrets**, never classic workspace scopes (§9).
- **Governance tag DDL and CDC change-count capture are post-deployment steps**, never inside
  the pipeline's own graph-definition code (§10, and `control_plane/post_deployment.py`).
- **Sink egress (`target_type: "sink"`/`"external_sink"`) is always a genuine
  `dlt.create_sink` + `@dlt.append_flow` pair**, registered inside the same pipeline graph —
  never a separate post-deployment batch write (§7).

---

## 2. Architecture

```
 onboarding spec (JSON/YAML)                 4 control tables (Delta, in <catalog>.config)
 ┌─────────────────────────┐   validate +   ┌──────────────────────────────────────────┐
 │ ingestion_flows[]       │   templated    │ dataflow_group_spec                       │
 │ transformation_flows[]  │ ─────upsert──► │ ingestion_flow_spec                       │
 │ reconciliation_flows[]  │                │ transformation_flow_spec                  │
 └─────────────────────────┘                │ onboarding_audit_log (+ 3 recon tables)    │
        02_onboarding_engine.py             └──────────────────────────────────────────┘
                                                             │
                                                  spark.conf("dataflow.group.id")
                                                             ▼
                                          03_lakeflow_declarative_pipeline.py
                                   (one generic engine notebook, graph-definition time)
                                                             │
                              reads active rows for this group_id, for each row:
                              read/build DataFrame → DQ quarantine → CDC dispatch
                                                             ▼
                                     Lakeflow Declarative Pipeline DAG
                         (@dlt.table / @dlt.view / dlt.create_sink+@dlt.append_flow)
                                                             │
                                     ── AFTER the pipeline update completes ──
                                                             ▼
                          04_apply_governance_and_egress.py (post-deployment job task)
                          apply_all_governance_tags · capture_all_scd_change_counts
```

Concretely, in file terms:

1. **Author a spec** — `metaflow_testing/*.json` for real worked examples, or start from
   `onboarding_templates/pipeline_onboarding_template.json` (or `.yaml` — byte-for-byte
   equivalent, format is picked by file extension, see
   `onboarding/spec_loader.py::load_and_template_spec`). `{{catalog}}`/`{{env}}` placeholders
   anywhere in the file are substituted before parsing.
2. **Onboard it** — run `notebooks/02_onboarding/02_onboarding_engine.py` (a Databricks Job
   task, widgets: `spec_file_path`, `catalog`, `env`, `action_type` =
   `CREATE`/`UPDATE`/`VALIDATE_ONLY`). It:
   - self-provisions the `config` schema/tables if they don't exist yet
     (`control_plane/schema_provisioner.py::ensure_control_schema_exists`);
   - loads + templates the spec (`onboarding/spec_loader.py`);
   - validates the **entire** spec and collects **every** error before raising, not just the
     first one (`onboarding/spec_validator.py::validate_spec`);
   - captures perception/audit metadata (`onboarding/client_context.py`);
   - `MERGE`-upserts one row per flow into the control tables
     (`onboarding/metadata_upsert.py`) and appends one `onboarding_audit_log` row
     (`onboarding/audit_logger.py`), on both success and failure.
3. **Run the pipeline** — `resources/lakeflow_metadata_pipeline.yml` deploys
   `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` as a Lakeflow Declarative
   Pipeline, configured with `dataflow.group.id`/`dataflow.control.catalog` Spark confs (see
   `databricks.yml`'s `dataflow_group_id` bundle variable). At graph-definition time it:
   - resolves the active group + its active ingestion/transformation/**reconciliation** rows
     (`control_plane/repository.py::load_active_group_metadata`, which since v1.5.0 returns a
     4-field `GroupMetadata` NamedTuple: `(group_row, ingestion_rows, transformation_rows,
     reconciliation_rows)`);
   - plans the **source plane** (`engine/source_plane.py::plan_source_plane` →
     `assert_acyclic` → `register_source_plane`), so every physical source table/path in the
     group is read **exactly once** per update and reused by every consumer. Each consumer then
     calls `bind(plan, consumer_id, want_stream)` instead of reading directly, and gets back a
     sibling reference to an in-graph table, one shared materialized node, or today's inline
     read (fanout 1 — `source_plane.materialize` defaults to `"auto"`, so nothing already
     deployed changes shape);
   - for each **ingestion** row: reads the source
     (`ingestion/readers.py::read_ingestion_source`), attaches technical metadata, applies
     `explode_columns`/`data_standardization_sql`, then hands off to the shared tail;
   - for each **transformation** row: registers one watermarked `@dlt.view` per
     `source_inputs[]` entry (`transformation/inputs.py::register_transformation_inputs`),
     substitutes `${param}` placeholders and `STREAM` keywords into `transformation_sql`
     (`transformation/parameters.py`, `transformation/inputs.py::mark_streaming_references`),
     runs it, then hands off to the same shared tail;
   - the **shared tail** (`engine/flow_registration.py`) attaches the ingestion timestamp +
     output-column encryption + quarantine columns
     (`register_staged_view`), then materializes the main/quarantine table(s) and dispatches
     the configured `cdc_load_strategy` (`register_flow_output` →
     `dq/quarantine.py::register_main_and_quarantine_tables` → `cdc/dispatcher.py`), or routes
     straight to a genuine Lakeflow sink for `target_type: "sink"`/`"external_sink"`
     (`engine/sink_registration.py`).
4. **Apply governance + capture CDC metrics (post-deployment)** —
   `notebooks/04_governance/04_apply_governance_and_egress.py` calls
   `control_plane/post_deployment.py::apply_all_governance_tags` (Unity Catalog `SET TAGS`
   DDL — must run after the table exists) and `capture_all_scd_change_counts` (reads Delta
   Change Data Feed for exact insert/update/delete counts per CDC target).
5. **Reconcile** — per `reconciliation_flow_spec` row, in whichever host that row's
   `execution_mode` names (§8). `"job"` (the default) runs
   `notebooks/05_reconciliation/05_reconciliation_engine.py` independently of any pipeline
   graph, exactly as before; `"pipeline"` / `"pipeline_audit_only"` register the flow as a
   third flow type **inside step 3's own update**
   (`engine/flow_generators.py::generate_reconciliation_flow` →
   `reconciliation/graph_registration.py::register_reconciliation_flow`).

Full step-by-step detail: [`docs/03_engine_execution_flow.md`](../docs/03_engine_execution_flow.md).

---

## 3. The onboarding spec shape

Top level (validated by `onboarding/spec_validator.py::validate_spec`):

```json
{
  "dataflow_group_id": "dfg_finance_txn_ingest",
  "pipeline_parameters": {"filter_country": "US"},
  "ingestion_flows": [ /* §3a */ ],
  "transformation_flows": [ /* §3b */ ],
  "reconciliation_flows": [ /* §8 */ ],
  "observability": [ /* §16 -- telemetry destinations, DLT observability engine */ ]
}
```

At least one of `ingestion_flows`/`transformation_flows`/`reconciliation_flows` must be
non-empty; any combination is valid — a single `dataflow_group_id` yields a unified,
ingestion-only, or transformation-only Lakeflow DAG purely based on which control tables have
active rows (no separate pipeline code path). `observability[]` is validated and upserted
alongside these three but does **not** count toward that "at least one non-empty" requirement
— telemetry with nothing to observe is meaningless.

**Every ingestion/transformation flow shares this shape**, each sub-object independently
optional except where noted:

| Key | Ingestion | Transformation | Purpose |
|---|---|---|---|
| `dataflow_id` (ingestion) / `flow_step_id` (transformation) | required (PK) | required (PK) | Unique identifier |
| `source_type` | required, one of §4 | — | — |
| `source_config` | shape depends on `source_type`, §4 | — | — |
| `source_inputs[]` | — | required-ish (one per upstream input) | §7 in `docs/01_control_metadata_schema.md`; each entry: `input_name`, `table`, `is_streaming`, `watermark`, `decrypted_columns[]` |
| `transformation_sql` | — | required | Native Spark SQL; `${param}` placeholders; supports `UNION`/`UNION ALL` |
| `target_catalog`/`target_schema`/`target_table` | required | required | Delta table coordinates |
| `target_type` | required, one of §5 | required, one of §5 | — |
| `target_config` | required (`cdc_load_strategy` inside it is required) | required | §6, plus storage/encryption/sink settings |
| `dq_config` | optional | optional | §11: `{rules[], quarantine_table, record_id_column}` |
| `governance_tags` | optional | optional | §10: `{column_tags[], table_tags}` |

Full field-by-field reference with types/defaults/examples:
[`docs/01_control_metadata_schema.md`](../docs/01_control_metadata_schema.md) (the
authoritative table — read it before hand-writing a spec) and the worked, field-commented
"kitchen sink" example: [`docs/17_onboarding_template_reference.md`](../docs/17_onboarding_template_reference.md)
/ `onboarding_templates/pipeline_onboarding_template.json`.

The validator itself
(`src/NextGen_Metadata_Framework/lakeflow_framework/onboarding/spec_validator.py`) is the
ground truth for exactly which fields are required, their allowed values
(`ALLOWED_SOURCE_TYPES`, `ALLOWED_TARGET_TYPES`, `ALLOWED_INGESTION_CDC_STRATEGIES`,
`ALLOWED_TRANSFORMATION_CDC_STRATEGIES`, `ALLOWED_DQ_ACTIONS`, `ALLOWED_SINK_FORMATS`, etc. —
all defined as module-level constants near the top of that file), and what a malformed value
produces as an error string (`"<json_path>: <what's wrong>"` — see
[`docs/04_onboarding_validation.md`](../docs/04_onboarding_validation.md)). When generating a
spec, prefer reading these constants directly over trusting this document's prose, since they
are what actually gets enforced.

---

## 4. `source_type` — the 3 ingestion sources

Ingestion flows only (transformation flows read from `source_inputs[]` tables instead).
Implemented in `ingestion/readers.py`; dispatched by `read_ingestion_source`.

| `source_type` | What it reads | Required `source_config` keys | Reader function |
|---|---|---|---|
| `"autoloader"` | Streaming Auto Loader (`cloudFiles`) over a landing-zone path/format (csv, parquet, json, avro, text). Renamed from `gcs_autoloader` in v1. | `path`, `format`, `schema_location` (auto-derived if omitted) | `read_autoloader_source` |
| `"zerobus"` | Streaming read of an existing Delta table (e.g. landed via Zerobus direct-write ingest) | `source_catalog`, `source_schema`, `source_table` | `read_zerobus_source` |
| `"asn1"` | Binary telecom CDR-style ingestion: Auto Loader (`binaryFile`) + distributed BER/DER decode via `mapInPandas` | `path`, `schema_location`, `asn1_schema_path` (a real `.asn` module file), `asn1_codec` (`ber`/`der`), `asn1_pdu_name` | `read_asn1_source` → `asn1/decoder.py::decode_asn1_binary_stream` |

Shared across all three: `capture_technical_metadata`, `landing_retention_policy`
(`clean_source`: `archive`/`delete`/`off`), `schema_evolution_mode`, `file_pattern`,
`reader_options`, `data_standardization_sql`
(`ingestion/standardization_sql.py` — a restricted, single-column-expression grammar, never a
full statement), `column_normalization` (`{enabled, case}` — opt-in trim/case-fold/
replace-special-characters column-name cleanup, `ingestion/column_normalization.py`; the legacy
`normalize_column_names` boolean was REMOVED in v1.4.0 and is now rejected by onboarding), and
`schema_config_path` (an
external JSON/YAML file with explicit type casts/comments/renames, exact file or a
latest-modified-file-in-a-directory — `ingestion/schema_config.py`; see
`docs/28_ingestion_schema_config.md` for the exact ordering between these two and every other
column-touching field). `autoloader` and `asn1` additionally support `source_zip_handling`
(decrypt, then unzip, before Auto Loader ever reads the extracted files — see
`ingestion/readers.py::_apply_source_zip_handling`) and, for JSON `autoloader` sources,
`explode_columns` (`ingestion/json_flattening.py`).

ASN.1 detail worth knowing before touching a CDR spec: the Spark output schema is **derived
automatically** from the real `.asn` module file (`asn1/decoder.py::derive_asn1_field_defs`,
via `asn1tools.parse_files` introspection) — never a hand-authored JSON field list. ASN.1
identifiers are camelCase with no underscores (per X.680), so expect fields like
`callDurationSeconds`, not `call_duration_seconds`.

**Choosing `asn1_pdu_name` on a real telecom module is the step that actually bites.** The PDU
must be a **top-level `SEQUENCE`**, and `CHOICE` is rejected *anywhere* in its resolved member
tree — not merely at the top. Real modules put a `CHOICE` at the root: in
`metaflow_testing/BT_Testing/TAP.310.asn1` (the genuine GSMA TAP 3.10 spec, 375 types) both
`DataInterChange` and, one level down, `CallEventDetail` are `CHOICE`, so neither
`DataInterChange` nor `TransferBatch` can be the PDU. 70 of that module's 93 top-level
`SEQUENCE` types *do* resolve; `Notification` is the one Sample 06 uses
(`resources/sample_jobs/metaflow_sample_06_asn1_tap3_job.yml`). To find the workable set for any
module, run `derive_asn1_field_defs` over every top-level `SEQUENCE` and keep the ones that do
not raise `Asn1DecodeError` — far faster than reading the module. See pitfall 37 in
`reference/common_pitfalls.md`.

---

## 5. `target_type` — the 5 output destinations

`ALLOWED_TARGET_TYPES` in `onboarding/spec_validator.py`; dispatched by
`engine/flow_registration.py::register_flow_output`.

| `target_type` | Materializes a table? | Behavior |
|---|---|---|
| `"streaming_table"` | yes | Standard Lakeflow streaming table. |
| `"materialized_view"` | yes | Standard Lakeflow materialized view (full batch recompute). |
| `"batch_table"` | yes | Standard Lakeflow batch table — the **only** `target_type` where `storage_format: "iceberg"` is valid. |
| `"external_sink"` | yes, **plus** an export | A real, governed, DQ-quarantined table is materialized (identical to the three above) **and additionally** exported via a second `@dlt.append_flow` reading from that now-materialized table into a `dlt.create_sink` (`engine/sink_registration.py::register_external_sink_export`). |
| `"sink"` | **no — never** | No table is ever materialized. The flow's DQ-quarantine-filtered staged view feeds a `dlt.create_sink`/`@dlt.append_flow` pair directly (`engine/sink_registration.py::register_sink_target`). Requires a genuinely streaming source. |

See §7 for the sink mechanics common to `"sink"`/`"external_sink"`.

---

## 6. `cdc_load_strategy` — every load/CDC strategy

Lives inside `target_config.cdc_load_strategy` (there is no separate `cdc_config` — a v1→v2
migration). Dispatched by `cdc/dispatcher.py::register_cdc_strategy`.

| Strategy | Ingestion? | Transformation? | Mechanism | Requires |
|---|---|---|---|---|
| `APPEND` | yes | yes | No-op here — caller registers the staged view directly as the target (streaming table → append). | — |
| `TRUNCATE_AND_LOAD` | yes | yes | No-op here — full recompute (materialized view semantics). | — |
| `SCD1` | yes | yes | `dlt.apply_changes(..., stored_as_scd_type="1")` — overwrite-on-match. `cdc/scd.py::register_scd1`. | `primary_keys` |
| `SCD2` | yes | yes | `dlt.apply_changes(..., stored_as_scd_type="2")` — full history via native `__START_AT`/`__END_AT`, plus a companion `<target>_current` reporting **table** (`valid_from`/`valid_to`/`is_current` aliases). `cdc/scd.py::register_scd2`/`register_scd2_reporting_view`. | `primary_keys` |
| `SCD3` | **no** (transformation only) | yes | Current/previous-value pivot, derived from an internal hidden SCD2 history table via window functions (`ROW_NUMBER` over `__START_AT DESC`). `cdc/scd.py::register_scd3`. | `primary_keys`, `columns_to_check` |
| `FULL_SNAPSHOT_CDC` | yes | yes | `dlt.apply_changes_from_snapshot(..., stored_as_scd_type="1")` — diffs successive full-extract snapshots. `cdc/snapshot.py::register_full_snapshot_cdc`. | `primary_keys` |
| ~~`FULL_SNAPSHOT_CDC_NO_PK`~~ | — | — | **REMOVED in v1.4.0.** It keyed the diff on a framework-generated `__framework_surrogate_key` (a SHA-256 over the whole payload). Onboarding rejects it by name. Migrate to `FULL_SNAPSHOT_CDC` with a real `primary_keys`, or to `TRUNCATE_AND_LOAD` if the source genuinely has no key. | — |

Optional `target_config` fields, **each scoped to the strategies named** — the validator
rejects one used outside its set, so do not treat these as universally available:

| Field | Valid for | Rejected on |
|---|---|---|
| `sequence_by_column` | every CDC-dispatched strategy | — (falls back to `__framework_ingestion_timestamp_utc`; `dlt.apply_changes` always needs *some* sequencer) |
| `generate_hash_columns` (default `true`) | every CDC-dispatched strategy | — |
| `columns_to_check` | `SCD1`, `SCD2`, `SCD3` | — (accepted but inert elsewhere) |
| `columns_to_exclude` | `SCD1`, `SCD2`, `SCD3` | **`APPEND`, `TRUNCATE_AND_LOAD`, `FULL_SNAPSHOT_CDC`** — they have no comparison-column concept to exclude from |
| `cdc_operation_column` / `cdc_operation_mapping.delete_values` | `SCD1`, `SCD2`, `FULL_SNAPSHOT_CDC` | **`SCD3`** — a current/previous pivot has no delete path at all |

`columns_to_check`/`columns_to_exclude` are comparison-only in v2 — neither drops a column from
the target table; see `cdc/comparison_columns.py`. `cdc_operation_column` marks source rows as
deletes: `SCD1`/`SCD2` pass it to `apply_changes` as `apply_as_deletes`, while
`FULL_SNAPSHOT_CDC` filters flagged rows out of the snapshot so the diff deletes them by
absence — the same end result by a different route. The two enforcing sets are
`strategies_supporting_comparison_exclusion` and `strategies_supporting_delete_marker` in
`onboarding/spec_validator.py`.

**Do not emit any of these — they are removed in v1.4.0 and onboarding rejects each by name:**
`target_config.generate_surrogate_key`, `target_config.surrogate_key_columns`,
`target_config.surrogate_key_exclude_columns`, `source_config.normalize_column_names`,
`reconciliation_flows[].recon_mode`, `reconciliation_flows[].generate_surrogate_key`, and the
`cdc_load_strategy` value `FULL_SNAPSHOT_CDC_NO_PK`. There is no `__framework_surrogate_key`
column any more. Row identity is `primary_keys`; a source with no key uses `TRUNCATE_AND_LOAD`.

**Every CDC-dispatched flow gets `__framework_hash_key`/`__framework_hash_value`** (SHA-256 of
ordered `primary_keys` / of the resolved comparison-column set) added to the target table,
gated by `generate_hash_columns`, and the target is liquid-clustered on
`__framework_hash_key` — this is what makes reconciliation's hash-based join cheap at scale
(§8). `APPEND`/`TRUNCATE_AND_LOAD` never get these columns.

Full detail, worked examples per strategy, and the Databricks feature each is built on:
[`docs/02_cdc_load_strategies.md`](../docs/02_cdc_load_strategies.md).

---

## 7. How sinks work

`target_type: "sink"` / `"external_sink"` are the **only** legitimate way to export data
outside a pipeline's own tables in this framework — the project's explicit requirement is
that "all external outputs must use genuine Lakeflow/DLT sink functionality
(`dlt.create_sink` + `@dlt.append_flow`), not ordinary DAG table writes." There is no
post-deployment egress step any more (`control_plane/post_deployment.py::run_external_sink_exports`
was removed entirely once this was fixed — see
[`docs/23_lakeflow_sinks.md`](../docs/23_lakeflow_sinks.md)).

`target_config.sink_config` (validated by
`onboarding/spec_validator.py::_validate_sink_config`, built into `dlt.create_sink` options by
`engine/sink_registration.py::_build_sink_options`) has three `format` values:

| `format` | What it needs | Notes |
|---|---|---|
| `"delta"` | `sink_config.path` | Native Lakeflow Delta sink. |
| `"kafka"` | `sink_config.kafka_options` (at minimum `kafka.bootstrap.servers`, `topic`); optional `kafka_secret_options` for a connector option needing a literal resolved secret value | Native Lakeflow Kafka sink — same options a Spark Structured Streaming Kafka writer takes. |
| `"pgp_zip"` | `sink_config.path` (staging dir) + `sink_config.post_export_archive.output_zip_path`; optional `sink_config.staged_file_format` (`"json"` default \| `"csv"`, **v1.6.0**) | This framework's own **custom Lakeflow sink** (`archive/pgp_zip_sink.py::PgpZipDataSource`, a real `pyspark.sql.datasource.DataSource`) — stages every micro-batch's rows per partition (JSON-Lines, or RFC-4180 CSV with a header row when `staged_file_format: "csv"`), then zips (optionally AES-password-protects via `post_export_archive.secret`, optionally PGP-encrypts+signs) them into one archive file. `staged_file_format` is presence-rejected on `"delta"`/`"kafka"`, which have no staging step. |

Hard constraint (Databricks platform limitation, not a framework choice): `dlt.create_sink`/
`@dlt.append_flow` are **streaming-only** — a batch/non-streaming source cannot feed a sink at
all. Both `register_sink_target` and `register_external_sink_export` check this up front and
raise a `FrameworkConfigError` naming the exact fix, rather than letting Lakeflow fail deep
inside graph resolution.

`"sink"` never persists a main/clean table (the DQ-quarantined **table** can still exist
independently — only the "clean" side is sink-only). `"external_sink"` always persists a real
table (identical to `streaming_table`/`materialized_view`/`batch_table`) and *additionally*
exports it. See §6 in `docs/23_lakeflow_sinks.md` for the exact dispatch in
`engine/flow_registration.py::register_flow_output`.

**Every sink secret is resolved eagerly, at graph-definition time, on the driver** —
never lazily inside `write()`/`commit()`/`abort()`. This is not a style preference; a Python
Streaming Data Source's `commit()`/`abort()` run in a separate, restricted worker process
where `dbutils` cannot construct a working gateway. See
`engine/sink_registration.py::_resolve_secret_into_options` and
[`reference/common_pitfalls.md`](reference/common_pitfalls.md) before touching this code.

---

## 8. How reconciliation works

Compares one `source_config` against one or more `target_configs[]` (a **list** — one
reconciliation flow can audit multiple targets against the same source).

**Since v1.5.0 a reconciliation flow chooses its host** via `reconciliation_flows[].execution_mode`
(`"job"` | `"pipeline"` | `"pipeline_audit_only"`, default **`"job"`** — nothing already deployed
changes until an author opts in, one flow at a time):

| `execution_mode` | Where it runs | What you get |
|---|---|---|
| `"job"` (default) | standalone job task `notebooks/05_reconciliation/05_reconciliation_engine.py`, independent of any pipeline graph | exactly today's behaviour, unchanged |
| `"pipeline"` | **inside the dataflow group's own Lakeflow pipeline update**, as a third first-class flow type beside ingestion and transformation | the comparison published as real UC datasets **and** the self-healing append |
| `"pipeline_audit_only"` | the same pipeline update, comparison half only | in-DAG comparison, metrics and `dq_config` expectations; healing stays in job mode |

In the two pipeline modes the flow is registered by
`engine/flow_generators.py::generate_reconciliation_flow` →
`reconciliation/graph_registration.py::register_reconciliation_flow`. **v1.6.0 changed the dataset
surface**: the L3/L4 plumbing (`_recon__*__src`/`__tgt`/`__classified`/`__missing`/pulse) is
pipeline-scoped `@dlt.table(temporary=True)` — materialized, never published (a healing flow's
`_src`/healing `_tgt` stay published because the L5 handler reads them via `spark.read.table`) —
and the two published audit datasets, `recon__<reconciliation_id>__<target_id>__metrics` /
`__mismatch`, land in `publish_schema` (defaults to the pipeline's own schema) **only when their
`logging_config` capture flag resolves true**. `run_log_capture` also gates `reconciliation_result`
(previously unconditional); both flags false == the flow persists only to its business targets.
The imperative half — the fingerprint-guarded append plus the control-table writes — is re-hosted
verbatim inside **one** `dlt.foreach_batch_sink` handler per flow. Mode-scoped spec rules, all
rejected on **presence**: `read_mode: "streaming"`, `task_run_id_column`, and a missing
`dataflow_group_id` are each errors in pipeline mode; `publish_schema` and `dq_config` are errors
in `"job"` mode; and (v1.6.0) `dq_config.rules` with `run_log_capture: false`, or
`pipeline_audit_only` with **both** capture flags false, are rejected at onboarding *and* at graph
definition — see `reference/common_pitfalls.md` **36**.
`dq_config` on the one-row `__metrics` dataset (e.g. `{"expr": "value_drift_count = 0", "action":
"fail"}`) is the first declarative way a reconciliation threshold can fail a pipeline update; it is
**additive** and does not repurpose `error_handling.on_failure`, which keeps its try/except meaning
(with a larger blast radius in pipeline mode — re-raising fails the whole update, not one job task).

Three behaviours an operator must know before switching a flow to `"pipeline"`: healing is
**source-change-triggered** (an update in which the recon source advances no offsets performs no
append — detection and the `dq_config` gate are unaffected, since the comparison datasets are batch
MVs recomputed every update); the per-run log-silencing widget override is replaced by the
`dataflow.recon.run_log_capture` / `dataflow.recon.mismatch_log` pipeline configuration keys, which
take effect on the **next** update; and one run-as identity must now hold every permission the job
task and the pipeline previously held separately.

**Verification status — read this before promising a behaviour.** `execution_mode: "pipeline"` is
**live-verified**: on 2026-08-31 pipeline `metaflow_test_003_autoload_recon_pipeline` registered
ingestion, the L3 prepared source/target, all three L4 datasets, the L5 gate, the L5 append flow and
a `dlt.foreach_batch_sink` in **one** update, per its own event log, with every task of
`metaflow_test_recon_dag_job` succeeding. That settles the one open platform question:
`dlt.foreach_batch_sink` **is** available on DBR serverless, so the heal lane is real and
`pipeline_audit_only` is a deliberate choice rather than a fallback for an unproven API. (The local
`databricks-dlt` 0.3.0 stub still lacks the symbol, so `register_foreach_batch_sink` stays guarded
by `hasattr` — the guard is correct, it is simply no longer expected to trip on DBR.) The
`pipeline_audit_only` geneva scenario
(`metaflow_testing/053_geneva_e41a47ba_recon_in_pipeline.json`) is by contrast verified **offline
only** — validator plus `plan_source_plane`, pinned by `tests/unit/test_geneva_e41a47ba_topology.py`
— because the pipeline's run-as identity lacks table-level `SELECT` on the reconciliation target.
Do not describe it as live-proven.

**Two hard constraints on choosing `"pipeline"`:**

- A flow whose reconciliation **source** is a table this same group produces with a non-append-only
  `cdc_load_strategy` — `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` **and `TRUNCATE_AND_LOAD`** — cannot
  stream from it. The `G-STREAM` plan-time guard in `engine/source_plane.py` rejects it and names
  `"pipeline_audit_only"` as the correct setting. `TRUNCATE_AND_LOAD` is the counter-intuitive one:
  it is a no-op in `cdc/dispatcher.py` exactly like `APPEND`, but its target is a full recompute, so
  Delta answers a `readStream` with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`
  (`reference/common_pitfalls.md` entry 31).
- The three pipeline-mode columns (`execution_mode`, `publish_schema`, `dq_config_json`) reach an
  **already-provisioned** `reconciliation_flow_spec` only via the additive migration in
  `control_plane/ddl_definitions.py::ADDITIVE_CONTROL_TABLE_COLUMNS`, applied by *running*
  `01_setup_control_tables.py` — **not** by `databricks bundle deploy`. On a pre-v1.5.0 workspace,
  onboarding a pipeline-mode flow without that run fails with `UNRESOLVED_COLUMN`
  (`reference/common_pitfalls.md` entries 26–27). On a brand-new workspace the migration is a no-op,
  since the CREATE DDL already carries the columns.

Pipeline (all in `reconciliation/`, and shared verbatim by both hosts):

1. **Read** each side — `dataset_reader.py::read_reconciliation_dataset` (type `table`/
   `file`/`sink`, `read_mode` batch or streaming independently per side, `filter_condition` +
   `data_standardization_sql` applied identically regardless of type/mode).
2. **Prepare** — `matcher.py::prepare_dataset_for_matching` ensures
   `__framework_hash_key`/`__framework_hash_value` are present. **`hash_precomputed` is an
   assertion about the dataset, never an instruction to compute:** `true` means the two columns
   are ALREADY on the table (put there by an upstream CDC-dispatched flow with
   `generate_hash_columns` — the common case, §6) and are trusted verbatim; `false` (default)
   computes both here from `match_keys` (declared order) and `compare_columns` (sorted). A `true`
   on a dataset missing the columns is a hard `FrameworkConfigError`, not a silent recompute. Set
   `true` only when the upstream flow's `primary_keys` are this flow's `match_keys` — a mismatched
   basis does not error, it just reports every row as drifted.
3. **Match** — `matcher.py::match_reconciliation_target` does **one** full outer join on
   `__framework_hash_key`, classifying every record as `MATCHED` / `MISSING_IN_TARGET` /
   `MISSING_IN_SOURCE` / `VALUE_DRIFT` in a single pass — `comparison_direction: "both"` is
   not two joins, it's this one join with the caller choosing which categories to act on.
   Duplicate keys on either side are explicitly handled (`F.max_by` priority collapse — see
   the module docstring and `reference/common_pitfalls.md`).
4. **Append** — `appender.py::run_target_reconciliation` appends the
   `source_to_target`/`both`-direction miss set into `target_configs[].append_target_table`
   (optionally reshaped via flow-level `transform_sql`), gated by a deterministic
   fingerprint-based idempotency check against `reconciliation_run_log` (batch mode) or Spark
   Structured Streaming's own checkpoint (streaming mode, `reconciliation/streaming.py`).
   `target_to_source` (`MISSING_IN_SOURCE`) is **audit-only** — never remediated, never
   written anywhere except the mismatch log.
5. **Log** — every non-`MATCHED` record's full detail (which columns differ, both sides'
   values) goes to `reconciliation_mismatch_log`
   (`mismatch_logging.py::write_mismatch_log_rows`); aggregate counts per target per run go to
   `reconciliation_run_log` (`appender.py::write_run_log_entry`).

Full field reference and worked examples: [`docs/07_reconciliation.md`](../docs/07_reconciliation.md).

---

## 9. Secrets: Unity Catalog 3-level namespace

**Every** secret reference anywhere in this framework — encryption/decryption keys, PGP
keys, ZIP archive passwords, sink credentials — uses the same shape:

```json
{"secret_catalog": "poc", "secret_schema": "security", "secret_key": "pii_encryption_key"}
```

Resolved via `dbutils.secrets.get(catalog=, schema=, key=)`
(`crypto/secrets.py::resolve_secret_value`) — **never** the SQL `secret(scope, key)` function,
which only resolves classic workspace-level scopes and cannot address a Unity Catalog secret
at all. Requires Databricks Runtime 17.3 LTS+ / serverless environment version 4+.

There is a second, sharper reason to never let a literal `secret(...)`-shaped substring reach
a Lakeflow graph-definition query's text: Databricks' credential-redaction machinery corrupts
the *result* of any such query inside Lakeflow's own observability layer (silently, with no
error) — see `reference/common_pitfalls.md` entry 1 and
[`docs/16_encryption_and_secrets.md`](../docs/16_encryption_and_secrets.md) before writing any
code that builds a SQL expression involving a secret.

Column-level AES encryption/decryption (`crypto/column_crypto.py`) and PGP (`crypto/pgp.py`,
via `PGPy` — pure Python, no external `gpg` binary, so it works on serverless) both consume
already-resolved key material; they never touch `dbutils` themselves.

### 9.1 The encrypt → decrypt type contract (`source_data_type`, v1.4.0)

Encryption replaces a column's physical type with ciphertext binary, so the framework records
the **pre-encryption** Spark type as the Unity Catalog `original_data_type` column tag. That
tag is what a downstream `transformation_flows[].source_inputs[].decrypted_columns[].cast_to_type`
is validated against at decryption time.

`target_config.encrypted_columns[].source_data_type` (optional, added v1.4.0) **declares** what
that type must be:

```json
{"column_name": "ssn", "mode": "GCM", "source_data_type": "string",
 "secret": {"secret_catalog": "poc", "secret_schema": "security", "secret_key": "pii_key"}}
```

- **Omitting it is safe and is the documented default path** — the type observed at encryption
  time is used, exactly as pre-v1.4.0. No existing spec needs editing. Emit the key only when
  the author asks for it; never emit `""`.
- Declaring it makes a silent source type drift fail *at encryption time*, naming both types,
  instead of breaking the decrypt side later and far from the cause. Comparison is
  case-insensitive and whitespace-trimmed; a mismatch raises `CryptoError` rather than picking
  a winner.
- Onboarding type-checks it only (non-empty string) — the validator has no Spark session to
  parse a type string and no source schema to check it against, so the meaningful comparison
  happens at runtime in `crypto/column_crypto.py::apply_aes_column_encryption`.

Decryption never lives in `target_config`; it belongs to `source_inputs[].decrypted_columns[]`.

---

## 10. Governance tags

Tags-only model (`governance/tags.py`) — the framework applies key-value tags to columns and
tables via `ALTER TABLE ... SET TAGS` / `ALTER TABLE ... ALTER COLUMN ... SET TAGS`. It does
**not** create or administer the Unity Catalog masking/row-filter *policy* that gives a tag
its actual enforcement behavior — that is a workspace admin's tag-policy configuration,
external to this repo.

```json
{
  "column_tags": [{"column": "ssn", "tags": {"mask": "PII", "classification": "restricted"}}],
  "table_tags": {"row_filter": "region_restricted", "domain": "finance"}
}
```

**Must run post-deployment**, never inside the pipeline's own graph-definition code — tag DDL
is a Unity Catalog operation against an already-materialized table
(`control_plane/post_deployment.py::apply_all_governance_tags`, invoked from
`notebooks/04_governance/04_apply_governance_and_egress.py` as a separate downstream job
task). Tag DDL is naturally idempotent (re-applying an identical value is a no-op), so unlike
v1's ABAC-policy-binding model, no idempotency ledger table is needed.

Deep dive: [`docs/06_governance_integration.md`](../docs/06_governance_integration.md).

---

## 11. Data quality: expectations vs. quarantine

`dq_config.rules[]` — each `{rule_id, expression, action}`:

- `"warn"` / `"drop"` / `"fail"` map directly onto **native** Lakeflow expectations
  (`dlt.expect_all` / `dlt.expect_all_or_drop` / `dlt.expect_all_or_fail` —
  `dq/expectations.py::apply_dq_expectations`).
- `"quarantine"` is this framework's **own extension** (no native Lakeflow equivalent) —
  failing rows are routed to a sibling `<target_table>_quarantine` table
  (`dq_config.quarantine_table` to override the name) carrying
  `__framework_dq_failed_rule_ids`/`__framework_dq_failure_reasons`/`__framework_quarantine_validated_at`/`__framework_pipeline_run_id`/
  `__framework_record_id` (from `dq_config.record_id_column`) — `dq/quarantine.py::add_quarantine_columns`
  / `register_main_and_quarantine_tables`. **The quarantine table is only ever created when
  at least one rule has `action: "quarantine"`** — naming a `quarantine_table` with no
  quarantine-action rule produces no table at all.

---

## 12. Walkthrough: onboarding a new flow, step by step

Say you need to ingest a new CSV drop into Bronze with SCD1 semantics on Silver. Concretely:

1. **Pick (or write) a spec file.** Copy the shape of an existing, similar flow from
   `metaflow_testing/*.json` or `onboarding_templates/pipeline_onboarding_template.json` —
   don't invent field names from memory; grep the validator
   (`onboarding/spec_validator.py`) for the exact key you need if unsure.
2. **Ingestion flow** — add an entry to `ingestion_flows[]`:
   - `dataflow_id`: a unique string (convention: `df_<source>_<entity>_ingest`).
   - `source_type: "autoloader"`, `source_config: {path, format, schema_location}` pointing at
     the landing Volume (§4).
   - `target_catalog`/`target_schema`/`target_table`, `target_type: "streaming_table"`.
   - `target_config: {"cdc_load_strategy": "APPEND"}` — Bronze is typically append-only; raw
     landing rarely needs CDC.
   - `dq_config.rules[]` for any Bronze-level sanity checks (often `"warn"`-only at this
     layer).
3. **Transformation flow** — add an entry to `transformation_flows[]`:
   - `flow_step_id`: unique string; `dataflow_id`: the same one used above, or a new logical
     grouping if this step spans multiple ingestion flows.
   - `source_inputs: [{"input_name": "...", "table": "<catalog>.<bronze_schema>.<bronze_table>", "is_streaming": true}]`.
   - `transformation_sql` referencing `input_name` in `FROM`/`JOIN` — plain SQL; the framework
     rewrites streaming references to `FROM STREAM ...` for you
     (`transformation/inputs.py::mark_streaming_references`) — do not write `STREAM` yourself.
   - `target_config: {"cdc_load_strategy": "SCD1", "primary_keys": [...]}` (§6) —
     `sequence_by_column` is optional (falls back to the framework's own ingestion timestamp).
   - Add `encrypted_columns`/`governance_tags`/`dq_config` as needed (§9, §10, §11).
4. **Validate before deploying anything.** Run
   `notebooks/02_onboarding/02_onboarding_engine.py` with `action_type: "VALIDATE_ONLY"` first
   — it reports **every** structural/type/SQL-syntax problem across the whole spec in one
   pass (`onboarding/spec_validator.py::validate_spec`), not one error at a time. Fix all of
   them, not just the first.
5. **Onboard for real** — re-run with `action_type: "CREATE"` (or `"UPDATE"` for an existing
   `dataflow_group_id`). This upserts the control-table rows and writes an audit log entry. If you
   are adding a **new job** that needs to onboard a spec, it must **delegate** to the generic
   parameterised `resources/metaflow_config_jobs/onboarding_job.yml` via `run_job_task` — never inline its own
   `02_onboarding_engine.py` `notebook_task` (`reference/common_pitfalls.md` entry 33):

   ```yaml
   - task_key: onboard_x
     run_job_task:
       job_id: ${resources.jobs.onboarding_job.id}
       job_parameters:
         spec_file_path: "${workspace.file_path}/metaflow_testing/<spec>.json"
         catalog: metaflow
         env: dev
         action_type: CREATE
   ```
6. **Point a pipeline at the group** — set the `dataflow_group_id` bundle variable
   (`databricks.yml`) or the pipeline's `dataflow.group.id` configuration
   (`resources/lakeflow_metadata_pipeline.yml`) to your spec's `dataflow_group_id`, then
   `databricks bundle deploy` and run the pipeline. The engine notebook
   (`notebooks/03_engine/03_lakeflow_declarative_pipeline.py`) needs **zero changes** — it
   already reads every active row for the resolved group.
7. **Apply governance + capture CDC metrics** — run
   `notebooks/04_governance/04_apply_governance_and_egress.py` after the pipeline update
   completes (§10).
8. **(Optional) Add reconciliation** — a `reconciliation_flows[]` entry comparing the new
   Silver table against, say, its own Bronze source or an external system of record (§8). Leave
   `execution_mode` unset for today's standalone job behaviour; set `"pipeline"` /
   `"pipeline_audit_only"` to fold it into step 6's own Lakeflow update instead, and then delete
   any standalone `run_*_reconciliation` task from the job, or it runs twice (§8).

On a **fresh workspace** the order is: run `setup_control_tables` first (it both creates the control
tables and applies the additive column migration, §8), then onboard via the generic
`onboarding_job`, then run the pipeline.

Whenever a spec fails validation, read the error message literally — it names the exact
`json_path` and explains the fix (see
[`docs/04_onboarding_validation.md`](../docs/04_onboarding_validation.md) for the format). Do
not guess at a fix; the validator's error text is generated to be actionable on its own.

---

## 13. Where to look for X

| Question | Answer |
|---|---|
| "Where does field X in the spec get validated?" | `onboarding/spec_validator.py` — search for the field name; every check appends to a shared `errors` list. |
| "Where does field X in the spec actually take effect at runtime?" | `reference/module_map.md` points to the right subpackage; then grep that subpackage for the field name (JSON keys are read via plain `.get("field_name")` almost everywhere). |
| "What does the engine actually do, in order?" | `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` (thin) → `engine/flow_registration.py` (shared tail) → `docs/03_engine_execution_flow.md` (narrated). |
| "How do I add a new `source_type`?" | Add a reader function + register it in `ingestion/readers.py::_SOURCE_READERS`, add the name to `onboarding/spec_validator.py::ALLOWED_SOURCE_TYPES` and its own `_validate_ingestion_source_config` branch. |
| "How do I add a new `cdc_load_strategy`?" | Add a `register_*` function in `cdc/`, wire it into `cdc/dispatcher.py::register_cdc_strategy`, add the name to the validator's `ALLOWED_*_CDC_STRATEGIES` sets. |
| "How do I add a new sink `format`?" | Add a branch in `engine/sink_registration.py::_build_sink_options`, add the name to `onboarding/spec_validator.py::ALLOWED_SINK_FORMATS` and its own validation branch in `_validate_sink_config`. |
| "What table/column does a flow actually write to?" | `target_catalog`.`target_schema`.`target_table`, always resolved via `storage/table_properties.py::qualified_table_name` — **never** trust a bare `name=` in a `@dlt.table`/`@dlt.view` call to land in the flow's configured schema (see `reference/common_pitfalls.md`). |
| "What exceptions can this code raise, and which should I catch?" | `exceptions.py` — a typed hierarchy off `FrameworkError` (`FrameworkConfigError`, `SecretResolutionError`, `CryptoError`, `ArchiveError`, `Asn1DecodeError`, `AbacApplicationError`, `CdcStrategyError`, `OnboardingValidationError`, `OnboardingUpsertError`). |
| "Where do I find a real, worked example of feature Y?" | `metaflow_testing/*.json` for currently-maintained examples; `docs/08`–`docs/23` for narrated worked examples of most individual features (§14 below). |
| "What are the control tables' exact DDL/columns?" | `control_plane/ddl_definitions.py` (pure string-building, no execution) — and `docs/01_control_metadata_schema.md`'s ER diagram. |
| "How do I add a column to a control table?" | **Two places, always**: the table's `CREATE TABLE` DDL in `control_plane/ddl_definitions.py` (fresh installs) *and* `ADDITIVE_CONTROL_TABLE_COLUMNS` in the same file (existing workspaces, applied by `control_plane/schema_provisioner.py::ensure_control_table_columns`). A CREATE-only change never reaches a workspace that already has the table. Then add it to the upsert's `StructType` **and** its `Row(...)`. `reference/common_pitfalls.md` 26–28. |
| "I set `execution_mode: \"pipeline\"` and the flow still ran as a job — why?" | Three candidates, in order: the control table predates the column (run `01_setup_control_tables.py`, §8); the value was never persisted (`onboarding/metadata_upsert.py`'s `Row(...)`); or the job still has a standalone `run_*_reconciliation` task that should have been deleted. `reference/common_pitfalls.md` 26, 28, 33. |
| "How do I wire onboarding into a new job?" | Never inline a `02_onboarding_engine.py` `notebook_task`. Delegate via `run_job_task` to `resources/metaflow_config_jobs/onboarding_job.yml` (one spec) or `resources/metaflow_config_jobs/framework_config_onboarding_job.yml` (a whole `spec_dir`). The ~20 legacy `metaflow_test_*_job.yml` files keep their inline copies deliberately. `reference/common_pitfalls.md` 33. |
| "Where do I put a new resource YAML, and how do I deploy only the app?" | `resources/` is grouped: `metaflow_app/`, `metaflow_config_jobs/`, `observability/`, `bt_tests/`, `feature_tests/`, `sample_jobs/` (the `metaflow_sample` reference suite — six sample jobs, six pipelines, and the one common `metaflow_sample_seed_job` that lands every fixture they consume), `stability_tests/` (each globbed by `databricks.yml`'s `include:`). Paths inside a resource are `../../`, not `../`. Scope a deploy with `databricks bundle deploy --select apps.metaflow_onboarding_app,jobs.onboarding_job,jobs.framework_config_onboarding_job,volumes.onboarding_specs_volume,volumes.framework_wheels_volume` — **never** by commenting out an `include:` line, which makes DABs delete those resources. `reference/common_pitfalls.md` 34. |
| "Why don't the `_staged`/`_src__*`/`_recon__*` tables show up in the catalog?" | v1.6.0 Intermediate Object Rule: intermediates are views or pipeline-scoped `temporary` tables, never published — only final sinks and the conditional `__metrics`/`__mismatch` audit datasets are. Upgrading an existing deployment renames/unpublishes them (streaming state resets). `reference/common_pitfalls.md` 35, `docs/13` O7. |
| "How do I switch reconciliation logging fully off, and why was my spec rejected?" | `logging_config` both-false persists to business targets only — no `__metrics`/`__mismatch` datasets, no `run_log`/`mismatch_log`/**`result`** rows. `dq_config.rules` + `run_log_capture: false` and `pipeline_audit_only` + both-false are rejected at onboarding and graph definition. `reference/common_pitfalls.md` 36, `docs/07` §6/§11.10. |
| "Where does the read-once guarantee live?" | `engine/source_plane.py` — `plan_source_plane` (pure, no Spark) → `assert_acyclic` → `register_source_plane` → `bind`, plus the `G-STREAM`/`G-SIDE` plan-time guards. `reference/module_map.md`'s `engine/` section; `reference/common_pitfalls.md` 24–25, 31. |

---

## 14. Deep-dive doc index

This skill deliberately does **not** duplicate `docs/*.md` — it indexes into them. Read the
doc, not just this table, before making a non-trivial change in its area.

| Doc | Covers |
|---|---|
| [**`00_master_reference_index.md`**](../docs/00_master_reference_index.md) | Single master attribute lookup dictionary, CDC quick reference, framework columns, Databricks links. |
| [**`01_platform_architecture.md`**](../docs/01_platform_architecture.md) | Two-phase execution model (Graph Definition vs Execution), Medallion layout, Control tables ERD. |
| [**`02_ingestion_and_sources.md`**](../docs/02_ingestion_and_sources.md) | Auto Loader, Zerobus streaming, ASN.1 `mapInPandas` decoding, PGP/ZIP pre-extraction, column normalization. |
| [**`03_transformation_and_cdc.md`**](../docs/03_transformation_and_cdc.md) | Complete CDC load strategies (`APPEND`, `TRUNCATE`, `SCD1`, `SCD2`, `SCD3`, `FULL_SNAPSHOT`), hash keys, `${param}`. |
| [**`04_data_quality_and_governance.md`**](../docs/04_data_quality_and_governance.md) | DQ Expectations (`warn`, `drop`, `fail`, `quarantine`), quarantine routing & metadata, Unity Catalog tagging. |
| [**`05_security_and_cryptography.md`**](../docs/05_security_and_cryptography.md) | Column AES encryption (`GCM`, `CBC`, `ECB`), PGP signatures, UC 3-level secret paths, key rotation. |
| [**`06_egress_and_lakeflow_sinks.md`**](../docs/06_egress_and_lakeflow_sinks.md) | Lakeflow in-graph sinks (`dlt.create_sink` + `@dlt.append_flow`), `delta`/`kafka`/`pgp_zip` formats. |
| [**`07_reconciliation_engine.md`**](../docs/07_reconciliation_engine.md) | Hash-first cross-dataset reconciliation, drift classification, per-record mismatch log, self-healing append. |
| [**`08_observability_and_telemetry.md`**](../docs/08_observability_and_telemetry.md) | DLT Event Log extraction, OTel payload builder, Volume and OTLP HTTP dispatchers, Error Handling Matrix. |
| [**`09_developer_guide_and_recipes.md`**](../docs/09_developer_guide_and_recipes.md) | 11-step onboarding walkthrough, copy-paste recipes, local testing with pytest, troubleshooting runbook. |
| [**`10_multi_role_faqs.md`**](../docs/10_multi_role_faqs.md) | Dedicated Developer FAQ, Data Architect FAQ, and Project Manager FAQ. |

---

## 15. Repo layout cheat sheet

```
NextGen_Metadata_Framework/
├── databricks.yml                      # Bundle definition; `include:` lists every resources/ group
├── resources/                          # Pipeline/job/app/volume definitions, grouped by purpose:
│   ├── metaflow_app/                   #   the Onboarding App + its spec Volume
│   ├── metaflow_config_jobs/           #   onboarding_job (one spec) + framework_config_onboarding_job (bulk)
│   ├── observability/                  #   DLT observability export job + OTEL streaming pipeline
│   ├── bt_tests/                       #   real-BT-fixture tests (geneva, ASN.1, PGP)
│   ├── feature_tests/                  #   the TC-* corpus — one job + one pipeline per case
│   ├── sample_jobs/                    #   metaflow_sample suite: 6 jobs + 6 pipelines + 1 common seed job
│   └── stability_tests/                #   reserved for STABILITY_TEST_PLAN.md A1-G4 (empty today)
├── src/NextGen_Metadata_Framework/lakeflow_framework/   # ALL business logic — see reference/module_map.md
├── notebooks/
│   ├── 01_setup/                       # Creates the config schema + control tables
│   ├── 02_onboarding/                  # Spec → control-table rows (§2, §12)
│   ├── 03_engine/                      # The Lakeflow Declarative Pipeline itself (§2)
│   ├── 04_governance/                  # Post-deployment: tags + CDC change-count capture (§10)
│   ├── 05_reconciliation/              # Standalone reconciliation engine (§8)
│   ├── 06_zip_ingestion/               # Multi-ZIP batch ingestion pipeline (archive/zip_ingestion_pipeline.py)
│   ├── 07_verification/                # Ad hoc verification notebooks
│   └── 08_observability/               # DLT observability engine entrypoint (§16 below)
├── metaflow_testing/*.json             # Real, current worked onboarding-spec examples
├── onboarding_templates/                # Standard "kitchen sink" template, JSON + YAML -- includes
│                                        #   an "observability" block (§16); onboarding_spec.schema.json
│                                        #   carries its $defs too, no separate observability schema file
├── docs/*.md                            # Deep-dive narrated documentation (§14)
├── tests/{unit,integration}/            # unit = no Spark session; integration = live Databricks Connect
└── agent_skills/                        # ← you are here (+ dlt_observability_tools.json, §16)
```

For CLI authentication, profile selection, live workspace data discovery, and the bundle
deployment workflow itself, use the `databricks-core` skill referenced from this repo's
`AGENTS.md` — it is a separate, tooling-focused skill; this document is about the
framework's own architecture and configuration surface, not about driving the `databricks`
CLI.

---

## 16. The DLT observability module

A **standalone, downstream** module — it runs as a separate Workflow task *after* a pipeline's
own `run_pipeline_update` task completes, and never touches pipeline graph-definition code. Do
not confuse it with `structured_logger.py`'s in-pipeline business-event logging (§ above); this
module instead reads the DLT/Lakeflow **event log** (Databricks' own native telemetry table)
after the fact and re-exports it as vendor-neutral OpenTelemetry (OTel) JSON.

```
onboarding spec's "observability": [...] array (validated + upserted with every other flow)
                                          │
                                          ▼
run_pipeline_update (pipeline_task)  →  observability_export (notebook_task, depends_on above)
                                          08_dlt_observability_engine.py:
                                          1. resolve {{tasks.run_pipeline_update.run_id}} → (pipeline_id, window)
                                          2. resolve dataflow_group_id (pipeline's dataflow.group.id conf)
                                          3. query event_log(:pipeline_id) for that window
                                          4. aggregate per (update_id, flow_id)
                                          5. build strict OTel ResourceLogs (one per flow)
                                          6. dispatch to every enabled observability_config destination
```

**No separate observability config file.** Telemetry destinations are declared in a top-level
`observability[]` array inside the *same* onboarding spec as `ingestion_flows`/
`transformation_flows`/`reconciliation_flows` (§3 above) — validated by
`onboarding/spec_validator.py::_validate_observability_destinations` and upserted by
`onboarding/metadata_upsert.py::upsert_observability_config`, both invoked from the ordinary
`02_onboarding_engine.py` run alongside every other flow. `observability_config` is keyed by
`dataflow_group_id` (the same scoping every other control table uses — known at onboarding
time, unlike the real DLT `pipeline_id`, which only exists once the pipeline is deployed), with
`"*"` as the global fallback.

Key files (all under `src/NextGen_Metadata_Framework/lakeflow_framework/observability/` unless
noted — see `reference/module_map.md`'s `observability/` section for full function signatures):

| File | Responsibility |
|---|---|
| `config_loader.py` | `observability_config` read/resolve by `dataflow_group_id` (read-only — see above for how rows get there). |
| `task_context_resolver.py` | `{{tasks.run_pipeline_update.run_id}}` → `(pipeline_id, start_time_ms, end_time_ms)` via the Jobs API. |
| `event_log_extractor.py` | `resolve_dataflow_group_id` (Pipelines API) + `event_log(:pipeline_id)` query + pure (no-Spark) aggregation into `FlowMetrics`/`UpdateSummary`/`ErrorDetail`. |
| `otel_payload_builder.py` | Pure mapping into strict OTLP/HTTP JSON `ResourceLogs` (one per flow) + compliance validation. |
| `destination_dispatcher.py` | `DATABRICKS_VOLUME` write / `OTLP_CONSUMER` HTTP POST, `env:`/`secret:` auth, gzip, retry/backoff. |
| `agent_tools.py` | The 3 pure-Python functions backing Deliverable 6's AI Agent tools (below). |
| `onboarding/spec_validator.py::_validate_observability_destinations` | Structural validation of `observability[]`, called from `validate_spec` alongside every other flow array. |
| `onboarding/metadata_upsert.py::upsert_observability_config` | `MERGE`-upserts `observability[]` into `observability_config`, from `02_onboarding_engine.py`. |
| `control_plane/ddl_definitions.py::get_observability_config_ddl` | The control table's DDL (auto-provisioned by `01_setup_control_tables.py`, alongside the other control tables). |
| `notebooks/08_observability/08_dlt_observability_engine.py` | The thin entrypoint notebook wiring the read-side (`config_loader.py` onward) together. |
| `resources/observability/dlt_observability_job.yml` | A complete, runnable example job wiring `onboard_100` (whose spec carries an `observability[]` array) → `run_pipeline_update` → `observability_export`. |

**`dataflow_group_id` is not a native event-log field.** This framework configures exactly one
`dataflow.group.id` Spark conf per pipeline (§2 above), so `event_log_extractor.py::
resolve_dataflow_group_id` resolves it once via the Pipelines API rather than expecting it
anywhere in `event_log()`'s own columns — this is also the value `config_loader.py` uses to
look up `observability_config`, bridging the real DLT `pipeline_id` back to the
onboarding-time-known `dataflow_group_id`.

**AI Agent tools (Deliverable 6)** — declarative specs in `agent_skills/dlt_observability_tools.json`
(OpenAI function-calling JSON; the same shape maps onto a LangChain `StructuredTool` or a
Semantic Kernel `KernelFunction`), backed by real pure-Python functions in
`observability/agent_tools.py`:
- `validate_observability_config` — lint an `{"observability": [...]}` fragment against the
  same schema `spec_validator.py` enforces at onboarding time
  (`onboarding_templates/onboarding_spec.schema.json`'s `observability` property/`$defs`).
- `generate_pipeline_onboarding_config` — auto-generate an `observability[]` array from short
  destination-target descriptors, to merge into a pipeline's onboarding spec.
- `diagnose_pipeline_telemetry_failures` — match a failed task run's error text against the
  Error Handling Matrix (`docs/25_dlt_observability_module.md`) for a likely cause + remediation.

Full detail: [`docs/25_dlt_observability_module.md`](../docs/25_dlt_observability_module.md)
(architecture + event-log-to-OTel field mapping + Error Handling Matrix),
[`docs/26_dlt_observability_testing_runbook.md`](../docs/26_dlt_observability_testing_runbook.md)
(test matrix + end-to-end instructions + validation checklist), and
[`docs/27_dlt_observability_onboarding_reference.md`](../docs/27_dlt_observability_onboarding_reference.md)
(onboarding templates + the complete attribute dictionary).
