# 🧩 Module Permutation Matrix

> **Purpose**: This is the master compatibility reference across Ingestion, Transformation, Reconciliation, and Observability. When you are not sure whether a configuration is legal — "can `zerobus` feed a `materialized_view`?", "does `partition_columns` do anything on `SCD2`?", "can I reconcile against a streaming target?" — check here first, then follow the link to the owning doc for the mechanism and the "why."
>
> This document introduces **no new facts**. Every constraint below is already documented (and sourced) in [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md), [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md), [`07_reconciliation_engine.md`](07_reconciliation_engine.md), [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md), or [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md). Its only job is to answer "yes / no / partial" in one place; it deliberately does not re-explain mechanism.
>
> All claims below were verified directly against `onboarding/spec_validator.py`, `cdc/dispatcher.py`, `dq/quarantine.py`, `notebooks/03_engine/03_lakeflow_declarative_pipeline.py`, `reconciliation/dataset_reader.py`, `reconciliation/streaming.py`, and `notebooks/05_reconciliation/05_reconciliation_engine.py` as of v1.3.0 — not copied from a possibly-stale prior version of this repo's docs.

---

## 1. Ingestion: `source_type` × `target_type` × `cdc_load_strategy`

**What the validator actually enforces** (`onboarding/spec_validator.py`): `source_type` (`ALLOWED_SOURCE_TYPES = {"autoloader", "zerobus", "asn1"}`) and `target_type` (`ALLOWED_TARGET_TYPES = {"streaming_table", "materialized_view", "batch_table", "external_sink", "sink"}`) are validated **independently of each other** — there is no `source_type`×`target_type` allow-list anywhere in `_validate_ingestion_flow`. The one structural cross-check that exists is on `cdc_load_strategy`: `SCD3` is rejected on every `ingestion_flow` (`ALLOWED_INGESTION_CDC_STRATEGIES` excludes it; only `ALLOWED_TRANSFORMATION_CDC_STRATEGIES` includes it), with the verbatim reason "SCD3 pivots current/previous state via an internal history table, which only makes sense downstream of a raw ingestion flow."

**What actually determines runtime behavior** (`notebooks/03_engine/03_lakeflow_declarative_pipeline.py:229`): `is_streaming = (target_type == "streaming_table")` — full stop. `source_type` plays no part in that decision; all three readers (`read_autoloader_source`, `read_zerobus_source`, `read_asn1_source` in `ingestion/readers.py`) call `spark.readStream...` regardless of `target_type`. What changes for `materialized_view`/`batch_table` is *downstream*: `dq/quarantine.py::register_main_and_quarantine_tables` reads the staged view with `dlt.read(...)` (batch) instead of `dlt.read_stream(...)` when `is_streaming` is `False`.

| `source_type` | Can feed any `target_type`? | Legal `cdc_load_strategy` values | Practical notes |
|---|---|---|---|
| `autoloader` | Yes — no validator restriction | `APPEND`, `TRUNCATE_AND_LOAD`, `SCD1`, `SCD2`, `FULL_SNAPSHOT_CDC` | The common, fully-supported case for every `target_type`. (`SCD3` is transformation-flow-only; `FULL_SNAPSHOT_CDC_NO_PK` was removed in v1.4.0.) |
| `zerobus` | Yes — no validator restriction | Same five strategies as `autoloader` | Legal on `materialized_view`/`batch_table`, but atypical: Zerobus's whole design point is low-latency streaming ingest, so pairing it with a full-recompute target discards that advantage. Not an error — just usually not what was meant. |
| `asn1` | Yes — no validator restriction | Same six strategies as `autoloader` | Same batch/streaming split as `autoloader`, decoded via `read_asn1_source`. |

**Restriction added in v1.3.0 (E01):** `source_config.landing_retention_policy` is legal **only** on `source_type` `autoloader`/`asn1`. Configuring it on `zerobus` is a hard onboarding-time validation error (verbatim, `onboarding/spec_validator.py`):

```
<path>.landing_retention_policy: only applicable to source_type 'autoloader'/'asn1' (Auto Loader file ingestion), but this flow's source_type is 'zerobus'
```

*Why:* `landing_retention_policy` maps to `cloudFiles.cleanSource`, which only exists on an Auto Loader file read. A `zerobus` source streams an existing Delta table — there is no landing zone of files to clean. See [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) §2.

`target_type: "sink"` is a special case worth calling out here: it bypasses CDC dispatch and the main/quarantine table registration entirely (`engine/flow_registration.py`) — `cdc_load_strategy` is not read at all for a pure `sink` target. See [`06_egress_and_lakeflow_sinks.md`](06_egress_and_lakeflow_sinks.md).

---

## 2. Ingestion: `partition_columns` / `liquid_clustering_columns` legality by `cdc_load_strategy`

Verified directly in `dq/quarantine.py::register_main_and_quarantine_tables`'s own docstring: *"Physical layout (`partition_cols`/`cluster_by`) is only ever applied on the `needs_cdc_dispatch=False` branch, because that is the only branch here that registers a real physical table — the CDC branch registers a `@dlt.view`, and the table it feeds is created later by `cdc/dispatcher.py`."* `needs_cdc_dispatch` is `False` for exactly `{APPEND, TRUNCATE_AND_LOAD}` (`cdc/dispatcher.py::_NO_OP_STRATEGIES`). Grepping `cdc/scd.py` and `cdc/snapshot.py` confirms neither module reads `partition_columns` or `liquid_clustering_columns` at all.

| `cdc_load_strategy` | `partition_columns` / `liquid_clustering_columns` take effect? |
|---|---|
| `APPEND` | **Yes** — the only two strategies where a physical `@dlt.table` is registered directly from `target_config`. |
| `TRUNCATE_AND_LOAD` | **Yes** — same branch as `APPEND`. |
| `SCD1` | **Not applicable.** Fields are silently ignored — `cdc/scd.py::register_scd1` never reads them. Not a validation error; just inert. |
| `SCD2` | **Not applicable.** Same as `SCD1` (`register_scd2`). |
| `SCD3` | **Not applicable.** Same (`register_scd3`; transformation-flow-only). |
| `FULL_SNAPSHOT_CDC` | **Not applicable.** `cdc/snapshot.py::register_full_snapshot_cdc` never reads them. |

This is stated explicitly here (rather than omitted) so a reader scanning for `SCD2` sees "not applicable" rather than wondering whether the matrix forgot it. See [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) (new "Table Layout: Partitioning & Liquid Clustering" section) for the full E06/E07 mechanism, including the ≤3-column liquid-clustering limit and the `partition_columns: []` deliberate-unpartitioned semantics.

---

## 3. Transformation: `cdc_load_strategy` × required `target_type`

This section is a cross-reference, not new content — it mirrors [`00_master_reference_index.md`](00_master_reference_index.md) §12, itself built from `cdc/dispatcher.py` and `onboarding/spec_validator.py::_validate_target_config`.

| `cdc_load_strategy` | Registration mechanism | Typical `target_type` |
|---|---|---|
| `APPEND` | No-op dispatch — staged view published directly | `streaming_table`, `batch_table` |
| `TRUNCATE_AND_LOAD` | No-op dispatch — full recompute each update. **Unguarded against an empty source**: `empty_target_if_source_empty` (E09) is accepted but not enforced (withdrawn 2026-08-29 — the guard made the target read itself, which Lakeflow rejects as a graph cycle) | `materialized_view`, `batch_table` |
| `SCD1` | `cdc/scd.py::register_scd1` (`dlt.apply_changes`, `stored_as_scd_type="1"`) | `streaming_table` |
| `SCD2` | `cdc/scd.py::register_scd2` (`dlt.apply_changes`, `stored_as_scd_type="2"`) | `streaming_table` |
| `SCD3` | `cdc/scd.py::register_scd3` — **transformation flows only**, rejected on `ingestion_flows` | `materialized_view` (batch pivot over an internal streaming history table) |
| `FULL_SNAPSHOT_CDC` | `cdc/snapshot.py::register_full_snapshot_cdc` | `streaming_table` |

---

## 4. Reconciliation: which sides are reconcilable

**Delta-tables-only scope (E12, breaking).** `ALLOWED_RECON_DATASET_TYPES = {"table"}` — narrowed from `{"table", "file", "sink"}`. Both `source_config` and every entry in `target_configs[]` must be `type: "table"`. `file`/`sink` are rejected at onboarding **and** at runtime (`reconciliation/dataset_reader.py`, defense in depth against a hand-edited control-table row):

```
f"{path_prefix}.type: reconciliation is supported for Delta tables only -- type must be 'table', got {dataset_type!r}. Read the file/sink output into a Delta table first, then reconcile against that table."
```

```
f"Unsupported reconciliation dataset type {dataset_type!r} -- reconciliation is supported for Delta tables only ('table'). Read the file/sink output into a Delta table first, then reconcile against that table."
```

At runtime the resolved table's provider is additionally asserted to be Delta:

```
f"Reconciliation dataset table '{table}' is not a Delta table (provider={provider!r}) -- reconciliation is supported for Delta tables only."
```

**`read_mode` is the only axis — verified against `reconciliation/streaming.py` and `notebooks/05_reconciliation/05_reconciliation_engine.py`:**

| `read_mode` | Path taken |
|---|---|
| `"batch"` on both sides | Plain one-shot batch comparison (`appender.py::run_target_reconciliation`). |
| `"streaming"` on exactly one side | Streaming path (`reconciliation/streaming.py`) with `trigger(availableNow=True)` — drains the backlog and stops, matching a bounded job-task run. |

`recon_mode` was the second axis until v1.4.0, and it never did what its name suggested: it did
**not** force a side to stream. `05_reconciliation_engine.py`'s branch selection
(`if source_is_streaming or target_is_streaming: ... else: run_target_reconciliation(...)`) was
keyed on `read_mode` alone, so `recon_mode: "continuous"` with both sides `read_mode: "batch"` ran
exactly the same one-shot batch comparison as `"triggered"` — a configuration that looked
continuous and was not. The attribute is removed; see
[`07_reconciliation_engine.md`](07_reconciliation_engine.md) §5.

Two further, code-verified restrictions:

* **At most one side may be `read_mode: "streaming"`.** Both sides streaming raises (`reconciliation/streaming.py`, verbatim): `f"target_id={target_id!r}: both source_config and this target are configured read_mode='streaming' -- stream-stream reconciliation is not supported ..."` — a stream-stream join would need watermarking and only supports inner/left-outer semantics, incompatible with the framework's four-way MATCHED/MISSING_IN_TARGET/MISSING_IN_SOURCE/VALUE_DRIFT classification.
* **A `read_mode: "streaming"` side must actually be append-friendly.** `reconciliation/dataset_reader.py` reads a streaming side with a plain `spark.readStream.table(table)` — no `ignoreChanges`/`ignoreDeletes` option is set. A table that is fully overwritten on every refresh (the output of `cdc_load_strategy: "TRUNCATE_AND_LOAD"`, or any `materialized_view`) will raise Delta's own "Detected a data update" error the first time it is overwritten while a streaming reconciliation query holds a checkpoint against it. This applies regardless of `recon_mode` — it is a property of `read_mode: "streaming"` against a non-append-only table, not of continuous mode specifically. Configure that side as `read_mode: "batch"` instead.

### 4.1 `execution_mode` × what is legal (v1.5.0)

| | `execution_mode: "job"` (default) | `"pipeline_audit_only"` | `"pipeline"` |
|---|---|---|---|
| `dataflow_group_id` | optional — a group-less flow is supported | **required** | **required** |
| `read_mode: "streaming"` on either side | legal (at most one side) | **rejected on presence** | **rejected on presence** |
| `task_run_id_column` | legal | **rejected on presence** | **rejected on presence** |
| `publish_schema` | **rejected on presence** | legal | legal |
| `dq_config` | **rejected on presence** | legal (on `__metrics`) | legal (on `__metrics`) |
| `dq_config` rule with `action: "quarantine"` | n/a | **rejected** | **rejected** |
| `two_tier_verification` | honoured (and, from v1.5.0, actually persisted) | honoured | honoured |
| `error_handling.on_failure: "fail"` blast radius | fails the job task | fails the job task | fails the **pipeline update** |
| Healing (`append_target_table` writes) | every job run | job run | per micro-batch of the recon source — an update where the source does not advance performs no append |
| Comparison datasets published to UC | none | `__classified` / `__metrics` / `__mismatch` | `__classified` / `__metrics` / `__mismatch` |

Narrative, the L0–L5 node map and the DAG:
[`07_reconciliation_engine.md` §11](07_reconciliation_engine.md#11-execution-modes-job-pipeline-pipeline_audit_only).

`task_run_id_column` narrowing (per-side field, E12) only ever applies to a **static** read: the `source_config`/`target_configs[]` side in a fully-batch target, or the non-streaming side's per-micro-batch read in a mixed target. It never narrows the streaming side — its own micro-batch offsets are already the batch boundary, and filtering it by a single producing-run id would silently discard every offset belonging to any other one, data the checkpoint then advances past and never re-offers. (Before v1.4.0 it also never applied under `recon_mode: "continuous"`; with that mode gone every run is bounded, so the narrowing is unconditional whenever both `task_run_id` and a `task_run_id_column` are configured.) See [`07_reconciliation_engine.md`](07_reconciliation_engine.md) §3 and §5.

---

## 5. Observability: `mode` × `destination_type` × trigger source

As of v1.3.0 all four combinations of `mode` (`triggered`/`continuous`) × `destination_type` (`DATABRICKS_VOLUME`/`OTLP_CONSUMER`) are legal — `continuous` + `DATABRICKS_VOLUME` is the one genuinely new combination this release adds.

| `mode` | `destination_type: "DATABRICKS_VOLUME"` | `destination_type: "OTLP_CONSUMER"` | Serving notebook |
|---|---|---|---|
| `"triggered"` (default) | Legal — existing behavior | Legal — existing behavior | `notebooks/08_observability/08_dlt_observability_engine.py` |
| `"continuous"` | **Legal, new in v1.3.0.** `dlt.create_sink(format="json", ...)` fed by a `@dlt.append_flow`. | Legal — existing behavior, now control-table-driven via `event_log_tables` rather than only the pipeline `configuration:` fallback. | `notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py` |

**A destination is served by exactly one engine, determined solely by its own `mode` field.** There is no combined or auto-switching entrypoint: `08_dlt_observability_engine.py` dispatches only to destinations whose resolved `mode` is `"triggered"` (`filter_destinations_by_mode(..., "triggered")`); the continuous pipeline resolves `mode == "continuous"` rows the same way. This is deliberate — a bounded post-update job task and an always-on `continuous: true` pipeline have fundamentally different lifecycles, and letting one destination be picked up by both would double-export it.

`mode: "continuous"` **requires** `destination_config.event_log_tables` (a non-empty array of `catalog.schema.event_log_table` names); `mode: "triggered"` must **not** set it (the triggered engine resolves its pipeline from the upstream task run instead). Both directions are validated — see [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md) §2 and §5 for the verbatim error strings.

---

## 6. Explicitly unsupported combinations — consolidated list

A single place to check "is X even legal," pulled from every other v1.3.0-touched doc so you don't have to hunt across files.

| Combination | Status | Owning doc |
|---|---|---|
| `source_config.landing_retention_policy` on `source_type: "zerobus"` | **Validation error** at onboarding | [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) §2 |
| `source_config.landing_retention_policy` applied to the `source_zip_handling` pre-extraction path | **Never happens** — the two mechanisms are structurally independent, not merely undocumented | [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) §5 |
| `target_config.liquid_clustering_columns` with more than 3 columns | **Validation error** at onboarding, plus a runtime guard in `storage/table_properties.py` as defense in depth | [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) |
| `target_config.partition_columns` / `liquid_clustering_columns` on `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` | **Not an error — silently inert.** See §2 above. | [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) |
| `target_config.empty_target_if_source_empty` on any `cdc_load_strategy` other than `TRUNCATE_AND_LOAD` | **Validation error** at onboarding | [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) |
| `target_config.cdc_load_strategy: "SCD3"` on an `ingestion_flow` | **Validation error** at onboarding — `SCD3` is transformation-flow-only | [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) |
| `target_config.cdc_load_strategy: "FULL_SNAPSHOT_CDC_NO_PK"` on any flow | **Validation error** at onboarding — removed in v1.4.0, rejected by name with the migration path | [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) §2.7 |
| `target_config.generate_surrogate_key` / `surrogate_key_columns` / `surrogate_key_exclude_columns` | **Validation error** at onboarding — all removed in v1.4.0 | [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) §2.7 |
| `source_config.normalize_column_names` | **Validation error** at onboarding — removed in v1.4.0; use `column_normalization.enabled` | [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) |
| `source_config.dedup_watermark` without `source_config.remove_dups: true` | **Validation error** at onboarding | [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) |
| `source_config.json_string_columns` entry with no `schema_ddl`, on a non-streaming DataFrame | **Runtime `FrameworkConfigError`** — the no-schema shorthand needs a streaming source with a checkpoint for schema inference | [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) |
| `reconciliation_flows[].source_config` / `target_configs[]` entry with `type: "file"` or `type: "sink"` | **Validation error** at onboarding, plus a runtime re-check | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) |
| Both sides of a reconciliation target configured `read_mode: "streaming"` | **Runtime `FrameworkConfigError`** — stream-stream reconciliation is unsupported | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) |
| Reconciliation `read_mode: "streaming"` against a table produced by `TRUNCATE_AND_LOAD` (or any non-append-only overwrite pattern) | **Runtime Delta error** ("Detected a data update...") — not validated at onboarding, since the validator cannot see how a table's producing pipeline writes it | §4 above; see also [`07_reconciliation_engine.md`](07_reconciliation_engine.md) |
| `reconciliation_flows[].recon_mode` (any value) | **Validation error** at onboarding — removed in v1.4.0; reconciliation is triggered-only | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) §5 |
| `reconciliation_flows[].execution_mode: "pipeline"`/`"pipeline_audit_only"` with `read_mode: "streaming"` on either side | **Validation error** at onboarding — the in-pipeline comparison is a whole-snapshot batch classification; a stream-static join cannot express `MISSING_IN_SOURCE` | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) §11 |
| `reconciliation_flows[].execution_mode: "pipeline"`/`"pipeline_audit_only"` with `task_run_id_column` | **Validation error** at onboarding — a Lakeflow update exposes no stable per-update run id, so the narrowing would be a silent no-op | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) §11 |
| `reconciliation_flows[].execution_mode: "pipeline"`/`"pipeline_audit_only"` without a `dataflow_group_id` | **Validation error** at onboarding — a group-less flow has no pipeline to live in | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) §11 |
| `reconciliation_flows[].publish_schema` or `dq_config` with `execution_mode: "job"` (the default) | **Validation error** at onboarding — a job task publishes no dataset to name or to attach expectations to | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) §11 |
| A reconciliation `dq_config` rule with `action: "quarantine"` | **Validation error** at onboarding — there is nothing to quarantine on a one-row `__metrics` table | [`07_reconciliation_engine.md`](07_reconciliation_engine.md) §11 |
| `spark_config` setting `pipelines.incompatibleViewCheck.enabled: false` | **Known and rejected** — pipeline-wide, and it silences the check without making a streaming plan batch-readable | [`13_known_limitations_and_gotchas.md` L8](13_known_limitations_and_gotchas.md#l8) |
| `observability[]` destination with `mode: "continuous"` and no `destination_config.event_log_tables` | **Validation error** at onboarding | [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md) |
| `observability[]` destination with `mode: "triggered"` (the default) but `destination_config.event_log_tables` set anyway | **Validation error** at onboarding — the field is continuous-only | [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md) |
| A single `observability[]` destination served by both the triggered and continuous engines | **Structurally impossible** — `mode` is the only selector, and each engine filters to its own value | [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md) |

---

## Related documents

- [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) — source-type mechanics, landing retention, ZIP handling, JSON explode/flatten, dedup, column normalization.
- [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) — CDC strategy bodies, partitioning/clustering, `empty_target_if_source_empty`, snapshot surrogate keys.
- [`07_reconciliation_engine.md`](07_reconciliation_engine.md) — reconciliation modes, log controls, two-tier verification, Delta-only scope.
- [`08_observability_and_telemetry.md`](08_observability_and_telemetry.md) — triggered vs. continuous observability engines.
- [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md) — the one canonical hash construction shared by CDC, snapshot, and reconciliation.
- [`00_master_reference_index.md`](00_master_reference_index.md) — full field-level dictionary for every attribute referenced above.
