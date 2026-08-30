# ⚖️ Metaflow — Reconciliation & Self-Healing Engine

> **Audience**: Data quality leads, analytics engineers, and operations teams managing cross-system consistency, automated audit checks, and self-healing data pipelines.

---

## 1. Overview of Reconciliation

The Metaflow Reconciliation Engine provides automated, hash-first verification between a **source baseline dataset** and one or more **target datasets** (e.g. comparing a Bronze landing table against a transformed Gold dimension).

### Key Features
- **Hash-First Matching**: Uses deterministic SHA-256 row hashes (`__framework_hash_key` / `__framework_hash_value`, see [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md)) for a single-column join instead of a multi-column `match_keys` join, so a flow with a dozen `compare_columns` costs the same at join time as one with a single column.
- **Two-Tier Verification (v1.3.0)**: a cheap Phase 1 per-side fingerprint runs before any join and short-circuits the whole comparison when both sides agree; the full hash-key join and column-level discrepancy mapping (Phase 2) run only when Phase 1 reports a difference. See [§2](#2-reconciliation-flow-architecture) and [§4](#4-two-tier-verification-phase-1--phase-2).
- **Triggered execution, always (v1.4.0)**: every run is a bounded job task — batch reads, and `trigger(availableNow=True)` for a `read_mode: "streaming"` side, so the run drains its backlog and finishes. `recon_mode` is removed; see [§5](#5-execution-model-triggered-only).
- **Drift Classification**: Categorizes discrepancies into `MATCHED`, `MISSING_IN_TARGET`, `MISSING_IN_SOURCE`, and `VALUE_DRIFT`.
- **Per-Record Mismatch Audit Logging**: Stores detailed mismatch records in `{catalog}.config.reconciliation_mismatch_log`, including a full column-by-column `differing_columns_json` for every `VALUE_DRIFT` record.
- **Delta Tables Only (v1.3.0, breaking)**: reconciliation is supported for Delta tables (`type: "table"`) only. `file` and `sink` dataset types, previously accepted, are now rejected at both onboarding and runtime. See [§7](#7-delta-tables-only-scope-breaking-change-in-v130).
- **Self-Healing Append**: for a target configured with `comparison_direction: "source_to_target"` or `"both"`, missing/drifted records are automatically appended into that target's `append_target_table`, so a subsequent pipeline run picks the correction up. See [§8](#8-self-healing-append-behavior).

---

## 2. Reconciliation Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCE BASELINE DATASET                  │
│         source_config: { type: "table", table: ... }        │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼ prepare_dataset_for_matching()
                    (compute or trust __framework_hash_key /
                              __framework_hash_value)
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
      ┌─────────────────────┐    ┌─────────────────────┐
      │   PHASE 1 (cheap)    │    │  one per target_configs[]  │
      │ compute_side_        │    │        entry                │
      │ fingerprint()        │    └─────────────┬───────────────┘
      │ row_count + XOR fold │                  │
      │ of hash_key/         │                  ▼
      │ hash_value           │       fingerprints_match()?
      └──────────┬───────────┘                  │
                 │                    ┌──────────┴──────────┐
             match             no match / two_tier_verification: false
                 │                    │
                 ▼                    ▼
      ┌─────────────────────┐   ┌─────────────────────────────┐
      │  EARLY-OUT (no join) │   │   PHASE 2: full outer join   │
      │  status=SUCCESS,      │   │   on __framework_hash_key    │
      │  matched=all rows     │   │   + per-key max_by collapse  │
      └──────────┬───────────┘   │   + 4-way classification     │
                 │                └───────────────┬──────────────┘
                 │                                 │
                 ▼                                 ▼
      ┌──────────────────────────────────────────────────────────┐
      │        RUN SUMMARY (reconciliation_run_log) +              │
      │        RESULT (reconciliation_result, always written)      │
      └──────────────────────────────┬───────────────────────────┘
                                      │  (Phase 2 only, and only for
                                      │   MISSING_IN_TARGET/VALUE_DRIFT/
                                      │   MISSING_IN_SOURCE records)
                       ┌──────────────┼──────────────────┐
                       ▼              ▼                  ▼
             ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐
             │ MISMATCH LOG │  │ SELF-HEALING │  │  AUDIT-ONLY LOGGING │
             │ per-record   │  │ APPEND       │  │ MISSING_IN_SOURCE   │
             │ diff detail  │  │ (source_to_  │  │ (target_to_source,  │
             │              │  │  target/both)│  │  never appended)    │
             └──────────────┘  └──────────────┘  └────────────────────┘
```

The whole comparison above runs once per `target_configs[]` entry (`reconciliation/appender.py::run_target_reconciliation`, or its streaming counterpart `reconciliation/streaming.py::run_streaming_target_reconciliation`), driven by `notebooks/05_reconciliation/05_reconciliation_engine.py`.

---

## 3. Configuration Schema

Field names below are verified against `onboarding/spec_validator.py::_validate_reconciliation_flows` / `_validate_reconciliation_dataset_config` / `_validate_reconciliation_target_configs` / `_validate_logging_config` and `control_plane/ddl_definitions.py::get_reconciliation_flow_spec_ddl`. **This replaces the pre-v1.3.0 schema wholesale** — the old field names (`source_dataset`, `target_datasets`, `dataset_type`, `table_name`, `primary_keys`) never matched the actual validator and should not be carried forward from any earlier version of this document.

```json
{
  "reconciliation_flows": [
    {
      "reconciliation_id": "recon_orders_daily",
      "two_tier_verification": true,
      "logging_config": { "run_log_capture": true, "mismatch_log_capture": true },
      "source_config": {
        "type": "table",
        "table": "poc.bronze_sales.orders_raw",
        "read_mode": "batch",
        "task_run_id_column": "__framework_pipeline_run_id",
        "hash_precomputed": false
      },
      "target_configs": [
        {
          "target_id": "orders_silver",
          "type": "table",
          "table": "poc.silver_sales.orders",
          "read_mode": "batch",
          "hash_precomputed": true,
          "comparison_direction": "both",
          "append_target_table": "poc.bronze_sales.orders_raw"
        }
      ],
      "match_keys": ["order_id"],
      "compare_columns": ["status", "total"],
      "error_handling": { "on_failure": "fail" }
    }
  ]
}
```

### 3.1 Flow-level fields

| Field | Type | Default | Description |
|---|---|---|---|
| `reconciliation_id` | `string` | — (required) | Unique identifier for this flow. |
| `dataflow_group_id` | — | — | **Not a `reconciliation_flows[]` input field.** There is no per-flow `dataflow_group_id` key — every flow in an onboarding spec inherits the spec's own top-level `dataflow_group_id` (required there) into its persisted `reconciliation_flow_spec.dataflow_group_id` column; all flows in one spec share the same value. When that column is non-null, `${param}` placeholders in `source_config`/`target_configs`/`filter_condition` resolve against the group's `pipeline_parameters_json`. |
| `two_tier_verification` | `boolean` | `true` | See [§4](#4-two-tier-verification-phase-1--phase-2). |
| `logging_config` | `object` | `{}` (both flags `true`) | See [§6](#6-runtime-log-controls). |
| `source_config` | `object` | — (required) | See [§3.2](#32-per-side-dataset-fields-source_config--each-target_configs-entry). |
| `target_configs` | `array` (non-empty) | — (required) | See [§3.2](#32-per-side-dataset-fields-source_config--each-target_configs-entry) + [§3.3](#33-target-only-fields). |
| `match_keys` | `array<string>` | — (required) | Columns identifying the same logical record across datasets. Hashed into `__framework_hash_key` when `hash_precomputed` is `false`. |
| `compare_columns` | `array<string>` | none | Columns compared for drift once matched by key. Null/absent means **key-presence-only matching** — every key present on both sides counts as matched regardless of its other column values (`matcher.py` forces `hash_equal = True` when this list is empty). |
| `transform_sql` | `string` | none | Optional SQL reshaping the missing/drifted record set before it is appended to a target's `append_target_table` — only needed when source and target schemas differ. Validated for parse-ability the same way `transformation_sql` is (an `EXPLAIN`-based check, not a keyword allowlist), since it legitimately needs a full `SELECT ... FROM`. |
| `error_handling.on_failure` | `enum("fail", "warn")` | `"fail"` | Evaluated **per target**. `"fail"` propagates the first target's failure and stops the run. `"warn"` logs it, writes a `FAILED` `reconciliation_run_log` row for that target, and continues to the next target. |

### 3.2 Per-side dataset fields (`source_config` + each `target_configs[]` entry)

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `enum("table")` | `"table"` | **As of v1.3.0 the only legal value.** `file`/`sink` are rejected — see [§7](#7-delta-tables-only-scope-breaking-change-in-v130). |
| `table` | `string` | — (required) | Fully-qualified `catalog.schema.table`. Must resolve to a Delta table. |
| `read_mode` | `enum("batch", "streaming")` | `"batch"` | One flow can freely mix a streaming source against a batch target or vice versa — at most **one** side of a given target's comparison may be streaming (`reconciliation/streaming.py` does not support a stream-stream join; the four-way classification needs full outer-join semantics a stream-stream join can't give it). |
| `task_run_id_column` | `string` | none | **New in v1.3.0.** The column on this side carrying the producing pipeline/job run id. See [§5.2](#52-task_run_id_column-and-what-task_run_id-actually-filters). |
| `filter_condition` | `string` | none | Applied after the read (and after `task_run_id_column` narrowing, if any) — supports `${param}` substitution. |
| `data_standardization_sql` | `string` | none | Applied last, after `filter_condition`. |
| `hash_precomputed` | `boolean` | `false` | `true` means this side already carries `__framework_hash_key`/`__framework_hash_value` (e.g. a CDC-dispatched table materialized with `generate_hash_columns` enabled) — they are trusted as-is, never recomputed. Only valid when `type == "table"` — validated both at onboarding and again at runtime (`matcher.py::prepare_dataset_for_matching` raises `FrameworkConfigError` if the columns are actually missing). **See the migration caveat in [§9](#9-hash_precomputed-and-the-v130-hashing-change).** |

### 3.3 Target-only fields

| Field | Type | Default | Description |
|---|---|---|---|
| `target_id` | `string` | — (required) | Unique within the flow. |
| `comparison_direction` | `enum("source_to_target", "target_to_source", "both")` | `"both"` | Gates *action*, not classification — `matcher.py` always computes all four categories regardless. See [§8](#8-self-healing-append-behavior). |
| `append_target_table` | `string` | — (required when `comparison_direction` is `"source_to_target"` or `"both"`) | Where `source_to_target` misses (missing + drifted records) are appended — typically the same CDC/Zerobus source table feeding this target's own downstream materialization cycle. |

---

## 4. Two-Tier Verification: Phase 1 + Phase 2

**Phase 2 is not new** — it is the pre-v1.3.0 matching engine, unchanged: a full outer join on `__framework_hash_key`, a per-key `F.max_by` collapse (MATCHED beats VALUE_DRIFT beats MISSING_*, so a key counts as matched as soon as *any* target-side row for it matches — this protects against an append-only target legitimately holding more than one historical row for the same key), and four-way classification. `mismatch_logging.py` already produces full column-by-column `differing_columns_json` for every `VALUE_DRIFT` record.

**Phase 1 is genuinely new in v1.3.0**: a cheap, shuffle-free, per-side aggregate computed *before* the join, used purely as an early-out.

```
row_count       = count(1)
hash_key_xor    = order-independent bitwise-XOR fold of __framework_hash_key   across all rows
hash_value_xor  = order-independent bitwise-XOR fold of __framework_hash_value across all rows
```

Both folds run inside the *same* `.agg(...)` as `row_count` — one scan of the side's hash columns, no `groupBy`, no `orderBy`, no join. XOR's commutativity/associativity means the fold does not depend on row or partition order.

**Escalation rule**: if `row_count` differs on either side, or either XOR digest differs → escalate to Phase 2 (the full path above). If all three agree on both sides → early-out, no join at all.

**Early-out bookkeeping.** On a Phase 1 match, `reconciliation_run_log`/`reconciliation_result` are still written exactly as any other run: `matched_count = source_record_count`, all four discrepancy counts `= 0`, `appended_count = 0`, `failed_count = 0`, `status = "SUCCESS"`, and `fingerprint` is written as the **source-side** `hash_key_xor` digest (there is no miss set to fingerprint the usual way). The structured `reconciliation_match` log event carries `phase="PHASE_1_MATCH"` on an early-out vs. `phase="PHASE_2_DETAIL"` on a run that actually joined — this field exists specifically so an operator reading the event stream can tell "skipped the join" apart from "joined and found everything matched," which would otherwise be indistinguishable.

> **Accepted probabilistic property (documented in `reconciliation/matcher.py`'s module docstring).** A bitwise XOR fold cancels in pairs (`x XOR x == 0`), so two datasets differing by an even number of *identical duplicate* rows can fold to the same digest. Including `row_count` in the fingerprint catches every case where the cardinalities differ — i.e. every duplicate-count drift — so what remains is the narrow same-cardinality, even-multiplicity permutation case, the same collision class `appender.py::compute_batch_fingerprint`'s restartability fingerprint has always accepted. Concretely: **Phase 1 can produce a false "equal," but it can never produce a false "different."** A flow that cannot tolerate the false-equal case should set:
>
> ```json
> { "two_tier_verification": false }
> ```
>
> which always runs Phase 2 and pays the join cost on every run.

A key-presence-only flow (`compare_columns` absent/empty) still benefits correctly from Phase 1: `__framework_hash_value` is NULL throughout on both sides in that configuration, and a side with no rows or an all-NULL hash-value column folds to `"0" * 64` on both sides identically — so Phase 1's comparison correctly reduces to "same keys, same cardinality," exactly what Phase 2 would have concluded for it anyway (`matcher.py` forces `hash_equal = True` when `compare_columns` is empty).

---

## 5. Execution Model: Triggered Only

Every reconciliation run is a **bounded job task**. There is one execution model and no switch:

- `read_mode: "batch"` sides are read with a plain `spark.read`.
- `read_mode: "streaming"` sides run under `trigger(availableNow=True)` — everything currently
  available is processed across as many internal micro-batches as needed, then the query stops on
  its own and `awaitTermination()` returns. That matches a Lakeflow Job task's own run/finish
  lifecycle instead of running forever.
- For any side declaring `task_run_id_column`, the read is narrowed to the `task_run_id` job
  parameter's own rows — see [§5.2](#52-task_run_id_column-and-what-task_run_id-actually-filters).
  That narrowing is now unconditional whenever both are configured.

> [!WARNING]
> **BREAKING (v1.4.0): `recon_mode` is removed** — from the spec, the JSON schema, the
> `reconciliation_flow_spec` DDL, the validator, and the `05_reconciliation_engine.py` widgets.
> Onboarding rejects it:
> ```
> reconciliation_flow[<id>].recon_mode: removed in v1.4.0 -- reconciliation is triggered-only. Every run is a bounded job task: batch reads, and trigger(availableNow=True) for a read_mode 'streaming' side, so the run drains its backlog and finishes. There is no continuous reconciliation mode and no recon_mode widget; delete the key. For continuous coverage, schedule the reconciliation job on the cadence you need.
> ```
> Both former values are rejected, `"triggered"` included — it is removed as an *attribute*, so a
> spec asserting the surviving behaviour still names a field that no longer exists.

**Why `"continuous"` went.** It wrapped a standing stream around a *batch-shaped* unit of work.
`run_target_reconciliation` writes one `reconciliation_run_log` row per invocation, checks a batch
fingerprint for idempotency, and reports one set of aggregate counts. Under a never-ending query
that produced a log row per micro-batch whose fingerprint could never repeat, and counts describing
an arbitrary slice of wall clock rather than a comparison anyone asked for. Reconciliation answers
*"do these two datasets agree as of now"* — a question with a boundary in it. **To ask it more
often, schedule the job more often**; a job running every 15 minutes gives you bounded, auditable,
individually-idempotent answers, which is what the run log was built to hold.

Also removed: `reconciliation_flows[].generate_surrogate_key`. A reconciliation flow matches on the
`match_keys` it declares, and both sides must carry those columns — there is no longer a mode in
which the framework hashes each side's whole payload to invent a key for it.

A given target's comparison supports at most **one** streaming side (`source_config` *or* that
`target_configs[]` entry, never both) — a stream-stream join would need watermarking and only
supports inner/left-outer semantics, incompatible with the framework's full four-way
classification. Configure the non-streaming side as `read_mode: "batch"`.

Any side using `read_mode: "streaming"` requires the notebook's `checkpoint_root` widget to be set
— the run raises `FrameworkConfigError` otherwise.

### 5.1 Widget reference (`05_reconciliation_engine.py`)

| Widget | Values | Effect |
|---|---|---|
| `reconciliation_id` | free text | Required — which `reconciliation_flow_spec` row to run. |
| `task_run_id` | free text, optional | Parent job's own run id (`{{job.run_id}}` / `{{job.parameters.task_run_id}}`). Always written to log/result rows for correlation; additionally used as a filter per [§5.2](#52-task_run_id_column-and-what-task_run_id-actually-filters). |
| `recon_run_log_capture` | `""` \| `"true"` \| `"false"` | Runtime override of `logging_config.run_log_capture`. See [§6](#6-runtime-log-controls). |
| `recon_mismatch_log` | `""` \| `"true"` \| `"false"` | Runtime override of `logging_config.mismatch_log_capture`. See [§6](#6-runtime-log-controls). |
| `checkpoint_root` | path, required only if any side is `read_mode: "streaming"` | Root path; the actual checkpoint used per target is `{checkpoint_root}/{reconciliation_id}/{target_id}`. |

### 5.2 `task_run_id_column` and what `task_run_id` actually filters

Pre-v1.3.0, `task_run_id` was purely a **correlation** value — threaded into `reconciliation_run_log`/`reconciliation_mismatch_log`/`reconciliation_result` for traceability, but it filtered nothing.

v1.3.0 adds an optional per-side field, `task_run_id_column`, on `source_config` and on each `target_configs[]` entry. When **both** hold:

- the `task_run_id` widget/job parameter is non-empty, and
- that side declares a `task_run_id_column`,

that side's read is narrowed with `df.filter(F.col(task_run_id_column) == task_run_id)` — applied in `dataset_reader.py` immediately after the read and **before** `filter_condition`, so a `filter_condition` can narrow the already-narrowed set further (the reverse order would let `filter_condition` appear to widen a set it cannot widen).

With no `task_run_id_column` configured on a side, `task_run_id` stays correlation-only — fully backward compatible with every pre-v1.3.0 flow.

The recommended value is `"__framework_pipeline_run_id"`: `dq/quarantine.py::add_quarantine_columns` stamps it on every row of every framework-materialized table, and it survives onto every downstream main table (`_clean_upstream` only drops the quarantine-process columns, not this one). It is **not** hardcoded anywhere in the framework — a third-party table's equivalent run-id column can be named anything and configured the same way.

There used to be a third condition — `recon_mode` resolving to `"triggered"` — because a standing stream had no bounded run for a single producing-run id to mean anything against. With `recon_mode` removed every run is bounded, so that condition is gone and the narrowing is unconditional.

One asymmetry remains and is deliberate: in a mixed batch/streaming comparison, only the **static** side is narrowed. The streaming side's own micro-batch offsets already are the batch boundary, and filtering it by a single producing-run id would silently discard every offset belonging to any other one — data the checkpoint then advances past and never re-offers.

---

## 6. Runtime Log Controls

Two independent layers control whether `reconciliation_run_log` / `reconciliation_mismatch_log` rows get written for a run. `reconciliation_result` is **always** written by both layers — a fully-silenced run still leaves a record that it happened.

1. **Onboarded, per-flow layer**: `logging_config.run_log_capture` / `logging_config.mismatch_log_capture`, both default `true`. Persisted as `reconciliation_flow_spec.logging_config_json`.
2. **Runtime, job-parameter layer** (v1.3.0): the notebook widgets `recon_run_log_capture` / `recon_mismatch_log`. Each is **tri-state**: `""` (default — defer to `logging_config`), `"true"`, `"false"`.

**Precedence, highest first** (`reconciliation/appender.py::resolve_log_capture_flags`, the single function that decides this — pure, no Spark, unit-tested with plain dicts):

1. `recon_run_log_capture` / `recon_mismatch_log` job parameter, when set to `"true"`/`"false"`.
2. `logging_config.run_log_capture` / `logging_config.mismatch_log_capture` from the flow spec.
3. `true`.

The two naming schemes are deliberately distinct rather than unified: `recon_*` names a runtime job/pipeline parameter that lives on one job run and evaporates afterward; `logging_config.*` names onboarded flow metadata that persists and is reviewed like any other spec field. An operator firefighting a runaway continuous flow needs to silence log writes for one run without re-onboarding the flow, and needs to be able to tell at a glance whether a value came from metadata or from the run they just launched — a plain boolean widget cannot express "I am not expressing an opinion," which is why the widgets are tri-state dropdowns rather than plain booleans.

```json
{ "logging_config": { "run_log_capture": true, "mismatch_log_capture": false } }
```

> **Note:** `logging_config` was already read by the validator and persisted to `reconciliation_flow_spec.logging_config_json` before v1.3.0, but it was never actually present in `onboarding_spec.schema.json` — it validated only because the reconciliation flow schema allowed additional properties. v1.3.0 adds it to the JSON Schema explicitly.

---

## 7. Delta-Tables-Only Scope (Breaking Change in v1.3.0)

`ALLOWED_RECON_DATASET_TYPES` narrows from `{"table", "file", "sink"}` to `{"table"}`. `file` and `sink` are rejected both at onboarding (`spec_validator.py`) and at runtime (`dataset_reader.py`) — there is no grace period or silent fallback.

**Onboarding-time error** (`onboarding/spec_validator.py::_validate_reconciliation_dataset_config`, verbatim):

```
<path>.type: reconciliation is supported for Delta tables only -- type must be 'table', got 'file'. Read the file/sink output into a Delta table first, then reconcile against that table.
```

**Runtime error** (`reconciliation/dataset_reader.py::read_reconciliation_dataset`, verbatim — the defense-in-depth check for a hand-edited control-table row):

```
Unsupported reconciliation dataset type 'file' -- reconciliation is supported for Delta tables only ('table'). Read the file/sink output into a Delta table first, then reconcile against that table.
```

`type: "table"` alone does not prove the table is Delta — Unity Catalog also holds foreign/federated tables, Parquet/CSV external tables, and views. `dataset_reader.py` additionally resolves the table's provider via `DESCRIBE TABLE EXTENDED` and raises if it is a determinable, non-Delta provider:

```
Reconciliation dataset table 'poc.bronze.orders_raw' is not a Delta table (provider='parquet') -- reconciliation is supported for Delta tables only.
```

If the provider genuinely cannot be determined (a temporary view, or a catalog that doesn't answer `DESCRIBE TABLE EXTENDED`), the read proceeds with a logged `WARNING` rather than failing — refusing to read something merely because its metadata was unreadable would fail runs that work today, which is judged a worse failure mode than the one this check guards against.

**Why the narrowing.** The dropped `type: "file"`/`"sink"` path read a raw location with `spark.read.format(...).load(path)`. That path could never carry precomputed `__framework_hash_key`/`__framework_hash_value` (forcing an inline re-hash of a whole file tree on every run), had no transaction boundary (a file landing mid-run could silently change what "the source" meant between the fingerprint and the append), and gave the matcher no file skipping to make its documented multiple re-reads of each side affordable. Every one of those problems disappears once the location has been ingested into a Delta table first — which every deployment of this framework is already set up to do. **Migration path**: land the file/sink output into a Delta table (via an ingestion flow, or any other means), then point `source_config`/`target_configs[].table` at that table.

---

## 8. Self-Healing Append Behavior

There is **no `enable_self_healing` flag** in the current validator, DDL, or runtime code — this is a correction to this document; a prior version referenced `enable_self_healing: true` as a flow-level switch, but no such field exists in `onboarding/spec_validator.py` or `control_plane/ddl_definitions.py`. The append behavior described below is unconditional for any target configured to want it, governed entirely by `comparison_direction` and `append_target_table` (see [§3.3](#33-target-only-fields)):

1. `matcher.py::match_reconciliation_target` classifies every record in one full outer join, regardless of `comparison_direction` — computing all four categories is effectively free once the join has run.
2. What `comparison_direction` actually gates is **action**:
   - `"source_to_target"` / `"both"` → the missing/drifted record set (`MISSING_IN_TARGET` + `VALUE_DRIFT`) is appended into `append_target_table`, and those records are mismatch-logged.
   - `"target_to_source"` / `"both"` → `MISSING_IN_SOURCE` records are mismatch-logged for audit **only** — this direction never appends, never mutates the target. This is an explicit design constraint: nothing in this module or its callers is permitted to delete or modify a target based on a `MISSING_IN_SOURCE` finding.
   - A `"source_to_target"`-only target still gets `missing_in_source_count` populated in `reconciliation_run_log` (it's part of the same free aggregation), but no `MISSING_IN_SOURCE` rows are written to `reconciliation_mismatch_log` for it.
3. A drifted record is **re-appended as a correction**, not updated in place — consistent with the append-only CDC/Zerobus bus model this framework's targets are typically built on.
4. **Restartability (`read_mode: "batch"` only)**: every run computes a deterministic fingerprint of the target's `source_to_target` miss set. Before appending, this target's slice of `reconciliation_run_log` is checked for a prior `SUCCESS` run with that exact fingerprint — if found, the run is a no-op (`status: "SKIPPED_ALREADY_PROCESSED"`), so re-running reconciliation against an unchanged source snapshot never appends duplicate correction rows. A `read_mode: "streaming"` target instead relies on Spark Structured Streaming's own checkpoint for restart safety (`reconciliation/streaming.py`) — the fingerprint mechanism is documented as batch-only on the `reconciliation_run_log.source_batch_fingerprint` column itself.
5. **Duplicate-key safety.** A target dataset is frequently an append-only bus and may legitimately hold more than one historical row for the same logical key (e.g. a prior reconciliation run's own correction, appended *alongside* the stale drifted row it corrects, not replacing it). After the hash-key join, every group of rows sharing the same `__framework_hash_key` is collapsed to one representative outcome via `F.max_by`, ordered by an explicit `MATCHED > VALUE_DRIFT > MISSING_*` priority — a key counts as matched as soon as *any* target-side row for it satisfies the match condition. Without this collapse, the same source record could appear as both matched (via its new corrected counterpart) and unmatched (via the stale row still sitting alongside it), and the append would never converge — it would re-append a fresh "correction" on every subsequent run.

---

## 9. `hash_precomputed` — Exact Mechanics

**It is an assertion about the dataset, not an instruction to the engine.** The name reads like an
imperative ("precompute the hashes"), and it is not one. `hash_precomputed: true` declares *"the
hashes for this dataset were already computed, upstream, when the table was materialized."* It
never causes anything to be precomputed.

It is a **per-side** field: it appears on `source_config` and independently on each
`target_configs[]` entry, and the two sides routinely disagree — a framework-managed target with
`true` reconciled against a third-party source with `false` is the ordinary shape.

### What each value actually does

Everything below happens in `reconciliation/matcher.py::prepare_dataset_for_matching`, called once
per side before any join.

| Value | Behaviour |
|---|---|
| **`false`** (default) | The engine computes both columns **now, from raw columns**: `__framework_hash_key` = SHA-256 over this flow's `match_keys` **in the declared order**; `__framework_hash_value` = SHA-256 over the resolved `compare_columns` (**alphabetically sorted**). Anything the dataset already carried under those two names is overwritten. An empty/absent `compare_columns` yields a NULL `__framework_hash_value`, and matching correctly reduces to key presence. |
| **`true`** | The dataset **must already carry** `__framework_hash_key` and `__framework_hash_value`. They are trusted verbatim; neither `match_keys` nor `compare_columns` is hashed here at all. |

So — to answer the question directly: **`hash_precomputed` does not precompute row hashes over
`match_columns`.** Computing hashes over the flow's declared keys is what `false` does. `true` is
the opposite: it expects pre-calculated hashes to be present, and skips the computation.

### Where the pre-calculated hashes come from

Not "from the source" in the sense of a third party's own hash column. They come from **this
framework**, one layer upstream: any CDC-dispatched flow (`SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`)
with `generate_hash_columns` enabled — the default — writes both columns onto its target table at
materialization time via `dq/quarantine.py::_apply_hash_columns`. See
[`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) §4.

A `true` on a dataset that never went through such a flow is a hard error, not a silent recompute:
```
hash_precomputed=True but dataset is missing ['__framework_hash_key', '__framework_hash_value'] -- this dataset was not actually produced by a CDC-dispatched flow with generate_hash_columns enabled (see cdc/hashing.py), so its __framework_hash_key/__framework_hash_value cannot be trusted for reconciliation matching
```
`hash_precomputed: true` is also rejected at onboarding for any dataset whose `type` is not
`"table"` — only a framework-managed table can carry pre-built hash columns.

### The precondition that makes the trust safe

This is the part to get right, because getting it wrong fails **silently**:

> The upstream flow's `primary_keys` must be this reconciliation flow's `match_keys`, and its
> resolved comparison columns must be this flow's `compare_columns`.

Both sides use the same canonical construction (see
[`11_hashing_and_determinism.md`](11_hashing_and_determinism.md)), so when they agree the hashes are
directly comparable and the join gets very cheap. When they **disagree**, nothing errors — the
hashes simply never match, and every row reports as `VALUE_DRIFT` or missing. Note the ordering
rules differ between the two columns and must be respected: `__framework_hash_key` preserves the
caller's declared key order (so `(a, b)` and `(b, a)` are different keys), while
`__framework_hash_value` is always computed over an alphabetically sorted column list.

### Best practice

| Situation | Setting |
|---|---|
| Framework-materialized table, CDC-dispatched, `match_keys` = its `primary_keys` | `true` — this is what the flag is for. Reconciling large tables is a hash-key join; precomputing once at materialization and clustering the table on `__framework_hash_key` (see `storage/table_properties.py`) is what makes it cheap at scale. |
| Third-party table, a view, or an `APPEND`/`TRUNCATE_AND_LOAD` target (no hash columns exist) | `false` |
| Framework table, but this flow matches on a **different** key or compares **different** columns than the flow that materialized it | `false` — the stored hashes describe a different question. |
| Unsure | `false`. It costs one hash pass per side and is always correct; `true` on a mismatched basis is silently wrong. |

### Upgrade note — the v1.3.0 hashing change

v1.3.0 replaced three independent hash constructions with one canonical implementation.

> A target with `hash_precomputed: true` whose stored `__framework_hash_key`/`__framework_hash_value` were computed by the **pre-v1.3.0** construction will report spurious drift (or a spurious Phase 1 mismatch) on the first reconciliation run after upgrade, because the stored hashes and the freshly-computed source-side hashes are no longer produced by the same algorithm. **Required action before the first post-upgrade run**: re-materialize that target through its own pipeline (so its stored hash columns are recomputed under the new standard), or temporarily set `hash_precomputed: false` on that side so its hashes are recomputed inline by the reconciliation engine itself instead of trusted as stored.

The same upgrade also means `reconciliation_run_log.source_batch_fingerprint` history from before the upgrade no longer matches what the new hash construction produces — the first post-upgrade run simply can't short-circuit to `SKIPPED_ALREADY_PROCESSED` and re-evaluates once. This is a one-time, expected side effect, not a correctness issue.

---

## 10. Drift Categories & Diagnostic Mismatch Schema

When discrepancies are detected (Phase 2 only — a Phase 1 early-out writes nothing here), rows are written to `{catalog}.config.reconciliation_mismatch_log`, conditional on `mismatch_log_capture` resolving `true` (see [§6](#6-runtime-log-controls)). Column names below are verified against `control_plane/ddl_definitions.py::get_reconciliation_mismatch_log_ddl` and `reconciliation/mismatch_logging.py::write_mismatch_log_rows` — **this corrects the previous version of this table**, which used field names (`dataset_id`, `primary_key_values_json`, `column_diffs_json`, `created_at`) that do not exist on the actual table.

| Column Name | Type | Description |
|---|---|---|
| `mismatch_id` | `STRING` | Unique identifier for this mismatch row (UUID). |
| `run_id` | `STRING` | The `reconciliation_run_log.run_id` this mismatch was detected during. |
| `reconciliation_id` | `STRING` | Identifier of the reconciliation flow. |
| `target_id` | `STRING` | Which `target_configs[]` entry this mismatch was detected against. |
| `match_key_values_json` | `STRING` | JSON object of `match_keys` column → value identifying the offending record. |
| `mismatch_type` | `STRING` | `MISSING_IN_TARGET`, `MISSING_IN_SOURCE`, or `VALUE_DRIFT`. (`MATCHED` never appears here — it exists only inside the matcher's own classification.) |
| `differing_columns_json` | `STRING` | JSON array of `{column, source_value, target_value}` for every `compare_columns` entry that actually differs — populated **only** for `VALUE_DRIFT`, and only lists columns genuinely responsible for the drift (not every compared column). |
| `source_hash_value` | `STRING` | `__framework_hash_value` on the source side, if available. |
| `target_hash_value` | `STRING` | `__framework_hash_value` on the target side, if available. |
| `task_run_id` | `STRING` | Same parent job run id as the owning `reconciliation_run_log` row, if any. |
| `detected_at` | `TIMESTAMP` | UTC timestamp when the discrepancy was recorded. |

`reconciliation_run_log` (one row per target per run, when `run_log_capture` resolves `true`) additionally reports `source_record_count`, `target_record_count`, `matched_count`, `missing_in_target_count` (union of `MISSING_IN_TARGET` + `VALUE_DRIFT`), `value_drift_count`, `missing_in_source_count`, `appended_count`, `failed_count`, `status` (`SUCCESS` | `FAILED` | `SKIPPED_ALREADY_PROCESSED`), and `source_batch_fingerprint`. `reconciliation_result` (always written, independent of both logging flags) carries a lighter always-populated pass/fail summary with the same four discrepancy counts, `status`, and `task_run_id` for correlation.

---

## 11. Related Documentation

- [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md) — the canonical `__framework_hash_key`/`__framework_hash_value` construction shared by ingestion, CDC/transformation, and this reconciliation engine, plus the v1.3.0 breaking-change migration checklist referenced in [§9](#9-hash_precomputed-and-the-v130-hashing-change).
- [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) — how `__framework_hash_key`/`__framework_hash_value` are computed at CDC-materialization time on the target side of a comparison (what makes `hash_precomputed: true` possible at all).
- [`00_master_reference_index.md`](00_master_reference_index.md) §8 — the full reconciliation flow field reference.
