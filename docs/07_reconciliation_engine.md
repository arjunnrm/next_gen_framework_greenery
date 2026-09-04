# ⚖️ FlowX — Reconciliation & Self-Healing Engine

> **Audience**: Data quality leads, analytics engineers, and operations teams managing cross-system consistency, automated audit checks, and self-healing data pipelines.

---

## 1. Overview of Reconciliation

The FlowX Reconciliation Engine provides automated, hash-first verification between a **source baseline dataset** and one or more **target datasets** (e.g. comparing a Bronze landing table against a transformed Gold dimension).

### Key Features
- **Hash-First Matching**: Uses deterministic SHA-256 row hashes (`__framework_hash_key` / `__framework_hash_value`, see [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md)) for a single-column join instead of a multi-column `match_keys` join, so a flow with a dozen `compare_columns` costs the same at join time as one with a single column.
- **Two-Tier Verification (v1.3.0)**: a cheap Phase 1 per-side fingerprint runs before any join and short-circuits the whole comparison when both sides agree; the full hash-key join and column-level discrepancy mapping (Phase 2) run only when Phase 1 reports a difference. See [§2](#2-reconciliation-flow-architecture) and [§4](#4-two-tier-verification-phase-1-phase-2).
- **Triggered execution, always (v1.4.0)**: every run is a bounded job task — batch reads, and `trigger(availableNow=True)` for a `read_mode: "streaming"` side, so the run drains its backlog and finishes. `recon_mode` is removed; see [§5](#5-execution-model-triggered-only).
- **Drift Classification**: Categorizes discrepancies into `MATCHED`, `MISSING_IN_TARGET`, `MISSING_IN_SOURCE`, and `VALUE_DRIFT`.
- **Per-Record Mismatch Audit Logging**: Stores detailed mismatch records in `{catalog}.config.reconciliation_mismatch_log`, including a full column-by-column `differing_columns_json` for every `VALUE_DRIFT` record.
- **Delta Tables Only (v1.3.0, breaking)**: reconciliation is supported for Delta tables (`type: "table"`) only. `file` and `sink` dataset types, previously accepted, are now rejected at both onboarding and runtime. See [§7](#7-delta-tables-only-scope-breaking-change-in-v130).
- **In-pipeline reconciliation (v1.5.0)**: `execution_mode` promotes a reconciliation flow to a **third flow type inside the dataflow group's own Lakeflow pipeline update**, materializing the comparison as real Lakeflow datasets (`__classified` / `__metrics` / `__mismatch`) and re-hosting the imperative append inside one `foreach_batch_sink` handler. It defaults to `"job"`, so nothing already deployed changes until an author opts in. **Proven live on 2026-08-31** — one update of pipeline `be78d88d-6064-414d-a10c-2aacd900fa86` registered ingestion, the L3/L4 comparison datasets, the L5 pulse, the heal flow *and* the `foreach_batch_sink` handler in a single DAG (the exact node list is in [§11.1](#111-the-l0l5-node-map)). See [§11](#11-execution-modes-job-pipeline-pipeline_audit_only), and — before switching any flow to `"pipeline"` — [§11.7](#117-the-reconciliation-source-must-be-append-only-in-pipeline-mode).
- **The Intermediate Object Rule + a tightened logging contract (v1.6.0)**: every true reconciliation intermediate (`_recon__*__src`/`__tgt`/`__classified`/`__missing`/pulse) is now a pipeline-scoped **temporary** table — materialized once per update, never published to Unity Catalog — and the two published audit datasets, `recon__*__metrics` / `recon__*__mismatch`, are registered **only when their capture flag resolves true**. `logging_config.run_log_capture` now also gates `reconciliation_result` (previously unconditional). See [§6](#6-runtime-log-controls), [§11.1](#111-the-l0l5-node-map) and [§11.10](#1110-v160--the-intermediate-object-rule-and-the-conditional-audit-datasets); **before upgrading an already-deployed pipeline**, read the migration trap [`13_known_limitations_and_gotchas.md` O7](13_known_limitations_and_gotchas.md#o7).
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
      │        RESULT (reconciliation_result) — both gated by      │
      │        run_log_capture (v1.6.0, see §6)                    │
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
| `dataflow_group_id` | `string` | the spec's own top-level `dataflow_group_id` | **New in v1.5.0 — this IS a per-flow field.** Which dataflow group's Lakeflow pipeline this flow is registered into. Omit it and the flow inherits the spec's own top-level group, which is the overwhelmingly common case and the pre-v1.5.0 behaviour. Naming a *different* group registers the flow inside THAT group's pipeline update instead — the supported way to reconcile against, or heal into, tables another group owns. **Required when `execution_mode` is `"pipeline"`/`"pipeline_audit_only"`** (V-CYC-6: a group-less flow has no pipeline to be registered into). Persisted to `reconciliation_flow_spec.dataflow_group_id`; when that column is non-null, `${param}` placeholders in `source_config`/`target_configs`/`filter_condition` resolve against that group's `pipeline_parameters_json`. |
| `two_tier_verification` | `boolean` | `true` | See [§4](#4-two-tier-verification-phase-1-phase-2). **v1.5.0** — now genuinely persisted to `reconciliation_flow_spec.two_tier_verification`. Before v1.5.0 that column had no `StructField` and the Row literal never set it, so `false` onboarded cleanly and was silently discarded, and the runtime then defaulted to `true` — a silent behaviour inversion. |
| `execution_mode` | `enum("job", "pipeline", "pipeline_audit_only")` | `"job"` | **New in v1.5.0.** Where this flow runs. `"job"` is today's `05_reconciliation_engine.py` job task, byte-for-byte. `"pipeline"` registers the flow as a third flow type *inside* the owning dataflow group's Lakeflow pipeline update — the L3/L4 published datasets **and** the L5 heal lane. `"pipeline_audit_only"` registers L3+L4 only: comparison, metrics and `dq_config` expectations run in-pipeline while healing stays in job mode. Full treatment: [§11](#11-execution-modes-job-pipeline-pipeline_audit_only). |
| `publish_schema` | `string` | the hosting pipeline's own schema | **New in v1.5.0.** Schema (inside the pipeline's own catalog) that this flow's **published** datasets land in — since v1.6.0 that is `recon__<rid>__<tid>__metrics` / `__mismatch` (each registered only when its capture flag is on) plus a healing flow's `_src`/healing `_tgt`; every other recon dataset is a pipeline-scoped temporary table and is never published anywhere ([§11.10](#1110-v160--the-intermediate-object-rule-and-the-conditional-audit-datasets)). They are real, externally visible UC tables, so the default — the pipeline's own schema — is often not where you want reconciliation output to land. Persisted to `reconciliation_flow_spec.publish_schema`. **Rejected on presence when `execution_mode` is `"job"`**, which publishes no datasets. How the default is actually resolved, and why it used to resolve to `None`: [§11.9](#119-where-the-published-datasets-land--publish_schema-resolution). |
| `dq_config` | `object` (the same shape as an ingestion/transformation `dq_config`) | SQL `NULL` | **New in v1.5.0.** Expectations attached to the one-row `__metrics` dataset — e.g. `{"rules": [{"rule_id": "no_value_drift", "expression": "value_drift_count = 0", "action": "fail"}]}`. Each rule requires `rule_id`, `expression` and `action` — `name`/`expr` are NOT accepted. Unlike the ingestion and transformation paths, which persist `{}` when the block is omitted, this one persists SQL `NULL` (`metadata_upsert.py`), so "no expectations" is distinguishable from "an empty rule set". This is the **first declarative way a reconciliation threshold can fail a pipeline update**, and it is *additive*: it does not repurpose `error_handling.on_failure`, which keeps its exception-level try/except meaning. `action: "quarantine"` is rejected (there is nothing to quarantine on a one-row metrics table), and the whole block is **rejected on presence when `execution_mode` is `"job"`**. |
| `logging_config` | `object` | `{}` (both flags `true`) | See [§6](#6-runtime-log-controls). |
| `source_config` | `object` | — (required) | See [§3.2](#32-per-side-dataset-fields-source_config-each-target_configs-entry). |
| `target_configs` | `array` (non-empty) | — (required) | See [§3.2](#32-per-side-dataset-fields-source_config-each-target_configs-entry) + [§3.3](#33-target-only-fields). |
| `match_keys` | `array<string>` | — (required) | Columns identifying the same logical record across datasets. Hashed into `__framework_hash_key` when `hash_precomputed` is `false`. |
| `compare_columns` | `array<string>` | none | Columns compared for drift once matched by key. Null/absent means **key-presence-only matching** — every key present on both sides counts as matched regardless of its other column values (`matcher.py` forces `hash_equal = True` when this list is empty). |
| `transform_sql` | `string` | none | Optional SQL reshaping the missing/drifted record set before it is appended to a target's `append_target_table` — only needed when source and target schemas differ. Validated for parse-ability the same way `transformation_sql` is (an `EXPLAIN`-based check, not a keyword allowlist), since it legitimately needs a full `SELECT ... FROM`. |
| `error_handling.on_failure` | `enum("fail", "warn")` | `"fail"` | Evaluated **per target**. `"fail"` propagates the first target's failure and stops the run. `"warn"` logs it, writes a `FAILED` `reconciliation_run_log` row for that target, and continues to the next target. |

### 3.2 Per-side dataset fields (`source_config` + each `target_configs[]` entry)

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `enum("table")` | `"table"` | **As of v1.3.0 the only legal value.** `file`/`sink` are rejected — see [§7](#7-delta-tables-only-scope-breaking-change-in-v130). |
| `table` | `string` | — (required) | Fully-qualified `catalog.schema.table`. Must resolve to a Delta table. |
| `read_mode` | `enum("batch", "streaming")` | `"batch"` | **v1.5.0: `"streaming"` is rejected on presence when `execution_mode` is `"pipeline"` or `"pipeline_audit_only"`** — the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner and left-outer semantics, which cannot express `MISSING_IN_SOURCE`. In job mode, unchanged: one flow can freely mix a streaming source against a batch target or vice versa — at most **one** side of a given target's comparison may be streaming (`reconciliation/streaming.py` does not support a stream-stream join; the four-way classification needs full outer-join semantics a stream-stream join can't give it). |
| `task_run_id_column` | `string` | none | **New in v1.3.0.** The column on this side carrying the producing pipeline/job run id. See [§5.2](#52-task_run_id_column-and-what-task_run_id-actually-filters). **v1.5.0: rejected on presence when `execution_mode` is `"pipeline"`/`"pipeline_audit_only"`.** A Lakeflow update exposes no stable per-update key: `pipelines.id` is the *pipeline* id, constant across every update, and it is the same value `dq/quarantine.py::add_quarantine_columns` stamps into `__framework_pipeline_run_id` — narrowing by it would match every row that pipeline has ever written, i.e. a **silent no-op**. Use `filter_condition`, or keep `execution_mode: "job"`. |
| `filter_condition` | `string` | none | Applied after the read (and after `task_run_id_column` narrowing, if any) — supports `${param}` substitution. |
| `data_standardization_sql` | `string` | none | Applied last, after `filter_condition`. |
| `hash_precomputed` | `boolean` | `false` | **An assertion, not an instruction — it never precomputes anything.** `true` declares this side ALREADY carries `__framework_hash_key`/`__framework_hash_value` from an upstream CDC-dispatched flow with `generate_hash_columns` enabled; they are trusted verbatim and `match_keys`/`compare_columns` are not hashed at all. `false` computes both here and now. Only valid when `type == "table"` — validated at onboarding and again at runtime (`matcher.py::prepare_dataset_for_matching` raises `FrameworkConfigError` if the columns are actually missing). **Full mechanics, the precondition that makes the trust safe, and the migration caveat: [§9](#9-hash_precomputed-exact-mechanics).** |

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

> **What a Phase-1 match skips, precisely (clarified in v1.5.0).** It skips **the join and the append** — and *never* the log writes. `_complete_phase_1_match` exists exactly to write the same `reconciliation_run_log` / `reconciliation_result` rows a Phase-2 run would have written, subject to the usual [§6](#6-runtime-log-controls) gates. The semantics are identical under `execution_mode: "pipeline"`: the fingerprint short-circuit lives in the `foreach_batch_sink` handler, where `.collect()` is legal, so it genuinely still skips the join and the append there too.

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
| `checkpoint_root` | path, required only if any side is `read_mode: "streaming"` | Root path; the actual checkpoint used per target is `{checkpoint_root}/{reconciliation_id}/{target_id}`. **Job mode only** — meaningless under `execution_mode: "pipeline"`, where Lakeflow owns every checkpoint. |

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

Two independent layers control whether a run's log rows get written. **What the flags gate widened in v1.6.0**:

| Resolved flag | Gates (v1.6.0) | Pre-v1.6.0 |
|---|---|---|
| `run_log_capture` | `reconciliation_run_log` rows, **`reconciliation_result` rows**, and — in pipeline mode — whether the `recon__*__metrics` dataset is **registered at all** | `reconciliation_run_log` rows only; `reconciliation_result` was always written; `__metrics` was always registered |
| `mismatch_log_capture` | `reconciliation_mismatch_log` rows and — in pipeline mode — whether the `recon__*__mismatch` dataset is registered | `reconciliation_mismatch_log` rows only; `__mismatch` was always registered |

**Both flags resolved `false` means reconciliation persists to NOTHING but its business targets** — no metric/log dataset is created, no control-table row is written (a failed run included), and the run's job/pipeline state plus the structured log events are the only failure signal. That is the deliberate v1.6.0 contract: `logging_config` is a real off-switch, not a partial one. Two contradictory configurations are rejected — at onboarding (`spec_validator.py::_validate_logging_config`) *and* again at graph definition (`graph_registration.py`, catching runtime pipeline-conf overrides):

* `dq_config.rules` while `run_log_capture` resolves `false` — the flow's expectations attach to its `__metrics` dataset, which would not exist, silently dropping declared data-quality checks.
* `execution_mode: "pipeline_audit_only"` with **both** flags `false` — audit-only exists solely to produce the metrics/mismatch datasets and their control-table exports, so this combination registers compute with no output at all.

The two layers:

1. **Onboarded, per-flow layer**: `logging_config.run_log_capture` / `logging_config.mismatch_log_capture`, both default `true`. Persisted as `reconciliation_flow_spec.logging_config_json`.
2. **Runtime, job-parameter layer** (v1.3.0): the notebook widgets `recon_run_log_capture` / `recon_mismatch_log`. Each is **tri-state**: `""` (default — defer to `logging_config`), `"true"`, `"false"`. In pipeline mode the equivalents are the pipeline-conf keys `dataflow.recon.run_log_capture` / `dataflow.recon.mismatch_log`, and since v1.6.0 they participate in the graph-definition decision (which datasets exist), not just the write decision.

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

`reconciliation_run_log` (one row per target per run, when `run_log_capture` resolves `true`) additionally reports `source_record_count`, `target_record_count`, `matched_count`, `missing_in_target_count` (union of `MISSING_IN_TARGET` + `VALUE_DRIFT`), `value_drift_count`, `missing_in_source_count`, `appended_count`, `failed_count`, `status` (`SUCCESS` | `FAILED` | `SKIPPED_ALREADY_PROCESSED`), and `source_batch_fingerprint`. `reconciliation_result` (v1.6.0: gated by the same `run_log_capture` flag — previously written unconditionally) carries a lighter pass/fail summary with the same four discrepancy counts, `status`, and `task_run_id` for correlation.

---

## 11. Execution Modes: `job`, `pipeline`, `pipeline_audit_only`

**New in v1.5.0.** Until v1.5.0 reconciliation was always a separate job task reading tables the
Lakeflow pipeline had already finished writing. `execution_mode` lets a flow instead become a
**third first-class flow type inside the dataflow group's own pipeline update**, beside ingestion
and transformation.

| `execution_mode` | What is registered | Where healing happens | When to choose it |
|---|---|---|---|
| `"job"` **(default)** | Nothing in the pipeline | `05_reconciliation_engine.py` job task, byte-for-byte as today | Anything already deployed; a flow with no `dataflow_group_id`; a flow whose source is static or low-change (see [§11.5](#115-limitation-healing-is-source-change-triggered-in-pipeline-mode)); any `read_mode: "streaming"` side |
| `"pipeline_audit_only"` | L3 + L4 (prepared sides, `__classified`, `__metrics`, `__mismatch`) | Still the job task | You want the comparison, the metrics and `dq_config` expectations inside the update — and healing on the job's own cadence. **Also the only legal pipeline mode when the reconciliation source is an in-graph `TRUNCATE_AND_LOAD` / `SCD1` / `SCD2` / `SCD3` / `FULL_SNAPSHOT_CDC` / `materialized_view` target** — see [§11.7](#117-the-reconciliation-source-must-be-append-only-in-pipeline-mode) |
| `"pipeline"` | L3 + L4 + L5 (pulse, heal flow, `foreach_batch_sink` handler) | Inside the update, in the handler | A group whose source genuinely advances every update, is written **append-only** by its producing flow, and which wants detection *and* correction in one graph |

`"job"` is the default deliberately and permanently: several DABs resources still run
reconciliation tasks against onboarded rows, and defaulting to `"pipeline"` would run those flows
**twice per cycle**. Opting in is one line, one flow at a time.

Two things become **required** in the two pipeline modes: `dataflow_group_id` (a group-less flow
has no pipeline to live in — rejected with a message naming both ways out), and `read_mode:
"batch"` on every side. Two things become **rejected on presence** in those modes:
`task_run_id_column` and `read_mode: "streaming"` (see
[§3.2](#32-per-side-dataset-fields-source_config-each-target_configs-entry)). Conversely,
`publish_schema` and `dq_config` are rejected on presence under `"job"`.

**Choosing between the two pipeline modes is not a preference — for many groups it is forced.**
`"pipeline"` streams the reconciliation source (that is what drives the L5 pulse), and a Delta
stream may only read an **append-only** table. If this group's own ingestion or transformation flow
writes that source by full recompute or by MERGE, `"pipeline"` is illegal and
`"pipeline_audit_only"` is the correct setting. Read [§11.7](#117-the-reconciliation-source-must-be-append-only-in-pipeline-mode)
**before** flipping any flow to `"pipeline"`; it is the trap that costs a deploy cycle.

### 11.1 The L0–L5 node map

Every dataset the framework registers now sits in one of six layers. L0 is shared by all three
flow types; L3–L5 are what `execution_mode: "pipeline"` adds.

| Layer | Node (name pattern) | Kind | Published? (v1.6.0) |
|---|---|---|---|
| **L0 · source plane** | `_src__<locator>__<8hex>__stream` | streaming table — one physical read of one external locator, registered only at fan-out ≥ 2 with at least one streaming consumer | **temporary** (pipeline-scoped, invisible in UC) — published qualified only when the spec sets *both* `source_plane.catalog` + `source_plane.schema` |
| **L0 · source plane** | `_src__<locator>__<8hex>__batch` | materialized view — same, when *every* consumer is batch | same rule as above |
| **L0 · source plane** | *(no node)* | `inline` (fan-out 1 — today's read, preserving predicate pushdown) or `in_graph_sibling` (`dlt.read`/`dlt.read_stream` of a table this same group publishes) | — |
| **L1 · ingestion** | `_<target_table>_staged` | `@dlt.view` under `@apply_dq_expectations`; becomes a `@dlt.table(temporary=True)` when it has more than one consumer (quarantine rules, or a `sink`/`external_sink` target) | **never** — view or temporary table, always bare-named |
| **L2 · transformation** | `<input_name>` | `dlt.view` — now a thin **alias** over an L0 binding rather than the read itself | never (a view) |
| **L3 · recon prepare** | `_recon__<rid>__src` | prepared source: `filter_condition` → `data_standardization_sql` → `prepare_dataset_for_matching`. A streaming table when the L5 pulse reads it, else an MV | **temporary**, except a *healing* flow's — the L5 handler reads it back via `spark.read.table`, so it stays a published qualified table |
| **L3 · recon prepare** | `_recon__<rid>__<tid>__tgt` | prepared target, always a batch MV — the far side is scanned **once** per update | **temporary**, except a *healing target's* (same `spark.read.table` reason) |
| **L4 · recon compare** | `_recon__<rid>__<tid>__classified` | MV — the full-outer join on `__framework_hash_key`, the when-chain and the `max_by` per-key collapse, algebra unchanged. (Renamed from published `recon__…__classified` in v1.6.0.) | **temporary** |
| **L4 · recon compare** | `recon__<rid>__<tid>__metrics` | MV, **exactly one row**, carrying the flow's `dq_config` expectations | **published — but registered ONLY when `run_log_capture` resolves `true`** (it is the staging feed for `reconciliation_run_log`/`_result`) |
| **L4 · recon compare** | `recon__<rid>__<tid>__mismatch` | MV — `build_mismatch_rows` over the non-`MATCHED` rows, gated by `comparison_direction` | **published — but registered ONLY when `mismatch_log_capture` resolves `true`** |
| **L4 · recon compare** | `_recon__<rid>__<tid>__missing` | MV — the exact append set, a `left_semi` against the *prepared source* (never the deduped classification, so genuine duplicate source rows are not silently dropped) | **temporary** |
| **L5 · recon heal** | `_recon__<rid>__pulse` | one-column streaming table (`lit(1)`) — the only legal bridge, because a Lakeflow sink accepts streaming queries only | **temporary** |
| **L5 · recon heal** | `recon__<rid>__heal_flow` | `@dlt.append_flow` — `read_stream(pulse)` INNER JOIN a one-row aggregate of `read(__classified)` on a constant column (v1.6.0 — was `__metrics`, now conditional) | — |
| **L5 · recon heal** | `_recon__<rid>__heal_sink` | `dlt.foreach_batch_sink` handler — **not a dataset; never appears in UC** | — |

> **v1.6.0 — the Intermediate Object Rule applied to this map.** "Temporary" above means
> `@dlt.table(temporary=True)` under the node's bare, pipeline-local name: materialized once per
> update (the read-once guarantee is intact) but never published to Unity Catalog. Only the two
> audit datasets — `__metrics`/`__mismatch`, each conditional on its capture flag — and a healing
> flow's `_src`/healing `_tgt` remain published. See [§11.10](#1110-v160--the-intermediate-object-rule-and-the-conditional-audit-datasets)
> for what this means for existing deployments and external consumers.

#### Verified live — the exact graph one update registered

The node map above is not a design sketch. On **2026-08-31**, job `flowx_test_recon_dag_job`
(id `854232399214818`) ran to SUCCESS on target `dev_flowx` — `setup_control_tables` →
`seed_flowx_testing_data` → `onboard_003` → `run_003_pipeline` — and pipeline
`be78d88d-6064-414d-a10c-2aacd900fa86` (`flowx_test_003_autoload_recon_pipeline`) registered
**all** of the following in **one** update, per its own event log:

| Dataset / construct as it appears in the event log | Lakeflow kind | Layer |
|---|---|---|
| `flowx.bronze_excalibur.autoload_bronze` | `STREAMING_TABLE` | L1 · ingestion |
| `_recon__recon_excalibur_autoload_vs_zerobus__src` | `STREAMING_TABLE` | L3 · prepared source |
| `_recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__tgt` | `MATERIALIZED_VIEW` | L3 · prepared target |
| `recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__classified` | `MATERIALIZED_VIEW` | L4 |
| `recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__metrics` | `MATERIALIZED_VIEW` | L4 |
| `recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__mismatch` | `MATERIALIZED_VIEW` | L4 |
| `_recon__recon_excalibur_autoload_vs_zerobus__pulse` | `STREAMING_TABLE` | L5 · gate |
| `recon__recon_excalibur_autoload_vs_zerobus__heal_flow` | `APPEND` flow | L5 |
| `_recon__recon_excalibur_autoload_vs_zerobus__heal_sink` | sink, defined as `foreachBatch` | L5 |

That is the full name shape spelled out: `__src` carries no `<target_id>` (one prepared source per
flow, shared by every target), every L4 node carries both `<reconciliation_id>` and `<target_id>`,
and the pulse / heal flow / heal sink are per-flow, not per-target. The internal
`_recon__…__<target_id>__missing` MV of the node map is registered on exactly the same condition as
the heal lane (`comparison_direction` wanting an append) and is not named in the excerpt above,
which lists what the event log reported.

> **`dlt.foreach_batch_sink` is confirmed available on DBR serverless.** This is the single most
> load-bearing thing the live run settled. The design previously treated the construct as
> *unproven* and named `"pipeline_audit_only"` as the fallback for a workspace that lacked it; the
> L5 heal lane is now confirmed real, registered and scheduled by Lakeflow in a genuine update.
>
> **The `hasattr` guard in `engine/sink_registration.py::register_foreach_batch_sink` stays, and is
> still correct.** The locally installed `databricks-dlt` 0.3.0 stub this project develops against
> **still** has no `foreach_batch_sink` attribute, so an unguarded call would surface as a bare
> `AttributeError` deep inside graph resolution instead of the actionable `FrameworkConfigError`
> that names `"pipeline_audit_only"` / `"job"` as the way out. What changed is the *expectation*:
> the guard is no longer expected to trip on DBR. If it does trip on a Databricks runtime, that is
> news — treat it as a runtime/workspace availability report, not as a normal fallback path.

The `dlt.read` on the static side of the heal flow is an **ordering edge, not decoration**: it is
what makes Lakeflow schedule the handler after the whole-snapshot classification has materialized,
so the handler reads *this* update's miss set. The join must be an equi-join on a constant column —
a `lit(True)` join raises `Detected implicit cartesian product` — and `INNER` is safe precisely
because the static side always emits exactly one row, even over an empty relation. **v1.6.0 moved
the edge's anchor**: it was `read(__metrics)` (a one-row MV by construction), but `__metrics` is now
conditional on `run_log_capture`, and a healing flow with logging suppressed must still heal — so
the flow now reads the always-registered `__classified` dataset and derives its own one-row gate
with a `groupBy`-less `.agg()`, which carries the identical exactly-one-row guarantee. (The live
event-log excerpt above predates this and shows the v1.5.0 shape: the `__classified` name gained a
leading underscore and became temporary in v1.6.0, so on a v1.6.0 runtime only `__metrics`/
`__mismatch` — when their flags are on — and a healing flow's `_src`/`_tgt` appear in Unity
Catalog.)

### 11.2 The DAG

```
                          ┌───────────────────────────────────────┐
   external locator  ───► │ L0  _src__…__stream / __batch         │  read ONCE per update
   (table / path)         │     (or inline / in-graph sibling)    │
                          └───────┬───────────────┬───────────────┘
                                  │               │
                ┌─────────────────▼──┐        ┌───▼────────────────────┐
        L1      │ _<target>_staged   │   L2   │ <input_name> view      │
                └─────────┬──────────┘        └───┬────────────────────┘
                          ▼                       ▼
                  bronze target table      silver/gold CDC target
                          │                       │
                          └───────────┬───────────┘
                                      ▼
                     ┌────────────────────────────────┐
              L3     │ _recon__<rid>__src             │
                     │ _recon__<rid>__<tid>__tgt      │
                     └───────────────┬────────────────┘
                                     ▼   FULL OUTER on __framework_hash_key
                     ┌────────────────────────────────┐
              L4     │ recon__…__classified   (pub)   │
                     │ recon__…__metrics (pub, 1 row) │──► dq_config expectations
                     │ recon__…__mismatch     (pub)   │
                     │ _recon__…__missing             │
                     └───────┬────────────────┬───────┘
                             │ ordering edge  │
   _recon__<rid>__pulse ─────┼────────────────┘
      (streaming, lit(1))    ▼
                     ┌────────────────────────────────┐
              L5     │ recon__…__heal_flow            │
                     │   └► _recon__…__heal_sink      │  foreach_batch_sink:
                     └────────────────────────────────┘  fingerprint ledger, append,
                                                         run_log / mismatch / result
```

Everything above the sink is a graph node. **The sink is not**, which is exactly how the
append-back edge into `append_target_table` is broken — and exactly why the *framework*, not
Lakeflow, has to police feedback loops (see
[`13_known_limitations_and_gotchas.md`](13_known_limitations_and_gotchas.md)).

### 11.3 What the handler still does, verbatim

The genuinely imperative half of reconciliation is re-hosted **unchanged** inside one
`dlt.foreach_batch_sink` handler per reconciliation flow: notebook 05's sequential per-target loop,
its `try`/`except`, its `FAILED` rows and its `break`. `compute_batch_fingerprint` /
`is_target_batch_already_processed` against `reconciliation_run_log.source_batch_fingerprint` are
kept **verbatim, and are load-bearing for a second reason in pipeline mode**: a full refresh
re-runs an `@dlt.append_flow` and appends again with no cleanup of prior writes, and the sink's
`batch_id` restarts at 0 while the target is untouched. Both "cheaper" replacements were evaluated
and rejected — Delta `txnAppId`/`txnVersion` keyed on `batch_id` is *sequence*-addressed and would
**silently drop** legitimate new corrections after a full refresh, and a content-addressed
anti-join on `(__framework_hash_key, __framework_hash_value)` provably matches nothing, because the
append set does not carry the hash-value column such a join would need.

### 11.4 `error_handling.on_failure` — same semantics, larger blast radius

`on_failure` is still a Python `try`/`except` in the handler, evaluated per target, and it is
deliberately **not** mapped onto `dlt` expectations: expectations are per-row predicates, there is
no row-level predicate meaning *"this target's reconciliation raised"*, and expectations cannot be
attached to a sink at all.

* **Unchanged:** because all targets stay in **one** handler, `"fail"` still stops the remaining
  targets of that `reconciliation_id` via the existing `break`.
* **Changed — read this before switching a flow to `"pipeline"`:** re-raising now fails that flow
  and therefore the **pipeline update**, whereas in job mode it fails only a job task. Sibling
  reconciliation flows in the same group may already have run by the time it does.
* The finding-level gate people usually expect from this attribute is now available separately and
  additively as `dq_config` on `__metrics` (see [§3.1](#31-flow-level-fields)).

### 11.5 Limitation — healing is source-change-triggered in pipeline mode

This is the one genuine capability narrowing in `execution_mode: "pipeline"`, and it is stated
here rather than buried.

The L5 handler fires **per micro-batch of the pulse**, and the pulse streams the reconciliation
source. **An update in which the source advances no offsets does not invoke the handler, so no
append happens that update.**

What is *not* affected is the comparison answer: `__classified`, `__metrics` and `__mismatch` are
batch MVs recomputed on **every** update regardless of whether the source moved, so target-side
drift or deletion is always **detected** and the `dq_config` expectation always evaluates. This is
a deferred correction, never an undetected breach.

Two first-class remedies, both one line:

* `execution_mode: "job"` — for a flow whose source is static or low-change this is the *correct*
  setting, not a workaround.
* `execution_mode: "pipeline_audit_only"` — in-DAG reporting and expectations, with healing on the
  job's own schedule.

**No heartbeat is fabricated.** Lakeflow offers no construct that emits one row per update into a
streaming table, and inventing one would be a lie about when healing ran.

Two related properties, neither of them new:

* **Convergence takes one update per correction round.** The topological sort orders the ingestion
  read strictly *before* the recon node that produces the corrections — that ordering is precisely
  what makes the recon source same-update fresh — and a sink write is outside the graph entirely.
  So this update's corrections cannot be consumed by this update's read. Read a run-log row as
  *"appended N, expect convergence next update."* The only shape that would close the loop
  in-update is exactly the self-cycle Lakeflow rejects
  ([`13_known_limitations_and_gotchas.md` L1](13_known_limitations_and_gotchas.md#l1)).
* **The per-run log-silencing override changes shape.** The `recon_run_log_capture` /
  `recon_mismatch_log` widget tri-state has no pipeline equivalent — `pipelines start-update`
  accepts only `--full-refresh`. It is replaced by the pipeline configuration keys
  `dataflow.recon.run_log_capture` / `dataflow.recon.mismatch_log`, tri-state preserved (absent or
  `""` defers to `logging_config`); `resolve_log_capture_flags` survives verbatim, only its input
  source moves. **The loss is real:** an operator firefighting a flow that is flooding the log
  tables now needs a pipeline settings edit that takes effect on the *next* update, not the current
  one.

### 11.6 Costs and prerequisites you are signing up for

* **`append_target_table` must pre-exist.** `spark.catalog.tableExists` is unusable in the
  graph-execution context, and creating on demand would race sibling flows. A table created on the
  fly would also get no `CLUSTER BY (__framework_hash_key)` — pre-create it with clustering where
  that matters.
* **Materialization is not free.** Every L3/L4 node is a real physical copy in UC storage plus an
  extra DAG step (and a checkpoint, for the streaming ones). The L0 source plane used to hedge
  against that cost — `materialize: "auto"` gave a shared node only at fan-out ≥ 2, and `"never"`
  existed for a huge, heavily-filtered table whose pushdown was worth more than the saved scan.
  **Since v1.7.3 it does not:** `materialize` defaults to `"always"`, so every external source
  identity is materialized regardless of fan-out, and `"never"` is prohibited. The cost above is
  therefore paid on every source — budget for it when sizing a group with many single-consumer
  sources.
* **Delete the flow's standalone job task when you opt in.** Switching a flow to `"pipeline"` does
  not remove anything from your DABs resources. A `run_<n>_reconciliation` notebook task left in
  place beside a now-pipeline-mode flow makes the comparison run **twice per cycle** — once inside
  the update, once as the job task — and each pass appends its own corrections into
  `append_target_table`. This actually happened: `resources/feature_tests/flowx_test_002_003_job.yml` still
  carried the standalone task after scenario 003 was flipped, and the task was deleted. Nothing
  detects this for you; see [`13_known_limitations_and_gotchas.md` R8](13_known_limitations_and_gotchas.md#r8).
* **One run-as identity, not two.** Today the recon job task and the pipeline can run as different
  principals. In pipeline mode a single identity must simultaneously hold `SELECT` on every
  external far-side table and `MODIFY` on every `append_target_table`.
* **Reconciliation `governance_tags` are deferred to v1.6.0** — the three published recon datasets
  do not carry governance tags in v1.5.0. A recorded decision taken to keep the release finishable,
  not an oversight.
* **`checkpoint_root` is meaningless in pipeline mode** — Lakeflow owns every checkpoint. It stays
  a job-mode notebook widget and is deliberately *not* a rejected spec key, because it has never
  been a spec attribute at all.

### 11.7 The reconciliation source must be append-only in `pipeline` mode

**This is the constraint that decides `"pipeline"` vs `"pipeline_audit_only"` for you.** Read it
before flipping a flow, because before v1.5.0's late fixes the failure landed at pipeline
**runtime**, after a full build → deploy → run cycle.

`execution_mode: "pipeline"` registers `_recon__<rid>__src` as a **streaming table** — the L5 pulse
streams it, and that is what makes healing fire once per source-advancing update
([§11.5](#115-limitation-healing-is-source-change-triggered-in-pipeline-mode)). A Delta stream can
only read a table that is written **append-only**. If the reconciliation source is a dataset **this
same pipeline publishes** and that dataset's producing flow does not write it append-only, the
streaming read is illegal.

| Producer of the recon source, in this same group | Why it is not append-only | `"pipeline"`? |
|---|---|---|
| `cdc_load_strategy: "APPEND"` | plain append | ✅ legal |
| `cdc_load_strategy: "TRUNCATE_AND_LOAD"` | a `@dlt.table` fed by a **full recompute** — every update *replaces* the table's contents | ❌ use `"pipeline_audit_only"` |
| `SCD1` / `SCD2` / `SCD3` | dispatched through `dlt.apply_changes` — real `MERGE`/`UPDATE`/`DELETE` writes | ❌ use `"pipeline_audit_only"` |
| `FULL_SNAPSHOT_CDC` | `dlt.apply_changes_from_snapshot` — same, snapshot-applied | ❌ use `"pipeline_audit_only"` |
| `target_type: "materialized_view"` | fully refreshed on every update | ❌ use `"pipeline_audit_only"` |

Delta's own answer to a non-append-only stream source is `DELTA_SOURCE_TABLE_IGNORE_CHANGES`, and
its own escape hatch — `skipChangeCommits` — is **refused outright by this framework**: it silently
drops every changed row rather than failing, which is strictly worse than failing the update.

**Where the guard lives, and what it says.** `engine/source_plane.py`'s plan-time **G-STREAM** check
(`_NON_APPEND_ONLY_CDC_STRATEGIES`) rejects the request before a single `dlt` call is made, naming
the consumer, the locator, the producing flow, and the setting to use instead:

```
Consumer '<consumer_id>' requested a streaming read of '<catalog.schema.table>', which is produced
in this same pipeline by flow '<flow_id>' with cdc_load_strategy='TRUNCATE_AND_LOAD' /
target_type='streaming_table'. A streaming read of a MERGE-written, snapshot-applied,
fully-recomputed (TRUNCATE_AND_LOAD) or materialized-view target raises Delta's
DELTA_SOURCE_TABLE_IGNORE_CHANGES at execution time. skipChangeCommits is refused as a workaround
because it silently drops changed rows rather than failing loudly -- read '<...>' as a batch
(dlt.read) consumer instead. For a reconciliation flow, that means execution_mode
'pipeline_audit_only' (which reads its source as a batch) rather than 'pipeline'.
```

> **`TRUNCATE_AND_LOAD` was the gap, and it was found the hard way.** The guard originally listed
> only `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`. `TRUNCATE_AND_LOAD` dispatches to no CDC strategy
> at all, so it slipped through: a reconciliation flow whose source was an in-graph
> `TRUNCATE_AND_LOAD` ingestion target passed **every** plan-time check under
> `execution_mode: "pipeline"` and then died at pipeline runtime with
> `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. The concrete case is the geneva tariffs group, whose recon
> source `geneva_admin.stg_tariffelementband` is that group's own `TRUNCATE_AND_LOAD` target —
> which is why `flowx_testing/053_geneva_e41a47ba_recon_in_pipeline.json` declares
> `pipeline_audit_only`, not `pipeline`. The strategy is now in the guard set and the topology is
> pinned by `tests/unit/test_geneva_e41a47ba_topology.py`.

**What you lose by taking `"pipeline_audit_only"` here is only the *lane*, not the answer.** L3 and
L4 still run in-update, so `__classified` / `__metrics` / `__mismatch` are recomputed on every
update and the `dq_config` expectation still evaluates; the corrective append moves back to the
standalone `05_reconciliation_engine.py` job task on its own cadence.

### 11.8 Validator severity — the V-CYC append-loop rules are mode-dependent

The `V-CYC` rules in `onboarding/spec_validator.py` that describe an **append loop** —
`V-CYC-2` (appending into this group's own target), `V-CYC-3` (appending into the raw ingestion
source of a flow in this *same* group), `V-CYC-5` (`append_target_table` equal to the flow's own
`source_config.table`), and the two `target_configs[]` self/cross-append checks — are routed
through one helper, `_append_cycle_finding(...)`, which grades them by `execution_mode`:

| `execution_mode` | Severity | Why |
|---|---|---|
| `"pipeline"` / `"pipeline_audit_only"` | **hard error** — onboarding fails | There *is* a Lakeflow graph, and the append races a read the same update is performing |
| `"job"` | **warning**, logged, spec still onboards | There is no graph. `05_reconciliation_engine.py` runs *after* the pipeline update has finished, so the append cannot race a read that is no longer happening |

**Why this is graded, not absolute.** Firing these unconditionally was a genuine
**backward-compatibility break**, not a stricter reading of an existing rule. The shipped,
pre-v1.5.0, purely job-mode spec `flowx_testing/038_rec_003_precomputed_hash.json` appends into
its own comparison target; under an unconditional rule it stopped validating and therefore could no
longer be onboarded at all, because `02_onboarding_engine.py` raises on any non-empty `errors` list
— with no edit by its author and no opt-in to any v1.5.0 attribute.

Job mode still gets the **warning**, because the underlying hazard is real there too: an append
into a table Lakeflow owns can be clobbered by the next refresh. The warning text says so
explicitly, and states that the same finding *would* be rejected under `"pipeline"` /
`"pipeline_audit_only"` — so an author reading the log knows exactly what changes if they opt in.

> **Not in this group:** `V-CYC-1`, `V-CYC-6` and `V-CYC-7` (source must be a dataset this group
> publishes; `dataflow_group_id` required; producer must be append-only) are unconditional errors
> *in pipeline mode only* and are not evaluated at all under `"job"` — they are placement rules, not
> loop rules. `V-CYC-8` is a pure **ingestion** rule (two flows landing on one raw path while
> disagreeing about `landing_retention_policy` / `source_zip_handling`) and is invoked from
> `validate_spec` directly. It had been called from inside the reconciliation placement validator,
> which early-returns when a spec declares no reconciliation flows — so the rule was **dead** for
> exactly the ordinary Auto Loader spec it exists to protect. It now runs for every spec.

### 11.9 Where the published datasets land — `publish_schema` resolution

`publish_schema` is optional, and its default is *"the hosting pipeline's own schema."* That
sentence hides a resolution chain worth knowing, because a wrong answer here does not produce a
misplaced table — it produces a failed update.

The node namer (`reconciliation/graph_registration.py::_node_name`) needs a concrete, safe schema
for every L3/L4/L5 node. `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` resolves
`PIPELINE_SCHEMA` in this order, first non-empty wins:

1. `spark.conf` `pipelines.schema` — the pipeline's **declared** target schema (current key).
2. `spark.conf` `pipelines.target` — the same thing under its pre-`schema` spelling, still set by
   older pipelines.
3. `spark.catalog.currentDatabase()`.
4. `GROUP_ROW.target_schema` from the dataflow group's control-table row.

If all four miss, a `WARNING` is logged saying that any reconciliation flow without an explicit
`publish_schema` will fail when its datasets are named. `PIPELINE_CATALOG` has always had an
equivalent chain (`currentCatalog()` → `GROUP_ROW.catalog_name` → `CONTROL_CATALOG`).

> **Why steps 1 and 2 exist at all.** `PIPELINE_SCHEMA` originally had *no* fallback chain — it was
> `spark.catalog.currentDatabase()` alone. During **graph definition** the session's current
> database is **not** the pipeline's target schema, so `PIPELINE_SCHEMA` came back `None` even for a
> pipeline that plainly declares `schema: bronze_excalibur`, and the first `_node_name()` call died
> with `ValueError: Unsafe or malformed target_schema: None`. Observed live on 2026-08-31 on
> pipeline `be78d88d`. **Anyone relying on the default is affected** — a flow that sets an explicit
> `publish_schema` never went near this path.

**Practical guidance.** The default puts the published reconciliation datasets — since v1.6.0 that
is `__metrics`/`__mismatch` (when their capture flags are on) plus a healing flow's `_src`/healing
`_tgt`, no longer the whole L3/L4 set — into the same schema your pipeline publishes its business
tables into. That is rarely where reconciliation output belongs. Set `publish_schema` explicitly —
to a dedicated audit schema in the pipeline's own catalog — for anything beyond a test fixture.

### 11.10 v1.6.0 — the Intermediate Object Rule and the conditional audit datasets

v1.6.0 applies one framework-wide design standard — the **Intermediate Object Rule** — to the
reconciliation graph: *an intermediate is a `@dlt.view` when it has a single reader and a
pipeline-scoped `@dlt.table(temporary=True)` when materialization is required; only final sinks are
durable, published tables.* For reconciliation, "final sinks" are the business
`append_target_table`s, the three control tables in `<catalog>.config`, and — because they are the
staging feed the post-pipeline backstop export reads to populate those control tables — the
`__metrics`/`__mismatch` audit datasets.

**What changed, concretely:**

* `_recon__<rid>__src`, `_recon__<rid>__<tid>__tgt`, `_recon__<rid>__<tid>__missing` and the pulse
  are temporary, bare-named datasets. Exception: a **healing** flow keeps `_src` and each healing
  target's `_tgt` published, because the L5 `foreach_batch_sink` handler reads them back with a
  plain `spark.read.table(...)`, which resolves through the metastore — a temporary table is not
  reachable that way.
* The classification node was renamed `recon__…__classified` → `_recon__…__classified` and is
  temporary. If a dashboard or ad-hoc query read the published classification, move it to
  `__mismatch` (per-record detail) or `__metrics` (counts) — that is what they are for.
* `__metrics` is registered **only when `run_log_capture` resolves `true`**; `__mismatch` only when
  `mismatch_log_capture` does. They are not gratuitous intermediates — each exists exactly when the
  control-table row it feeds is wanted, which is why their registration and the corresponding
  control-table writes are gated by the same flag.
* `reconciliation_result` is written only when `run_log_capture` resolves `true` (previously
  unconditional). With both flags `false`, a reconciliation flow persists to **nothing but its
  business targets** — see [§6](#6-runtime-log-controls) for the two rejected contradictions.
* The backstop export (`observability/reconciliation_export.py`, hosted by the observability job
  task) is flag-aware: it probes for `__metrics` when `run_log_capture` is on (else `__mismatch`),
  skips flows whose flags are both off, and gates its `reconciliation_result` writes under
  `run_log_capture`.

**Upgrading an already-deployed pipeline-mode flow**: the first v1.6.0 update renames and
unpublishes the intermediates — Lakeflow treats a renamed dataset as a new one, so the previously
published `recon__…__classified` / non-healing `_recon__…` tables drop out of Unity Catalog and a
streaming `_src`'s checkpoint state resets. Read the migration trap
[`13_known_limitations_and_gotchas.md` O7](13_known_limitations_and_gotchas.md#o7) before deploying
over a production pipeline.

---

## 12. Related Documentation

- [`11_hashing_and_determinism.md`](11_hashing_and_determinism.md) — the canonical `__framework_hash_key`/`__framework_hash_value` construction shared by ingestion, CDC/transformation, and this reconciliation engine, plus the v1.3.0 breaking-change migration checklist referenced in [§9](#9-hash_precomputed-exact-mechanics).
- [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) — how `__framework_hash_key`/`__framework_hash_value` are computed at CDC-materialization time on the target side of a comparison (what makes `hash_precomputed: true` possible at all).
- [`00_master_reference_index.md`](00_master_reference_index.md) §8 — the full reconciliation flow field reference.
- [`13_known_limitations_and_gotchas.md`](13_known_limitations_and_gotchas.md) — the Lakeflow platform rules the in-pipeline design is built on, including why a `@dlt.view` is not a read-once construct and why `pipelines.incompatibleViewCheck.enabled=false` is rejected.
- [`01_platform_architecture.md`](01_platform_architecture.md) §7 — the read-once source plane shared by all three flow types.
