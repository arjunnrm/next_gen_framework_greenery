# 🔄 Metaflow — Transformation & CDC Engine

> **Audience**: Data engineers and analytics engineers building Silver and Gold layer pipelines, Slowly Changing Dimensions, and multi-table joins.

---

## 1. Transformation Flow Architecture

A transformation flow consumes one or more upstream Delta tables or views, applies Spark SQL transformations, optionally evaluates data quality rules, and merges results into a target Delta table using a declared **CDC load strategy**.

```
┌─────────────────────────────────────────────────────────────┐
│                       UPSTREAM INPUTS                       │
│    table: poc.bronze.orders_raw        (Alias: ord)         │
│    table: poc.silver.customers_dim     (Alias: cust)        │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 TRANSFORMATION SQL QUERY                    │
│ SELECT ord.id, ord.amount, cust.tier, ...                   │
│ FROM ord INNER JOIN cust ON ord.customer_id = cust.id       │
│ WHERE ord.country = '${target_country}'                     │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                   CDC STRATEGY DISPATCHER                   │
│ • SCD1 (In-Place Update)                                    │
│ • SCD2 (Historical Versioning with __start_at / __end_at)   │
│ • SCD3 (Current vs. Previous Column Snapshot)               │
│ • FULL_SNAPSHOT_CDC (Diffing against prior snapshot)        │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 TARGET DELTA LAKE DATASET                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Complete CDC Load Strategies Guide

Metaflow provides 6 built-in load and merge strategies configured via `target_config.cdc_load_strategy`. They fall into two families: `APPEND` and `TRUNCATE_AND_LOAD` need no key and skip the CDC dispatcher entirely; `SCD1`, `SCD2`, `SCD3` and `FULL_SNAPSHOT_CDC` all merge on `primary_keys`.

### 2.0 Choosing one

Three questions, in order. Answer them about the **source**, not the target.

| | Question | Answer | Strategy |
|---|---|---|---|
| **Q1** | Does each row have a business identity you can key on? | No — immutable events, telemetry, audit trails | `APPEND` |
| | | No — but it is a small reference table to replace wholesale | `TRUNCATE_AND_LOAD` |
| | | Yes — a real natural key exists | *go to Q2* |
| **Q2** | Does the source deliver **every** live row, **every** run? | Yes, a full dump — and vanished rows must vanish downstream | `FULL_SNAPSHOT_CDC` |
| | | Yes, a full dump — but small, and nobody needs to know what changed | `TRUNCATE_AND_LOAD` |
| | | No, incremental — only what changed since last time | *go to Q3* |
| **Q3** | How much history does the business need? | None — current state only | `SCD1` |
| | | All of it — audit, point-in-time | `SCD2` |
| | | Current and previous only (transformation flows only) | `SCD3` |

**Use when / avoid when / worst failure mode:**

| Strategy | Use when | Avoid when | Worst failure mode |
|---|---|---|---|
| `APPEND` | Immutable events, telemetry, audit logs, raw Bronze landing | The source can redeliver rows; consumers expect one row per entity | **Silent duplicates** — a replayed file appends everything again and nothing detects it |
| `TRUNCATE_AND_LOAD` | Small reference/lookup dimensions; source has no key at all | The table is large; downstream reads it as a streaming table | **Silent table wipe** — a zero-record extract blanks a populated target (see 2.2) |
| `SCD1` | Current-state entity tables; incremental keyed feeds | History is required; the source is a full dump whose deletes must propagate | **Stale deleted rows** — without `cdc_operation_column`, removed rows live forever |
| `SCD2` | Regulatory audit, point-in-time joins, historical dimensions | Only current state is queried; tracked attributes churn every run | **Version explosion** — an unscoped `columns_to_check` versions every row on every run |
| `SCD3` | The business explicitly wants current vs previous side by side | More than one prior state may be needed; the flow is an ingestion flow | **Slow recompute** — the target rebuilds from the whole history table each run |
| `FULL_SNAPSHOT_CDC` | Full extracts from a keyed source where deletes must propagate | The source is incremental; the landing zone accumulates files | **Silent mass delete** — every key absent from the snapshot is removed |

> [!WARNING]
> **The two silent-data-loss paths.** `TRUNCATE_AND_LOAD` will blank a populated target when its source returns zero rows, and the `empty_target_if_source_empty` guard has had no runtime effect since it was withdrawn on 2026-08-29 (§2.2). `FULL_SNAPSHOT_CDC` pointed at an *incremental* feed deletes every key not present in the current batch — nothing validates that the source is actually complete. Both report the pipeline update as successful.

**What the four merging strategies share.** `primary_keys` is mandatory and validated twice (at onboarding, then again at registration) · `delta.enableChangeDataFeed` is set to `true`, which is what makes the per-run insert/update/delete counts in `capture_all_scd_change_counts` possible · `__framework_hash_key` and `__framework_hash_value` are added by default via `generate_hash_columns` · **`partition_columns` and `liquid_clustering_columns` are inert** — `apply_changes`/`apply_changes_from_snapshot` do not accept them, and setting either is silently ignored rather than rejected (§3).

**Sequencing.** `sequence_by_column` is optional on `SCD1`/`SCD2`/`SCD3`; omitting it falls back to `__framework_ingestion_timestamp_utc`, which is `current_timestamp()` evaluated **once per batch**. That is monotonic and correct across runs, but two versions of the same key *inside one batch* tie, and the winner is non-deterministic — on `SCD2` a tie corrupts history order, not just the surviving value. On `FULL_SNAPSHOT_CDC` the field passes validation and is then never read: `apply_changes_from_snapshot` has no `sequence_by` parameter.

### 2.1 `APPEND`
- **Use Case**: Immutable event logs, telemetry streams, audit trails.
- **Behavior**: Appends incoming rows directly without checking for duplicates.
- **Target Requirement**: `streaming_table` or `batch_table`.

### 2.2 `TRUNCATE_AND_LOAD`
- **Use Case**: Small reference dimensions, currency lookup tables, daily batch overwrites.
- **Behavior**: Atomically replaces the entire target table (`CREATE OR REPLACE TABLE`).
- **Target Requirement**: `materialized_view` or `batch_table`.
- **Empty-Source Guard (`empty_target_if_source_empty`) — WITHDRAWN, currently not enforced**:
  `TRUNCATE_AND_LOAD` registers its target as a `@dlt.table` fed by a full recompute of its clean
  upstream — there is never an explicit "truncate" statement; it is simply what a materialized view
  does when its defining query returns nothing. A missed or short delivery is therefore
  indistinguishable from a genuinely empty snapshot, and a zero-record extract silently wipes a
  populated target.

  v1.3.0 shipped `target_config.empty_target_if_source_empty` to make that an explicit, opt-in
  decision, preserving the target's previously-materialized contents when the source came back
  empty. **That guard was withdrawn on 2026-08-29 and the option currently has no runtime effect.**
  Two independent reasons, both established by live execution:

  1. Preserving the contents means reading the target back and handing it to the materialized view
     as its own defining result — the dataset reads *itself*. Lakeflow rejects that at graph
     construction: `Graph is not topologically sorted. There is a cycle between <target> and
     <target>`. Every `TRUNCATE_AND_LOAD` pipeline failed before a single flow ran (`TC-CDC-002`).
  2. The emptiness test is evaluated during graph *construction*, when an upstream dataset produced
     by the same update legitimately holds no data yet. "Source is empty" and "source has not been
     materialized yet" cannot be told apart there, so the guard also fired on flows whose source was
     never empty.

  The option is still accepted by the onboarding schema (specs remain valid), and
  `cdc/dispatcher.py::resolve_truncate_and_load_source` is retained unwired and annotated. Enforcing
  the policy properly requires a check **outside** the pipeline graph — a post-update task comparing
  the target's row count across updates, in the style of `reconciliation/`. Until then, treat a
  `TRUNCATE_AND_LOAD` target as unprotected against an empty source.

  **Why default to `false`:** blanking a table from inside a pipeline is irreversible, and in practice a zero-record extract is far more often a delivery failure than an intentional "this really is empty now" signal. The safe default is "keep what we have and tell someone"; unconditional truncation is available as an explicit opt-in.

  It is a **validation error** to set `empty_target_if_source_empty` on any `cdc_load_strategy` other than `TRUNCATE_AND_LOAD` — the field is otherwise silently inert, and the validator rejects it rather than let an author believe it is doing something on, say, `SCD2`:
  ```
  <path>.empty_target_if_source_empty: only meaningful for cdc_load_strategy 'TRUNCATE_AND_LOAD', but this flow uses 'SCD2'
  ```

  The guard is illegal against a **streaming** source (`source_df.isStreaming`) because an existence check is an eager action Spark rejects outright on a streaming plan. This should not occur for `TRUNCATE_AND_LOAD` today (its upstream is always read with `dlt.read`, not `dlt.read_stream`), but the guard checks anyway and degrades to the untouched source with a `WARNING`, so a future wiring change fails safe instead of breaking an entire pipeline update from inside a safety mechanism.

  ```json
  { "target_config": { "cdc_load_strategy": "TRUNCATE_AND_LOAD", "empty_target_if_source_empty": false } }
  ```

### 2.3 `SCD1` (Slowly Changing Dimension Type 1)
- **Use Case**: Entity current-state tables where history is not required (e.g. `customer_current`).
- **Behavior**: Matches on `primary_keys`. Updates matching rows in-place; inserts new rows.
- **Underlying Engine**: [Lakeflow `dlt.apply_changes(stored_as_scd_type="1")`](https://docs.databricks.com/en/delta-live-tables/cdc.html).
- **Configuration**:
  ```json
  "target_config": {
    "cdc_load_strategy": "SCD1",
    "primary_keys": ["customer_id"],
    "sequence_by_column": "updated_at",
    "columns_to_check": ["email", "address", "phone_number"],
    "generate_hash_columns": true
  }
  ```

### 2.4 `SCD2` (Slowly Changing Dimension Type 2)
- **Use Case**: Regulatory auditing, historical tracking, point-in-time dimensional analysis.
- **Behavior**: Maintains a full version history of each entity. When attributes change, the previous version's `__end_at` is set, and a new version with `__start_at` and `__end_at = NULL` is inserted.
- **Underlying Engine**: `dlt.apply_changes(stored_as_scd_type="2")`.
- **Target Requirement**: `streaming_table`.
- **Configuration**:
  ```json
  "target_config": {
    "cdc_load_strategy": "SCD2",
    "primary_keys": ["account_id"],
    "sequence_by_column": "txn_timestamp",
    "columns_to_check": ["status", "balance", "credit_limit"]
  }
  ```

### 2.5 `SCD3` (Slowly Changing Dimension Type 3)
- **Use Case**: Tracking current vs. previous attribute state side-by-side in distinct columns (e.g. `current_sales_rep` vs. `previous_sales_rep`).
- **Behavior**: Maintains exactly two states per entity without spawning new rows.
- **Scope**: Allowed **only** in `transformation_flows` (never in `ingestion_flows`).
- **Target Requirement**: `materialized_view`.

### 2.6 `FULL_SNAPSHOT_CDC`
- **Use Case**: Upstream systems (e.g. legacy RDBMS, mainframes) providing daily full table dumps without incremental change feeds.
- **Behavior**: Compares the incoming daily snapshot against the prior snapshot using `primary_keys`. Identifies and emits `INSERT`, `UPDATE`, and `DELETE` operations via `dlt.apply_changes_from_snapshot` — the [Databricks-native snapshot CDC pattern](https://docs.databricks.com/aws/en/ldp/cdc).
- **Target Requirement**: `streaming_table`.
- **`primary_keys` is required**, and is the diff key. As of v1.4.0 there is no framework-generated substitute for it:
  ```json
  {
    "target_config": {
      "cdc_load_strategy": "FULL_SNAPSHOT_CDC",
      "primary_keys": ["account_no", "product_code"]
    }
  }
  ```
  Omitting it raises at registration time:
  ```
  Flow '<flow_id>': FULL_SNAPSHOT_CDC requires target_config.primary_keys -- apply_changes_from_snapshot diffs successive snapshots on a key, and as of v1.4.0 the framework no longer manufactures one. Declare the source's natural key, or switch this flow to TRUNCATE_AND_LOAD if it genuinely has none.
  ```
- **Key-presence guard at graph-execution time.** A `primary_keys` entry that never reaches the clean upstream — renamed by `column_normalization`, projected away by `data_standardization_sql` — is caught inside the snapshot-input dataset, where `source_view` first has a schema, and reported with the available columns and the two usual causes. Without it the failure surfaces as a generic missing-key error from `apply_changes_from_snapshot` that names neither the column nor the spec field it came from.

### 2.7 `FULL_SNAPSHOT_CDC_NO_PK` — **REMOVED in v1.4.0**

> [!WARNING]
> **This strategy no longer exists.** Onboarding rejects it by name:
> ```
> <path>.cdc_load_strategy: removed in v1.4.0 -- it existed only to consume the surrogate-key engine, hashing every payload column of every row on every run to manufacture a diff key. Use FULL_SNAPSHOT_CDC with target_config.primary_keys (the Databricks-native apply_changes_from_snapshot pattern -- https://docs.databricks.com/aws/en/ldp/cdc), or TRUNCATE_AND_LOAD if this source genuinely has no key to diff on.
> ```

**Why it went.** `dlt.apply_changes_from_snapshot` requires `keys`. With no natural key, the
framework manufactured one — `__framework_surrogate_key`, a SHA-256 over *every payload column of
every row*, recomputed on every run and forced on even against an explicit
`generate_surrogate_key: false`. Three costs, none of them small:

1. **Compute.** A full-width hash per row per snapshot, on top of the CDC diff itself.
2. **Fragility.** Row identity depended on the exclusion list staying correct. A single volatile
   column leaking into the basis re-keyed the entire table, and `apply_changes_from_snapshot` then
   read that as a delete-and-reinsert of everything. This is not hypothetical — the framework
   shipped exactly that defect (`__framework_source_file_name` in the basis meant re-delivering a
   byte-identical snapshot under a new filename churned every row) and had to fix it.
3. **It did not model the data.** Two rows identical in every column were one row to the hash. A
   snapshot with genuine duplicates could not be represented at all, and a Day-2 field change was
   reported as a delete plus an insert — the same entity, under two identities — rather than the
   update it was.

**Migration.** Two honest options, and which one applies is a question about the source, not about
the framework:

| Situation | Replacement |
|---|---|
| The source has a natural key, it was just never declared (the common case — a customer id, an account number, a composite of two columns) | `FULL_SNAPSHOT_CDC` with `primary_keys`. Row-level inserts/updates/deletes, and an update is now reported as an update. |
| The source genuinely has no key | `TRUNCATE_AND_LOAD`. It replaces the target wholesale and needs no key to do it — an honest full refresh instead of a synthetic diff. |

Removed alongside it: `target_config.generate_surrogate_key`, `surrogate_key_columns`,
`surrogate_key_exclude_columns` (all rejected by onboarding), the `crypto/hashing.py` module, and
the `__framework_surrogate_key` column itself. A table materialized before the upgrade still
physically carries that column; it is left alone, excluded from comparison-column resolution, and
no longer front-loaded by `storage/column_ordering.py`.

---

## 3. Table Layout: Partitioning & Liquid Clustering

`target_config` carries two independent physical-layout fields, both consumed by the single shared helper `storage/table_properties.py::build_partition_and_cluster_kwargs` — every writer that registers a physical target table (currently `dq/quarantine.py::register_main_and_quarantine_tables`) goes through it, so the rules below cannot drift between call sites.

**Scope.** Both fields only take effect for `cdc_load_strategy` in `{APPEND, TRUNCATE_AND_LOAD}` — these are the two strategies whose target is registered directly as a `@dlt.table` (a streaming table or materialized view) fed by a clean-upstream projection. `SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC` are dispatched through `dlt.apply_changes`/`apply_changes_from_snapshot` instead, which do not accept `partition_cols`/`cluster_by` at all — configuring either field on one of those strategies is simply inert, not an error. If a table on one of those strategies looks unpartitioned/unclustered despite the fields being set, this is why.

### `partition_columns`

- **Field**: `target_config.partition_columns` — an array of strings. There is **no alias**: `partition_by` is not a recognized field name, in this release or any prior one.
- **An explicitly empty array (`[]`) configures the target with NO partitioning, and this is never an error.** It is behaviorally identical to omitting the field entirely — both produce an unpartitioned table — but the two are **diagnostically distinct**: an explicitly empty list logs an `INFO` recording that a human deliberately opted out of partitioning, so an operator reading the pipeline log can tell "nobody ever configured this" apart from "someone considered it and said no." An omitted field or an explicit JSON `null` logs nothing (there is nothing noteworthy about the common case).
  ```
  target_config.partition_columns is an explicitly empty list for '<target>' -- the target table will be created with NO partitioning (this is a deliberate configuration, not a missing one).
  ```
- Internally, an empty list is never passed through to the `@dlt.table` decorator as `partition_cols=[]` — a zero-column `partitionBy()` has no contracted Delta/Lakeflow behavior, so "explicitly empty" and "omitted" both translate to *omitting the kwarg entirely*.

```json
{ "target_config": { "cdc_load_strategy": "APPEND", "partition_columns": [] } }
```

### `liquid_clustering_columns`

- **Field**: `target_config.liquid_clustering_columns` — an array of strings, **at most 3 columns** (Delta Liquid Clustering's own limit). An explicitly empty array is likewise valid and means no clustering, following the same omitted-vs-empty diagnostic rule as `partition_columns` (no dedicated log line is emitted for the empty-clustering case today — only the partitioning case is).
- **More than 3 columns is a hard validation error at onboarding time**, in `onboarding/spec_validator.py::_validate_target_config`, with the identical guard re-enforced at runtime inside `build_partition_and_cluster_kwargs` as defense in depth (so a control-table row edited outside onboarding, or a future writer, cannot silently produce a broken configuration). The two messages are identical except for the path prefix:

  Onboarding-time (`spec_validator.py`, `path_prefix` is the flow's JSON path, e.g. `ingestion_flow[df_orders].target_config`):
  ```
  <path_prefix>.liquid_clustering_columns: at most 3 columns are supported by Delta Liquid Clustering, got 4 (['a', 'b', 'c', 'd']) -- reduce the list to 3 or fewer columns
  ```
  Runtime (`storage/table_properties.py`, raised as `FrameworkConfigError`; no spec path is available at this point, so the prefix is fixed):
  ```
  target_config.liquid_clustering_columns: at most 3 columns are supported by Delta Liquid Clustering, got 4 (['a', 'b', 'c', 'd']) -- reduce the list to 3 or fewer columns
  ```
- **Breaking for any spec configuring more than 3 columns.** No shipped `metaflow_testing/` or `onboarding_templates/` spec does; an already-deployed table clustered on more than 3 columns is unaffected until its flow is re-onboarded.

```json
{ "target_config": { "cdc_load_strategy": "APPEND", "liquid_clustering_columns": ["region", "customer_id", "order_ts"] } }
```

---

## 4. Hash Column Computation

Metaflow injects deterministic **SHA-256** hash columns to drive change detection. These are computed only for CDC-dispatched strategies (`SCD1`/`SCD2`/`SCD3`/`FULL_SNAPSHOT_CDC`) inside `dq/quarantine.py::_apply_hash_columns`, before the clean upstream reaches `dlt.apply_changes`/`apply_changes_from_snapshot`:

| Setting | Generated Column(s) | Description |
|---|---|---|
| `target_config.generate_hash_columns` (boolean, **default `true`**) | `__framework_hash_key`, `__framework_hash_value` | On by default for every CDC-dispatched strategy. `__framework_hash_key` is the SHA-256 hash of `primary_keys`, in the order given, delimited by `\|\|`. `__framework_hash_value` is the SHA-256 hash of the resolved comparison columns (alphabetically sorted; see `columns_to_check`/`columns_to_exclude`). With no `primary_keys` configured, both columns are skipped with a `WARNING` — unreachable through onboarding, which requires `primary_keys` for every CDC-dispatched strategy, so it only fires for a hand-edited control-table row. |

> **v1.4.0:** there is no third hash column. `__framework_surrogate_key` and the surrogate-key engine that produced it are removed — see §2.7. The function that computes the two survivors was `_apply_hash_and_surrogate_key_columns`; it is now `_apply_hash_columns`.

> **Note on field naming:** `compute_hash_key`/`compute_hash_value` — as seen in some existing examples elsewhere in this repository's docs and templates — are **not** real `target_config` fields; there is no such toggle in `onboarding/spec_validator.py` or the JSON schema. The one real switch controlling both hash columns together is `generate_hash_columns`, shown above.

**Reconciliation consumes these directly.** A reconciliation dataset declaring `hash_precomputed: true` is *asserting* that its table already carries both columns from a run of this code — it is not asking reconciliation to compute anything. See [`docs/07_reconciliation_engine.md`](07_reconciliation_engine.md) §`hash_precomputed` for the exact mechanics and the precondition that makes the trust safe.

As of v1.3.0 both columns — across ingestion, transformation, and reconciliation — are produced by **one canonical implementation**: every participating value is normalized as `trim(lower(cast(col as string)))` before hashing, with a `__NULL__` sentinel and a `sha2(..., 256)` digest. This replaced three independently-drifted hash constructions and fixed real defects (values differing only by case/whitespace hashing differently; a collidable null sentinel; an incomplete technical-column exclusion list).

**This is a breaking change** — see [`docs/11_hashing_and_determinism.md`](11_hashing_and_determinism.md) for the full construction, the column-ordering rules (which of these sort their inputs and which don't), a reproducible Spark SQL snippet for spot-checking a digest by hand, and the required post-upgrade migration checklist. Do not rely on this section for the construction details — it is a summary, not the canonical reference.

---

## 5. Parameter Substitution (`${param}`, `{{catalog}}`, `{{env}}`)

Metaflow supports flexible parameter binding across deployment environments:

### Template Variables (Resolved at Onboarding Time)
- `{{catalog}}`: Replaced with the active Unity Catalog catalog (e.g. `poc` in dev, `enterprise_prod` in prod).
- `{{env}}`: Replaced with the deployment environment (e.g. `dev`, `stage`, `prod`).

### Dynamic Runtime Parameters (`${param}`)
Defined under root `pipeline_parameters` and dynamically substituted into SQL queries and filter expressions:
```json
{
  "pipeline_parameters": {
    "min_order_value": 100.00,
    "region_code": "EMEA"
  },
  "transformation_flows": [
    {
      "transformation_sql": "SELECT * FROM ord WHERE amount >= ${min_order_value} AND region = ${region_code}"
    }
  ]
}
```
- String parameters are automatically wrapped in single quotes (`'EMEA'`) — **never wrap a placeholder in your own quotes** in `transformation_sql` (e.g. do not write `region = '${region_code}'`); the substitution already supplies the quotes, so a hand-added pair produces a doubled-quote literal (`''EMEA''`) and a `ParseException` at runtime.
- Numeric and boolean parameters are rendered as raw literals (`100.00`).
