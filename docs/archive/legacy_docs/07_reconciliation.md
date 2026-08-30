# Reconciliation

> See also: [Documentation index](README.md).

Metaflow's reconciliation engine is implemented in
`src/NextGen_Metadata_Framework/lakeflow_framework/reconciliation/` (`dataset_reader.py`,
`matcher.py`, `appender.py`, `mismatch_logging.py`, `streaming.py`, `metrics.py`) and driven by
the standalone job-task notebook `notebooks/05_reconciliation/05_reconciliation_engine.py`. See
[01_control_metadata_schema.md §8](01_control_metadata_schema.md#8-reconciliation_flows) for the
one-paragraph schema summary this doc expands on.

## Purpose

A reconciliation flow compares one `source_config` dataset against one or more
`target_configs[]` datasets and, for each target independently, classifies every logical
record (by `match_keys`) as matched, missing on one side, or present-but-drifted. Depending on
each target's `comparison_direction`, the flow either **self-heals** by appending
source-side records the target is missing into that target's `append_target_table` (so a
subsequent CDC/Zerobus pipeline run picks the correction up naturally — no manual backfill), or
**audits** target-side records the source no longer has, strictly for visibility — never by
writing to anything.

It runs as a standalone Lakeflow Job task (`05_reconciliation_engine.py`), independent of any
[Lakeflow Declarative Pipeline](https://docs.databricks.com/aws/en/dlt/) graph, on a schedule
(e.g. hourly) rather than continuously —
`reconciliation/matcher.py`'s module docstring documents this as **Decision 11 (future, not
implemented)**: a follow-up could register a reconciliation comparison as a node *inside* the
same pipeline graph that already materializes its source/target whenever they're both flows in
the same `dataflow_group_id`, skipping a redundant table read. This pass ships the
must-work standalone path only; the hook-in point (`prepare_dataset_for_matching`/
`match_reconciliation_target` already operate on plain DataFrames, no notebook-specific state)
is left discoverable for later.

**This is not the only "reconciliation-shaped" thing in the repo.**
[19_bt_group_test_suite.md §UC004](19_bt_group_test_suite.md#uc004-intra-pipeline-reconciliation-drift-audit)
implements a drift *audit* (not self-healing) as a plain `LEFT ANTI JOIN` transformation flow
running **inside** a DLT pipeline graph — deliberately not this package, because this package's
self-healing append is an out-of-band write to a second table, a side effect with no business
living inside a `@dlt.table` function body. Reach for the `reconciliation/` package (this doc)
when you need self-healing append and/or bidirectional audit with per-record mismatch detail;
reach for a plain anti-join transformation flow when "route the gap into an audit table" is
the entire requirement and everything already lives in one pipeline graph.

## Control tables

Three control tables, created by `notebooks/01_setup/01_setup_control_tables.py` in
`{catalog}.config` (DDL: `control_plane/ddl_definitions.py`):

### `reconciliation_flow_spec`

| Column | Type | Description |
|---|---|---|
| `reconciliation_id` | STRING (PK) | Unique identifier for this reconciliation flow. |
| `dataflow_group_id` | STRING | Parent dataflow group, if any — optional; a reconciliation flow need not belong to one. When set, `05_reconciliation_engine.py` resolves that group's `pipeline_parameters_json` for `${param}` substitution in `filter_condition`/`transform_sql`, same mechanism as `transformation_sql`. |
| `source_config_json` | STRING (JSON object) | `{type, table\|path+format, read_mode, filter_condition, data_standardization_sql, hash_precomputed}` for the baseline dataset. |
| `target_configs_json` | STRING (JSON array) | `[{target_id, type, table\|path+format, read_mode, filter_condition, data_standardization_sql, hash_precomputed, comparison_direction, append_target_table}]` — one flow can compare its source against multiple targets. |
| `match_keys_json` | STRING (JSON array) | Column names identifying the same logical record across datasets. |
| `compare_columns_json` | STRING (JSON array) | Column names compared for drift once matched by key; null/empty means key-presence-only matching. |
| `generate_surrogate_key` | BOOLEAN | When true, generate `__framework_surrogate_key` for records lacking a clean natural match key. |
| `transform_sql` | STRING | Optional SQL reshaping the missing/unmatched record set before append (only when source and target schemas differ). |
| `error_handling_json` | STRING (JSON object) | `{on_failure: "fail"\|"warn"}`. |
| `is_active`, `created_at`, `updated_at` | — | Same as every other control table. |

### `reconciliation_run_log`

One row **per target per run** — restartability ledger + metrics history.

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING (PK) | UUID for this run. |
| `reconciliation_id`, `target_id` | STRING | Which flow / which `target_configs[]` entry. |
| `source_batch_fingerprint` | STRING | sha256 of the unmatched-record key set — reruns with an identical fingerprint are idempotent no-ops. **Batch `read_mode` only** (the DDL comment says so explicitly); a streaming target's restart safety comes from its Structured Streaming checkpoint instead, not this column. |
| `source_record_count`, `target_record_count` | BIGINT | Row counts read from each side. |
| `matched_count` | BIGINT | Present and consistent on both sides. |
| `missing_in_target_count` | BIGINT | Present in source, absent/drifted in this target (`source_to_target`). |
| `missing_in_source_count` | BIGINT | Present in this target, absent from source (`target_to_source`, audit-only). |
| `value_drift_count` | BIGINT | Subset of `missing_in_target_count` — matched by key but differing on a compared column. |
| `appended_count` | BIGINT | Rows actually appended this run (`0` if this fingerprint was already processed). |
| `failed_count` | BIGINT | Records that could not be compared/appended due to an error. |
| `status` | STRING | `SUCCESS`, `FAILED`, or `SKIPPED_ALREADY_PROCESSED`. |
| `error_message` | STRING | Populated when `status = FAILED`. |
| `run_at` | TIMESTAMP | UTC. |

### `reconciliation_mismatch_log`

One row **per non-matched record** — the per-record detail behind `reconciliation_run_log`'s
aggregate counts.

| Column | Type | Description |
|---|---|---|
| `mismatch_id` | STRING (PK) | UUID. |
| `run_id` | STRING | Foreign key into `reconciliation_run_log.run_id` — this is how you drill from an aggregate count down to the exact rows. |
| `reconciliation_id`, `target_id` | STRING | Same as above. |
| `match_key_values_json` | STRING | JSON object of `match_keys` column → value identifying the offending record. |
| `mismatch_type` | STRING | One of `MISSING_IN_TARGET`, `MISSING_IN_SOURCE`, `VALUE_DRIFT` (`MATCHED` never appears here). |
| `differing_columns_json` | STRING | JSON array of `{column, source_value, target_value}` — populated **only** for `VALUE_DRIFT`, and only lists the columns that actually differ, not every compared column. |
| `source_hash_value`, `target_hash_value` | STRING | Each side's `__framework_hash_value`, if available. |
| `detected_at` | TIMESTAMP | UTC. |

## Spec shape: `source_config` and `target_configs[]`

Every field below is enforced by `onboarding/spec_validator.py`'s
`_validate_reconciliation_dataset_config`/`_validate_reconciliation_target_configs`/
`_validate_reconciliation_flows`.

**Flow level:**

| Attribute | Type | Required |
|---|---|---|
| `reconciliation_id` | string | **yes** |
| `source_config` | object | **yes** — shape below |
| `target_configs` | array of objects | **yes**, non-empty — shape below |
| `match_keys` | array\<string\> | **yes** |
| `compare_columns` | array\<string\> | no |
| `generate_surrogate_key` | boolean | no |
| `transform_sql` | string | no — EXPLAIN-validated at onboarding, see below |
| `error_handling.on_failure` | string | no (default `"fail"`) — `"fail"` \| `"warn"` |

**`source_config` / each `target_configs[]` entry** (a target adds three more fields on top —
see below):

| Attribute | Type | Required | Notes |
|---|---|---|---|
| `type` | string | no (default `"table"`) | `"table"` \| `"file"` \| `"sink"` — see [Type dispatch](#type-dispatch-table--file--sink) |
| `table` | string | **yes if** `type == "table"` | Fully-qualified `catalog.schema.table` |
| `path`, `format` | string | **yes if** `type` is `"file"`/`"sink"` | Raw location + Spark format |
| `read_mode` | string | no (default `"batch"`) | `"batch"` \| `"streaming"` — see [read_mode](#read_mode-batch-vs-streaming) |
| `filter_condition` | string | no | Parameterized Spark SQL predicate, `${param}`-substituted, applied via `.filter(...)` right after read |
| `data_standardization_sql` | array\<string\> | no | Same restricted single-column-expression grammar as ingestion's `source_config.data_standardization_sql` — see `ingestion/standardization_sql.py` |
| `hash_precomputed` | boolean | no (default `false`) | **Only valid when `type == "table"`** — a file/sink location cannot carry pre-built `__framework_hash_key`/`__framework_hash_value` columns |

**`target_configs[]`-only fields:**

| Attribute | Type | Required |
|---|---|---|
| `target_id` | string | **yes**, unique within the flow |
| `comparison_direction` | string | no (default `"both"`) — `"source_to_target"` \| `"target_to_source"` \| `"both"` |
| `append_target_table` | string | **yes if** `comparison_direction` is `"source_to_target"` or `"both"` |

`filter_condition`/`data_standardization_sql` are applied identically regardless of dataset
`type` or `read_mode`, immediately after the read and before matching ever sees the
DataFrame — this is what keeps `matcher.py` fully type- and read-mode-agnostic: by the time it
receives a DataFrame, it's already filtered and standardized, indistinguishable from any other
dataset shape.

```json
{
  "reconciliation_id": "recon_customer_master_volume_vs_cdc",
  "source_config": {
    "type": "table",
    "table": "{{catalog}}.bronze_recon_ops.raw_customer_master_baseline"
  },
  "target_configs": [
    {
      "target_id": "primary",
      "type": "table",
      "table": "{{catalog}}.bronze_recon_ops.raw_customer_master_cdc",
      "append_target_table": "{{catalog}}.bronze_recon_ops.raw_customer_master_cdc"
    }
  ],
  "match_keys": ["customer_id"],
  "compare_columns": ["status", "region"],
  "error_handling": {"on_failure": "fail"}
}
```

(`test_specs/spec_09_reconciliation_volume_vs_cdc.json`, live-verified — `read_mode` and
`comparison_direction` both omitted, so both default: batch reads, `comparison_direction:
"both"`. Worked through end to end in [Worked example: spec_09](#worked-example-spec_09-end-to-end) below.)

## Type dispatch: `table` / `file` / `sink`

`reconciliation/dataset_reader.py::read_reconciliation_dataset` dispatches on `type`:

* **`"table"`** — `(spark.readStream if streaming else spark.read).table(table)`.
* **`"file"`** and **`"sink"`** — `reader.format(config["format"]).load(config["path"])`. These
  two share one code path: a Lakeflow sink's own prior output is read back exactly the same way
  any other raw file location is — there is no special "read a sink" API, only the format/path
  it was written with.

**Naming collision to watch for.** `type: "sink"` here means "read back what a
`target_type: "sink"`/`"external_sink"` flow previously *wrote*" (see
[the target_config `target_type` reference](01_control_metadata_schema.md#4-target_config-ingestion-and-transformation-flows)
and `engine/sink_registration.py`) — it is unrelated to, and does not construct, a Lakeflow
`dlt.create_sink`. A reconciliation flow never writes to a sink; `type: "sink"` only tells the
reader "treat this like `type: 'file'`, because I know this path was produced by one." For
example, comparing against `ts_flagship_customer_direct_sink`'s Delta output
(`test_specs/spec_24_new_27_08_test_flagship.json`) would use:

```json
{"type": "sink", "path": "/Volumes/{{catalog}}/egress/zips/flagship_direct_sink/{{env}}/", "format": "delta"}
```

## `read_mode`: batch vs. streaming

`read_mode` is set independently per side (`source_config` and each `target_configs[]` entry),
so one flow can freely mix a streaming source against a batch target, a batch source against a
streaming target, or batch against batch.

### Batch (the default) — fingerprint restartability

`spark.read...` reads a point-in-time snapshot. `05_reconciliation_engine.py` step 4 prepares
`source_config` (hash columns computed or trusted) **once**, shared across every target that
isn't itself streaming — `matcher.py` still reads it twice per target regardless (a narrow join
projection, then a `left_semi` to build the full-column miss set), and once more per additional
target sharing the same source, but this at least avoids repeating the read/filter/
standardize/hash step itself per target. Delta's own file skipping on `filter_condition` keeps
each re-read bounded; there's no cheaper alternative available since this framework's Lakeflow
Jobs run on **[serverless compute](https://docs.databricks.com/aws/en/compute/serverless/)**,
which rejects `DataFrame.cache()`/`.persist()` outright
(`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute` —
confirmed empirically against this project's own `dev` profile).

Restartability: `appender.py::compute_batch_fingerprint` hashes the distinct `match_keys` value
combinations of the `source_to_target` miss set (`sha256` of the sorted, comma/pipe-joined key
tuples). Before appending, `is_target_batch_already_processed` checks this exact
`(reconciliation_id, target_id, fingerprint)` combination against `reconciliation_run_log` for
a prior `SUCCESS`; a match short-circuits the run to `status = "SKIPPED_ALREADY_PROCESSED"` and
`appended_count = 0` — a rerun against an unchanged gap never appends a duplicate correction. A
rerun after the gap changes (new drift, or the old gap got fixed and a new one appeared)
produces a different fingerprint and is correctly treated as a new batch.

### Streaming — checkpoint restartability

`reconciliation/streaming.py::run_streaming_target_reconciliation` drives a `foreachBatch`
query on whichever side is configured `read_mode: "streaming"`, with
`trigger(availableNow=True)` — it processes every currently-available record and stops on its
own, matching a bounded job-task's run/finish lifecycle rather than running forever.
Restartability comes from Structured Streaming's own source-offset checkpoint, **not** the
fingerprint mechanism (the run-log DDL's own comment says the fingerprint is "batch `read_mode`
only") — a crashed or cancelled run resumes exactly where its checkpoint left off, the same
guarantee every other streaming source in this framework already gets, without this module
reimplementing it. `checkpoint_location` must be unique per `(reconciliation_id, target_id)`;
`05_reconciliation_engine.py` builds it as
`{checkpoint_root widget}/{reconciliation_id}/{target_id}`.

**Reuse, not reimplementation.** Spark hands `foreachBatch` a plain *static* DataFrame per
micro-batch — already materialized for that offset range — so it's indistinguishable from a
batch-mode read once it reaches this module. Every micro-batch is run through the exact same
`matcher.py`/`appender.py`/`mismatch_logging.py` functions the batch path uses (via
`appender.py::run_target_reconciliation`), just scoped to one micro-batch instead of one
whole-table snapshot. This does mean each micro-batch still pays the batch path's fingerprint
check — harmless, slightly redundant overhead given the checkpoint already guarantees each
micro-batch's data is new, not a correctness concern.

**Two mechanical subtleties worth knowing:**

* **If `source_config` itself is streaming, every target in the flow runs through the streaming
  path** — even a target explicitly configured `read_mode: "batch"` — because there is only one
  shared streaming source to drive micro-batches from; there's no way to read a streaming
  source "once, batch-style" for one target and "streamed" for another.
* **The non-streaming side is re-read fresh on every micro-batch**, never reused from
  `05_reconciliation_engine.py`'s step-4 precomputed `prepared_source_df` — that precomputation
  is only ever consumed by targets that go through the pure-batch path. A target that goes
  through the streaming path (because it itself is streaming, with a batch source) re-reads and
  re-prepares the shared source from scratch inside its own `foreachBatch` handler, once per
  micro-batch, so each comparison runs against that side's *current* state rather than a
  snapshot frozen from whenever the streaming query first started.

**Unsupported: both sides streaming, for one target.** A stream-stream join would need
watermarking and only supports inner/left-outer semantics — incompatible with this framework's
full four-way classification. `run_streaming_target_reconciliation` raises
`FrameworkConfigError` if both `source_config` and a given target are configured streaming; this
is a deliberate scope boundary, not an oversight. Configure the non-streaming side as
`read_mode: "batch"`.

## Hash-first matching, and why

`matcher.py::prepare_dataset_for_matching` ensures a side carries `__framework_hash_key`/
`__framework_hash_value` before matching ever runs — trusted as-is when `hash_precomputed:
true` (only valid for `type: "table"`, and only meaningful when that table was actually
materialized by a CDC-dispatched flow with `generate_hash_columns` enabled — see
[01_control_metadata_schema.md §4a](01_control_metadata_schema.md#4a-hash-keyvalue--a-framework-design-principle)),
computed inline via `cdc/hashing.py::compute_hash_columns` otherwise. Because both paths use
the exact same `sha2(concat_ws('||', <coalesced columns>), 256)` construction, two sides that
independently compute their own (non-precomputed) hashes over the same underlying `match_keys`/
`compare_columns` values land on identical hash bytes — hash-first matching doesn't require
precomputation to work, precomputation is purely a cost optimization that reuses a hash a
CDC-dispatched flow was already computing anyway.

**Why hash-first, not a direct multi-column join.** `match_reconciliation_target` joins on a
single `__framework_hash_key` equality (`full_outer`) and compares drift via a single
`__framework_hash_value` equality, instead of an N-column `match_keys` join and an N-column
`compare_columns` comparison. A reconciliation flow with a dozen `compare_columns` costs the
same at join time as one with a single column. `compare_columns` empty/omitted forces
`hash_equal = True` unconditionally (never real hash equality) — deliberately, since a
`hash_precomputed` target's own `__framework_hash_value` may have been computed from a
*different* comparison-column set (its own upstream CDC flow's), which this reconciliation flow
explicitly asked to ignore by leaving `compare_columns` empty.

**Duplicate-key safety — the one correctness property this can't compromise on.** A target is
frequently an append-only CDC/Zerobus-style table and may legitimately hold more than one
historical row for the same logical key — most commonly, a prior reconciliation run's own
correction sitting *alongside* (not replacing) the stale drifted row it corrects. The
pre-redesign matcher discovered this the hard way, live: classifying *per joined row* let the
same source record land in both the matched set (via its new corrected counterpart) and the
unmatched set (via the stale row still sitting next to it) at once, so the self-healing append
never converged — it re-appended a fresh duplicate "correction" on every subsequent run instead
of stabilizing after one. The fix: after the hash-key join, every group of rows sharing one
`__framework_hash_key` is collapsed to a single representative outcome via `F.max_by`, ordered
by an explicit `MATCHED (3) > VALUE_DRIFT (2) > MISSING_* (1)` priority, in one
`groupBy(...).agg(F.max_by(...))` pass (no window-function sort) — a key counts as matched as
soon as *any* target-side row for it satisfies the match condition, exactly the old semantics,
just correct under duplication. The append set itself is then built as a `left_semi` join
against the **original, un-deduped** `source_df` — deduplication is purely a classification
safeguard; it never drops a genuine duplicate source row from what gets appended.

## Four-way classification and how `comparison_direction` gates action

One `full_outer` join on `__framework_hash_key` classifies every key into exactly one of:

| Classification | Meaning |
|---|---|
| `MATCHED` | Present on both sides, hash values equal (or `compare_columns` empty). |
| `MISSING_IN_TARGET` | Present in source, absent from target. |
| `MISSING_IN_SOURCE` | Present in target, absent from source. |
| `VALUE_DRIFT` | Present on both sides, hash values differ. |

This happens **every time, in one pass, regardless of `comparison_direction`** — computing all
four is effectively free once the join has run. What `comparison_direction` actually gates is
*action*, in `appender.py::run_target_reconciliation`:

* **`source_to_target`** (and `both`): the `MISSING_IN_TARGET ∪ VALUE_DRIFT` set becomes
  `missing_in_target_df`, fingerprinted, and (unless already processed) appended into
  `append_target_table`; those same records are written to `reconciliation_mismatch_log`.
* **`target_to_source`** (and `both`): `MISSING_IN_SOURCE` records are written to
  `reconciliation_mismatch_log` for audit — **never appended, never used to mutate the
  target** (see [target_to_source is audit-only](#target_to_source-is-audit-only--a-hard-design-constraint) below).

So `comparison_direction: "both"` is **not** two join passes — it's this one join, with the
caller choosing which of the four categories to act on. A target configured
`source_to_target`-only still gets `missing_in_source_count` populated in
`reconciliation_run_log` (it's part of the same free aggregation, and it's useful signal even
if you're not acting on it), but **no** `MISSING_IN_SOURCE` rows are written to
`reconciliation_mismatch_log` for it — logging a per-record audit trail for a direction the
flow author explicitly didn't ask to audit would be misleading, not merely extra detail.

`append_target_table` is required by the validator exactly when it would actually be used
(`comparison_direction` includes `source_to_target`) — a `target_to_source`-only target may
omit it entirely, since nothing is ever written there.

## `transform_sql`

Unlike `data_standardization_sql` (the restricted single-column-expression grammar shared with
ingestion), `transform_sql` legitimately needs full `SELECT`/`FROM` — joins, unions, column
renames — to reshape a miss set from the source's column layout into a target's, when the two
schemas differ. It's flow-level (applies identically across every target in the flow) and
executed as arbitrary Spark SQL against a temp view of the miss set, named
**`_reconciliation_unmatched_records`**
(`reconciliation/appender.py::UNMATCHED_RECORDS_VIEW_NAME`) — every `transform_sql` must read
`FROM _reconciliation_unmatched_records`. `${param}` placeholders resolve first (same mechanism
as `filter_condition` — `transformation/parameters.py`), and, like `transformation_sql`,
`transform_sql` is syntax-validated at onboarding time via the same EXPLAIN-based check
(`onboarding/spec_validator.py::_validate_sql_syntax`) — a `ParseException` is a hard error, an
`AnalysisException` (unresolved table/view — expected, since the target doesn't exist yet at
onboarding time) is tolerated.

```json
"transform_sql": "SELECT customer_id, status, region FROM _reconciliation_unmatched_records WHERE region IS NOT NULL"
```

A `None`/empty `transform_sql` is a pure passthrough — the miss set is appended with its
existing columns unchanged, which is the common case whenever source and
`append_target_table` already share a schema (as in `spec_09` above).

## `generate_surrogate_key`

Flow-level flag (same general-purpose mechanism as ingestion/transformation flows —
`crypto/hashing.py::generate_surrogate_key_hash`, gated by `target_config.generate_surrogate_key`
there), for when neither side has a clean natural key to reconcile on. It's safe to leave set
on every call: `__framework_surrogate_key` is only computed once per side (skipped if the
column is already present). When enabled, the flow's own `match_keys` is expected to reference
`__framework_surrogate_key` directly — `prepare_dataset_for_matching` does not silently
substitute it in place of a differently-named configured `match_keys`, mirroring
`dq/quarantine.py`'s identical convention for CDC-dispatched targets.

## Appending and the mismatch log

`appender.py::append_missing_records` writes the (`transform_sql`-reshaped, if configured) miss
set with `mode("append")` and `mergeSchema=true` — an appended row can introduce a column
`append_target_table` didn't originally have (most commonly `__framework_hash_key`, if the
target predates hash-column support) without requiring the original DDL to have anticipated it;
existing rows simply get `NULL` there, standard Delta schema evolution.
**[Liquid Clustering](https://docs.databricks.com/aws/en/delta/clustering) on
`__framework_hash_key` is applied only the first time this function actually creates
`append_target_table`** (`spark.catalog.tableExists(...)` check) and only when that column is
present on the reshaped DataFrame — an already-existing table keeps whatever clustering it
already has, since Delta clustering is fixed at creation and can't be altered by a later write.
`__framework_hash_key` itself is *always* kept on the miss set regardless of whether the source
table already had it (a pure, deterministic function of `match_keys`); `__framework_hash_value`
is dropped unless it was genuinely already a real column on the source table, since it's a
by-product of *this* join's `compare_columns`, not something a freshly-created
`append_target_table` was designed to carry.

`mismatch_logging.py::write_mismatch_log_rows` projects every non-`MATCHED` record into one
`reconciliation_mismatch_log` row, entirely via native Spark column expressions
(`to_json(struct(...))`, `array`/`filter`/`to_json` for the differing-columns array) — **never**
a driver-side `collect()` + per-row `json.dumps` loop. A reconciliation flow with millions of
drifted/missing records still writes via a normal distributed DataFrame write; funneling that
through the driver would be the difference between "fast at any scale" and "an OOM waiting to
happen." For `VALUE_DRIFT`, `differing_columns_json` lists **only the columns that actually
differ** (null-safe: a `NULL` on one side counts as "differs" whenever the other side is
non-null, matching the hash comparison's own coalesce semantics), not every configured
`compare_columns` entry.

### Drilling from an aggregate count down to the offending records

```sql
-- 1. Find the run (aggregate counts)
SELECT run_id, target_id, run_at, status,
       missing_in_target_count, value_drift_count, missing_in_source_count, appended_count
FROM poc.config.reconciliation_run_log
WHERE reconciliation_id = 'recon_customer_master_volume_vs_cdc'
ORDER BY run_at DESC LIMIT 1;

-- 2. Drill into that run's per-record detail via run_id
SELECT mismatch_type, match_key_values_json, differing_columns_json,
       source_hash_value, target_hash_value
FROM poc.config.reconciliation_mismatch_log
WHERE run_id = '<run_id from step 1>'
ORDER BY mismatch_type;
```

`run_id` is generated once in `run_target_reconciliation` and threaded through both the
`reconciliation_run_log` write and the `reconciliation_mismatch_log` write for that
target/run — it's a genuine foreign key, not merely a shared naming convention, which is exactly
what makes step 2 above work.

## `target_to_source` is audit-only — a hard design constraint

`matcher.py` classifies `MISSING_IN_SOURCE` (records present in a target, absent from source)
in the same single pass as every other category — but the module has **no write path at all**
for it. It is the caller's (`appender.py::run_target_reconciliation`'s) explicit responsibility,
and this framework's explicit design constraint, to never feed a `MISSING_IN_SOURCE` record
into anything that deletes or modifies the target: `target_to_source`/`both` only ever writes
those records to `reconciliation_mismatch_log` for a human/downstream consumer to review. There
is no "remediate the target by deleting rows the source no longer has" code path anywhere in
this package, by design — a target dataset is typically a governed CDC/Zerobus bus with its own
`apply_changes` delete semantics (an explicit `cdc_operation_column`/delete marker, or a real
upstream absence propagated through normal CDC), and reconciliation silently deleting rows out
from under that would bypass and corrupt that mechanism rather than support it.

## Design decisions

* **Standalone job task, not an in-DAG node.** See [Purpose](#purpose) and matcher.py's
  "Decision 11" — a deliberate scope boundary for this pass, with the hook-in point documented
  for a future optimization, not an oversight.
* **`ReconciliationMatchResult` is never cached.** Every one of its derived DataFrames
  (`missing_in_target_df`, `mismatch_detail_df`, the counts aggregate) independently re-executes
  the hash-key join/groupBy from `deduped_df`'s lineage, because serverless compute — every job
  in this framework runs on it — rejects `DataFrame.cache()`/`.persist()` outright. Accepted as
  a deliberate, environment-forced trade-off (correctness/portability over avoiding redundant
  shuffles); a future optimization could materialize `deduped_df` to a temporary Delta table
  (a storage-backed alternative serverless compute does allow) if this proves to be a real
  bottleneck at production scale.
* **`run_id` is caller-generated, not internal**, specifically so `reconciliation_mismatch_log`
  can carry it as a real foreign key into the exact `reconciliation_run_log` row that detected
  those mismatches (see the drill-down example above).
* **`write_run_log_entry` builds its `Row` by walking the schema's own field order, not by
  hand-ordering a `Row(**kwargs)` call** — `spark.createDataFrame(rows, schema=...)` zips a
  `Row`'s values against the given schema *positionally*, not by name, so hand-ordering the
  `Row(...)` construction to match the schema (and hoping nobody reorders either one later) is a
  latent bug waiting to happen; building the positional tuple by walking
  `_RUN_LOG_SCHEMA.fields` directly is immune to that by construction.
* **Structured logging.** Every completed (or skipped-as-already-processed) target run emits a
  `"reconciliation_match"` structured JSON log event via
  `observability/structured_logger.py::log_flow_event`, carrying every `ReconciliationMetrics`
  count plus `reconciliation_id`/`target_id`/`run_id` — landing in driver/cluster logs
  (queryable via the Jobs UI's per-task Logs tab), complementing rather than duplicating
  `reconciliation_run_log`. This event is **not** attempted on a raised exception — the function
  genuinely has no way to know, mid-failure, how much of a run actually completed, so a
  best-effort partial event would be misleading; the caller's own `FAILED`
  `reconciliation_run_log` row remains the authoritative failure record.
  `mismatch_logging.py::write_mismatch_log_rows` similarly wraps itself in a
  `"reconciliation_mismatch_log_write"` logged operation (`SUCCESS` with `records_written`, or
  `FAILED` with the error — the exception itself is always re-raised unchanged, never masked).

## Error handling

`error_handling.on_failure` (default `"fail"`) is evaluated **per target** by
`05_reconciliation_engine.py`'s own loop: `"fail"` propagates the first target's
`FrameworkError` and stops the run (remaining targets in this flow are never attempted);
`"warn"` logs it, writes a `FAILED` `reconciliation_run_log` row for that target
(`fingerprint="unknown"` — the failure may have happened before a fingerprint could even be
computed), and continues to the next target. Either way, the attempt to write that `FAILED` row
happens regardless of `on_failure` — only the *propagation* of the exception differs.

## Worked example: spec_09 end-to-end

`test_specs/spec_09_reconciliation_volume_vs_cdc.json` (shown in full above), live-verified via
`tests/integration/test_reconciliation.py`. Source (`raw_customer_master_baseline`) has 6 rows,
`R001`–`R006`; target (`raw_customer_master_cdc`) has 4, `R001`–`R004`, with `R003`'s `status`
drifted to `INACTIVE` against the baseline's `ACTIVE`. `match_keys: ["customer_id"]`,
`compare_columns: ["status", "region"]`, `read_mode`/`comparison_direction` both defaulted
(batch, `"both"`).

First run's `reconciliation_run_log` row:

| Field | Value | Why |
|---|---|---|
| `source_record_count` | 6 | All of `R001`–`R006`. |
| `target_record_count` | 4 | All of `R001`–`R004`. |
| `matched_count` | 3 | `R001`, `R002`, `R004`. |
| `value_drift_count` | 1 | `R003` (status differs). |
| `missing_in_target_count` | 3 | `R003` (drift) + `R005` + `R006` (absent from target). |
| `missing_in_source_count` | 0 | Every target row has a baseline counterpart — nothing to audit for `target_to_source`. |
| `appended_count` | 3 | `R003`, `R005`, `R006` appended into `append_target_table` (the CDC table itself). |
| `status` | `SUCCESS` | |

`reconciliation_mismatch_log` gets 3 rows for this run: two `MISSING_IN_TARGET` (`R005`,
`R006`), one `VALUE_DRIFT` (`R003`, `differing_columns_json` containing exactly the `status`
entry — `region` didn't differ). `R003`'s corrected row is appended *alongside* its stale
`INACTIVE` row (append-only semantics — nothing is deleted), so after this run the CDC table has
**two** `R003` rows (`ACTIVE` and `INACTIVE`) until a downstream CDC-dispatched pipeline
consumes the correction. A second identical run against an unchanged gap logs
`SKIPPED_ALREADY_PROCESSED` and appends nothing further — no duplicate third `R003` correction.

## Worked example: shared ingestion/reconciliation table (flagship spec_24)

`test_specs/spec_24_new_27_08_test_flagship.json` builds two **independently-materialized**
SCD1 tables (`silver_flagship.customer_scd1` and `.customer_scd1_replica`) from the **same**
shared streaming ingestion source (`bronze_flagship.customer_raw`), then reconciles them against
each other — a genuine "did two parallel materializations of the same upstream data actually
agree" check, not a baseline-vs-CDC gap check:

```json
{
  "reconciliation_id": "recon_flagship_customer_scd1_vs_replica",
  "source_config": {
    "type": "table",
    "table": "{{catalog}}.silver_flagship.customer_scd1",
    "read_mode": "batch",
    "hash_precomputed": true
  },
  "target_configs": [
    {
      "target_id": "customer_scd1_replica",
      "type": "table",
      "table": "{{catalog}}.silver_flagship.customer_scd1_replica",
      "read_mode": "batch",
      "hash_precomputed": true,
      "comparison_direction": "both",
      "append_target_table": "{{catalog}}.silver_flagship.customer_scd1_replica"
    }
  ],
  "match_keys": ["customer_id"],
  "compare_columns": ["customer_name", "country", "tier"],
  "generate_surrogate_key": false,
  "error_handling": {"on_failure": "warn"}
}
```

Both sides set `hash_precomputed: true` — both `customer_scd1` and `customer_scd1_replica` are
`SCD1` targets with `generate_hash_columns: true`, so both already carry real
`__framework_hash_key`/`__framework_hash_value` columns from their own CDC materialization; this
flow reuses both without recomputing either. `comparison_direction: "both"` means a genuine
divergence between the two replicas gets **both** self-healed (the replica missing/drifted
records are appended back into it, `source_to_target`) **and** audited in the opposite direction
(`target_to_source` — any `customer_scd1_replica` row with no `customer_scd1` counterpart is
logged, never remediated). `error_handling.on_failure: "warn"` reflects that this is a
consistency check between two already-materialized tables, not a load-bearing correction path —
a failure here shouldn't block the job.

`resources/new_27_08_test_job.yml`'s `run_flagship_reconciliation` task runs this exact flow
after `run_pipeline_update`, hardcoded to the dedicated `test_2026_08_07` catalog (see
[05_deployment_guide.md](05_deployment_guide.md) for why this one pipeline targets its own
catalog instead of the shared `poc` one).

## Illustrative: streaming reconciliation

No `test_specs/*.json` file currently exercises `read_mode: "streaming"` — every live spec in
this repo reconciles with both sides on the batch/fingerprint path. The shape below is
constructed from the real, validated field names above (not a live-tested spec) to illustrate
the mechanics from [read_mode: batch vs. streaming](#read_mode-batch-vs-streaming):

```json
{
  "reconciliation_id": "recon_orders_stream_vs_baseline",
  "source_config": {
    "type": "table",
    "table": "{{catalog}}.bronze_orders.raw_orders_baseline"
  },
  "target_configs": [
    {
      "target_id": "orders_cdc_stream",
      "type": "table",
      "table": "{{catalog}}.silver_orders.orders_scd1",
      "read_mode": "streaming",
      "hash_precomputed": true,
      "comparison_direction": "source_to_target",
      "append_target_table": "{{catalog}}.silver_orders.orders_scd1"
    }
  ],
  "match_keys": ["order_id"],
  "compare_columns": ["status", "amount"],
  "error_handling": {"on_failure": "warn"}
}
```

Run via `05_reconciliation_engine.py` with the `checkpoint_root` widget set (e.g.
`/Volumes/{{catalog}}/landing/_checkpoints/reconciliation/`); the query checkpoints to
`.../recon_orders_stream_vs_baseline/orders_cdc_stream`. Because only this one target is
streaming (`source_config` stays batch), only this target goes through
`streaming.py` — the batch-only `orders_baseline` source is re-read fresh inside every
micro-batch's handler, independent of anything `05_reconciliation_engine.py`'s step 4 would have
precomputed for a pure-batch target.

## Example usage

```bash
# spec_09 (single-target, batch, poc catalog)
databricks bundle run sample_pipelines_job --profile dev

# flagship (shared-table reconciliation, dedicated test_2026_08_07 catalog)
databricks bundle run new_27_08_test_job --profile dev
```

## Relevant tests

`tests/integration/test_reconciliation.py` — against `spec_09`'s live fixture: appended records
present (and not duplicated) in the CDC table, `reconciliation_run_log` metrics matching the
exact counts worked through above, one `reconciliation_mismatch_log` row per missing/drifted
record with populated `differing_columns_json`/hash values, and idempotent no-op
(`SKIPPED_ALREADY_PROCESSED` or a `SUCCESS` with `appended_count` of `0`/`None`) on a second
identical run.
