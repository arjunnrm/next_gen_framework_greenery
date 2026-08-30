# CDC / Load Strategies Reference

See also: [README.md](README.md) for the full Metaflow documentation index.

Set via `target_config.cdc_load_strategy` on an `ingestion_flow_spec` or
`transformation_flow_spec` row -- **there is no separate `cdc_config` block**; every
CDC-related field (`primary_keys`, `sequence_by_column`, `columns_to_check`,
`columns_to_exclude`, `cdc_operation_column`/`cdc_operation_mapping`, the hash/surrogate-key
options) lives directly inside `target_config`, alongside the storage-format/partitioning
fields. See [01_control_metadata_schema.md §4](01_control_metadata_schema.md#4-target_config-ingestion-and-transformation-flows)
for the full `target_config` shape and every non-CDC field in it.

Implemented in `src/NextGen_Metadata_Framework/lakeflow_framework/cdc/`:
`cdc/dispatcher.py::register_cdc_strategy` routes by strategy name to `cdc/scd.py`
(SCD1/SCD2/SCD3) or `cdc/snapshot.py` (the two full-snapshot strategies); `APPEND` and
`TRUNCATE_AND_LOAD` are handled directly by `engine/flow_registration.py` and
`dq/quarantine.py` and never reach the dispatcher at all.

| Strategy | Ingestion? | Transformation? | Dispatch module | Native Lakeflow feature |
|---|---|---|---|---|
| `APPEND` | ✅ | ✅ | *(no-op)* | plain streaming table |
| `TRUNCATE_AND_LOAD` | ✅ | ✅ | *(no-op)* | materialized view (full recompute) |
| `SCD1` | ✅ | ✅ | `cdc/scd.py::register_scd1` | `dlt.apply_changes(stored_as_scd_type="1")` |
| `SCD2` | ✅ | ✅ | `cdc/scd.py::register_scd2` | `dlt.apply_changes(stored_as_scd_type="2")` |
| `SCD3` | ❌ | ✅ | `cdc/scd.py::register_scd3` | **not native** -- pivot over an internal SCD2 history table |
| `FULL_SNAPSHOT_CDC` | ✅ | ✅ | `cdc/snapshot.py::register_full_snapshot_cdc` | `dlt.apply_changes_from_snapshot()` |
| `FULL_SNAPSHOT_CDC_NO_PK` | ✅ | ✅ | same, `cdc_load_strategy == "FULL_SNAPSHOT_CDC_NO_PK"` | `dlt.apply_changes_from_snapshot()` + synthetic SHA-256 key |

`spec_validator.py::_validate_target_config` rejects `SCD3` on an `ingestion_flow` explicitly:
*"'SCD3' is only valid for transformation_flows, not ingestion_flows (SCD3 pivots
current/previous state via an internal history table, which only makes sense downstream of a
raw ingestion flow)"*. Every other strategy is valid on both flow types.

## `target_type` is orthogonal to `cdc_load_strategy`

A flow's `target_type` (`streaming_table`/`materialized_view`/`batch_table`/`external_sink`/
`sink`) and its `cdc_load_strategy` are independent choices -- picking `SCD2` does not require
`target_type: "streaming_table"`. What actually happens for every strategy **except**
`APPEND`/`TRUNCATE_AND_LOAD`:

1. The staged, quarantine-filtered upstream is registered as an *internal* `@dlt.view`
   (`_<target_table>_clean`), not the final table (`dq/quarantine.py::register_main_and_quarantine_tables`,
   gated by its `needs_cdc_dispatch` flag) -- this frees the real `target_table` name for the
   CDC dispatcher to own. Registering both under the same name unconditionally raised `Cannot
   redefine dataset` the first time an SCD flow actually ran end to end; that's the bug this
   split fixes.
2. `cdc/scd.py`/`cdc/snapshot.py` then publish the *actual* target via
   `dlt.create_streaming_table(name=<qualified_target>, ...)` followed by
   `dlt.apply_changes`/`apply_changes_from_snapshot` -- **a genuine Lakeflow Streaming Table,
   unconditionally, regardless of the flow's own `target_type`/`is_streaming` value**, because
   `apply_changes` requires one. `SCD3` is the sole exception: its *public* target is a derived
   batch `@dlt.table` (a window-function pivot over an internal streaming history table, via a
   plain `dlt.read`) -- its hidden `_<target_table>_scd2_history` table is still a genuine
   Streaming Table underneath.

So a `SCD1` flow declared with `target_type: "batch_table"` still ends up backed by a Streaming
Table for its real storage -- `target_type` mainly matters for whether the *staged input* is
read via `dlt.read_stream` vs `dlt.read`, and for `external_sink`/`sink` dispatch (see
[23_lakeflow_sinks.md](23_lakeflow_sinks.md)).

## `APPEND`

Every micro-batch is appended to the target as-is -- no deduplication, no history tracking, no
hash columns (there's no CDC comparison concept for a no-op strategy). Use for immutable
event/log data (raw transactions, telemetry, CDRs) where every record is a distinct fact.

```json
{ "target_type": "streaming_table", "target_config": { "cdc_load_strategy": "APPEND" } }
```

No other `target_config` field is CDC-specific. Example: `test_specs/spec_01_ingest_gcs_autoloader_quarantine.json`,
`spec_08_zerobus_scd_encrypted.json` (the raw ingestion side, before the SCD1/SCD2
transformation steps in the same spec). Reference:
[Load data with Lakeflow Declarative Pipelines](https://docs.databricks.com/en/delta-live-tables/load.html).

## `TRUNCATE_AND_LOAD`

The target is fully recomputed from its source(s) on every pipeline update -- Lakeflow handles
the diff/recompute as a materialized view. Use for small reference/dimension data, daily
snapshots, or any target where "recompute the whole thing" is cheaper than incremental logic.

```json
{ "target_type": "materialized_view", "target_config": { "cdc_load_strategy": "TRUNCATE_AND_LOAD" } }
```

Example: `test_specs/spec_05_transform_truncate_and_load.json`. Reference:
[Materialized views](https://docs.databricks.com/en/delta-live-tables/materialized-views.html).

## `SCD1`

Upserts on primary-key match -- the latest value (by `sequence_by_column`) overwrites the
previous one, no history kept. Use for dimension tables where only the current state matters
(e.g. "current customer address").

| `target_config` field | Required? |
|---|---|
| `primary_keys` | **yes** |
| `sequence_by_column` | no -- falls back to `__framework_ingestion_timestamp_utc` |
| `cdc_operation_column` + `cdc_operation_mapping.delete_values` | no (optional delete marker) |
| `columns_to_exclude` | no (comparison-only, see below) |
| `generate_hash_columns` | no (default `true`) |

```json
{
  "target_config": {
    "cdc_load_strategy": "SCD1",
    "primary_keys": ["customer_id"],
    "sequence_by_column": "updated_at"
  }
}
```

`cdc/scd.py::register_scd1` builds this as `dlt.create_streaming_table(...)` +
`dlt.apply_changes(..., stored_as_scd_type="1")`. `primary_keys` is required at both
onboarding time (`spec_validator.py`) and, defensively, again at pipeline-deployment time --
`cdc/scd.py::_resolve_keys_and_sequence` raises `CdcStrategyError(f"Flow '{flow_id}': SCD1
requires target_config.primary_keys")` if it's empty. Note the validator's
`check_list_of_str(..., required=True)` only rejects a **missing** `primary_keys`, not an
**empty array** (`[]` is a valid list of strings) -- `primary_keys: []` passes onboarding
validation and only fails at deployment time with the `CdcStrategyError` above.

**Deletes:** add `cdc_operation_column`/`cdc_operation_mapping.delete_values` (same shape used
by `FULL_SNAPSHOT_CDC` below) to mark matching source rows as deletes --
`dlt.apply_changes(..., apply_as_deletes=...)` then removes the key from the target instead of
upserting it:

```json
{
  "target_config": {
    "cdc_load_strategy": "SCD1",
    "primary_keys": ["account_id"],
    "sequence_by_column": "event_ts",
    "cdc_operation_column": "op",
    "cdc_operation_mapping": { "delete_values": ["D"] }
  }
}
```

(`test_specs/spec_08_zerobus_scd_encrypted.json`, a Zerobus/CDC-bus feed carrying an explicit
`op` column.) Omit both fields when the feed never carries deletes -- a normal, valid
configuration; `cdc_operation_column`/`cdc_operation_mapping` stay optional regardless of
whether `primary_keys` is set.

**Excluding columns (wide sources):** `columns_to_exclude` is passed straight through to
`dlt.apply_changes(..., except_column_list=...)` for SCD1 -- since SCD1 always overwrites a
key's row wholesale by `sequence_by_column`, there's no per-column diff to skip; the field
changes *what lands in the target table's schema*, not whether an upsert happens. This is the
practical way to onboard a wide source (e.g. 50 columns) without enumerating every column to
keep:

```json
{
  "target_config": {
    "cdc_load_strategy": "SCD1",
    "primary_keys": ["customer_id"],
    "sequence_by_column": "updated_at",
    "columns_to_exclude": ["batch_load_ts", "source_extract_filename", "etl_run_id", "checksum_hash", "ingestion_notes"]
  }
}
```

(`test_specs/spec_14_wide_table_scd1_column_exclusion.json`.) `columns_to_exclude` is
double-duty here: it *also* feeds `apply_changes`'s `except_column_list` for
SCD1/SCD2/SCD3 specifically (not just the comparison-column resolution described in
[§ Comparison vs. storage](#comparison-vs-storage-columns_to_check--columns_to_exclude) below)
-- `cdc/scd.py::_build_except_column_list` reads the same field for this purpose. Reference:
[Change data capture with `APPLY CHANGES`](https://docs.databricks.com/en/delta-live-tables/cdc.html).

## `SCD2`

Every change to a monitored column opens a new history row (`__START_AT`/`__END_AT` tracking
columns, managed natively by Lakeflow); nothing is overwritten. Use for full audit history --
"what was this account's status on any given date."

| `target_config` field | Required? |
|---|---|
| `primary_keys` | **yes** |
| `sequence_by_column` | no -- same fallback as SCD1 |
| `columns_to_check` | no -- empty/absent means compare **every** applicable column |
| `cdc_operation_column` + `cdc_operation_mapping.delete_values` | no |
| `columns_to_exclude` | no |
| `generate_hash_columns` | no (default `true`) |

```json
{
  "target_config": {
    "cdc_load_strategy": "SCD2",
    "primary_keys": ["txn_id"],
    "sequence_by_column": "txn_ts",
    "columns_to_check": ["amount", "account_name"]
  }
}
```

`columns_to_check` (→ `dlt.apply_changes(..., track_history_column_list=...)`) limits history
tracking to just those columns -- a change in any *other* column does not open a new version.
**Never include an encrypted column here**: AES-GCM's random IV means the same plaintext
re-encrypts to different ciphertext on every run, which would look like a spurious change every
single update.

Same optional delete-marking as SCD1 -- a delete closes the currently-open version (`__END_AT`
set) rather than removing the row outright, preserving history. Same optional
`columns_to_exclude`, and it is a **different lever** than `columns_to_check`: `columns_to_check`
controls which columns *open a new history version*; `columns_to_exclude` controls which columns
are *in the table at all* (current or historical, via `except_column_list`). A column can be
excluded from the table entirely, included but untracked for history, or included and tracked --
three independent choices.

A companion reporting **table**, `<target_table>_current`, is registered automatically for
every SCD2 target (`cdc/scd.py::register_scd2_reporting_view`) -- it aliases the native
`__START_AT`/`__END_AT`/current-row columns to `valid_from`/`valid_to`/`is_current` for
consumers who expect those conventional names, without a second copy of the history-*tracking*
logic (the SCD2 table remains the sole source of truth `apply_changes` maintains). Despite the
function's name, this is a real `@dlt.table`, not a `@dlt.view` -- two real bugs were found in
sequence getting here, both via live deployment: qualifying a `@dlt.view`'s own `name=` the
same way a table's is qualified raised `AnalysisException: View with multipart name '...' is
not supported`; switching to an unqualified view fixed that, but once an SCD2 flow actually ran
and something tried to query the reporting view *afterward*, it raised
`TABLE_OR_VIEW_NOT_FOUND` -- a `@dlt.view` is never a durable, queryable catalog object once the
pipeline update that defined it finishes. A real `@dlt.table` has neither problem.

Example: `test_specs/spec_04_transform_stream_join_scd2.json` (stream-stream join + SCD2
target), `spec_07_volume_scd1_scd2_customer_360.json` (3×SCD1 + 2×SCD2 from one Volume
source), `spec_08_zerobus_scd_encrypted.json` (SCD2 with encryption + deletes). Reference:
[CDC -- SCD Type 2 example](https://docs.databricks.com/en/delta-live-tables/cdc.html#example-scd-type-2).

## `SCD3`

Keeps only the *current* and *immediately-previous* value of each tracked column as
`current_<col>`/`previous_<col>` on the same row -- no full history, no separate lookup either.
**Transformation flows only** (see the ingestion-time rejection above).

| `target_config` field | Required? |
|---|---|
| `primary_keys` | **yes** |
| `columns_to_check` | **yes** -- SCD3 has no meaning without knowing which columns to pivot |
| `sequence_by_column` | no -- same fallback as SCD1/SCD2 |
| `columns_to_exclude` | no |

`dlt.apply_changes` only supports `stored_as_scd_type` `"1"`/`"2"` -- there's no native SCD3.
`cdc/scd.py::register_scd3` implements it as a **pivot over an internal SCD2 history table**:

1. A hidden `_<target_table>_scd2_history` streaming table is built with
   `stored_as_scd_type="2"` (same `apply_changes` call as `register_scd2`, minus the reporting
   view).
2. A public `@dlt.table` ranks each key's versions by `__START_AT` descending
   (`ROW_NUMBER() OVER (PARTITION BY <primary_keys> ORDER BY __START_AT DESC)`) and projects
   rank 1 as `current_<col>` and rank 2 as `previous_<col>` for every name in `columns_to_check`,
   left-joined back onto the passthrough (non-tracked) columns from rank 1.

`columns_to_check` is enforced only at pipeline-deployment time here --
`cdc/scd.py::register_scd3` raises `CdcStrategyError(f"Flow '{flow_id}': SCD3 requires
target_config.columns_to_check")` if it's empty, but `spec_validator.py` does not reject a
missing/empty `columns_to_check` for `SCD3` at onboarding time (it only type-checks the field
when present, the same way it does for every other strategy) -- a spec that would fail at
deployment can still pass onboarding validation.

```json
{
  "target_config": {
    "cdc_load_strategy": "SCD3",
    "primary_keys": ["example_id"],
    "sequence_by_column": "updated_at",
    "columns_to_check": ["status_code"]
  }
}
```

(`onboarding_templates/pipeline_onboarding_template.json`'s `ts_template_scd3_example`.)
Result columns: `example_id`, `current_status_code`, `previous_status_code`, plus every other
passthrough column. `columns_to_exclude` applies to the hidden internal SCD2 history table
before the pivot, same as SCD1/SCD2 -- an excluded column never reaches the public target
either. A key that has only ever had one version has `previous_<col>` columns that are all
`NULL` (the rank-2 join simply finds nothing). Reference (for the underlying mechanism):
[`apply_changes`](https://docs.databricks.com/en/delta-live-tables/cdc.html).

## `FULL_SNAPSHOT_CDC`

Each pipeline update supplies one *full* extract of the source; Lakeflow diffs it against the
previous extract by primary key to derive inserts/updates/deletes. Use for sources that only
ever hand you a complete dump (no CDC feed, no incrementing timestamp) but *do* have a natural
business key.

| `target_config` field | Required? |
|---|---|
| `primary_keys` | **yes** |
| `cdc_operation_column` + `cdc_operation_mapping.delete_values` | no |

```json
{
  "target_config": {
    "cdc_load_strategy": "FULL_SNAPSHOT_CDC",
    "primary_keys": ["employee_id"],
    "cdc_operation_column": "op",
    "cdc_operation_mapping": { "delete_values": ["D", "X"] }
  }
}
```

(`onboarding_templates/pipeline_onboarding_template.json`'s `ts_template_full_snapshot_cdc_example`.)
`cdc/snapshot.py::register_full_snapshot_cdc` raises `CdcStrategyError` if `primary_keys` is
empty. `cdc_operation_column`/`cdc_operation_mapping.delete_values` are optional: when the
source extract carries an explicit delete indicator column, rows matching those values are
filtered out of the snapshot *before* diffing (`dlt.apply_changes_from_snapshot`'s `source`
callable filters, then hands the pruned DataFrame + an incrementing `latest_snapshot_version`
to Lakeflow) -- so a hard delete in the source is expressed as "absent from the next snapshot,"
which is what `apply_changes_from_snapshot` expects. `columns_to_check`/`columns_to_exclude`/
`generate_hash_columns` are all still honored for hash-value purposes (hash columns are computed
upstream of this dispatch, in `dq/quarantine.py`, not inside `cdc/snapshot.py` itself) --
`columns_to_exclude` here is comparison-only (it does **not** feed `except_column_list`;
`apply_changes_from_snapshot` has no such parameter -- `columns_to_exclude` support for
`except_column_list` is SCD1/SCD2/SCD3-specific only, as described in
[§ Comparison vs. storage](#comparison-vs-storage-columns_to_check--columns_to_exclude) below). Reference:
[Simplify change data capture with the `APPLY CHANGES FROM SNAPSHOT` API](https://docs.databricks.com/en/delta-live-tables/cdc-snapshot.html).

`sequence_by_column` has **no effect** on either full-snapshot strategy: `cdc/snapshot.py`
never reads it -- `apply_changes_from_snapshot` orders versions via the incrementing integer
its own `_latest_snapshot` callable returns, not a data column, so there's nothing for a
sequencer to do here. `spec_validator.py` type-checks `sequence_by_column` independently of
`cdc_load_strategy`, so setting it on a `FULL_SNAPSHOT_CDC*` flow passes validation and is
silently ignored rather than rejected.

## `FULL_SNAPSHOT_CDC_NO_PK`

Identical to `FULL_SNAPSHOT_CDC`, but for sources with **no natural business key at all**.
`crypto/hashing.py::generate_surrogate_key_hash` computes a deterministic
`sha2(concat_ws('||', <every non-audit column, sorted, null-coalesced>), 256)` across the full
row, and `cdc/snapshot.py` uses that hash column (`__framework_surrogate_key`) -- not a
user-supplied column -- as the diff key. A row that changes *any* field looks like a
delete-of-the-old-row + insert-of-a-new-row, which is the correct (and only possible)
interpretation when there's no stable identity to hang an "update" off of.

```json
{ "target_config": { "cdc_load_strategy": "FULL_SNAPSHOT_CDC_NO_PK" } }
```

No `primary_keys` needed. Example: `test_specs/spec_03_ingest_no_pk_snapshot_cdc.json` -- see
`sample_data/sample_mainframe_customer_master_day1.csv` vs. `_day2.csv` for a worked
insert/update/delete scenario (one row's status changes, one row disappears, one row is new).

**`generate_surrogate_key` defaults to `true` for this strategy only** (`dq/quarantine.py::
_apply_hash_and_surrogate_key_columns`: `target_config.get("generate_surrogate_key",
cdc_load_strategy == "FULL_SNAPSHOT_CDC_NO_PK")`) -- you don't need to set it explicitly.
**Setting it to `false` on a `FULL_SNAPSHOT_CDC_NO_PK` flow breaks the strategy**:
`cdc/snapshot.py::register_full_snapshot_cdc` unconditionally sets
`keys = ["__framework_surrogate_key"]` for this `cdc_load_strategy` regardless of what
`generate_surrogate_key` resolves to, but the column itself is only actually added to the
DataFrame when `generate_surrogate_key` is truthy -- an explicit `false` leaves
`apply_changes_from_snapshot` pointed at a column that was never added, and `spec_validator.py`
does not cross-check `generate_surrogate_key` against `cdc_load_strategy` (it only type-checks
the boolean in isolation). Leave the field unset (or explicitly `true`) for this strategy.
Reference: same [`apply_changes_from_snapshot`](https://docs.databricks.com/en/delta-live-tables/cdc-snapshot.html)
page -- the surrogate-hash-as-key technique itself is this framework's own addition, not a
documented Databricks pattern.

---

## The hash-key/hash-value framework

**Every CDC-dispatched flow (SCD1/SCD2/SCD3/`FULL_SNAPSHOT_CDC`/`FULL_SNAPSHOT_CDC_NO_PK`) gets
two SHA-256 columns added to its target table**, gated by `target_config.generate_hash_columns`
(default `true` -- `APPEND`/`TRUNCATE_AND_LOAD` never get them, there's no CDC comparison
concept for a no-op load):

* **`__framework_hash_key`** -- `sha2(concat_ws('||', <coalesced primary_keys>), 256)`.
  Identifies "the same logical row" across datasets without needing every key column
  individually.
* **`__framework_hash_value`** -- `sha2(concat_ws('||', <coalesced comparison columns>), 256)`.
  Identifies "has this row changed" in one column. The comparison-column set is exactly what
  [`cdc/comparison_columns.py::resolve_comparison_columns`](#comparison-vs-storage-columns_to_check--columns_to_exclude)
  resolves from `columns_to_check`/`columns_to_exclude`/`primary_keys` -- the same input SCD2's
  native `track_history_column_list` uses, so both mechanisms agree on "what counts as a
  change" from one shared resolution, not two independently-maintained definitions.

Both are computed **centrally**, once, in `dq/quarantine.py::_apply_hash_and_surrogate_key_columns`
-- upstream of CDC dispatch, on the quarantine-filtered clean view, before
`dlt.apply_changes`/`apply_changes_from_snapshot` ever sees the data. `cdc/scd.py` and
`cdc/snapshot.py` never compute a hash themselves; they just consume whatever columns are
already present on `source_view`. This keeps the hash logic in exactly one place regardless of
which of the five CDC-dispatched strategies a flow uses.

**Why this exists: reconciliation.** [07_reconciliation.md](07_reconciliation.md)'s matcher
does one `full_outer` join on `__framework_hash_key` (instead of an N-column join on
`match_keys`) and compares drift via one `__framework_hash_value` equality (instead of an
N-column comparison) -- a reconciliation flow with a dozen `compare_columns` costs the same at
join time as one with a single column. A `reconciliation_flows[].target_configs[]` entry with
`hash_precomputed: true` (only valid for `type: "table"`) reuses a CDC target's own
`__framework_hash_key`/`__framework_hash_value` directly instead of recomputing them inline via
the exact same `compute_hash_columns` function -- pure cost avoidance, since both paths produce
byte-identical hashes for the same underlying column values. `test_specs/spec_24_new_27_08_test_flagship.json`
is the concrete worked example this framework uses to prove it end to end: two independent SCD1
targets (`customer_scd1`, `customer_scd1_replica`) fed by the same shared ingestion source, both
with `generate_hash_columns: true`, reconciled against each other with `hash_precomputed: true`
on both sides:

```json
{
  "target_config": { "cdc_load_strategy": "SCD1", "primary_keys": ["customer_id"], "generate_hash_columns": true }
}
```

```json
{
  "reconciliation_id": "recon_flagship_customer_scd1_vs_replica",
  "source_config": { "type": "table", "table": "{{catalog}}.silver_flagship.customer_scd1", "read_mode": "batch", "hash_precomputed": true },
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
  "compare_columns": ["customer_name", "country", "tier"]
}
```

**Column ordering: keeping keys/clustering/hash columns inside Delta's default stats window.**
Delta automatically collects min/max/null-count data-skipping statistics for only the
*first* `delta.dataSkippingNumIndexedCols` columns of a table -- **32 by default** -- and
that's a column-**position** limit, not a configurable whitelist of column names. Every
`withColumn` call that adds a framework-generated column (`compute_hash_columns`,
`generate_surrogate_key_hash`) naturally appends it *last* in the schema, which silently
drops it out of stats coverage for any target with more than ~32 business columns ahead of
it -- exactly the columns (primary keys, clustering keys, `__framework_hash_key`/
`__framework_hash_value`) that most need that coverage for fast point lookups and
reconciliation joins at scale. `storage/column_ordering.py::reorder_columns_for_delta_stats`
fixes this with one final `DataFrame.select` projection -- applied once, inside
`dq/quarantine.py`'s `_clean_upstream()` (the same single convergence point the hash-column
computation above already uses, so it covers every CDC-dispatched and non-CDC target
alike) -- moving, in priority order, `target_config.primary_keys`, then
`target_config.liquid_clustering_columns`, then `__framework_surrogate_key`, then
`__framework_hash_key`/`__framework_hash_value` to the front of the schema; every other
column keeps its existing relative order after that. `partition_columns` is deliberately
excluded from this reorder -- partition pruning is directory-based, not stats-based, so a
partition column has no 32-column concern to solve. This is a pure schema-level projection
(no shuffle, no value recomputation), and is a true no-op (returns the same DataFrame
object, no `select` at all) when a flow has none of these columns configured or generated.

**Where the [liquid clustering](https://docs.databricks.com/en/delta/clustering.html) actually lands.** `reconciliation/appender.py::append_missing_records`
applies `DataFrameWriter.clusterBy("__framework_hash_key")` the first time it creates a
reconciliation flow's `append_target_table` (never on a table that already exists -- Delta
clustering is fixed at creation) -- this is what keeps the hash-based join fast at scale on the
*reconciliation* side. A CDC target table's own clustering is controlled independently, by that
flow's own (unrelated, general-purpose) `target_config.liquid_clustering_columns`; nothing in
`cdc/scd.py`/`cdc/snapshot.py` clusters a freshly-created SCD/snapshot target on
`__framework_hash_key` automatically.

**No key, no hash columns.** If `generate_hash_columns` resolves truthy but a flow has neither
`primary_keys` nor a generated surrogate key (see below), there's nothing to hash into
`__framework_hash_key` -- `_apply_hash_and_surrogate_key_columns` logs a warning and skips both
hash columns entirely for that flow, rather than emitting a meaningless constant hash.

## `generate_surrogate_key`: a general-purpose option

`crypto/hashing.py::generate_surrogate_key_hash` adds `__framework_surrogate_key` --
`sha2(concat_ws('||', <every payload column, sorted, null-coalesced>), 256)`, excluding a fixed
set of technical/audit columns (`_rescued_data`, `_metadata`, `_asn1_decode_error`, and the
other `__framework_*` columns). Its original, still-default use case is `FULL_SNAPSHOT_CDC_NO_PK`
(a hash of the full payload stands in for a missing natural key -- see that strategy's section
above), but `target_config.generate_surrogate_key` is a general-purpose option across
**any of the five CDC-dispatched strategies** -- SCD1/SCD2/SCD3/`FULL_SNAPSHOT_CDC` can all opt
into a surrogate key too, e.g. as an extra stable row hash for downstream deduplication/joins,
alongside a real `primary_keys`. The equivalent flow-level flag exists on reconciliation flows
too (`crypto/hashing.py`'s same function, gated by `reconciliation_flows[].generate_surrogate_key`
-- see [07_reconciliation.md § generate_surrogate_key](07_reconciliation.md#generate_surrogate_key)).

**It has no effect on `APPEND`/`TRUNCATE_AND_LOAD`.** `spec_validator.py` type-checks
`generate_surrogate_key` as a boolean regardless of `cdc_load_strategy`, so setting it on an
`APPEND`/`TRUNCATE_AND_LOAD` flow passes onboarding validation -- but `dq/quarantine.py`'s
`_apply_hash_and_surrogate_key_columns` (the only call site for an ingestion/transformation
flow) only ever runs when `needs_cdc_dispatch` is `True`, i.e. `cdc_load_strategy` is anything
*other than* `APPEND`/`TRUNCATE_AND_LOAD`. A `TRUNCATE_AND_LOAD` flow with
`generate_surrogate_key: true` onboards cleanly and simply never gets the column.

Defaults: `true` only when `cdc_load_strategy == "FULL_SNAPSHOT_CDC_NO_PK"`; `false` for every
other CDC-dispatched strategy (`dq/quarantine.py::_apply_hash_and_surrogate_key_columns`).
**When a surrogate key is generated and the flow has no `primary_keys` configured, it also
becomes the key hashed into `__framework_hash_key`** -- so a `FULL_SNAPSHOT_CDC_NO_PK` flow
(which never has `primary_keys`) still gets a meaningful `__framework_hash_key`, derived from
`__framework_surrogate_key` rather than an empty key list.

```json
{ "target_config": { "cdc_load_strategy": "SCD1", "primary_keys": ["order_id"], "generate_surrogate_key": true } }
```

## `sequence_by_column`: optional, with one real caveat

`dlt.apply_changes` always needs *some* `sequence_by` value -- there's no concept of "no
sequencer." `cdc/scd.py::_resolve_keys_and_sequence` falls back to
`__framework_ingestion_timestamp_utc` (the framework's own always-present processing timestamp,
`ingestion/technical_metadata.py::FRAMEWORK_INGESTION_TIMESTAMP_COLUMN`) whenever
`target_config.sequence_by_column` is unset, for SCD1/SCD2/SCD3 alike:

```python
sequence_by = target_config.get("sequence_by_column") or FRAMEWORK_INGESTION_TIMESTAMP_COLUMN
```

**The fallback depends on `capture_technical_metadata` staying enabled.**
`__framework_ingestion_timestamp_utc` is itself gated by `capture_technical_metadata` (default
`true` -- `source_config.capture_technical_metadata` for ingestion flows,
`target_config.capture_technical_metadata` for transformation flows, since they have no
`source_config` to host it). A flow that sets `capture_technical_metadata: false` **and** omits
`sequence_by_column` leaves `apply_changes` with a `sequence_by` expression pointing at a column
that was never added -- an unresolved-column failure at pipeline-execution time, not a validator
error, since `spec_validator.py` checks the two settings independently of each other. Set an
explicit `sequence_by_column` on any flow that also disables `capture_technical_metadata`.

## Comparison vs. storage: `columns_to_check` / `columns_to_exclude`

**This design fully decouples "what counts as a change" from "what's stored in the target table."**
`cdc/comparison_columns.py::resolve_comparison_columns` is the single function both SCD2's
native `track_history_column_list` and `__framework_hash_value`'s computation go through:

```python
excluded = set(columns_to_exclude or []) | set(primary_keys or []) | FRAMEWORK_TECHNICAL_COLUMNS
base = list(columns_to_check) if columns_to_check else list(all_columns)
return sorted({c for c in base if c not in excluded})
```

* `columns_to_check` empty/absent -> compare every applicable column on the staged DataFrame
  (resolved at execution time against real `df.columns`, never hardcoded against a design-time
  schema).
* `columns_to_check` populated -> scope comparison to exactly those columns (still filtered by
  `columns_to_exclude`/`primary_keys`, in case of an author mistake naming a key column there).
* `primary_keys` are always excluded from comparison -- a key column defines row identity, not
  "did the row change."
* `FRAMEWORK_TECHNICAL_COLUMNS` (`__framework_ingestion_timestamp_utc`, `__framework_hash_key`,
  `__framework_hash_value`, `__framework_surrogate_key`) are always excluded, unconditionally --
  they change on every run by construction (a timestamp, a hash of the very columns being
  compared) and would make every row look "changed" every single update if included.
* The result is **sorted**, so hash computation is deterministic regardless of source column
  ordering -- two runs (or two independently-reconciled datasets) that logically carry the same
  columns always hash the same way.

**`columns_to_exclude` is comparison-only for the hash/history-tracking purpose above** -- it
never drops a column from the target table by itself. A column excluded here is still stored
on the target; it's just not counted when deciding whether a row changed. **The one place
`columns_to_exclude` *does*
still remove a column from the table is `except_column_list` for SCD1/SCD2/SCD3 specifically**
(`cdc/scd.py::_build_except_column_list` reads the same field for that separate purpose) -- so
for those three strategies, one field name serves two distinct mechanisms at once: "don't store
this column" (via `except_column_list`) and, incidentally, "don't compare this column" (since an
absent column can't meaningfully be compared either). `FULL_SNAPSHOT_CDC`/`FULL_SNAPSHOT_CDC_NO_PK`
have no `except_column_list` equivalent (`apply_changes_from_snapshot` doesn't support one), so
`columns_to_exclude` there is comparison-only in the literal sense, affecting only
`__framework_hash_value`.

`spec_validator.py` restricts `columns_to_exclude` to `cdc_load_strategy in
{"SCD1", "SCD2", "SCD3"}` -- setting it on `FULL_SNAPSHOT_CDC`/`FULL_SNAPSHOT_CDC_NO_PK` (or
`APPEND`/`TRUNCATE_AND_LOAD`) is a validation error naming the allowed set.

## Change-count capture via Change Data Feed

`dlt.apply_changes`/`apply_changes_from_snapshot` don't return a per-update
insert/update/delete breakdown. Instead, every CDC-dispatched target gets
`delta.enableChangeDataFeed = "true"` set on it (`storage/table_properties.py::build_table_properties`,
gated to `cdc_load_strategy in {"SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC", "FULL_SNAPSHOT_CDC_NO_PK"}`),
so Delta's native [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)
can answer the question directly:

```python
SELECT _change_type, COUNT(*) FROM table_changes('<qualified_table>', <start>, <end>) GROUP BY _change_type
```

`cdc/change_metrics.py::capture_scd_change_counts` runs that query and aggregates into
`{"inserted_count", "updated_count", "deleted_count"}`, counting only `update_postimage` rows
for updates (each logical update produces one `update_preimage` + one `update_postimage` row in
the feed; counting only the postimage avoids double-counting one update as two changes).

**This runs post-deployment, not inside the pipeline graph.** A `@dlt.table` closure that
materializes a CDC target only ever builds a *lazy query plan* at graph-definition time --
Lakeflow executes and commits it afterward, so the Delta commit version range needed for
`table_changes(...)` ("the version right before this update" / "right after") isn't knowable
from inside the pipeline's own code. `control_plane/post_deployment.py::capture_all_scd_change_counts`
is called from `notebooks/04_governance/04_apply_governance_and_egress.py`, a downstream job
task that runs *after* the pipeline update finishes (the same task that also applies governance
tags -- see [06_governance_integration.md](06_governance_integration.md)).

**The version-range heuristic, and its one honest limitation.** With no persisted "version
before this update started" ledger (deliberately not added -- a new control table purely to
serve this metric was out of scope), the function instead reads each target's own most recent
commit (`DESCRIBE HISTORY <table> LIMIT 1`) and queries `table_changes` for *that single
commit's version against itself* (`starting_version == ending_version`, which Delta's
inclusive-both-ends range semantics resolve to "exactly this one commit's changes"). For a
*triggered* (non-continuous) pipeline update -- the only mode this framework's jobs use --
`apply_changes`/`apply_changes_from_snapshot` normally produce exactly one commit per update per
target, so in the common case this captures precisely that run's own changes. Two scenarios it
does **not** capture correctly, both a direct consequence of having no persisted
last-captured-version ledger:

* A single update that produces *more than one* commit to the same target (checkpoint recovery,
  an unusually large backlog split across internal micro-batches) -- only the last commit is
  reflected.
* A target that received *no* new commit during a given update (e.g. no new source data since
  the last run) -- the function has no way to tell "nothing changed" from "the last commit's
  changes, already reported last time," and re-reports the same commit's counts again.

Never raises: a CDF/history query failure for one target (e.g. a flow whose target table was
never actually created/committed yet, on a group's very first deployment) is logged as a
`FAILED` structured event and skipped -- a metrics-capture problem must never fail the
governance job it rides alongside. Counts are emitted as a structured JSON log line via
`observability/structured_logger.py::log_flow_event(operation="cdc_change_capture", ...)`,
carrying `inserted_count`/`updated_count`/`deleted_count`, `records_written` (their sum),
`target_table`, `cdc_load_strategy`, and `captured_version` -- see that module's own docs for
where these lines land (driver/cluster logs, queryable via the Jobs UI's per-task Logs tab),
since Lakeflow's event log has no documented API for injecting a custom event of this shape.

---

## `target_config` CDC field reference

| Field | Type | Required for | Default |
|---|---|---|---|
| `cdc_load_strategy` | string | always | none -- required |
| `primary_keys` | array\<string\> | `SCD1`, `SCD2`, `SCD3`, `FULL_SNAPSHOT_CDC` | none -- required for those strategies |
| `sequence_by_column` | string | — | `__framework_ingestion_timestamp_utc` |
| `columns_to_check` | array\<string\> | `SCD3` (hard requirement, deployment-time only) | all applicable columns |
| `columns_to_exclude` | array\<string\> | — (`SCD1`/`SCD2`/`SCD3` only) | none excluded |
| `cdc_operation_column` | string | — (paired with `cdc_operation_mapping`) | none |
| `cdc_operation_mapping.delete_values` | array\<string\> | **yes if** `cdc_operation_column` set | none |
| `generate_hash_columns` | boolean | — | `true` for any CDC-dispatched strategy |
| `generate_surrogate_key` | boolean | — | `true` for `FULL_SNAPSHOT_CDC_NO_PK`, `false` otherwise |

Full non-CDC `target_config` fields (`storage_format`, `partition_columns`,
`liquid_clustering_columns`, `table_properties`, `auto_ttl`, `encrypted_columns`, `sink_config`)
are documented in [01_control_metadata_schema.md §4](01_control_metadata_schema.md#4-target_config-ingestion-and-transformation-flows).

---

## Choosing a strategy

```
Does every micro-batch/extract carry the FULL current state of the source?
├─ No, it's an incremental/append-only feed
│   ├─ Records are immutable facts (events, logs, CDRs)         -> APPEND
│   └─ Records represent entity state that changes over time
│       ├─ Only the current value matters                        -> SCD1
│       ├─ Full history matters                                   -> SCD2
│       └─ Only current + previous value matters (transformation only) -> SCD3
└─ Yes, it's a full extract/dump each time
    ├─ Small enough to just recompute entirely, no per-row diff   -> TRUNCATE_AND_LOAD
    ├─ Has a natural business key                                 -> FULL_SNAPSHOT_CDC
    └─ Has no natural business key                                -> FULL_SNAPSHOT_CDC_NO_PK
```
