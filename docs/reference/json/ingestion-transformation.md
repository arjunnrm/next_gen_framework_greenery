<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# CDC / load strategy

Attributes under `target_config` that only apply to particular CDC load strategies. The Spec Builder shows these on the **Load strategy** step and hides the ones the selected strategy does not use.


!!! info "9 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`target_config.cdc_load_strategy`](#target-configcdc-load-strategy) | string | no | — |
| [`target_config.cdc_operation_column`](#target-configcdc-operation-column) | string | no | — |
| [`target_config.cdc_operation_mapping.delete_values`](#target-configcdc-operation-mappingdelete-values) | array<string> | **yes** | — |
| [`target_config.columns_to_check`](#target-configcolumns-to-check) | array<string> | no | — |
| [`target_config.columns_to_exclude`](#target-configcolumns-to-exclude) | array<string> | no | — |
| [`target_config.empty_target_if_source_empty`](#target-configempty-target-if-source-empty) | boolean | no | — |
| [`target_config.generate_hash_columns`](#target-configgenerate-hash-columns) | boolean | no | — |
| [`target_config.primary_keys`](#target-configprimary-keys) | array<string> | **yes** | — |
| [`target_config.sequence_by_column`](#target-configsequence-by-column) | string | no | — |

## Attributes

### `target_config.cdc_load_strategy` { #target-configcdc-load-strategy }

How rows are merged into the target table.


This single choice decides which other target_config attributes are meaningful; the strategy tabs re-scope the form around it. Two families: APPEND and TRUNCATE_AND_LOAD need no key and skip the CDC dispatcher entirely, while SCD1, SCD2, SCD3 and FULL_SNAPSHOT_CDC all merge on primary_keys. Choose by asking three things about the SOURCE, in order -- does each row have a business key, does the source deliver every live row on every run, and how much history the business actually needs.


**Type** `string` · **Required** no


```json
"target_config": {
  "cdc_load_strategy": "SCD2",
  "primary_keys": ["customer_id"],
  "sequence_by_column": "updated_at",
  "columns_to_check": ["status", "tier"]
}
```


!!! tip "Best practice"

    - APPEND -- immutable events, telemetry, audit logs. No key, no merge, no deduplication: a replayed file appends every row a second time and nothing detects it.
    - TRUNCATE_AND_LOAD -- small reference and lookup tables replaced wholesale each run. Cost scales with table size, not with how much changed.
    - SCD1 -- current-state entity tables. Deletes never propagate unless you set cdc_operation_column together with cdc_operation_mapping.delete_values.
    - SCD2 -- full version history for audit and point-in-time joins. Scope columns_to_check, or one volatile source column versions every row on every run.
    - SCD3 -- current and previous side by side, transformation flows only. It stores complete SCD2 history in a hidden table to expose two states; if you are paying that cost anyway, SCD2 gives you strictly more.
    - FULL_SNAPSHOT_CDC -- full extracts from a source that has a real key, where rows that disappear must disappear downstream. A key absent from the snapshot is DELETED. Never point it at an incremental feed.
    - APPEND and TRUNCATE_AND_LOAD are the only strategies where partitioning, liquid clustering and auto TTL take effect.
    - The other four enable Change Data Feed on the target and add the framework hash columns, which is what makes per-run insert/update/delete counts possible.
    - SCD3 is transformation-only -- it is not available to ingestion flows.
    - Never include encrypted columns in columns_to_check: AES-GCM uses a random IV, so the ciphertext differs every run and every row looks changed.


!!! warning "Known errors and limitations"

    **A TRUNCATE_AND_LOAD target was emptied overnight**  
    *Cause:* The source recomputed to zero rows. A short or missed delivery is indistinguishable from a genuinely empty extract, and empty_target_if_source_empty has had no runtime effect since it was withdrawn on 2026-08-29.  
    *Fix:* Compare the target's row count across updates in a post-update job task, outside the pipeline graph. The in-graph guard cannot work -- preserving the contents means the target reads itself, which Lakeflow rejects as a cycle.

    **Almost every row vanished from a FULL_SNAPSHOT_CDC target**  
    *Cause:* The source is an incremental feed, not a full snapshot. apply_changes_from_snapshot deletes any key absent from the incoming snapshot, and nothing validates that the source is actually complete.  
    *Fix:* Switch to SCD1, which merges without deleting. Reserve FULL_SNAPSHOT_CDC for sources that deliver every live row on every run.

    **A FULL_SNAPSHOT_CDC target never deletes anything, and updated keys appear twice**  
    *Cause:* The upstream is streaming (Auto Loader), so each new extract is appended to the snapshot input rather than replacing it -- after a second delivery the input holds Day-1 union Day-2.  
    *Fix:* Overwrite the landing directory in place so each run sees exactly one snapshot. Multi-snapshot diffing over an append-only landing zone is a known limitation.

    **Rows deleted at source are still present in an SCD1 or SCD2 target**  
    *Cause:* No cdc_operation_column is configured, so apply_changes has no delete signal.  
    *Fix:* Set cdc_operation_column and cdc_operation_mapping.delete_values, or accept that the target only ever grows.

    **The same key resolves to a different version on different runs**  
    *Cause:* sequence_by_column was omitted, so rows sequence by __framework_ingestion_timestamp_utc -- one value per batch. Two versions of a key inside one batch tie, and the winner is not deterministic.  
    *Fix:* Declare a real sequence_by_column from the source.

    **A new history row appears on every run with no real change**  
    *Cause:* columns_to_check includes an encrypted or non-deterministic column.  
    *Fix:* Restrict columns_to_check to stable business columns.

    **partition_columns appears to be ignored**  
    *Cause:* Partitioning is silently skipped for CDC-dispatched strategies.  
    *Fix:* Expected behaviour -- use APPEND/TRUNCATE_AND_LOAD, or accept an unpartitioned CDC target.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.cdc_operation_column` { #target-configcdc-operation-column }

Column carrying the source's insert/update/delete indicator.


Without it, SCD1 and SCD2 have no delete signal at all and their targets only ever grow. Optional regardless of primary_keys -- a source can have a real key and no delete marker.


**Type** `string` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "cdc_operation_column": "op"
  }
}
```


!!! tip "Best practice"

    - Requires cdc_operation_mapping.delete_values alongside it; the validator rejects one without the other.
    - SCD1 removes the row outright. SCD2 closes the current version and keeps the history.
    - On FULL_SNAPSHOT_CDC, flagged rows are filtered out of the snapshot before the diff, which deletes them by absence -- the same end result by a different route.
    - A validation error on SCD3, which has no delete path at all.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html) · [delta change data feed](https://docs.databricks.com/delta/delta-change-data-feed.html)


---

### `target_config.cdc_operation_mapping.delete_values` { #target-configcdc-operation-mappingdelete-values }

Values in cdc_operation_column meaning this row is a delete.


**Type** `array<string>` · **Required** yes · **Section** Load strategy


```json
{
  "target_config": {
    "cdc_operation_mapping": {
      "delete_values": [
        "D"
      ]
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html) · [delta change data feed](https://docs.databricks.com/delta/delta-change-data-feed.html)


---

### `target_config.columns_to_check` { #target-configcolumns-to-check }

Limits which columns count as a change.


It means something different per strategy: SCD2 tracks history only on these columns, SCD3 pivots exactly these into current_/previous_ pairs, and SCD1 uses them only to narrow the change-detection hash -- the merge itself is unaffected, so an unchanged row still costs a write.


**Type** `array<string>` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "columns_to_check": [
      "column_a",
      "column_b"
    ]
  }
}
```


!!! tip "Best practice"

    - Required for SCD3 -- registration fails without it.
    - Leave it empty on SCD2 and every column counts, so one volatile source column (a load timestamp, a batch id) versions every row on every run.
    - Framework technical columns are excluded from comparison automatically. Columns the SOURCE stamps are not -- exclude those yourself.
    - Never include encrypted columns: AES-GCM's random IV makes the ciphertext differ every run.
    - Rejected on APPEND, TRUNCATE_AND_LOAD and FULL_SNAPSHOT_CDC.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.columns_to_exclude` { #target-configcolumns-to-exclude }

Columns that should not participate in change comparison.


Aimed at technical columns a source stamps on every row -- load timestamps, batch ids, checksums -- which would otherwise make every row look changed on every run.


**Type** `array<string>` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "columns_to_exclude": [
      "column_a",
      "column_b"
    ]
  }
}
```


!!! tip "Best practice"

    - SCD1, SCD2 and SCD3 only; a validation error on APPEND, TRUNCATE_AND_LOAD and FULL_SNAPSHOT_CDC.
    - primary_keys and the framework's own technical columns are excluded automatically -- you do not need to list them.
    - Whether the listed columns are also dropped from the target's schema is currently inconsistent between the validator and the SCD registrar. Do not rely on this field to shape the target schema; use it for comparison scoping only.


---

### `target_config.empty_target_if_source_empty` { #target-configempty-target-if-source-empty }

Whether a zero-row TRUNCATE_AND_LOAD source is allowed to blank the target.


WITHDRAWN -- this field currently has no runtime effect. It still validates so existing specs stay valid, but nothing reads it, and a zero-row source blanks the target either way.


**Type** `boolean` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "empty_target_if_source_empty": true
  }
}
```


!!! tip "Best practice"

    - Withdrawn on 2026-08-29. Setting it true or false changes nothing today.
    - The guard cannot run inside the pipeline graph: preserving the contents means the target reads itself, which Lakeflow rejects with a cycle error before any flow runs.
    - Its emptiness test was also unusable in-graph, because an upstream produced by the same update legitimately holds no data yet -- 'empty' and 'not built yet' are indistinguishable there.
    - Enforce the policy outside the graph instead: a post-update task comparing the target's row count across updates.
    - A validation error on any strategy other than TRUNCATE_AND_LOAD.


!!! warning "Known errors and limitations"

    **The target was blanked even though this is set to false**  
    *Cause:* The guard is withdrawn and unwired; the value is not read at runtime.  
    *Fix:* Treat every TRUNCATE_AND_LOAD target as unprotected against an empty source and add a row-count check in a post-update job task.


---

### `target_config.generate_hash_columns` { #target-configgenerate-hash-columns }

Adds __framework_hash_key and __framework_hash_value to a CDC target.


Two SHA-256 columns -- one over the ordered primary keys, one over the resolved comparison columns -- that let reconciliation match rows and detect drift without comparing every column on both sides.


**Type** `boolean` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "generate_hash_columns": true
  }
}
```


!!! tip "Best practice"

    - Defaults to true for every CDC-dispatched strategy. Never added for APPEND or TRUNCATE_AND_LOAD, which have no comparison concept.
    - The value hash follows columns_to_check and columns_to_exclude, so changing either re-hashes every row and the next run reports everything as changed -- once, expected.
    - Reconciliation flows using hash_precomputed: true depend on these columns existing on the target.


---

### `target_config.primary_keys` { #target-configprimary-keys }

The business key apply_changes uses to match an incoming row to an existing one.


Without it, CDC cannot tell an update from an insert.


**Type** `array<string>` · **Required** yes · **Section** Load strategy


```json
"primary_keys": ["customer_id", "region"]
```


!!! tip "Best practice"

    - Use the natural business key, not a surrogate generated downstream.
    - FULL_SNAPSHOT_CDC requires one too, as of v1.4.0 -- the keyless variant was withdrawn along with the surrogate-key engine that stood in for the missing key.
    - On FULL_SNAPSHOT_CDC this is the diff key: a key present in the target but absent from the incoming snapshot is deleted.
    - A key that never reaches the clean upstream -- renamed by column_normalization, projected away by data_standardization_sql -- is only caught once the graph executes.


!!! warning "Known errors and limitations"

    **Rows duplicate instead of updating**  
    *Cause:* The declared key is not actually unique in the source.  
    *Fix:* Add the missing key column, or deduplicate upstream with source_config.remove_dups.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.sequence_by_column` { #target-configsequence-by-column }

Ordering column that decides which version of a key wins.


apply_changes always needs a sequencer. Omit this and the framework sequences by __framework_ingestion_timestamp_utc, which is current_timestamp() evaluated once per batch -- monotonic and correct across runs, but tied within a single one.


**Type** `string` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "sequence_by_column": "__framework_ingestion_timestamp_utc"
  }
}
```


!!! tip "Best practice"

    - Declare a real source timestamp or version column whenever the feed carries one.
    - Ties resolve non-deterministically. If the same key can appear twice in one batch, the fallback is not enough -- and on SCD2 a tie corrupts history order, not just the winner.
    - Ignored entirely by FULL_SNAPSHOT_CDC: apply_changes_from_snapshot has no sequence_by parameter, so a value here passes validation and is then silently discarded.
    - Setting capture_technical_metadata to false removes the fallback column, which makes this field mandatory.


!!! warning "Known errors and limitations"

    **Pipeline fails naming a missing __framework_ingestion_timestamp_utc column**  
    *Cause:* capture_technical_metadata is false and sequence_by_column was left unset, so the fallback sequencer does not exist on the flow.  
    *Fix:* Set sequence_by_column explicitly, or leave capture_technical_metadata at its default.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---
