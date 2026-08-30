<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# CDC / load strategy

Attributes under `target_config` that only apply to particular CDC load strategies. The Spec Builder shows these on the **Load strategy** step and hides the ones the selected strategy does not use.


!!! info "12 attributes"
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
| [`target_config.generate_surrogate_key`](#target-configgenerate-surrogate-key) | boolean | no | — |
| [`target_config.primary_keys`](#target-configprimary-keys) | array<string> | **yes** | — |
| [`target_config.sequence_by_column`](#target-configsequence-by-column) | string | no | — |
| [`target_config.surrogate_key_columns`](#target-configsurrogate-key-columns) | array<string> | no | — |
| [`target_config.surrogate_key_exclude_columns`](#target-configsurrogate-key-exclude-columns) | array<string> | no | — |

## Attributes

### `target_config.cdc_load_strategy` { #target-configcdc-load-strategy }

How rows are merged into the target table.


This single choice decides which other target_config attributes are meaningful; the strategy tabs re-scope the form around it.


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

    - APPEND and TRUNCATE_AND_LOAD are the only strategies where partitioning, liquid clustering and auto TTL take effect.
    - SCD3 is transformation-only — it is not available to ingestion flows.
    - Never include encrypted columns in columns_to_check: AES-GCM uses a random IV, so the ciphertext differs every run and every row looks changed.


!!! warning "Known errors and limitations"

    **A new history row appears on every run with no real change**  
    *Cause:* columns_to_check includes an encrypted or non-deterministic column.  
    *Fix:* Restrict columns_to_check to stable business columns.

    **partition_columns appears to be ignored**  
    *Cause:* Partitioning is silently skipped for CDC-dispatched strategies.  
    *Fix:* Expected behaviour — use APPEND/TRUNCATE_AND_LOAD, or accept an unpartitioned CDC target.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.cdc_operation_column` { #target-configcdc-operation-column }

Column carrying insert/update/delete indicators.


Column carrying insert/update/delete indicators. Optional regardless of primary_keys. Not applicable to FULL_SNAPSHOT_CDC_NO_PK — snapshot diffing derives deletes from the snapshot itself.


**Type** `string` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "cdc_operation_column": "op"
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


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

Limits which columns trigger a new history version (SCD2) or a current/previous pivot (SCD3).


Limits which columns trigger a new history version (SCD2) or a current/previous pivot (SCD3). Never include encrypted columns — AES-GCM's random IV causes spurious changes every run.


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

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.columns_to_exclude` { #target-configcolumns-to-exclude }

Excludes columns from the target schema (except_column_list) and from comparison.


Excludes columns from the target schema (except_column_list) and from comparison. Validation error with APPEND, TRUNCATE_AND_LOAD or FULL_SNAPSHOT_CDC.


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

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


---

### `target_config.empty_target_if_source_empty` { #target-configempty-target-if-source-empty }

Governs a TRUNCATE_AND_LOAD run whose source recomputes to zero rows.


Governs a TRUNCATE_AND_LOAD run whose source recomputes to zero rows. false (the default, and the safe choice) leaves the target untouched; true truncates it to empty, the pre-v1.3.0 behaviour.


**Type** `boolean` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "empty_target_if_source_empty": true
  }
}
```


!!! tip "Best practice"

    - default false — an empty source leaves the target untouched
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `target_config.generate_hash_columns` { #target-configgenerate-hash-columns }

Adds __framework_hash_key (SHA-256 of primary keys) and __framework_hash_value (SHA-256 of comparison columns).


Adds __framework_hash_key (SHA-256 of primary keys) and __framework_hash_value (SHA-256 of comparison columns). Never added for APPEND or TRUNCATE_AND_LOAD.


**Type** `boolean` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "generate_hash_columns": true
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `target_config.generate_surrogate_key` { #target-configgenerate-surrogate-key }

Adds __framework_surrogate_key (SHA-256 of the scoped columns).


Adds __framework_surrogate_key (SHA-256 of the scoped columns). FULL_SNAPSHOT_CDC_NO_PK forces it on regardless of an explicit false, emitting a warning rather than failing.


**Type** `boolean` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "generate_surrogate_key": true
  }
}
```


!!! tip "Best practice"

    - forced on for FULL_SNAPSHOT_CDC_NO_PK
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.


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
    - FULL_SNAPSHOT_CDC_NO_PK deliberately does not take one — it diffs whole rows instead.


!!! warning "Known errors and limitations"

    **Rows duplicate instead of updating**  
    *Cause:* The declared key is not actually unique in the source.  
    *Fix:* Add the missing key column, or deduplicate upstream with source_config.remove_dups.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.sequence_by_column` { #target-configsequence-by-column }

Ordering column for CDC.


Ordering column for CDC. Falls back to the framework ingestion timestamp — but if capture_technical_metadata is false you must set this explicitly or the pipeline fails.


**Type** `string` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "sequence_by_column": "__framework_ingestion_timestamp_utc"
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.surrogate_key_columns` { #target-configsurrogate-key-columns }

Pins the surrogate key's basis to exactly these columns.


Pins the surrogate key's basis to exactly these columns. An explicitly-empty list is respected as empty — it is not widened to every column.


**Type** `array<string>` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "surrogate_key_columns": [
      "column_a",
      "column_b"
    ]
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


---

### `target_config.surrogate_key_exclude_columns` { #target-configsurrogate-key-exclude-columns }

Excludes these columns from the surrogate key's basis.


Excludes these columns from the surrogate key's basis. Applied after surrogate_key_columns.


**Type** `array<string>` · **Required** no · **Section** Load strategy


```json
{
  "target_config": {
    "surrogate_key_exclude_columns": [
      "column_a",
      "column_b"
    ]
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


---
