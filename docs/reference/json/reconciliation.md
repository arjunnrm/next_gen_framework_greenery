<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Reconciliation flows

One entry per `reconciliation_flows[]` element — comparing a baseline against targets.


!!! info "29 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`compare_columns`](#compare-columns) | array<string> | no | — |
| [`error_handling.on_failure`](#error-handlingon-failure) | string (enum) | no | — |
| [`generate_surrogate_key`](#generate-surrogate-key) | boolean | no | — |
| [`logging_config.mismatch_log_capture`](#logging-configmismatch-log-capture) | boolean | no | — |
| [`logging_config.run_log_capture`](#logging-configrun-log-capture) | boolean | no | — |
| [`match_keys`](#match-keys) | array<string> | **yes** | — |
| [`recon_mode`](#recon-mode) | string (enum) | no | — |
| [`reconciliation_id`](#reconciliation-id) | string | **yes** | — |
| [`source_config.data_standardization_sql`](#source-configdata-standardization-sql) | array<string> | no | — |
| [`source_config.filter_condition`](#source-configfilter-condition) | string (SQL) | no | — |
| [`source_config.hash_precomputed`](#source-confighash-precomputed) | boolean | no | — |
| [`source_config.read_mode`](#source-configread-mode) | string (enum) | no | — |
| [`source_config.table`](#source-configtable) | string | **yes** | — |
| [`source_config.task_run_id_column`](#source-configtask-run-id-column) | string | no | — |
| [`source_config.type`](#source-configtype) | string (enum) | no | — |
| [`target_configs`](#target-configs) | array<object> | no | — |
| [`target_configs[].append_target_table`](#target-configsappend-target-table) | string | **yes** | — |
| [`target_configs[].comparison_direction`](#target-configscomparison-direction) | string (enum) | no | — |
| [`target_configs[].data_standardization_sql`](#target-configsdata-standardization-sql) | array<string> | no | — |
| [`target_configs[].filter_condition`](#target-configsfilter-condition) | string (SQL) | no | — |
| [`target_configs[].hash_precomputed`](#target-configshash-precomputed) | boolean | no | — |
| [`target_configs[].read_mode`](#target-configsread-mode) | string (enum) | no | — |
| [`target_configs[].target_catalog`](#target-configstarget-catalog) | string | **yes** | — |
| [`target_configs[].target_id`](#target-configstarget-id) | string | **yes** | — |
| [`target_configs[].target_schema`](#target-configstarget-schema) | string | **yes** | — |
| [`target_configs[].target_table`](#target-configstarget-table) | string | **yes** | — |
| [`target_configs[].task_run_id_column`](#target-configstask-run-id-column) | string | no | — |
| [`transform_sql`](#transform-sql) | string (SQL) | no | — |
| [`two_tier_verification`](#two-tier-verification) | boolean | no | — |

## Attributes

### `compare_columns` { #compare-columns }

Columns compared for drift after key matching.


Columns compared for drift after key matching. Defaults to all columns.


**Type** `array<string>` · **Required** no · **Section** Matching & healing


```json
{
  "compare_columns": [
    "amount",
    "status"
  ]
}
```


!!! tip "Best practice"

    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


---

### `error_handling.on_failure` { #error-handlingon-failure }

fail raises an error, warn logs and continues.


**Type** `string (enum)` · **Required** no · **Section** Reconciliation identity


```json
{
  "error_handling": {
    "on_failure": "fail"
  }
}
```


!!! tip "Best practice"

    - Allowed values: fail, warn.


---

### `generate_surrogate_key` { #generate-surrogate-key }

Generate __framework_surrogate_key for records.


**Type** `boolean` · **Required** no · **Section** Matching & healing


```json
{
  "generate_surrogate_key": true
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `logging_config.mismatch_log_capture` { #logging-configmismatch-log-capture }

Per-flow gate on mismatch-log writes.


Per-flow gate on mismatch-log writes. Overridable at runtime by the recon_mismatch_log job parameter.


**Type** `boolean` · **Required** no · **Section** Reconciliation identity


```json
{
  "logging_config": {
    "mismatch_log_capture": true
  }
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `logging_config.run_log_capture` { #logging-configrun-log-capture }

Per-flow gate on run-log writes.


Per-flow gate on run-log writes. Overridable at runtime by the recon_run_log_capture job parameter.


**Type** `boolean` · **Required** no · **Section** Reconciliation identity


```json
{
  "logging_config": {
    "run_log_capture": true
  }
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `match_keys` { #match-keys }

Columns identifying the same logical record across datasets.


**Type** `array<string>` · **Required** yes · **Section** Matching & healing


```json
{
  "match_keys": [
    "example_id"
  ]
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


---

### `recon_mode` { #recon-mode }

triggered reads task_run_id as a dynamic job parameter and bounds the reconciliation to one pipeline run.


triggered reads task_run_id as a dynamic job parameter and bounds the reconciliation to one pipeline run. continuous runs as a decoupled streaming process.


**Type** `string (enum)` · **Required** no · **Section** Reconciliation identity


```json
{
  "recon_mode": "triggered"
}
```


!!! tip "Best practice"

    - Allowed values: triggered, continuous.


---

### `reconciliation_id` { #reconciliation-id }

Unique ID for this reconciliation flow.


**Type** `string` · **Required** yes · **Section** Reconciliation identity


```json
{
  "reconciliation_id": "recon_template_example"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_config.data_standardization_sql` { #source-configdata-standardization-sql }

Per-column expressions, each ending AS <column>.


Per-column expressions, each ending AS <column>. Restricted grammar: no SELECT/FROM/JOIN/UNION/WHERE/DML/DDL and no semicolons.


**Type** `array<string>` · **Required** no · **Section** Source · nested data & standardization


```json
{
  "source_config": {
    "data_standardization_sql": [
      "trim(region) AS region",
      "upper(country_code) AS country_code"
    ]
  }
}
```


!!! tip "Best practice"

    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `source_config.filter_condition` { #source-configfilter-condition }

Boolean SQL applied after read.


Boolean SQL applied after read. Supports ${param} substitution.


**Type** `string (SQL)` · **Required** no · **Section** Source dataset


```json
{
  "source_config": {
    "filter_condition": "load_date = '${run_date}'"
  }
}
```


!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `source_config.hash_precomputed` { #source-confighash-precomputed }

Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing.


Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing. Only valid for type table.


**Type** `boolean` · **Required** no · **Section** Source dataset


```json
{
  "source_config": {
    "hash_precomputed": true
  }
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `source_config.read_mode` { #source-configread-mode }

At most one side of a given target's comparison may be streaming.


**Type** `string (enum)` · **Required** no · **Section** Source dataset


```json
{
  "source_config": {
    "read_mode": "batch"
  }
}
```


!!! tip "Best practice"

    - Allowed values: batch, streaming.


---

### `source_config.table` { #source-configtable }

Three-part fully-qualified table name.


**Type** `string` · **Required** yes · **Section** Source dataset


```json
{
  "source_config": {
    "table": "{{catalog}}.bronze.table"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_config.task_run_id_column` { #source-configtask-run-id-column }

When recon_mode is triggered and the task_run_id job parameter is set, narrows this side's read to that run's rows.


**Type** `string` · **Required** no · **Section** Source dataset


```json
{
  "source_config": {
    "task_run_id_column": "__framework_pipeline_run_id"
  }
}
```


---

### `source_config.type` { #source-configtype }

Reconciliation is scoped to Delta tables only.


Reconciliation is scoped to Delta tables only. Any other value is rejected outright — path-based file and sink sources are no longer valid here.


**Type** `string (enum)` · **Required** no · **Section** Source dataset


```json
{
  "source_config": {
    "type": "table"
  }
}
```


!!! tip "Best practice"

    - Allowed values: table.


---

### `target_configs` { #target-configs }

Sets target_configs[].


**Type** `array<object>` · **Required** no · **Section** Target dataset


```json
{
  "target_configs": [
    {
      "...": "one object per entry"
    }
  ]
}
```


!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.


---

### `target_configs[].append_target_table` { #target-configsappend-target-table }

Where self-healed records are appended.


Where self-healed records are appended. Required when the direction includes source-to-target.


**Type** `string` · **Required** yes · **Section** Target dataset


```json
{
  "append_target_table": "{{catalog}}.bronze_example.example_raw_cdc"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `target_configs[].comparison_direction` { #target-configscomparison-direction }

source_to_target self-heals missing records, target_to_source audits orphaned records, both does each.


**Type** `string (enum)` · **Required** no · **Section** Target dataset


```json
{
  "comparison_direction": "both"
}
```


!!! tip "Best practice"

    - Allowed values: both, source_to_target, target_to_source.


---

### `target_configs[].data_standardization_sql` { #target-configsdata-standardization-sql }

Column-level cleanup before matching.


Column-level cleanup before matching. Same restricted grammar as ingestion.


**Type** `array<string>` · **Required** no · **Section** Target dataset


```json
{
  "data_standardization_sql": [
    "trim(status) AS status"
  ]
}
```


!!! tip "Best practice"

    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `target_configs[].filter_condition` { #target-configsfilter-condition }

Boolean SQL applied after read.


Boolean SQL applied after read. Supports ${param} substitution.


**Type** `string (SQL)` · **Required** no · **Section** Target dataset


```json
{
  "filter_condition": "load_date = '${run_date}'"
}
```


!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `target_configs[].hash_precomputed` { #target-configshash-precomputed }

Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing.


Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing. Valid because reconciliation targets are always tables.


**Type** `boolean` · **Required** no · **Section** Target dataset


```json
{
  "hash_precomputed": true
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `target_configs[].read_mode` { #target-configsread-mode }

At most one side of a given target's comparison may be streaming.


**Type** `string (enum)` · **Required** no · **Section** Target dataset


```json
{
  "read_mode": "batch"
}
```


!!! tip "Best practice"

    - Allowed values: batch, streaming.


---

### `target_configs[].target_catalog` { #target-configstarget-catalog }

Unity Catalog catalog of the target table.


Unity Catalog catalog of the target table. Composed into the three-part table name on save.


**Type** `string` · **Required** yes · **Section** Target dataset


```json
{
  "target_catalog": "{{catalog}}"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_configs[].target_id` { #target-configstarget-id }

Identifies this target in run and mismatch logs.


**Type** `string` · **Required** yes · **Section** Target dataset


```json
{
  "target_id": "primary_product_table"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `target_configs[].target_schema` { #target-configstarget-schema }

Schema of the target table.


Schema of the target table. Composed into the three-part table name on save.


**Type** `string` · **Required** yes · **Section** Target dataset


```json
{
  "target_schema": "bronze_example"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `target_configs[].target_table` { #target-configstarget-table }

Target table name.


Target table name. Composed into the three-part table name on save.


**Type** `string` · **Required** yes · **Section** Target dataset


```json
{
  "target_table": "example_raw_final"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `target_configs[].task_run_id_column` { #target-configstask-run-id-column }

When recon_mode is triggered and the task_run_id job parameter is set, narrows this side's read to that run's rows.


**Type** `string` · **Required** no · **Section** Target dataset


```json
{
  "task_run_id_column": "__framework_pipeline_run_id"
}
```


---

### `transform_sql` { #transform-sql }

Reshapes missing records before append when source and target schemas differ.


Reshapes missing records before append when source and target schemas differ. Must read FROM _reconciliation_unmatched_records. Full Spark SQL, not the restricted grammar.


**Type** `string (SQL)` · **Required** no · **Section** Matching & healing


```json
{
  "transform_sql": "SELECT example_id, amount, status FROM _reconciliation_unmatched_records"
}
```


!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.


---

### `two_tier_verification` { #two-tier-verification }

Runs a cheap Phase 1 per-side fingerprint (row_count plus an XOR-fold of the framework hash columns) (bit_xor(hash) plus a per-side count) first, and only falls through to the full matcher join when that phase disagrees.


**Type** `boolean` · **Required** no · **Section** Reconciliation identity


```json
{
  "two_tier_verification": true
}
```


!!! tip "Best practice"

    - phase 1 fingerprint, phase 2 full join
    - Omitting the attribute is not the same as setting it false — check the default above.


---
