<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Reconciliation flows

One entry per `reconciliation_flows[]` element — comparing a baseline against targets.


!!! info "34 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`compare_columns`](#compare-columns) | array<string> | no | — |
| [`dataflow_group_id`](#dataflow-group-id) | string | **yes** | — |
| [`dq_config.rules`](#dq-configrules) | array<object> | no | — |
| [`dq_config.rules[].action`](#dq-configrulesaction) | string (enum) | **yes** | — |
| [`dq_config.rules[].expression`](#dq-configrulesexpression) | string (SQL) | **yes** | — |
| [`dq_config.rules[].rule_id`](#dq-configrulesrule-id) | string | **yes** | — |
| [`error_handling.on_failure`](#error-handlingon-failure) | string (enum) | no | — |
| [`execution_mode`](#execution-mode) | string (enum) | no | — |
| [`logging_config.mismatch_log_capture`](#logging-configmismatch-log-capture) | boolean | no | — |
| [`logging_config.run_log_capture`](#logging-configrun-log-capture) | boolean | no | — |
| [`match_keys`](#match-keys) | array<string> | **yes** | — |
| [`publish_schema`](#publish-schema) | string | no | — |
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

### `dataflow_group_id` { #dataflow-group-id }

The dataflow group whose Lakeflow pipeline this flow is registered into.


The dataflow group whose Lakeflow pipeline this flow is registered into. Required when execution_mode is pipeline or pipeline_audit_only -- a group-less reconciliation flow has no pipeline update to live in. Usually this spec's own dataflow_group_id; naming another group registers the flow inside that group's pipeline instead. Stays optional in job mode, where the standalone engine handles the group-less case.


**Type** `string` · **Required** yes · **Section** Reconciliation identity


```json
{
  "dataflow_group_id": "dfg_example_group"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `dq_config.rules` { #dq-configrules }

Data-quality expectations evaluated on every row.


Each rule becomes a pipeline expectation; the action decides what happens to a failing row.


**Type** `array<object>` · **Required** no · **Section** Data quality


```json
"rules": [
  { "rule_id": "order_id_not_null", "expression": "order_id IS NOT NULL", "action": "drop" },
  { "rule_id": "amount_non_negative", "expression": "amount >= 0", "action": "quarantine" }
]
```


!!! tip "Best practice"

    - warn keeps the row and records the violation; drop discards it; fail stops the update; quarantine routes it to a sibling table.
    - quarantine is a framework extension, not native pipeline behaviour — it needs dq_config.quarantine_table set.
    - Give each rule a stable rule_id: it is what appears in __framework_dq_failed_rule_ids.


!!! warning "Known errors and limitations"

    **quarantine rows go nowhere**  
    *Cause:* quarantine_table was not configured.  
    *Fix:* Set dq_config.quarantine_table, and record_id_column so rows can be traced back.

    **The whole update fails on one bad row**  
    *Cause:* A rule uses action: fail.  
    *Fix:* Downgrade to drop or quarantine unless the condition really is unrecoverable.


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].action` { #dq-configrulesaction }

warn logs and keeps the row, drop silently removes it, fail aborts the pipeline, quarantine routes it to the quarantine table.


**Type** `string (enum)` · **Required** yes · **Section** Data quality


```json
{
  "action": "warn"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: warn, drop, fail, quarantine.


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].expression` { #dq-configrulesexpression }

Boolean Spark SQL expression evaluated per row.


**Type** `string (SQL)` · **Required** yes · **Section** Data quality


```json
{
  "expression": "amount >= 0"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].rule_id` { #dq-configrulesrule-id }

Unique rule identifier.


**Type** `string` · **Required** yes · **Section** Data quality


```json
{
  "rule_id": "dq_amount_non_negative"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


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

### `execution_mode` { #execution-mode }

Chooses where a reconciliation flow runs: as its own job task (job), or as a third flow type inside its dataflow group's Lakeflow pipeline update (pipeline, pipeline_audit_only).


In-pipeline reconciliation is the whole point of v1.5.0: ingestion, transformation and reconciliation in ONE pipeline update, so the comparison reads the rows this update just wrote instead of a stale snapshot from the previous cycle. It stays opt-in, and job stays the default, because job resources already run reconciliation tasks against onboarded rows -- flipping the default would run those flows twice per cycle.


**Type** `string (enum)` · **Required** no · **Section** Reconciliation identity


```json
"execution_mode": "pipeline"
```


!!! tip "Best practice"

    - pipeline_audit_only publishes the classified/metrics/mismatch datasets and runs dq_config expectations in-pipeline, but leaves the append-back healing lane in job mode. It is the right setting for a flow whose source is a static table.
    - Both pipeline modes require a flow-level dataflow_group_id, an append-only source producer (not an apply_changes strategy, and not a fully-refreshed materialized view), and read_mode batch on every side.
    - publish_schema and dq_config only exist in the pipeline modes; task_run_id_column and read_mode 'streaming' only exist in job mode. The builder hides the first pair in job mode; the last two are rejected at onboarding.


!!! warning "Known errors and limitations"

    **Onboarding rejects the flow naming dataflow_group_id**  
    *Cause:* execution_mode is pipeline or pipeline_audit_only but the flow declares no dataflow_group_id, so there is no pipeline update to register it in.  
    *Fix:* Set the flow's dataflow_group_id (usually this spec's own), or keep execution_mode 'job'.

    **Onboarding rejects source_config.table as not produced by this group**  
    *Cause:* V-CYC-1: a pipeline-mode flow must reconcile a dataset THIS pipeline update publishes, otherwise it silently degrades to an external, always-one-update-stale read.  
    *Fix:* Point source_config.table at an ingestion or transformation target of the same group, or set execution_mode to 'job'.

    **The reconciliation runs twice per cycle**  
    *Cause:* The flow was switched to pipeline mode while a job resource still runs a reconciliation task against the same reconciliation_id.  
    *Fix:* Remove the reconciliation task from the job, or move the flow back to execution_mode 'job'.


---

### `logging_config.mismatch_log_capture` { #logging-configmismatch-log-capture }

Per-flow gate on mismatch-log writes.


Per-flow gate on mismatch-log writes. DEFAULTS TO FALSE since v1.7.3 (it defaulted to true through v1.7.2): leaving this unset means no reconciliation_mismatch_log rows and, in pipeline mode, no recon__*__mismatch dataset. Set it true to opt in. Overridable at runtime by the recon_mismatch_log job parameter.


**Type** `boolean` · **Required** no · **Section** Reconciliation identity


```json
{
  "logging_config": {
    "mismatch_log_capture": true
  }
}
```


!!! tip "Best practice"

    - v1.7.3: defaults false -- auditing is opt-in
    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `logging_config.run_log_capture` { #logging-configrun-log-capture }

Per-flow gate on run-log writes.


Per-flow gate on run-log writes. DEFAULTS TO FALSE since v1.7.3 (it defaulted to true through v1.7.2): reconciliation is silent by default, so leaving this unset means no reconciliation_run_log or reconciliation_result rows and, in pipeline mode, no recon__*__metrics dataset at all. Set it true to opt in -- and you MUST set it true when the flow declares dq_config.rules, whose expectations attach to that dataset. Overridable at runtime by the recon_run_log_capture job parameter.


**Type** `boolean` · **Required** no · **Section** Reconciliation identity


```json
{
  "logging_config": {
    "run_log_capture": true
  }
}
```


!!! tip "Best practice"

    - v1.7.3: defaults false -- auditing is opt-in
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

### `publish_schema` { #publish-schema }

The schema, inside the hosting pipeline's own catalog, where a pipeline-mode reconciliation flow publishes its recon__<reconciliation_id>__<target_id>__metrics and __mismatch datasets (and a healing flow's prepared _src/_tgt).


Since v1.7.07 publish_schema is the only thing that publishes. Leave it unset and the flow publishes nothing: its audit datasets are pipeline-scoped temporary tables, a dq_config gate still fails the update, but no reconciliation_run_log / reconciliation_mismatch_log row can be exported. Before v1.7.07 an unset value fell back to the pipeline's own schema and dropped recon__*__metrics materialized views beside the business tables.


**Type** `string` · **Required** no · **Section** Reconciliation identity


```json
"publish_schema": "recon_results"
```


!!! tip "Best practice"

    - The catalog is always the hosting pipeline's own; only the schema is configurable.
    - Required when run_log_capture or mismatch_log_capture is true (the control-table rows are exported from the published datasets), and when execution_mode is 'pipeline' with an append_target_table (the heal handler reads the prepared source/target back through the metastore).
    - Leave it unset for a pure presence/threshold gate (dq_config only, both capture flags false): the gate runs, nothing lands in any schema.
    - It is rejected on presence when execution_mode is 'job' -- a job-mode flow publishes none of those datasets.
    - The schema must already exist and be writable by the pipeline's run-as identity; the framework does not create it.


!!! warning "Known errors and limitations"

    **Onboarding rejects publish_schema**  
    *Cause:* execution_mode is 'job' (or left unset, which defaults to 'job').  
    *Fix:* Set execution_mode to 'pipeline' or 'pipeline_audit_only', or clear publish_schema.

    **Onboarding rejects run_log_capture / mismatch_log_capture: 'true but the flow has no publish_schema'**  
    *Cause:* A capture flag is on but nothing is published, so the audit row could never be written.  
    *Fix:* Set publish_schema to a dedicated reconciliation schema, or set both capture flags false and rely on dq_config.

    **Onboarding rejects a pipeline-mode healing flow: 'requires publish_schema'**  
    *Cause:* The heal handler needs the prepared source/target as published tables.  
    *Fix:* Set publish_schema, or use execution_mode 'pipeline_audit_only' and heal from the job task.


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


At most one side of a given target's comparison may be streaming. Rejected on presence with value 'streaming' when execution_mode is 'pipeline' or 'pipeline_audit_only': the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner and left_outer, which cannot express MISSING_IN_SOURCE. Use read_mode 'batch' (the default), or set execution_mode to 'job' to keep the standalone streaming engine.


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

When the task_run_id job parameter is set, narrows this side's read to that run's rows.


When the task_run_id job parameter is set, narrows this side's read to that run's rows. Reconciliation is triggered-only as of v1.4.0. Rejected on presence when execution_mode is 'pipeline' or 'pipeline_audit_only': no stable per-update key exists inside a Lakeflow update, so the narrowing would match every row that pipeline ever wrote -- a silent no-op. Use filter_condition, or set execution_mode to 'job'.


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


At most one side of a given target's comparison may be streaming. Rejected on presence with value 'streaming' when execution_mode is 'pipeline' or 'pipeline_audit_only': the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner and left_outer, which cannot express MISSING_IN_SOURCE.


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

When the task_run_id job parameter is set, narrows this side's read to that run's rows.


When the task_run_id job parameter is set, narrows this side's read to that run's rows. Reconciliation is triggered-only as of v1.4.0. Rejected on presence when execution_mode is 'pipeline' or 'pipeline_audit_only': no stable per-update key exists inside a Lakeflow update, so the narrowing would be a silent no-op. Use filter_condition instead.


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
