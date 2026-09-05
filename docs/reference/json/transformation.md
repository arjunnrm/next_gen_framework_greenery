<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Transformation flows

One entry per `transformation_flows[]` element — SQL plus a CDC load strategy.


!!! info "76 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`dataflow_id`](#dataflow-id) | string | **yes** | — |
| [`decrypted_columns`](#decrypted-columns) | array<object> | no | — |
| [`decrypted_columns[].cast_to_type`](#decrypted-columnscast-to-type) | string | **yes** | — |
| [`decrypted_columns[].column_name`](#decrypted-columnscolumn-name) | string | **yes** | — |
| [`decrypted_columns[].input_name`](#decrypted-columnsinput-name) | string | **yes** | — |
| [`decrypted_columns[].mode`](#decrypted-columnsmode) | string (enum) | no | — |
| [`decrypted_columns[].output_column`](#decrypted-columnsoutput-column) | string | no | — |
| [`decrypted_columns[].secret.secret_catalog`](#decrypted-columnssecretsecret-catalog) | string | **yes** | — |
| [`decrypted_columns[].secret.secret_key`](#decrypted-columnssecretsecret-key) | string | **yes** | — |
| [`decrypted_columns[].secret.secret_schema`](#decrypted-columnssecretsecret-schema) | string | **yes** | — |
| [`dq_config.quarantine_table`](#dq-configquarantine-table) | string | no | — |
| [`dq_config.record_id_column`](#dq-configrecord-id-column) | string | no | — |
| [`dq_config.rules`](#dq-configrules) | array<object> | no | — |
| [`dq_config.rules[].action`](#dq-configrulesaction) | string (enum) | **yes** | — |
| [`dq_config.rules[].expression`](#dq-configrulesexpression) | string (SQL) | **yes** | — |
| [`dq_config.rules[].rule_id`](#dq-configrulesrule-id) | string | **yes** | — |
| [`flow_step_id`](#flow-step-id) | string | **yes** | — |
| [`governance_tags.column_tags`](#governance-tagscolumn-tags) | array<object> | no | — |
| [`governance_tags.column_tags[].column`](#governance-tagscolumn-tagscolumn) | string | **yes** | — |
| [`governance_tags.column_tags[].tags`](#governance-tagscolumn-tagstags) | object<string,string> | **yes** | — |
| [`governance_tags.table_tags`](#governance-tagstable-tags) | object<string,string> | no | — |
| [`source_inputs`](#source-inputs) | array<object> | no | — |
| [`source_inputs[].input_name`](#source-inputsinput-name) | string | **yes** | — |
| [`source_inputs[].is_streaming`](#source-inputsis-streaming) | boolean | no | — |
| [`source_inputs[].table`](#source-inputstable) | string | **yes** | — |
| [`source_inputs[].watermark.delay_threshold`](#source-inputswatermarkdelay-threshold) | string | no | — |
| [`source_inputs[].watermark.event_time_column`](#source-inputswatermarkevent-time-column) | string | no | — |
| [`target_catalog`](#target-catalog) | string | **yes** | — |
| [`target_config.auto_ttl.expire_in_days`](#target-configauto-ttlexpire-in-days) | integer | no | — |
| [`target_config.auto_ttl.timestamp_column`](#target-configauto-ttltimestamp-column) | string | no | — |
| [`target_config.capture_technical_metadata`](#target-configcapture-technical-metadata) | boolean | no | — |
| [`target_config.encrypted_columns`](#target-configencrypted-columns) | array<object> | no | — |
| [`target_config.encrypted_columns[].column_name`](#target-configencrypted-columnscolumn-name) | string | **yes** | — |
| [`target_config.encrypted_columns[].mode`](#target-configencrypted-columnsmode) | string (enum) | no | — |
| [`target_config.encrypted_columns[].output_column`](#target-configencrypted-columnsoutput-column) | string | no | — |
| [`target_config.encrypted_columns[].secret.secret_catalog`](#target-configencrypted-columnssecretsecret-catalog) | string | **yes** | — |
| [`target_config.encrypted_columns[].secret.secret_key`](#target-configencrypted-columnssecretsecret-key) | string | **yes** | — |
| [`target_config.encrypted_columns[].secret.secret_schema`](#target-configencrypted-columnssecretsecret-schema) | string | **yes** | — |
| [`target_config.encrypted_columns[].source_data_type`](#target-configencrypted-columnssource-data-type) | string | no | — |
| [`target_config.liquid_clustering_columns`](#target-configliquid-clustering-columns) | array<string> | no | — |
| [`target_config.partition_columns`](#target-configpartition-columns) | array<string> | no | — |
| [`target_config.partition_mode`](#target-configpartition-mode) | string (enum) | no | — |
| [`target_config.sink_config.format`](#target-configsink-configformat) | string (enum) | **yes** | — |
| [`target_config.sink_config.kafka_options`](#target-configsink-configkafka-options) | object<string,string> | **yes** | — |
| [`target_config.sink_config.path`](#target-configsink-configpath) | string | **yes** | — |
| [`target_config.sink_config.post_export_archive.archive_format`](#target-configsink-configpost-export-archivearchive-format) | string (enum) | no | — |
| [`target_config.sink_config.post_export_archive.enabled`](#target-configsink-configpost-export-archiveenabled) | boolean | no | — |
| [`target_config.sink_config.post_export_archive.export_file_name_format`](#target-configsink-configpost-export-archiveexport-file-name-format) | string | no | — |
| [`target_config.sink_config.post_export_archive.output_zip_path`](#target-configsink-configpost-export-archiveoutput-zip-path) | string | **yes** | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.enabled`](#target-configsink-configpost-export-archivepgp-encryptionenabled) | boolean | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-schema) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema) | string | no | — |
| [`target_config.sink_config.post_export_archive.secret.secret_catalog`](#target-configsink-configpost-export-archivesecretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.secret.secret_key`](#target-configsink-configpost-export-archivesecretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.secret.secret_schema`](#target-configsink-configpost-export-archivesecretsecret-schema) | string | no | — |
| [`target_config.sink_config.staged_file_format`](#target-configsink-configstaged-file-format) | string (enum) | no | — |
| [`target_config.sink_config.staged_file_options.delimiter`](#target-configsink-configstaged-file-optionsdelimiter) | string | no | — |
| [`target_config.sink_config.staged_file_options.include_header`](#target-configsink-configstaged-file-optionsinclude-header) | boolean | no | — |
| [`target_config.sink_config.staged_file_options.line_terminator`](#target-configsink-configstaged-file-optionsline-terminator) | string (enum) | no | — |
| [`target_config.sink_config.write_mode`](#target-configsink-configwrite-mode) | string (enum) | no | — |
| [`target_config.storage_format`](#target-configstorage-format) | string (enum) | no | — |
| [`target_config.table_properties`](#target-configtable-properties) | object<string,string> | no | — |
| [`target_schema`](#target-schema) | string | **yes** | — |
| [`target_table`](#target-table) | string | **yes** | — |
| [`target_type`](#target-type) | string (enum) | **yes** | — |
| [`transformation_sql`](#transformation-sql) | string (SQL) | **yes** | — |

## Attributes

### `dataflow_id` { #dataflow-id }

Unique ID for this ingestion flow.


Unique ID for this ingestion flow. Referenced by transformation flows.


**Type** `string` · **Required** yes · **Section** Flow identity


```json
{
  "dataflow_id": "df_template_ingest"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `decrypted_columns` { #decrypted-columns }

Sets source_inputs[].decrypted_columns[].


**Type** `array<object>` · **Required** no · **Section** Decrypted columns


```json
{
  "decrypted_columns": [
    {
      "...": "one object per entry"
    }
  ]
}
```


!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.


---

### `decrypted_columns[].cast_to_type` { #decrypted-columnscast-to-type }

Spark SQL type to cast the decrypted value to.


Spark SQL type to cast the decrypted value to. Cross-validated against the original_data_type tag.


**Type** `string` · **Required** yes · **Section** Decrypted columns


```json
{
  "cast_to_type": "string"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `decrypted_columns[].column_name` { #decrypted-columnscolumn-name }

Source ciphertext column on the input table.


**Type** `string` · **Required** yes · **Section** Decrypted columns


```json
{
  "column_name": "pii_column"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `decrypted_columns[].input_name` { #decrypted-columnsinput-name }

Which source_inputs[] entry this decryption belongs to.


**Type** `string` · **Required** yes · **Section** Decrypted columns


```json
{
  "input_name": "example_raw_input"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `decrypted_columns[].mode` { #decrypted-columnsmode }

Must match the mode the column was encrypted with.


**Type** `string (enum)` · **Required** no · **Section** Decrypted columns


```json
{
  "mode": "GCM"
}
```


!!! tip "Best practice"

    - Allowed values: GCM, CBC, ECB.


---

### `decrypted_columns[].output_column` { #decrypted-columnsoutput-column }

Output plaintext column.


Output plaintext column. Use a different name to keep both ciphertext and plaintext.


**Type** `string` · **Required** no · **Section** Decrypted columns


```json
{
  "output_column": "pii_column_plain"
}
```


---

### `decrypted_columns[].secret.secret_catalog` { #decrypted-columnssecretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** yes · **Section** Decrypted columns


```json
{
  "secret": {
    "secret_catalog": "{{catalog}}"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `decrypted_columns[].secret.secret_key` { #decrypted-columnssecretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** yes · **Section** Decrypted columns


```json
{
  "secret": {
    "secret_key": "pii_encryption_key"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `decrypted_columns[].secret.secret_schema` { #decrypted-columnssecretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** yes · **Section** Decrypted columns


```json
{
  "secret": {
    "secret_schema": "security"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `dq_config.quarantine_table` { #dq-configquarantine-table }

Quarantine table name.


Quarantine table name. Only created when at least one rule uses action quarantine.


**Type** `string` · **Required** no · **Section** Data quality


```json
{
  "dq_config": {
    "quarantine_table": "<target_table>_quarantine"
  }
}
```


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.record_id_column` { #dq-configrecord-id-column }

Surfaced as __framework_record_id on quarantined rows for traceability.


**Type** `string` · **Required** no · **Section** Data quality


```json
{
  "dq_config": {
    "record_id_column": "example_id"
  }
}
```


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


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

### `flow_step_id` { #flow-step-id }

Unique ID for this transformation step.


**Type** `string` · **Required** yes · **Section** Flow identity


```json
{
  "flow_step_id": "ts_template_scd1_example"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `governance_tags.column_tags` { #governance-tagscolumn-tags }

Unity Catalog tags applied to individual columns after deployment.


Drives discovery and classification. Applied via ALTER TABLE SET TAGS once the table exists.


**Type** `array<object>` · **Required** no · **Section** Governance tags


```json
"column_tags": [
  { "column_name": "email", "tag_key": "pii", "tag_value": "true" }
]
```


!!! tip "Best practice"

    - This framework applies tags only. It does not create masking policies or row filters — tagging a column does not protect it.
    - Tags are applied post-deployment, so they appear after the first successful update, not at onboarding time.


!!! warning "Known errors and limitations"

    **Tags never appear**  
    *Cause:* The run-as principal lacks APPLY TAG on the target.  
    *Fix:* GRANT APPLY TAG ON TABLE ... TO <principal>.


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `governance_tags.column_tags[].column` { #governance-tagscolumn-tagscolumn }

Column to tag.


**Type** `string` · **Required** yes · **Section** Governance tags


```json
{
  "column": "pii_column"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `governance_tags.column_tags[].tags` { #governance-tagscolumn-tagstags }

Key-value tags, e.g.


Key-value tags, e.g. mask: PII, classification: restricted.


**Type** `object<string,string>` · **Required** yes · **Section** Governance tags


```json
{
  "tags": {
    "option_name": "value"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `governance_tags.table_tags` { #governance-tagstable-tags }

Table-level tags, e.g.


Table-level tags, e.g. row_filter: region_restricted, domain: finance.


**Type** `object<string,string>` · **Required** no · **Section** Governance tags


```json
{
  "governance_tags": {
    "table_tags": {
      "option_name": "value"
    }
  }
}
```


!!! tip "Best practice"

    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `source_inputs` { #source-inputs }

Sets source_inputs[].


**Type** `array<object>` · **Required** no · **Section** Source inputs


```json
{
  "source_inputs": [
    {
      "...": "one object per entry"
    }
  ]
}
```


!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.


---

### `source_inputs[].input_name` { #source-inputsinput-name }

Must be unique across the entire spec, not just this flow.


Must be unique across the entire spec, not just this flow. Referenced in transformation_sql FROM/JOIN.


**Type** `string` · **Required** yes · **Section** Source inputs


```json
{
  "input_name": "example_raw_input"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_inputs[].is_streaming` { #source-inputsis-streaming }

true reads with spark.readStream.table, false with spark.read.table.


**Type** `boolean` · **Required** no · **Section** Source inputs


```json
{
  "is_streaming": true
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `source_inputs[].table` { #source-inputstable }

Fully-qualified upstream table.


Fully-qualified upstream table. Can be any table, not only ones produced by this spec.


**Type** `string` · **Required** yes · **Section** Source inputs


```json
{
  "table": "{{catalog}}.bronze_example.example_raw"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_inputs[].watermark.delay_threshold` { #source-inputswatermarkdelay-threshold }

Maximum allowed event lateness.


Maximum allowed event lateness. Required when streaming and joined with another stream.


**Type** `string` · **Required** no · **Section** Source inputs


```json
{
  "watermark": {
    "delay_threshold": "10 minutes"
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `source_inputs[].watermark.event_time_column` { #source-inputswatermarkevent-time-column }

Event-time column for watermarking.


Event-time column for watermarking. Auto-cast to timestamp. Required when streaming and joined with another stream.


**Type** `string` · **Required** no · **Section** Source inputs


```json
{
  "watermark": {
    "event_time_column": "updated_at"
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `target_catalog` { #target-catalog }

Unity Catalog catalog for the target table.


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

### `target_config.auto_ttl.expire_in_days` { #target-configauto-ttlexpire-in-days }

Rows older than this many days are auto-deleted.


**Type** `integer` · **Required** no · **Section** Target · storage & table


```json
{
  "target_config": {
    "auto_ttl": {
      "expire_in_days": 90
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `target_config.auto_ttl.timestamp_column` { #target-configauto-ttltimestamp-column }

The column whose value decides how old a row is, for automatic expiry.


auto TTL deletes rows whose timestamp is older than expire_in_days.


**Type** `string` · **Required** no · **Section** Target · storage & table


```json
"auto_ttl": { "timestamp_column": "updated_at", "expire_in_days": 90 }
```


!!! tip "Best practice"

    - Must be DATE, TIMESTAMP or TIMESTAMP_NTZ.
    - Valid only for APPEND and TRUNCATE_AND_LOAD — a hard error on CDC strategies.
    - Deletion is permanent. Confirm your retention policy before enabling it.


!!! warning "Known errors and limitations"

    **Onboarding fails with a hard error on a CDC flow**  
    *Cause:* auto TTL is incompatible with CDC-dispatched strategies.  
    *Fix:* Remove the auto_ttl block, or move expiry into a separate maintenance job.


---

### `target_config.capture_technical_metadata` { #target-configcapture-technical-metadata }

Transformation flows only.


Transformation flows only. Gates __framework_ingestion_timestamp_utc.


**Type** `boolean` · **Required** no · **Section** Target · storage & table


```json
{
  "target_config": {
    "capture_technical_metadata": true
  }
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `target_config.encrypted_columns` { #target-configencrypted-columns }

Sets target_config.encrypted_columns[].


**Type** `array<object>` · **Required** no · **Section** Target · encrypted columns


```json
{
  "target_config": {
    "encrypted_columns": [
      {
        "...": "one object per entry"
      }
    ]
  }
}
```


!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.


---

### `target_config.encrypted_columns[].column_name` { #target-configencrypted-columnscolumn-name }

Plaintext column on this flow's DataFrame to encrypt.


**Type** `string` · **Required** yes · **Section** Target · encrypted columns


```json
{
  "column_name": "pii_column"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `target_config.encrypted_columns[].mode` { #target-configencrypted-columnsmode }

AES cipher mode.


AES cipher mode. GCM is recommended — it adds a random IV.


**Type** `string (enum)` · **Required** no · **Section** Target · encrypted columns


```json
{
  "mode": "GCM"
}
```


!!! tip "Best practice"

    - Allowed values: GCM, CBC, ECB.


---

### `target_config.encrypted_columns[].output_column` { #target-configencrypted-columnsoutput-column }

Output column name.


Output column name. Defaults to column_name — set the same name to encrypt in place.


**Type** `string` · **Required** no · **Section** Target · encrypted columns


```json
{
  "output_column": "pii_column"
}
```


---

### `target_config.encrypted_columns[].secret.secret_catalog` { #target-configencrypted-columnssecretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** yes · **Section** Target · encrypted columns


```json
{
  "secret": {
    "secret_catalog": "{{catalog}}"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.encrypted_columns[].secret.secret_key` { #target-configencrypted-columnssecretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** yes · **Section** Target · encrypted columns


```json
{
  "secret": {
    "secret_key": "pii_encryption_key"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.encrypted_columns[].secret.secret_schema` { #target-configencrypted-columnssecretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** yes · **Section** Target · encrypted columns


```json
{
  "secret": {
    "secret_schema": "security"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.encrypted_columns[].source_data_type` { #target-configencrypted-columnssource-data-type }

Optional.


Optional. The column's original Spark type before encryption replaces it with ciphertext binary (string, decimal(18,2), timestamp, ...). Becomes the Unity Catalog original_data_type tag that a downstream decrypted_columns.cast_to_type is checked against. Leave blank to use the type observed at encryption time; declare it to make a silent source type change fail loudly instead.


**Type** `string` · **Required** no · **Section** Target · encrypted columns


```json
{
  "source_data_type": "string"
}
```


---

### `target_config.liquid_clustering_columns` { #target-configliquid-clustering-columns }

Clustering keys for Delta liquid clustering.


Gives data skipping without the file-count problems of partitioning, and can be changed later without rewriting the table.


**Type** `array<string>` · **Required** no · **Section** Target · storage & table


```json
"liquid_clustering_columns": ["customer_id", "order_date"]
```


!!! tip "Best practice"

    - Maximum of three columns — enforced at onboarding and again at runtime.
    - Prefer this over partition_columns for anything under about 1 TB.
    - Choose the columns your queries actually filter on.


!!! warning "Known errors and limitations"

    **Onboarding rejects the flow**  
    *Cause:* More than three columns were listed.  
    *Fix:* Reduce to the three most selective predicates.


**Databricks documentation:** [liquid clustering](https://docs.databricks.com/delta/clustering.html)


---

### `target_config.partition_columns` { #target-configpartition-columns }

Physical Hive-style partitioning of the target table.


Splits the table into directories by column value. Effective only for APPEND and TRUNCATE_AND_LOAD.


**Type** `array<string>` · **Required** no · **Section** Target · storage & table


```json
"partition_columns": ["ingest_date"]

// explicitly unpartitioned:
"partition_columns": []
```


!!! tip "Best practice"

    - Databricks guidance: do not partition tables under roughly 1 TB. Use liquid clustering instead.
    - An empty array is a deliberate, documented statement that the table is unpartitioned — it is not an error.
    - Partition on a low-cardinality column. Partitioning on an id produces millions of tiny files.


!!! warning "Known errors and limitations"

    **Queries got slower after partitioning**  
    *Cause:* Over-partitioning created many small files.  
    *Fix:* Drop the partitioning and switch to liquid_clustering_columns.


**Databricks documentation:** [partitioning](https://docs.databricks.com/tables/partitions.html) · [liquid clustering](https://docs.databricks.com/delta/clustering.html)


---

### `target_config.partition_mode` { #target-configpartition-mode }

absent omits partition_columns entirely.


absent omits partition_columns entirely. unpartitioned writes partition_columns: [] — an explicit, documented declaration that the table is not partitioned. named writes the columns you list below.


**Type** `string (enum)` · **Required** no · **Section** Target · storage & table


```json
{
  "target_config": {
    "partition_mode": "absent"
  }
}
```


!!! tip "Best practice"

    - [] means explicitly unpartitioned, never an error
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: absent, unpartitioned, named.


**Databricks documentation:** [partitioning](https://docs.databricks.com/tables/partitions.html) · [liquid clustering](https://docs.databricks.com/delta/clustering.html)


---

### `target_config.sink_config.format` { #target-configsink-configformat }

The export format for a sink or external_sink target.


sink exports only; external_sink writes a governed table and also exports.


**Type** `string (enum)` · **Required** yes · **Section** Target · sink config


```json
"sink_config": { "format": "pgp_zip", "path": "/Volumes/{{catalog}}/egress/orders/" }
```


!!! tip "Best practice"

    - pgp_zip uses the framework's own PySpark DataSource, not a built-in writer.
    - kafka takes connection options instead of a path.


!!! warning "Known errors and limitations"

    **Onboarding rejects a missing path**  
    *Cause:* delta and pgp_zip both require sink_config.path.  
    *Fix:* Supply an output directory, or switch the format to kafka.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.kafka_options` { #target-configsink-configkafka-options }

Connection options for a kafka sink — the same flat options a Spark Structured Streaming Kafka writer takes.


Connection options for a kafka sink — the same flat options a Spark Structured Streaming Kafka writer takes. Onboarding requires both kafka.bootstrap.servers and topic. A kafka sink has no filesystem path. Prefer databricks.serviceCredential over an inline credential. sink_config.kafka_secret_options (option name → UC secret ref, for an option whose literal value must embed a resolved secret such as kafka.sasl.jaas.config) is supported by the framework but cannot be authored here — add it by hand to the exported JSON.


**Type** `object<string,string>` · **Required** yes · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "kafka_options": {
        "option_name": "value"
      }
    }
  }
}
```


!!! tip "Best practice"

    - kafka.bootstrap.servers and topic are both mandatory
    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.path` { #target-configsink-configpath }

Output directory.


Output directory. Required for delta and pgp_zip.


**Type** `string` · **Required** yes · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "path": "/Volumes/{{catalog}}/egress/example/"
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.archive_format` { #target-configsink-configpost-export-archivearchive-format }

The container the finished export is delivered in: 'zip' or 'gzip'.


The sink was ZIP-only, so an interface specifying a .csv.gz drop could not be served without a downstream repack — which would have meant either breaking the supplier contract or hand-rolling file handling outside the framework.


**Type** `string (enum)` · **Required** no · **Section** Target · sink config


```json
// Emits EE_20260901-LEIDOS_TELEPHONE_001.csv.gz
"post_export_archive": {
  "enabled": true,
  "output_zip_path": "/Volumes/{{catalog}}/staging/uc_6/output/",
  "export_file_name_format": "EE_${export_file_date}-LEIDOS_TELEPHONE_${export_file_sequence}",
  "archive_format": "gzip"
}
// add pgp_encryption.passphrase_secret -> ....csv.gz.gpg
```


!!! tip "Best practice"

    - A gzip holds exactly ONE member, so the staged partition files are concatenated into a single stream. The CSV header is written per partition, so all but the first are dropped during concatenation.
    - A gzip stream has no archive password: post_export_archive.secret does not apply. To protect a gzip export, use pgp_encryption.
    - The extension follows the content: .csv.gz or .jsonl.gz, and .gpg is appended when pgp_encryption is on (a ZIP archive uses .pgp instead).


!!! warning "Known errors and limitations"

    **The consumer rejects the file as a corrupt gzip.**  
    *Cause:* Some tools stop at the first member boundary in a multi-member gzip.  
    *Fix:* The framework emits a single-member stream, so this should not occur. If it does, confirm nothing downstream is re-concatenating the exports.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.enabled` { #target-configsink-configpost-export-archiveenabled }

Enable post-write archiving for pgp_zip exports.


**Type** `boolean` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "enabled": true
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.export_file_name_format` { #target-configsink-configpost-export-archiveexport-file-name-format }

str.format()-style template for the exported archive's own file name.


str.format()-style template for the exported archive's own file name. Placeholders: {batch_id}, {timestamp}. Omit for the framework default.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "export_file_name_format": "export_{batch_id}_{timestamp}.zip"
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.output_zip_path` { #target-configsink-configpost-export-archiveoutput-zip-path }

Where the final ZIP is written.


**Type** `string` · **Required** yes · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "output_zip_path": "/Volumes/{{catalog}}/egress/zips/"
      }
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.enabled` { #target-configsink-configpost-export-archivepgp-encryptionenabled }

PGP-encrypt the output archive.


**Type** `boolean` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "enabled": true
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-catalog }

SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key.


SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key. Mutually exclusive with recipient_public_key_secret: set exactly one. Signing is unavailable in this mode (it needs a sender keypair).


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "passphrase_secret": {
            "secret_catalog": "{{catalog}}"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-key }

SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key.


SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key. Mutually exclusive with recipient_public_key_secret: set exactly one. Signing is unavailable in this mode (it needs a sender keypair).


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "passphrase_secret": {
            "secret_key": "pii_encryption_key"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-schema }

SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key.


SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key. Mutually exclusive with recipient_public_key_secret: set exactly one. Signing is unavailable in this mode (it needs a sender keypair).


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "passphrase_secret": {
            "secret_schema": "security"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "recipient_public_key_secret": {
            "secret_catalog": "{{catalog}}"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "recipient_public_key_secret": {
            "secret_key": "pii_encryption_key"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "recipient_public_key_secret": {
            "secret_schema": "security"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "sign_passphrase_secret": {
            "secret_catalog": "{{catalog}}"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "sign_passphrase_secret": {
            "secret_key": "pii_encryption_key"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "sign_passphrase_secret": {
            "secret_schema": "security"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "sign_with_private_key_secret": {
            "secret_catalog": "{{catalog}}"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "sign_with_private_key_secret": {
            "secret_key": "pii_encryption_key"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "pgp_encryption": {
          "sign_with_private_key_secret": {
            "secret_schema": "security"
          }
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_catalog` { #target-configsink-configpost-export-archivesecretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "secret": {
          "secret_catalog": "{{catalog}}"
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_key` { #target-configsink-configpost-export-archivesecretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "secret": {
          "secret_key": "pii_encryption_key"
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_schema` { #target-configsink-configpost-export-archivesecretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "post_export_archive": {
        "secret": {
          "secret_schema": "security"
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.staged_file_format` { #target-configsink-configstaged-file-format }

File format of the staged export files a pgp_zip sink writes before archiving.


json (the default when absent) writes JSON-Lines; csv writes RFC-4180 files with a header row, one file per written partition. Only meaningful when sink_config.format is pgp_zip.


**Type** `string (enum)` · **Required** no · **Section** Target · sink config


```json
"sink_config": { "format": "pgp_zip", "path": "/Volumes/{{catalog}}/egress/orders/", "staged_file_format": "csv" }
```


!!! tip "Best practice"

    - Leave it unset to keep the JSON-Lines staging the framework has always produced.
    - csv emits one file per written partition, each with its own header row.


!!! warning "Known errors and limitations"

    **Onboarding rejects staged_file_format**  
    *Cause:* It was set on a sink whose format is not pgp_zip.  
    *Fix:* Remove the attribute, or switch sink_config.format to pgp_zip.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.staged_file_options.delimiter` { #target-configsink-configstaged-file-optionsdelimiter }

The field separator for the staged CSV a pgp_zip sink writes.


Before v1.7.4 the writer used Python's csv `excel` dialect with no override — comma-separated, always headered, CRLF-terminated. A supplier interface that specifies anything else simply could not be expressed, which is what UC6's pipe-delimited Leidos feed needed.


**Type** `string` · **Required** no · **Section** Target · sink config


```json
"sink_config": {
  "format": "pgp_zip",
  "staged_file_format": "csv",
  "staged_file_options": {
    "delimiter": "|",
    "include_header": true,
    "line_terminator": "lf"
  }
}
```


!!! tip "Best practice"

    - Exactly one character. Python's csv writer cannot emit a multi-character delimiter and the framework rejects one rather than silently truncating it.
    - Omit the whole staged_file_options object to keep the pre-1.7.4 dialect exactly.
    - Only applies to staged_file_format 'csv'. A JSON-Lines export has no delimiter.


!!! warning "Known errors and limitations"

    **The consumer reads one giant column, or splits on the wrong character.**  
    *Cause:* The delimiter here does not match what the receiving interface expects.  
    *Fix:* Match the interface specification exactly. If fields can contain the delimiter, the writer quotes them per RFC-4180 — confirm the consumer honours quoting.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.staged_file_options.include_header` { #target-configsink-configstaged-file-optionsinclude-header }

Write the header row.


Write the header row. Absent = true. Set false for a supplier interface that specifies a headerless body. NOTE: with archive_format gzip the header is emitted per partition and all but the first are dropped on concatenation.


**Type** `boolean` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "staged_file_options": {
        "include_header": true
      }
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.staged_file_options.line_terminator` { #target-configsink-configstaged-file-optionsline-terminator }

The record separator for the staged CSV: 'crlf' or 'lf'.


Spelled as a name because JSON cannot carry a bare control character — you cannot write a literal CR in a JSON string and have it survive round-tripping.


**Type** `string (enum)` · **Required** no · **Section** Target · sink config


```json
"staged_file_options": { "line_terminator": "lf" }
// crlf (default when absent) = RFC-4180, the pre-1.7.4 behaviour
// lf                          = bare \n, what most Unix consumers expect
```


!!! tip "Best practice"

    - Absent = crlf. Legacy mainframe and Windows interfaces usually want crlf; most Unix-side consumers want lf.


!!! warning "Known errors and limitations"

    **The consumer reports a stray \r at the end of the last field on every row.**  
    *Cause:* The file is CRLF-terminated but the consumer splits on \n only.  
    *Fix:* Set line_terminator to 'lf'.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.write_mode` { #target-configsink-configwrite-mode }

Delta write mode.


Delta write mode. Currently accepted but inert — @dlt.append_flow always appends.


**Type** `string (enum)` · **Required** no · **Section** Target · sink config


```json
{
  "target_config": {
    "sink_config": {
      "write_mode": "append"
    }
  }
}
```


!!! tip "Best practice"

    - Allowed values: append, overwrite.


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.storage_format` { #target-configstorage-format }

Target table format.


Target table format. iceberg is only valid when target_type is batch_table — for every other target type, leave this on delta and add the table property enable_iceberg_read_uniformity: true instead, which turns on Delta UniForm so Iceberg readers can read the Delta table.


**Type** `string (enum)` · **Required** no · **Section** Target · storage & table


```json
{
  "target_config": {
    "storage_format": "delta"
  }
}
```


!!! tip "Best practice"

    - iceberg: batch_table only · Iceberg reads elsewhere via UniForm table property
    - Allowed values: delta, iceberg.


**Databricks documentation:** [uniform](https://docs.databricks.com/delta/uniform.html) · [table properties](https://docs.databricks.com/delta/table-properties.html)


---

### `target_config.table_properties` { #target-configtable-properties }

Delta table properties applied to the target.


Controls log retention, VACUUM safety windows, and Iceberg read compatibility.


**Type** `object<string,string>` · **Required** no · **Section** Target · storage & table


```json
"table_properties": {
  "log_retention_duration": "interval 30 days",
  "enable_iceberg_read_uniformity": "true"
}
```


!!! tip "Best practice"

    - enable_iceberg_read_uniformity turns on Delta UniForm so Iceberg readers can read a Delta table — valid for any target_type, unlike storage_format: iceberg.
    - Setting deleted_file_retention_duration below your longest-running query risks VACUUM removing files still being read.


!!! warning "Known errors and limitations"

    **FileNotFoundException in a long query**  
    *Cause:* VACUUM removed files while the query was running.  
    *Fix:* Raise deleted_file_retention_duration above your longest query duration.


**Databricks documentation:** [table properties](https://docs.databricks.com/delta/table-properties.html) · [uniform](https://docs.databricks.com/delta/uniform.html)


---

### `target_schema` { #target-schema }

Schema for the target table.


**Type** `string` · **Required** yes · **Section** Target dataset


```json
{
  "target_schema": "bronze_example"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `target_table` { #target-table }

Target Delta table name.


**Type** `string` · **Required** yes · **Section** Target dataset


```json
{
  "target_table": "example_raw"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `target_type` { #target-type }

The kind of Lakeflow dataset registered for this flow.


Decides whether the target is incrementally maintained, fully recomputed, or export-only.


**Type** `string (enum)` · **Required** yes · **Section** Target dataset


```json
"target_type": "streaming_table"
```


!!! tip "Best practice"

    - streaming_table is the default for incremental ingestion.
    - materialized_view fully recomputes — pair it with TRUNCATE_AND_LOAD, not a CDC strategy.
    - storage_format: iceberg is only valid for batch_table; elsewhere use the enable_iceberg_read_uniformity table property.


!!! warning "Known errors and limitations"

    **Onboarding rejects storage_format: iceberg**  
    *Cause:* Iceberg storage is restricted to batch_table.  
    *Fix:* Keep delta and add enable_iceberg_read_uniformity to table_properties.


**Databricks documentation:** [streaming tables](https://docs.databricks.com/tables/streaming.html) · [materialized views](https://docs.databricks.com/views/materialized.html)


---

### `transformation_sql` { #transformation-sql }

The SQL transform.


The SQL transform. Reads from input_name values declared in source_inputs.


**Type** `string (SQL)` · **Required** yes · **Section** Transformation SQL


```json
{
  "transformation_sql": "SELECT example_id, region, amount FROM example_raw_input WHERE amount > ${min_amount}"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---
