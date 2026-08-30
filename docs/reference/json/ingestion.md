<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Ingestion flows

One entry per `ingestion_flows[]` element — reading from a landing zone into Bronze.


!!! info "100 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`dataflow_id`](#dataflow-id) | string | **yes** | — |
| [`dq_config.quarantine_table`](#dq-configquarantine-table) | string | no | — |
| [`dq_config.record_id_column`](#dq-configrecord-id-column) | string | no | — |
| [`dq_config.rules`](#dq-configrules) | array<object> | no | — |
| [`dq_config.rules[].action`](#dq-configrulesaction) | string (enum) | **yes** | — |
| [`dq_config.rules[].expression`](#dq-configrulesexpression) | string (SQL) | **yes** | — |
| [`dq_config.rules[].rule_id`](#dq-configrulesrule-id) | string | **yes** | — |
| [`governance_tags.column_tags`](#governance-tagscolumn-tags) | array<object> | no | — |
| [`governance_tags.column_tags[].column`](#governance-tagscolumn-tagscolumn) | string | **yes** | — |
| [`governance_tags.column_tags[].tags`](#governance-tagscolumn-tagstags) | object<string,string> | **yes** | — |
| [`governance_tags.table_tags`](#governance-tagstable-tags) | object<string,string> | no | — |
| [`source_config.asn1_codec`](#source-configasn1-codec) | string (enum) | **yes** | — |
| [`source_config.asn1_pdu_name`](#source-configasn1-pdu-name) | string | **yes** | — |
| [`source_config.asn1_schema_path`](#source-configasn1-schema-path) | string | **yes** | — |
| [`source_config.auto_flatten_all`](#source-configauto-flatten-all) | boolean | no | — |
| [`source_config.capture_technical_metadata`](#source-configcapture-technical-metadata) | boolean | no | — |
| [`source_config.column_normalization.case`](#source-configcolumn-normalizationcase) | string (enum) | no | — |
| [`source_config.column_normalization.enabled`](#source-configcolumn-normalizationenabled) | boolean | no | — |
| [`source_config.data_standardization_sql`](#source-configdata-standardization-sql) | array<string> | no | — |
| [`source_config.dedup_watermark.delay_threshold`](#source-configdedup-watermarkdelay-threshold) | string | no | — |
| [`source_config.dedup_watermark.event_time_column`](#source-configdedup-watermarkevent-time-column) | string | no | — |
| [`source_config.explode_columns`](#source-configexplode-columns) | array<string> | no | — |
| [`source_config.explode_mode`](#source-configexplode-mode) | string (enum) | no | — |
| [`source_config.file_pattern`](#source-configfile-pattern) | string | no | — |
| [`source_config.format`](#source-configformat) | string (enum) | **yes** | — |
| [`source_config.json_string_columns`](#source-configjson-string-columns) | array<string> | no | — |
| [`source_config.landing_retention_policy.archive_path`](#source-configlanding-retention-policyarchive-path) | string | no | — |
| [`source_config.landing_retention_policy.clean_source`](#source-configlanding-retention-policyclean-source) | string (enum) | no | — |
| [`source_config.landing_retention_policy.retention_days`](#source-configlanding-retention-policyretention-days) | integer | no | — |
| [`source_config.max_bytes_per_trigger`](#source-configmax-bytes-per-trigger) | string | no | — |
| [`source_config.path`](#source-configpath) | string | **yes** | — |
| [`source_config.reader_options`](#source-configreader-options) | object<string,string> | no | — |
| [`source_config.remove_dups`](#source-configremove-dups) | boolean | no | — |
| [`source_config.schema_config_path`](#source-configschema-config-path) | string | no | — |
| [`source_config.schema_evolution_mode`](#source-configschema-evolution-mode) | string (enum) | no | — |
| [`source_config.schema_location`](#source-configschema-location) | string | no | — |
| [`source_config.source_catalog`](#source-configsource-catalog) | string | **yes** | — |
| [`source_config.source_schema`](#source-configsource-schema) | string | **yes** | — |
| [`source_config.source_table`](#source-configsource-table) | string | **yes** | — |
| [`source_config.source_zip_handling.delete_source_after_extract.action`](#source-configsource-zip-handlingdelete-source-after-extractaction) | string (enum) | no | — |
| [`source_config.source_zip_handling.delete_source_after_extract.days`](#source-configsource-zip-handlingdelete-source-after-extractdays) | integer | **yes** | — |
| [`source_config.source_zip_handling.enabled`](#source-configsource-zip-handlingenabled) | boolean | **yes** | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_catalog`](#source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-catalog) | string | no | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_key`](#source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-key) | string | no | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_schema`](#source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-schema) | string | no | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_catalog`](#source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-catalog) | string | **yes** | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_key`](#source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-key) | string | **yes** | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_schema`](#source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-schema) | string | **yes** | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_catalog`](#source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-catalog) | string | no | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_key`](#source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-key) | string | no | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_schema`](#source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-schema) | string | no | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.type`](#source-configsource-zip-handlingpre-extraction-decryptiontype) | string (enum) | no | — |
| [`source_config.source_zip_handling.source_zip_path`](#source-configsource-zip-handlingsource-zip-path) | string | **yes** | — |
| [`source_config.source_zip_handling.target_volume_path`](#source-configsource-zip-handlingtarget-volume-path) | string | **yes** | — |
| [`source_config.source_zip_handling.zip_file_pattern`](#source-configsource-zip-handlingzip-file-pattern) | string | **yes** | — |
| [`source_config.starting_version`](#source-configstarting-version) | integer | no | — |
| [`source_database`](#source-database) | string | no | — |
| [`source_description`](#source-description) | string (SQL) | no | — |
| [`source_system`](#source-system) | string | no | — |
| [`source_table_name`](#source-table-name) | string | no | — |
| [`source_type`](#source-type) | string (enum) | **yes** | — |
| [`target_catalog`](#target-catalog) | string | **yes** | — |
| [`target_config.auto_ttl.expire_in_days`](#target-configauto-ttlexpire-in-days) | integer | no | — |
| [`target_config.auto_ttl.timestamp_column`](#target-configauto-ttltimestamp-column) | string | no | — |
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
| [`target_config.sink_config.post_export_archive.enabled`](#target-configsink-configpost-export-archiveenabled) | boolean | no | — |
| [`target_config.sink_config.post_export_archive.export_file_name_format`](#target-configsink-configpost-export-archiveexport-file-name-format) | string | no | — |
| [`target_config.sink_config.post_export_archive.output_zip_path`](#target-configsink-configpost-export-archiveoutput-zip-path) | string | **yes** | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.enabled`](#target-configsink-configpost-export-archivepgp-encryptionenabled) | boolean | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog) | string | **yes** | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key) | string | **yes** | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema) | string | **yes** | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema) | string | no | — |
| [`target_config.sink_config.post_export_archive.secret.secret_catalog`](#target-configsink-configpost-export-archivesecretsecret-catalog) | string | no | — |
| [`target_config.sink_config.post_export_archive.secret.secret_key`](#target-configsink-configpost-export-archivesecretsecret-key) | string | no | — |
| [`target_config.sink_config.post_export_archive.secret.secret_schema`](#target-configsink-configpost-export-archivesecretsecret-schema) | string | no | — |
| [`target_config.sink_config.write_mode`](#target-configsink-configwrite-mode) | string (enum) | no | — |
| [`target_config.storage_format`](#target-configstorage-format) | string (enum) | no | — |
| [`target_config.table_properties`](#target-configtable-properties) | object<string,string> | no | — |
| [`target_schema`](#target-schema) | string | **yes** | — |
| [`target_table`](#target-table) | string | **yes** | — |
| [`target_type`](#target-type) | string (enum) | **yes** | — |

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

### `source_config.asn1_codec` { #source-configasn1-codec }

Which ASN.1 encoding to decode.


**Type** `string (enum)` · **Required** yes · **Section** Source · ASN.1


```json
{
  "source_config": {
    "asn1_codec": "ber"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: ber, der.


---

### `source_config.asn1_pdu_name` { #source-configasn1-pdu-name }

The top-level SEQUENCE type in the .asn file to decode each record as.


**Type** `string` · **Required** yes · **Section** Source · ASN.1


```json
{
  "source_config": {
    "asn1_pdu_name": "CallDetailRecord"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_config.asn1_schema_path` { #source-configasn1-schema-path }

ASN.1 module definition file.


ASN.1 module definition file. Must be a real .asn file.


**Type** `string` · **Required** yes · **Section** Source · ASN.1


```json
{
  "source_config": {
    "asn1_schema_path": "/Volumes/{{catalog}}/landing/_asn1_schemas/cdr.asn"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_config.auto_flatten_all` { #source-configauto-flatten-all }

Recursively flattens all nested structs and explodes all arrays regardless of explode_columns — the same effect as an explicitly-empty explode_columns.


Recursively flattens all nested structs and explodes all arrays regardless of explode_columns — the same effect as an explicitly-empty explode_columns. Only takes effect while explode_columns is absent or empty.


**Type** `boolean` · **Required** no · **Section** Source · nested data & standardization


```json
{
  "source_config": {
    "auto_flatten_all": true
  }
}
```


!!! tip "Best practice"

    - same effect as the empty explode_columns mode
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `source_config.capture_technical_metadata` { #source-configcapture-technical-metadata }

Adds the framework's file-provenance columns and gates the ingestion timestamp.


Populates __framework_source_file_name, _size, _modification_time and _metadata_headers, and switches on __framework_ingestion_timestamp_utc.


**Type** `boolean` · **Required** no · **Section** Source · reader options


```json
"capture_technical_metadata": true
```


!!! tip "Best practice"

    - Leave it on unless you have a strong column-count reason not to — it is the only record of which file a row came from.


!!! warning "Known errors and limitations"

    **CDC pipeline fails at startup complaining about sequence_by_column**  
    *Cause:* This was set false, so __framework_ingestion_timestamp_utc does not exist, and the CDC default sequencing column is missing.  
    *Fix:* Either re-enable this, or set target_config.sequence_by_column to a real column explicitly.


---

### `source_config.column_normalization.case` { #source-configcolumn-normalizationcase }

Case applied to normalized column names.


Case applied to normalized column names. lower is the default; preserve keeps the source casing; upper uppercases.


**Type** `string (enum)` · **Required** no · **Section** Source · column normalization


```json
{
  "source_config": {
    "column_normalization": {
      "case": "lower"
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: lower, preserve, upper.


---

### `source_config.column_normalization.enabled` { #source-configcolumn-normalizationenabled }

Switch for column normalization.


Switch for column normalization. When off, source column names are passed through unchanged.


**Type** `boolean` · **Required** no · **Section** Source · column normalization


```json
{
  "source_config": {
    "column_normalization": {
      "enabled": true
    }
  }
}
```


!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.


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

### `source_config.dedup_watermark.delay_threshold` { #source-configdedup-watermarkdelay-threshold }

Spark interval string paired with dedup_watermark.event_time_column.


**Type** `string` · **Required** no · **Section** Source · reader options


```json
{
  "source_config": {
    "dedup_watermark": {
      "delay_threshold": "2 hours"
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `source_config.dedup_watermark.event_time_column` { #source-configdedup-watermarkevent-time-column }

Bounds dedup state on a streaming source.


Bounds dedup state on a streaming source. A TIMESTAMP column on the ingested DataFrame. Only meaningful with remove_dups: true.


**Type** `string` · **Required** no · **Section** Source · reader options


```json
{
  "source_config": {
    "dedup_watermark": {
      "event_time_column": "updated_at"
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `source_config.explode_columns` { #source-configexplode-columns }

Only the named top-level columns are processed.


Only the named top-level columns are processed. A column that is neither struct nor array at runtime is an error.


**Type** `array<string>` · **Required** no · **Section** Source · nested data & standardization


```json
{
  "source_config": {
    "explode_columns": [
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

### `source_config.explode_mode` { #source-configexplode-mode }

How explode_columns is written to the spec.


How explode_columns is written to the spec. absent omits the key entirely — schema-preserving pass-through. empty writes explode_columns: [] — auto-flattens every nested struct and explodes every array in the schema. named writes the columns you list below.


**Type** `string (enum)` · **Required** no · **Section** Source · nested data & standardization


```json
{
  "source_config": {
    "explode_mode": "absent"
  }
}
```


!!! tip "Best practice"

    - the absent / explicitly-empty distinction is load-bearing
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: absent, empty, named.


---

### `source_config.file_pattern` { #source-configfile-pattern }

Glob or regex filtering which files are picked up.


Glob or regex filtering which files are picked up. Maps to cloudFiles.fileNamePattern.


**Type** `string` · **Required** no · **Section** Source · reader options


```json
{
  "source_config": {
    "file_pattern": "orc_*"
  }
}
```


---

### `source_config.format` { #source-configformat }

How Auto Loader parses each file it picks up.


Chooses the reader and therefore which other source_config options apply.


**Type** `string (enum)` · **Required** yes · **Section** Source · autoloader


```json
"format": "csv",
"reader_options": { "header": "true", "delimiter": "," }
```


!!! tip "Best practice"

    - csv and json need reader_options to behave predictably; parquet carries its own schema.
    - For json, decide explode behaviour deliberately — an absent explode_columns preserves nesting.


!!! warning "Known errors and limitations"

    **Every column arrives as a string**  
    *Cause:* cloudFiles.inferColumnTypes is not set for csv.  
    *Fix:* Add "cloudFiles.inferColumnTypes": "true" to reader_options, or declare a schema_config.


**Databricks documentation:** [auto loader options](https://docs.databricks.com/ingestion/auto-loader/options.html)


---

### `source_config.json_string_columns` { #source-configjson-string-columns }

STRING columns holding a JSON document, parsed with from_json / schema_of_json before flattening — giving Parquet sources parity with JSON.


STRING columns holding a JSON document, parsed with from_json / schema_of_json before flattening — giving Parquet sources parity with JSON. Each entry is either a column name or an object of the form {"column": "col", "schema_ddl": "struct<...>"}.


**Type** `array<string>` · **Required** no · **Section** Source · nested data & standardization


```json
{
  "source_config": {
    "json_string_columns": [
      "column_a",
      "column_b"
    ]
  }
}
```


!!! tip "Best practice"

    - name, or {column, schema_ddl} for an explicit schema
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.


---

### `source_config.landing_retention_policy.archive_path` { #source-configlanding-retention-policyarchive-path }

Where archived files are moved.


Where archived files are moved. Maps to cloudFiles.cleanSource.moveDestination. No longer a hard requirement: archive with a missing or empty archive_path degrades to off with a runtime warning. delete never needs it.


**Type** `string` · **Required** no · **Section** Source · landing retention


```json
{
  "source_config": {
    "landing_retention_policy": {
      "archive_path": "/Volumes/{{catalog}}/landing/_archive/zone/"
    }
  }
}
```


!!! tip "Best practice"

    - missing path degrades archive to off — not a validation error
    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `source_config.landing_retention_policy.clean_source` { #source-configlanding-retention-policyclean-source }

off leaves files in place, archive moves them, delete removes them.


**Type** `string (enum)` · **Required** no · **Section** Source · landing retention


```json
{
  "source_config": {
    "landing_retention_policy": {
      "clean_source": "off"
    }
  }
}
```


!!! tip "Best practice"

    - Allowed values: off, archive, delete.


---

### `source_config.landing_retention_policy.retention_days` { #source-configlanding-retention-policyretention-days }

Maps to cloudFiles.cleanSource.retentionDuration as N days.


Maps to cloudFiles.cleanSource.retentionDuration as N days. Minimum 0; defaults to 7 when omitted.


**Type** `integer` · **Required** no · **Section** Source · landing retention


```json
{
  "source_config": {
    "landing_retention_policy": {
      "retention_days": 7
    }
  }
}
```


!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.


---

### `source_config.max_bytes_per_trigger` { #source-configmax-bytes-per-trigger }

Throttles how much data each micro-batch reads.


Throttles how much data each micro-batch reads. Maps to maxBytesPerTrigger.


**Type** `string` · **Required** no · **Section** Source · zerobus


```json
{
  "source_config": {
    "max_bytes_per_trigger": "1g"
  }
}
```


**Databricks documentation:** [structured streaming](https://docs.databricks.com/structured-streaming/index.html)


---

### `source_config.path` { #source-configpath }

The Unity Catalog Volume directory Auto Loader watches for new files.


Auto Loader lists and streams from this directory; it is the physical entry point of the flow.


**Type** `string` · **Required** yes · **Section** Source · autoloader


```json
"path": "/Volumes/{{catalog}}/landing/orders/{{env}}/incoming/"
```


!!! tip "Best practice"

    - Always end with a trailing slash — a path that looks like a file makes Auto Loader watch the parent directory.
    - Point at a dedicated incoming/ directory. Mixing archived and incoming files in one directory makes retention rules dangerous.
    - When source_zip_handling is enabled this must be the extraction target, not the directory holding the archives.


!!! warning "Known errors and limitations"

    **Update succeeds and reports SUCCESS but writes zero rows**  
    *Cause:* The path does not match where the files actually land — most often source_zip_handling.target_volume_path points somewhere else.  
    *Fix:* Confirm the extraction target and this path are the same directory. The onboarding validator does not cross-check them.

    **PERMISSION_DENIED on the Volume**  
    *Cause:* The pipeline's run-as identity lacks READ VOLUME.  
    *Fix:* GRANT READ VOLUME ON VOLUME <catalog>.<schema>.<volume> TO <principal>.


**Databricks documentation:** [auto loader](https://docs.databricks.com/ingestion/auto-loader/index.html) · [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `source_config.reader_options` { #source-configreader-options }

Passthrough to the Spark reader — one .option(key, value) per entry.


Passthrough to the Spark reader — one .option(key, value) per entry. Can override file_pattern and schema_evolution_mode if keys collide.


**Type** `object<string,string>` · **Required** no · **Section** Source · reader options


```json
{
  "source_config": {
    "reader_options": {
      "option_name": "value"
    }
  }
}
```


!!! tip "Best practice"

    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


**Databricks documentation:** [auto loader options](https://docs.databricks.com/ingestion/auto-loader/options.html)


---

### `source_config.remove_dups` { #source-configremove-dups }

Full-row dropDuplicates over every column except __framework_*-prefixed columns and _rescued_data / _metadata.


Full-row dropDuplicates over every column except __framework_*-prefixed columns and _rescued_data / _metadata. Without a watermark this holds unbounded dedup state.


**Type** `boolean` · **Required** no · **Section** Source · reader options


```json
{
  "source_config": {
    "remove_dups": true
  }
}
```


!!! tip "Best practice"

    - set the watermark below to bound state
    - Omitting the attribute is not the same as setting it false — check the default above.


---

### `source_config.schema_config_path` { #source-configschema-config-path }

External JSON/YAML declaring explicit casts, nullability, UC column comments and renames.


External JSON/YAML declaring explicit casts, nullability, UC column comments and renames. A directory resolves to its most recently modified file.


**Type** `string` · **Required** no · **Section** Source · reader options


```json
{
  "source_config": {
    "schema_config_path": "/Volumes/.../schema_config.json"
  }
}
```


---

### `source_config.schema_evolution_mode` { #source-configschema-evolution-mode }

Maps to cloudFiles.schemaEvolutionMode.


Maps to cloudFiles.schemaEvolutionMode. rescue sends new or mismatched columns to _rescued_data.


**Type** `string (enum)` · **Required** no · **Section** Source · reader options


```json
{
  "source_config": {
    "schema_evolution_mode": "addNewColumns"
  }
}
```


!!! tip "Best practice"

    - Allowed values: addNewColumns, addNewColumnsWithTypeWidening, rescue, failOnNewColumns, none.


**Databricks documentation:** [auto loader schema](https://docs.databricks.com/ingestion/auto-loader/schema.html)


---

### `source_config.schema_location` { #source-configschema-location }

Where Auto Loader persists the inferred schema and its evolution history.


Without a durable schema location, Auto Loader cannot detect that a column is new, so schema evolution and the rescue column stop working.


**Type** `string` · **Required** no · **Section** Source · autoloader


```json
"schema_location": "/Volumes/{{catalog}}/landing/_schemas/orders/"
```


!!! tip "Best practice"

    - Give every flow its own schema location. Sharing one between flows corrupts both schemas.
    - Never point it inside the directory being ingested — the checkpoint files become input files.


!!! warning "Known errors and limitations"

    **Pipeline reprocesses everything after a redeploy**  
    *Cause:* The schema location moved, so the stream lost its checkpoint identity.  
    *Fix:* Restore the original path, or accept a one-time full reprocess.


**Databricks documentation:** [auto loader schema](https://docs.databricks.com/ingestion/auto-loader/schema.html)


---

### `source_config.source_catalog` { #source-configsource-catalog }

Catalog of the existing Delta table to stream from.


**Type** `string` · **Required** yes · **Section** Source · zerobus


```json
{
  "source_config": {
    "source_catalog": "example_source_catalog"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_config.source_schema` { #source-configsource-schema }

Schema of the source table.


**Type** `string` · **Required** yes · **Section** Source · zerobus


```json
{
  "source_config": {
    "source_schema": "example_source_schema"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_config.source_table` { #source-configsource-table }

Table name of the source.


**Type** `string` · **Required** yes · **Section** Source · zerobus


```json
{
  "source_config": {
    "source_table": "example_source_zerobus_table"
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.


---

### `source_config.source_zip_handling.delete_source_after_extract.action` { #source-configsource-zip-handlingdelete-source-after-extractaction }

What happens to the ZIP after a successful extraction.


What happens to the ZIP after a successful extraction. delete_now removes the archive as soon as its members are extracted; delete_after_x_days runs an age-based sweep of the landing directory, skipping only archives whose extraction failed in the same run. Leaving this unset keeps the archive.


**Type** `string (enum)` · **Required** no · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "delete_source_after_extract": {
        "action": "delete_now"
      }
    }
  }
}
```


!!! tip "Best practice"

    - unset keeps the archive
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: delete_now, delete_after_x_days.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.delete_source_after_extract.days` { #source-configsource-zip-handlingdelete-source-after-extractdays }

Age in days after which a successfully-extracted archive is swept.


**Type** `integer` · **Required** yes · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "delete_source_after_extract": {
        "days": 7
      }
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.enabled` { #source-configsource-zip-handlingenabled }

Master switch for ZIP extraction.


**Type** `boolean` · **Required** yes · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "enabled": true
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Omitting the attribute is not the same as setting it false — check the default above.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_catalog` { #source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-catalog }

Passphrase protecting the PGP PRIVATE KEY above.


Passphrase protecting the PGP PRIVATE KEY above. Optional — only when the key itself is passphrase-protected. Not the ZIP's password: that is secret_passphrase, below.


**Type** `string` · **Required** no · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "passphrase_secret": {
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

### `source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_key` { #source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-key }

Passphrase protecting the PGP PRIVATE KEY above.


Passphrase protecting the PGP PRIVATE KEY above. Optional — only when the key itself is passphrase-protected. Not the ZIP's password: that is secret_passphrase, below.


**Type** `string` · **Required** no · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "passphrase_secret": {
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

### `source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_schema` { #source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-schema }

Passphrase protecting the PGP PRIVATE KEY above.


Passphrase protecting the PGP PRIVATE KEY above. Optional — only when the key itself is passphrase-protected. Not the ZIP's password: that is secret_passphrase, below.


**Type** `string` · **Required** no · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "passphrase_secret": {
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

### `source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_catalog` { #source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** yes · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "private_key_secret": {
          "secret_catalog": "{{catalog}}"
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_key` { #source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** yes · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "private_key_secret": {
          "secret_key": "pii_encryption_key"
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_schema` { #source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** yes · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "private_key_secret": {
          "secret_schema": "security"
        }
      }
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_catalog` { #source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-catalog }

AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time.


AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time. Independent of, and combinable with, the PGP layer above. Not the PGP key's passphrase: that is passphrase_secret.


**Type** `string` · **Required** no · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "secret_passphrase": {
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

### `source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_key` { #source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-key }

AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time.


AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time. Independent of, and combinable with, the PGP layer above. Not the PGP key's passphrase: that is passphrase_secret.


**Type** `string` · **Required** no · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "secret_passphrase": {
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

### `source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_schema` { #source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-schema }

AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time.


AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time. Independent of, and combinable with, the PGP layer above. Not the PGP key's passphrase: that is passphrase_secret.


**Type** `string` · **Required** no · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "pre_extraction_decryption": {
        "secret_passphrase": {
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

### `source_config.source_zip_handling.pre_extraction_decryption.type` { #source-configsource-zip-handlingpre-extraction-decryptiontype }

Decryption applied to the archive before it is unzipped.


Encrypted archives must be decrypted as a distinct outer layer before any member can be read.


**Type** `string (enum)` · **Required** no · **Section** Source · ZIP handling


```json
"pre_extraction_decryption": {
  "type": "pgp",
  "private_key_secret": { "secret_catalog": "{{catalog}}", "secret_schema": "security", "secret_key": "pgp_private_key" }
}
```


!!! tip "Best practice"

    - Only pgp is supported.
    - Choosing a type is what reveals the secret fields below — they stay hidden until then.
    - Secrets are referenced by catalog/schema/key. Never paste key material into the spec.


!!! warning "Known errors and limitations"

    **Decryption fails with no obvious cause**  
    *Cause:* The passphrase secret is missing while the private key requires one.  
    *Fix:* Populate secret_passphrase as well as private_key_secret.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html)


---

### `source_config.source_zip_handling.source_zip_path` { #source-configsource-zip-handlingsource-zip-path }

Directory where ZIP files are found — never a single file.


**Type** `string` · **Required** yes · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "source_zip_path": "/Volumes/{{catalog}}/landing/zone/incoming/"
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.target_volume_path` { #source-configsource-zip-handlingtarget-volume-path }

The directory archive members are extracted into before ingestion reads them.


Extraction and ingestion are two separate steps; this is the handover point between them.


**Type** `string` · **Required** yes · **Section** Source · ZIP handling


```json
"source_zip_handling": {
  "enabled": true,
  "source_zip_path": "/Volumes/{{catalog}}/landing/cdr/incoming/",
  "target_volume_path": "/Volumes/{{catalog}}/landing/cdr/extracted/"
}
// source_config.path must equal target_volume_path
```


!!! tip "Best practice"

    - This must match source_config.path exactly. Nothing validates that they agree.


!!! warning "Known errors and limitations"

    **Extraction succeeds, ingestion reads zero rows, update reports SUCCESS**  
    *Cause:* This path and source_config.path point at different directories, so the reader watches an empty directory.  
    *Fix:* Make the two identical. This is the single most common silent misconfiguration in ZIP flows.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `source_config.source_zip_handling.zip_file_pattern` { #source-configsource-zip-handlingzip-file-pattern }

Which ZIP files to pick up.


**Type** `string` · **Required** yes · **Section** Source · ZIP handling


```json
{
  "source_config": {
    "source_zip_handling": {
      "zip_file_pattern": "*.zip"
    }
  }
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.starting_version` { #source-configstarting-version }

Maps to reader option startingVersion.


**Type** `integer` · **Required** no · **Section** Source · zerobus


```json
{
  "source_config": {
    "starting_version": 0
  }
}
```


**Databricks documentation:** [structured streaming](https://docs.databricks.com/structured-streaming/index.html) · [delta change data feed](https://docs.databricks.com/delta/delta-change-data-feed.html)


---

### `source_database` { #source-database }

Descriptive metadata.


**Type** `string` · **Required** no · **Section** Flow identity


```json
{
  "source_database": "example_landing_db"
}
```


---

### `source_description` { #source-description }

Becomes the target Delta table's COMMENT.


Becomes the target Delta table's COMMENT. Supports {{catalog}} and {{env}}.


**Type** `string (SQL)` · **Required** no · **Section** Flow identity


```json
{
  "source_description": "Bronze ingestion from a Volume in the {{env}} environment"
}
```


!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.


---

### `source_system` { #source-system }

Descriptive metadata — never validated.


**Type** `string` · **Required** no · **Section** Flow identity


```json
{
  "source_system": "example_source_system"
}
```


---

### `source_table_name` { #source-table-name }

Descriptive metadata.


**Type** `string` · **Required** no · **Section** Flow identity


```json
{
  "source_table_name": "example_raw_table"
}
```


---

### `source_type` { #source-type }

autoloader reads files from a Volume, zerobus streams an existing Delta table, asn1 decodes binary CDR files.


**Type** `string (enum)` · **Required** yes · **Section** Source type


```json
{
  "source_type": "autoloader"
}
```


!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: autoloader, zerobus, asn1.


**Databricks documentation:** [auto loader](https://docs.databricks.com/ingestion/auto-loader/index.html)


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

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog }

Unity Catalog secret catalog holding the key.


**Type** `string` · **Required** yes · **Section** Target · sink config


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

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key }

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type** `string` · **Required** yes · **Section** Target · sink config


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

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema }

Unity Catalog secret schema.


**Type** `string` · **Required** yes · **Section** Target · sink config


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

    - Required — onboarding rejects the flow if this is missing.
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
