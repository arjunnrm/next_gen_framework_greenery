<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Ingestion flows

One entry per `ingestion_flows[]` element — reading from a landing zone into Bronze.


!!! info "110 attributes · 454 FAQs"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form. Badges: <span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Flow kind</span> <span class="fx-badge fx-ver">vX.Y+ added in</span> <span class="fx-badge fx-only">Spec Builder section</span>.


**Recipes and deep dive:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Schema tree](tree.md) · [Removed & rejected](removed.md)


## Summary

| Attribute | Type | Required | Default | Since |
|---|---|---|---|---|
| [`dataflow_id`](#dataflow-id) | string | **yes** | — | — |
| [`dq_config.quarantine_table`](#dq-configquarantine-table) | string | no | — | — |
| [`dq_config.record_id_column`](#dq-configrecord-id-column) | string | no | — | — |
| [`dq_config.rules`](#dq-configrules) | array<object> | no | — | — |
| [`dq_config.rules[].action`](#dq-configrulesaction) | string (enum) | **yes** | — | — |
| [`dq_config.rules[].expression`](#dq-configrulesexpression) | string (SQL) | **yes** | — | — |
| [`dq_config.rules[].rule_id`](#dq-configrulesrule-id) | string | **yes** | — | — |
| [`governance_tags.column_tags`](#governance-tagscolumn-tags) | array<object> | no | — | — |
| [`governance_tags.column_tags[].column`](#governance-tagscolumn-tagscolumn) | string | **yes** | — | — |
| [`governance_tags.column_tags[].tags`](#governance-tagscolumn-tagstags) | object<string,string> | **yes** | — | — |
| [`governance_tags.table_tags`](#governance-tagstable-tags) | object<string,string> | no | — | — |
| [`source_config.asn1_codec`](#source-configasn1-codec) | string (enum) | **yes** | — | — |
| [`source_config.asn1_pdu_name`](#source-configasn1-pdu-name) | string | no | — | — |
| [`source_config.asn1_schema_path`](#source-configasn1-schema-path) | string | **yes** | — | — |
| [`source_config.auto_flatten_all`](#source-configauto-flatten-all) | boolean | no | `false` | — |
| [`source_config.capture_technical_metadata`](#source-configcapture-technical-metadata) | boolean | no | — | — |
| [`source_config.column_normalization.case`](#source-configcolumn-normalizationcase) | string (enum) | no | `"lower"` | — |
| [`source_config.column_normalization.enabled`](#source-configcolumn-normalizationenabled) | boolean | no | `false` | — |
| [`source_config.data_standardization_sql`](#source-configdata-standardization-sql) | array<string> | no | — | — |
| [`source_config.dedup_watermark.delay_threshold`](#source-configdedup-watermarkdelay-threshold) | string | no | — | — |
| [`source_config.dedup_watermark.event_time_column`](#source-configdedup-watermarkevent-time-column) | string | no | — | — |
| [`source_config.explode_columns`](#source-configexplode-columns) | array<string> | no | — | — |
| [`source_config.explode_mode`](#source-configexplode-mode) | string (enum) | no | — | — |
| [`source_config.file_pattern`](#source-configfile-pattern) | string | no | — | — |
| [`source_config.format`](#source-configformat) | string (enum) | **yes** | — | — |
| [`source_config.json_string_columns`](#source-configjson-string-columns) | array<string> | no | — | — |
| [`source_config.landing_retention_policy.archive_path`](#source-configlanding-retention-policyarchive-path) | string | no | — | — |
| [`source_config.landing_retention_policy.clean_source`](#source-configlanding-retention-policyclean-source) | string (enum) | no | `"off"` | — |
| [`source_config.landing_retention_policy.retention_days`](#source-configlanding-retention-policyretention-days) | integer | no | `7` | — |
| [`source_config.max_bytes_per_trigger`](#source-configmax-bytes-per-trigger) | string | no | — | — |
| [`source_config.path`](#source-configpath) | string | **yes** | — | — |
| [`source_config.reader_options`](#source-configreader-options) | object<string,string> | no | — | — |
| [`source_config.remove_dups`](#source-configremove-dups) | boolean | no | `false` | — |
| [`source_config.schema_config_path`](#source-configschema-config-path) | string | no | — | — |
| [`source_config.schema_evolution_mode`](#source-configschema-evolution-mode) | string (enum) | no | — | — |
| [`source_config.schema_location`](#source-configschema-location) | string | no | — | — |
| [`source_config.source_catalog`](#source-configsource-catalog) | string | **yes** | — | — |
| [`source_config.source_schema`](#source-configsource-schema) | string | **yes** | — | — |
| [`source_config.source_table`](#source-configsource-table) | string | **yes** | — | — |
| [`source_config.source_zip_handling.delete_source_after_extract.action`](#source-configsource-zip-handlingdelete-source-after-extractaction) | string (enum) | no | — | — |
| [`source_config.source_zip_handling.delete_source_after_extract.days`](#source-configsource-zip-handlingdelete-source-after-extractdays) | integer | **yes** | — | — |
| [`source_config.source_zip_handling.enabled`](#source-configsource-zip-handlingenabled) | boolean | **yes** | — | — |
| [`source_config.source_zip_handling.member_format`](#source-configsource-zip-handlingmember-format) | string (enum) | no | — | v1.7.4 |
| [`source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_catalog`](#source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-catalog) | string | no | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_key`](#source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-key) | string | no | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_schema`](#source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-schema) | string | no | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_catalog`](#source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-catalog) | string | **yes** | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_key`](#source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-key) | string | **yes** | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_schema`](#source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-schema) | string | **yes** | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_catalog`](#source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-catalog) | string | no | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_key`](#source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-key) | string | no | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_schema`](#source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-schema) | string | no | — | — |
| [`source_config.source_zip_handling.pre_extraction_decryption.type`](#source-configsource-zip-handlingpre-extraction-decryptiontype) | string (enum) | no | — | v1.7.4 |
| [`source_config.source_zip_handling.source_zip_path`](#source-configsource-zip-handlingsource-zip-path) | string | **yes** | — | — |
| [`source_config.source_zip_handling.target_volume_path`](#source-configsource-zip-handlingtarget-volume-path) | string | **yes** | — | — |
| [`source_config.source_zip_handling.zip_file_pattern`](#source-configsource-zip-handlingzip-file-pattern) | string | **yes** | — | — |
| [`source_config.starting_version`](#source-configstarting-version) | integer | no | — | — |
| [`source_database`](#source-database) | string | no | — | — |
| [`source_description`](#source-description) | string (SQL) | no | — | — |
| [`source_system`](#source-system) | string | no | — | — |
| [`source_table_name`](#source-table-name) | string | no | — | — |
| [`source_type`](#source-type) | string (enum) | **yes** | — | — |
| [`target_catalog`](#target-catalog) | string | **yes** | — | — |
| [`target_config.auto_ttl.expire_in_days`](#target-configauto-ttlexpire-in-days) | integer | no | — | — |
| [`target_config.auto_ttl.timestamp_column`](#target-configauto-ttltimestamp-column) | string | no | — | — |
| [`target_config.encrypted_columns`](#target-configencrypted-columns) | array<object> | no | — | — |
| [`target_config.encrypted_columns[].column_name`](#target-configencrypted-columnscolumn-name) | string | **yes** | — | — |
| [`target_config.encrypted_columns[].mode`](#target-configencrypted-columnsmode) | string (enum) | no | — | — |
| [`target_config.encrypted_columns[].output_column`](#target-configencrypted-columnsoutput-column) | string | no | — | — |
| [`target_config.encrypted_columns[].secret.secret_catalog`](#target-configencrypted-columnssecretsecret-catalog) | string | **yes** | — | — |
| [`target_config.encrypted_columns[].secret.secret_key`](#target-configencrypted-columnssecretsecret-key) | string | **yes** | — | — |
| [`target_config.encrypted_columns[].secret.secret_schema`](#target-configencrypted-columnssecretsecret-schema) | string | **yes** | — | — |
| [`target_config.encrypted_columns[].source_data_type`](#target-configencrypted-columnssource-data-type) | string | no | — | v1.4.0 |
| [`target_config.liquid_clustering_columns`](#target-configliquid-clustering-columns) | array<string> | no | — | — |
| [`target_config.partition_columns`](#target-configpartition-columns) | array<string> | no | — | — |
| [`target_config.partition_mode`](#target-configpartition-mode) | string (enum) | no | — | — |
| [`target_config.sink_config.export_trigger`](#target-configsink-configexport-trigger) | string (enum) | no | — | v1.7.5 |
| [`target_config.sink_config.format`](#target-configsink-configformat) | string (enum) | **yes** | — | — |
| [`target_config.sink_config.kafka_options`](#target-configsink-configkafka-options) | object<string,string> | **yes** | — | — |
| [`target_config.sink_config.path`](#target-configsink-configpath) | string | **yes** | — | — |
| [`target_config.sink_config.post_export_archive.archive_format`](#target-configsink-configpost-export-archivearchive-format) | string (enum) | no | — | v1.7.4 |
| [`target_config.sink_config.post_export_archive.enabled`](#target-configsink-configpost-export-archiveenabled) | boolean | no | — | — |
| [`target_config.sink_config.post_export_archive.export_file_name_format`](#target-configsink-configpost-export-archiveexport-file-name-format) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.output_zip_path`](#target-configsink-configpost-export-archiveoutput-zip-path) | string | **yes** | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.enabled`](#target-configsink-configpost-export-archivepgp-encryptionenabled) | boolean | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-catalog) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-key) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-schema) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-key) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-key) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema`](#target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.secret.secret_catalog`](#target-configsink-configpost-export-archivesecretsecret-catalog) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.secret.secret_key`](#target-configsink-configpost-export-archivesecretsecret-key) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.secret.secret_schema`](#target-configsink-configpost-export-archivesecretsecret-schema) | string | no | — | — |
| [`target_config.sink_config.staged_file_format`](#target-configsink-configstaged-file-format) | string (enum) | no | — | v1.6.0 |
| [`target_config.sink_config.staged_file_options.delimiter`](#target-configsink-configstaged-file-optionsdelimiter) | string | no | — | — |
| [`target_config.sink_config.staged_file_options.include_header`](#target-configsink-configstaged-file-optionsinclude-header) | boolean | no | — | — |
| [`target_config.sink_config.staged_file_options.line_terminator`](#target-configsink-configstaged-file-optionsline-terminator) | string (enum) | no | — | — |
| [`target_config.sink_config.write_mode`](#target-configsink-configwrite-mode) | string (enum) | no | — | — |
| [`target_config.storage_format`](#target-configstorage-format) | string (enum) | no | — | — |
| [`target_config.table_properties`](#target-configtable-properties) | object<string,string> | no | — | — |
| [`target_schema`](#target-schema) | string | **yes** | — | — |
| [`target_table`](#target-table) | string | **yes** | — | — |
| [`target_type`](#target-type) | string (enum) | **yes** | — | — |

## Attributes

### `dataflow_id` { #dataflow-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Flow identity</span>

Unique ID for this ingestion flow.


Unique ID for this ingestion flow. Referenced by transformation flows.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.dataflow_id`


=== "JSON"

    ```json
    {
      "dataflow_id": "df_template_ingest"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- dataflow_id is persisted as its own column
    SELECT dataflow_id,
           dataflow_id,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if I forget dataflow_id on an ingestion flow?"

    Onboarding rejects the flow with 'is required but was missing or empty' against `ingestion_flow[<unknown>].dataflow_id`. The flow label itself falls back to `<missing dataflow_id>` in the error, so fix the first flow listed first.

??? question "Format gotcha · Can dataflow_id contain spaces or dots like a table name?"

    It is validated only as a non-empty string; there is no pattern restriction in `spec_validator.py`. Keep it identifier-like anyway since it is used verbatim to build the source-plane consumer id `f"{dataflow_id}:source"`.

??? question "Performance impact · Does the length or format of dataflow_id affect pipeline runtime?"

    Negligible. It is only used as a lookup key and DAG-node/consumer-id string, never used in a data-shaping expression or scan.

??? question "Edge case · How does a transformation flow actually reference an ingestion flow's dataflow_id?"

    A transformation flow reads it via `source_inputs[].table` (the qualified target the ingestion flow published), not by naming `dataflow_id` directly in the referencing flow. `dataflow_id` is required on both ingestion and transformation flows, but transformation flows also carry their own `flow_step_id`. See docs/00_master_reference_index.md.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-dataflow-id) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `dq_config.quarantine_table` { #dq-configquarantine-table }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Data quality</span>

Quarantine table name.


Quarantine table name. Only created when at least one rule uses action quarantine.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.dq_config_json`


=== "JSON"

    ```json
    {
      "dq_config": {
        "quarantine_table": "<target_table>_quarantine"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- dq_config.quarantine_table lives inside the dq_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if I don't set dq_config.quarantine_table at all?"

    No quarantine table is registered. If a rule still uses `action: "quarantine"`, those rows have nowhere to go; the validator does not cross-check this today, so the failure surfaces only at pipeline registration/runtime, not at onboarding.

??? question "Format gotcha · Is quarantine_table a full three-part name or just a suffix?"

    It is a plain string table name, validated only as a non-empty string by `check_string`. The sample convention is `<target_table>_quarantine`; it is not auto-derived, so you must spell it out yourself.

??? question "Performance impact · Does naming a quarantine table cost anything if no rule ever quarantines a row?"

    Negligible. `_validate_dq_config` in `spec_validator.py` explicitly notes a `quarantine_table` name with no `action: "quarantine"` rule is accepted, not an error — it just produces no table, flagged only as a `logger.warning`-worthy no-op.

??? question "Edge case · I set quarantine_table but rows still aren't landing anywhere — why?"

    The quarantine sibling table is only registered when at least one rule in `dq_config.rules` has `action: "quarantine"` (see `dq/quarantine.py::register_main_and_quarantine_tables`). A `warn`/`drop`/`fail`-only rule set means `quarantine_table` is configured but inert.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configquarantine-table) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.record_id_column` { #dq-configrecord-id-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Data quality</span>

Surfaced as __framework_record_id on quarantined rows for traceability.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.dq_config_json`


=== "JSON"

    ```json
    {
      "dq_config": {
        "record_id_column": "example_id"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- dq_config.record_id_column lives inside the dq_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What is written to __framework_record_id if record_id_column is not set?"

    `NULL` for every quarantined row. `dq/quarantine.py` fills `__framework_record_id` from `record_id_column` when configured and present on the DataFrame, else `NULL` — it never errors for its absence.

??? question "Format gotcha · Does record_id_column need to be a unique/primary key column?"

    No — it is just a string column name, validated only as a non-empty string (`check_string`). It is meant for traceability, not uniqueness enforcement, so any column that helps you find the source record is fine.

??? question "Performance impact · Does adding record_id_column slow down quarantine processing?"

    Negligible — it is a single extra column lookup added alongside the other diagnostic fields (`__framework_quarantine_timestamp_utc`, failed-rule ids) already being written to quarantined rows.

??? question "Edge case · What if the column named in record_id_column doesn't exist on the incoming DataFrame?"

    It resolves to `NULL` on `__framework_record_id` rather than failing — `dq/quarantine.py` only populates the value "when configured and present on the DataFrame", so a typo'd column name degrades silently rather than erroring.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrecord-id-column) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules` { #dq-configrules }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Data-quality expectations evaluated on every row.


Each rule becomes a pipeline expectation; the action decides what happens to a failing row.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.dq_config_json`


=== "JSON"

    ```json
    "rules": [
      { "rule_id": "order_id_not_null", "expression": "order_id IS NOT NULL", "action": "drop" },
      { "rule_id": "amount_non_negative", "expression": "amount >= 0", "action": "quarantine" }
    ]
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- dq_config.rules lives inside the dq_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
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

**FAQs** (5)

??? question "If omitted · What happens if dq_config.rules is left empty or omitted?"

    No expectations are evaluated at all — every row passes through untouched. This is a valid, inert configuration, not an error.

??? question "Format gotcha · Is rules a JSON array or a keyed object of rule definitions?"

    Always a JSON array of objects: `"rules": [{...}, {...}]`. `_validate_dq_config` explicitly checks `isinstance(rules, list)` and errors with 'expected a list of DQ rule objects' otherwise.

??? question "Performance impact · Does adding many DQ rules meaningfully slow down a pipeline update?"

    Each rule becomes one Lakeflow expectation evaluated per row, so cost scales roughly linearly with rule count — for typical rule counts (single digits) this is negligible next to the read/write cost of the flow itself; not separately benchmarked in this repo.

??? question "Edge case · Can one rule set both drop and quarantine behaviour for the same condition?"

    No — each rule object has exactly one `action`. To both drop obviously-bad rows and quarantine borderline ones, write two separate rules with different `expression`s, each carrying its own `action`.

??? question "Edge case · What happens if a rule's action is fail and the condition triggers mid-run?"

    The whole pipeline update aborts and the target transaction rolls back — see docs/04 Data Quality Actions table. Reserve `fail` for genuinely unrecoverable conditions; downgrade to `drop` or `quarantine` otherwise.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrules) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].action` { #dq-configrulesaction }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

warn logs and keeps the row, drop silently removes it, fail aborts the pipeline, quarantine routes it to the quarantine table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `warn`, `drop`, `fail`, `quarantine` | — |

**Persisted in** `config.ingestion_flow_spec.dq_config_json`


=== "JSON"

    ```json
    {
      "action": "warn"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- dq_config.rules[].action lives inside the dq_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: warn, drop, fail, quarantine.

**FAQs** (4)

??? question "If omitted · What error do I get if a rule is missing its action?"

    Onboarding rejects the flow: `check_string` is called with `required=True`, producing '<path>.action: is required but was missing or empty'. There is no default action.

??? question "Format gotcha · Are the action values case-sensitive, e.g. can I write WARN or Quarantine?"

    Yes, case-sensitive — `ALLOWED_DQ_ACTIONS = {"warn", "drop", "fail", "quarantine"}` is an exact-match set checked via `check_string(..., allowed_values=...)`. `"WARN"` is rejected as not in the allowed set.

??? question "Performance impact · Is any one action (warn/drop/fail/quarantine) noticeably more expensive than the others?"

    Negligible difference — all four evaluate the same per-row boolean `expression`; `quarantine` additionally forks the stream to write a second table (`dq/quarantine.py`), which is the one action with a real extra I/O cost, though still proportional to failing-row volume, not total volume.

??? question "Edge case · If I use action: quarantine but never configured quarantine_table, what happens?"

    Onboarding does not cross-validate this — the flow still validates. At runtime the quarantine sibling table registration depends on `dq_config.quarantine_table` being set; per docs/04 'Errors' guidance, quarantined rows effectively go nowhere without it.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesaction) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].expression` { #dq-configrulesexpression }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Boolean Spark SQL expression evaluated per row.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.dq_config_json`


=== "JSON"

    ```json
    {
      "expression": "amount >= 0"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- dq_config.rules[].expression lives inside the dq_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.

**FAQs** (4)

??? question "If omitted · What happens if a rule's expression is left blank?"

    Onboarding rejects the flow with '<path>.expression: is required but was missing or empty', since `check_string` is called with `required=True`.

??? question "Format gotcha · Can expression reference {{catalog}} or {{env}} template placeholders?"

    Yes — the attribute inspector's tip confirms `expression` supports `{{catalog}}` and `{{env}}` template variables, resolved at onboarding time, the same mechanism documented in docs/03 §5 Parameter Substitution.

??? question "Performance impact · Does a complex SQL expression in a DQ rule slow the pipeline down noticeably?"

    It runs as a per-row boolean Spark SQL predicate alongside the rest of the flow's transformations, so cost is proportional to expression complexity times row count — negligible for simple comparisons, but a heavy subquery-style expression would not be typical or recommended here.

??? question "Edge case · Will expression be validated for correct SQL syntax at onboarding time?"

    No — `spec_validator.py` only checks it is a non-empty string (`check_string`); it does not parse or execute the SQL. A malformed expression is only caught when the pipeline actually runs it as a Lakeflow expectation.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesexpression) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].rule_id` { #dq-configrulesrule-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Unique rule identifier.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.dq_config_json`


=== "JSON"

    ```json
    {
      "rule_id": "dq_amount_non_negative"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- dq_config.rules[].rule_id lives inside the dq_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if rule_id is missing from a DQ rule?"

    Onboarding rejects the flow: '<path>.rule_id: is required but was missing or empty', since `check_string` runs with `required=True`.

??? question "Format gotcha · Does rule_id need to follow a particular naming convention like a prefix?"

    No format is enforced beyond being a non-empty string. The reference sample uses a `dq_`-prefixed convention (`dq_amount_non_negative`) for readability, but this is a convention, not a validated rule.

??? question "Performance impact · Does rule_id itself have any runtime cost?"

    None — it is purely an identifier used for traceability, not evaluated as part of the DQ logic.

??? question "Edge case · What happens if two rules in the same dq_config.rules list share the same rule_id?"

    Not validated at onboarding — `_validate_dq_config` does not check uniqueness across the list. Since `rule_id` is what appears in `__framework_dq_failed_rule_ids`, a duplicate makes quarantine diagnostics ambiguous about which rule actually failed; verify at runtime.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesrule-id) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `governance_tags.column_tags` { #governance-tagscolumn-tags }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Governance tags</span>

Unity Catalog tags applied to individual columns after deployment.


Drives discovery and classification. Applied via ALTER TABLE SET TAGS once the table exists.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.governance_tags_json`


=== "JSON"

    ```json
    "column_tags": [
      { "column_name": "email", "tag_key": "pii", "tag_value": "true" }
    ]
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- governance_tags.column_tags lives inside the governance_tags_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - This framework applies tags only. It does not create masking policies or row filters — tagging a column does not protect it.
    - Tags are applied post-deployment, so they appear after the first successful update, not at onboarding time.

!!! warning "Known errors and limitations"

    **Tags never appear**  
    *Cause:* The run-as principal lacks APPLY TAG on the target.  
    *Fix:* GRANT APPLY TAG ON TABLE ... TO <principal>.

**FAQs** (4)

??? question "If omitted · What happens if I don't set governance_tags.column_tags?"

    No column-level Unity Catalog tags are applied. This is inert, not an error — `_validate_governance_tags` returns early when `governance_tags` is `None`, and `column_tags` itself is optional.

??? question "Format gotcha · Is column_tags a list of {column, tags} objects or a dict keyed by column name?"

    A list of objects: `[{"column": "email", "tags": {"pii": "true"}}]`. `_validate_governance_tags` checks `isinstance(column_tags, list)` and errors 'expected a list' otherwise — a dict keyed by column name is not the validated shape, even though it appears that way in some older doc examples.

??? question "Performance impact · Does tagging many columns slow down the pipeline update?"

    Negligible impact on the pipeline itself — tags are applied by a separate post-deployment job task (`04_apply_governance_and_egress.py`) via `ALTER TABLE ... SET TAGS`, decoupled from the streaming DAG, not during pipeline execution.

??? question "Edge case · Why haven't my column tags shown up right after deploying the pipeline?"

    Tags are applied post-deployment, after the pipeline update completes successfully, by a dedicated job task — not at onboarding or during the update itself. Also confirm the run-as principal has `APPLY TAG` on the target; without it tags never appear.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagscolumn-tags) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `governance_tags.column_tags[].column` { #governance-tagscolumn-tagscolumn }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Governance tags</span>

Column to tag.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.governance_tags_json`


=== "JSON"

    ```json
    {
      "column": "pii_column"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- governance_tags.column_tags[].column lives inside the governance_tags_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What if the column field is missing inside a column_tags entry?"

    Onboarding rejects the flow: `check_string` runs with `required=True`, producing '<entry_path>.column: is required but was missing or empty'.

??? question "Format gotcha · Does column need to be the exact physical column name on the target table?"

    Yes — it is validated only as a non-empty string, with no cross-check against the actual target schema. A misspelled column name is accepted at onboarding and only surfaces as a failed `ALTER TABLE` at the post-deployment tagging step.

??? question "Performance impact · Is there a limit to how many column_tags entries I can list?"

    Not validated — no maximum is enforced. Cost is negligible per entry since tagging happens once, post-deployment, outside the streaming DAG.

??? question "Edge case · What happens if column names a column that was dropped via target_config.columns_to_exclude?"

    Not cross-validated at onboarding. Since `columns_to_exclude` physically drops the column from the target table, the later `ALTER TABLE ... SET TAGS` for that column would fail at the post-deployment tagging step — verify at runtime.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagscolumn-tagscolumn) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `governance_tags.column_tags[].tags` { #governance-tagscolumn-tagstags }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Governance tags</span>

Key-value tags, e.g.


Key-value tags, e.g. mask: PII, classification: restricted.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.governance_tags_json`


=== "JSON"

    ```json
    {
      "tags": {
        "option_name": "value"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- governance_tags.column_tags[].tags lives inside the governance_tags_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (4)

??? question "If omitted · What happens if tags is left out of a column_tags entry?"

    Onboarding rejects the flow: `check_dict_of_str` runs with `required=True`, producing '<entry_path>.tags: is required but was missing'.

??? question "Format gotcha · Can a tag value be a number or boolean, like retention: 7?"

    No — `check_dict_of_str` requires every key and value to be a string, erroring 'expected an object of string -> string' otherwise. Write `"7"` as a string, not a bare number.

??? question "Performance impact · Does having many key-value tags on one column cost anything at runtime?"

    Negligible — tags are applied once via `ALTER TABLE ... SET TAGS` in the post-deployment job task, not evaluated per row.

??? question "Edge case · What happens if I typo a tag key, like pii_typ instead of pii_type?"

    Nothing flags it. Per the inspector tip, keys are written verbatim to the `ALTER TABLE SET TAGS` call — a typo becomes a silently-applied but wrongly-named tag, not a validation error.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagscolumn-tagstags) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `governance_tags.table_tags` { #governance-tagstable-tags }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Governance tags</span>

Table-level tags, e.g.


Table-level tags, e.g. row_filter: region_restricted, domain: finance.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.governance_tags_json`


=== "JSON"

    ```json
    {
      "governance_tags": {
        "table_tags": {
          "option_name": "value"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- governance_tags.table_tags lives inside the governance_tags_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (4)

??? question "If omitted · What happens if governance_tags.table_tags is not set?"

    No table-level Unity Catalog tags are applied; this is inert, not an error, since `table_tags` is optional in `ALLOWED_GOVERNANCE_TAGS_KEYS`.

??? question "Format gotcha · Is table_tags a flat object or does it need column-style {key, value} entries?"

    A flat string-to-string object, e.g. `{"cost_center": "CC-9041"}` — validated by `check_dict_of_str`, which requires every key and value to be a string, unlike `column_tags` which is a list of objects.

??? question "Performance impact · Is there a runtime cost to setting several table_tags entries?"

    Negligible — like column tags, these apply once via `ALTER TABLE ... SET TAGS` in the post-deployment task, outside the pipeline's streaming execution.

??? question "Edge case · Do table_tags get applied for a sink-type target?"

    No — per docs/04 §3, `target_type: "sink"` flows are skipped entirely for tag application (v1.7.5), since a pure sink has no persisted dataset to `ALTER`. A `governance_tags` block on a sink flow is accepted but inert; `external_sink` is not skipped since it materializes a real table first.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagstable-tags) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `source_config.asn1_codec` { #source-configasn1-codec }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ASN.1</span>

Which ASN.1 encoding to decode.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `ber`, `der` | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "asn1_codec": "ber"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.asn1_codec lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: ber, der.

**FAQs** (4)

??? question "If omitted · What error do I get if asn1_codec is left out on an asn1 flow?"

    Onboarding rejects it: `source_config.asn1_codec: is required but was missing or empty` (required only when `source_type` is `asn1`).

??? question "Format gotcha · Is asn1_codec case-sensitive, and what are the only legal values?"

    Yes. Only the lowercase literals `ber` and `der` are accepted (`ALLOWED_ASN1_CODECS`); anything else fails with 'allowed values are [\'ber\', \'der\']'.

??? question "Performance impact · Does choosing ber vs der change decode performance?"

    Not documented as a performance differentiator in this framework — both dispatch to the same partition-level `mapInPandas` decoder in `asn1/decoder.py`. Pick the codec matching your telecom module's actual wire encoding; verify at runtime if throughput differs for your payloads.

??? question "Edge case · Does asn1_codec interact with source_zip_handling.member_format?"

    No direct validator interaction — `asn1_codec` decodes the binary payload after ZIP/gzip extraction, while `member_format` governs the ZIP/gzip container. Both are independently required/optional pieces of the same `source_type: asn1` flow. See docs/02 §4.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#asn1-specific-source_type-asn1) · [Schema tree](tree.md#tree-ingestion-source-configasn1-codec) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.asn1_pdu_name` { #source-configasn1-pdu-name }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ASN.1</span>

Optional.


Optional. The top-level SEQUENCE/CHOICE type in the .asn file to decode each record as. Leave blank to auto-detect the root PDU; supply a name only to override detection.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "asn1_pdu_name": "CallDetailRecord"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.asn1_pdu_name lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if I don't set asn1_pdu_name for my ASN.1 source?"

    It is genuinely optional: an absent value makes the decoder auto-detect the root PDU (`asn1/decoder.py::detect_root_pdu_name` walks the compiled module for the type nothing else references). This was made optional in 0.0.2 specifically so auto-detection is reachable from a spec.

??? question "Format gotcha · Does asn1_pdu_name need to match the exact case used in the .asn file?"

    It must be a real type name from the compiled `.asn` module (a plain string, type-checked only); match the identifier exactly as declared in the schema file, since it selects a specific PDU rather than pattern-matching one.

??? question "Performance impact · Does supplying asn1_pdu_name explicitly speed up decoding versus auto-detect?"

    Not documented as a performance factor — auto-detection walks the compiled module once per partition just like an explicit name would. Supply it only to override detection, not for speed.

??? question "Edge case · What happens if my ASN.1 module has a root CHOICE instead of a SEQUENCE?"

    The PDU must resolve to a top-level `SEQUENCE`, and `CHOICE` is rejected anywhere in that SEQUENCE's member tree, not only at the top. Real telecom modules are usually root-`CHOICE` shaped, so `asn1_pdu_name` must point at the specific SEQUENCE variant you want decoded. See docs/02 §4 'Choosing asn1_pdu_name on a real telecom module'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#asn1-specific-source_type-asn1) · [Schema tree](tree.md#tree-ingestion-source-configasn1-pdu-name) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.asn1_schema_path` { #source-configasn1-schema-path }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ASN.1</span>

ASN.1 module definition file.


ASN.1 module definition file. Must be a real .asn file.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "asn1_schema_path": "/Volumes/{{catalog}}/landing/_asn1_schemas/cdr.asn"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.asn1_schema_path lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What if asn1_schema_path is missing from an asn1 ingestion flow?"

    Onboarding rejects it: `source_config.asn1_schema_path: is required but was missing or empty` whenever `source_type` is `asn1`.

??? question "Format gotcha · Does asn1_schema_path have to point at a real .asn file?"

    Yes — it must be a real `.asn` module definition file; onboarding only checks it is a non-empty string, so a wrong or non-existent path is not caught until the pipeline actually tries to compile the schema at runtime.

??? question "Performance impact · Is there a runtime cost to a large or complex .asn schema file?"

    The framework compiles the ASN.1 schema once per Spark partition via `mapInPandas` (not once per record), which is specifically the optimization that avoids a severe per-record compilation bottleneck. See docs/02 §4 'High-Performance Partitioned Architecture'.

??? question "Edge case · Can asn1_schema_path live on the same Volume as the data path?"

    There is no rule against it, but keep schema files in a dedicated location (the sample uses `_asn1_schemas/`) — the reader treats `path` as the binary CDR landing directory and `asn1_schema_path` as a separate, single schema file, not something Auto Loader itself scans for new files.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#asn1-specific-source_type-asn1) · [Schema tree](tree.md#tree-ingestion-source-configasn1-schema-path) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.auto_flatten_all` { #source-configauto-flatten-all }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · nested data &amp; standardization</span>

Recursively flattens all nested structs and explodes all arrays regardless of explode_columns — the same effect as an explicitly-empty explode_columns.


Recursively flattens all nested structs and explodes all arrays regardless of explode_columns — the same effect as an explicitly-empty explode_columns. Only takes effect while explode_columns is absent or empty.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `false` | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "auto_flatten_all": true
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.auto_flatten_all lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - same effect as the empty explode_columns mode
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens if I leave out auto_flatten_all entirely?"

    It defaults to `false` per the JSON schema. Combined with an absent `explode_columns`, the source stays a schema-preserving pass-through — nothing is flattened or exploded.

??? question "Format gotcha · Do I set auto_flatten_all as a JSON boolean or a quoted string?"

    A real JSON boolean (`true`/`false`); `check_bool` rejects a quoted `"true"` with 'Use the JSON literals true/false, not a quoted string'.

??? question "Performance impact · Does auto_flatten_all make ingestion slower than named explode_columns?"

    It recursively flattens every nested struct and explodes every array anywhere in the schema, which can produce a much larger row count (cartesian growth per array) than a scoped, named list — cost scales with how deeply/widely nested the actual payload is, not a fixed overhead.

??? question "Edge case · What happens if I set auto_flatten_all: true and also populate explode_columns?"

    It only takes effect while `explode_columns` is absent or empty — a populated `explode_columns` list wins and scopes flattening to just those named columns, regardless of `auto_flatten_all`. See docs/02 §6 table.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configauto-flatten-all) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.capture_technical_metadata` { #source-configcapture-technical-metadata }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

Adds the framework's file-provenance columns and gates the ingestion timestamp.


Populates __framework_source_file_name, _size, _modification_time and _metadata_headers, and switches on __framework_ingestion_timestamp_utc.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    "capture_technical_metadata": true
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.capture_technical_metadata lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Leave it on unless you have a strong column-count reason not to — it is the only record of which file a row came from.

!!! warning "Known errors and limitations"

    **CDC pipeline fails at startup complaining about sequence_by_column**  
    *Cause:* This was set false, so __framework_ingestion_timestamp_utc does not exist, and the CDC default sequencing column is missing.  
    *Fix:* Either re-enable this, or set target_config.sequence_by_column to a real column explicitly.

**FAQs** (4)

??? question "If omitted · What happens if capture_technical_metadata is not set at all?"

    Its schema default is `null`/unset (no default shown in the schema), meaning the framework's file-provenance columns and `__framework_ingestion_timestamp_utc` are not populated. This is distinct from an explicit `false` only in that omitting vs. `false` both leave it off.

??? question "Format gotcha · Is capture_technical_metadata a boolean or a string flag?"

    A real JSON boolean; `check_bool` flags a quoted `"true"`/`"false"` as invalid — 'expected a boolean (true/false in JSON)'.

??? question "Performance impact · Does turning on capture_technical_metadata add noticeable overhead?"

    Negligible — it only adds four metadata columns (`__framework_source_file_name`, `_size`, `_modification_time`, `_metadata_headers`) and a timestamp column per row; no extra scan or shuffle is introduced.

??? question "Edge case · Why does my CDC pipeline fail at startup complaining about sequence_by_column when this is off?"

    With `capture_technical_metadata` set `false`, `__framework_ingestion_timestamp_utc` never gets created, and that column is the CDC default sequencing column — so `dlt.apply_changes` has nothing to sequence by. Either re-enable this attribute, or set `target_config.sequence_by_column` to a real column explicitly.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configcapture-technical-metadata) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.column_normalization.case` { #source-configcolumn-normalizationcase }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · column normalization</span>

Case applied to normalized column names.


Case applied to normalized column names. lower is the default; preserve keeps the source casing; upper uppercases.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | `"lower"` | `lower`, `preserve`, `upper` | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "column_normalization": {
          "case": "lower"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.column_normalization.case lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: lower, preserve, upper.

**FAQs** (4)

??? question "If omitted · What case does column_normalization use if I don't set the case field?"

    It defaults to `lower` per the JSON schema, matching pre-v1.3.0 behaviour byte-for-byte, but only takes effect when `column_normalization.enabled` is `true`.

??? question "Format gotcha · What are the exact allowed values for column_normalization.case?"

    Only `lower`, `preserve`, or `upper` (`ALLOWED_COLUMN_NORMALIZATION_CASES`); any other string fails with the standard allowed-values error from `check_string`.

??? question "Performance impact · Does choosing preserve or upper cost more than the default lower?"

    Negligible — only the case-fold step of the per-column name transform changes; character normalization (whitespace trim, non-alphanumeric replacement, `_` collapsing) always runs regardless of `case`.

??? question "Edge case · Can case: preserve produce two columns that collide after normalization?"

    Collision detection always runs on the lowercased projection of the produced names regardless of `case`, so `Order_ID` and `order_id` are still caught even under `preserve`/`upper`. A collision raises: 'normalization produced duplicate column names ... Rename one of the source columns, or use schema_config_path for explicit renaming instead.' See docs/02 §8.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configcolumn-normalizationcase) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.column_normalization.enabled` { #source-configcolumn-normalizationenabled }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · column normalization</span>

Switch for column normalization.


Switch for column normalization. When off, source column names are passed through unchanged.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `false` | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json` · **Behaviour changed in** v1.4.0


=== "JSON"

    ```json
    {
      "source_config": {
        "column_normalization": {
          "enabled": true
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.column_normalization.enabled lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · Is column normalization on by default if I set case but not enabled?"

    No. As of v1.4.0, `enabled` defaults to `false` and is the only switch — an absent object, absent key, or explicit `false` all mean off, even if `case` is supplied alongside it. Omitting the attribute is not the same as it being truthy; check the default explicitly.

??? question "Format gotcha · Do I write enabled as true/false or as a quoted flag?"

    A real JSON boolean; a quoted string fails `check_bool`'s type check with 'Use the JSON literals true/false, not a quoted string.'

??? question "Performance impact · Is there a runtime cost to enabling column_normalization?"

    Negligible per-column string transform (trim, character replace, case-fold) plus one collision check on the lowercased projection of all output names — not a data scan or shuffle.

??? question "Edge case · I used the old normalize_column_names flag before — does it still work?"

    No — `source_config.normalize_column_names` is REJECTED as of v1.4.0, not silently ignored, because an ignored `true` would silently switch renaming off and let raw special-character column names hit the Delta target. Migrate `{"normalize_column_names": true}` to `{"column_normalization": {"enabled": true}}`. See docs/02 §8 migration table.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configcolumn-normalizationenabled) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.data_standardization_sql` { #source-configdata-standardization-sql }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source · nested data &amp; standardization</span>

Per-column expressions, each ending AS <column>.


Per-column expressions, each ending AS <column>. Restricted grammar: no SELECT/FROM/JOIN/UNION/WHERE/DML/DDL and no semicolons.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json` · **Behaviour changed in** v1.7.07


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.data_standardization_sql lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What happens if data_standardization_sql is left out?"

    It is fully optional; the validator returns immediately when it is `None`, so no standardization expressions run and the ingested columns pass through as read (after explode/dedup).

??? question "Format gotcha · Can I put a SELECT or a semicolon inside a data_standardization_sql entry?"

    No — each entry is a restricted per-column expression that must end `AS <column>`; the grammar forbids `SELECT`/`FROM`/`JOIN`/`UNION`/`WHERE`, any DML/DDL keyword, and semicolons. An empty/blank entry is also rejected: '`<path>[<index>]`: must not be empty'.

??? question "Performance impact · Does adding many standardization expressions slow down ingestion noticeably?"

    Each expression is a plain column-level `withColumn`/`select` transform evaluated once per surviving row (after dedup, per docs/02 §7) — cost scales roughly linearly with expression count and complexity, not a separate scan.

??? question "Edge case · In what order does data_standardization_sql run relative to remove_dups and column_normalization?"

    It runs after explode/auto-flatten and after `remove_dups`, and after `column_normalization`'s renames — so expressions must reference the already-normalized column names, not the raw source names. See docs/02 §7 and §8 'Ordering'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configdata-standardization-sql) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `source_config.dedup_watermark.delay_threshold` { #source-configdedup-watermarkdelay-threshold }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

Spark interval string paired with dedup_watermark.event_time_column.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "dedup_watermark": {
          "delay_threshold": "2 hours"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.dedup_watermark.delay_threshold lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What if I set remove_dups: true but skip delay_threshold?"

    If you supply the `dedup_watermark` block at all, `delay_threshold` is required alongside `event_time_column` — omitting it fails with 'is required but was missing or empty'. If you omit the whole `dedup_watermark` block, dedup runs with unbounded state instead (no error, just a runtime WARNING).

??? question "Format gotcha · What format does delay_threshold expect — a number of seconds or an interval string?"

    A Spark interval string like `"2 hours"` or `"30 minutes"`, not a bare number; it is validated only as a non-empty string, so a malformed interval is not caught until Spark's `withWatermark` call at runtime.

??? question "Performance impact · Does a longer delay_threshold cost more memory in the dedup state store?"

    Yes — a larger threshold retains more historical rows in dedup state before they age out, trading memory/state size against tolerance for late-arriving duplicates. A shorter threshold bounds memory tighter but risks treating a late duplicate as new.

??? question "Edge case · Does dedup_watermark do anything if remove_dups is false?"

    No — it is rejected outright at onboarding when `remove_dups` is not `true`: 'only meaningful alongside remove_dups: true, but remove_dups is not enabled for this source'. It configures nothing on its own. See docs/02 §7.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configdedup-watermarkdelay-threshold) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.dedup_watermark.event_time_column` { #source-configdedup-watermarkevent-time-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

Bounds dedup state on a streaming source.


Bounds dedup state on a streaming source. A TIMESTAMP column on the ingested DataFrame. Only meaningful with remove_dups: true.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "dedup_watermark": {
          "event_time_column": "updated_at"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.dedup_watermark.event_time_column lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is event_time_column optional if delay_threshold is set?"

    No — both are required together whenever `dedup_watermark` is present; missing `event_time_column` fails with 'is required but was missing or empty' regardless of `delay_threshold` being set.

??? question "Format gotcha · Does event_time_column need to reference the raw source column name or the normalized one?"

    It must name a TIMESTAMP column on the DataFrame at the point dedup runs, which is after explode/auto-flatten and after column normalization — so use the post-normalization name if `column_normalization` is enabled.

??? question "Performance impact · Does choosing event_time_column affect ingestion throughput?"

    Negligible in itself — it only feeds Spark's `withWatermark(...)` call, which bounds state rather than adding a scan. The real cost driver is `delay_threshold`, not which column is chosen.

??? question "Edge case · What happens on a batch (non-streaming) source with dedup_watermark configured?"

    On a batch source, `dropDuplicates` is a plain shuffle with no state to bound, so `dedup_watermark` — if present — is simply ignored (logged at INFO), not an error. See docs/02 §7.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configdedup-watermarkevent-time-column) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.explode_columns` { #source-configexplode-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · nested data &amp; standardization</span>

Only the named top-level columns are processed.


Only the named top-level columns are processed. A column that is neither struct nor array at runtime is an error.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.explode_columns lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What is the difference between not setting explode_columns and setting it to an empty list?"

    This distinction is load-bearing: an absent key (or explicit `null`) is a schema-preserving pass-through — nothing is exploded. `"explode_columns": []` (present and empty) auto-flattens every nested struct and explodes every array recursively, equivalent to `auto_flatten_all: true`. Getting this backwards changes production row counts, not just column shapes. See docs/02 §6.

??? question "Format gotcha · Can I list a column that is a plain scalar, not a struct or array?"

    The onboarding validator only checks it is a list of strings; a listed column that resolves to neither a struct nor an array at runtime raises `FrameworkConfigError` when the pipeline actually runs, since onboarding has no access to the source's real schema.

??? question "Performance impact · Does naming specific explode_columns cost less than auto_flatten_all?"

    Yes in general — a populated list scopes struct-flatten/array-explode treatment to only the named top-level columns, versus `auto_flatten_all`/empty-list which processes every nested struct and array anywhere in the schema recursively.

??? question "Edge case · What happens when an array<struct<...>> column is named in explode_columns?"

    It is exploded first, then its resulting struct element is flattened in the same step — one entry handles both the array-explode and the struct-flatten for that column. See docs/02 §6 table.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configexplode-columns) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.explode_mode` { #source-configexplode-mode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · nested data &amp; standardization</span>

How explode_columns is written to the spec.


How explode_columns is written to the spec. absent omits the key entirely — schema-preserving pass-through. empty writes explode_columns: [] — auto-flattens every nested struct and explodes every array in the schema. named writes the columns you list below.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "explode_mode": "absent"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.explode_mode lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - the absent / explicitly-empty distinction is load-bearing
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: absent, empty, named.

**FAQs** (4)

??? question "If omitted · What if explode_mode is left unset in the onboarding form?"

    This is a form-only convenience field (not a real spec key — it does not appear in `source_config`'s allowed keys); its default `absent` simply means the form omits `explode_columns` entirely, giving a schema-preserving pass-through.

??? question "Format gotcha · Is explode_mode itself written into the JSON spec?"

    No — it only controls how the form writes `explode_columns`: `absent` omits the key, `empty` writes `explode_columns: []`, `named` writes the list you supply. Check `all_attribute_paths.txt`/the schema for the real spec key, which is `source_config.explode_columns`.

??? question "Performance impact · Does picking empty vs named in explode_mode change ingestion cost?"

    Yes, indirectly — `empty` triggers the same recursive flatten-everything behaviour as `auto_flatten_all`, which is more expensive than `named`, which scopes the flatten/explode to only the columns you list.

??? question "Edge case · Why does the form hide explode_mode sometimes?"

    It only applies to certain source/format configurations (e.g. it is meaningful mainly for `json`/nested sources); the form hides it when it is not relevant to the current source_type/format combination selected.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configexplode-mode) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.file_pattern` { #source-configfile-pattern }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

Glob or regex filtering which files are picked up.


Glob or regex filtering which files are picked up. Maps to cloudFiles.fileNamePattern.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "file_pattern": "orc_*"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.file_pattern lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if file_pattern is not set on an Auto Loader source?"

    No `pathGlobFilter` option is applied at all, so every file under `path` matching Auto Loader's own format-detection rules is picked up — there is no implicit filtering default.

??? question "Format gotcha · Does file_pattern map to cloudFiles.fileNamePattern?"

    No — despite looking like it should, `file_pattern` always maps to Spark's generic `pathGlobFilter` option, for every format, not to `cloudFiles.fileNamePattern`. Auto Loader rejects any `cloudFiles.`-prefixed key outside its closed whitelist with `CF_UNKNOWN_OPTION_KEYS_ERROR`, which is exactly why the framework never emits that spelling.

??? question "Performance impact · Does a broad glob in file_pattern slow down file discovery?"

    `pathGlobFilter` is applied by the underlying file source during listing, so a narrower pattern reduces the file set Auto Loader has to consider each trigger — a very broad pattern (e.g. `*`) has effectively no filtering benefit over omitting it.

??? question "Edge case · Can reader_options override my file_pattern setting?"

    Yes — `reader_options` is applied after `file_pattern`'s `pathGlobFilter` option in `_apply_common_autoloader_options`, so a colliding key in `reader_options` silently overrides it. Keys are written verbatim with no collision warning.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configfile-pattern) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.format` { #source-configformat }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · autoloader</span>

How Auto Loader parses each file it picks up.


Chooses the reader and therefore which other source_config options apply.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    "format": "csv",
    "reader_options": { "header": "true", "delimiter": "," }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.format lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - csv and json need reader_options to behave predictably; parquet carries its own schema.
    - For json, decide explode behaviour deliberately — an absent explode_columns preserves nesting.

!!! warning "Known errors and limitations"

    **Every column arrives as a string**  
    *Cause:* cloudFiles.inferColumnTypes is not set for csv.  
    *Fix:* Add "cloudFiles.inferColumnTypes": "true" to reader_options, or declare a schema_config.

**FAQs** (4)

??? question "If omitted · What error do I get if format is missing on an autoloader flow?"

    Onboarding rejects it: `source_config.format: is required but was missing or empty`, since `format` is required whenever `source_type` is `autoloader`.

??? question "Format gotcha · Are format values case-sensitive, e.g. CSV vs csv?"

    The validator only checks it is a non-empty string — there is no enum restriction in `spec_validator.py` — but it is passed verbatim as `cloudFiles.format`, so it must match Auto Loader's own expected lowercase values (e.g. `csv`, `json`, `parquet`).

??? question "Performance impact · Does the choice of format materially change ingestion cost?"

    Yes — `parquet` carries its own schema and needs no extra type inference, while `csv`/`json` need `reader_options` (e.g. `cloudFiles.inferColumnTypes`) to behave predictably, and JSON's nested structure adds explode/flatten cost downstream if configured.

??? question "Edge case · Why did every column come back as a string when I used csv?"

    `cloudFiles.inferColumnTypes` was not set for the `csv` reader. Add `"cloudFiles.inferColumnTypes": "true"` to `reader_options`, or declare a `schema_config_path` for explicit types.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configformat) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [auto loader options](https://docs.databricks.com/ingestion/auto-loader/options.html)


---

### `source_config.json_string_columns` { #source-configjson-string-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · nested data &amp; standardization</span>

STRING columns holding a JSON document, parsed with from_json / schema_of_json before flattening — giving Parquet sources parity with JSON.


STRING columns holding a JSON document, parsed with from_json / schema_of_json before flattening — giving Parquet sources parity with JSON. Each entry is either a column name or an object of the form {"column": "col", "schema_ddl": "struct<...>"}.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.json_string_columns lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - name, or {column, schema_ddl} for an explicit schema
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What happens if json_string_columns is left out on a Parquet source with JSON-in-a-string columns?"

    It is fully optional; when absent, no `from_json` parsing runs on any string column, so a JSON document sitting in a STRING column stays an opaque string and is never flattened.

??? question "Format gotcha · What are the two accepted shapes for a json_string_columns entry?"

    Either a bare column-name string (or `{"column": "col"}`), or `{"column": "col", "schema_ddl": "struct<...>"}` — the recommended, fully deterministic form. Listing the same column twice is rejected at onboarding: 'column '<col>' is listed more than once'.

??? question "Performance impact · Is there a runtime cost difference between supplying schema_ddl and omitting it?"

    Omitting `schema_ddl` falls back to Databricks' inferring `from_json`, which persists the inferred schema under a `schemaLocationKey` in the checkpoint — extra bookkeeping the explicit `schema_ddl` form avoids, since that form is deterministic with no inference step.

??? question "Edge case · Can I use the no-schema_ddl shorthand on a materialized_view or batch_table target?"

    No — the no-`schema_ddl` shorthand only works on a streaming DataFrame; on a batch/materialized source it raises `FrameworkConfigError`: 'schema inference via from_json's schemaLocationKey requires a streaming source with a checkpoint. Supply an explicit schema_ddl...'. Always supply `schema_ddl` for `batch_table`/`materialized_view` targets. See docs/02 §6.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configjson-string-columns) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.landing_retention_policy.archive_path` { #source-configlanding-retention-policyarchive-path }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · landing retention</span>

Where archived files are moved.


Where archived files are moved. Maps to cloudFiles.cleanSource.moveDestination. No longer a hard requirement: archive with a missing or empty archive_path degrades to off with a runtime warning. delete never needs it.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "landing_retention_policy": {
          "archive_path": "/Volumes/{{catalog}}/landing/_archive/zone/"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.landing_retention_policy.archive_path lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - missing path degrades archive to off — not a validation error
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is archive_path required when clean_source is archive?"

    No, never — not even for `clean_source: archive`. An empty or absent `archive_path` degrades the whole policy to `off` with a runtime WARNING, not a validation error. This was a deliberate v1.3.0 relaxation so an operator can pause archiving without deleting the block.

??? question "Format gotcha · What does the log say when archive_path is blank under clean_source: archive?"

    'landing_retention_policy.clean_source='archive' but archive_path is empty/absent -- degrading to clean_source='off': NO retention or cleanup action will be taken for this source.' It maps to `cloudFiles.cleanSource.moveDestination` only when non-empty.

??? question "Performance impact · Does setting archive_path add overhead to every micro-batch?"

    It only adds a file-move operation for files Auto Loader has already ingested and committed — a per-committed-file cost, not a per-trigger scan cost; negligible relative to the ingestion read itself.

??? question "Edge case · What happens if I leave archive_path set but change clean_source to delete?"

    It is silently ignored, not flagged — `clean_source: delete` deletes by age alone and never reads `archive_path`, so a stray leftover value from an earlier `archive` configuration is an unused sibling field, not a misconfiguration. See docs/02 §2.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configlanding-retention-policyarchive-path) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.landing_retention_policy.clean_source` { #source-configlanding-retention-policyclean-source }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · landing retention</span>

off leaves files in place, archive moves them, delete removes them.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | `"off"` | `archive`, `delete`, `off` | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "landing_retention_policy": {
          "clean_source": "off"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.landing_retention_policy.clean_source lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: off, archive, delete.

**FAQs** (4)

??? question "If omitted · What happens if clean_source is omitted?"

    It defaults to `off`, which emits no `cloudFiles.cleanSource*` option at all — this is already Auto Loader's own default, so omitting it is byte-for-byte equivalent to the framework not setting the option.

??? question "Format gotcha · What are the only legal values for clean_source?"

    `archive`, `delete`, or `off` (`ALLOWED_CLEAN_SOURCE_MODES`); any other string fails `check_string`'s allowed-values check.

??? question "Performance impact · Which clean_source mode is cheapest at runtime?"

    `off` is free (no option is even sent). `delete` is a metadata-only removal by age. `archive` costs a physical file move per committed file to `archive_path` — negligible per-file but scales with ingestion volume.

??? question "Edge case · Can I set landing_retention_policy.clean_source on a zerobus source?"

    No — it is rejected at onboarding for any `source_type` other than `autoloader`/`asn1`: 'only applicable to source_type 'autoloader'/'asn1' (Auto Loader file ingestion), but this flow's source_type is 'zerobus''. A Zerobus stream reads an existing Delta table, so there is no landing zone to clean. See docs/02 §2.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configlanding-retention-policyclean-source) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.landing_retention_policy.retention_days` { #source-configlanding-retention-policyretention-days }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · landing retention</span>

Maps to cloudFiles.cleanSource.retentionDuration as N days.


Maps to cloudFiles.cleanSource.retentionDuration as N days. Minimum 0; defaults to 7 when omitted.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | `7` | — | min `0` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "landing_retention_policy": {
          "retention_days": 7
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.landing_retention_policy.retention_days lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What retention_days value applies if I don't set it?"

    It defaults to `7` (`DEFAULT_LANDING_RETENTION_DAYS` in `ingestion/readers.py`) — not zero, and not Auto Loader's own untouched default. A spec relying on the old implicit behaviour must now set it explicitly.

??? question "Format gotcha · Can retention_days be a decimal or a string like '7d'?"

    No — it must be a real JSON integer `>= 0`; `check_int` also rejects a JSON boolean (Python's `bool` subclasses `int`) even though it type-checks as one.

??? question "Performance impact · What does retention_days: 0 actually do?"

    It is legal and means 'no age threshold at all' — a file becomes eligible for archive/delete the moment Auto Loader commits it (`cloudFiles.cleanSource.retentionDuration = '0 days'`), which is the most aggressive, not a disabled, setting.

??? question "Edge case · Does retention_days matter if clean_source is off?"

    No — when `clean_source` resolves to `off` (explicit or degraded from a blank `archive_path`), no `cloudFiles.cleanSource*` option is emitted at all, so `retention_days` is never read. See docs/02 §2.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configlanding-retention-policyretention-days) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.max_bytes_per_trigger` { #source-configmax-bytes-per-trigger }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · zerobus</span>

Throttles how much data each micro-batch reads.


Throttles how much data each micro-batch reads. Maps to maxBytesPerTrigger.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "max_bytes_per_trigger": "1g"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.max_bytes_per_trigger lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if max_bytes_per_trigger is not set on a zerobus source?"

    No `maxBytesPerTrigger` option is applied at all — the reader lets Delta's streaming source use its own default micro-batch sizing with no throttle from the framework.

??? question "Format gotcha · What format does max_bytes_per_trigger expect — a plain number or a suffixed string?"

    A string like `"1g"`, matching Spark's own `maxBytesPerTrigger` option format (a size string with a unit suffix); it is validated only as a non-empty string, so an invalid unit is not caught until the stream starts.

??? question "Performance impact · Does max_bytes_per_trigger help control ingestion cost spikes?"

    Yes — it throttles how much data each micro-batch reads (`maxBytesPerTrigger`), which is exactly its purpose: bounding per-trigger read volume for a Zerobus/Delta streaming source to smooth compute and avoid oversized batches.

??? question "Edge case · Does max_bytes_per_trigger apply to autoloader sources too?"

    It is documented and implemented only for the `zerobus` reader (`read_zerobus_source`), not `read_autoloader_source`; the equivalent Auto Loader throttle would be a different `reader_options` key like `maxFilesPerTrigger`, passed through separately.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configmax-bytes-per-trigger) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [structured streaming](https://docs.databricks.com/structured-streaming/index.html)


---

### `source_config.path` { #source-configpath }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · autoloader</span>

The Unity Catalog Volume directory Auto Loader watches for new files.


Auto Loader lists and streams from this directory; it is the physical entry point of the flow.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    "path": "/Volumes/{{catalog}}/landing/orders/{{env}}/incoming/"
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.path lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
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

**FAQs** (4)

??? question "If omitted · What if source_config.path is missing on an autoloader flow?"

    Onboarding rejects it: `source_config.path: is required but was missing or empty`, since it is required whenever `source_type` is `autoloader` (and also for `asn1`).

??? question "Format gotcha · Why must path always end with a trailing slash?"

    A path that looks like a file (no trailing slash) makes Auto Loader watch the parent directory instead of the intended one — always end with `/` to point at the actual incoming directory, e.g. `/Volumes/{{catalog}}/landing/orders/{{env}}/incoming/`.

??? question "Performance impact · Does mixing archived and incoming files in the same path directory slow things down?"

    It is worse than a performance issue — the docs warn it makes retention rules dangerous, since `landing_retention_policy` acts on the same directory Auto Loader reads from. Point `path` at a dedicated `incoming/` directory.

??? question "Edge case · My pipeline update reports SUCCESS but wrote zero rows — why?"

    Most often `path` does not match where files actually land, especially when `source_zip_handling.target_volume_path` points somewhere else. The onboarding validator does not cross-check `path` against `target_volume_path` — confirm the extraction target and `path` are the same directory yourself.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configpath) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [auto loader](https://docs.databricks.com/ingestion/auto-loader/index.html) · [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `source_config.reader_options` { #source-configreader-options }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

Passthrough to the Spark reader — one .option(key, value) per entry.


Passthrough to the Spark reader — one .option(key, value) per entry. Can override file_pattern and schema_evolution_mode if keys collide.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "reader_options": {
          "option_name": "value"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.reader_options lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (4)

??? question "If omitted · What happens if reader_options is left empty or absent for a CSV source?"

    No extra `.option(key, value)` calls are made beyond `cloudFiles.format`/`schemaLocation` (and evolution/retention/file_pattern if set) — for `csv` specifically this typically means no header handling or type inference, so every column may arrive as a string.

??? question "Format gotcha · Are reader_options keys and values validated against real Spark option names?"

    No — `check_dict_of_str` only verifies it is an object of string keys to string values; a typo'd key is written verbatim to the reader and becomes a silently ignored option, not an onboarding or runtime error.

??? question "Performance impact · Can reader_options be used to control micro-batch throughput?"

    Yes, e.g. `maxFilesPerTrigger` in `reader_options` throttles files per Auto Loader trigger the same way `max_bytes_per_trigger` does for Zerobus — this is a direct passthrough, so any Spark reader option is available at the cost of one `.option()` call per entry.

??? question "Edge case · What happens if a key in reader_options collides with file_pattern or schema_evolution_mode?"

    `reader_options` is applied after `file_pattern` (`pathGlobFilter`) and after `schema_evolution_mode` (`cloudFiles.schemaEvolutionMode`) are set on the reader, so a colliding key in `reader_options` overrides both silently. See `ingestion/readers.py::_apply_common_autoloader_options`.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configreader-options) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [auto loader options](https://docs.databricks.com/ingestion/auto-loader/options.html)


---

### `source_config.remove_dups` { #source-configremove-dups }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

Full-row dropDuplicates over every column except __framework_*-prefixed columns and _rescued_data / _metadata.


Full-row dropDuplicates over every column except __framework_*-prefixed columns and _rescued_data / _metadata. Without a watermark this holds unbounded dedup state.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `false` | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "remove_dups": true
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.remove_dups lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - set the watermark below to bound state
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens if remove_dups is not set?"

    It defaults to `false` — no deduplication runs, and duplicate rows from the source (e.g. reprocessed files) pass straight through unchanged.

??? question "Format gotcha · Is remove_dups a boolean or should I quote it?"

    A real JSON boolean; `check_bool` rejects a quoted `"true"` as invalid, requiring the literal `true`/`false`.

??? question "Performance impact · What is the cost of enabling remove_dups on a large streaming source?"

    Without a `dedup_watermark`, `dropDuplicates` keeps state for every distinct row seen since the stream started, forever — this is unbounded and will eventually degrade or fail a long-running pipeline. Set `dedup_watermark` to bound the state.

??? question "Edge case · Which row survives when remove_dups collapses a duplicate group?"

    Spark gives no ordering guarantee, so the surviving row's `__framework_source_file_name`/`__framework_ingestion_timestamp_utc` is an arbitrary pick among the duplicates. A flow needing a deterministic 'keep earliest/latest' rule should use an SCD strategy or windowed `transformation_sql` instead. See docs/02 §7.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configremove-dups) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.schema_config_path` { #source-configschema-config-path }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

External JSON/YAML declaring explicit casts, nullability, UC column comments and renames.


External JSON/YAML declaring explicit casts, nullability, UC column comments and renames. A directory resolves to its most recently modified file.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "schema_config_path": "/Volumes/.../schema_config.json"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.schema_config_path lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if schema_config_path is not set?"

    It is fully optional; when absent, no external casts/nullability/comments/renames are applied — columns keep whatever type Auto Loader infers (or the native schema for parquet/Zerobus).

??? question "Format gotcha · Can schema_config_path point to a directory instead of a specific file?"

    Yes — if it points to a directory, FlowX automatically resolves and loads the most recently modified file in that directory, so you do not need to hardcode a filename that changes over time.

??? question "Performance impact · Is there a validation cost to using schema_config_path at onboarding time?"

    Onboarding only checks it is a non-empty, required-when-present string — the referenced file's own shape is validated later, when it is actually loaded at pipeline graph-definition time, since onboarding has no Volume/workspace file access of its own.

??? question "Edge case · Does schema_config_path run before or after column_normalization?"

    Its renames apply before column normalization — normalization runs immediately after the raw source read and after `schema_config_path`'s renames, so every downstream reference (explode_columns, data_standardization_sql, dq_config expressions) uses the post-normalization name. See docs/02 §8 'Ordering'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configschema-config-path) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.schema_evolution_mode` { #source-configschema-evolution-mode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · reader options</span>

Maps to cloudFiles.schemaEvolutionMode.


Maps to cloudFiles.schemaEvolutionMode. rescue sends new or mismatched columns to _rescued_data.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `addNewColumns`, `addNewColumnsWithTypeWidening`, `rescue`, `failOnNewColumns`, `none` | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "schema_evolution_mode": "addNewColumns"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.schema_evolution_mode lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: addNewColumns, addNewColumnsWithTypeWidening, rescue, failOnNewColumns, none.

**FAQs** (4)

??? question "If omitted · What schema evolution mode applies if I never set schema_evolution_mode?"

    The attribute has no schema default listed and the check only runs `if evolution_mode:` truthy at runtime, so leaving it unset means no `cloudFiles.schemaEvolutionMode` option is set at all, and Auto Loader falls back to its own native default behaviour.

??? question "Format gotcha · What are the exact legal values for schema_evolution_mode?"

    `addNewColumns`, `addNewColumnsWithTypeWidening`, `rescue`, `failOnNewColumns`, or `none` (`ALLOWED_SCHEMA_EVOLUTION_MODES`); an unsupported value also raises `FrameworkConfigError: Unsupported schema_evolution_mode: <value>` at the reader itself, in addition to the onboarding check.

??? question "Performance impact · Does rescue mode add processing cost compared to addNewColumns?"

    Not documented as materially different in cost — `rescue` redirects unparseable rows/unknown columns to `_rescued_data` rather than failing the stream, which is a routing decision, not an extra scan.

??? question "Edge case · What happens with failOnNewColumns if the upstream source adds a column?"

    The stream immediately stops ingestion the moment a new column is detected — this is the strictest mode and is the opposite of `rescue`, which instead redirects the surprise data to `_rescued_data` and keeps running. See docs/02 'Schema Evolution Policies'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configschema-evolution-mode) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [auto loader schema](https://docs.databricks.com/ingestion/auto-loader/schema.html)


---

### `source_config.schema_location` { #source-configschema-location }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · autoloader</span>

Where Auto Loader persists the inferred schema and its evolution history.


Without a durable schema location, Auto Loader cannot detect that a column is new, so schema evolution and the rescue column stop working.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    "schema_location": "/Volumes/{{catalog}}/landing/_schemas/orders/"
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.schema_location lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Give every flow its own schema location. Sharing one between flows corrupts both schemas.
    - Never point it inside the directory being ingested — the checkpoint files become input files.

!!! warning "Known errors and limitations"

    **Pipeline reprocesses everything after a redeploy**  
    *Cause:* The schema location moved, so the stream lost its checkpoint identity.  
    *Fix:* Restore the original path, or accept a one-time full reprocess.

**FAQs** (4)

??? question "If omitted · What happens if I omit schema_location for an autoloader flow?"

    It is auto-derived: the loader defaults it to `/Volumes/<target_catalog>/landing/_schemas/<target_table>/` before the required-field check runs, using this flow's own `target_catalog`/`target_table` — so omitting it in practice is not an onboarding failure even though the schema marks it required.

??? question "Format gotcha · Can two flows share the same schema_location?"

    No — give every flow its own schema location. Sharing one between flows corrupts both schemas, since Auto Loader persists the inferred schema and evolution history there per-stream.

??? question "Performance impact · Does changing schema_location cause a reprocess?"

    Yes — moving it loses the stream's checkpoint identity, so the pipeline reprocesses everything after a redeploy. Restore the original path, or accept a one-time full reprocess.

??? question "Edge case · What happens if schema_location is inside the ingested directory itself?"

    Never point it inside the directory being ingested — the checkpoint/schema files themselves become input files Auto Loader tries to read, corrupting ingestion. Keep it in a separate `_schemas/` location as the auto-derived default does. See docs/02 §2.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configschema-location) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [auto loader schema](https://docs.databricks.com/ingestion/auto-loader/schema.html)


---

### `source_config.source_catalog` { #source-configsource-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · zerobus</span>

Catalog of the existing Delta table to stream from.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "source_catalog": "example_source_catalog"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_catalog lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What error appears if source_catalog is missing on a zerobus flow?"

    Onboarding rejects it: `source_config.source_catalog: is required but was missing or empty`, since it is required whenever `source_type` is `zerobus`.

??? question "Format gotcha · Does source_catalog need three-part qualification itself?"

    No — it is just the catalog name segment; it is combined with `source_schema`/`source_table` at runtime into a fully-qualified `catalog.schema.table` string (`read_zerobus_source`), so keep it a bare catalog identifier, not a dotted path.

??? question "Performance impact · Does source_catalog choice affect ingestion cost?"

    Negligible — it only resolves which table the Delta streaming reader attaches to; cost is driven by the actual table's change volume and `max_bytes_per_trigger`, not by the catalog name.

??? question "Edge case · What happens if source_catalog/schema/table point at a table this same pipeline also publishes?"

    That would create a genuine self-read graph edge (a fully-qualified three-part name on a table the pipeline itself writes is a sibling reference, not an escape hatch) — Lakeflow's self-read guard applies exactly as it would to a `dlt.read`. Point `zerobus` sources at genuinely external tables.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configsource-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.source_schema` { #source-configsource-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · zerobus</span>

Schema of the source table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "source_schema": "example_source_schema"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_schema lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if source_schema is left out on a zerobus source?"

    Onboarding rejects it: `source_config.source_schema: is required but was missing or empty`, required whenever `source_type` is `zerobus`.

??? question "Format gotcha · Is source_schema the Unity Catalog schema or a Kafka-style topic namespace?"

    It is the Unity Catalog schema segment of the existing Delta table being streamed — combined into `source_catalog.source_schema.source_table` — not a messaging-system concept, since Zerobus here means streaming an existing Delta table.

??? question "Performance impact · Does source_schema affect read throughput?"

    Negligible — like `source_catalog`, it only participates in resolving the qualified table name for the Delta streaming reader.

??? question "Edge case · What happens if source_schema resolves to a schema this dataflow group doesn't have READ access to?"

    Not validated at onboarding — verify at runtime; the reader will fail with a permissions error from Unity Catalog when the stream actually attempts to attach to the table.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configsource-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.source_table` { #source-configsource-table }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · zerobus</span>

Table name of the source.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "source_table": "example_source_zerobus_table"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_table lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if source_table is missing on a zerobus flow?"

    Onboarding rejects it: `source_config.source_table: is required but was missing or empty`, required whenever `source_type` is `zerobus`.

??? question "Format gotcha · Does source_table need to be fully qualified with catalog and schema?"

    No — it is just the bare table name; it is combined with `source_catalog`/`source_schema` into the qualified name by the reader, so a dotted `catalog.schema.table` string here would be wrong.

??? question "Performance impact · Does source_table's size affect the initial stream startup cost?"

    Yes, indirectly — Delta streaming from a table with a long history (many versions) can affect how much state/log the stream processes on first attach, especially combined with `starting_version`; use `starting_version`/`max_bytes_per_trigger` to bound the initial catch-up.

??? question "Edge case · Can source_table be a table that this same pipeline group also ingests via autoloader elsewhere?"

    The Single-Read DAG mandate keys identity per `(path/table, execution mode)` — a genuinely external Delta table streamed via Zerobus is a distinct external read from an unrelated Auto Loader path, so this is fine as long as it is not the same physical table read twice under the same mode.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configsource-table) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.source_zip_handling.delete_source_after_extract.action` { #source-configsource-zip-handlingdelete-source-after-extractaction }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

What happens to the ZIP after a successful extraction.


What happens to the ZIP after a successful extraction. delete_now removes the archive as soon as its members are extracted; delete_after_x_days runs an age-based sweep of the landing directory, skipping only archives whose extraction failed in the same run. Leaving this unset keeps the archive.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.delete_source_after_extract.action lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - unset keeps the archive
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: delete_now, delete_after_x_days.

**FAQs** (4)

??? question "If omitted · What happens to the source archive if delete_source_after_extract is left unset?"

    Absent means today's default: delete this run's own archive immediately after a successful extract (equivalent to `{"action": "delete_now"}`), per both the validator docstring and the schema description's stated default.

??? question "Format gotcha · Can I still use a plain boolean instead of the action object for delete_source_after_extract?"

    Yes -- both spellings are first-class and neither is deprecated. `true` normalizes to `delete_now` and `false` to an internal-only `never`, via `archive/zip_utils.py::resolve_zip_delete_policy`.

??? question "Performance impact · Does delete_after_x_days scan every archive on every run?"

    It runs an age-based sweep of `source_zip_path` at extraction time on every run, removing archives matching `zip_file_pattern` older than `days` -- proportional to files in that directory, not a one-time cost, but not flagged as expensive either.

??? question "Edge case · What if extraction fails partway -- does delete_after_x_days still remove that archive?"

    No -- the sweep explicitly skips any archive whose extraction failed in that same run, so a failed extraction is never swept out from under a retry, per the validator docstring.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingdelete-source-after-extractaction) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.delete_source_after_extract.days` { #source-configsource-zip-handlingdelete-source-after-extractdays }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Age in days after which a successfully-extracted archive is swept.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.delete_source_after_extract.days lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is days required when action is delete_after_x_days?"

    Yes -- `check_int(..., required=True, minimum=0)` runs only for `delete_after_x_days`; a sweep with no threshold isn't a policy, per the validator docstring, so onboarding rejects it if `days` is missing.

??? question "Format gotcha · Is days: 0 a valid value, and what does it mean?"

    Yes, `days: 0` is legal and means sweep everything already committed -- the validator's `minimum=0` allows it, and the docstring explicitly mirrors this to `retention_days: 0`.

??? question "Performance impact · Does a small days value increase how often the sweep runs?"

    The sweep itself runs every extraction-time pass regardless of the `days` value; a smaller value just widens which archives qualify for removal, not how often the check executes.

??? question "Edge case · What happens if I set days alongside action: delete_now?"

    Rejected -- the validator errors with '`{path}.days`: only meaningful for action '\''delete_after_x_days'\'', but action is '\''delete_now'\''' because it would be silently inert, which reads as a working configuration but is not one.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingdelete-source-after-extractdays) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.enabled` { #source-configsource-zip-handlingenabled }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Master switch for ZIP extraction.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "source_zip_handling": {
          "enabled": true
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.enabled lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens if I leave source_config.source_zip_handling.enabled out of my spec entirely?"

    `source_zip_handling` is optional at the top level, but once you supply the block `enabled` itself is required inside it -- the validator rejects a `source_zip_handling` object with `enabled` missing. Omit the whole block if you have no ZIP/gzip pre-processing to do.

??? question "Format gotcha · Does enabled: false still require the other source_zip_handling fields?"

    No. `source_zip_path`, `zip_file_pattern`, `target_volume_path` and the decryption sub-blocks are only checked when `enabled` is true; the validator's own branch (`if zip_handling.get("enabled")`) skips all of them otherwise.

??? question "Performance impact · Does turning on source_zip_handling cost anything for an unencrypted, unzipped file?"

    Yes -- if the file needs no unzip and no decryption, enabling this block still buys a staging copy for nothing; docs/02 section 5 and the member_format tip both say an unencrypted `.gz` needs no handling at all since Spark decompresses it natively on read.

??? question "Edge case · Is source_zip_handling available for zerobus sources?"

    No. It is shared by `autoloader` and `asn1` only; `zerobus` streams an existing Delta table with no landing-zone file to unzip, so the block has no meaning there. See docs/02 section 5.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingenabled) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.member_format` { #source-configsource-zip-handlingmember-format }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-ver">v1.7.4+</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

The landing archive's CONTAINER — how the bytes are packed, independent of whether they are also encrypted.


'zip' is a real archive: a member table plus N named entries, opened by pyzipper. 'gzip' is a single compressed stream with no member table at all, so pyzipper cannot open it. Before v1.7.4 the extractor assumed ZIP unconditionally, which made an encrypted .gz unreachable — you could decrypt it or decompress it, never both.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `zip`, `gzip` | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    // An ENCRYPTED gzip: EA_REQUEST_20260901.csv.gz.gpg
    "source_zip_handling": {
      "enabled": true,
      "source_zip_path": "/Volumes/{{catalog}}/staging/uc_6/raw/",
      "zip_file_pattern": "*.csv.gz.gpg",
      "target_volume_path": "/Volumes/{{catalog}}/staging/uc_6/raw/",
      "member_format": "gzip",
      "pre_extraction_decryption": {
        "type": "pgp_symmetric",
        "passphrase_secret": {
          "secret_catalog": "{{catalog}}",
          "secret_schema": "config",
          "secret_key": "pgpkey"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.member_format lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - An UNENCRYPTED .gz needs no ZIP handling whatsoever. Spark and Auto Loader decompress gzip natively on read — just point source_config.path at it and leave source_zip_handling disabled. Turning it on buys you nothing and costs a copy.
    - Use 'gzip' only when the .gz is wrapped in something Spark cannot read through, which in practice means encryption: .csv.gz.gpg.
    - A gzip stream holds exactly ONE member, so there is no member-selection step and no per-member pattern. The output file is named by stripping the envelope suffixes (.gpg/.pgp/.decrypted) and then the compression suffix (.gz/.gzip).

!!! warning "Known errors and limitations"

    **Extraction reports success, the file lands, and ingestion reads ZERO rows while the update still reports SUCCESS.**  
    *Cause:* The landed filename kept an envelope suffix (e.g. EA_REQUEST.csv.gz.gpg became EA_REQUEST.csv.gz.gpg.decompressed), so it matched no pathGlobFilter and Auto Loader silently ignored it.  
    *Fix:* Fixed in v1.7.4 — the suffix strip now removes .gpg/.pgp/.decrypted before the .gz. If you see this on an older build, check the actual filename in target_volume_path against your reader_options pathGlobFilter.

    **BadZipFile: File is not a zip file**  
    *Cause:* member_format is 'zip' (or absent) but the payload is a gzip stream.  
    *Fix:* Set member_format to 'gzip'.

**FAQs** (5)

??? question "If omitted · What container format is assumed if member_format is not set?"

    Default is `zip` -- the only pre-v1.7.4 behaviour: a real archive with a member table, opened by pyzipper. This is stated in both the JSON schema description and docs/02 section 5's table.

??? question "Format gotcha · What exact values does member_format accept and what happens with an unsupported one?"

    Only `zip` and `gzip` (`ALLOWED_SOURCE_MEMBER_FORMATS`). Per the v1.7.4 attribute delta, an unsupported value is rejected verbatim as: 'source_config.source_zip_handling.member_format '\''tar'\'' is not supported (known formats: ['\''gzip'\'', '\''zip'\''])'.

??? question "Performance impact · Is there any performance difference between zip and gzip member_format?"

    Not documented as a performance concern either way; the distinction is structural, not a cost one -- `zip` has a member table, `gzip` is a single stream with no such table, so pyzipper can only open the former.

??? question "Edge case · Can I set member_format to gzip and also set an archive password via secret_passphrase?"

    No -- rejected. A gzip stream has no archive-level password, so `pre_extraction_decryption.secret_passphrase` under `member_format: gzip` fails onboarding with: 'an AES password on a ZIP archive, meaningless for member_format '\''gzip'\'' (a gzip stream has no password)'. Use `pre_extraction_decryption.type` for an outer envelope instead.

??? question "Edge case · Do I need source_zip_handling at all for a plain unencrypted .gz file?"

    No -- an unencrypted `.gz` needs no ZIP handling whatsoever. Spark and Auto Loader decompress gzip natively on read; turning this on just adds a staging copy for nothing. Use `member_format: gzip` only when the `.gz` is wrapped in something Spark cannot read through, i.e. encrypted (`.csv.gz.gpg`).


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingmember-format) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_catalog` { #source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Depends on type.


Depends on type. Under 'pgp_symmetric' this is REQUIRED and is the passphrase the OpenPGP message itself was encrypted with (`gpg --symmetric`). Under 'pgp' it is OPTIONAL and protects the PRIVATE KEY above. Neither is the ZIP's AES password: that is secret_passphrase, below.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_catalog lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is passphrase_secret.secret_catalog required for pgp_symmetric, and what error do I get if it's missing?"

    Yes -- under `type: pgp_symmetric`, `passphrase_secret` is called with `required=True`, and `check_secret_ref` in turn requires `secret_catalog`; a missing value fails onboarding with an 'is required but was missing or empty' error.

??? question "Format gotcha · Is passphrase_secret the same field whether type is pgp or pgp_symmetric?"

    Same three-level shape (`secret_catalog`/`secret_schema`/`secret_key`), but different meaning: under `pgp_symmetric` it is the passphrase the message itself was encrypted with (required); under `pgp` it merely unlocks the private key above (optional).

??? question "Performance impact · Does resolving passphrase_secret.secret_catalog twice (for two flows) cost more?"

    Negligible -- each flow resolves its own reference independently as a single secret lookup; there's no documented caching or extra cost from reuse across flows.

??? question "Edge case · Can passphrase_secret and secret_passphrase be confused with each other?"

    Yes, easily -- they are different secrets despite the near-identical name. `passphrase_secret` unlocks the OpenPGP layer (message passphrase under `pgp_symmetric`, or private-key unlock under `pgp`); `secret_passphrase` is the unrelated AES password on the ZIP archive itself. Never inline either as a literal.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_key` { #source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-key }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Depends on type.


Depends on type. Under 'pgp_symmetric' this is REQUIRED and is the passphrase the OpenPGP message itself was encrypted with (`gpg --symmetric`). Under 'pgp' it is OPTIONAL and protects the PRIVATE KEY above. Neither is the ZIP's AES password: that is secret_passphrase, below.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_key lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if passphrase_secret.secret_key is left blank under pgp_symmetric?"

    Rejected -- `check_secret_ref(..., required=True)` requires `secret_key` as a non-empty string; a blank value fails onboarding for `type: pgp_symmetric`.

??? question "Format gotcha · Can I put the literal passphrase string directly as secret_key instead of a secret name?"

    No -- `secret_key` is only the name of a Unity Catalog secret, resolved at runtime; the Spec Builder additionally blocks `passphrase` as a forbidden literal key name so an inline passphrase is never accepted here.

??? question "Performance impact · Is there a performance cost difference between a short and long passphrase_secret.secret_key value?"

    Negligible -- it is just a lookup name string; the resolution cost is a single `dbutils.secrets.get` call regardless of the key name's length.

??? question "Edge case · What if the secret_key value points at a secret that doesn't exist?"

    Not validated at onboarding -- verify at runtime. `resolve_secret_value` raises `SecretResolutionError` when the catalog/schema/key doesn't exist or isn't resolvable, after trying the classic-scope fallback.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_schema` { #source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Depends on type.


Depends on type. Under 'pgp_symmetric' this is REQUIRED and is the passphrase the OpenPGP message itself was encrypted with (`gpg --symmetric`). Under 'pgp' it is OPTIONAL and protects the PRIVATE KEY above. Neither is the ZIP's AES password: that is secret_passphrase, below.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.passphrase_secret.secret_schema lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is passphrase_secret.secret_schema required under type pgp (asymmetric)?"

    Only if `passphrase_secret` is supplied at all -- under `pgp` the whole block is optional, but once present, `check_secret_ref` requires `secret_schema` (and the other two fields) same as any secret ref.

??? question "Format gotcha · Does secret_schema support dots or hyphens for nested schema names?"

    No -- `assert_safe_identifier` deliberately disallows `.`/`-` even though UC schema names permit more characters, closing off SQL-injection risk since these values get spliced into DDL/error labels.

??? question "Performance impact · Does secret_schema resolution add noticeable pipeline runtime?"

    Negligible -- it is part of the same single `dbutils.secrets.get` call as `secret_catalog`/`secret_key`; there is no separate schema-level lookup.

??? question "Edge case · What happens if secret_schema is right but rotation changed the underlying secret value?"

    Not validated at onboarding or by this framework -- secret rotation is a Unity Catalog concern; the framework always resolves the current value via `dbutils.secrets.get` at run time, so a rotated value is picked up automatically on the next resolution with no framework-side caching documented.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionpassphrase-secretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_catalog` { #source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_catalog lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is private_key_secret.secret_catalog required for type pgp?"

    Yes when `type` is `pgp` -- `check_secret_ref(..., required=True)` validates the whole `private_key_secret` block including `secret_catalog`, which is itself required inside a supplied secret ref.

??? question "Format gotcha · What is the three-level shape expected for private_key_secret?"

    `{secret_catalog, secret_schema, secret_key}` -- a Unity Catalog three-level secret reference, resolved via `dbutils.secrets.get(catalog=, schema=, key=)`, never a classic workspace scope (`check_secret_ref` in spec_validator.py).

??? question "Performance impact · Does resolving private_key_secret.secret_catalog add runtime overhead?"

    Negligible -- it is a single UC secret lookup per pipeline run/update, not per row; the cost is a fixed secret-resolution call, not proportional to data volume.

??? question "Edge case · Can I set private_key_secret when pre_extraction_decryption.type is pgp_symmetric?"

    No -- rejected. Under `pgp_symmetric` the validator errors: '`private_key_secret`: not valid for type '\''pgp_symmetric'\'' -- a passphrase-encrypted OpenPGP message has no recipient keypair. Use type '\''pgp'\'' for a key-encrypted message.'


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_key` { #source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-key }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_key lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What error appears if private_key_secret.secret_key is missing under type pgp?"

    It is required inside a supplied `private_key_secret` block -- `check_secret_ref` calls `check_string(..., required=True)` on `secret_key`, so a missing value fails with an 'is required but was missing or empty' error at that path.

??? question "Format gotcha · Is secret_key just the UC secret name, or does it include the key material?"

    It is only the UC secret name -- the third segment of `catalog.schema.key` -- resolved at runtime via `dbutils.secrets.get(catalog=, schema=, key=)`. The private key material itself lives in Unity Catalog, never in the spec.

??? question "Performance impact · Does grants checking for secret_key add latency to onboarding?"

    No -- onboarding only validates shape (non-empty string), not that the secret exists or that the caller has grants. Grant/permission failures surface at runtime as a `SecretResolutionError`, not at onboarding.

??? question "Edge case · What if the caller lacks READ SECRET grants on this secret_key at runtime?"

    `resolve_secret_value` raises `SecretResolutionError` naming the qualified `catalog.schema.key` label; this is a runtime permissions failure and is not validated at onboarding -- verify at runtime.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_schema` { #source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.private_key_secret.secret_schema lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is private_key_secret.secret_schema required, and what if I skip it?"

    Yes, required whenever `private_key_secret` is supplied -- `check_secret_ref` requires all three of `secret_catalog`, `secret_schema`, `secret_key`; omitting any one fails onboarding with a missing-field error.

??? question "Format gotcha · Is secret_schema case sensitive?"

    The framework's own identifier guard (`assert_safe_identifier`) only allows `[A-Za-z0-9_]` and treats the value literally -- it does not fold case, so `Security` and `security` are different schema names to `dbutils.secrets.get`.

??? question "Performance impact · Does splitting the private key secret across secret_catalog/secret_schema/secret_key cost extra lookups?"

    No -- all three segments are combined into one `dbutils.secrets.get(catalog=, schema=, key=)` call; it is a single resolution, not three.

??? question "Edge case · What happens if secret_schema is correct but the metastore has UC secrets disabled?"

    The UC lookup raises `UC_SECRETS_NOT_ENABLED`, and `resolve_secret_value` falls back to a classic workspace scope, trying `catalog.schema`, `catalog_schema`, `schema`, then `catalog` in that order with the same `secret_key`. See docs/05 section 4.2.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionprivate-key-secretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_catalog` { #source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time.


AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time. Independent of, and combinable with, the PGP layer above. Not the PGP key's passphrase: that is passphrase_secret.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_catalog lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What if secret_passphrase is omitted entirely?"

    It is fully optional at every level -- absent means the ZIP archive itself has no AES password, i.e. a plain ZIP. `_validate_pre_extraction_decryption` only checks it `if config.get("secret_passphrase") is not None`.

??? question "Format gotcha · Is secret_passphrase a top-level ZIP setting or nested under type?"

    It sits directly under `pre_extraction_decryption`, as a sibling of `type` -- not nested inside `type: pgp`/`pgp_symmetric`. It is independent of, and combinable with, the PGP layer above.

??? question "Performance impact · Does using secret_passphrase alongside PGP decryption double the decryption cost?"

    It adds one more resolution and one more decrypt step (PGP-decrypt the envelope, then AES-unlock the ZIP), proportional to file size; not flagged as a documented performance concern beyond that extra pass.

??? question "Edge case · Can secret_passphrase be set when member_format is gzip?"

    No -- rejected. A gzip stream has no archive password, so setting `secret_passphrase` under `member_format: gzip` fails onboarding: 'an AES password on a ZIP archive, meaningless for member_format '\''gzip'\'' (a gzip stream has no password)'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_key` { #source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-key }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time.


AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time. Independent of, and combinable with, the PGP layer above. Not the PGP key's passphrase: that is passphrase_secret.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_key lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · If I set secret_catalog and secret_schema for secret_passphrase but forget secret_key, what happens?"

    Rejected -- once `secret_passphrase` is supplied, `check_secret_ref` requires all three fields including `secret_key` as a non-empty string; a missing one fails onboarding.

??? question "Format gotcha · Does secret_passphrase.secret_key name the archive password itself or a UC secret?"

    It names a Unity Catalog secret holding the AES password on the ZIP archive -- resolved by pyzipper at extraction time -- never the literal password inline.

??? question "Performance impact · Is there overhead from resolving a ZIP archive password on every extraction?"

    Negligible -- one secret lookup per archive extraction, not per row or per member; cost does not scale with archive contents.

??? question "Edge case · Is secret_passphrase.secret_key the same secret as decrypted_columns[].secret.secret_key or encrypted_columns[].secret.secret_key?"

    Not necessarily -- they are independent secret references that happen to share the same three-field shape (`check_secret_ref`). One protects the ZIP archive; the others protect AES-encrypted column values. Point them at the same UC secret only if you intend to reuse one key across concerns.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_schema` { #source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time.


AES password on the ZIP ARCHIVE itself, resolved by pyzipper at extraction time. Independent of, and combinable with, the PGP layer above. Not the PGP key's passphrase: that is passphrase_secret.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.secret_passphrase.secret_schema lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What's the effect of leaving secret_passphrase.secret_schema unset while secret_catalog is set?"

    Rejected -- `check_secret_ref` validates `secret_passphrase` as a whole object once present, requiring `secret_schema` alongside `secret_catalog` and `secret_key`; a partial secret ref fails onboarding.

??? question "Format gotcha · Can secret_passphrase.secret_schema be an empty string to mean 'no schema'?"

    No -- `check_string` treats `None` or `""` as missing when `required=True`, so an empty string is rejected the same as an absent field, not accepted as a no-op.

??? question "Performance impact · Does the three-level namespace (secret_catalog/secret_schema/secret_key) for the ZIP password affect extraction speed?"

    No -- it is resolved once per archive via a single `dbutils.secrets.get` call; the namespace depth does not add measurable overhead.

??? question "Edge case · What happens if secret_schema is misspelled and doesn't match any real UC schema?"

    Not validated at onboarding -- verify at runtime. The UC lookup fails and, on a metastore with UC secrets enabled, there is no classic-scope fallback attempted for a genuinely wrong schema name; `resolve_secret_value` raises `SecretResolutionError`.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptionsecret-passphrasesecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `source_config.source_zip_handling.pre_extraction_decryption.type` { #source-configsource-zip-handlingpre-extraction-decryptiontype }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-ver">v1.7.4+</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Which OpenPGP decryption to apply to the landing file before it is unpacked.


'pgp' and 'pgp_symmetric' are not two ways to do one thing — they decrypt two structurally different messages. A key-encrypted message carries a PKESK packet addressed to a recipient keypair; a passphrase-encrypted one carries a SKESK packet derived from a shared secret. Neither can open the other, which is why this is one select rather than an optional extra secret.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `pgp`, `pgp_symmetric` | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    // Symmetric — what `gpg --symmetric --cipher-algo AES256 file` produces.
    "pre_extraction_decryption": {
      "type": "pgp_symmetric",
      "passphrase_secret": {
        "secret_catalog": "{{catalog}}",
        "secret_schema": "config",
        "secret_key": "pgpkey"
      }
    }
    
    // Asymmetric — encrypted to your public key by the sender.
    "pre_extraction_decryption": {
      "type": "pgp",
      "private_key_secret": { "...": "the recipient private key" },
      "passphrase_secret":  { "...": "optional: unlocks that key" }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.pre_extraction_decryption.type lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Three different secrets live in this block and they are easy to confuse. Under 'pgp_symmetric', passphrase_secret is the passphrase THE MESSAGE was encrypted with. Under 'pgp', passphrase_secret unlocks the PRIVATE KEY. In both cases secret_passphrase (note the reversed name) is something else entirely: the AES password on a ZIP archive.
    - Store the passphrase as a Unity Catalog secret and reference it. Never inline the literal — the builder blocks 'passphrase' as a forbidden key name for this reason.
    - Verified against GnuPG 2.4.9 in both directions: the framework decrypts what gpg wrote, and gpg decrypts what the framework wrote.

!!! warning "Known errors and limitations"

    **CryptoError: Symmetric PGP decryption failed**  
    *Cause:* Wrong passphrase, or the message is key-encrypted rather than passphrase-encrypted.  
    *Fix:* Confirm the message type with `gpg --list-packets`. A SKESK packet means symmetric ('pgp_symmetric'); a PKESK packet means asymmetric ('pgp').

**FAQs** (4)

??? question "If omitted · What happens if pre_extraction_decryption.type is left out?"

    Omit `type` when the file has no outer decryption layer at all -- the block is fully optional, and `_validate_pre_extraction_decryption` no-ops when the whole config is `None` or `{}`.

??? question "Format gotcha · What are the only two legal values for pre_extraction_decryption.type?"

    `pgp` and `pgp_symmetric` (`ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES`), checked via `check_string` with `allowed_values` -- any other value fails with an 'invalid value ... allowed values are' error.

??? question "Performance impact · Is PGP decryption before extraction expensive?"

    Not documented as a performance concern in the validator or docs; it runs once per landed archive before the ZIP is opened, on the raw bytes -- cost scales with file size, same as any decrypt-then-read step.

??? question "Edge case · Why can't type just be a shared optional secret instead of two enum values?"

    'pgp' and 'pgp_symmetric' decrypt two structurally different OpenPGP messages (PKESK vs SKESK packets) that cannot open each other, so this is one mutually-exclusive select rather than an optional extra secret on a single type.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingpre-extraction-decryptiontype) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.source_zip_path` { #source-configsource-zip-handlingsource-zip-path }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Directory where ZIP files are found — never a single file.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "source_zip_handling": {
          "source_zip_path": "/Volumes/{{catalog}}/landing/zone/incoming/"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.source_zip_path lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What error do I get if source_zip_path is missing while ZIP handling is enabled?"

    Onboarding rejects it: `check_string` runs with `required=True`, so a missing or empty `source_zip_path` fails with an 'is required but was missing or empty' style error at that path.

??? question "Format gotcha · Can source_zip_path point at a single ZIP file instead of a directory?"

    No -- it must be a directory. The validator docstring is explicit: `source_zip_path` is a landing directory, never a single file, because a real landing zone accumulates more than one archive between updates.

??? question "Performance impact · Does the size of source_zip_path's directory affect onboarding time?"

    Negligible -- onboarding only checks that the field is a non-empty string; it does not list or scan the directory. Runtime listing cost is a pipeline-execution concern, not an onboarding one.

??? question "Edge case · Can two ingestion flows share the same source_zip_path with different retention or zip settings?"

    No -- more than one distinct `landing_retention_policy` or `source_zip_handling` regime across flows sharing the same source path is rejected, per schema comment (20): both are destructive, path-scoped side effects, so two regimes on one directory is a data-loss race.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingsource-zip-path) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.source_zip_handling.target_volume_path` { #source-configsource-zip-handlingtarget-volume-path }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

The directory archive members are extracted into before ingestion reads them.


Extraction and ingestion are two separate steps; this is the handover point between them.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    "source_zip_handling": {
      "enabled": true,
      "source_zip_path": "/Volumes/{{catalog}}/landing/cdr/incoming/",
      "target_volume_path": "/Volumes/{{catalog}}/landing/cdr/extracted/"
    }
    // source_config.path must equal target_volume_path
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.target_volume_path lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - This must match source_config.path exactly. Nothing validates that they agree.

!!! warning "Known errors and limitations"

    **Extraction succeeds, ingestion reads zero rows, update reports SUCCESS**  
    *Cause:* This path and source_config.path point at different directories, so the reader watches an empty directory.  
    *Fix:* Make the two identical. This is the single most common silent misconfiguration in ZIP flows.

**FAQs** (4)

??? question "If omitted · What happens if target_volume_path is left blank on an enabled ZIP handling block?"

    It is required when `enabled` is true, checked with `check_string(..., required=True)`; omitting it fails onboarding with a missing/empty-field error.

??? question "Format gotcha · Does target_volume_path need to exactly match source_config.path?"

    Yes, but nothing enforces it: the docs/02 sample states `source_config.path must equal target_volume_path`, and the tip is explicit that nothing validates that they agree -- a mismatch is a silent runtime failure, not an onboarding rejection.

??? question "Performance impact · Is there a performance cost to using a separate target_volume_path from source_zip_path?"

    Negligible at onboarding; at runtime the extraction step writes one copy of each member to this directory before ingestion reads it, which is an unavoidable extra write for any ZIP/gzip pipeline, documented as the handover point between extraction and ingestion.

??? question "Edge case · What happens if target_volume_path and source_config.path point at different directories?"

    Extraction reports success, but ingestion reads zero rows while the update still reports SUCCESS -- the reader watches an empty directory. This is called out as the single most common silent misconfiguration in ZIP flows; make the two paths identical.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingtarget-volume-path) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html)


---

### `source_config.source_zip_handling.zip_file_pattern` { #source-configsource-zip-handlingzip-file-pattern }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · ZIP handling</span>

Which ZIP files to pick up.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "source_zip_handling": {
          "zip_file_pattern": "*.zip"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.source_zip_handling.zip_file_pattern lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if zip_file_pattern is omitted when source_zip_handling is enabled?"

    It is required whenever `enabled` is true; the validator calls `check_string(..., required=True)` and onboarding rejects the flow with a missing/empty-field error if it is absent.

??? question "Format gotcha · Is zip_file_pattern a regex or a glob, and is it case sensitive?"

    It is a glob, matched the same way `file_pattern`/`pathGlobFilter` selects ingested files (e.g. `orders_*.zip` or `*.zip`), per the validator docstring. Matching is filesystem-glob semantics, so case sensitivity follows the underlying storage, not a documented framework override.

??? question "Performance impact · Does a broad zip_file_pattern like *.zip slow down extraction?"

    It can pick up more archives per run than intended, but the cost is proportional to files matched, not the pattern itself -- there is no documented per-pattern overhead beyond scanning `source_zip_path` for matches.

??? question "Edge case · Does zip_file_pattern apply to the extracted files too, or only the archives?"

    Only to the archives in `source_zip_path`. The extracted files in `target_volume_path` are picked up by `source_config.path`/`file_pattern` for ingestion -- a separate, unrelated pattern. See docs/02 section 5's note distinguishing `zip_file_pattern` from top-level `file_pattern`.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#source-zip-handling-source_zip_handling) · [Schema tree](tree.md#tree-ingestion-source-configsource-zip-handlingzip-file-pattern) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc volumes](https://docs.databricks.com/connect/unity-catalog/volumes.html) · [files api](https://docs.databricks.com/api/workspace/files)


---

### `source_config.starting_version` { #source-configstarting-version }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source · zerobus</span>

Maps to reader option startingVersion.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "starting_version": 0
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_config.starting_version lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if starting_version is not set on a zerobus source?"

    No `startingVersion` option is applied at all — the code only sets it `if source_config.get("starting_version") is not None`, so the stream uses Delta's own default starting point (typically the latest/current version at first attach).

??? question "Format gotcha · Should starting_version be a quoted string like 'latest' or a plain integer?"

    The onboarding schema types it as an integer (a specific Delta table version number). The docs example shows `"starting_version": "latest"` as a string, which is a different literal Delta reader keyword — check the type your onboarding form actually sends before assuming both spellings validate the same way.

??? question "Performance impact · Does starting_version at 0 vs a recent version change initial load cost?"

    Yes materially — starting from an early version means the stream catches up through every intervening Delta version before reaching current state, which is far more expensive than starting near the latest version. Pair with `max_bytes_per_trigger` to bound the catch-up rate.

??? question "Edge case · What happens if starting_version points at a version that has already been vacuumed?"

    Not validated at onboarding — verify at runtime; a `VACUUM`-removed version will fail the stream at startup since Delta can no longer reconstruct that snapshot.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configstarting-version) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [structured streaming](https://docs.databricks.com/structured-streaming/index.html) · [delta change data feed](https://docs.databricks.com/delta/delta-change-data-feed.html)


---

### `source_database` { #source-database }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Flow identity</span>

Descriptive metadata.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_database`


=== "JSON"

    ```json
    {
      "source_database": "example_landing_db"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_database is persisted as its own column
    SELECT dataflow_id,
           source_database,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if source_database is not provided?"

    Nothing functional — it is purely descriptive metadata stored in the control table, never validated or read by the engine. It is optional and has no default beyond being absent.

??? question "Format gotcha · Does source_database need to match a real Unity Catalog schema name?"

    No — it is free text (e.g. `example_landing_db`), stored as-is; it has no relationship to `target_catalog`/`target_schema` and is not cross-checked against anything.

??? question "Performance impact · Does setting source_database affect pipeline runtime at all?"

    None — it is stored in the control table purely for documentation/observability purposes and never touched by the DAG-building or execution code.

??? question "Edge case · Is source_database used anywhere in observability views or dashboards?"

    It feeds descriptive metadata in control tables generally, per docs/00 §Flow identity; check `docs/17_framework_observability_and_genie.md` if you need the exact observability view column it surfaces in — verify the specific view mapping at runtime if unsure.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-source-database) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_description` { #source-description }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Flow identity</span>

Becomes the target Delta table's COMMENT.


Becomes the target Delta table's COMMENT. Supports {{catalog}} and {{env}}.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_description` · **Behaviour changed in** v1.7.4


=== "JSON"

    ```json
    {
      "source_description": "Bronze ingestion from a Volume in the {{env}} environment"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_description is persisted as its own column
    SELECT dataflow_id,
           source_description,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.

**FAQs** (4)

??? question "If omitted · What happens if source_description is left blank?"

    The target Delta table simply gets no `COMMENT`, or an empty one — it is optional descriptive metadata with no required-field error.

??? question "Format gotcha · Can source_description contain single quotes or other characters that might break the table COMMENT DDL?"

    It supports `{{catalog}}` and `{{env}}` template placeholders, resolved via a plain raw-text `.replace()` over the whole spec at onboarding time before parsing — avoid embedding unescaped single quotes, since the value ultimately lands inside a SQL `COMMENT '...'` string.

??? question "Performance impact · Does a long source_description affect anything at runtime?"

    Negligible — it is stored once as the target Delta table's COMMENT metadata, not evaluated per row or per micro-batch.

??? question "Edge case · Does {{env}} in source_description get resolved the same way as {{catalog}}?"

    Both are substituted at onboarding time by `substitute_environment_placeholders`, a literal string replace over the raw spec text — so both tokens resolve identically wherever they appear in the document, not just inside this field.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-source-description) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_system` { #source-system }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Flow identity</span>

Descriptive metadata — never validated.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_system`


=== "JSON"

    ```json
    {
      "source_system": "example_source_system"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_system is persisted as its own column
    SELECT dataflow_id,
           source_system,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if source_system is omitted?"

    Nothing — it is descriptive metadata that is never validated, per its own purpose text: 'Descriptive metadata — never validated.' It is stored in the control table if supplied.

??? question "Format gotcha · Is there a required format or enum for source_system?"

    No — it is free text, e.g. `"SAP_ERP"` or `"excalibur"`; any non-empty string is accepted, with no allowed-values restriction.

??? question "Performance impact · Does source_system's value influence pipeline scheduling or routing?"

    No — it carries no runtime behaviour; it is purely a labeling field for humans and observability/reporting.

??? question "Edge case · Is source_system the same as governance_tags.table_tags['source_system']?"

    They are two independent places a similar concept can be recorded — `source_system` is a dedicated flow-identity field, while `table_tags` can carry an arbitrary key like `source_system` as a free-form governance tag (see docs/16_usecase_implementation_guide.md's example). Setting one does not populate the other.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-source-system) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_table_name` { #source-table-name }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Flow identity</span>

Descriptive metadata.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.source_table_name`


=== "JSON"

    ```json
    {
      "source_table_name": "example_raw_table"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_table_name is persisted as its own column
    SELECT dataflow_id,
           source_table_name,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if source_table_name is not filled in?"

    Nothing functional — it is descriptive metadata stored in the control table, with no validation or default beyond being optional.

??? question "Format gotcha · Should source_table_name be a fully qualified three-part name?"

    No — it is free text describing the source object logically (e.g. `example_raw_table`), not a real three-part `catalog.schema.table` reference the engine resolves; it is never cross-checked against `source_config.path`/`source_table`.

??? question "Performance impact · Does source_table_name affect ingestion performance?"

    None — it is a label only, never read by the reader or DAG-building code.

??? question "Edge case · Does source_table_name need to match source_config.source_table for a zerobus flow?"

    No relationship is enforced — `source_table_name` is purely descriptive metadata while `source_config.source_table` is the actual Delta table name the Zerobus reader attaches to; they can differ with no error.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-source-table-name) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_type` { #source-type }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-only">Source type</span>

autoloader reads files from a Volume, zerobus streams an existing Delta table, asn1 decodes binary CDR files.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `autoloader`, `zerobus`, `asn1` | — |

**Persisted in** `config.ingestion_flow_spec.source_type`


=== "JSON"

    ```json
    {
      "source_type": "autoloader"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- source_type is persisted as its own column
    SELECT dataflow_id,
           source_type,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Allowed values: autoloader, zerobus, asn1.

**FAQs** (4)

??? question "If omitted · What happens if source_type is missing on an ingestion flow?"

    Onboarding rejects it: `source_type: is required but was missing or empty` — every ingestion flow must declare one.

??? question "Format gotcha · Can I still use the old gcs_autoloader value for source_type?"

    No — the schema's own migration notes record that `source_type: "gcs_autoloader"` was renamed to `"autoloader"`; only `autoloader`, `zerobus`, `asn1` are in `ALLOWED_SOURCE_TYPES` today, so the old spelling fails the allowed-values check.

??? question "Performance impact · Does source_type choice affect whether the flow streams or batches?"

    No — per `docs/12_module_permutation_matrix.md`, `is_streaming` is decided purely by `target_type == "streaming_table"`; all three readers (`read_autoloader_source`, `read_zerobus_source`, `read_asn1_source`) call `spark.readStream` regardless of `source_type`.

??? question "Edge case · Can any source_type feed any target_type?"

    Yes — the validator enforces `source_type` and `target_type` independently; there is no `source_type` x `target_type` allow-list. The one structural cross-check is that `cdc_load_strategy: SCD3` is rejected on every ingestion flow regardless of `source_type`, since SCD3 only makes sense downstream of a raw ingestion flow. See docs/12 §1.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-source-type) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [auto loader](https://docs.databricks.com/ingestion/auto-loader/index.html)


---

### `target_catalog` { #target-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

Unity Catalog catalog for the target table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_catalog`


=== "JSON"

    ```json
    {
      "target_catalog": "{{catalog}}"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_catalog is persisted as its own column
    SELECT dataflow_id,
           target_catalog,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if target_catalog is not set?"

    Onboarding rejects it: `target_catalog: is required but was missing or empty` on both ingestion and transformation flows.

??? question "Format gotcha · Can target_catalog use the {{catalog}} template placeholder?"

    Yes — the sample spec uses `"target_catalog": "{{catalog}}"`, resolved via the same raw-text substitution as `{{env}}` at onboarding time before the document is parsed.

??? question "Performance impact · Does target_catalog choice affect runtime cost?"

    None directly — it only determines the Unity Catalog namespace the target table registers in; cost is driven by the actual data volume and `target_type`/`cdc_load_strategy`, not the catalog name.

??? question "Edge case · Does target_catalog also feed the auto-derived schema_location default?"

    Yes — for `autoloader`/`asn1` sources, an omitted `source_config.schema_location` is auto-derived as `/Volumes/<target_catalog>/landing/_schemas/<target_table>/`, using this same flow's `target_catalog` and `target_table`. See `spec_validator.py`'s schema-location derivation.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.auto_ttl.expire_in_days` { #target-configauto-ttlexpire-in-days }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Rows older than this many days are auto-deleted.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | — | — | min `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "auto_ttl": {
          "expire_in_days": 90
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.auto_ttl.expire_in_days lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if I set timestamp_column but leave out expire_in_days?"

    Auto TTL is treated as not configured at all — `build_auto_ttl_kwarg` logs a WARNING and returns `None` when either sub-field is missing; supplying only one half is not a validation error, it just means TTL never applies.

??? question "Format gotcha · Can expire_in_days be 0 to expire rows immediately?"

    No — the schema sets `minimum: 1`, and `_validate_auto_ttl` calls `check_int(..., minimum=1)`. `expire_in_days: 0` is a hard onboarding error, not a valid 'expire everything now' value.

??? question "Performance impact · Does auto TTL deletion add ongoing overhead to every pipeline run?"

    Not benchmarked in this repo, but it is implemented as a Delta/Lakeflow row-level auto-TTL decorator kwarg (`storage/table_properties.py::build_auto_ttl_kwarg`) rather than an ad hoc DELETE job, so it runs as part of normal table maintenance rather than a separate expensive scan per update.

??? question "Edge case · Can I use auto_ttl.expire_in_days together with cdc_load_strategy SCD2?"

    No — `_validate_auto_ttl` hard-errors unless `cdc_load_strategy` is `APPEND` or `TRUNCATE_AND_LOAD`: 'only supported for cdc_load_strategy in [\'APPEND\', \'TRUNCATE_AND_LOAD\']'. On any CDC-dispatched strategy, remove the `auto_ttl` block or handle expiry in a separate maintenance job.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configauto-ttlexpire-in-days) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.auto_ttl.timestamp_column` { #target-configauto-ttltimestamp-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

The column whose value decides how old a row is, for automatic expiry.


auto TTL deletes rows whose timestamp is older than expire_in_days.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    "auto_ttl": { "timestamp_column": "updated_at", "expire_in_days": 90 }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.auto_ttl.timestamp_column lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Must be DATE, TIMESTAMP or TIMESTAMP_NTZ.
    - Valid only for APPEND and TRUNCATE_AND_LOAD — a hard error on CDC strategies.
    - Deletion is permanent. Confirm your retention policy before enabling it.

!!! warning "Known errors and limitations"

    **Onboarding fails with a hard error on a CDC flow**  
    *Cause:* auto TTL is incompatible with CDC-dispatched strategies.  
    *Fix:* Remove the auto_ttl block, or move expiry into a separate maintenance job.

**FAQs** (4)

??? question "If omitted · What happens if auto_ttl is present but timestamp_column is left blank?"

    Auto TTL is not applied — `build_auto_ttl_kwarg` treats an incomplete block (missing `timestamp_column` or `expire_in_days`) the same as 'not configured', logging a WARNING rather than failing, since skipping TTL has no correctness impact.

??? question "Format gotcha · What column types are valid for auto_ttl.timestamp_column?"

    It must be `DATE`, `TIMESTAMP` or `TIMESTAMP_NTZ` per the inspector tip; `spec_validator.py` only checks it is a non-empty string, so an incompatible column type is caught at pipeline runtime, not onboarding.

??? question "Performance impact · Does the choice of timestamp_column affect scan cost for TTL expiry?"

    Not documented beyond the general auto-TTL mechanism; a well-indexed/partitioned timestamp column would be the natural choice but this is not enforced or benchmarked here.

??? question "Edge case · Can I use auto_ttl.timestamp_column on an SCD1 or SCD2 target?"

    No — same restriction as `expire_in_days`: `_validate_auto_ttl` only allows `APPEND`/`TRUNCATE_AND_LOAD`. Setting it on any CDC-dispatched strategy is a hard onboarding error naming the incompatible strategy.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configauto-ttltimestamp-column) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns` { #target-configencrypted-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Sets target_config.encrypted_columns[].


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.

**FAQs** (4)

??? question "If omitted · What happens if encrypted_columns is absent?"

    Nothing is encrypted; every column is written in clear. `_validate_encrypted_columns` returns when the list is `None`.

??? question "Format gotcha · What shape does each encrypted_columns entry need?"

    An object with `column_name` and `secret` (both required) plus optional `output_column`, `mode` and `source_data_type`. A non-list value is rejected with `expected a list of column configs`.

??? question "Performance impact · How much does column encryption cost per update?"

    One `aes_encrypt` per row per listed column, plus a cast to string first. Ciphertext is binary and larger than the plaintext, so storage and downstream scans grow slightly. The key is resolved once per flow.

??? question "Edge case · Why does an encrypted column make SCD2 create a new version every run?"

    GCM and CBC use a fresh IV per call, so the ciphertext of an unchanged value differs on every write. If the column is in `columns_to_check` (or in a reconciliation's `compare_columns`), every row looks changed. Exclude it via `columns_to_exclude`, or use `ECB` when deterministic ciphertext is acceptable.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columns) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].column_name` { #target-configencrypted-columnscolumn-name }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Plaintext column on this flow's DataFrame to encrypt.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "column_name": "pii_column"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns[].column_name lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · Is column_name mandatory?"

    Yes: `...encrypted_columns[<n>].column_name: is required but was missing or empty`.

??? question "Format gotcha · Should I use the source column name or the post-normalisation name?"

    The name as it exists in the DataFrame when encryption runs, which is after `column_normalization` and standardisation on an ingestion flow. It must also pass `assert_safe_identifier`.

??? question "Performance impact · Does encrypting a wide column cost more than a narrow one?"

    Yes, linearly with plaintext size: the value is cast to string and enciphered whole. Encrypt the sensitive column only, not a struct that contains it.

??? question "Edge case · What if the column is not present at run time?"

    The update fails with `Column '<name>' configured for encryption not present in DataFrame` wrapped in a `CryptoError`. Onboarding cannot see the schema, so a rename upstream surfaces here.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnscolumn-name) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].mode` { #target-configencrypted-columnsmode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

AES cipher mode.


AES cipher mode. GCM is recommended — it adds a random IV.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `GCM`, `CBC`, `ECB` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "mode": "GCM"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns[].mode lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: GCM, CBC, ECB.

**FAQs** (4)

??? question "If omitted · Which AES mode applies when mode is omitted?"

    `GCM` (`col_config.get("mode", "GCM")`), which also authenticates the ciphertext. The validator only checks the key when it is present.

??? question "Format gotcha · Is `gcm` in lower case accepted?"

    No. `ALLOWED_AES_MODES = {"GCM", "CBC", "ECB"}` is matched exactly at onboarding; `gcm` is rejected as not an allowed value.

??? question "Performance impact · Which mode is cheapest?"

    All three are single-pass ciphers; GCM adds a tag computation. The measurable difference is in storage, not CPU: GCM carries a 12-byte IV and a 16-byte tag per value, CBC a 16-byte IV, ECB neither.

??? question "Edge case · When is ECB the right choice despite being deterministic?"

    When two tables must join on the encrypted value without decrypting, or when the column feeds `columns_to_check`, hashing or reconciliation comparisons: only ECB yields the same ciphertext for the same plaintext. Accept that equal values are visible as equal.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnsmode) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].output_column` { #target-configencrypted-columnsoutput-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Output column name.


Output column name. Defaults to column_name — set the same name to encrypt in place.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "output_column": "pii_column"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns[].output_column lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · Where does the ciphertext go if output_column is not set?"

    It replaces the source column in place: `output_column` defaults to `column_name` in `apply_aes_column_encryption`, and the plaintext is gone from the target.

??? question "Format gotcha · Any restriction on the output column name?"

    It must pass `assert_safe_identifier` at run time; onboarding checks only that it is a string.

??? question "Performance impact · Does keeping both plaintext and ciphertext columns cost much?"

    One extra binary column per row. The real cost is the security one: the plaintext is still in the table.

??? question "Edge case · Which name does the downstream decrypted_columns entry use?"

    This one. The `original_data_type` tag is recorded against `output_column`, and `decrypted_columns[].cast_to_type` is checked against that tag. Point `column_name` on the decrypt side at `output_column` from the encrypt side.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnsoutput-column) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].secret.secret_catalog` { #target-configencrypted-columnssecretsecret-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "secret": {
        "secret_catalog": "{{catalog}}"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns[].secret.secret_catalog lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What if secret_catalog is missing on an encrypted column?"

    `check_secret_ref` reports `...secret.secret_catalog: is required but was missing or empty` and onboarding fails.

??? question "Format gotcha · Can I pass a classic secret scope here?"

    No. The reference is a Unity Catalog three-level secret resolved with `dbutils.secrets.get(catalog=, schema=, key=)`; there is no scope form anywhere in the framework.

??? question "Performance impact · Is the secret fetched on every row or every micro-batch?"

    Neither. It is resolved once at graph definition per column config and embedded as a literal in the encryption expression.

??? question "Edge case · Can encryption keys live in a catalog other than the data catalog?"

    Yes. Many teams keep a dedicated security catalog. The pipeline's run-as identity must be able to read the secret there; onboarding does not check grants.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssecretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.encrypted_columns[].secret.secret_key` { #target-configencrypted-columnssecretsecret-key }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "secret": {
        "secret_key": "pii_encryption_key"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns[].secret.secret_key lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens when secret_key is missing?"

    Onboarding rejects the column: `...secret.secret_key: is required but was missing or empty`.

??? question "Format gotcha · What must the secret contain, and is its length validated?"

    The AES key bytes; Spark's `aes_encrypt` accepts 16, 24 or 32-byte keys. Onboarding validates only the reference, not the key, so a wrong-length key fails at graph definition.

??? question "Performance impact · Is a 32-byte key slower than a 16-byte key?"

    Marginally more rounds per block, not measurable against I/O. Choose the length by policy, not speed.

??? question "Edge case · What happens if I rotate the secret value in place?"

    New writes use the new key immediately; rows already encrypted with the old key cannot be decrypted by the downstream `decrypted_columns` entry, which fails the consumer's update. Follow the rotation runbook in docs/05 §5 and re-encrypt before switching consumers.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssecretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.encrypted_columns[].secret.secret_schema` { #target-configencrypted-columnssecretsecret-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "secret": {
        "secret_schema": "security"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns[].secret.secret_schema lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · Is secret_schema optional?"

    No: `...secret.secret_schema: is required but was missing or empty`.

??? question "Format gotcha · How should the schema be written?"

    The bare schema name, unquoted, case as created in Unity Catalog.

??? question "Performance impact · Does the schema choice matter for performance?"

    No; it is part of a single lookup made once per flow.

??? question "Edge case · What if the schema exists but the pipeline cannot read the secret?"

    Graph definition fails when `dbutils.secrets.get` is denied; onboarding passes because grants are not visible to the validator. Grant the pipeline principal before the first update.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssecretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.encrypted_columns[].source_data_type` { #target-configencrypted-columnssource-data-type }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-ver">v1.4.0+</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Optional.


Optional. The column's original Spark type before encryption replaces it with ciphertext binary (string, decimal(18,2), timestamp, ...). Becomes the Unity Catalog original_data_type tag that a downstream decrypted_columns.cast_to_type is checked against. Leave blank to use the type observed at encryption time; declare it to make a silent source type change fail loudly instead.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "source_data_type": "string"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.encrypted_columns[].source_data_type lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · Is source_data_type needed?"

    No. When omitted the framework records the type Spark observes at encryption time as the `original_data_type` tag, the pre-v1.4.0 behaviour. Declaring it only adds a check.

??? question "Format gotcha · How is the declared type compared with the real one?"

    Case-insensitively and whitespace-trimmed against Spark's reported type string, so `DECIMAL(18,2)` equals `decimal(18,2)`. Onboarding validates only that it is a non-empty string.

??? question "Performance impact · Does the declaration add run-time cost?"

    None beyond one string comparison per column per update.

??? question "Edge case · What does a mismatch look like?"

    The update fails loudly: `encrypted_columns source_data_type mismatch for column '<name>': the spec declares source_data_type='string' but Spark reports 'int'`. That is the point: a silent upstream type change becomes a visible failure instead of a wrong tag that breaks `cast_to_type` downstream.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssource-data-type) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.liquid_clustering_columns` { #target-configliquid-clustering-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Clustering keys for Delta liquid clustering.


Gives data skipping without the file-count problems of partitioning, and can be changed later without rewriting the table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    "liquid_clustering_columns": ["customer_id", "order_date"]
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.liquid_clustering_columns lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Maximum of three columns — enforced at onboarding and again at runtime.
    - Prefer this over partition_columns for anything under about 1 TB.
    - Choose the columns your queries actually filter on.

!!! warning "Known errors and limitations"

    **Onboarding rejects the flow**  
    *Cause:* More than three columns were listed.  
    *Fix:* Reduce to the three most selective predicates.

**FAQs** (4)

??? question "If omitted · What happens if liquid_clustering_columns is not set?"

    The target table is created with no liquid clustering — behaviourally identical to an explicitly empty list, though no diagnostic log line is emitted for the empty/absent clustering case (unlike `partition_columns`, which does log an INFO when explicitly empty).

??? question "Format gotcha · Can liquid_clustering_columns include more than 3 columns if I really need them?"

    No — capped at 3, Delta Liquid Clustering's own limit. `_validate_target_config` hard-rejects more than `MAX_LIQUID_CLUSTERING_COLUMNS` (3) at onboarding, and `build_partition_and_cluster_kwargs` re-enforces the identical guard at runtime as defense in depth.

??? question "Performance impact · Is liquid clustering cheaper than partitioning for a large table?"

    For anything under roughly 1 TB, Databricks guidance is to prefer liquid clustering over partitioning — it gives data skipping without the small-file problems partitioning causes, and its clustering keys can be changed later without rewriting the table.

??? question "Edge case · Does liquid_clustering_columns take effect on an SCD2 target?"

    No — per docs/03 §3, both `partition_columns` and `liquid_clustering_columns` only take effect for `cdc_load_strategy` in `{APPEND, TRUNCATE_AND_LOAD}`. On SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC, `apply_changes`/`apply_changes_from_snapshot` do not accept `cluster_by` at all, so the field is silently inert, not rejected.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configliquid-clustering-columns) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [liquid clustering](https://docs.databricks.com/delta/clustering.html)


---

### `target_config.partition_columns` { #target-configpartition-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Physical Hive-style partitioning of the target table.


Splits the table into directories by column value. Effective only for APPEND and TRUNCATE_AND_LOAD.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    "partition_columns": ["ingest_date"]
    
    // explicitly unpartitioned:
    "partition_columns": []
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.partition_columns lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Databricks guidance: do not partition tables under roughly 1 TB. Use liquid clustering instead.
    - An empty array is a deliberate, documented statement that the table is unpartitioned — it is not an error.
    - Partition on a low-cardinality column. Partitioning on an id produces millions of tiny files.

!!! warning "Known errors and limitations"

    **Queries got slower after partitioning**  
    *Cause:* Over-partitioning created many small files.  
    *Fix:* Drop the partitioning and switch to liquid_clustering_columns.

**FAQs** (4)

??? question "If omitted · Is omitting partition_columns different from setting it to an empty array?"

    Behaviourally identical — both produce an unpartitioned table, since `build_partition_and_cluster_kwargs` never passes an empty list through as `partition_cols=[]` (a zero-column `partitionBy()` has no defined behaviour). They are diagnostically distinct only: an explicitly empty list logs an INFO recording a deliberate opt-out; an omitted field logs nothing.

??? question "Format gotcha · Is there an alias like partition_by I can use instead of partition_columns?"

    No — docs/03 §3 is explicit: 'There is no alias: partition_by is not a recognized field name, in this release or any prior one.' Use `target_config.partition_columns` exactly.

??? question "Performance impact · Will partitioning a small table improve query performance?"

    Likely the opposite — Databricks guidance is not to partition tables under roughly 1 TB; use `liquid_clustering_columns` instead. Partitioning on a high-cardinality column like an id produces millions of tiny files, which is a documented failure mode here.

??? question "Edge case · Why does partition_columns appear to have no effect on my SCD1 target?"

    Partitioning is silently skipped for CDC-dispatched strategies — `apply_changes` does not accept `partition_cols`. This is expected behaviour, not a bug; use APPEND/TRUNCATE_AND_LOAD if you need a partitioned target, or accept an unpartitioned CDC target. See docs/03 §3.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configpartition-columns) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [partitioning](https://docs.databricks.com/tables/partitions.html) · [liquid clustering](https://docs.databricks.com/delta/clustering.html)


---

### `target_config.partition_mode` { #target-configpartition-mode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

absent omits partition_columns entirely.


absent omits partition_columns entirely. unpartitioned writes partition_columns: [] — an explicit, documented declaration that the table is not partitioned. named writes the columns you list below.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "partition_mode": "absent"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.partition_mode lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - [] means explicitly unpartitioned, never an error
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Allowed values: absent, unpartitioned, named.

**FAQs** (4)

??? question "If omitted · What happens if partition_mode is left unset in the Databricks App form?"

    It behaves as "absent" — `partition_columns` is omitted from the emitted spec entirely. This attribute is a Databricks App UI-only helper (`server/core/deserializer.py`), not a field the onboarding validator or JSON schema recognizes as a `target_config` key.

??? question "Format gotcha · Does partition_mode ever get written into the actual onboarding spec JSON?"

    No — it is a form-only concept with three states (`absent`, `unpartitioned`, `named`) that the app deserializes into the real spec attribute `target_config.partition_columns` (omitted, `[]`, or a named list respectively). Only `partition_columns` reaches `spec_validator.py`.

??? question "Performance impact · Does choosing unpartitioned vs absent in partition_mode change runtime cost?"

    No difference at all — both produce the same emitted spec behaviour (no `partition_cols` kwarg reaches the `@dlt.table` decorator); the distinction is purely diagnostic logging (an INFO line for the explicit `unpartitioned`/empty-list case), not a performance one.

??? question "Edge case · If I pick named in partition_mode but leave the column list empty, what is emitted?"

    Not directly documented for that exact combination; treat it as equivalent to `unpartitioned` (an empty `partition_columns: []`) since the underlying spec attribute is what actually matters — verify the app's exact deserialization behaviour in `server/core/deserializer.py` if precision matters.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configpartition-mode) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [partitioning](https://docs.databricks.com/tables/partitions.html) · [liquid clustering](https://docs.databricks.com/delta/clustering.html)


---

### `target_config.sink_config.export_trigger` { #target-configsink-configexport-trigger }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-ver">v1.7.5+</span> <span class="fx-badge fx-only">Target · sink config</span>

WHAT drives a pgp_zip export: one archive per micro-batch, or exactly one per pipeline update.


A Lakeflow sink is streaming-only, and Delta refuses to stream from a table that is fully recomputed each update (DELTA_SOURCE_TABLE_IGNORE_CHANGES). Together those two facts meant an AGGREGATING target -- a materialized_view, or any TRUNCATE_AND_LOAD flow -- could be computed and published and then had no way to leave the platform as a file. Not a missing feature: a structural contradiction. 'per_update' resolves it by separating the trigger (an update-scoped pulse carrying no data) from the payload (a batch read), so the append flow is genuinely streaming while the exported rows are an aggregation.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `per_micro_batch`, `per_update` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    // Exporting a GROUP BY result -- impossible before v1.7.5
    "sink_config": {
      "format": "pgp_zip",
      "path": "/Volumes/{{catalog}}/staging/uc_6/output/_staging/tel/",
      "staged_file_format": "csv",
      "export_trigger": "per_update",
      "post_export_archive": {
        "enabled": true,
        "output_zip_path": "/Volumes/{{catalog}}/staging/uc_6/output/",
        "archive_format": "gzip"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.export_trigger lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Omit this key for an append-only feed. 'per_micro_batch' is the default and the right answer whenever the source genuinely streams.
    - Reach for 'per_update' when the flow this sink reads AGGREGATES -- GROUP BY, DISTINCT, a windowed rollup. Those produce a materialized_view, which cannot be streamed from at all.
    - 'per_update' fires even on an update that ingested nothing, which is the point: a contractual feed must produce its file every cycle, not only when new rows arrived.
    - One temporary dataset, _flowx_export_pulse, is shared by every per_update sink in the pipeline. It appears once in the Lakeflow DAG and carries no business data.

!!! warning "Known errors and limitations"

    **FrameworkConfigError: Consumer '...' requested a streaming read of '<table>', which is produced in this same pipeline by flow '...' with cdc_load_strategy='TRUNCATE_AND_LOAD' / target_type='materialized_view'.**  
    *Cause:* A sink is trying to stream an aggregating target. Delta cannot stream a table that is fully overwritten each update.  
    *Fix:* Set export_trigger to 'per_update' on that sink. Do NOT relabel the producing flow as a streaming_table -- that silences a plan-time error and converts it into a runtime one.

**FAQs** (5)

??? question "If omitted · What happens if I don't set export_trigger on a pgp_zip sink?"

    It defaults to `per_micro_batch`, the only pre-v1.7.5 behaviour: the sink is fed from the flow's own staged view and produces one archive per micro-batch of an append-only stream. This is fine for any genuinely streaming source.

??? question "Format gotcha · Is export_trigger valid on a delta or kafka sink?"

    No. It is presence-rejected for `format` values other than `pgp_zip` with 'only meaningful for format pgp_zip ... whose write cadence Lakeflow itself owns. Remove the attribute.' Only `per_micro_batch` and `per_update` are legal values, and only on a `pgp_zip` sink.

??? question "Performance impact · Does per_update cost more than per_micro_batch per pipeline run?"

    Negligible extra cost: `per_update` adds one shared `_flowx_export_pulse` dataset (one row, no business data) per pipeline, reused by every `per_update` sink, so N sinks on it are N graph edges, not N streams. The real cost is producing exactly one archive per update regardless of whether new data arrived, which is the intended behaviour for a contractual feed.

??? question "Edge case · Why can't I export a materialized_view or TRUNCATE_AND_LOAD target through this sink without export_trigger?"

    A Lakeflow sink is streaming-only, and Delta refuses to stream a table that is fully recomputed each update (`DELTA_SOURCE_TABLE_IGNORE_CHANGES`). Set `export_trigger` to `per_update` so an update-scoped pulse drives the sink while the aggregated rows are read as a batch. See docs/06 'Export trigger'.

??? question "Edge case · Will per_update silently skip writing a file when nothing was ingested that update?"

    No, that is the point of `per_update`: it fires even on an update that ingested nothing, so a contractual feed always produces its file. Do not relabel the producing flow as a `streaming_table` to work around the plan-time error instead -- that converts it into a runtime failure.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configexport-trigger) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.format` { #target-configsink-configformat }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

The export format for a sink or external_sink target.


sink exports only; external_sink writes a governed table and also exports.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `delta`, `kafka`, `pgp_zip` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    "sink_config": { "format": "pgp_zip", "path": "/Volumes/{{catalog}}/egress/orders/" }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.format lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - pgp_zip uses the framework's own PySpark DataSource, not a built-in writer.
    - kafka takes connection options instead of a path.

!!! warning "Known errors and limitations"

    **Onboarding rejects a missing path**  
    *Cause:* delta and pgp_zip both require sink_config.path.  
    *Fix:* Supply an output directory, or switch the format to kafka.

**FAQs** (5)

??? question "If omitted · What happens if I leave sink_config.format unset?"

    Onboarding rejects the flow: `format` is a required key on `sink_config` (`"required": ["format"]` in the schema), and `_validate_sink_config` calls `check_string(..., required=True, allowed_values=ALLOWED_SINK_FORMATS)`. There is no default.

??? question "Format gotcha · What are the only legal values for sink_config.format?"

    Exactly `delta`, `kafka`, or `pgp_zip` (`ALLOWED_SINK_FORMATS`). Any other value is rejected at onboarding. `delta`/`kafka` dispatch to Lakeflow's own native `dlt.create_sink` formats; `pgp_zip` is this framework's own custom PySpark `DataSource`.

??? question "Performance impact · Is one sink format more expensive at runtime than another?"

    Kafka and delta write directly via Lakeflow's native sink writers with no extra staging step. pgp_zip adds a stage-then-archive step per micro-batch (write raw-row files, then zip/gzip and optionally PGP-encrypt them), so it carries more CPU and I/O overhead per batch than the native formats -- proportional to archive size, not a fixed constant.

??? question "Edge case · Does format decide whether path or kafka_options is required?"

    Yes. `kafka` requires `kafka_options` (no filesystem `path` at all); `delta` and `pgp_zip` both require `path` instead. Setting the wrong pairing (e.g. `path` on a `kafka` sink with no `kafka_options`) is rejected at onboarding.

??? question "Edge case · Does target_type change what format ends up doing?"

    Yes. A pure `sink` target skips CDC dispatch entirely -- no main table, no quarantine table -- and the staged view feeds the chosen `format` directly. `external_sink` first materializes a real, governed, DQ-quarantined table and then additionally exports it via a second append flow. See docs/13 K2.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configformat) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.kafka_options` { #target-configsink-configkafka-options }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Connection options for a kafka sink — the same flat options a Spark Structured Streaming Kafka writer takes.


Connection options for a kafka sink — the same flat options a Spark Structured Streaming Kafka writer takes. Onboarding requires both kafka.bootstrap.servers and topic. A kafka sink has no filesystem path. Prefer databricks.serviceCredential over an inline credential. sink_config.kafka_secret_options (option name → UC secret ref, for an option whose literal value must embed a resolved secret such as kafka.sasl.jaas.config) is supported by the framework but cannot be authored here — add it by hand to the exported JSON.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.kafka_options lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - kafka.bootstrap.servers and topic are both mandatory
    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (5)

??? question "If omitted · What happens if kafka_options is missing on a kafka-format sink?"

    Onboarding rejects the flow. The schema requires `kafka_options` whenever `format == 'kafka'`, and `_validate_sink_config` further hard-errors if `kafka.bootstrap.servers` or `topic` is missing or falsy inside it, even if the key is present as an empty object.

??? question "Format gotcha · Do I write kafka option keys exactly as Spark's Kafka writer expects, like kafka.bootstrap.servers?"

    Yes -- keys are the same flat options a Spark Structured Streaming Kafka writer takes, written verbatim. A typo in a key name is not caught: it becomes a silently ignored option rather than an onboarding error, since `kafka_options` accepts any string-to-string map beyond the two required keys.

??? question "Performance impact · Does adding more kafka_options entries slow down the sink?"

    Negligible -- these are one-time connection/producer configuration values (batching, compression, security) read once when the Kafka writer is constructed, not per-record processing. Any performance impact comes from the option values themselves (e.g. `linger.ms`, `compression.type`), not the number of keys.

??? question "Edge case · How do I put a secret value like a SASL JAAS config into kafka_options without it being a plaintext literal?"

    Use `kafka_secret_options` (option name to a UC secret reference) for an option whose literal value must embed a resolved secret such as `kafka.sasl.jaas.config`; each entry is validated as a required `secret_catalog`/`secret_schema`/`secret_key` reference. Prefer `kafka_options["databricks.serviceCredential"]` when a UC service credential is available instead.

??? question "Edge case · Are the secrets I reference from kafka_options resolved lazily when the sink writes?"

    No -- all `sink_config` secrets are resolved eagerly at graph-definition time (Phase 1). If a secret scope or key does not exist, the pipeline fails immediately before any compute starts, not mid-batch. See docs/06 section 3.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configkafka-options) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.path` { #target-configsink-configpath }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Output directory.


Output directory. Required for delta and pgp_zip.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "sink_config": {
          "path": "/Volumes/{{catalog}}/egress/example/"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.path lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (5)

??? question "If omitted · What happens if path is omitted on a delta or pgp_zip sink?"

    Onboarding rejects the flow with a required-field error: `path` is mandatory for `format` in `{delta, pgp_zip}` (`check_string(..., required=True)` in `_validate_sink_config`, mirrored by the schema's conditional `required` on `path`). It is not required, and not used, for `kafka`.

??? question "Format gotcha · Does path mean the same thing for delta and pgp_zip sinks?"

    No -- `path` is the actual Delta table/directory output for `format: delta`, but for `pgp_zip` it is only the per-micro-batch raw-row staging directory (see `archive/pgp_zip_sink.py`), never the finished archive location. The real pgp_zip output lives at `post_export_archive.output_zip_path`. See docs/13 K3.

??? question "Performance impact · Does the choice of path location affect sink throughput?"

    Indirectly -- writing staged files (pgp_zip) or Delta files (delta) to a slower storage tier or a heavily-throttled Volume adds I/O latency per micro-batch, same as any Spark writer. The framework applies no extra overhead on top of the underlying storage's own characteristics.

??? question "Edge case · If I point a downstream consumer at sink_config.path for a pgp_zip sink, will they get the finished archive?"

    No -- they will only see the staging area's raw per-partition files, not the archive. Point consumers at `post_export_archive.output_zip_path` instead, which is where the finished ZIP or gzip file actually lands. See docs/13 K3.

??? question "Edge case · Is path required for a kafka sink even though kafka has no filesystem output?"

    No -- `kafka` has no filesystem `path` at all and it is not required; connection details live entirely under `kafka_options` instead.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpath) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.archive_format` { #target-configsink-configpost-export-archivearchive-format }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-ver">v1.7.4+</span> <span class="fx-badge fx-only">Target · sink config</span>

The container the finished export is delivered in: 'zip' or 'gzip'.


The sink was ZIP-only, so an interface specifying a .csv.gz drop could not be served without a downstream repack — which would have meant either breaking the supplier contract or hand-rolling file handling outside the framework.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `zip`, `gzip` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.archive_format lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - A gzip holds exactly ONE member, so the staged partition files are concatenated into a single stream. The CSV header is written per partition, so all but the first are dropped during concatenation.
    - A gzip stream has no archive password: post_export_archive.secret does not apply. To protect a gzip export, use pgp_encryption.
    - The extension follows the content: .csv.gz or .jsonl.gz, and .gpg is appended when pgp_encryption is on (a ZIP archive uses .pgp instead).

!!! warning "Known errors and limitations"

    **The consumer rejects the file as a corrupt gzip.**  
    *Cause:* Some tools stop at the first member boundary in a multi-member gzip.  
    *Fix:* The framework emits a single-member stream, so this should not occur. If it does, confirm nothing downstream is re-concatenating the exports.

**FAQs** (5)

??? question "If omitted · What archive container do I get if archive_format is left unset?"

    It defaults to `zip` -- the unchanged pre-v1.7.4 behaviour: an AES-capable ZIP holding one file per staged partition, with `.zip`/`.zip.pgp` suffixing unaffected.

??? question "Format gotcha · What file extension does a gzip archive_format actually produce?"

    `<export_file_name_format>.<csv|jsonl>.gz`, or `....gz.gpg` when `pgp_encryption` is also enabled -- the suffix is derived from `staged_file_format` so it always matches what is actually inside, e.g. `EE_20260901-LEIDOS_TELEPHONE_001.csv.gz`. A ZIP archive uses `.gpg`/`.pgp` styling instead, never `.gz`.

??? question "Performance impact · Is gzip faster or slower than zip for large exports?"

    Gzip concatenates all staged partition files into a single gzip stream per micro-batch instead of zipping each file separately, which is typically cheaper for a single-recipient text feed since there is no per-file ZIP entry/CRC overhead; both are otherwise a one-time compression pass proportional to export size, not a per-row cost.

??? question "Edge case · Can I still password-protect the archive with post_export_archive.secret if I switch to gzip?"

    No -- a gzip stream has no archive password, so `post_export_archive.secret` (an AES password on the ZIP) has no meaning for `gzip`. To protect a gzip export, use `pgp_encryption` instead.

??? question "Edge case · Why does only the first partition's CSV header survive when using gzip with staged_file_format csv?"

    A headered CSV writes its header per staged file, and the writer cannot know which partition lands first, so on gzip's single-member concatenation all headers after the first are dropped. This is why `include_header` and `archive_format` interact; JSON-Lines has no header so nothing is dropped there. See docs/06 'Archive container'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivearchive-format) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.enabled` { #target-configsink-configpost-export-archiveenabled }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Enable post-write archiving for pgp_zip exports.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.enabled lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (5)

??? question "If omitted · Do I need post_export_archive.enabled if my sink format is pgp_zip?"

    Yes -- it is required and must be `true` for `pgp_zip`; archiving IS what that sink format does. If `enabled` is present as `false` on a `pgp_zip` sink, onboarding rejects it: 'must be true for format pgp_zip ... use format delta or kafka instead for a sink with no archiving step.'

??? question "Format gotcha · Is enabled just a boolean, or does it gate other required fields?"

    It is a plain boolean, but when `true` it makes `output_zip_path` required underneath it -- both the schema's conditional `required` and `_validate_sink_config`'s runtime check enforce this together.

??? question "Performance impact · Does setting post_export_archive.enabled add measurable overhead to every micro-batch?"

    Yes, but it is inherent to the pgp_zip sink's design, not an optional toggle you can skip while still using that format -- since `enabled` must be `true` for `pgp_zip`, every micro-batch always pays the archive step's cost proportional to the staged data volume.

??? question "Edge case · Can post_export_archive.enabled be set on a delta or kafka sink?"

    It is structurally accepted (validated) but unused by the engine for `delta`/`kafka` if a spec author supplies it anyway -- neither native sink format has any concept of a post-write archiving step to hook it up to, so setting it there has no runtime effect.

??? question "Edge case · What happens if post_export_archive itself is omitted entirely on a pgp_zip sink?"

    Onboarding rejects the flow: `post_export_archive` is required whenever `format == 'pgp_zip'`, with `enabled` required to be `true` inside it -- there is no way to run a pgp_zip sink without archiving.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archiveenabled) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.export_file_name_format` { #target-configsink-configpost-export-archiveexport-file-name-format }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

str.format()-style template for the exported archive's own file name.


str.format()-style template for the exported archive's own file name. Placeholders: {batch_id}, {timestamp}. Omit for the framework default.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.export_file_name_format lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (5)

??? question "If omitted · What file name do I get if export_file_name_format is not set?"

    It defaults to `"batch_{batch_id}"`, e.g. `batch_42.zip` -- the framework's pre-existing naming convention (`archive/pgp_zip_sink.py::_render_export_file_name`).

??? question "Format gotcha · What placeholders can I actually use inside export_file_name_format?"

    Only `{batch_id}` (the micro-batch id) and `{timestamp}` (UTC, rendered as `YYYYMMDDTHHMMSSZ` at commit time). Any other placeholder, such as `{export_file_date}` or `{export_file_sequence}`, raises an `ArchiveError`: 'unknown placeholder ... (supported: {batch_id}, {timestamp})' -- this is a runtime error from the sink's `str.format()` call, not caught at onboarding.

??? question "Performance impact · Does a complex export_file_name_format template slow down each export?"

    Negligible -- it is a single `str.format()` call per micro-batch commit, not a per-row operation. Choose any literal text plus the two supported placeholders freely.

??? question "Edge case · Do I need to add the .zip or .csv.gz extension myself in export_file_name_format?"

    No -- this template renders only the bare file name; the `.zip`/`.zip.pgp` or `.csv.gz`/`.jsonl.gz`(`.gz.gpg`) suffix is always appended by the caller afterwards based on `archive_format`, `staged_file_format` and whether `pgp_encryption` is on. Never encode the archive extension yourself.

??? question "Edge case · Is export_file_name_format validated at onboarding time for bad placeholders?"

    Not fully -- onboarding only checks it is a string (`check_string`). An unknown placeholder is not rejected at onboarding; it fails at pipeline runtime with `ArchiveError: Invalid export_file_name_format ...: unknown placeholder ...` when the sink actually tries to render a file name.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archiveexport-file-name-format) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.output_zip_path` { #target-configsink-configpost-export-archiveoutput-zip-path }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Where the final ZIP is written.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.output_zip_path lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (5)

??? question "If omitted · What happens if I enable post_export_archive but leave output_zip_path blank?"

    Onboarding rejects the flow: `output_zip_path` is required whenever `enabled` is `true`, both in the schema's conditional `required` and in `_validate_sink_config`'s `check_string(..., required=True)`.

??? question "Format gotcha · Is output_zip_path the same directory as sink_config.path?"

    No -- they should be different directories. `sink_config.path` is the per-micro-batch staging location for a pgp_zip sink, while `output_zip_path` is where the finished archive (ZIP or gzip) is actually written; consumers should read from `output_zip_path`, not `path`. See docs/13 K3.

??? question "Performance impact · Does the destination volume for output_zip_path affect export latency?"

    Yes, in the same way any Spark/DBFS write does -- writing the finished archive to a slower storage tier or external Volume with throttling adds I/O latency to the commit step; the framework itself adds no additional overhead beyond the underlying storage write.

??? question "Edge case · Can output_zip_path point at an external partner Volume outside my own catalog?"

    Yes -- it is just a string path and is commonly an external Volume path (e.g. `/Volumes/{{catalog}}/egress/zips/`) for partner drop zones; the framework does not restrict it to the pipeline's own catalog.

??? question "Edge case · One output_zip_path, many micro-batches -- do files overwrite each other?"

    Not by default, because the file name normally includes `{batch_id}` or `{timestamp}` (via `export_file_name_format`), which differs per micro-batch. If you set a static file name with no varying placeholder, later micro-batches will overwrite earlier archives at the same path.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archiveoutput-zip-path) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.enabled` { #target-configsink-configpost-export-archivepgp-encryptionenabled }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

PGP-encrypt the output archive.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.enabled lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (5)

??? question "If omitted · What happens if pgp_encryption block is present but enabled is left out?"

    Onboarding rejects it: `enabled` is a required key on `pgp_encryption` (`check_bool(..., required=True)` and the schema's `"required": ["enabled"]`). Omit the whole `pgp_encryption` object instead if you don't want PGP encryption at all.

??? question "Format gotcha · Is pgp_encryption.enabled just a plain boolean like other enabled flags?"

    Yes -- a plain boolean, but when `true` it makes exactly one of `passphrase_secret` or `recipient_public_key_secret` required underneath it; when `false` (or the object absent) the archive is written unencrypted.

??? question "Performance impact · How much overhead does turning on PGP encryption add per export?"

    Not negligible but bounded -- it is one additional encryption pass over the finished archive bytes per micro-batch/update, proportional to archive size; it is not a per-row cost and does not affect the staging step itself.

??? question "Edge case · What happens if I set pgp_encryption.enabled true but supply neither passphrase_secret nor recipient_public_key_secret?"

    Onboarding falls into the asymmetric branch by default and requires `recipient_public_key_secret` (`check_secret_ref(recipient_secret, ..., required=True)` when `passphrase_secret` is absent), so it is rejected as a missing required secret reference.

??? question "Edge case · Can pgp_encryption.enabled be true on a delta or kafka sink's post_export_archive?"

    It is structurally accepted if a spec author supplies `post_export_archive` on `delta`/`kafka`, but it is unused by the engine -- neither native sink format has an archiving step to encrypt in the first place.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionenabled) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key.


SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key. Mutually exclusive with recipient_public_key_secret: set exactly one. Signing is unavailable in this mode (it needs a sender keypair).


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_catalog lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if I set passphrase_secret but leave out secret_catalog?"

    Onboarding rejects it: `secret_catalog` is required inside every UC secret reference (`check_string(..., required=True)` in `check_secret_ref`), alongside `secret_schema` and `secret_key`.

??? question "Format gotcha · Does secret_catalog need to match the pipeline's own catalog?"

    Not necessarily by rule, but conventionally it does -- the sample in docs and UC6's live spec both use `{{catalog}}` (the pipeline's own bundle-variable catalog) so the secret lives alongside the pipeline it serves; there is no validator constraint forcing this.

??? question "Performance impact · Does resolving secret_catalog at graph-definition time add latency to every pipeline start?"

    Negligible, a one-time `dbutils.secrets.get(catalog=, schema=, key=)` lookup per sink at Phase 1 graph definition, not a per-micro-batch cost. If the catalog/scope does not exist, the pipeline fails immediately before any compute starts.

??? question "Edge case · Is secret_catalog the Unity Catalog three-level namespace for this key, and does it differ from source_zip_handling's decryption secret catalog?"

    Yes -- it is the same three-level UC secret reference shape (`secret_catalog`/`secret_schema`/`secret_key`) used identically for every encryption/decryption key, ZIP password, PGP key and Kafka credential in the framework; it is independent of and can differ from the ingest-side `pre_extraction_decryption` secret catalog.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-key }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key.


SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key. Mutually exclusive with recipient_public_key_secret: set exactly one. Signing is unavailable in this mode (it needs a sender keypair).


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_key lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (5)

??? question "If omitted · What happens if secret_key is left blank inside passphrase_secret?"

    Onboarding rejects it: `secret_key` is required and must be a non-empty string inside a secret reference. A missing or empty `secret_key` fails `check_string(..., required=True)`.

??? question "Format gotcha · Does secret_key need any particular naming convention like a prefix?"

    No format is enforced beyond being a non-empty string; the framework's own examples use plain descriptive names like `pgpkey` or `pii_encryption_key`. It must match a real key already created via `databricks secrets put-secret`.

??? question "Performance impact · Does the length or content of the passphrase behind secret_key affect encryption speed?"

    Negligible -- encryption is AES256 regardless of passphrase content; the passphrase itself only feeds key derivation, not the cost of the encryption pass over the archive.

??? question "Edge case · If I rotate the secret value behind secret_key, do I need to redeploy the pipeline?"

    No redeploy is needed for a value rotation since the reference (catalog/schema/key names) is unchanged and resolution happens fresh at each graph-definition-time run; only changing which secret_key name is referenced requires an onboarding spec change and redeploy.

??? question "Edge case · What if secret_key names a key that does not exist in the given secret_catalog/secret_schema?"

    Not caught at onboarding -- validation only checks the three fields are non-empty strings. It fails at pipeline graph-definition time (Phase 1) when `dbutils.secrets.get` cannot resolve it, before any compute starts. See docs/06 section 3 and docs/13 S6.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key.


SYMMETRIC egress encryption — encrypt with a shared passphrase instead of a recipient key. Mutually exclusive with recipient_public_key_secret: set exactly one. Signing is unavailable in this mode (it needs a sender keypair).


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret.secret_schema lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if secret_schema is omitted from passphrase_secret?"

    Onboarding rejects it: `secret_schema` is required and must be a non-empty string, exactly like `secret_catalog` and `secret_key` in the same `check_secret_ref` call.

??? question "Format gotcha · Can secret_schema be any schema name, or must it be a schema literally named 'security' or 'config'?"

    Any schema name is accepted by the validator; the samples use `config` or `security` by convention (UC6 uses `config`, other samples use `security`), but nothing enforces a particular schema name.

??? question "Performance impact · Does putting the secret in a dedicated schema change lookup performance?"

    Negligible -- it is one `dbutils.secrets.get` call at graph-definition time regardless of which schema the secret lives in; schema choice is an organizational/governance decision, not a performance one.

??? question "Edge case · Who needs grants on secret_catalog.secret_schema to onboard and run this flow?"

    Not documented in the validator or docs/06/13 read for this group -- the code only validates the reference shape, not permissions. Grounding is thin here: verify the required UC secret-scope grant (e.g. `USE SCHEMA`/`SELECT` equivalent on the secret) at deploy/runtime rather than assuming a default.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if enabled is true and recipient_public_key_secret has no secret_catalog?"

    Onboarding rejects it: `secret_catalog` is required inside the secret reference, and `recipient_public_key_secret` itself is required whenever `passphrase_secret` is absent and `enabled` is `true`.

??? question "Format gotcha · Is secret_catalog for the recipient key the same catalog as the sink's own output path?"

    Not required to be -- it is only a UC secret lookup location, independent of `sink_config.path`/`output_zip_path`. Samples commonly use `{{catalog}}` for convenience, but any catalog holding the secret works.

??? question "Performance impact · Is resolving a recipient public key more expensive than a passphrase secret?"

    Negligible -- both are the same single `dbutils.secrets.get` call at graph-definition time; the difference is the cryptographic operation performed afterward (asymmetric vs symmetric), not the secret lookup cost.

??? question "Edge case · Can I use recipient_public_key_secret together with sign_with_private_key_secret for signed asymmetric export?"

    Yes -- signing is only available in the asymmetric (`recipient_public_key_secret`) mode, since it requires a sender keypair; `sign_with_private_key_secret` is exactly for this combination and is rejected only when paired with `passphrase_secret` instead.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_key lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if I set enabled true, recipient_public_key_secret object present, but secret_key missing?"

    Onboarding rejects it: `secret_key` is required and must be a non-empty string, same as every other secret reference field validated by `check_secret_ref`.

??? question "Format gotcha · Does secret_key here need to reference a public key specifically, not a private key?"

    The framework does not validate key content at onboarding -- it only validates the reference shape. You must put the recipient's actual PGP public key material behind this `secret_key`; using the wrong key type is not caught until decryption/encryption actually runs.

??? question "Performance impact · Does key size (2048 vs 4096-bit RSA) behind secret_key affect export runtime noticeably?"

    Not documented; likely a small, bounded per-archive cost difference from asymmetric key operations, but no measurement is given in docs/05 or docs/06. Treat as negligible relative to the archive's own I/O and compression cost unless observed otherwise.

??? question "Edge case · What error do I get if secret_key names a key that does not exist in Unity Catalog?"

    Not validated at onboarding -- it fails at pipeline graph-definition time (Phase 1) when the secret cannot be resolved, before any compute starts. See docs/06 section 3 and docs/13 S6 for why resolution happens eagerly outside the sink's own worker process.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if secret_schema is left out of recipient_public_key_secret?"

    Onboarding rejects it: `secret_schema` is required and must be a non-empty string, alongside `secret_catalog` and `secret_key`, in the same `check_secret_ref` validation.

??? question "Format gotcha · Do secret_catalog, secret_schema and secret_key together form a three-level namespace like a table reference?"

    Yes -- it is resolved via `dbutils.secrets.get(catalog=, schema=, key=)`, structurally the same three-level namespace used for every encryption/decryption key, ZIP password, PGP key and Kafka credential in the framework.

??? question "Performance impact · Does grouping many partner keys under one secret_schema slow down secret resolution?"

    Negligible -- each lookup is an independent, one-time `dbutils.secrets.get` call at graph-definition time; the number of other secrets sharing the same schema does not affect resolution cost.

??? question "Edge case · If I rename the secret_schema without updating the spec, does the pipeline still deploy?"

    It deploys (onboarding only checks the string is non-empty) but fails at graph-definition time when the old schema name can no longer be resolved -- not validated at onboarding, fails at pipeline runtime with a secret-resolution error. Update the spec's `secret_schema` to match before redeploying.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_catalog lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if I set sign_passphrase_secret without also setting sign_with_private_key_secret?"

    Onboarding rejects it: 'sign_passphrase_secret: only meaningful alongside sign_with_private_key_secret, but that field is not set.' It is optional even when the signing key IS set -- an unprotected signing key is a valid configuration too.

??? question "Format gotcha · Does secret_catalog here follow the same three-level shape as the other PGP secret fields?"

    Yes -- identical `secret_catalog`/`secret_schema`/`secret_key` shape, validated the same way via `check_secret_ref`, resolved via `dbutils.secrets.get(catalog=, schema=, key=)`.

??? question "Performance impact · Does adding a signing passphrase on top of encryption noticeably slow down export commits?"

    Negligible -- it is one additional secret lookup at graph-definition time plus unlocking the signing key during the encryption pass, not a per-row cost.

??? question "Edge case · Can sign_passphrase_secret be used with passphrase_secret (symmetric encryption) instead of a recipient key?"

    No -- signing is asymmetric-only. Any signing field, including `sign_passphrase_secret`, set alongside `passphrase_secret` is rejected: 'signing requires a sender keypair and is not available for symmetric (passphrase_secret) encryption.'


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-key }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_key lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · If sign_with_private_key_secret is set but sign_passphrase_secret's secret_key is left out, does signing still work?"

    Yes -- `sign_passphrase_secret` is entirely optional even when `sign_with_private_key_secret` is set, since an unprotected signing private key is a valid configuration. Only when `sign_passphrase_secret` is present is it required to have all three fields, including `secret_key`, filled in.

??? question "Format gotcha · Is secret_key here the passphrase that protects the signing key itself, not the signing key value?"

    Yes -- this secret reference points to the passphrase that unlocks the private signing key referenced by `sign_with_private_key_secret`; it is a different secret from the signing key material itself.

??? question "Performance impact · Does unlocking a passphrase-protected signing key add noticeable latency versus an unprotected one?"

    Not documented with a measurement; expect a small, one-time key-unlock cost during the signing step, negligible relative to archive compression and encryption.

??? question "Edge case · What happens if the value behind this secret_key does not actually match the signing key's passphrase?"

    Not validated at onboarding -- the reference shape is checked, not the passphrase's correctness. A mismatch fails at pipeline runtime when the sink attempts to unlock and use the signing key, not before.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_schema lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is sign_passphrase_secret.secret_schema required if I only set sign_with_private_key_secret and no sign_passphrase_secret at all?"

    No -- the whole `sign_passphrase_secret` object, including `secret_schema`, is optional and only validated when the object is present at all. Omit it entirely for an unprotected signing key.

??? question "Format gotcha · Can secret_schema for the signing passphrase live in a different schema than the signing key itself?"

    Yes -- each secret reference (`sign_with_private_key_secret`, `sign_passphrase_secret`) is resolved independently with its own `secret_catalog`/`secret_schema`/`secret_key`, so they can point at different schemas if needed.

??? question "Performance impact · Does the schema chosen for sign_passphrase_secret affect pipeline startup time?"

    Negligible -- it is one more independent secret lookup at graph-definition time regardless of which schema it lives in.

??? question "Edge case · What error appears if sign_passphrase_secret is fully specified but sign_with_private_key_secret is absent?"

    Onboarding rejects the flow verbatim: 'sign_passphrase_secret: only meaningful alongside sign_with_private_key_secret, but that field is not set.' Add the signing key reference or remove `sign_passphrase_secret` entirely.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if I want a signed export but leave out sign_with_private_key_secret entirely?"

    No error -- signing is fully optional. `pgp_encryption` proceeds unsigned (recipient-key encryption only) when neither `sign_with_private_key_secret` nor `sign_passphrase_secret` is set.

??? question "Format gotcha · Does secret_catalog for the signing key need to be the same catalog as recipient_public_key_secret?"

    Not required -- each secret reference is resolved independently; the signing key and the recipient's public key can live in different catalogs/schemas.

??? question "Performance impact · Does adding a digital signature meaningfully slow down the export compared to encryption alone?"

    Adds one signing operation per archive on top of encryption, a bounded cost proportional to archive size -- not documented with a specific number here, but structurally it is one extra crypto pass, not a per-row cost.

??? question "Edge case · Can I set sign_with_private_key_secret when using passphrase_secret (symmetric) encryption?"

    No -- rejected. Signing requires a sender keypair, which symmetric encryption has none of; setting `sign_with_private_key_secret` alongside `passphrase_secret` triggers: 'signing requires a sender keypair and is not available for symmetric (passphrase_secret) encryption.'


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-key }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_key lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · If sign_with_private_key_secret object is present but secret_key is blank, what happens?"

    Onboarding rejects it: `secret_key` is required and must be a non-empty string, same as every field inside a `check_secret_ref`-validated object.

??? question "Format gotcha · Does secret_key here reference the private signing key material itself, not a public key?"

    Yes -- this is the sender's private key used to sign the export, distinct from `recipient_public_key_secret` (the recipient's public key used to encrypt). The framework does not validate key content, only that the reference resolves to a string.

??? question "Performance impact · Is there extra overhead from using a large RSA signing key here?"

    Not measured; expect a bounded per-archive signing cost from the asymmetric operation itself, negligible relative to compression and I/O for typical export sizes.

??? question "Edge case · What happens at runtime if the value behind secret_key is not actually a valid PGP private key?"

    Not validated at onboarding -- only the reference shape is checked. An invalid key fails at pipeline graph-definition time when the sink resolves and attempts to use it for signing, before any data moves (see docs/13 S6 on why resolution happens outside the sink's worker process).


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Is secret_schema required if I only set enabled true with a recipient key and no signing at all?"

    No -- the entire `sign_with_private_key_secret` object is optional; when absent, no signing is attempted and none of its sub-fields, including `secret_schema`, are validated.

??? question "Format gotcha · Should the signing key's secret_schema match the encryption key's secret_schema for consistency?"

    Not required by the validator -- each is an independent three-level UC secret reference; organizing them in the same schema is a convention, not a rule.

??? question "Performance impact · Does resolving both an encryption key and a signing key from different schemas add noticeable latency?"

    Negligible -- both are one-time `dbutils.secrets.get` lookups at graph-definition time (Phase 1), done once per pipeline deployment, not per micro-batch.

??? question "Edge case · Can sign_with_private_key_secret's secret_schema be reused across multiple sinks in the same pipeline?"

    Yes -- there is no per-sink uniqueness constraint on secret references; the same catalog/schema/key can be referenced by multiple sinks that need to sign with the same organizational key.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_catalog` { #target-configsink-configpost-export-archivesecretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.secret.secret_catalog lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if post_export_archive.secret is set for AES password protection but secret_catalog is missing?"

    Onboarding rejects it: `secret_catalog` is required inside `secret` just like any other `check_secret_ref`-validated object, via `check_secret_ref(archive_config.get("secret"), ...)`.

??? question "Format gotcha · Is post_export_archive.secret the same three-level shape as pgp_encryption's secrets?"

    Yes -- identical `secret_catalog`/`secret_schema`/`secret_key` shape, resolved the same way via `dbutils.secrets.get`. It is a separate, independent password: an AES password on the ZIP itself, not a PGP key.

??? question "Performance impact · Does adding a ZIP password via secret slow down archive creation?"

    Negligible -- one additional secret lookup at graph-definition time; the AES-password ZIP write itself is comparable in cost to an unencrypted ZIP write of the same size.

??? question "Edge case · Can I combine post_export_archive.secret with pgp_encryption at the same time?"

    Yes -- the ZIP password (`secret`) is independent of, and combinable with, `pgp_encryption`; you get an AES-password-protected ZIP that is then also PGP-encrypted. This combination has no meaning under `archive_format: gzip`, since a gzip stream has no archive password at all.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivesecretsecret-catalog) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_key` { #target-configsink-configpost-export-archivesecretsecret-key }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.secret.secret_key lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if secret.secret_key is left out while secret.secret_catalog and secret_schema are set?"

    Onboarding rejects it: `secret_key` is required and must be a non-empty string, same as every field validated by `check_secret_ref`.

??? question "Format gotcha · Is the value behind this secret_key the ZIP password itself?"

    Yes -- it is the AES password applied to the ZIP archive (independent of any PGP key), resolved via `dbutils.secrets.get` and never written as a literal into the spec, control tables or logs.

??? question "Performance impact · Does the ZIP password length affect archive write performance?"

    Not documented with a measurement here; AES-capable ZIP encryption cost scales with archive size, not passphrase length, so any effect from password length itself should be negligible.

??? question "Edge case · Does secret.secret_key apply when archive_format is gzip?"

    No -- `secret` (an AES password on the ZIP) has no meaning for `gzip`, since a gzip stream carries no archive password at all. Use `pgp_encryption` to protect a gzip export instead.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivesecretsecret-key) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_schema` { #target-configsink-configpost-export-archivesecretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.post_export_archive.secret.secret_schema lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if secret.secret_schema is missing while enabling the ZIP password?"

    Onboarding rejects it: `secret_schema` is required and must be a non-empty string inside `secret`, validated the same way as `secret_catalog` and `secret_key`.

??? question "Format gotcha · Can secret.secret_schema be the same schema as the pgp_encryption keys, or must it be separate?"

    Either works -- there is no constraint tying `post_export_archive.secret`'s schema to `pgp_encryption`'s; they are independently resolved three-level secret references and can share or differ in schema freely.

??? question "Performance impact · Does resolving the ZIP password secret add to pipeline startup time alongside the PGP secrets?"

    Negligible -- one more `dbutils.secrets.get` call at graph-definition time (Phase 1), same cost class as any other secret reference in `sink_config`.

??? question "Edge case · If secret is fully specified but archive_format is gzip, does onboarding warn me it will be ignored?"

    `_validate_sink_config` validates `secret` unconditionally whenever present, with no check against `archive_format`. Not validated at onboarding; it silently has no effect at runtime for `gzip` per docs/06's 'Archive container' note. Verify at runtime rather than assuming a rejection.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivesecretsecret-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.staged_file_format` { #target-configsink-configstaged-file-format }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-ver">v1.6.0+</span> <span class="fx-badge fx-only">Target · sink config</span>

File format of the staged export files a pgp_zip sink writes before archiving.


json (the default when absent) writes JSON-Lines; csv writes RFC-4180 files with a header row, one file per written partition. Only meaningful when sink_config.format is pgp_zip.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `json`, `csv` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    "sink_config": { "format": "pgp_zip", "path": "/Volumes/{{catalog}}/egress/orders/", "staged_file_format": "csv" }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.staged_file_format lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Leave it unset to keep the JSON-Lines staging the framework has always produced.
    - csv emits one file per written partition, each with its own header row.

!!! warning "Known errors and limitations"

    **Onboarding rejects staged_file_format**  
    *Cause:* It was set on a sink whose format is not pgp_zip.  
    *Fix:* Remove the attribute, or switch sink_config.format to pgp_zip.

**FAQs** (4)

??? question "If omitted · What staged file shape do I get inside the archive if staged_file_format is left unset?"

    It defaults to `json` -- JSON-Lines, one JSON object per row, the only pre-v1.6.0 behaviour. Non-JSON-native values (dates, decimals, binary) are stringified rather than failing the micro-batch.

??? question "Format gotcha · Can I use staged_file_format on a delta or kafka sink to control its output shape?"

    No -- it is presence-rejected there: 'only meaningful for format pgp_zip ...; format "delta"/"kafka" is a native Lakeflow sink with no staging step. Remove the attribute.' It only applies to `pgp_zip`.

??? question "Performance impact · Is csv staging more expensive than json staging per micro-batch?"

    Not quantified in docs/06, but structurally csv writes one file per non-empty partition with its own header row versus JSON-Lines' one-object-per-row stream; both are a single pass over the micro-batch's rows, so any difference should be negligible relative to archive compression and I/O.

??? question "Edge case · Does csv staged_file_format interact with archive_format gzip in a way I should know about?"

    Yes -- because a headered CSV writes its header per staged file and gzip concatenates all staged files into one stream, every header after the first is dropped on concatenation. JSON-Lines has no header, so nothing is lost there. See docs/06 'Archive container'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-format) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.staged_file_options.delimiter` { #target-configsink-configstaged-file-optionsdelimiter }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

The field separator for the staged CSV a pgp_zip sink writes.


Before v1.7.4 the writer used Python's csv `excel` dialect with no override — comma-separated, always headered, CRLF-terminated. A supplier interface that specifies anything else simply could not be expressed, which is what UC6's pipe-delimited Leidos feed needed.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.staged_file_options.delimiter lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Exactly one character. Python's csv writer cannot emit a multi-character delimiter and the framework rejects one rather than silently truncating it.
    - Omit the whole staged_file_options object to keep the pre-1.7.4 dialect exactly.
    - Only applies to staged_file_format 'csv'. A JSON-Lines export has no delimiter.

!!! warning "Known errors and limitations"

    **The consumer reads one giant column, or splits on the wrong character.**  
    *Cause:* The delimiter here does not match what the receiving interface expects.  
    *Fix:* Match the interface specification exactly. If fields can contain the delimiter, the writer quotes them per RFC-4180 — confirm the consumer honours quoting.

**FAQs** (5)

??? question "If omitted · What field separator do I get in the staged CSV if delimiter is not set?"

    It defaults to `,` (comma) -- Python's csv `excel` dialect, the pre-v1.7.4 behaviour, when `staged_file_options` (or just `delimiter`) is omitted.

??? question "Format gotcha · Can I use a multi-character delimiter like || for a pipe-pair separated feed?"

    No -- it must be exactly one character. Onboarding rejects a multi-character value verbatim: 'must be exactly one character, got \'||\' -- Python's csv writer cannot emit a multi-character delimiter.'

??? question "Performance impact · Does choosing a non-comma delimiter like | affect write throughput?"

    Negligible -- the delimiter is just a single character passed to Python's csv writer dialect; it has no measurable effect on staging throughput regardless of which single character is chosen.

??? question "Edge case · Is delimiter valid if staged_file_format is left at the json default?"

    No -- `staged_file_options` (and therefore `delimiter`) only applies when `staged_file_format == 'csv'`; JSON-Lines has no delimiter concept, and setting it otherwise is rejected: 'only meaningful when staged_file_format == csv (JSON-Lines staging has no delimiter or header row).'

??? question "Edge case · What happens downstream if a field value itself contains the delimiter character?"

    The writer quotes such fields per RFC-4180, so a value containing the delimiter is still correctly delimited -- but confirm the receiving consumer actually honours CSV quoting rather than naively splitting on the delimiter character.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-optionsdelimiter) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.staged_file_options.include_header` { #target-configsink-configstaged-file-optionsinclude-header }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Write the header row.


Write the header row. Absent = true. Set false for a supplier interface that specifies a headerless body. NOTE: with archive_format gzip the header is emitted per partition and all but the first are dropped on concatenation.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

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

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.staged_file_options.include_header lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · If I don't set include_header, does the staged CSV get a header row?"

    Yes -- it defaults to `true`. Omitting the attribute is not the same as setting it `false`; you must explicitly set `false` to suppress the header for a headerless supplier interface.

??? question "Format gotcha · Is include_header meaningful for a JSON-Lines staged file?"

    No -- like `delimiter` and `line_terminator`, it is only meaningful when `staged_file_format == 'csv'`; JSON-Lines has no header row concept and setting `staged_file_options` there is rejected.

??? question "Performance impact · Does turning off include_header measurably speed up export writes?"

    Negligible -- writing or skipping one header row per staged partition file has no measurable effect on overall micro-batch throughput.

??? question "Edge case · If include_header is true and archive_format is gzip, do I get one header per row group or one for the whole file?"

    You get one header written per staged partition file, but since gzip concatenates all staged files into a single stream, only the first file's header survives -- every subsequent header is dropped during concatenation. This is a direct interaction between `include_header` and `archive_format`; see docs/06 'Archive container'.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-optionsinclude-header) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.staged_file_options.line_terminator` { #target-configsink-configstaged-file-optionsline-terminator }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

The record separator for the staged CSV: 'crlf' or 'lf'.


Spelled as a name because JSON cannot carry a bare control character — you cannot write a literal CR in a JSON string and have it survive round-tripping.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `crlf`, `lf` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    "staged_file_options": { "line_terminator": "lf" }
    // crlf (default when absent) = RFC-4180, the pre-1.7.4 behaviour
    // lf                          = bare \n, what most Unix consumers expect
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.staged_file_options.line_terminator lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Absent = crlf. Legacy mainframe and Windows interfaces usually want crlf; most Unix-side consumers want lf.

!!! warning "Known errors and limitations"

    **The consumer reports a stray \r at the end of the last field on every row.**  
    *Cause:* The file is CRLF-terminated but the consumer splits on \n only.  
    *Fix:* Set line_terminator to 'lf'.

**FAQs** (5)

??? question "If omitted · What line ending does the staged CSV use if line_terminator is left unset?"

    It defaults to `crlf` -- RFC-4180 style, and the pre-v1.7.4 behaviour, matching legacy mainframe/Windows interface expectations.

??? question "Format gotcha · Why is line_terminator spelled as crlf or lf instead of a literal control character?"

    Because a JSON string cannot carry a bare control character unambiguously and reliably survive round-tripping, so it is spelled as a name (`"crlf"`/`"lf"`) rather than embedding a literal `\r\n` or `\n` in the spec.

??? question "Performance impact · Does the choice between crlf and lf affect write performance?"

    Negligible -- it only changes which two-or-one-byte sequence terminates each row; there is no measurable throughput difference between the two.

??? question "Edge case · My Unix consumer reports a stray \r at the end of every row -- what setting fixes this?"

    Set `line_terminator` to `lf`. The default `crlf` leaves a trailing `\r` before the `\n` on each row, which a consumer that splits on `\n` only will see as a stray carriage return at the end of the last field.

??? question "Edge case · Does line_terminator apply if staged_file_format is json?"

    No -- like `delimiter` and `include_header`, it is only valid alongside `staged_file_format: 'csv'`; JSON-Lines staging has no configurable line terminator concept in this validator path.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-optionsline-terminator) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.write_mode` { #target-configsink-configwrite-mode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Delta write mode.


Delta write mode. Currently accepted but inert — @dlt.append_flow always appends.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `overwrite`, `append` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "sink_config": {
          "write_mode": "append"
        }
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.sink_config.write_mode lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: append, overwrite.

**FAQs** (4)

??? question "If omitted · What write behaviour do I get for a delta sink if write_mode is left unset?"

    Always append, regardless of whether `write_mode` is set at all -- `@dlt.append_flow` (the mechanism every sink uses) is inherently append-only. Omitting it changes nothing.

??? question "Format gotcha · What values does write_mode accept, and does setting overwrite actually overwrite the sink?"

    Only `overwrite` or `append` (`ALLOWED_SINK_WRITE_MODES`) pass onboarding validation, but per the schema's own `$comment`, this is a dead field: it validates fine but is never read by the engine's `delta` sink branch, so setting `overwrite` has no effect -- the sink still only appends.

??? question "Performance impact · Does setting write_mode to overwrite reduce storage growth on the sink target?"

    No -- since the value is never read by the engine, setting `overwrite` has zero effect on runtime behaviour or storage growth; the sink always appends regardless of this attribute.

??? question "Edge case · Should I still include write_mode in new specs for documentation purposes?"

    No -- the schema explicitly says to omit it in real specs ('Kept here only for fidelity with the validator's own constant; omit it in real specs'). It is retained purely so the schema matches the validator's allowed-values constant, not as a meaningful configuration knob.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configwrite-mode) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.storage_format` { #target-configstorage-format }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Target table format.


Target table format. iceberg is only valid when target_type is batch_table — for every other target type, leave this on delta and add the table property enable_iceberg_read_uniformity: true instead, which turns on Delta UniForm so Iceberg readers can read the Delta table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `delta`, `iceberg` | — |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "storage_format": "delta"
      }
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.storage_format lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - iceberg: batch_table only · Iceberg reads elsewhere via UniForm table property
    - Allowed values: delta, iceberg.

**FAQs** (4)

??? question "If omitted · What is the target table format if storage_format is not set?"

    The schema declares no default; the physical table is always Delta regardless — `storage_format: "iceberg"` itself is implemented as the UniForm read-compatibility property, since Lakeflow has no separate native-Iceberg storage engine. Treat `delta` as the safe assumption when omitted.

??? question "Format gotcha · Are delta and iceberg the only accepted values, case-sensitive?"

    Yes — `ALLOWED_STORAGE_FORMATS = {"delta", "iceberg"}` (lowercase), checked via `check_string(..., allowed_values=...)`. `"Iceberg"` or `"Delta"` would be rejected.

??? question "Performance impact · Does setting storage_format to iceberg add overhead versus plain delta?"

    Not separately benchmarked here; it enables IcebergCompat properties (UniForm) so both Delta and Iceberg readers can read the table, which has some write-side metadata overhead versus plain Delta, but this repo does not quantify it.

??? question "Edge case · Can I set storage_format: iceberg on a streaming_table or materialized_view target?"

    No — `_validate_target_config` hard-errors: 'iceberg is only valid when target_type == batch_table'. For any other target_type, leave `storage_format` on `delta` and instead set `table_properties.enable_iceberg_read_uniformity: true`, which achieves Iceberg read compatibility via UniForm without changing `storage_format`.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configstorage-format) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uniform](https://docs.databricks.com/delta/uniform.html) · [table properties](https://docs.databricks.com/delta/table-properties.html)


---

### `target_config.table_properties` { #target-configtable-properties }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Delta table properties applied to the target.


Controls log retention, VACUUM safety windows, and Iceberg read compatibility.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | no unknown keys |

**Persisted in** `config.ingestion_flow_spec.target_config_json`


=== "JSON"

    ```json
    "table_properties": {
      "log_retention_duration": "interval 30 days",
      "enable_iceberg_read_uniformity": "true"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_config.table_properties lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - enable_iceberg_read_uniformity turns on Delta UniForm so Iceberg readers can read a Delta table — valid for any target_type, unlike storage_format: iceberg.
    - Setting deleted_file_retention_duration below your longest-running query risks VACUUM removing files still being read.

!!! warning "Known errors and limitations"

    **FileNotFoundException in a long query**  
    *Cause:* VACUUM removed files while the query was running.  
    *Fix:* Raise deleted_file_retention_duration above your longest query duration.

**FAQs** (4)

??? question "If omitted · What happens if table_properties is not set at all?"

    No custom Delta table properties are applied — `_validate_table_properties` returns immediately when the value is `None`. Log retention, VACUUM windows and Iceberg-read-uniformity all stay at their platform defaults.

??? question "Format gotcha · Are table_properties values like log_retention_duration strings or numbers?"

    Always strings, even for a duration like `"interval 30 days"` — `check_string` is used for both `log_retention_duration` and `deleted_file_retention_duration`. `enable_iceberg_read_uniformity` is the one key validated as a real boolean via `check_bool`, not a string `"true"`.

??? question "Performance impact · Does raising log_retention_duration or deleted_file_retention_duration cost storage?"

    Yes, directly — a longer retention window keeps more Delta log versions and unreferenced data files around (for time-travel and safe concurrent reads), which increases storage footprint proportionally to the retention period and table churn rate.

??? question "Edge case · What happens if I set deleted_file_retention_duration too low for a long-running query?"

    VACUUM can remove files still being read by that query, causing a `FileNotFoundException` mid-query, per the documented known error. Raise `deleted_file_retention_duration` above your longest expected query duration to avoid this.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configtable-properties) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [table properties](https://docs.databricks.com/delta/table-properties.html) · [uniform](https://docs.databricks.com/delta/uniform.html)


---

### `target_schema` { #target-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

Schema for the target table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_schema`


=== "JSON"

    ```json
    {
      "target_schema": "bronze_example"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_schema is persisted as its own column
    SELECT dataflow_id,
           target_schema,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What error appears if target_schema is left blank?"

    Onboarding rejects it: `target_schema: is required but was missing or empty`, on both ingestion and transformation flows.

??? question "Format gotcha · Is there a naming convention target_schema should follow, like bronze_ prefix?"

    Not enforced by the validator — it is just a non-empty string. The sample spec uses a `bronze_example` convention, but this is a project convention, not a validated rule.

??? question "Performance impact · Does target_schema's name affect pipeline performance?"

    None — it only determines the Unity Catalog schema namespace for the target table; no runtime cost is tied to the name itself.

??? question "Edge case · Can target_schema differ between an ingestion flow and its downstream transformation flow?"

    Yes — nothing forces them to match; a transformation flow references its upstream via `source_inputs[].table` (a fully-qualified name), which can point at any catalog/schema/table combination regardless of this flow's own `target_schema`.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-schema) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_table` { #target-table }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

Target Delta table name.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.ingestion_flow_spec.target_table`


=== "JSON"

    ```json
    {
      "target_table": "example_raw"
    }
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_table is persisted as its own column
    SELECT dataflow_id,
           target_table,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if target_table is omitted?"

    Onboarding rejects it: `target_table: is required but was missing or empty`, on both ingestion and transformation flows.

??? question "Format gotcha · Does target_table need to be a bare name or fully qualified?"

    A bare table name — it is combined with `target_catalog`/`target_schema` elsewhere (e.g. `_qualified_or_none`) to build the fully-qualified reference; do not put dots in it.

??? question "Performance impact · Does the target_table name affect performance?"

    None — it only names the registered Lakeflow dataset; performance is governed by `target_type`, `cdc_load_strategy`, partitioning/clustering, and data volume, not the table name.

??? question "Edge case · Does target_table feed the auto-derived schema_location path?"

    Yes — for `autoloader`/`asn1` sources with no explicit `schema_location`, the loader derives `/Volumes/<target_catalog>/landing/_schemas/<target_table>/` using this flow's own `target_catalog` and `target_table`. Renaming `target_table` later without moving the schema location will point at the wrong path.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-table) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_type` { #target-type }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

The kind of Lakeflow dataset registered for this flow.


Decides whether the target is incrementally maintained, fully recomputed, or export-only.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `streaming_table`, `materialized_view`, `batch_table`, `external_sink`, `sink` | — |

**Persisted in** `config.ingestion_flow_spec.target_type`


=== "JSON"

    ```json
    "target_type": "streaming_table"
    ```

=== "Validate offline"

    ```python
    from flowx.lakeflow_framework.onboarding.agent_tools import validate_json

    with open("dfg_orders.json", encoding="utf-8") as fh:
        result = validate_json(fh.read(), catalog="<catalog>", env="dev")

    print(result["summary"])        # PASSED, or FAILED with the error count
    for err in result["errors"]:    # each error names the offending spec path
        print(err)
    ```

    No cluster, sub-second. The same `spec_validator.py` that the onboarding job runs.

=== "Onboard (CLI)"

    ```bash
    # 1. Dry run against the live catalog: validates, writes nothing
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=VALIDATE_ONLY

    # 2. Onboard: CREATE for a new group, UPDATE to upsert an existing one
    databricks bundle run onboarding_job -t <target> \
      --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/dfg_orders.json,catalog=<catalog>,env=<target>,action_type=CREATE
    ```

    Onboarding writes control-table rows only. The pipeline picks the change up on its next update.

=== "Verify (SQL)"

    ```sql
    -- target_type is persisted as its own column
    SELECT dataflow_id,
           target_type,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - streaming_table is the default for incremental ingestion.
    - materialized_view fully recomputes — pair it with TRUNCATE_AND_LOAD, not a CDC strategy.
    - storage_format: iceberg is only valid for batch_table; elsewhere use the enable_iceberg_read_uniformity table property.

!!! warning "Known errors and limitations"

    **Onboarding rejects storage_format: iceberg**  
    *Cause:* Iceberg storage is restricted to batch_table.  
    *Fix:* Keep delta and add enable_iceberg_read_uniformity to table_properties.

**FAQs** (4)

??? question "If omitted · What happens if target_type is missing?"

    Onboarding rejects it: `target_type: is required but was missing or empty`, on both ingestion and transformation flows — there is no implicit default.

??? question "Format gotcha · What are the only legal values for target_type?"

    `streaming_table`, `materialized_view`, `batch_table`, `external_sink`, `sink` (`ALLOWED_TARGET_TYPES`); any other value fails the allowed-values check from `check_string`.

??? question "Performance impact · Does materialized_view cost more than streaming_table at runtime?"

    Yes — `materialized_view` is fully recomputed on every update (not incrementally maintained), so it should be paired with `TRUNCATE_AND_LOAD`, not a CDC strategy; `streaming_table` is the default for incremental, lower-cost ingestion. See the attribute reference's tips and docs/13 line 963.

??? question "Edge case · Can I set storage_format: iceberg with target_type: streaming_table?"

    No — onboarding rejects it: 'iceberg' is only valid when `target_type == 'batch_table'`. Elsewhere, use `table_properties.enable_iceberg_read_uniformity` with `storage_format: delta` instead. Also note `target_type: sink` bypasses CDC dispatch entirely — `cdc_load_strategy` is never read for a pure sink target. See docs/13 K2 and docs/12 §1.


**See also:** [Pillar 1 · Ingestion](../../pillars/ingestion.md) · [Attribute dictionary](../../00_master_reference_index.md#2-ingestion-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-type) · [Spec Builder · Ingestion tab](../../console/spec_builder.md#ingestion-tab) · [Control dashboard · Ingestion Flows](../../console/control_dashboard.md#ingestion-flows) · [Observability dashboard · Data Flow & Throughput](../../console/observability_dashboard.md#data-flow-throughput) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [streaming tables](https://docs.databricks.com/tables/streaming.html) · [materialized views](https://docs.databricks.com/views/materialized.html)


---

