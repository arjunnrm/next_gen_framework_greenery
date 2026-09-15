<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Transformation flows

One entry per `transformation_flows[]` element — SQL plus a CDC load strategy.


!!! info "77 attributes · 321 FAQs"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form. Badges: <span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Flow kind</span> <span class="fx-badge fx-ver">vX.Y+ added in</span> <span class="fx-badge fx-only">Spec Builder section</span>.


**Recipes and deep dive:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Schema tree](tree.md) · [Removed & rejected](removed.md)


## Summary

| Attribute | Type | Required | Default | Since |
|---|---|---|---|---|
| [`dataflow_id`](#dataflow-id) | string | **yes** | — | — |
| [`decrypted_columns`](#decrypted-columns) | array<object> | no | — | — |
| [`decrypted_columns[].cast_to_type`](#decrypted-columnscast-to-type) | string | **yes** | — | — |
| [`decrypted_columns[].column_name`](#decrypted-columnscolumn-name) | string | **yes** | — | — |
| [`decrypted_columns[].input_name`](#decrypted-columnsinput-name) | string | **yes** | — | — |
| [`decrypted_columns[].mode`](#decrypted-columnsmode) | string (enum) | no | — | — |
| [`decrypted_columns[].output_column`](#decrypted-columnsoutput-column) | string | no | — | — |
| [`decrypted_columns[].secret.secret_catalog`](#decrypted-columnssecretsecret-catalog) | string | **yes** | — | — |
| [`decrypted_columns[].secret.secret_key`](#decrypted-columnssecretsecret-key) | string | **yes** | — | — |
| [`decrypted_columns[].secret.secret_schema`](#decrypted-columnssecretsecret-schema) | string | **yes** | — | — |
| [`dq_config.quarantine_table`](#dq-configquarantine-table) | string | no | — | — |
| [`dq_config.record_id_column`](#dq-configrecord-id-column) | string | no | — | — |
| [`dq_config.rules`](#dq-configrules) | array<object> | no | — | — |
| [`dq_config.rules[].action`](#dq-configrulesaction) | string (enum) | **yes** | — | — |
| [`dq_config.rules[].expression`](#dq-configrulesexpression) | string (SQL) | **yes** | — | — |
| [`dq_config.rules[].rule_id`](#dq-configrulesrule-id) | string | **yes** | — | — |
| [`flow_step_id`](#flow-step-id) | string | **yes** | — | — |
| [`governance_tags.column_tags`](#governance-tagscolumn-tags) | array<object> | no | — | — |
| [`governance_tags.column_tags[].column`](#governance-tagscolumn-tagscolumn) | string | **yes** | — | — |
| [`governance_tags.column_tags[].tags`](#governance-tagscolumn-tagstags) | object<string,string> | **yes** | — | — |
| [`governance_tags.table_tags`](#governance-tagstable-tags) | object<string,string> | no | — | — |
| [`source_inputs`](#source-inputs) | array<object> | no | — | — |
| [`source_inputs[].input_name`](#source-inputsinput-name) | string | **yes** | — | — |
| [`source_inputs[].is_streaming`](#source-inputsis-streaming) | boolean | no | — | — |
| [`source_inputs[].table`](#source-inputstable) | string | **yes** | — | — |
| [`source_inputs[].watermark.delay_threshold`](#source-inputswatermarkdelay-threshold) | string | no | — | — |
| [`source_inputs[].watermark.event_time_column`](#source-inputswatermarkevent-time-column) | string | no | — | — |
| [`target_catalog`](#target-catalog) | string | **yes** | — | — |
| [`target_config.auto_ttl.expire_in_days`](#target-configauto-ttlexpire-in-days) | integer | no | — | — |
| [`target_config.auto_ttl.timestamp_column`](#target-configauto-ttltimestamp-column) | string | no | — | — |
| [`target_config.capture_technical_metadata`](#target-configcapture-technical-metadata) | boolean | no | — | — |
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
| [`transformation_sql`](#transformation-sql) | string (SQL) | **yes** | — | — |

## Attributes

### `dataflow_id` { #dataflow-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Flow identity</span>

Unique ID for this ingestion flow.


Unique ID for this ingestion flow. Referenced by transformation flows.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.dataflow_id`


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
    SELECT flow_step_id,
           dataflow_id,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-ingestion-dataflow-id) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `decrypted_columns` { #decrypted-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Sets source_inputs[].decrypted_columns[].


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "decrypted_columns": [
        {
          "...": "one object per entry"
        }
      ]
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
    -- decrypted_columns lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.

**FAQs** (4)

??? question "If omitted · What happens when a source_inputs entry has no decrypted_columns?"

    Nothing is decrypted: `_validate_decrypted_columns` returns when the list is absent, and the input view exposes ciphertext columns as stored. Add the list only for columns the SQL must read in clear.

??? question "Format gotcha · Where in the spec may decrypted_columns appear?"

    Only inside a `source_inputs[]` entry of a transformation flow. It is not a `target_config` key and not an ingestion attribute; the schema and `ALLOWED_SOURCE_INPUT_KEYS` allow it in that one place.

??? question "Performance impact · Does decryption slow the transformation?"

    One `aes_decrypt` plus a cast per row per listed column, applied on the input view before the SQL runs. The key is resolved once per flow. Cost scales with rows times columns listed, so decrypt only what the SQL needs.

??? question "Edge case · Does the decrypted plaintext get written anywhere?"

    Only if your `transformation_sql` selects it into the target. The overlay lives on the input view inside the pipeline graph; re-encrypt on the target with `target_config.encrypted_columns` if the output must stay protected.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columns) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `decrypted_columns[].cast_to_type` { #decrypted-columnscast-to-type }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Spark SQL type to cast the decrypted value to.


Spark SQL type to cast the decrypted value to. Cross-validated against the original_data_type tag.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "cast_to_type": "string"
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
    -- decrypted_columns[].cast_to_type lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · Can I leave cast_to_type out and let Spark infer the type?"

    No. It is `required=True`: `...cast_to_type: is required but was missing or empty`. `aes_decrypt` returns binary, so the framework needs the target type to cast to.

??? question "Format gotcha · What values does cast_to_type accept, and is case significant?"

    Any Spark SQL type string such as `string`, `int`, `decimal(18,2)` or `timestamp`. It is compared case-insensitively against the encrypted column's `original_data_type` tag, so `STRING` and `string` are equal.

??? question "Performance impact · Is the cast expensive?"

    Negligible against the decrypt: `.cast(cast_to_type)` is a per-row conversion of the already decrypted string.

??? question "Edge case · What if cast_to_type disagrees with how the column was encrypted?"

    The update fails at graph definition with `decrypted_columns cast_to_type mismatch for column '<name>': configured cast_to_type='int' but the encrypted column's tagged original_data_type is 'string'`. The tag comes from the producer's `source_data_type` declaration or the type Spark observed at encryption time.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columnscast-to-type) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `decrypted_columns[].column_name` { #decrypted-columnscolumn-name }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Source ciphertext column on the input table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


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
    -- decrypted_columns[].column_name lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · Is column_name required on a decrypted column?"

    Yes: `...decrypted_columns[<n>].column_name: is required but was missing or empty`. It names the ciphertext column in the input table.

??? question "Format gotcha · Which name goes here, the original column or the encrypted output column?"

    The column as it exists in the input table, which is the producer's `encrypted_columns[].output_column` (or its `column_name` when it was encrypted in place). It must pass `assert_safe_identifier` at run time.

??? question "Performance impact · Does naming a column that is not encrypted cost anything?"

    It fails rather than costs: `aes_decrypt` on plaintext raises inside the pipeline and the update fails. Not validated at onboarding.

??? question "Edge case · What if the column is missing from the input table?"

    Graph definition fails with `Column '<name>' configured for decryption not present in DataFrame`, wrapped in a `CryptoError`. Onboarding cannot see the input schema, so this surfaces only at run time.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columnscolumn-name) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `decrypted_columns[].input_name` { #decrypted-columnsinput-name }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Which source_inputs[] entry this decryption belongs to.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "input_name": "example_raw_input"
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
    -- decrypted_columns[].input_name lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · Do I need input_name inside a decrypted column?"

    No. In the spec a decrypted column is nested under the `source_inputs[]` entry it belongs to, so the input is implied. The Spec Builder shows `input_name` here only to attach the row to its parent input.

??? question "Format gotcha · What happens if I hand-write input_name inside a decrypted column?"

    The JSON-schema gate rejects it: `decryptedColumn` has `additionalProperties: false` and no such key. The Python validator does not check unknown keys inside `decrypted_columns`, so validate against the schema too.

??? question "Performance impact · Does this helper affect the pipeline?"

    No. It never reaches the spec or the control table.

??? question "Edge case · How do I decrypt the same column from two different inputs?"

    Declare a `decrypted_columns` entry under each `source_inputs[]` entry. Each input view gets its own overlay; nothing is shared between inputs.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-decrypted-columnsinput-name) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `decrypted_columns[].mode` { #decrypted-columnsmode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Must match the mode the column was encrypted with.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


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
    -- decrypted_columns[].mode lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: GCM, CBC, ECB.

**FAQs** (4)

??? question "If omitted · Which cipher mode is used when mode is omitted on decryption?"

    `GCM`, the same default the encryption side uses (`col_config.get("mode", "GCM")`). If the producer encrypted with `CBC` or `ECB`, you must say so here.

??? question "Format gotcha · Is mode case-sensitive?"

    Yes: the validator accepts exactly `GCM`, `CBC` or `ECB` for the encryption side, and the runtime passes the string straight to `aes_decrypt`. Write it in upper case.

??? question "Performance impact · Is one mode faster to decrypt than another?"

    Differences are small next to I/O. GCM additionally verifies the authentication tag; CBC and ECB do not. None of them is a reason to pick a mode over its security properties.

??? question "Edge case · What happens when the decryption mode does not match the encryption mode?"

    `aes_decrypt` fails or returns garbage and the update fails inside the pipeline; onboarding cannot detect it because the two sides live in different flows. Keep mode identical on both `encrypted_columns[]` and `decrypted_columns[]` for the same column.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columnsmode) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `decrypted_columns[].output_column` { #decrypted-columnsoutput-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Output plaintext column.


Output plaintext column. Use a different name to keep both ciphertext and plaintext.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "output_column": "pii_column_plain"
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
    -- decrypted_columns[].output_column lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if I omit output_column on a decrypted column?"

    The plaintext replaces the ciphertext column in place: `output_column` defaults to `column_name` in `apply_aes_column_decryption`. Set it to keep both.

??? question "Format gotcha · Are there naming rules for output_column?"

    It must be a safe identifier (`assert_safe_identifier`): letters, digits and underscores. Onboarding checks only that it is a string; an unsafe name fails at graph definition.

??? question "Performance impact · Does writing to a new output_column cost more than in-place?"

    Marginally: the view carries one extra column. The decrypt itself is identical.

??? question "Edge case · Can output_column collide with a column the SQL already uses?"

    Yes, silently: `withColumn` overwrites an existing column of that name on the input view before the SQL runs. Choose a distinct name such as `<column>_plain`.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columnsoutput-column) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `decrypted_columns[].secret.secret_catalog` { #decrypted-columnssecretsecret-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


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
    -- decrypted_columns[].secret.secret_catalog lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · Is secret_catalog required for a decrypted column?"

    Yes. `check_secret_ref` requires all three of `secret_catalog`, `secret_schema` and `secret_key`; a missing part reports `...secret.secret_catalog: is required but was missing or empty`.

??? question "Format gotcha · Is this a Unity Catalog catalog or a classic secret scope?"

    A Unity Catalog three-level secret: the runtime resolves `dbutils.secrets.get(catalog=, schema=, key=)`. Classic workspace scopes are not supported anywhere in the framework.

??? question "Performance impact · Is the key fetched per row?"

    No. `resolve_secret_ref` runs once per column config at graph definition; the resolved key is a literal in the decrypt expression.

??? question "Edge case · Can the decryption secret live in a different catalog from the encryption secret?"

    Yes, as long as both resolve to the same key bytes. Use the same reference on both sides to avoid rotation drift.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columnssecretsecret-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `decrypted_columns[].secret.secret_key` { #decrypted-columnssecretsecret-key }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

UC secret key name.


UC secret key name. AES keys must be exactly 16, 24 or 32 bytes.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


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
    -- decrypted_columns[].secret.secret_key lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What error appears when secret_key is missing?"

    `...secret.secret_key: is required but was missing or empty` at onboarding.

??? question "Format gotcha · Does the value here hold the key material?"

    No. It is the name of the secret; the bytes are read from Unity Catalog at graph definition. A literal key in the spec would be stored in plain text in the control table, and the Spec Builder refuses keys named like secrets.

??? question "Performance impact · Does the key length affect decrypt speed?"

    Negligibly. Spark's `aes_decrypt` accepts 16, 24 or 32-byte keys; throughput differences between them are dwarfed by I/O.

??? question "Edge case · After rotating the key, what happens to rows encrypted with the old one?"

    They no longer decrypt: `aes_decrypt` with the wrong key fails the update. Rotate by re-encrypting the source column, then switch both `encrypted_columns[]` and `decrypted_columns[]` references together. See docs/05 §5.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columnssecretsecret-key) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `decrypted_columns[].secret.secret_schema` { #decrypted-columnssecretsecret-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Decrypted columns</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


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
    -- decrypted_columns[].secret.secret_schema lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What if secret_schema is left out?"

    Onboarding rejects the column: `...secret.secret_schema: is required but was missing or empty`.

??? question "Format gotcha · Should the schema name be quoted or qualified?"

    A bare schema name, e.g. `security`; the catalog is the sibling `secret_catalog` key. Case is passed through as written.

??? question "Performance impact · Does the schema choice change anything at run time?"

    No; it is one component of a single lookup made once per column.

??? question "Edge case · What happens when the pipeline's run-as identity cannot read the secret?"

    Onboarding cannot check grants; the first update fails at graph definition when `dbutils.secrets.get` is denied. Grant the pipeline principal access to the secret before running.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsdecrypted-columnssecretsecret-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `dq_config.quarantine_table` { #dq-configquarantine-table }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Data quality</span>

Quarantine table name.


Quarantine table name. Only created when at least one rule uses action quarantine.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.dq_config_json`


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
    SELECT flow_step_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configquarantine-table) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.record_id_column` { #dq-configrecord-id-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Data quality</span>

Surfaced as __framework_record_id on quarantined rows for traceability.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.dq_config_json`


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
    SELECT flow_step_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrecord-id-column) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.dq_config_json`


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
    SELECT flow_step_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrules) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].action` { #dq-configrulesaction }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

warn logs and keeps the row, drop silently removes it, fail aborts the pipeline, quarantine routes it to the quarantine table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `warn`, `drop`, `fail`, `quarantine` | — |

**Persisted in** `config.transformation_flow_spec.dq_config_json`


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
    SELECT flow_step_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesaction) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].expression` { #dq-configrulesexpression }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Boolean Spark SQL expression evaluated per row.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.dq_config_json`


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
    SELECT flow_step_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesexpression) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].rule_id` { #dq-configrulesrule-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Unique rule identifier.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.dq_config_json`


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
    SELECT flow_step_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesrule-id) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `flow_step_id` { #flow-step-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Flow identity</span>

Unique ID for this transformation step.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.flow_step_id`


=== "JSON"

    ```json
    {
      "flow_step_id": "ts_template_scd1_example"
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
    -- flow_step_id is persisted as its own column
    SELECT flow_step_id,
           flow_step_id,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if a transformation flow has no flow_step_id?"

    Onboarding rejects it: `check_string(..., required=True)` reports `transformation_flow[<missing flow_step_id>].flow_step_id: is required but was missing or empty`. Every other error for that flow is labelled with the same placeholder, so fix this first.

??? question "Format gotcha · Is there a naming convention for flow_step_id, and is it validated?"

    Only a non-empty string is enforced. The governance skill's convention is `tf_<entity>_<purpose>`; the value becomes the row key of `config.transformation_flow_spec` and the prefix of every input view id (`<flow_step_id>:input:<input_name>`), so keep it short and stable.

??? question "Performance impact · Does the choice of flow_step_id affect runtime cost?"

    Negligible. It is metadata: a MERGE key at onboarding and a label in the pipeline graph. No data path depends on its length or content.

??? question "Edge case · What if two flows in one spec reuse the same flow_step_id?"

    The validator does not flag duplicate `flow_step_id` values. `upsert_transformation_flow_spec` merges on `t.flow_step_id = s.flow_step_id`, so the second definition silently overwrites the first in the control table and only one flow enters the graph. Treat uniqueness as your responsibility.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-flow-step-id) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `governance_tags.column_tags` { #governance-tagscolumn-tags }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Governance tags</span>

Unity Catalog tags applied to individual columns after deployment.


Drives discovery and classification. Applied via ALTER TABLE SET TAGS once the table exists.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.transformation_flow_spec.governance_tags_json`


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
    SELECT flow_step_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagscolumn-tags) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `governance_tags.column_tags[].column` { #governance-tagscolumn-tagscolumn }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Governance tags</span>

Column to tag.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.governance_tags_json`


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
    SELECT flow_step_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagscolumn-tagscolumn) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.governance_tags_json`


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
    SELECT flow_step_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagscolumn-tagstags) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.governance_tags_json`


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
    SELECT flow_step_id,
           governance_tags_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#6-governance-tagging) · [Schema tree](tree.md#tree-ingestion-governance-tagstable-tags) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)


---

### `source_inputs` { #source-inputs }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Source inputs</span>

Sets source_inputs[].


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "source_inputs": [
        {
          "...": "one object per entry"
        }
      ]
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
    -- source_inputs lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.

**FAQs** (4)

??? question "If omitted · Is source_inputs mandatory on a transformation flow?"

    Not for the validator: `_validate_source_inputs` returns silently when the list is absent. Without inputs, `transformation_sql` can only name fully qualified tables, which bypasses the read-once source plane for external tables. Declare every upstream as an input so the binding is planned.

??? question "Format gotcha · What shape does each source_inputs entry take?"

    An object with `input_name` and `table` (both required), optional `is_streaming`, `watermark` and `decrypted_columns`. Any other key is rejected: `reject_unknown_keys` runs with `ALLOWED_SOURCE_INPUT_KEYS`. In particular there is no `alias` key; `input_name` is the SQL identifier.

??? question "Performance impact · Do two inputs naming the same table read it twice?"

    No. Since v1.5.0 an input view is an alias over the read-once source plane: both inputs bind to one base node and share a single external read, per the Single-Read rule. See docs/03 §1.

??? question "Edge case · What happens when an input names a table this same dataflow group publishes?"

    It becomes a real graph edge (`dlt.read` or `dlt.read_stream` on the sibling dataset) rather than a second scan, so Lakeflow orders the flows for you. Streaming such an input is only legal when the producer is append-only; a MERGE-written or fully recomputed producer is rejected by the streaming-read guard.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputs) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_inputs[].input_name` { #source-inputsinput-name }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Source inputs</span>

Must be unique across the entire spec, not just this flow.


Must be unique across the entire spec, not just this flow. Referenced in transformation_sql FROM/JOIN.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "input_name": "example_raw_input"
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
    -- source_inputs[].input_name lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What if an input has no input_name?"

    Onboarding rejects it: `transformation_flow[<id>].source_inputs[<n>].input_name: is required but was missing or empty`. The name is the `dlt.view` the SQL reads from, so there is no default.

??? question "Format gotcha · Does input_name have to be a valid SQL identifier?"

    It is used verbatim as the view name and in `FROM <input_name>`, so keep it to letters, digits and underscores. The validator checks only for a non-empty string; a name with spaces or dashes fails at graph definition, not at onboarding.

??? question "Performance impact · Does input_name influence how much data is read?"

    No. It labels a binding; the read itself is planned once per source identity and execution mode regardless of how many names point at it.

??? question "Edge case · Can two transformation flows in one spec both call an input `ord`?"

    No. `_validate_no_duplicate_input_names` rejects it: `'ord' is already used by transformation_flow[<other>] ... input_name must be unique across all transformation_flows in this spec`, because every input is registered as a pipeline-wide view under its literal name.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsinput-name) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_inputs[].is_streaming` { #source-inputsis-streaming }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Source inputs</span>

true reads with spark.readStream.table, false with spark.read.table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "is_streaming": true
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
    -- source_inputs[].is_streaming lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What does an input do when is_streaming is not set?"

    It defaults to `false` (`input_config.get("is_streaming", False)` in `transformation/inputs.py`): a batch bind, a point-in-time snapshot of the table on every update.

??? question "Format gotcha · Can I write is_streaming as the string 'true'?"

    No. `check_bool` rejects a quoted value; use the JSON boolean `true`. In YAML, `true` without quotes.

??? question "Performance impact · When is a streaming input cheaper than a batch one?"

    For an append-only source that grows continuously: the stream reads only what arrived since the last checkpoint, while a batch bind rescans the whole table each update. For a small dimension table the batch read is simpler and costs little.

??? question "Edge case · Why does is_streaming: true fail on a table produced by an SCD1 flow?"

    A MERGE-written, snapshot-applied or fully recomputed producer cannot be streamed; the source plane raises `Consumer '<id>' requested a streaming read of '<table>', which is ... cdc_load_strategy=... / target_type=...`. Set `is_streaming: false` for that input. A materialized view is rejected the same way (`a materialized view cannot be read with dlt.read_stream`).


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputsis-streaming) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_inputs[].table` { #source-inputstable }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Source inputs</span>

Fully-qualified upstream table.


Fully-qualified upstream table. Can be any table, not only ones produced by this spec.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "table": "{{catalog}}.bronze_example.example_raw"
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
    -- source_inputs[].table lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · Can I leave table empty and rely on input_name alone?"

    No. `table` is `required=True`; the error is `...source_inputs[<n>].table: is required but was missing or empty`. The input has nothing to bind to without it.

??? question "Format gotcha · What form must table take?"

    A fully qualified `catalog.schema.table`; `{{catalog}}` and `{{env}}` are substituted once at onboarding, `${param}` is substituted on every update. The Spec Builder renders it as a single box, unlike a reconciliation target which is split into three.

??? question "Performance impact · Is a large table read in full on every update?"

    For a batch binding, yes: the input is a point-in-time snapshot each update. Set `is_streaming: true` on an append-only source to read only new files or rows through a checkpoint.

??? question "Edge case · What if the table does not exist yet when I onboard?"

    Onboarding succeeds; the validator tolerates unresolved references because inputs may be produced by this same group. The first pipeline update fails at graph definition if the name never resolves. Check the three-part name and the catalog grant of the pipeline's run-as identity.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputstable) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_inputs[].watermark.delay_threshold` { #source-inputswatermarkdelay-threshold }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Source inputs</span>

Maximum allowed event lateness.


Maximum allowed event lateness. Required when streaming and joined with another stream.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "watermark": {
        "delay_threshold": "10 minutes"
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
    -- source_inputs[].watermark.delay_threshold lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What if I give event_time_column but no delay_threshold?"

    Onboarding rejects the input: `...watermark.delay_threshold: is required but was missing or empty`. Both halves of a watermark are mandatory.

??? question "Format gotcha · What syntax does delay_threshold accept?"

    A Spark interval string passed straight to `withWatermark`, for example `"10 minutes"` or `"2 hours"`. The validator checks only that it is a non-empty string; a malformed interval fails at graph definition.

??? question "Performance impact · How does the threshold size affect memory?"

    State is retained for at least the threshold: a larger delay keeps more keys in the state store and delays output of late-arriving windows; a smaller one drops late events. Size it to the real lateness of the source.

??? question "Edge case · Is the threshold applied when the input is read in batch mode?"

    No. The watermark overlay is skipped unless `is_streaming` is true, so the value is inert on a batch input and no late-event filtering occurs there.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputswatermarkdelay-threshold) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_inputs[].watermark.event_time_column` { #source-inputswatermarkevent-time-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Source inputs</span>

Event-time column for watermarking.


Event-time column for watermarking. Auto-cast to timestamp. Required when streaming and joined with another stream.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.source_inputs_json`


=== "JSON"

    ```json
    {
      "watermark": {
        "event_time_column": "updated_at"
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
    -- source_inputs[].watermark.event_time_column lives inside the source_inputs_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           source_inputs_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · Can I set delay_threshold without event_time_column?"

    No. Once `watermark` is present both keys are required: `...watermark.event_time_column: is required but was missing or empty`. Omit the whole `watermark` object if you do not need one.

??? question "Format gotcha · Does the column have to be a TIMESTAMP already?"

    No. `transformation/inputs.py` casts it to `timestamp` before `withWatermark`, because Spark requires a real `TimestampType`. A string that does not parse becomes null and the watermark never advances, so prefer a proper timestamp column.

??? question "Performance impact · Does a watermark cost anything on a batch input?"

    Nothing useful: `withWatermark` is applied only when `is_streaming` is true (`if is_streaming and event_time_column`). On a batch input the setting is inert.

??? question "Edge case · When do I actually need a watermark on an input?"

    For stateful streaming operations downstream of the input: stream-stream joins, windowed aggregations and deduplication. Without it Spark keeps state for every key forever. Ingestion-side dedup has its own `dedup_watermark` on `source_config`.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-source-inputswatermarkevent-time-column) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_catalog` { #target-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

Unity Catalog catalog for the target table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_catalog`


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
    SELECT flow_step_id,
           target_catalog,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.auto_ttl.expire_in_days` { #target-configauto-ttlexpire-in-days }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Rows older than this many days are auto-deleted.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `integer` | — | — | min `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configauto-ttlexpire-in-days) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.auto_ttl.timestamp_column` { #target-configauto-ttltimestamp-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

The column whose value decides how old a row is, for automatic expiry.


auto TTL deletes rows whose timestamp is older than expire_in_days.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configauto-ttltimestamp-column) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.capture_technical_metadata` { #target-configcapture-technical-metadata }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Transformation flows only.


Transformation flows only. Gates __framework_ingestion_timestamp_utc.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "capture_technical_metadata": true
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
    -- target_config.capture_technical_metadata lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens if capture_technical_metadata is left unset on a transformation flow?"

    Not explicitly defaulted by the validator — the JSON schema notes it is 'conventionally transformation_flows-only ... not actually restricted by the validator'. Omitting it is not the same as `false`; check the runtime default in `ingestion/technical_metadata.py` before relying on either behaviour.

??? question "Format gotcha · Is capture_technical_metadata a boolean, or can I pass a string like 'true'?"

    Strictly boolean — `check_bool` is used, so `"true"` (a string) is rejected; use the JSON literal `true`/`false`.

??? question "Performance impact · Does turning capture_technical_metadata off save meaningful compute?"

    Negligible — it only gates whether `__framework_ingestion_timestamp_utc` is added to the row, a single extra column computed once per batch (`current_timestamp()`), not a per-row cost.

??? question "Edge case · What breaks if I set capture_technical_metadata to false and don't set sequence_by_column?"

    Setting it `false` removes the `__framework_ingestion_timestamp_utc` fallback column that `sequence_by_column` defaults to, which then makes `sequence_by_column` effectively mandatory. Per the attribute reference's errors, the pipeline fails naming a missing `__framework_ingestion_timestamp_utc` column if you leave `sequence_by_column` unset in that case.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configcapture-technical-metadata) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns` { #target-configencrypted-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Sets target_config.encrypted_columns[].


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columns) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].column_name` { #target-configencrypted-columnscolumn-name }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Plaintext column on this flow's DataFrame to encrypt.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnscolumn-name) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].mode` { #target-configencrypted-columnsmode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

AES cipher mode.


AES cipher mode. GCM is recommended — it adds a random IV.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `GCM`, `CBC`, `ECB` | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnsmode) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].output_column` { #target-configencrypted-columnsoutput-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Output column name.


Output column name. Defaults to column_name — set the same name to encrypt in place.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnsoutput-column) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.encrypted_columns[].secret.secret_catalog` { #target-configencrypted-columnssecretsecret-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssecretsecret-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssecretsecret-key) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.encrypted_columns[].secret.secret_schema` { #target-configencrypted-columnssecretsecret-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · encrypted columns</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssecretsecret-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configencrypted-columnssource-data-type) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.liquid_clustering_columns` { #target-configliquid-clustering-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · storage &amp; table</span>

Clustering keys for Delta liquid clustering.


Gives data skipping without the file-count problems of partitioning, and can be changed later without rewriting the table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configliquid-clustering-columns) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configpartition-columns) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configpartition-mode) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configexport-trigger) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configformat) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configkafka-options) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpath) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivearchive-format) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.enabled` { #target-configsink-configpost-export-archiveenabled }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Enable post-write archiving for pgp_zip exports.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archiveenabled) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archiveexport-file-name-format) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.output_zip_path` { #target-configsink-configpost-export-archiveoutput-zip-path }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Where the final ZIP is written.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archiveoutput-zip-path) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [sinks](https://docs.databricks.com/delta-live-tables/sinks.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.enabled` { #target-configsink-configpost-export-archivepgp-encryptionenabled }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

PGP-encrypt the output archive.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionenabled) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-key) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionpassphrase-secretsecret-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-key) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.recipient_public_key_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionrecipient-public-key-secretsecret-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-key) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_passphrase_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-passphrase-secretsecret-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_catalog` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-key) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.sign_with_private_key_secret.secret_schema` { #target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionsign-with-private-key-secretsecret-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_catalog` { #target-configsink-configpost-export-archivesecretsecret-catalog }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret catalog holding the key.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivesecretsecret-catalog) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivesecretsecret-key) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_config.sink_config.post_export_archive.secret.secret_schema` { #target-configsink-configpost-export-archivesecretsecret-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target · sink config</span>

Unity Catalog secret schema.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivesecretsecret-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-format) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-optionsdelimiter) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-optionsinclude-header) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configstaged-file-optionsline-terminator) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configwrite-mode) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configstorage-format) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


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

**Persisted in** `config.transformation_flow_spec.target_config_json`


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
    SELECT flow_step_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configtable-properties) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [table properties](https://docs.databricks.com/delta/table-properties.html) · [uniform](https://docs.databricks.com/delta/uniform.html)


---

### `target_schema` { #target-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

Schema for the target table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_schema`


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
    SELECT flow_step_id,
           target_schema,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-schema) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_table` { #target-table }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

Target Delta table name.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.target_table`


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
    SELECT flow_step_id,
           target_table,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-table) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_type` { #target-type }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Target dataset</span>

The kind of Lakeflow dataset registered for this flow.


Decides whether the target is incrementally maintained, fully recomputed, or export-only.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `streaming_table`, `materialized_view`, `batch_table`, `external_sink`, `sink` | — |

**Persisted in** `config.transformation_flow_spec.target_type`


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
    SELECT flow_step_id,
           target_type,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
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


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-ingestion-target-type) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [streaming tables](https://docs.databricks.com/tables/streaming.html) · [materialized views](https://docs.databricks.com/views/materialized.html)


---

### `transformation_sql` { #transformation-sql }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-only">Transformation SQL</span>

The SQL transform.


The SQL transform. Reads from input_name values declared in source_inputs.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | min length `1` |

**Persisted in** `config.transformation_flow_spec.transformation_sql`


=== "JSON"

    ```json
    {
      "transformation_sql": "SELECT example_id, region, amount FROM example_raw_input WHERE amount > ${min_amount}"
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
    -- transformation_sql is persisted as its own column
    SELECT flow_step_id,
           transformation_sql,
           is_active, updated_at
    FROM   <catalog>.config.transformation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.

**FAQs** (4)

??? question "If omitted · Can a transformation flow work without transformation_sql?"

    No. It is `required=True`: `transformation_flow[<id>].transformation_sql: is required but was missing or empty`. A pass-through flow still needs a `SELECT * FROM <input_name>`.

??? question "Format gotcha · How should I write ${param} placeholders inside transformation_sql?"

    Bare, never inside your own quotes. `substitute_dynamic_parameters` renders a string parameter as a single-quoted SQL literal, so `WHERE country = '${filter_country}'` becomes `''US''` and fails with a `ParseException`. Write `WHERE country = ${filter_country}`. A placeholder with no matching `pipeline_parameters` key fails onboarding naming the parameter.

??? question "Performance impact · Is transformation_sql executed during onboarding, and how heavy is that?"

    Only an `EXPLAIN` of the post-substitution SQL runs, never the query itself. A `ParseException` is a hard error; structural planning errors (`NUM_COLUMNS_MISMATCH`, `INCOMPATIBLE_COLUMN_TYPE`, typically a misaligned `UNION`) are hard errors; unresolved table references are tolerated because input views exist only at pipeline run time.

??? question "Edge case · Why does my SQL pass onboarding but fail with an unresolved table at pipeline start?"

    The `EXPLAIN` check deliberately tolerates unresolved references, since `FROM <input_name>` resolves only inside the running graph. At run time a name must match a `source_inputs[].input_name` in the same spec or a fully qualified table. A typo in the alias is therefore caught by the pipeline, not by onboarding.


**See also:** [Pillar 2 · Transformation](../../pillars/transformation.md) · [Attribute dictionary](../../00_master_reference_index.md#7-transformation-flow-schema) · [Schema tree](tree.md#tree-transformation-transformation-sql) · [Spec Builder · Transformation tab](../../console/spec_builder.md#transformation-tab) · [Control dashboard · Transformation Flows](../../console/control_dashboard.md#transformation-flows) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

