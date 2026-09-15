<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# CDC / load strategy

Attributes under `target_config` that only apply to particular CDC load strategies. The Spec Builder shows these on the **Load strategy** step and hides the ones the selected strategy does not use.


!!! info "10 attributes · 43 FAQs"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form. Badges: <span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Flow kind</span> <span class="fx-badge fx-ver">vX.Y+ added in</span> <span class="fx-badge fx-only">Spec Builder section</span>.


**Recipes and deep dive:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Schema tree](tree.md) · [Removed & rejected](removed.md)


## Summary

| Attribute | Type | Required | Default | Since |
|---|---|---|---|---|
| [`target_config.cdc_load_strategy`](#target-configcdc-load-strategy) | string | no | — | — |
| [`target_config.cdc_operation_column`](#target-configcdc-operation-column) | string | no | — | — |
| [`target_config.cdc_operation_mapping.delete_values`](#target-configcdc-operation-mappingdelete-values) | array<string> | **yes** | — | — |
| [`target_config.columns_to_check`](#target-configcolumns-to-check) | array<string> | no | — | — |
| [`target_config.columns_to_exclude`](#target-configcolumns-to-exclude) | array<string> | no | — | — |
| [`target_config.empty_target_if_source_empty`](#target-configempty-target-if-source-empty) | boolean | no | `false` | — |
| [`target_config.generate_hash_columns`](#target-configgenerate-hash-columns) | boolean | no | — | — |
| [`target_config.primary_keys`](#target-configprimary-keys) | array<string> | **yes** | — | — |
| [`target_config.sequence_by_column`](#target-configsequence-by-column) | string | no | — | — |
| [`target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret`](#target-configsink-configpost-export-archivepgp-encryptionpassphrase-secret) | string | no | — | v1.7.4 |

## Attributes

### `target_config.cdc_load_strategy` { #target-configcdc-load-strategy }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span>

How rows are merged into the target table.


This single choice decides which other target_config attributes are meaningful; the strategy tabs re-scope the form around it. Two families: APPEND and TRUNCATE_AND_LOAD need no key and skip the CDC dispatcher entirely, while SCD1, SCD2, SCD3 and FULL_SNAPSHOT_CDC all merge on primary_keys. Choose by asking three things about the SOURCE, in order -- does each row have a business key, does the source deliver every live row on every run, and how much history the business actually needs.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | `APPEND`, `TRUNCATE_AND_LOAD`, `SCD1`, `SCD2`, `SCD3`, `FULL_SNAPSHOT_CDC` | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.cdc_load_strategy` · **Behaviour changed in** v1.7.07


=== "JSON"

    ```json
    "target_config": {
      "cdc_load_strategy": "SCD2",
      "primary_keys": ["customer_id"],
      "sequence_by_column": "updated_at",
      "columns_to_check": ["status", "tier"]
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
    -- target_config.cdc_load_strategy is persisted as its own column
    SELECT dataflow_id,
           cdc_load_strategy,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
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

**FAQs** (5)

??? question "If omitted · What happens if cdc_load_strategy is not set at all?"

    Onboarding rejects the flow — `_validate_target_config` calls `check_string(..., required=True)` for `cdc_load_strategy`, so it is mandatory whenever `target_config` is present; there is no implicit default strategy.

??? question "Format gotcha · Are the strategy names case-sensitive, e.g. can I write scd2 or Append?"

    Yes, case-sensitive uppercase only — the enum is exactly `["APPEND", "TRUNCATE_AND_LOAD", "SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"]`; anything else, including lowercase, fails the `allowed_values` check.

??? question "Performance impact · Which cdc_load_strategy is cheapest to run at scale?"

    APPEND is cheapest (no merge, pure append). TRUNCATE_AND_LOAD costs scale with full table size regardless of how much changed (docs/03 §2.0). SCD2 is the most expensive to query historically since it retains every version; SCD3 is flagged in docs/03 as having the 'slow recompute' failure mode since it rebuilds from a hidden history table each run.

??? question "Edge case · Can I use SCD3 on an ingestion flow?"

    No — SCD3 is transformation-flows-only, enforced as a hard error at the ingestion-flow level (not inside `_validate_target_config` itself, which accepts the transformation superset). Docs/03 §2.5 confirms it is 'Allowed only in transformation_flows (never in ingestion_flows)'.

??? question "Edge case · I set partition_columns alongside an SCD2 strategy — why is the table still unpartitioned?"

    Partitioning and liquid clustering only take effect for APPEND and TRUNCATE_AND_LOAD. For SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC, `apply_changes`/`apply_changes_from_snapshot` do not accept `partition_cols`/`cluster_by` at all, so the fields are silently inert, not rejected. See docs/03 §3.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configcdc-load-strategy) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.cdc_operation_column` { #target-configcdc-operation-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

Column carrying the source's insert/update/delete indicator.


Without it, SCD1 and SCD2 have no delete signal at all and their targets only ever grow. Optional regardless of primary_keys -- a source can have a real key and no delete marker.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json` · **Behaviour changed in** v1.4.0


=== "JSON"

    ```json
    {
      "target_config": {
        "cdc_operation_column": "op"
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
    -- target_config.cdc_operation_column lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Requires cdc_operation_mapping.delete_values alongside it; the validator rejects one without the other.
    - SCD1 removes the row outright. SCD2 closes the current version and keeps the history.
    - On FULL_SNAPSHOT_CDC, flagged rows are filtered out of the snapshot before the diff, which deletes them by absence -- the same end result by a different route.
    - A validation error on SCD3, which has no delete path at all.

**FAQs** (4)

??? question "If omitted · What happens if cdc_operation_column is not set on an SCD1 target?"

    Deletes never propagate — the target only ever grows. This is a valid, common configuration (a source can have a real primary key with no explicit delete marker); it is not an onboarding error.

??? question "Format gotcha · Does cdc_operation_column need to match a specific value type like a single character code?"

    No fixed format — it is validated only as a non-empty column name string. The actual delete-indicating values it carries are declared separately in `cdc_operation_mapping.delete_values`.

??? question "Performance impact · Does configuring a delete-marker column add noticeable overhead to the merge?"

    Negligible — `_build_apply_as_deletes_expr` in `cdc/scd.py` just derives one extra predicate column passed to `dlt.apply_changes`'s native `apply_as_deletes` parameter; it does not add a separate scan or pass.

??? question "Edge case · Can I set cdc_operation_column on an SCD3 target?"

    No — the validator confirms this is a validation error on SCD3, which has no delete path at all. It is only meaningful for SCD1, SCD2 and FULL_SNAPSHOT_CDC, per `_validate_target_config`'s `strategies_supporting_delete_marker` set.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configcdc-operation-column) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html) · [delta change data feed](https://docs.databricks.com/delta/delta-change-data-feed.html)


---

### `target_config.cdc_operation_mapping.delete_values` { #target-configcdc-operation-mappingdelete-values }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

Values in cdc_operation_column meaning this row is a delete.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | min items `1` |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json`


=== "JSON"

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
    -- target_config.cdc_operation_mapping.delete_values lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What error do I get if I set cdc_operation_column but leave out delete_values?"

    Onboarding rejects it — `_validate_target_config` requires `cdc_operation_mapping` as a dict (`required=True`) whenever `cdc_operation_column` is set, and within it `check_list_of_str(..., required=True)` for `delete_values`, so one without the other is a hard error.

??? question "Format gotcha · Is delete_values a JSON array or a comma-separated string in the spec?"

    A JSON array of strings, e.g. `["D"]`. Per the inspector tip, the Databricks App UI accepts it as a comma-separated list in the form and serializes it to a JSON array — but the underlying spec attribute itself is always an array.

??? question "Performance impact · Does listing multiple delete_values slow down the CDC merge?"

    Negligible — it becomes one `isin(...)`-style predicate passed as `apply_as_deletes` to `dlt.apply_changes`, evaluated once per row alongside the merge itself.

??? question "Edge case · On FULL_SNAPSHOT_CDC, how does delete_values interact with the snapshot diff?"

    Per the inspector tip, on FULL_SNAPSHOT_CDC flagged rows are filtered out of the snapshot before the diff runs, so they are deleted by absence rather than by an explicit apply_as_deletes predicate — the same end result reached by a different mechanism than SCD1/SCD2.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configcdc-operation-mappingdelete-values) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html) · [delta change data feed](https://docs.databricks.com/delta/delta-change-data-feed.html)


---

### `target_config.columns_to_check` { #target-configcolumns-to-check }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

Limits which columns count as a change.


It means something different per strategy: SCD2 tracks history only on these columns, SCD3 pivots exactly these into current_/previous_ pairs, and SCD1 uses them only to narrow the change-detection hash -- the merge itself is unaffected, so an unchanged row still costs a write.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json`


=== "JSON"

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
    -- target_config.columns_to_check lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required for SCD3 -- registration fails without it.
    - Leave it empty on SCD2 and every column counts, so one volatile source column (a load timestamp, a batch id) versions every row on every run.
    - Framework technical columns are excluded from comparison automatically. Columns the SOURCE stamps are not -- exclude those yourself.
    - Never include encrypted columns: AES-GCM's random IV makes the ciphertext differ every run.
    - Rejected on APPEND, TRUNCATE_AND_LOAD and FULL_SNAPSHOT_CDC.

**FAQs** (5)

??? question "If omitted · What happens if I don't set columns_to_check on an SCD2 flow?"

    Every applicable column is compared for change detection. Per docs/03 §2.0, leaving it empty on SCD2 means one volatile source column (a load timestamp, a batch id) can version every row on every run — scope it deliberately.

??? question "Format gotcha · Is columns_to_check a list of column names or SQL expressions?"

    A plain JSON array of column name strings, validated by `check_list_of_str`. It does not accept SQL expressions.

??? question "Performance impact · Does a narrow columns_to_check list reduce merge cost on SCD1?"

    Not the merge cost itself — per the attribute's rationale, on SCD1 it only narrows the change-detection hash basis; the merge is unaffected, so an unchanged row still costs a write. The savings are in avoiding false-positive 'changed' detections, not I/O.

??? question "Edge case · Is columns_to_check accepted on an APPEND or TRUNCATE_AND_LOAD target?"

    No — the validator rules state it is rejected on APPEND, TRUNCATE_AND_LOAD and FULL_SNAPSHOT_CDC. It is meaningful only where a comparison basis exists: SCD1, SCD2 (history scoping) and SCD3 (which requires it to pivot into current_/previous_ column pairs — registration fails without it).

??? question "Edge case · Can I include an AES-GCM encrypted column in columns_to_check?"

    Never do this — AES-GCM uses a random IV, so ciphertext differs every run even when the plaintext hasn't changed, making every row look changed on every run. This is a documented trap, not currently caught by onboarding validation.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configcolumns-to-check) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.columns_to_exclude` { #target-configcolumns-to-exclude }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

Columns that should not participate in change comparison.


Aimed at technical columns a source stamps on every row -- load timestamps, batch ids, checksums -- which would otherwise make every row look changed on every run.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json`


=== "JSON"

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
    -- target_config.columns_to_exclude lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - SCD1, SCD2 and SCD3 only; a validation error on APPEND, TRUNCATE_AND_LOAD and FULL_SNAPSHOT_CDC.
    - primary_keys and the framework's own technical columns are excluded automatically -- you do not need to list them.
    - This field does TWO things: it narrows change comparison AND it is passed to apply_changes's except_column_list, which DROPS the listed columns from the target table's schema. It is the only way to keep a column out of a CDC target -- data_standardization_sql can only add or replace a column, never remove one.

**FAQs** (4)

??? question "If omitted · What happens if columns_to_exclude is not set on an SCD1 target?"

    All resolved columns participate in comparison, and nothing is dropped from the target schema via `except_column_list`. This is the default, inert state — no columns are excluded unless you list them.

??? question "Format gotcha · Is columns_to_exclude a list of column names, same shape as columns_to_check?"

    Yes — both are plain JSON arrays of strings, validated by `check_list_of_str`. Do not list `primary_keys` or framework technical columns here; they are excluded automatically.

??? question "Performance impact · Does excluding columns reduce the size of the target table?"

    Yes, meaningfully for wide sources — `cdc/scd.py::_build_except_column_list` passes it straight to `dlt.apply_changes`'s native `except_column_list`, which drops those columns from the target's physical schema entirely (per row and per version on SCD2), not just from comparison.

??? question "Edge case · Is columns_to_exclude valid on APPEND or FULL_SNAPSHOT_CDC?"

    No — `_validate_target_config` only allows it for SCD1, SCD2 and SCD3 (`strategies_supporting_comparison_exclusion`); on any other strategy it is a hard onboarding error: 'only meaningful for cdc_load_strategy in [...]'.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configcolumns-to-exclude) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.empty_target_if_source_empty` { #target-configempty-target-if-source-empty }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

Whether a zero-row TRUNCATE_AND_LOAD source is allowed to blank the target.


WITHDRAWN -- this field currently has no runtime effect. It still validates so existing specs stay valid, but nothing reads it, and a zero-row source blanks the target either way.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `false` | — | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "empty_target_if_source_empty": true
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
    -- target_config.empty_target_if_source_empty lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
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

**FAQs** (4)

??? question "If omitted · What is the effective default for empty_target_if_source_empty if I don't set it?"

    Schema default is `false` — but the field is WITHDRAWN with no runtime effect since 2026-08-29 regardless of its value. A zero-row TRUNCATE_AND_LOAD source blanks the target either way; the in-graph guard cannot run because preserving contents would make the target read itself, which Lakeflow rejects as a cycle.

??? question "Format gotcha · Is empty_target_if_source_empty a boolean or does it take an object like delete_source_after_extract?"

    A plain boolean (`true`/`false`), validated by `check_bool` — unlike some other withdrawn/legacy fields in this framework, it has no object form.

??? question "Performance impact · Does setting this flag add any evaluation cost since it's a no-op?"

    Negligible — the value is validated at onboarding but never read at runtime; it costs nothing beyond the one-time validation check.

??? question "Edge case · If I set empty_target_if_source_empty to false, is my TRUNCATE_AND_LOAD target protected from a zero-row source?"

    No. Per docs/03 §2.2 and the documented known errors, the guard has no runtime effect regardless of the value set. Treat every TRUNCATE_AND_LOAD target as unprotected and add a row-count check in a post-update job task outside the pipeline graph instead.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configempty-target-if-source-empty) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.generate_hash_columns` { #target-configgenerate-hash-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

Adds __framework_hash_key and __framework_hash_value to a CDC target.


Two SHA-256 columns -- one over the ordered primary keys, one over the resolved comparison columns -- that let reconciliation match rows and detect drift without comparing every column on both sides.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "generate_hash_columns": true
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
    -- target_config.generate_hash_columns lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Defaults to true for every CDC-dispatched strategy. Never added for APPEND or TRUNCATE_AND_LOAD, which have no comparison concept.
    - The value hash follows columns_to_check and columns_to_exclude, so changing either re-hashes every row and the next run reports everything as changed -- once, expected.
    - Reconciliation flows using hash_precomputed: true depend on these columns existing on the target.

**FAQs** (4)

??? question "If omitted · What is the default for generate_hash_columns if I don't set it on an SCD2 flow?"

    Defaults to `true` for every CDC-dispatched strategy (SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC) per docs/03 §4 — `__framework_hash_key` and `__framework_hash_value` are added automatically unless explicitly disabled.

??? question "Format gotcha · Is generate_hash_columns a boolean or does it accept a list of columns to hash?"

    A plain boolean, validated by `check_bool`. It only toggles the two fixed columns `__framework_hash_key`/`__framework_hash_value`; which source columns feed the value hash is controlled separately via `columns_to_check`/`columns_to_exclude`.

??? question "Performance impact · Does generating hash columns add meaningful compute per row?"

    A SHA-256 computation per row, over the primary keys and the resolved comparison columns — cheap per row but scales with row count on every run, since both columns are recomputed each update, not incrementally.

??? question "Edge case · What happens if generate_hash_columns is true but no primary_keys are configured?"

    Both hash columns are skipped with a `WARNING` (docs/03 §4) — but this path is unreachable through normal onboarding, since `primary_keys` is required for every CDC-dispatched strategy; it only fires for a hand-edited control-table row bypassing the validator. Also note: changing `columns_to_check`/`columns_to_exclude` re-hashes every row on the next run, reporting everything as changed once — expected, not a bug.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configgenerate-hash-columns) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_config.primary_keys` { #target-configprimary-keys }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

The business key apply_changes uses to match an incoming row to an existing one.


Without it, CDC cannot tell an update from an insert.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json` · **Behaviour changed in** v1.4.0


=== "JSON"

    ```json
    "primary_keys": ["customer_id", "region"]
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
    -- target_config.primary_keys lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Use the natural business key, not a surrogate generated downstream.
    - FULL_SNAPSHOT_CDC requires one too, as of v1.4.0 -- the keyless variant was withdrawn along with the surrogate-key engine that stood in for the missing key.
    - On FULL_SNAPSHOT_CDC this is the diff key: a key present in the target but absent from the incoming snapshot is deleted.
    - A key that never reaches the clean upstream -- renamed by column_normalization, or never present in the source (data_standardization_sql cannot remove a column: it is a withColumn loop that only adds or replaces) -- is only caught once the graph executes.

!!! warning "Known errors and limitations"

    **Rows duplicate instead of updating**  
    *Cause:* The declared key is not actually unique in the source.  
    *Fix:* Add the missing key column, or deduplicate upstream with source_config.remove_dups.

**FAQs** (4)

??? question "If omitted · What happens if primary_keys is missing on an SCD1 flow?"

    Onboarding rejects the flow — `check_list_of_str(..., required=True)` fires for `cdc_load_strategy` in `{SCD1, SCD2, SCD3, FULL_SNAPSHOT_CDC}`. It is not required for APPEND/TRUNCATE_AND_LOAD, which need no key at all.

??? question "Format gotcha · Should primary_keys be a single string or always a list, even for one column?"

    Always a JSON array of strings, even for a single-column key: `["customer_id"]`, validated by `check_list_of_str`. A bare string is rejected.

??? question "Performance impact · Does a composite (multi-column) primary_keys list cost more at merge time?"

    Marginally — `apply_changes` matches on the full composite key, and `__framework_hash_key` hashes the ordered keys joined by `||`. Not separately benchmarked here, but a composite key is standard practice for a genuine composite business key, not a performance anti-pattern.

??? question "Edge case · What happens if the declared primary_keys column is renamed by column_normalization before it reaches the target?"

    This is only caught once the graph executes, not at onboarding — a key that never reaches the clean upstream (renamed by `column_normalization`, or never present in the source, since `data_standardization_sql` can only add/replace columns, never remove them) surfaces as a runtime failure, not a validation error.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configprimary-keys) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.sequence_by_column` { #target-configsequence-by-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-only">Load strategy</span>

Ordering column that decides which version of a key wins.


apply_changes always needs a sequencer. Omit this and the framework sequences by __framework_ingestion_timestamp_utc, which is current_timestamp() evaluated once per batch -- monotonic and correct across runs, but tied within a single one.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json`


=== "JSON"

    ```json
    {
      "target_config": {
        "sequence_by_column": "__framework_ingestion_timestamp_utc"
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
    -- target_config.sequence_by_column lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
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

**FAQs** (4)

??? question "If omitted · What sequencer is used if sequence_by_column is not set?"

    Falls back to `__framework_ingestion_timestamp_utc`, which is `current_timestamp()` evaluated once per batch — monotonic and correct across runs, but tied within a single batch, so two versions of the same key in one batch resolve non-deterministically.

??? question "Format gotcha · Can sequence_by_column reference more than one column, like a tie-breaker pair?"

    No — it is a single string column name, validated by `check_string`. There is no composite/multi-column sequencer support in this field.

??? question "Performance impact · Does declaring a real sequence_by_column add overhead compared to the fallback?"

    Negligible — `apply_changes` always needs some sequencer; supplying a real source column instead of the batch-timestamp fallback costs nothing extra and simply gives a more precise ordering.

??? question "Edge case · Does sequence_by_column have any effect on a FULL_SNAPSHOT_CDC flow?"

    No — it is silently discarded. `apply_changes_from_snapshot` has no `sequence_by` parameter at all, so a value here passes validation and is then never read. Also note: setting `capture_technical_metadata` to `false` removes the fallback column, making this field effectively mandatory on APPEND/TRUNCATE_AND_LOAD-style flows that rely on the default sequencer.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsequence-by-column) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [apply changes](https://docs.databricks.com/delta-live-tables/cdc.html)


---

### `target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret` { #target-configsink-configpost-export-archivepgp-encryptionpassphrase-secret }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion · Transformation</span> <span class="fx-badge fx-ver">v1.7.4+</span>

Encrypt the export with a SHARED PASSPHRASE instead of a recipient's public key.


Egress encryption previously required a recipient keypair. Where two parties already share a passphrase — as UC6 does, using the same secret that decrypts the inbound request — demanding a keypair means managing one purely as ceremony.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | no unknown keys |

**Persisted in** `config.ingestion_flow_spec / transformation_flow_spec.target_config_json`


=== "JSON"

    ```json
    "pgp_encryption": {
      "enabled": true,
      "passphrase_secret": {
        "secret_catalog": "{{catalog}}",
        "secret_schema": "config",
        "secret_key": "pgpkey"
      }
    }
    // AES256. Decrypts with: gpg --decrypt file.csv.gz.gpg
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
    -- target_config.sink_config.post_export_archive.pgp_encryption.passphrase_secret lives inside the target_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_id,
           target_config_json,
           is_active, updated_at
    FROM   <catalog>.config.ingestion_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Mutually exclusive with recipient_public_key_secret — set exactly one. The builder hides whichever you did not choose.
    - Signing is unavailable in this mode: sign_with_private_key_secret needs a sender keypair, which symmetric encryption does not have.
    - Anyone holding the passphrase can both decrypt AND forge an identical file. Where you need provenance rather than only confidentiality, use a recipient key and sign.

!!! warning "Known errors and limitations"

    **Onboarding rejects the flow with 'mutually exclusive — set exactly one'.**  
    *Cause:* Both passphrase_secret and recipient_public_key_secret are present.  
    *Fix:* Delete whichever you are not using. Leaving both is ambiguous rather than additive.

**FAQs** (5)

??? question "If omitted · What happens if pgp_encryption is enabled and I don't set passphrase_secret or recipient_public_key_secret?"

    Onboarding treats the recipient-key path as required by default and rejects the flow for a missing `recipient_public_key_secret`, since neither secret was supplied. There is no default encryption key -- one of the two must be explicit.

??? question "Format gotcha · Can I set both passphrase_secret and recipient_public_key_secret at once for extra safety?"

    No -- they are mutually exclusive; a PGP message is encrypted either to a recipient key or under a shared passphrase, never both. Onboarding rejects the flow verbatim: 'passphrase_secret (symmetric) and recipient_public_key_secret (asymmetric) are mutually exclusive -- set exactly one.'

??? question "Performance impact · Is symmetric passphrase encryption faster than encrypting to a recipient key?"

    Not documented as meaningfully different -- both are a single AES256 encryption pass over the archive per micro-batch/update (`crypto/pgp.py::pgp_encrypt_symmetric`); the difference is cryptographic mode (shared secret vs public key), not measured throughput.

??? question "Edge case · Can I sign the export if I use passphrase_secret instead of a recipient key?"

    No -- signing (`sign_with_private_key_secret`) requires a sender keypair, which symmetric encryption has none of. Setting `sign_with_private_key_secret` or `sign_passphrase_secret` alongside `passphrase_secret` is rejected: 'signing requires a sender keypair and is not available for symmetric (passphrase_secret) encryption.'

??? question "Edge case · Since anyone with the passphrase can decrypt, can they also forge a file that looks like it came from us?"

    Yes -- with symmetric encryption the same secret both encrypts and decrypts, so anyone holding the passphrase can produce an identical, indistinguishable file. Use a recipient key plus a signature (`sign_with_private_key_secret`) wherever the receiver must be able to prove who sent the file. See docs/05 section 4.1.


**See also:** [Pillar 2 · Transformation (load strategies)](../../pillars/transformation.md#load-strategies) · [Attribute dictionary](../../00_master_reference_index.md#4-target-config-cdc-reference) · [Schema tree](tree.md#tree-ingestion-target-configsink-configpost-export-archivepgp-encryptionpassphrase-secret) · [Spec Builder · Load strategy step](../../console/spec_builder.md#ingestion-tab) · [Observability dashboard · Framework & Lineage](../../console/observability_dashboard.md#framework-lineage) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [secrets](https://docs.databricks.com/security/secrets/index.html) · [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

