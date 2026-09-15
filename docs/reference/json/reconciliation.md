<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Reconciliation flows

One entry per `reconciliation_flows[]` element — comparing a baseline against targets.


!!! info "34 attributes · 138 FAQs"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form. Badges: <span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Flow kind</span> <span class="fx-badge fx-ver">vX.Y+ added in</span> <span class="fx-badge fx-only">Spec Builder section</span>.


**Recipes and deep dive:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Schema tree](tree.md) · [Removed & rejected](removed.md)


## Summary

| Attribute | Type | Required | Default | Since |
|---|---|---|---|---|
| [`compare_columns`](#compare-columns) | array<string> | no | — | — |
| [`dataflow_group_id`](#dataflow-group-id) | string | **yes** | — | — |
| [`dq_config.rules`](#dq-configrules) | array<object> | no | — | — |
| [`dq_config.rules[].action`](#dq-configrulesaction) | string (enum) | **yes** | — | — |
| [`dq_config.rules[].expression`](#dq-configrulesexpression) | string (SQL) | **yes** | — | — |
| [`dq_config.rules[].rule_id`](#dq-configrulesrule-id) | string | **yes** | — | — |
| [`error_handling.on_failure`](#error-handlingon-failure) | string (enum) | no | — | — |
| [`execution_mode`](#execution-mode) | string (enum) | no | `"job"` | v1.5.0 |
| [`logging_config.mismatch_log_capture`](#logging-configmismatch-log-capture) | boolean | no | `false` | — |
| [`logging_config.run_log_capture`](#logging-configrun-log-capture) | boolean | no | `false` | — |
| [`match_keys`](#match-keys) | array<string> | **yes** | — | — |
| [`publish_schema`](#publish-schema) | string | no | — | v1.5.0 |
| [`reconciliation_id`](#reconciliation-id) | string | **yes** | — | — |
| [`source_config.data_standardization_sql`](#source-configdata-standardization-sql) | array<string> | no | — | — |
| [`source_config.filter_condition`](#source-configfilter-condition) | string (SQL) | no | — | — |
| [`source_config.hash_precomputed`](#source-confighash-precomputed) | boolean | no | — | — |
| [`source_config.read_mode`](#source-configread-mode) | string (enum) | no | — | — |
| [`source_config.table`](#source-configtable) | string | **yes** | — | — |
| [`source_config.task_run_id_column`](#source-configtask-run-id-column) | string | no | — | — |
| [`source_config.type`](#source-configtype) | string (enum) | no | — | — |
| [`target_configs`](#target-configs) | array<object> | no | — | — |
| [`target_configs[].append_target_table`](#target-configsappend-target-table) | string | **yes** | — | — |
| [`target_configs[].comparison_direction`](#target-configscomparison-direction) | string (enum) | no | — | — |
| [`target_configs[].data_standardization_sql`](#target-configsdata-standardization-sql) | array<string> | no | — | — |
| [`target_configs[].filter_condition`](#target-configsfilter-condition) | string (SQL) | no | — | — |
| [`target_configs[].hash_precomputed`](#target-configshash-precomputed) | boolean | no | — | — |
| [`target_configs[].read_mode`](#target-configsread-mode) | string (enum) | no | — | — |
| [`target_configs[].target_catalog`](#target-configstarget-catalog) | string | **yes** | — | — |
| [`target_configs[].target_id`](#target-configstarget-id) | string | **yes** | — | — |
| [`target_configs[].target_schema`](#target-configstarget-schema) | string | **yes** | — | — |
| [`target_configs[].target_table`](#target-configstarget-table) | string | **yes** | — | — |
| [`target_configs[].task_run_id_column`](#target-configstask-run-id-column) | string | no | — | — |
| [`transform_sql`](#transform-sql) | string (SQL) | no | — | — |
| [`two_tier_verification`](#two-tier-verification) | boolean | no | `true` | — |

## Attributes

### `compare_columns` { #compare-columns }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Matching &amp; healing</span>

Columns compared for drift after key matching.


Columns compared for drift after key matching. Defaults to all columns.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.compare_columns_json`


=== "JSON"

    ```json
    {
      "compare_columns": [
        "amount",
        "status"
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
    -- compare_columns lives inside the compare_columns_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           compare_columns_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What happens if compare_columns is left empty or unset?"

    It defaults to key-presence-only matching: every key present on both sides counts as matched regardless of other column values. `__framework_hash_value` is NULL throughout, and `matcher.py` forces `hash_equal = True` when the list is empty.

??? question "Format gotcha · Is compare_columns entered as a JSON array or a comma-separated string?"

    In the onboarding UI it is entered as a comma-separated list, but it is written to the spec as a JSON array of strings, e.g. `["amount", "status"]`.

??? question "Performance impact · Does listing more compare_columns increase join cost?"

    No -- they are folded into a single `__framework_hash_value` SHA-256 digest (columns alphabetically sorted), so the join itself stays a single-column comparison regardless of how many columns are listed; the cost is one extra hash computation pass, not a wider join.

??? question "Edge case · Does compare_columns interact with hash_precomputed?"

    Yes -- when `hash_precomputed` is `true`, `compare_columns` is not hashed at all here; the side's stored `__framework_hash_value` is trusted verbatim, so it must already have been computed over the same resolved, alphabetically-sorted column set this flow declares, or drift will be silently misreported. See docs/07 section 9.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-compare-columns) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `dataflow_group_id` { #dataflow-group-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Reconciliation identity</span>

The dataflow group whose Lakeflow pipeline this flow is registered into.


The dataflow group whose Lakeflow pipeline this flow is registered into. Required when execution_mode is pipeline or pipeline_audit_only -- a group-less reconciliation flow has no pipeline update to live in. Usually this spec's own dataflow_group_id; naming another group registers the flow inside that group's pipeline instead. Stays optional in job mode, where the standalone engine handles the group-less case.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.dataflow_group_id` · **Behaviour changed in** v1.5.0


=== "JSON"

    ```json
    {
      "dataflow_group_id": "dfg_example_group"
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
    -- dataflow_group_id is persisted as its own column
    SELECT reconciliation_id,
           dataflow_group_id,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · What happens if I leave dataflow_group_id unset on a reconciliation flow?"

    It defaults to the spec's own top-level `dataflow_group_id` -- the common case. It stays optional under `execution_mode: job`, where the standalone engine handles the group-less case, but onboarding rejects it as required once `execution_mode` is `pipeline` or `pipeline_audit_only` (V-CYC-6): 'is required when execution_mode is ... a group-less reconciliation flow has no Lakeflow pipeline to be registered into'.

??? question "Format gotcha · Is dataflow_group_id the same value as the spec's top-level dataflow_group_id?"

    Usually yes -- omitting the flow-level key inherits the spec's own top-level `dataflow_group_id`. Naming a different value registers this flow inside that OTHER group's Lakeflow pipeline update instead, which is the supported way to reconcile against or heal into tables another group owns. It is a plain string, not a three-part name.

??? question "Performance impact · Does pointing dataflow_group_id at another group cost anything extra?"

    It changes where compute runs, not how much. The flow's L3-L5 nodes get registered in that other group's pipeline update rather than this spec's own, so that pipeline pays the extra scan/join cost described in docs/07 section 11.6 instead of this one.

??? question "Edge case · What happens if I switch a job-mode flow to pipeline mode but a job resource still runs a reconciliation task for the same reconciliation_id?"

    The reconciliation runs twice per cycle -- once inside the pipeline update (via dataflow_group_id) and once as the standalone job task -- and each pass appends its own corrections into `append_target_table`. Remove the job task, or keep `execution_mode: job`. See docs/13 R8.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-dataflow-group-id) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `dq_config.rules` { #dq-configrules }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Data-quality expectations evaluated on every row.


Each rule becomes a pipeline expectation; the action decides what happens to a failing row.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.dq_config_json`


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
    SELECT reconciliation_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
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


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrules) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].action` { #dq-configrulesaction }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

warn logs and keeps the row, drop silently removes it, fail aborts the pipeline, quarantine routes it to the quarantine table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `warn`, `drop`, `fail`, `quarantine` | — |

**Persisted in** `config.reconciliation_flow_spec.dq_config_json`


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
    SELECT reconciliation_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
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


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesaction) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].expression` { #dq-configrulesexpression }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Boolean Spark SQL expression evaluated per row.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.dq_config_json`


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
    SELECT reconciliation_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
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


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesexpression) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `dq_config.rules[].rule_id` { #dq-configrulesrule-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Transformation</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Data quality</span>

Unique rule identifier.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.dq_config_json`


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
    SELECT reconciliation_id,
           dq_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
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


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#5-data-quality-config) · [Schema tree](tree.md#tree-ingestion-dq-configrulesrule-id) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [dlt expectations](https://docs.databricks.com/delta-live-tables/expectations.html)


---

### `error_handling.on_failure` { #error-handlingon-failure }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Reconciliation identity</span>

fail raises an error, warn logs and continues.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `fail`, `warn` | — |

**Persisted in** `config.reconciliation_flow_spec.error_handling_json` · **Behaviour changed in** v1.5.0


=== "JSON"

    ```json
    {
      "error_handling": {
        "on_failure": "fail"
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
    -- error_handling.on_failure lives inside the error_handling_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           error_handling_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: fail, warn.

**FAQs** (4)

??? question "If omitted · What happens if error_handling.on_failure is not set?"

    It defaults to `fail`, per the JSON schema description ('Default: 'fail'. Evaluated per target.'). A target's reconciliation failure propagates and stops the run.

??? question "Format gotcha · What are the only legal values for error_handling.on_failure?"

    `fail` or `warn` -- `ALLOWED_RECONCILIATION_FAILURE_MODES = {"fail", "warn"}`; any other value fails the allowed-values check.

??? question "Performance impact · Does setting on_failure to warn cost more at runtime?"

    Negligible -- `warn` just logs the failure and writes a FAILED `reconciliation_run_log` row for that target instead of raising, then continues to the next target. It avoids re-running already-succeeded targets on retry, which can save cost versus `fail` aborting the whole run.

??? question "Edge case · Does error_handling.on_failure behave the same in pipeline mode as in job mode?"

    The try/except semantics are unchanged, but the blast radius is not: in job mode `fail` fails only that job task, while in pipeline mode re-raising fails that flow and therefore the whole pipeline update -- sibling reconciliation flows in the same group may already have run by then. See docs/07 section 11.4.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-error-handlingon-failure) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `execution_mode` { #execution-mode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-ver">v1.5.0+</span> <span class="fx-badge fx-only">Reconciliation identity</span>

Chooses where a reconciliation flow runs: as its own job task (job), or as a third flow type inside its dataflow group's Lakeflow pipeline update (pipeline, pipeline_audit_only).


In-pipeline reconciliation is the whole point of v1.5.0: ingestion, transformation and reconciliation in ONE pipeline update, so the comparison reads the rows this update just wrote instead of a stale snapshot from the previous cycle. It stays opt-in, and job stays the default, because job resources already run reconciliation tasks against onboarded rows -- flipping the default would run those flows twice per cycle.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | `"job"` | `job`, `pipeline`, `pipeline_audit_only` | — |

**Persisted in** `config.reconciliation_flow_spec.execution_mode` · **Behaviour changed in** v1.7.11


=== "JSON"

    ```json
    "execution_mode": "pipeline"
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
    -- execution_mode is persisted as its own column
    SELECT reconciliation_id,
           execution_mode,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - pipeline_audit_only publishes the classified/metrics/mismatch datasets and runs dq_config expectations in-pipeline, but leaves the append-back healing lane in job mode. It is the right setting for a flow whose source is a static table.
    - Both pipeline modes require a flow-level dataflow_group_id and read_mode batch on every side. Only 'pipeline' additionally requires an append-only source producer, because it streams the source to drive the heal lane; 'pipeline_audit_only' reads the source as a batch and is the correct mode when the source is written by SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC or is a fully-refreshed materialized view.
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

**FAQs** (4)

??? question "If omitted · What happens if execution_mode is not set on a reconciliation flow?"

    It defaults to `job` -- the flow runs as its own `05_reconciliation_engine.py` job task, exactly as before v1.5.0. This default is deliberate and permanent: several DABs resources already run recon tasks against onboarded rows, so defaulting to `pipeline` would run those flows twice per cycle.

??? question "Format gotcha · What are the only legal values for execution_mode?"

    `job`, `pipeline`, and `pipeline_audit_only` -- any other value fails the allowed-values check in `_validate_reconciliation_flows`. Note the invalid value still falls back to the more restrictive `job` reading for every other mode-conditional check that follows.

??? question "Performance impact · Is pipeline mode more expensive than job mode?"

    Yes. Every L3/L4 node materializes a real physical copy in UC storage plus an extra DAG step (and a checkpoint for streaming ones), paid on every source since `materialize` defaults to `always` as of v1.7.3. Budget for this when sizing a group with many single-consumer reconciliation sources. See docs/07 section 11.6.

??? question "Edge case · Can I use execution_mode pipeline if my source is written by SCD2 or TRUNCATE_AND_LOAD?"

    No -- `pipeline` streams the source to drive the L5 heal pulse, which requires an append-only producer (V-CYC-7). A source produced by `SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC` or `TRUNCATE_AND_LOAD` into a `materialized_view` is rejected; use `pipeline_audit_only` instead, which reads the source as a batch and registers no heal lane. See docs/07 section 11.7.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-execution-mode) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `logging_config.mismatch_log_capture` { #logging-configmismatch-log-capture }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Reconciliation identity</span>

Per-flow gate on mismatch-log writes.


Per-flow gate on mismatch-log writes. DEFAULTS TO FALSE since v1.7.3 (it defaulted to true through v1.7.2): leaving this unset means no reconciliation_mismatch_log rows and, in pipeline mode, no recon__*__mismatch dataset. Set it true to opt in. Overridable at runtime by the recon_mismatch_log job parameter.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `false` | — | — |

**Persisted in** `config.reconciliation_flow_spec.logging_config_json` · **Behaviour changed in** v1.7.3


=== "JSON"

    ```json
    {
      "logging_config": {
        "mismatch_log_capture": true
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
    -- logging_config.mismatch_log_capture lives inside the logging_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           logging_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - v1.7.3: defaults false -- auditing is opt-in
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens if logging_config.mismatch_log_capture is never set?"

    It defaults to `false` since v1.7.3 (was `true` through v1.7.2). Omitting it means no `reconciliation_mismatch_log` rows, and in pipeline mode no `recon__*__mismatch` dataset is registered at all. Set it `true` to opt in.

??? question "Format gotcha · Does mismatch_log_capture gate the same rows as run_log_capture?"

    No -- they are independent. `run_log_capture` gates `reconciliation_run_log`/`reconciliation_result` and the `__metrics` dataset; `mismatch_log_capture` gates only `reconciliation_mismatch_log` rows and the `__mismatch` dataset. You can set one true and the other false.

??? question "Performance impact · Is mismatch_log_capture expensive to turn on for a high-drift table?"

    Its cost scales with mismatch volume, not table size: it writes one `reconciliation_mismatch_log` row per non-MATCHED record with a full `differing_columns_json`, so a flow with heavy VALUE_DRIFT can generate substantial log volume per run -- unlike `run_log_capture`, which is a fixed one-row-per-target cost.

??? question "Edge case · If mismatch_log_capture is true but publish_schema is unset in pipeline mode, what happens?"

    Onboarding rejects it: 'run_log_capture/mismatch_log_capture is true but the flow has no publish_schema.' Since v1.7.07 nothing is exported without a `publish_schema`, because the control-table rows are exported from the PUBLISHED `__mismatch` dataset. Set `publish_schema`, or set both capture flags false.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-logging-configmismatch-log-capture) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `logging_config.run_log_capture` { #logging-configrun-log-capture }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Reconciliation identity</span>

Per-flow gate on run-log writes.


Per-flow gate on run-log writes. DEFAULTS TO FALSE since v1.7.3 (it defaulted to true through v1.7.2): reconciliation is silent by default, so leaving this unset means no reconciliation_run_log or reconciliation_result rows and, in pipeline mode, no recon__*__metrics dataset at all. Set it true to opt in -- and you MUST set it true when the flow declares dq_config.rules, whose expectations attach to that dataset. Overridable at runtime by the recon_run_log_capture job parameter.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `false` | — | — |

**Persisted in** `config.reconciliation_flow_spec.logging_config_json` · **Behaviour changed in** v1.7.3, v1.7.07


=== "JSON"

    ```json
    {
      "logging_config": {
        "run_log_capture": true
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
    -- logging_config.run_log_capture lives inside the logging_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           logging_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - v1.7.3: defaults false -- auditing is opt-in
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens if logging_config.run_log_capture is never set?"

    It defaults to `false` since v1.7.3 (it defaulted to `true` through v1.7.2) -- reconciliation is silent by default. Omitting it means no `reconciliation_run_log`/`reconciliation_result` rows, and in pipeline mode the `recon__*__metrics` dataset is not even registered. Set it `true` explicitly to opt in.

??? question "Format gotcha · Is run_log_capture nested inside logging_config or a top-level flow key?"

    Nested: `logging_config.run_log_capture`, a plain boolean. It is not a per-target field -- one value governs the whole flow's run-log writes across all its targets.

??? question "Performance impact · Does turning run_log_capture on increase reconciliation cost?"

    It adds one published MV (`recon__*__metrics`, exactly one row) and its control-table export row per run in pipeline mode; the cost is negligible since the metrics dataset is a single-row aggregate, not a per-record scan. In job mode it is just an extra log write.

??? question "Edge case · What happens if I declare dq_config.rules but run_log_capture resolves false?"

    Onboarding rejects it, since the rules attach to the `__metrics` dataset which would not exist without capture. The error names how the flag got its value (written false, defaulted false, or overridden false) and tells you to set `run_log_capture: true` explicitly, together with `publish_schema`. See docs/07 section 6.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-logging-configrun-log-capture) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `match_keys` { #match-keys }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Matching &amp; healing</span>

Columns identifying the same logical record across datasets.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | min items `1` |

**Persisted in** `config.reconciliation_flow_spec.match_keys_json`


=== "JSON"

    ```json
    {
      "match_keys": [
        "example_id"
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
    -- match_keys lives inside the match_keys_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           match_keys_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What happens if match_keys is not provided?"

    Onboarding rejects the flow -- `check_list_of_str(flow.get("match_keys"), ..., required=True)` fails, since match_keys is how the engine identifies the same logical record on both sides; there is no default.

??? question "Format gotcha · Does the order of columns in match_keys matter?"

    Yes -- `__framework_hash_key` preserves the caller's declared key order, so `(a, b)` and `(b, a)` are different keys. Keep the same declared order on both sides of a hash-precomputed comparison, or the hashes will not line up. See docs/07 section 9.

??? question "Performance impact · Does adding more columns to match_keys make the join more expensive?"

    No -- match_keys are hashed into a single `__framework_hash_key` column, so the join is always a single-column equi-join regardless of how many key columns you list; a dozen match_keys costs the same at join time as one.

??? question "Edge case · What happens if match_keys uses a different key than the table's own primary_keys when hash_precomputed is true?"

    The stored hashes describe a different question and the comparison is silently wrong -- use `hash_precomputed: false` in that case so the engine recomputes hashes from this flow's own `match_keys` instead of trusting mismatched stored ones. See docs/07 section 9's best-practice table.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-match-keys) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `publish_schema` { #publish-schema }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-ver">v1.5.0+</span> <span class="fx-badge fx-only">Reconciliation identity</span>

The schema, inside the hosting pipeline's own catalog, where a pipeline-mode reconciliation flow publishes its recon__<reconciliation_id>__<target_id>__metrics and __mismatch datasets (and a healing flow's prepared _src/_tgt).


Since v1.7.07 publish_schema is the only thing that publishes. Leave it unset and the flow publishes nothing: its audit datasets are pipeline-scoped temporary tables, a dq_config gate still fails the update, but no reconciliation_run_log / reconciliation_mismatch_log row can be exported. Before v1.7.07 an unset value fell back to the pipeline's own schema and dropped recon__*__metrics materialized views beside the business tables.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.publish_schema` · **Behaviour changed in** v1.7.07


=== "JSON"

    ```json
    "publish_schema": "recon_results"
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
    -- publish_schema is persisted as its own column
    SELECT reconciliation_id,
           publish_schema,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
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

**FAQs** (5)

??? question "If omitted · What happens if I leave publish_schema unset on a pipeline-mode reconciliation flow?"

    Since v1.7.07, absent means publish nothing: the `__metrics`/`__mismatch` datasets are registered as pipeline-scoped temporary tables. A `dq_config` gate still fires, but no `reconciliation_run_log`/`reconciliation_mismatch_log` row can ever be exported. Leave it unset only for a pure gate (dq_config only, both capture flags false).

??? question "Format gotcha · Does publish_schema take a catalog.schema value or just the schema name?"

    Just the schema name, as a plain string -- for example `recon_results`. The catalog is always the hosting pipeline's own; only the schema is configurable. The schema must already exist and be writable by the pipeline's run-as identity, the framework does not create it.

??? question "Performance impact · Does setting publish_schema add runtime cost beyond the metrics/mismatch materialization?"

    Not beyond what publishing itself costs: the L3/L4 nodes it makes durable (metrics, mismatch, and a healing flow's prepared `_src`/`_tgt`) are real UC-visible tables instead of pipeline-scoped temporaries, so they persist storage and appear in lineage, but no extra join or scan is introduced by the key itself.

??? question "Edge case · Why does onboarding reject publish_schema when execution_mode is job?"

    Because a job-mode flow never produces the `recon__<reconciliation_id>__<target_id>__metrics/__mismatch` datasets publish_schema is meant to place -- rejected on presence via `reject_mode_incompatible_keys`. Set `execution_mode` to `pipeline` or `pipeline_audit_only`, or delete the key.

??? question "Edge case · Is publish_schema required for a pipeline-mode healing flow (one with append_target_table)?"

    Yes -- rejected as missing: 'execution_mode 'pipeline' with a healing target (append_target_table) requires publish_schema. The L5 heal handler reads the flow's prepared source and healing target back through the metastore (spark.read.table)'. Set publish_schema, or use `pipeline_audit_only` and heal from the job task.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-publish-schema) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `reconciliation_id` { #reconciliation-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Reconciliation identity</span>

Unique ID for this reconciliation flow.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.reconciliation_id`


=== "JSON"

    ```json
    {
      "reconciliation_id": "recon_template_example"
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
    -- reconciliation_id is persisted as its own column
    SELECT reconciliation_id,
           reconciliation_id,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if I forget to set reconciliation_id on a flow?"

    Onboarding rejects the flow. `spec_validator.py::_validate_reconciliation_flows` calls `check_string(..., required=True)` on it, so a missing or empty `reconciliation_id` fails validation before anything else in the flow is checked.

??? question "Format gotcha · Does reconciliation_id need to follow any naming pattern like a three-part table name?"

    No. It is a free-form string identifier, not a table reference. It is used verbatim to build dataset names such as `recon__<reconciliation_id>__<target_id>__metrics` and as the restartability key for `reconciliation_run_log`, so keep it stable across onboarding re-runs.

??? question "Performance impact · Does changing reconciliation_id have any performance impact?"

    None directly. But since it is the restartability key `reconciliation_run_log.source_batch_fingerprint` is checked against, renaming it makes the engine treat the flow as new and loses prior-run fingerprint history, so a batch target cannot short-circuit to `SKIPPED_ALREADY_PROCESSED` on its next run. See docs/07 section 8.

??? question "Edge case · Can two reconciliation flows in the same spec share a reconciliation_id?"

    The validator does not check cross-flow uniqueness of `reconciliation_id` explicitly, but it is the restartability key for `reconciliation_run_log` and the naming key for published `recon__<reconciliation_id>__<target_id>__*` datasets, so two flows sharing one would collide on those dataset names. Keep it unique per flow.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-reconciliation-id) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.data_standardization_sql` { #source-configdata-standardization-sql }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Ingestion</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source · nested data &amp; standardization</span>

Per-column expressions, each ending AS <column>.


Per-column expressions, each ending AS <column>. Restricted grammar: no SELECT/FROM/JOIN/UNION/WHERE/DML/DDL and no semicolons.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.source_config_json` · **Behaviour changed in** v1.7.07


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
    SELECT reconciliation_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
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


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configdata-standardization-sql) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `source_config.filter_condition` { #source-configfilter-condition }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source dataset</span>

Boolean SQL applied after read.


Boolean SQL applied after read. Supports ${param} substitution.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "filter_condition": "load_date = '${run_date}'"
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
    -- source_config.filter_condition lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.

**FAQs** (4)

??? question "If omitted · What happens if source_config.filter_condition is left unset?"

    No filter is applied after the read -- the whole source table (after any task_run_id_column narrowing) flows into matching, unmodified. There is no default filter.

??? question "Format gotcha · How do I substitute a date literal into source_config.filter_condition safely?"

    Use a bare `${param}` placeholder without surrounding quotes, e.g. `load_date = ${run_date}` -- `substitute_dynamic_parameters` already supplies the quotes for a string parameter, so writing `load_date = '${run_date}'` becomes `load_date = ''2024-01-01''`, a ParseException. It also supports `{{catalog}}`/`{{env}}` template variables, resolved at onboarding time.

??? question "Performance impact · Does source_config.filter_condition reduce the cost of the reconciliation source read?"

    Yes -- it is applied right after the read (and after task_run_id_column narrowing), so a selective filter shrinks the row count that reaches the Phase 1 fingerprint and the Phase 2 hash-key join for the source side.

??? question "Edge case · How do I re-run reconciliation over an ad-hoc historical window using source_config.filter_condition?"

    Add `${start_date}`/`${end_date}` placeholders in `filter_condition`, change the root `pipeline_parameters`, then re-run onboarding with `action_type=UPDATE` (CREATE/UPDATE/VALIDATE_ONLY are the accepted values) and run the job -- there is no job widget that overrides these dates directly.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configfilter-condition) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `source_config.hash_precomputed` { #source-confighash-precomputed }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source dataset</span>

Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing.


Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing. Only valid for type table.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.source_config_json` · **Behaviour changed in** v1.4.0


=== "JSON"

    ```json
    {
      "source_config": {
        "hash_precomputed": true
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
    -- source_config.hash_precomputed lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What happens if source_config.hash_precomputed is left unset?"

    It behaves as `false`: the engine computes `__framework_hash_key` (SHA-256 over `match_keys` in declared order) and `__framework_hash_value` (SHA-256 over `compare_columns`, alphabetically sorted) now, from raw columns, overwriting anything already present under those names.

??? question "Format gotcha · Is hash_precomputed an instruction to precompute hashes?"

    No -- it is an assertion, not an instruction. `true` declares the hashes were ALREADY computed upstream when the table was materialized; it never causes anything to be precomputed. `false` (default) computes both columns now. See docs/07 section 9.

??? question "Performance impact · Does hash_precomputed true make reconciliation cheaper?"

    Yes, when the precondition holds -- it skips a full hash pass over this side's rows, and clustering the table on `__framework_hash_key` makes the join cheap at scale. It only helps for a framework-managed table already materialized by a CDC-dispatched flow with `generate_hash_columns` on.

??? question "Edge case · What happens if hash_precomputed is true but the table's columns don't actually match this flow's match_keys/compare_columns?"

    Nothing errors at match time -- the hashes simply never match, and every row reports as VALUE_DRIFT or missing, silently wrong. The precondition is that the upstream flow's `primary_keys` equal this flow's `match_keys` and its comparison columns equal `compare_columns`; onboarding does reject `hash_precomputed: true` when `type` is not `table`. See docs/07 section 9.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-confighash-precomputed) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.read_mode` { #source-configread-mode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source dataset</span>

At most one side of a given target's comparison may be streaming.


At most one side of a given target's comparison may be streaming. Rejected on presence with value 'streaming' when execution_mode is 'pipeline' or 'pipeline_audit_only': the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner and left_outer, which cannot express MISSING_IN_SOURCE. Use read_mode 'batch' (the default), or set execution_mode to 'job' to keep the standalone streaming engine.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `batch`, `streaming` | — |

**Persisted in** `config.reconciliation_flow_spec.source_config_json` · **Behaviour changed in** v1.5.0


=== "JSON"

    ```json
    {
      "source_config": {
        "read_mode": "batch"
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
    -- source_config.read_mode lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: batch, streaming.

**FAQs** (4)

??? question "If omitted · What is the default for source_config.read_mode if omitted?"

    `batch` -- `ALLOWED_READ_MODES = {"batch", "streaming"}` and the sample in docs/07 shows `"batch"` as the baseline; the validator only runs its allowed-values/mode check when the key is present, but the runtime treats an absent value as batch.

??? question "Format gotcha · Are there only two legal values for source_config.read_mode?"

    Yes -- `batch` or `streaming`; any other string fails `check_string`'s allowed-values check.

??? question "Performance impact · Is streaming read_mode more expensive than batch for the reconciliation source?"

    Streaming runs under `trigger(availableNow=True)` in job mode -- it processes everything currently available across as many micro-batches as needed then stops, matching a bounded job run; it needs a checkpoint (`checkpoint_root` widget) so it costs more state management than a plain batch read, which has none.

??? question "Edge case · Can source_config.read_mode be streaming when execution_mode is pipeline?"

    No -- rejected on presence: 'streaming' is not supported when execution_mode is 'pipeline'/'pipeline_audit_only'. The in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner/left_outer, which cannot express MISSING_IN_SOURCE. Use `batch`, or set `execution_mode` to `job`.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configread-mode) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.table` { #source-configtable }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source dataset</span>

Three-part fully-qualified table name.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "table": "{{catalog}}.bronze.table"
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
    -- source_config.table lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if source_config.table is missing?"

    Onboarding rejects the flow -- `check_string(config.get("table"), ..., required=True)` fails when `type` is `table` (the default), since `table` is the only supported identity for a reconciliation source.

??? question "Format gotcha · Does source_config.table need to be a three-part name, and does it support {{catalog}} templating?"

    Yes, a fully-qualified `catalog.schema.table` string, and it supports `{{catalog}}`/`{{env}}` template variables resolved at onboarding time, e.g. `{{catalog}}.bronze.table`. It must resolve to a real Delta table -- runtime double-checks via `DESCRIBE TABLE EXTENDED`.

??? question "Performance impact · Does the size of source_config.table's table matter for reconciliation cost?"

    It drives the cost directly: reconciliation matches via a single-column hash-key join, so a wider `compare_columns` list costs the same as a narrow one at join time, but table row count still drives the join and the Phase 1 fingerprint scan. Setting `hash_precomputed: true` where valid avoids a full re-hash pass over this table's rows.

??? question "Edge case · Can source_config.table point at a table produced by this same pipeline in pipeline mode?"

    It is required to, in pipeline mode -- V-CYC-1 rejects a `pipeline`/`pipeline_audit_only` flow whose `source_config.table` does not resolve to an in-spec `ingestion_flows[]`/`transformation_flows[]` target of the same dataflow group, because the point of pipeline mode is comparing this update's own freshly-written rows, not an external, always-stale read.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configtable) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.task_run_id_column` { #source-configtask-run-id-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source dataset</span>

When the task_run_id job parameter is set, narrows this side's read to that run's rows.


When the task_run_id job parameter is set, narrows this side's read to that run's rows. Reconciliation is triggered-only as of v1.4.0. Rejected on presence when execution_mode is 'pipeline' or 'pipeline_audit_only': no stable per-update key exists inside a Lakeflow update, so the narrowing would match every row that pipeline ever wrote -- a silent no-op. Use filter_condition, or set execution_mode to 'job'.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.source_config_json` · **Behaviour changed in** v1.4.0, v1.5.0


=== "JSON"

    ```json
    {
      "source_config": {
        "task_run_id_column": "__framework_pipeline_run_id"
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
    -- source_config.task_run_id_column lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What happens if source_config.task_run_id_column is not set?"

    The `task_run_id` job parameter stays correlation-only (written into log rows for traceability) but filters nothing on this side -- the pre-v1.3.0 behaviour. This is fully backward compatible; no error.

??? question "Format gotcha · What column name should I use for task_run_id_column?"

    The recommended value is `__framework_pipeline_run_id`, since `dq/quarantine.py::add_quarantine_columns` stamps it on every row of every framework-materialized table and it survives onto downstream tables. It is not hardcoded -- a third-party table's equivalent column can be named anything.

??? question "Performance impact · Does setting task_run_id_column slow down the source read?"

    It narrows the read via a simple `df.filter(F.col(task_run_id_column) == task_run_id)` applied immediately after read and before `filter_condition` -- this reduces, not increases, the rows scanned downstream, and the filter itself is negligible cost.

??? question "Edge case · Can source_config.task_run_id_column be used when execution_mode is pipeline?"

    No -- rejected on presence, because a Lakeflow update exposes no stable per-update key (`pipelines.id` is the pipeline id, constant across every update), so the narrowing would match every row that pipeline ever wrote -- a silent no-op. Use `filter_condition` instead, or keep `execution_mode: job`.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configtask-run-id-column) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `source_config.type` { #source-configtype }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Source dataset</span>

Reconciliation is scoped to Delta tables only.


Reconciliation is scoped to Delta tables only. Any other value is rejected outright — path-based file and sink sources are no longer valid here.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `table` | — |

**Persisted in** `config.reconciliation_flow_spec.source_config_json`


=== "JSON"

    ```json
    {
      "source_config": {
        "type": "table"
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
    -- source_config.type lives inside the source_config_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           source_config_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: table.

**FAQs** (4)

??? question "If omitted · What happens if source_config.type is left unset?"

    It defaults to `table` (`config.get("type", "table")` in the validator), which is the only legal value anyway, so omitting it is equivalent to writing it explicitly.

??? question "Format gotcha · Can source_config.type be set to file or sink like ingestion sources?"

    No. As of v1.3.0 reconciliation is scoped to Delta tables only; any other value is rejected outright: 'reconciliation is supported for Delta tables only -- type must be 'table', got '<value>'. Read the file/sink output into a Delta table first, then reconcile against that table.' See docs/07 section 7.

??? question "Performance impact · Does source_config.type table have any cost implication?"

    Negligible by itself -- it is a scope check, not a read strategy. But scoping to Delta tables is what makes the hash-key join and file-skipping affordable in the first place: a raw file/sink path could never carry precomputed hash columns or a transaction boundary.

??? question "Edge case · Does the runtime re-check source_config.type even after onboarding accepts it?"

    Yes -- `dataset_reader.py::read_reconciliation_dataset` has a defense-in-depth check for a hand-edited control-table row, and it also resolves the table's provider via `DESCRIBE TABLE EXTENDED` and raises if it is a non-Delta provider (e.g. Parquet/CSV external tables or foreign catalogs), even though `type: table` alone doesn't prove Delta. See docs/07 section 7.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#3-source-config-reference) · [Schema tree](tree.md#tree-ingestion-source-configtype) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs` { #target-configs }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Sets target_configs[].


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<object>` | — | — | min items `1` |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json` · **Behaviour changed in** v1.7.4


=== "JSON"

    ```json
    {
      "target_configs": [
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
    -- target_configs lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Each entry becomes one object in a JSON array. A wholly blank entry is dropped on save.

**FAQs** (4)

??? question "If omitted · What happens if target_configs is left empty or missing?"

    Onboarding rejects the flow: 'is required and must be a non-empty list of target objects' -- a reconciliation flow needs at least one target to compare the source against.

??? question "Format gotcha · Is target_configs a single object or an array?"

    An array of objects -- one flow can compare its source against multiple, independently-configured targets. Each entry becomes one object in a JSON array; a wholly blank entry is dropped on save.

??? question "Performance impact · Does adding more entries to target_configs multiply the reconciliation cost?"

    Yes, roughly linearly -- each `target_configs[]` entry gets its own L3 prepared-target node and L4 classify/metrics/mismatch nodes, so N targets means N independent full outer joins against the shared source, each materializing its own set of datasets.

??? question "Edge case · Must every target_configs[] entry use the same read_mode as the source?"

    Not necessarily the same, but constrained: at most one side of a given target's comparison may be streaming (source_config or that target_configs[] entry, never both), because a stream-stream join only supports inner/left_outer semantics and cannot express the framework's full four-way classification.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configs) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].append_target_table` { #target-configsappend-target-table }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Where self-healed records are appended.


Where self-healed records are appended. Required when the direction includes source-to-target.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "append_target_table": "{{catalog}}.bronze_example.example_raw_cdc"
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
    -- target_configs[].append_target_table lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.
    - Only applies to some configurations; the form hides it when it is not relevant.

**FAQs** (4)

??? question "If omitted · When is append_target_table actually required?"

    Only when `comparison_direction` is `source_to_target` or `both` (the default) -- `check_string(..., required=True)` fires in that branch. With `comparison_direction: target_to_source` alone, it can be omitted since nothing is ever appended in that direction.

??? question "Format gotcha · Does append_target_table need to already exist before onboarding?"

    Yes -- it must pre-exist. `spark.catalog.tableExists` is unusable in the graph-execution context and creating it on demand would race sibling flows, so a table created on the fly would also miss any `CLUSTER BY (__framework_hash_key)` clustering. See docs/07 section 11.6.

??? question "Performance impact · Is the append into append_target_table an update-in-place or insert-only?"

    Insert-only -- a drifted record is re-appended as a correction, not updated in place, consistent with the append-only CDC/Zerobus model these targets are typically built on. A restartability fingerprint check prevents duplicate corrections on an unchanged batch source.

??? question "Edge case · What happens if append_target_table equals this flow's own source_config.table or another target's table?"

    It is rejected as an append-loop hazard (V-CYC-2/3/5): must not equal this group's own ingestion/transformation target, this flow's own source, or another target_configs[] entry's table -- in pipeline modes this is a hard onboarding error, in job mode it is a warning since there is no live graph to race. See docs/07 section 11.8.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configsappend-target-table) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].comparison_direction` { #target-configscomparison-direction }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

source_to_target self-heals missing records, target_to_source audits orphaned records, both does each.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `source_to_target`, `target_to_source`, `both` | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "comparison_direction": "both"
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
    -- target_configs[].comparison_direction lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: both, source_to_target, target_to_source.

**FAQs** (4)

??? question "If omitted · What is the default for comparison_direction if I don't set it?"

    `both` -- the validator resolves `target_config.get("comparison_direction", "both")` when deciding whether `append_target_table` is required, and the JSON schema documents the same default.

??? question "Format gotcha · What are the legal values for comparison_direction?"

    `source_to_target`, `target_to_source`, or `both` -- `ALLOWED_COMPARISON_DIRECTIONS`; any other string fails the allowed-values check.

??? question "Performance impact · Does comparison_direction change how much work the join does?"

    No -- `matcher.py::match_reconciliation_target` always computes all four classification categories in one full outer join regardless of `comparison_direction`; computing all four is effectively free once the join has run. What it gates is action (append vs. audit-only), not classification cost.

??? question "Edge case · Does target_to_source ever write corrections back anywhere?"

    No -- `target_to_source`/`both` only mismatch-logs MISSING_IN_SOURCE records for audit; this direction never appends and never mutates the target. Nothing in the module is permitted to delete or modify a target based on a MISSING_IN_SOURCE finding. See docs/07 section 8.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configscomparison-direction) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].data_standardization_sql` { #target-configsdata-standardization-sql }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Column-level cleanup before matching.


Column-level cleanup before matching. Same restricted grammar as ingestion.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `array<string>` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "data_standardization_sql": [
        "trim(status) AS status"
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
    -- target_configs[].data_standardization_sql lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Entered as a comma-separated list; written to the spec as a JSON array of strings.

**FAQs** (4)

??? question "If omitted · What happens if data_standardization_sql is not set on a target_configs entry?"

    No cleanup is applied -- the target's columns are used as read (after filter_condition), unmodified, going into matching.

??? question "Format gotcha · What grammar does target_configs[].data_standardization_sql accept?"

    The same restricted single-column-expression grammar as ingestion: each entry must be one column expression ending in `AS <column_name>` (e.g. `trim(status) AS status`), must not contain `;`, and must not contain a full SELECT/FROM/JOIN/UNION statement -- `_validate_standardization_expression` rejects any of those. Entered as a comma-separated list in the UI, stored as a JSON array of strings.

??? question "Performance impact · Is data_standardization_sql expensive to run on a large target?"

    Negligible per-entry -- each is a simple column-level expression (trim, cast, etc.) evaluated once per row during the read pass, not a join or aggregation; cost scales linearly with row count and number of expressions, same as any projection.

??? question "Edge case · Does data_standardization_sql run before or after filter_condition on a target?"

    After -- it is applied last, following filter_condition, per docs/07 section 3.2's per-side field ordering. It also runs before hashing, so standardized values (not raw ones) feed into the __framework_hash_value computation when hash_precomputed is false.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configsdata-standardization-sql) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `target_configs[].filter_condition` { #target-configsfilter-condition }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Boolean SQL applied after read.


Boolean SQL applied after read. Supports ${param} substitution.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "filter_condition": "load_date = '${run_date}'"
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
    -- target_configs[].filter_condition lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.

**FAQs** (4)

??? question "If omitted · What happens if filter_condition is left unset on a target_configs entry?"

    No filter is applied after the read -- the full table (after any task_run_id_column narrowing) is used for matching. No default value is substituted.

??? question "Format gotcha · Does target_configs[].filter_condition support ${param} substitution the same way as elsewhere?"

    Yes -- it supports `${param}` substitution, resolved against the owning dataflow group's `pipeline_parameters` on every pipeline update. Do not wrap the placeholder in quotes yourself for a string parameter: `substitute_dynamic_parameters` already supplies the quotes, so `load_date = '${run_date}'` becomes `load_date = ''2024-01-01''`, a ParseException. Write `load_date = ${run_date}` instead.

??? question "Performance impact · Does filter_condition reduce reconciliation cost?"

    Yes -- it is applied after the read but before matching, so a selective filter reduces the row count that reaches the hash-key join and the Phase 1/2 fingerprint computation for that side.

??? question "Edge case · In what order is target_configs[].filter_condition applied relative to task_run_id_column and data_standardization_sql?"

    Task_run_id_column narrowing applies first (immediately after read), then filter_condition, then data_standardization_sql last. A filter_condition can narrow an already task_run_id-narrowed set further, but never widen it. See docs/07 section 5.2.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configsfilter-condition) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [spark sql functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)


---

### `target_configs[].hash_precomputed` { #target-configshash-precomputed }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing.


Reuse existing __framework_hash_key and __framework_hash_value instead of recomputing. Valid because reconciliation targets are always tables.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "hash_precomputed": true
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
    -- target_configs[].hash_precomputed lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What is the default for hash_precomputed on a target_configs entry?"

    `false` -- same default as on `source_config`: the engine computes `__framework_hash_key`/`__framework_hash_value` now from this target's raw (post-standardization) columns rather than trusting stored ones.

??? question "Format gotcha · Is hash_precomputed valid on every target_configs entry regardless of type?"

    It is valid because reconciliation targets are always `type: table` -- but onboarding still rejects `hash_precomputed: true` if that entry's `type` is somehow not `table`, since only a framework-managed table can carry pre-built hash columns.

??? question "Performance impact · Does setting hash_precomputed true on a target save more than on the source?"

    The saving is symmetric per side -- each side that sets it true skips its own hash computation pass. For a large target this is often the bigger win, since targets are frequently the larger, already-materialized dataset in the comparison.

??? question "Edge case · What if hash_precomputed is true on the target but the upstream CDC flow that wrote it didn't enable generate_hash_columns?"

    It fails at runtime, not onboarding, with a hard error naming the missing columns: 'hash_precomputed=True but dataset is missing [__framework_hash_key, __framework_hash_value] -- this dataset was not actually produced by a CDC-dispatched flow with generate_hash_columns enabled'. See docs/07 section 9.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configshash-precomputed) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].read_mode` { #target-configsread-mode }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

At most one side of a given target's comparison may be streaming.


At most one side of a given target's comparison may be streaming. Rejected on presence with value 'streaming' when execution_mode is 'pipeline' or 'pipeline_audit_only': the in-pipeline comparison is a whole-snapshot batch classification, and a stream-static join supports only inner and left_outer, which cannot express MISSING_IN_SOURCE.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (enum)` | — | `batch`, `streaming` | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "read_mode": "batch"
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
    -- target_configs[].read_mode lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Allowed values: batch, streaming.

**FAQs** (4)

??? question "If omitted · What is the default for a target's read_mode?"

    `batch` -- same default as `source_config.read_mode`; `ALLOWED_READ_MODES = {"batch", "streaming"}` and the sample spec shows `"read_mode": "batch"` as the norm.

??? question "Format gotcha · Are the legal values for target read_mode the same as for the source?"

    Yes -- `batch` or `streaming`, validated by the same `_validate_reconciliation_dataset_config` function that handles both `source_config` and each `target_configs[]` entry.

??? question "Performance impact · Is a streaming target read cheaper than a batch one?"

    Not inherently cheaper -- it trades a full table scan for incremental micro-batch reads plus checkpoint state, which helps for a large append-only target read repeatedly but adds checkpoint management overhead a plain batch read does not need.

??? question "Edge case · Can source read_mode be batch while a target's read_mode is streaming?"

    Yes, in job mode -- one flow can freely mix a streaming source against a batch target or vice versa, as long as at most one side of a given target's comparison is streaming (a stream-stream join is unsupported). In `pipeline`/`pipeline_audit_only` mode, `streaming` is rejected on presence for every side.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configsread-mode) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].target_catalog` { #target-configstarget-catalog }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Unity Catalog catalog of the target table.


Unity Catalog catalog of the target table. Composed into the three-part table name on save.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


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
    -- target_configs[].target_catalog lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if target_catalog is not set on a target_configs entry?"

    Onboarding rejects it as required -- it is composed with `target_schema`/`target_table` into the three-part table name the target side reads, so it cannot be left out.

??? question "Format gotcha · Can target_catalog use the {{catalog}} template variable?"

    Yes -- the sample spec shows `"target_catalog": "{{catalog}}"`, resolved at onboarding time the same way `{{catalog}}`/`{{env}}` resolve elsewhere in the spec.

??? question "Performance impact · Does target_catalog choice affect reconciliation cost?"

    Negligible directly -- it only participates in composing the fully-qualified table name. Cross-catalog reads have the same cost profile as same-catalog reads in Unity Catalog; the actual cost driver is the target table's row count and whether hash_precomputed avoids a re-hash.

??? question "Edge case · Is target_catalog required even when comparison_direction is target_to_source only?"

    Yes -- it, along with target_schema/target_table, identifies the dataset to read for this side regardless of comparison_direction; comparison_direction only gates what action is taken on the classification, not whether the target is read at all.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configstarget-catalog) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [uc privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)


---

### `target_configs[].target_id` { #target-configstarget-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Identifies this target in run and mismatch logs.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "target_id": "primary_product_table"
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
    -- target_configs[].target_id lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if target_id is missing on a target_configs entry?"

    Onboarding rejects it -- `check_string(target_id, ..., required=True)` fails; every target needs an identifier for logging and dataset naming.

??? question "Format gotcha · Can target_id repeat across two entries in the same flow's target_configs?"

    No -- the validator tracks `seen_target_ids` and rejects a duplicate: 'target_id must be unique within a reconciliation flow', naming the earlier index it collides with.

??? question "Performance impact · Does the value chosen for target_id affect performance?"

    No -- it is purely an identifier used in log rows and in the published dataset names `recon__<reconciliation_id>__<target_id>__metrics`/`__mismatch`; it carries no runtime cost of its own.

??? question "Edge case · Where does target_id show up in the published datasets?"

    It is embedded in every L3/L4 node name for that target, e.g. `_recon__<rid>__<tid>__tgt`, `recon__<rid>__<tid>__metrics`, `recon__<rid>__<tid>__mismatch`, and in `reconciliation_run_log`/`reconciliation_mismatch_log` rows to identify which target a row belongs to. See docs/07 section 11.1's L0-L5 node map.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configstarget-id) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].target_schema` { #target-configstarget-schema }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Schema of the target table.


Schema of the target table. Composed into the three-part table name on save.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


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
    -- target_configs[].target_schema lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if target_schema is left out of a target_configs entry?"

    Onboarding rejects it as required -- like `target_catalog` and `target_table`, it is composed into the three-part table name on save and there is no default.

??? question "Format gotcha · Can target_schema contain templating like {{env}}?"

    The docs show `{{catalog}}`/`{{env}}` template variables supported on related string fields such as `target_catalog`/`filter_condition`; `target_schema` is a plain string field composed the same way, e.g. `"bronze_example"` in the sample.

??? question "Performance impact · Does target_schema choice have a performance effect?"

    No direct effect -- it is purely part of the fully-qualified table identity used to read the target. Cost is driven by the target table's size and read pattern, not its schema name.

??? question "Edge case · Does target_schema participate in the append-loop V-CYC checks?"

    Indirectly, yes -- `target_catalog.target_schema.target_table` together form the qualified reference the V-CYC placement checks (V-CYC-2/3/5) compare against `append_target_table` and other in-spec targets to detect append-loop hazards. See spec_validator.py's `_validate_reconciliation_pipeline_placement`.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configstarget-schema) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].target_table` { #target-configstarget-table }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

Target table name.


Target table name. Composed into the three-part table name on save.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "target_table": "example_raw_final"
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
    -- target_configs[].target_table lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Required — onboarding rejects the flow if this is missing.

**FAQs** (4)

??? question "If omitted · What happens if target_table is missing on a target_configs entry?"

    Onboarding rejects it as required -- composed with `target_catalog`/`target_schema` into the three-part name this target side reads; there is no default and no way to reconcile against an unnamed table.

??? question "Format gotcha · Is target_table just the bare table name?"

    Yes -- a plain string like `example_raw_final`; it is combined with `target_catalog` and `target_schema` to form the fully-qualified `catalog.schema.table` read reference, not a pre-qualified name itself.

??? question "Performance impact · Does target_table's row count matter for reconciliation runtime?"

    Yes -- it directly drives the L3 prepared-target scan and the L4 full outer join cost; a large target benefits most from `hash_precomputed: true` (when the precondition holds) to skip a full re-hash pass.

??? question "Edge case · Can target_table be the same table as this flow's own source_config.table?"

    As the target itself, no rule forbids that directly, but the composed reference participates in append-loop checks against `append_target_table` (V-CYC-5 and the target_configs[] self/cross-append checks) -- a target's own table must not equal `append_target_table` on the same or another entry, since that would re-arm every future run against its own output.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configstarget-table) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `target_configs[].task_run_id_column` { #target-configstask-run-id-column }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Target dataset</span>

When the task_run_id job parameter is set, narrows this side's read to that run's rows.


When the task_run_id job parameter is set, narrows this side's read to that run's rows. Reconciliation is triggered-only as of v1.4.0. Rejected on presence when execution_mode is 'pipeline' or 'pipeline_audit_only': no stable per-update key exists inside a Lakeflow update, so the narrowing would be a silent no-op. Use filter_condition instead.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.reconciliation_flow_spec.target_configs_json`


=== "JSON"

    ```json
    {
      "task_run_id_column": "__framework_pipeline_run_id"
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
    -- target_configs[].task_run_id_column lives inside the target_configs_json JSON document; inspect it with from_json / get_json_object
    SELECT reconciliation_id,
           target_configs_json,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

**FAQs** (4)

??? question "If omitted · What if task_run_id_column is not set on a target_configs entry?"

    That target side's read stays correlation-only for `task_run_id` -- no narrowing filter is applied, same backward-compatible behaviour as the source-side default.

??? question "Format gotcha · Does target_configs[].task_run_id_column need a specific column name?"

    No fixed name is enforced -- any existing column on that target table works. The framework's own tables typically carry `__framework_pipeline_run_id`, which is the conventional value to use.

??? question "Performance impact · Does setting task_run_id_column on a target reduce read cost?"

    Yes, when the `task_run_id` job parameter is set -- the target's read is narrowed to that run's rows before `filter_condition` applies, reducing rows scanned in downstream matching for that side.

??? question "Edge case · Is task_run_id_column allowed on a target when execution_mode is pipeline_audit_only?"

    No -- rejected on presence in both pipeline modes, for the same reason as on `source_config`: no stable per-update key exists inside a Lakeflow update, so the narrowing would be a silent no-op. Use `filter_condition` instead.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#83-target-only-fields) · [Schema tree](tree.md#tree-reconciliation-target-configstask-run-id-column) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `transform_sql` { #transform-sql }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Matching &amp; healing</span>

Reshapes missing records before append when source and target schemas differ.


Reshapes missing records before append when source and target schemas differ. Must read FROM _reconciliation_unmatched_records. Full Spark SQL, not the restricted grammar.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string (SQL)` | — | — | — |

**Persisted in** `config.reconciliation_flow_spec.transform_sql` · **Behaviour changed in** v1.7.11


=== "JSON"

    ```json
    {
      "transform_sql": "SELECT example_id, amount, status FROM _reconciliation_unmatched_records"
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
    -- transform_sql is persisted as its own column
    SELECT reconciliation_id,
           transform_sql,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Supports {{catalog}} and {{env}} template variables, resolved at onboarding time.

**FAQs** (4)

??? question "If omitted · What happens if transform_sql is not set?"

    No reshaping happens -- the missing/drifted record set is appended into `append_target_table` using its own schema as read from `_reconciliation_unmatched_records`, unmodified. Only needed when source and target schemas differ.

??? question "Format gotcha · What must transform_sql select from, and what grammar does it accept?"

    It must read FROM `_reconciliation_unmatched_records` and is full Spark SQL, not the restricted single-expression grammar `data_standardization_sql` uses -- it legitimately needs a complete SELECT/FROM since it reshapes a miss set. It supports `{{catalog}}`/`{{env}}` template variables and is syntax/plan-validated live via EXPLAIN, same as `transformation_sql`.

??? question "Performance impact · Does transform_sql add meaningful overhead to the append?"

    It adds one extra transform pass over the miss set only (not the full source/target tables), so cost scales with the number of missing/drifted records, which is typically small relative to the compared datasets.

??? question "Edge case · Can transform_sql use ${param} placeholders the same way filter_condition does?"

    The doc groups it with filter_condition as supporting `{{catalog}}`/`{{env}}` template resolution at onboarding time; as with any parameterized SQL in this framework, do not wrap a `${param}` placeholder in quotes for a string parameter -- `substitute_dynamic_parameters` already supplies them, and double-quoting causes a ParseException. Write `${param}` bare in the expression.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-transform-sql) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `two_tier_verification` { #two-tier-verification }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Reconciliation</span> <span class="fx-badge fx-only">Reconciliation identity</span>

Runs a cheap Phase 1 per-side fingerprint (row_count plus an XOR-fold of the framework hash columns) (bit_xor(hash) plus a per-side count) first, and only falls through to the full matcher join when that phase disagrees.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `boolean` | `true` | — | — |

**Persisted in** `config.reconciliation_flow_spec.two_tier_verification` · **Behaviour changed in** v1.5.0


=== "JSON"

    ```json
    {
      "two_tier_verification": true
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
    -- two_tier_verification is persisted as its own column
    SELECT reconciliation_id,
           two_tier_verification,
           is_active, updated_at
    FROM   <catalog>.config.reconciliation_flow_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - phase 1 fingerprint, phase 2 full join
    - Omitting the attribute is not the same as setting it false — check the default above.

**FAQs** (4)

??? question "If omitted · What is the effect of not setting two_tier_verification at all?"

    It defaults to `true`. A cheap Phase 1 per-side fingerprint (row_count plus an XOR-fold of `__framework_hash_key`/`__framework_hash_value`) runs first and short-circuits the whole comparison when both sides agree; the full hash-key join runs only when Phase 1 disagrees. See docs/07 section 4.

??? question "Format gotcha · Is two_tier_verification a per-target setting or per-flow?"

    Per-flow -- it is a top-level boolean on the reconciliation flow object, not nested under `source_config` or `target_configs[]`. It applies uniformly to every target this flow compares.

??? question "Performance impact · Does two_tier_verification true make reconciliation cheaper?"

    Yes when the sides genuinely agree -- Phase 1 is shuffle-free (one `.agg()` scan, no join) and skips the full outer join entirely on a match. Setting it `false` always runs Phase 2 and pays the join cost on every run, which is only worth it if you cannot tolerate Phase 1's accepted false-equal edge case.

??? question "Edge case · Can two_tier_verification produce a wrong reconciliation result?"

    It can produce a false 'equal' but never a false 'different': the XOR fold cancels in pairs, so two datasets differing by an even number of identical duplicate rows can fold to the same digest. `row_count` in the fingerprint catches every cardinality difference, so this only matters for the narrow same-cardinality, even-multiplicity case. Set it `false` if that risk is unacceptable. See docs/07 section 4.


**See also:** [Pillar 3 · Reconciliation](../../pillars/reconciliation.md) · [Attribute dictionary](../../00_master_reference_index.md#8-reconciliation-flow-schema) · [Schema tree](tree.md#tree-reconciliation-two-tier-verification) · [Spec Builder · Reconciliation tab](../../console/spec_builder.md#reconciliation-tab) · [Control dashboard · Reconciliation](../../console/control_dashboard.md#reconciliation) · [Observability dashboard · Quality & Reconciliation](../../console/observability_dashboard.md#quality-reconciliation) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

