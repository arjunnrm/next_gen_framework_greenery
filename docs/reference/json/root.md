<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Spec root

Top-level attributes of the onboarding document. Everything else hangs off these.


!!! info "3 attributes · 14 FAQs"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form. Badges: <span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Flow kind</span> <span class="fx-badge fx-ver">vX.Y+ added in</span> <span class="fx-badge fx-only">Spec Builder section</span>.


**Recipes and deep dive:** [Platform architecture](../../01_platform_architecture.md) · [Schema tree](tree.md) · [Removed & rejected](removed.md)


## Summary

| Attribute | Type | Required | Default | Since |
|---|---|---|---|---|
| [`@dataflow_group_id`](#dataflow-group-id) | string | **yes** | — | — |
| [`@pipeline_parameters`](#pipeline-parameters) | object<string,string> | no | — | — |
| [`@spark_config`](#spark-config) | object<string,string> | no | — | — |

## Attributes

### `@dataflow_group_id` { #dataflow-group-id }

<span class="fx-badge fx-req">Required</span> <span class="fx-badge fx-flow">Spec root</span> <span class="fx-badge fx-only">Spec root</span>

The primary key for this whole onboarding document. Every flow in the spec is upserted into the framework control tables under this one identifier.


Re-running onboarding with the same group id updates the existing rows in place. Changing it creates a second, independent set of control-table rows — the original flows keep running.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `string` | — | — | min length `1` |

**Persisted in** `config.dataflow_group_spec.dataflow_group_id`


=== "JSON"

    ```json
    "dataflow_group_id": "dfg_finance_txn_ingest"
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
    -- @dataflow_group_id is persisted as its own column
    SELECT dataflow_group_id,
           dataflow_group_id,
           is_active, updated_at
    FROM   <catalog>.config.dataflow_group_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Treat it as immutable once a pipeline is live.
    - Use a stable business-domain name, not an environment name — the same group id should exist in dev and prod.

!!! warning "Known errors and limitations"

    **Duplicate pipelines appear after onboarding**  
    *Cause:* The group id was changed between runs, so the old rows were never superseded.  
    *Fix:* Restore the original id, or delete the orphaned control-table rows for the abandoned id.

**FAQs** (5)

??? question "If omitted · What happens if I leave dataflow_group_id out of the spec?"

    Onboarding hard-rejects the spec: `check_string(spec.get("dataflow_group_id"), "dataflow_group_id", errors, required=True)` fails validation with a missing-field error before any flow is upserted. There is no default; every spec must carry one.

??? question "Format gotcha · Is there a naming convention or pattern enforced for dataflow_group_id?"

    The validator only checks it is a non-empty string; there is no regex or pattern constraint. Convention (per docs/00 §1) is a stable business-domain name such as `dfg_finance_txn_ingest`, kept identical across dev and prod rather than encoding the environment.

??? question "Performance impact · Does changing dataflow_group_id have any runtime cost?"

    Not a performance concern but a correctness one: every control-table row is keyed by this id, so it is cheap to change but expensive in effect — see edge_case below. There is no measurable pipeline runtime impact from the id's value itself.

??? question "Edge case · Why do I see duplicate pipelines after I renamed dataflow_group_id and re-ran onboarding?"

    Re-onboarding upserts rows keyed on this id, so a changed id creates a second, independent set of control-table rows while the original flows under the old id keep running untouched. Fix by restoring the original id or deleting the orphaned rows for the abandoned id.

??? question "Edge case · Can a reconciliation flow use a different dataflow_group_id than the spec root?"

    Yes — `reconciliation_flows[].dataflow_group_id` is independent and, if absent, defaults to the spec's own root `dataflow_group_id`. Declaring a different one places that recon flow's registration into another group's pipeline instead (V-CYC-4 warns naming both groups). See docs/14 §4.4.


**See also:** [Platform architecture](../../01_platform_architecture.md) · [Attribute dictionary](../../00_master_reference_index.md#1-top-level-spec-schema) · [Schema tree](tree.md#tree-root-dataflow-group-id) · [Spec Builder · Spec root](../../console/spec_builder.md#spec-root-and-observability) · [Gotchas](../../13_known_limitations_and_gotchas.md)


---

### `@pipeline_parameters` { #pipeline-parameters }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Spec root</span> <span class="fx-badge fx-only">Spec root</span>

String to value map substituted into ${param} placeholders in transformation_sql, filter_condition and transform_sql.


String to value map substituted into ${param} placeholders in transformation_sql, filter_condition and transform_sql. Strings are single-quoted automatically.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.dataflow_group_spec.pipeline_parameters_json`


=== "JSON"

    ```json
    {
      "pipeline_parameters": {
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
    -- @pipeline_parameters lives inside the pipeline_parameters_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_group_id,
           pipeline_parameters_json,
           is_active, updated_at
    FROM   <catalog>.config.dataflow_group_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (4)

??? question "If omitted · What happens to ${param} placeholders if pipeline_parameters is omitted?"

    `pipeline_parameters` defaults to `{}` (docs/00 §1); any `${param}` placeholder in `transformation_sql`, `filter_condition` or path fields then has no matching key, and `substitute_dynamic_parameters`/`substitute_path_parameters` raise `FrameworkConfigError` for an undefined placeholder — this is checked at onboarding time by `_validate_path_parameters`/`_validate_sql_syntax`.

??? question "Format gotcha · Do I need to add quotes around a string pipeline_parameters value inside my SQL?"

    No — string values are automatically single-quoted during substitution; wrapping a placeholder in your own quotes (`'${region_code}'`) produces a doubled-quote literal (`''EMEA''`) and a `ParseException` at runtime. Write `region = ${region_code}` unquoted, per docs/03 §5.

??? question "Performance impact · Does changing pipeline_parameters require re-onboarding, and is that expensive?"

    No re-onboarding is needed — unlike `{{catalog}}`/`{{env}}` template variables (resolved once at onboarding), `${param}` values are resolved fresh on every pipeline update by editing `pipeline_parameters` and re-running, per docs/03 §5's comparison table.

??? question "Edge case · Can pipeline_parameters be used inside observability destination_config.volume_path?"

    No — `transformation/parameters.py` deliberately excludes `observability_config.destination_config.volume_path` from path-parameter substitution, since a `${param}`-prefixed value would newly fail the strict `/Volumes/`-prefix check at onboarding; it is left out of scope rather than half-supported.


**See also:** [Platform architecture](../../01_platform_architecture.md) · [Attribute dictionary](../../00_master_reference_index.md#11-template-variables-parameter-substitution) · [Schema tree](tree.md#tree-root-pipeline-parameters) · [Spec Builder · Spec root](../../console/spec_builder.md#spec-root-and-observability) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [pipeline settings](https://docs.databricks.com/delta-live-tables/settings.html)


---

### `@spark_config` { #spark-config }

<span class="fx-badge fx-opt">Optional</span> <span class="fx-badge fx-flow">Spec root</span> <span class="fx-badge fx-only">Spec root</span>

Group-scoped Spark configuration.


Group-scoped Spark configuration. Keys must start with "spark."; values may be string, number or boolean. Pipeline and runtime values strictly override the framework's built-in defaults.


**Type & constraints**

| Type | Default | Allowed values | Constraints |
|---|---|---|---|
| `object<string,string>` | — | — | — |

**Persisted in** `config.dataflow_group_spec.spark_config_json`


=== "JSON"

    ```json
    {
      "spark_config": {
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
    -- @spark_config lives inside the spark_config_json JSON document; inspect it with from_json / get_json_object
    SELECT dataflow_group_id,
           spark_config_json,
           is_active, updated_at
    FROM   <catalog>.config.dataflow_group_spec
    WHERE  dataflow_group_id = '<dataflow_group_id>';
    ```

!!! tip "Best practice"

    - keys must start with spark.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.

**FAQs** (5)

??? question "If omitted · What Spark settings apply if spark_config is left out of the spec?"

    Only the framework's built-in defaults apply — as of the documented contract, `FRAMEWORK_SPARK_DEFAULTS` in `engine/spark_config.py` carries exactly one entry, `{"spark.sql.shuffle.partitions": "200"}`, restating Spark's own default explicitly. No group-scoped overrides are applied.

??? question "Format gotcha · Can spark_config values be nested objects, or must a key start with a specific prefix?"

    Every key must be a string starting with `"spark."`, and every value must be a `str`/`int`/`float`/`bool` — a dict or list value, or a key without the `spark.` prefix, is rejected by `_validate_spark_config` with e.g. `'{path}.{key}: expected a Spark configuration key ... got ... -- keys must start with 'spark.''`. Values are rendered with plain `str()` before `spark.conf.set` (booleans as lowercase `true`/`false`), so a numeric value like `200` is stored as the string `"200"`.

??? question "Performance impact · Can spark_config actually change pipeline performance, e.g. shuffle partitions?"

    Yes — it is applied via `spark.conf.set` on the pipeline session before any flow is registered, so every `@dlt.table`/`@dlt.view` closure subsequently defined executes under it; e.g. `spark.sql.shuffle.partitions` genuinely changes shuffle parallelism for the whole group.

??? question "Edge case · Does a value in spark_config always win over the pipeline resource's own dataflow.spark.conf setting?"

    No — precedence is the opposite: this is the middle of a three-layer chain (`FRAMEWORK_SPARK_DEFAULTS` < `spark_config` < the pipeline resource's own `dataflow.spark.conf`), so the pipeline resource's bundle-level setting overrides `spark_config` per-key, not the reverse; resolution is a per-key merge, never a wholesale replace. See docs/01 §5.

??? question "Edge case · Should I ever put pipelines.incompatibleViewCheck.enabled=false into spark_config to work around a streaming-view read error?"

    No — docs/13 explicitly records this as evaluated and rejected: it is pipeline-wide (disables the guard for every dataset in the group via `engine/spark_config.py`) and does not make a genuinely streaming plan batch-readable, it only silences the check. See docs/13 (L4 streaming-view section).


**See also:** [Platform architecture](../../01_platform_architecture.md) · [Attribute dictionary](../../00_master_reference_index.md#1-top-level-spec-schema) · [Schema tree](tree.md#tree-root-spark-config) · [Spec Builder · Spec root](../../console/spec_builder.md#spec-root-and-observability) · [Gotchas](../../13_known_limitations_and_gotchas.md)


**Databricks documentation:** [pipeline settings](https://docs.databricks.com/delta-live-tables/settings.html)


---

