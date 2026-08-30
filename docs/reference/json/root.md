<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Spec root

Top-level attributes of the onboarding document. Everything else hangs off these.


!!! info "3 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`@dataflow_group_id`](#dataflow-group-id) | string | **yes** | — |
| [`@pipeline_parameters`](#pipeline-parameters) | object<string,string> | no | — |
| [`@spark_config`](#spark-config) | object<string,string> | no | — |

## Attributes

### `@dataflow_group_id` { #dataflow-group-id }

The primary key for this whole onboarding document. Every flow in the spec is upserted into the framework control tables under this one identifier.


Re-running onboarding with the same group id updates the existing rows in place. Changing it creates a second, independent set of control-table rows — the original flows keep running.


**Type** `string` · **Required** yes · **Section** Spec root


```json
"dataflow_group_id": "dfg_finance_txn_ingest"
```


!!! tip "Best practice"

    - Treat it as immutable once a pipeline is live.
    - Use a stable business-domain name, not an environment name — the same group id should exist in dev and prod.


!!! warning "Known errors and limitations"

    **Duplicate pipelines appear after onboarding**  
    *Cause:* The group id was changed between runs, so the old rows were never superseded.  
    *Fix:* Restore the original id, or delete the orphaned control-table rows for the abandoned id.


---

### `@pipeline_parameters` { #pipeline-parameters }

String to value map substituted into ${param} placeholders in transformation_sql, filter_condition and transform_sql.


String to value map substituted into ${param} placeholders in transformation_sql, filter_condition and transform_sql. Strings are single-quoted automatically.


**Type** `object<string,string>` · **Required** no · **Section** Spec root


```json
{
  "pipeline_parameters": {
    "option_name": "value"
  }
}
```


!!! tip "Best practice"

    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


**Databricks documentation:** [pipeline settings](https://docs.databricks.com/delta-live-tables/settings.html)


---

### `@spark_config` { #spark-config }

Group-scoped Spark configuration.


Group-scoped Spark configuration. Keys must start with "spark."; values may be string, number or boolean. Pipeline and runtime values strictly override the framework's built-in defaults.


**Type** `object<string,string>` · **Required** no · **Section** Spec root


```json
{
  "spark_config": {
    "option_name": "value"
  }
}
```


!!! tip "Best practice"

    - keys must start with spark.
    - Keys are written verbatim — a typo becomes a silently ignored option, not an error.


**Databricks documentation:** [pipeline settings](https://docs.databricks.com/delta-live-tables/settings.html)


---
