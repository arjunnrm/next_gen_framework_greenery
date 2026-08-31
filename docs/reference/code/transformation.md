<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `transformation`

Silver/Gold SQL execution and target materialisation.


2 modules.


## `lakeflow_framework/transformation/inputs.py`

Transformation-engine multi-input registration: one watermarked ``@dlt.view`` per input.


### Functions

| Signature | Purpose |
|---|---|
| `register_transformation_inputs(spark: SparkSession, source_inputs: List[Dict[str, Any]], plan: SourcePlanePlan, flow_step_id: str) -> None` | Register one ``@dlt.view`` per configured transformation input, applying watermarks. |
| `mark_streaming_references(sql_text: str, source_inputs: List[Dict[str, Any]]) -> str` | Prefix every ``FROM``/``JOIN`` reference to a streaming input with SQL's ``STREAM`` keyword. |


## `lakeflow_framework/transformation/parameters.py`

Dynamic runtime parameter substitution for ``${param}`` placeholders.


### Functions

| Signature | Purpose |
|---|---|
| `substitute_dynamic_parameters(sql_text: str, parameters: Dict[str, Any]) -> str` | Substitute ``${param}`` placeholders in transformation SQL with resolved values. |
| `substitute_path_parameters(text: str, parameters: Dict[str, Any]) -> str` | Substitute ``${param}`` placeholders in path/URI-shaped text with resolved values. |

