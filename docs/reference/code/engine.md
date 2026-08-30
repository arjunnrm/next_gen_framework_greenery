<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `engine`

Graph construction: flow and sink registration, run context, Spark configuration.


4 modules.


## `lakeflow_framework/engine/flow_registration.py`

Shared flow-output registration: staged view -> main/quarantine tables -> CDC dispatch.


### Functions

| Signature | Purpose |
|---|---|
| `register_staged_view(staged_view_name: str, comment: str, dq_rules: List[Dict[str, Any]], build_dataframe: Callable[[], DataFrame], target_config: Dict[str, Any], pipeline_run_id: Optional[str] = None, record_id_column: Optional[str] = None, capture_technical_metadata: bool = True, flow_id: Optional[str] = None, read_operation_name: str = 'flow_read') -> None` | Register the ``@dlt.view`` staged intermediate shared by both engines. |
| `register_flow_output(flow_label: str, staged_view_name: str, target_table: str, target_catalog: str, target_schema: str, cdc_load_strategy: str, target_config: Dict[str, Any], dq_rules: List[Dict[str, Any]], comment: Optional[str], is_streaming: bool, target_type: str, quarantine_table_override: Optional[str] = None) -> bool` | Register this flow's output according to its ``target_type`` -- the shared tail end of both ``generate_ingestion_flow`` and ``generate_transformation_flow``. |


## `lakeflow_framework/engine/run_context.py`

Best-effort pipeline/update run-identifier resolution, for quarantine-row traceability.


### Functions

| Signature | Purpose |
|---|---|
| `resolve_pipeline_run_id(spark: SparkSession, group_id: str) -> str` | Best-effort resolution of a run/update identifier to attach to quarantine metadata. |


## `lakeflow_framework/engine/sink_registration.py`

Genuine Lakeflow sink registration: ``target_type: "sink"`` and the export half of ``target_type: "external_sink"``.


### Functions

| Signature | Purpose |
|---|---|
| `register_sink_target(flow_label: str, staged_view_name: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], dq_rules: List[Dict[str, Any]], is_streaming: bool, quarantine_table_override: Optional[str] = None) -> None` | Register ``target_type: "sink"``: staged view -> ``dlt.create_sink`` + ``@dlt.append_flow``. |
| `register_external_sink_export(flow_label: str, qualified_main_table: str, target_table: str, target_config: Dict[str, Any], main_table_is_streaming: bool) -> None` | Register the export half of ``target_type: "external_sink"``: a SECOND, separate ``@dlt.append_flow`` reading from the already-materialized main table (``dlt.read_stream(qualified_main_table)``) into a ``dlt.create_sink``. |


## `lakeflow_framework/engine/spark_config.py`

Hierarchical Spark session configuration for a Lakeflow Declarative Pipeline update.


### Functions

| Signature | Purpose |
|---|---|
| `coerce_spark_conf_value(value: Any) -> str` | Render one configuration value the way ``spark.conf.set`` expects it. |
| `resolve_spark_conf(framework_defaults: Optional[Dict[str, Any]] = None, spec_spark_config: Optional[Dict[str, Any]] = None, pipeline_spark_config: Optional[Dict[str, Any]] = None) -> Dict[str, str]` | Merge the three precedence layers into one flat ``{key: str-value}`` mapping. |
| `read_pipeline_spark_config(spark: SparkSession) -> Dict[str, Any]` | Read and JSON-decode the pipeline resource's own :data:`PIPELINE_SPARK_CONF_KEY` entry. |
| `apply_spark_conf(spark: SparkSession, resolved_conf: Dict[str, str]) -> Dict[str, str]` | Thin applier: ``spark.conf.set`` each resolved key, returning the subset actually applied. |

