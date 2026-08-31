<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `engine`

Graph construction: flow and sink registration, run context, Spark configuration.


7 modules.


## `lakeflow_framework/engine/flow_generators.py`

The three per-row flow generators -- ingestion, transformation, reconciliation.


### Functions

| Signature | Purpose |
|---|---|
| `resolve_pipeline_schema(spark: Any, group_row: Any = None) -> Optional[str]` | Resolve the schema this pipeline publishes into, or ``None`` if nothing can determine it. |
| `generate_ingestion_flow(spark, dbutils, flow_row, plan: SourcePlanePlan, pipeline_parameters: Dict[str, Any], pipeline_run_id: Optional[str] = None) -> None` | Register one ``ingestion_flow_spec`` row's staged intermediate + flow output. |
| `generate_transformation_flow(spark, dbutils, flow_row = None, plan: SourcePlanePlan, pipeline_parameters: Dict[str, Any], pipeline_run_id: Optional[str] = None) -> None` | Register one ``transformation_flow_spec`` row's input views, staged intermediate and flow output -- the twin of :func:`generate_ingestion_flow`. |
| `generate_reconciliation_flow(spark, flow_row, plan: SourcePlanePlan, publish_catalog: str, publish_schema: str, control_schema: str, pipeline_update_id: Optional[str] = None, log_capture_overrides: Optional[Dict[str, Optional[bool]]] = None, pipeline_parameters: Optional[Dict[str, Any]] = None) -> None` | Register one ``reconciliation_flow_spec`` row as the DAG's THIRD flow type. |


## `lakeflow_framework/engine/flow_registration.py`

Shared flow-output registration: staged view -> main/quarantine tables -> CDC dispatch.


### Functions

| Signature | Purpose |
|---|---|
| `register_staged_view(staged_view_name: str, comment: str, dq_rules: List[Dict[str, Any]], build_dataframe: Callable[[], DataFrame], target_config: Dict[str, Any], pipeline_run_id: Optional[str] = None, record_id_column: Optional[str] = None, capture_technical_metadata: bool = True, flow_id: Optional[str] = None, read_operation_name: str = 'flow_read', materialize: bool = False, target_catalog: Optional[str] = None, target_schema: Optional[str] = None) -> str` | Register the staged intermediate shared by both engines, as a ``@dlt.view`` (default) or, when ``materialize`` is set, a genuine ``@dlt.table``. |
| `register_flow_output(flow_label: str, staged_view_name: str, target_table: str, target_catalog: str, target_schema: str, cdc_load_strategy: str, target_config: Dict[str, Any], dq_rules: List[Dict[str, Any]], comment: Optional[str], is_streaming: bool, target_type: str, quarantine_table_override: Optional[str] = None) -> bool` | Register this flow's output according to its ``target_type`` -- the shared tail end of both ``generate_ingestion_flow`` and ``generate_transformation_flow``. |


## `lakeflow_framework/engine/identifiers.py`

Shared Lakeflow dataset/sink identifier helpers.


### Functions

| Signature | Purpose |
|---|---|
| `sanitize_identifier(value: str) -> str` | Turn an arbitrary configured name into a valid Lakeflow dataset/sink identifier fragment. |
| `stable_node_name(prefix: str, locator: str, suffix: str, max_core: int = 80) -> str` | Build a collision-safe Lakeflow dataset name fragment for a shared source-plane node. |


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
| `register_foreach_batch_sink(sink_name: str, handler: Callable[[DataFrame, int], None]) -> None` | Register a genuine Lakeflow ``dlt.foreach_batch_sink`` -- the ForEachBatch sink type (see https://learn.microsoft.com/en-us/azure/databricks/ldp/for-each-batch) -- and apply ``handler`` to it, exactly as ``@dlt.foreach_batch_sink(name=...)`` would if used as a decorator. |
| `require_streaming_source(flow_label: str, is_streaming: bool, construct: str, detail: str) -> None` | Raise :class:`FrameworkConfigError` unless ``is_streaming`` is ``True``. |
| `register_sink_target(flow_label: str, staged_view_name: str, target_table: str, target_catalog: str, target_schema: str, target_config: Dict[str, Any], dq_rules: List[Dict[str, Any]], is_streaming: bool, quarantine_table_override: Optional[str] = None) -> None` | Register ``target_type: "sink"``: staged view -> ``dlt.create_sink`` + ``@dlt.append_flow``. |
| `register_external_sink_export(flow_label: str, qualified_main_table: str, target_table: str, target_config: Dict[str, Any], main_table_is_streaming: bool) -> None` | Register the export half of ``target_type: "external_sink"``: a SECOND, separate ``@dlt.append_flow`` reading from the already-materialized main table (``dlt.read_stream(qualified_main_table)``) into a ``dlt.create_sink``. |


## `lakeflow_framework/engine/source_plane.py`

L0 source plane: plan and register exactly-once external reads for one dataflow-group pipeline.


### class `FrameworkGraphCycleError`

Raised by :func:`assert_acyclic` when the source plane's producer -> consumer edges contain a cycle.


### class `ReadIdentity`

The read-once identity key for one external physical locator.


### class `ConsumerRequest`

One dataset body's declared need for a read of ``identity``.


### class `PlaneNode`

One materialized L0 source-plane node: a shared external read backing 2+ consumers (or every consumer, when ``materialize="always"``).


### class `Binding`

How one ``consumer_id`` resolves at bind time.


### class `SourcePlanePlan`

The full plan output of :func:`plan_source_plane`.


### Functions

| Signature | Purpose |
|---|---|
| `plan_source_plane(ingestion_rows: List[Any], transformation_rows: List[Any], reconciliation_rows: List[Any], pipeline_parameters: Optional[Dict[str, Any]] = None, materialize: str = 'auto', node_catalog: Optional[str] = None, node_schema: Optional[str] = None) -> SourcePlanePlan` | Plan the L0 source plane for one dataflow-group pipeline update. |
| `assert_acyclic(plan: SourcePlanePlan) -> None` | Kahn's algorithm over ``plan.edges`` (producer-owner -> consumer-owner). |
| `register_source_plane(spark, plan: SourcePlanePlan) -> None` | Register one ``@dlt.table`` per :class:`PlaneNode` in ``plan.nodes``. |
| `bind(plan: SourcePlanePlan, consumer_id: str, want_stream: bool)` | Resolve ``consumer_id`` to a ``DataFrame`` per its planned :class:`Binding`. |
| `describe_plan(plan: SourcePlanePlan) -> List[Dict[str, Any]]` | A flat, JSON-serializable description of the plan for logging/observability/tests. |


## `lakeflow_framework/engine/spark_config.py`

Hierarchical Spark session configuration for a Lakeflow Declarative Pipeline update.


### Functions

| Signature | Purpose |
|---|---|
| `coerce_spark_conf_value(value: Any) -> str` | Render one configuration value the way ``spark.conf.set`` expects it. |
| `resolve_spark_conf(framework_defaults: Optional[Dict[str, Any]] = None, spec_spark_config: Optional[Dict[str, Any]] = None, pipeline_spark_config: Optional[Dict[str, Any]] = None) -> Dict[str, str]` | Merge the three precedence layers into one flat ``{key: str-value}`` mapping. |
| `read_pipeline_spark_config(spark: SparkSession) -> Dict[str, Any]` | Read and JSON-decode the pipeline resource's own :data:`PIPELINE_SPARK_CONF_KEY` entry. |
| `apply_spark_conf(spark: SparkSession, resolved_conf: Dict[str, str]) -> Dict[str, str]` | Thin applier: ``spark.conf.set`` each resolved key, returning the subset actually applied. |

