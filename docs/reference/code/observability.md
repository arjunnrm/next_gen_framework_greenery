<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `observability`

Event-log extraction and OpenTelemetry export.


11 modules.


## `lakeflow_framework/observability/agent_tools.py`

Real Python implementations backing the 3 AI Agent / Copilot tools this module ships (Deliverable 6 -- see ``agent_skills/dlt_observability_tools.json`` for the declarative OpenAI-function-calling-style tool specs an agent framework loads, and ``docs/25_dlt_observability_module.md``'s Error Handling Matrix, which :func:`diagnose_pipeline_telemetry_failures` is a machine-readable mirror of).


### Functions

| Signature | Purpose |
|---|---|
| `validate_observability_config(config_text: str, catalog: str = '', env: str = '') -> Dict[str, Any]` | Lint/validate an ``{"observability": [...]}`` JSON or YAML fragment against the same ``observability`` schema ``onboarding/spec_validator.py`` enforces at onboarding time (via ``onboarding_templates/onboarding_spec.schema.json`` -- there is no separate observability config file or schema; this validates the fragment on its own, before it's merged into a full onboarding spec's own ``observability[]`` array). |
| `generate_pipeline_onboarding_config(dataflow_group_id: str, destination_targets: List[Dict[str, Any]], service_name: Optional[str] = None, deployment_environment: Optional[str] = None) -> Dict[str, Any]` | Auto-generate an ``observability[]`` array to merge into a pipeline's onboarding spec, from a short list of destination target descriptors -- so a caller doesn't have to hand-author the full ``destination_config`` shape for common destination types. |
| `diagnose_pipeline_telemetry_failures(error_message: str) -> Dict[str, Any]` | Match a failed observability task run's error message against the known failure matrix and return a diagnosis + remediation steps. |


## `lakeflow_framework/observability/config_loader.py`

Loads and resolves telemetry destination configuration from ``observability_config``.


### class `DestinationConfig`

One resolved, enabled telemetry destination for a specific dataflow group.


### Functions

| Signature | Purpose |
|---|---|
| `parse_config_rows(rows: List[Dict[str, Any]], dataflow_group_id: str) -> List[DestinationConfig]` | Parse raw ``observability_config`` rows (as plain dicts) into enabled, resolved :class:`DestinationConfig` objects for one target ``dataflow_group_id``. |
| `load_destination_configs(spark: Any, control_catalog: str, dataflow_group_id: str) -> List[DestinationConfig]` | Read ``<control_catalog>.config.observability_config`` and resolve the enabled destinations for ``dataflow_group_id`` (see :func:`parse_config_rows` for resolution rules). |
| `filter_destinations_by_mode(destinations: List[DestinationConfig], mode: str) -> List[DestinationConfig]` | The subset of ``destinations`` one engine is responsible for. |
| `resolve_event_log_tables(destinations: List[DestinationConfig]) -> List[str]` | The de-duplicated, order-preserving union of every *continuous* destination's ``destination_config.event_log_tables``. |


## `lakeflow_framework/observability/dataflow_documenter.py`

Render a per-dataflow-group design document from the FlowX control metadata.


### Functions

| Signature | Purpose |
|---|---|
| `render_group_document(group: Dict[str, Any], flows: List[Dict[str, Any]], health: Optional[Dict[str, Any]] = None, dq: Optional[List[Dict[str, Any]]] = None, recon: Optional[List[Dict[str, Any]]] = None, lineage: Optional[List[Dict[str, Any]]] = None, generated_at: Optional[str] = None) -> str` | Render one dataflow group as a Markdown design document. |
| `render_index(groups: List[Dict[str, Any]], health_by_group: Optional[Dict[str, Dict[str, Any]]] = None, generated_at: Optional[str] = None) -> str` | Render the index page listing every documented dataflow group. |


## `lakeflow_framework/observability/destination_dispatcher.py`

Dispatches a built OTel payload to every configured, enabled telemetry destination.


### class `DispatchResult`


### Functions

| Signature | Purpose |
|---|---|
| `resolve_credential(value: str, secret_resolver: Callable[[str, str], str]) -> str` | Resolve an ``env:<VAR_NAME>`` or ``secret:<scope>:<key>`` credential reference. |
| `build_auth_headers(auth_config: Dict[str, Any], secret_resolver: Callable[[str, str], str]) -> Dict[str, str]` | Build HTTP headers for ``auth_config`` (``{}``/``NONE`` -> no headers). |
| `compress_payload(data: bytes, compression: Optional[str]) -> 'tuple[bytes, Optional[str]]'` | Return ``(payload_bytes, content_encoding_header)``. |
| `compute_backoff_delay_seconds(attempt: int, retry_config: Dict[str, Any], retry_after_header: Optional[str] = None) -> float` | Delay before the *next* attempt (``attempt`` is the 1-indexed attempt number that just failed). |
| `merge_resource_attributes(resource_logs: List[Dict[str, Any]], extra_attributes: Dict[str, Any]) -> List[Dict[str, Any]]` | Return a deep-enough copy of ``resource_logs`` with ``extra_attributes`` merged into every entry's ``resource.attributes`` -- an existing key of the same name is overridden (destination-specific co… |
| `dispatch_to_volume(resource_logs: List[Dict[str, Any]], destination: DestinationConfig, dataflow_group_id: str, task_run_id: str) -> DispatchResult` | Write one JSONL line per ``ResourceLogs`` entry to the configured Volume path. |
| `dispatch_to_otlp(resource_logs: List[Dict[str, Any]], destination: DestinationConfig, secret_resolver: Callable[[str, str], str], post_fn: Optional[Callable[..., Any]] = None, sleep_fn: Callable[[float], None] = time.sleep) -> DispatchResult` | POST the OTLP export request body to the configured HTTP endpoint, retrying on ``429``/``5xx`` with exponential backoff up to ``retry_config.max_attempts``. |
| `dispatch_all(resource_logs: List[Dict[str, Any]], destinations: List[DestinationConfig], dataflow_group_id: str, task_run_id: str, secret_resolver: Callable[[str, str], str]) -> List[DispatchResult]` | Dispatch to every destination in ``destinations``, one at a time, isolating failures. |


## `lakeflow_framework/observability/event_log_extractor.py`

Extracts DLT/Lakeflow event log rows for a time window and aggregates them per flow.


### class `ExpectationMetric`


### class `ErrorDetail`


### class `FlowMetrics`


| Method | Purpose |
|---|---|
| `duration_ms() -> Optional[int]` |  |


### class `UpdateSummary`


### class `DataflowGroupTelemetry`


### Functions

| Signature | Purpose |
|---|---|
| `resolve_dataflow_group_id(workspace_client: Any, pipeline_id: str) -> str` | Resolve ``pipeline_id``'s configured ``dataflow.group.id`` Spark conf via the Pipelines API. |
| `resolve_update_ids_for_window(workspace_client: Any, pipeline_id: str, start_time_ms: int, end_time_ms: int) -> Optional[List[str]]` | Ask the Pipelines API which of ``pipeline_id``'s updates were created inside the ``[start_time_ms, end_time_ms]`` window, for :func:`extract_raw_events` to narrow on. |
| `extract_raw_events(spark: Any, pipeline_id: str, start_time_ms: int, end_time_ms: int, update_ids: Optional[List[str]] = None) -> Dict[str, Any]` | Query the ``event_log(:pipeline_id)`` table-valued function for the given window. |
| `aggregate_flow_metrics(events: List[Dict[str, Any]], dataflow_group_id: str, pipeline_id: str, window_start_ms: int, window_end_ms: int) -> DataflowGroupTelemetry` | Aggregate raw event dicts (see :func:`extract_raw_events`) into one :class:`DataflowGroupTelemetry`, keyed per ``(update_id, flow_id)`` -- a flow that runs across multiple updates within the window (continuous-mode / multiple triggered updates) gets one :class:`FlowMetrics` entry per update, since row counts reset per update rather than accumulating across them. |


## `lakeflow_framework/observability/otel_payload_builder.py`

Builds strict OpenTelemetry (OTLP/HTTP JSON) ``ResourceLogs`` payloads from :class:`event_log_extractor.DataflowGroupTelemetry`.


### Functions

| Signature | Purpose |
|---|---|
| `build_attribute(key: str, value: Any) -> Dict[str, Any]` |  |
| `build_resource_logs(telemetry: DataflowGroupTelemetry, job_context: Dict[str, Any], service_name: str = 'dlt-observability', deployment_environment: Optional[str] = None) -> List[Dict[str, Any]]` | Build one ``ResourceLogs`` entry per flow (plus pipeline-level-error/zero-flow entries). |
| `validate_resource_logs(resource_logs: List[Dict[str, Any]]) -> None` | Assert every mandatory OTel Logs field is present. |
| `to_export_request(resource_logs: List[Dict[str, Any]]) -> Dict[str, Any]` | Wrap a list of ``ResourceLogs`` in the top-level ``ExportLogsServiceRequest`` shape an OTLP/HTTP consumer expects as its POST body. |
| `build_resource_logs_from_event_rows(rows: List[Dict[str, Any]], service_name: str = 'otel-streaming-sink') -> List[Dict[str, Any]]` | Build one ``ResourceLogs`` entry per distinct ``source_pipeline`` value found in ``rows`` (one micro-batch's worth of raw event-log rows, each already carrying a ``source_pipeline`` column -- see ``observability/otel_streaming_sink.py``), with each row becoming one ``logRecord``. |


## `lakeflow_framework/observability/otel_streaming_sink.py`

A genuine Lakeflow custom sink (PySpark Data Source Sink API) that continuously exports a streaming micro-batch of raw Lakeflow event-log rows to an OpenTelemetry OTLP/HTTP JSON logs endpoint -- registered via ``spark.dataSource.register(OtelStreamingDataSource)`` and referenced from ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py`` as ``dlt.create_sink(format="otel_streaming", ...)``.


### class `OtelStreamingCommitMessage`

Commit message for one partition's ``write()`` call.


### class `OtelStreamingDataSource`

Registered via ``spark.dataSource.register(OtelStreamingDataSource)`` before any ``dlt.create_sink(format="otel_streaming", ...)`` call references it (see ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py``).


| Method | Purpose |
|---|---|
| `name() -> str` |  |
| `streamWriter(schema: StructType, overwrite: bool) -> DataSourceStreamWriter` |  |


## `lakeflow_framework/observability/reconciliation_export.py`

Reconciliation control-table BACKSTOP export -- the audit guarantee for a reconciliation flow that runs inside a Lakeflow Declarative Pipeline (``execution_mode`` ``"pipeline"`` / ``"pipeline_audit_only"``).


### Functions

| Signature | Purpose |
|---|---|
| `export_reconciliation_control_rows(spark: SparkSession, control_catalog: str, group_id: str, pipeline_update_id: str) -> int` | Back-fill ``reconciliation_run_log`` / ``reconciliation_result`` / ``reconciliation_mismatch_log`` from the L4 datasets this group's pipeline-mode reconciliation flows published during ``pipeline_update_id``. |


## `lakeflow_framework/observability/runtime_params.py`

Runtime parameter contract for the **triggered** observability engine.


### class `TriggeredRunParameters`

The validated four.


### Functions

| Signature | Purpose |
|---|---|
| `resolve_triggered_run_parameters(raw_parameters: Mapping[str, Any]) -> TriggeredRunParameters` | Validate and normalize the four required triggered-mode task parameters. |
| `assert_dataflow_group_id_matches(declared: str, resolved_from_pipeline: Optional[str], pipeline_id: str) -> str` | Cross-check the declared ``dataflow_group_id`` against the upstream pipeline's own. |


## `lakeflow_framework/observability/structured_logger.py`

Structured JSON logging for ingestion/transformation/reconciliation/sink operations (Phase 10).


### Functions

| Signature | Purpose |
|---|---|
| `log_flow_event(operation: str, flow_id: str, status: str, records_read: Optional[int] = None, records_written: Optional[int] = None, records_rejected: Optional[int] = None, records_quarantined: Optional[int] = None, duration_ms: Optional[float] = None, error: Optional[str] = None, **extra) -> None` | Emit one structured JSON log line describing a single business-level flow operation. |
| `logged_operation(operation: str, flow_id: str, **extra) -> Iterator[_OperationContext]` | Context manager that times a block of code and emits exactly one :func:`log_flow_event` call describing it, on both the success and failure path. |


## `lakeflow_framework/observability/task_context_resolver.py`

Resolves the upstream ``pipeline_task`` run into a concrete execution window.


### class `TaskExecutionContext`

The resolved identity + time window of the upstream ``pipeline_task`` run.


### Functions

| Signature | Purpose |
|---|---|
| `resolve_task_context(workspace_client: Any, upstream_task_run_id: str) -> TaskExecutionContext` | Resolve the upstream ``pipeline_task`` run into a :class:`TaskExecutionContext`. |

