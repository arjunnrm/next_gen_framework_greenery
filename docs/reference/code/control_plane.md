<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `control_plane`

The eight control tables — DDL, provisioning, repository access, post-deployment steps.


4 modules.


## `lakeflow_framework/control_plane/ddl_definitions.py`

DDL text for the control metadata schema and its control tables (see ``get_all_control_table_ddls`` for the authoritative, current list).


### Functions

| Signature | Purpose |
|---|---|
| `render_table_properties_clause(properties: Dict[str, str]) -> str` | Render a Python dict of Delta table properties as a SQL ``TBLPROPERTIES`` clause. |
| `get_schema_ddl(control_schema: str) -> str` |  |
| `get_dataflow_group_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_ingestion_flow_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_transformation_flow_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_onboarding_audit_log_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_reconciliation_flow_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_reconciliation_run_log_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_reconciliation_mismatch_log_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_reconciliation_result_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_observability_config_ddl(control_schema: str, table_properties: Dict[str, str]) -> str` |  |
| `get_all_control_table_ddls(control_schema: str, table_properties: Dict[str, str]) -> List[Tuple[str, str]]` | Return ``(description, ddl)`` pairs for all control tables, in creation order. |
| `get_preflight_function_ddl(control_schema: str, onboarding_spec_schema_json: str) -> str` | Build the ``CREATE OR REPLACE FUNCTION`` statement for ``preflight_check_onboarding_spec``, a Unity Catalog Python Function callable directly via SQL -- by Genie, a Mosaic AI Agent, or any MCP tool-calling loop -- with no Python host process required. |


## `lakeflow_framework/control_plane/post_deployment.py`

Post-deployment governance (ABAC tag application), run *after* a pipeline update.


### Functions

| Signature | Purpose |
|---|---|
| `apply_all_governance_tags(spark: SparkSession, control_catalog: str, group_id: str) -> None` | Apply governance tags (column + table) for every active flow in `group_id` with `governance_tags_json` set. |
| `capture_all_scd_change_counts(spark: SparkSession, control_catalog: str, group_id: str) -> None` | Best-effort insert/update/delete count capture (Phase 10) for every CDC-dispatched flow in ``group_id``, emitted as structured JSON log events via ``observability.structured_logger.log_flow_event`` -- the "records updated, inserted, deleted" half of this framework's structured-logging requirement for SCD/CDC flows, closing out the dependency an earlier phase's own code comment in ``cdc/change_metrics.py`` left for this one. |


## `lakeflow_framework/control_plane/repository.py`

Read access to the active dataflow-group / ingestion-flow / transformation-flow metadata.


### Functions

| Signature | Purpose |
|---|---|
| `load_active_group_metadata(spark: SparkSession, control_catalog: str, group_id: str) -> Tuple[Any, List[Any], List[Any]]` | Load the active group row plus its active ingestion/transformation flow rows. |


## `lakeflow_framework/control_plane/schema_provisioner.py`

Idempotent control-schema/table provisioning, shared by the setup notebook and onboarding.


### Functions

| Signature | Purpose |
|---|---|
| `ensure_control_schema_exists(spark: SparkSession, control_catalog: str, table_properties: Optional[Dict[str, str]] = None) -> None` | Create the ``config`` schema and all four control tables if they don't already exist. |
| `is_already_exists_race(exc: Exception) -> bool` | True when ``exc`` means "another session created this object concurrently". |
| `ensure_preflight_function_exists(spark: SparkSession, control_catalog: str, onboarding_spec_schema_json: str) -> None` | Create/replace the ``preflight_check_onboarding_spec`` Unity Catalog Python Function in ``<control_catalog>.config``, so it's directly SQL/Genie/MCP-tool-callable (see ``ddl_definitions.get_preflight_function_ddl`` for exactly what it checks and why it's a structural-only sibling of ``onboarding/uc_spec_preflight.py``'s full Python tool). |

