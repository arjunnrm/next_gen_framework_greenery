<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `onboarding`

Turning a spec document into control-table rows.


8 modules.


## `lakeflow_framework/onboarding/agent_tools.py`

Agent-facing onboarding and lifecycle tools for FlowX (FlowX).


### Functions

| Signature | Purpose |
|---|---|
| `validate_json(spec_content: str, catalog: str = 'poc', env: str = 'dev', strict_mode: bool = True) -> Dict[str, Any]` | Validate candidate onboarding spec text (JSON or YAML) immediately after generation. |
| `onboard_entity(spec_content: str, action_type: str, catalog: str, environment: str, auto_deploy: bool = False, dry_run: bool = False) -> Dict[str, Any]` | Execute idempotent create or update lifecycle onboarding action. |
| `get_catalog_schema_parameters(catalog_name: str, schema_name: str, table_name: Optional[str] = None, include_column_metadata: bool = True, include_governance_tags: bool = True) -> Dict[str, Any]` | Retrieve Unity Catalog table and schema configurations. |


## `lakeflow_framework/onboarding/audit_logger.py`

Perception-audit trail writer for `onboarding_audit_log`.


### Functions

| Signature | Purpose |
|---|---|
| `write_audit_log_entry(spark: SparkSession, control_schema: str, dataflow_group_id: Optional[str], action_type: str, environment: str, onboarded_by: str, spec_version: str, client_context_json: str, status: str, raw_spec_payload: str, error_message: Optional[str] = None) -> None` | Append one perception-audit row to ``onboarding_audit_log``. |


## `lakeflow_framework/onboarding/bulk_onboarding.py`

Bulk (many-spec) onboarding -- one job run that onboards an entire directory of specs.


### Functions

| Signature | Purpose |
|---|---|
| `discover_spec_files(dbutils: Any, spec_dir: str, exclude_names: Optional[List[str]] = None) -> List[str]` | List every onboarding spec file directly inside ``spec_dir``, sorted by file name. |
| `onboard_single_spec(spark: Any, dbutils: Any, spec_path: str, catalog: str, environment: str, action_type: str, control_schema: str, client_context_json: str, validate_spec_fn: Any) -> Dict[str, Any]` | Onboard exactly one spec and return a structured per-spec result record. |
| `summarize_results(results: List[Dict[str, Any]]) -> Dict[str, Any]` | Reduce per-spec result records to a run-level summary (counts + failed file names). |
| `format_results_table(results: List[Dict[str, Any]]) -> str` | Render per-spec results as a fixed-width text table for the job's driver log. |


## `lakeflow_framework/onboarding/client_context.py`

Best-effort perception/client-context capture for the onboarding audit trail.


### Functions

| Signature | Purpose |
|---|---|
| `build_client_context_json(spark: SparkSession, dbutils: Any) -> str` | Best-effort capture of onboarding perception metadata as a JSON string. |


## `lakeflow_framework/onboarding/metadata_upsert.py`

MERGE INTO upserts of onboarded metadata into the control-spec tables (v2 schema).


### Functions

| Signature | Purpose |
|---|---|
| `upsert_dataflow_group_spec(spark: SparkSession, control_schema: str, spec: Dict[str, Any], ingestion_flows: List[Dict[str, Any]], transformation_flows: List[Dict[str, Any]], catalog: str, environment: str) -> None` | Upsert the single ``dataflow_group_spec`` row for this onboarding spec. |
| `upsert_ingestion_flow_spec(spark: SparkSession, control_schema: str, group_id: str, ingestion_flows: List[Dict[str, Any]]) -> None` | Upsert one row per ingestion flow into ``ingestion_flow_spec``. |
| `upsert_transformation_flow_spec(spark: SparkSession, control_schema: str, group_id: str, transformation_flows: List[Dict[str, Any]]) -> None` | Upsert one row per transformation flow into ``transformation_flow_spec``. |
| `upsert_reconciliation_flow_spec(spark: SparkSession, control_schema: str, group_id: str, reconciliation_flows: List[Dict[str, Any]]) -> None` | Upsert one row per reconciliation flow into ``reconciliation_flow_spec``. |
| `upsert_observability_config(spark: SparkSession, control_schema: str, group_id: str, observability_destinations: List[Dict[str, Any]]) -> None` | Upsert one row per telemetry destination into ``observability_config``, scoped to ``group_id`` -- from the ``observability[]`` array in the *same* onboarding spec as every other flow, not a separate config file (see ``docs/25_dlt_observability_module.md``). |


## `lakeflow_framework/onboarding/spec_loader.py`

Onboarding spec loading and ``{{catalog}}`` / ``{{env}}`` environment templating.


### Functions

| Signature | Purpose |
|---|---|
| `read_raw_spec_text(dbutils: Any, path: str) -> str` | Read the raw spec file contents from a UC Volume, Workspace Files, or DBFS path. |
| `substitute_environment_placeholders(raw_text: str, catalog: str, environment: str) -> str` | Substitute ``{{catalog}}`` / ``{{env}}`` placeholders throughout the raw spec text. |
| `parse_spec_text(templated_text: str, file_extension: str, source_label: str) -> Dict[str, Any]` | Parse already-templated spec text as JSON or YAML, chosen by ``file_extension``. |
| `load_and_template_spec(dbutils: Any, path: str, catalog: str, environment: str) -> Tuple[Dict[str, Any], str, str]` | Read, template, and parse an onboarding spec (JSON or YAML) in one step. |


## `lakeflow_framework/onboarding/spec_validator.py`

Structural, type, allowed-value, and SQL-syntax validation for onboarding specs (v2 schema).


### Functions

| Signature | Purpose |
|---|---|
| `reject_unknown_keys(config: Any, path_prefix: str, errors: List[str], allowed: set) -> None` | Append one error per key on ``config`` the framework does not read. |
| `reject_removed_keys(config: Any, path_prefix: str, errors: List[str], removed: Dict[str, str]) -> None` | Append one error per removed key present on ``config``. |
| `reject_mode_incompatible_keys(config: Any, path_prefix: str, errors: List[str], incompatible: Dict[str, str], mode: str) -> None` | Append one error per key present on ``config`` that is incompatible with the ``mode`` the caller has already determined applies here -- e.g. |
| `check_bool(value: Any, path: str, errors: List[str], required: bool = False) -> None` | Validate that `value` is a real Python bool -- catches the classic 'abc' / 'true' (string) mistake. |
| `check_string(value: Any, path: str, errors: List[str], required: bool = False, allowed_values: Optional[set] = None) -> None` | Validate that `value` is a non-empty string, optionally restricted to `allowed_values`. |
| `check_int(value: Any, path: str, errors: List[str], required: bool = False, minimum: Optional[int] = None) -> None` | Validate that `value` is an int (booleans are rejected even though `bool` subclasses `int`). |
| `check_list_of_str(value: Any, path: str, errors: List[str], required: bool = False) -> None` | Validate that `value` is a JSON array of strings. |
| `check_dict(value: Any, path: str, errors: List[str], required: bool = False) -> bool` | Validate that `value` is a JSON object. |
| `check_dict_of_str(value: Any, path: str, errors: List[str], required: bool = False) -> bool` | Validate that `value` is a JSON object whose keys and values are all strings. |
| `check_secret_ref(value: Any, path: str, errors: List[str], required: bool = False) -> None` | Validate a Unity Catalog three-level secret reference: ``{secret_catalog, secret_schema, secret_key}``. |
| `check_credential_ref(value: Any, path: str, errors: List[str], required: bool = False) -> None` | Validate an ``observability`` credential reference: ``'env:<VAR_NAME>'`` or ``'secret:<scope>:<key>'`` -- never a literal secret value. |
| `validate_spec(spark: SparkSession, spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[str]]` | Validate the full onboarding spec: structure, types, allowed values, and SQL syntax. |


## `lakeflow_framework/onboarding/uc_spec_preflight.py`

Agent-facing "is this spec safe to onboard?" preflight tool for onboarding specs (v2 schema).


### Functions

| Signature | Purpose |
|---|---|
| `preflight_check_onboarding_spec(spec_json_or_yaml_text: str, catalog: str) -> str` | Check whether an onboarding spec is safe to onboard. |

