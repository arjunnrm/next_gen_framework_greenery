"""DDL text for the control metadata schema and its control tables (see
``get_all_control_table_ddls`` for the authoritative, current list).

Kept as pure string-building functions (no ``spark.sql`` execution here) so the DDL can be
reviewed, diffed, or unit tested independently of a live cluster. ``01_setup_control_tables.py``
is the only place that actually executes these statements.
"""

import base64
from typing import Dict, List, Tuple


def render_table_properties_clause(properties: Dict[str, str]) -> str:
    """Render a Python dict of Delta table properties as a SQL ``TBLPROPERTIES`` clause."""
    if not properties:
        return ""
    rendered = ", ".join(f"'{key}' = '{value}'" for key, value in properties.items())
    return f"TBLPROPERTIES ({rendered})"


def get_schema_ddl(control_schema: str) -> str:
    return (
        f"CREATE SCHEMA IF NOT EXISTS {control_schema} "
        f"COMMENT 'Control metadata schema for the metadata-driven Lakeflow framework'"
    )


def get_dataflow_group_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.dataflow_group_spec (
    dataflow_group_id           STRING NOT NULL COMMENT 'Unique identifier for the pipeline group / DAG',
    environment                 STRING NOT NULL COMMENT 'Target environment: dev, staging, prod, etc.',
    catalog_name                STRING NOT NULL COMMENT 'Resolved Unity Catalog catalog for this group',
    has_ingestion_flows         BOOLEAN NOT NULL COMMENT 'True when this group defines one or more ingestion flows',
    has_transformation_flows    BOOLEAN NOT NULL COMMENT 'True when this group defines one or more transformation flows',
    pipeline_parameters_json    STRING COMMENT 'JSON object of dynamic runtime parameters substituted into ${{param}} placeholders in SQL and paths (transformation/parameters.py) -- string substitution, NOT Spark configuration; see spark_config_json for the latter',
    spark_config_json           STRING COMMENT 'JSON object of Spark configuration applied to the pipeline session for this group -- see engine/spark_config.py for the precedence chain (framework defaults < this < the pipeline resource configuration: dataflow.spark.conf). Distinct from pipeline_parameters_json, which is ${{param}} string substitution.',
    is_active                   BOOLEAN NOT NULL COMMENT 'Soft-disable flag; inactive groups are skipped by the engine',
    created_at                  TIMESTAMP NOT NULL COMMENT 'Row creation timestamp (UTC)',
    updated_at                  TIMESTAMP NOT NULL COMMENT 'Last upsert timestamp (UTC)',
    CONSTRAINT dataflow_group_spec_pk PRIMARY KEY (dataflow_group_id)
)
USING DELTA
CLUSTER BY (dataflow_group_id, environment)
COMMENT 'Top-level control record for one orchestrated Lakeflow dataflow group -- dependency ordering between groups is a Lakeflow Jobs concern (job task depends_on), not tracked here'
{render_table_properties_clause(table_properties)}
"""


def get_ingestion_flow_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.ingestion_flow_spec (
    dataflow_id            STRING NOT NULL COMMENT 'Unique identifier for this ingestion flow',
    dataflow_group_id      STRING NOT NULL COMMENT 'Parent dataflow group identifier',
    source_system          STRING COMMENT 'Logical source system name (e.g. billing, network_probes)',
    source_database        STRING COMMENT 'Source database / dataset name, when applicable',
    source_table_name      STRING COMMENT 'Source table or logical object name',
    source_description     STRING COMMENT 'Free-text description used as the target table comment',
    source_type            STRING NOT NULL COMMENT 'One of: autoloader, zerobus, asn1',
    target_catalog         STRING NOT NULL COMMENT 'Target Unity Catalog catalog',
    target_schema          STRING NOT NULL COMMENT 'Target Unity Catalog schema',
    target_table           STRING NOT NULL COMMENT 'Target table name (Bronze)',
    target_type            STRING NOT NULL COMMENT 'One of: streaming_table, materialized_view, batch_table, external_sink, sink',
    cdc_load_strategy      STRING NOT NULL COMMENT 'Denormalized copy of target_config.cdc_load_strategy for fast SQL filtering/dispatch: one of APPEND, TRUNCATE_AND_LOAD, SCD1, SCD2, FULL_SNAPSHOT_CDC (FULL_SNAPSHOT_CDC_NO_PK was removed in v1.4.0 -- snapshot CDC requires real primary_keys)',
    source_config_json     STRING COMMENT 'JSON: Auto Loader options, file_pattern, reader_options, zip/PGP handling (source_zip_handling.delete_source_after_extract is either the legacy boolean or an action object), ASN.1 schema ref, explode_columns (PRESENT-but-EMPTY means auto-flatten everything; ABSENT means schema-preserving pass-through), json_string_columns, remove_dups/dedup_watermark, column_normalization ({{enabled, case}} -- the legacy normalize_column_names boolean was removed in v1.4.0), data_standardization_sql, and landing_retention_policy (cloudFiles.cleanSource; archive_path is never required -- an empty one degrades to off)',
    target_config_json     STRING COMMENT 'JSON: storage_format, partition_columns/liquid_clustering_columns (an explicitly empty list means none; at most 3 clustering columns), table_properties, auto_ttl, encrypted_columns (each entry may declare source_data_type -- the original type of the column being encrypted; omitted means the type Spark observes at encryption time), empty_target_if_source_empty (TRUNCATE_AND_LOAD only -- default false, so a zero-record source never blanks the target), and every CDC setting (cdc_load_strategy, primary_keys, sequence_by_column, columns_to_check/exclude, cdc_operation_column/mapping, generate_hash_columns, sink_config). NOTE v1.4.0: generate_surrogate_key/surrogate_key_columns/surrogate_key_exclude_columns are REMOVED and rejected by onboarding, as is cdc_load_strategy FULL_SNAPSHOT_CDC_NO_PK -- snapshot CDC requires real primary_keys',
    dq_config_json         STRING COMMENT 'JSON: {{rules: [{{rule_id, expression, action}}], quarantine_table, record_id_column}}',
    governance_tags_json    STRING COMMENT 'JSON: {{column_tags: [{{column, tags: {{k: v}}}}], table_tags: {{k: v}}}} -- tags-only governance model',
    is_active               BOOLEAN NOT NULL COMMENT 'Soft-disable flag',
    created_at              TIMESTAMP NOT NULL COMMENT 'Row creation timestamp (UTC)',
    updated_at              TIMESTAMP NOT NULL COMMENT 'Last upsert timestamp (UTC)',
    last_processed_cdc_version BIGINT COMMENT 'Watermark (Phase 10, post_deployment.py::capture_all_scd_change_counts): Delta commit version of this flow''s target table already reflected in a prior SCD/CDC change-count capture. NULL means never captured (next capture reads table_changes from version 0). Never set by onboarding -- advanced only by the post-deployment governance job after a successful capture.',
    CONSTRAINT ingestion_flow_spec_pk PRIMARY KEY (dataflow_id)
)
USING DELTA
CLUSTER BY (dataflow_group_id, dataflow_id)
COMMENT 'One row per metadata-driven Bronze ingestion flow'
{render_table_properties_clause(table_properties)}
"""


def get_transformation_flow_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.transformation_flow_spec (
    flow_step_id           STRING NOT NULL COMMENT 'Unique identifier for this transformation step',
    dataflow_id            STRING NOT NULL COMMENT 'Logical flow identifier this step belongs to',
    dataflow_group_id      STRING NOT NULL COMMENT 'Parent dataflow group identifier',
    target_catalog         STRING NOT NULL COMMENT 'Target Unity Catalog catalog',
    target_schema          STRING NOT NULL COMMENT 'Target Unity Catalog schema',
    target_table           STRING NOT NULL COMMENT 'Target table name (Silver/Gold)',
    target_type            STRING NOT NULL COMMENT 'One of: streaming_table, materialized_view, batch_table, external_sink, sink',
    cdc_load_strategy      STRING NOT NULL COMMENT 'Denormalized copy of target_config.cdc_load_strategy: one of APPEND, TRUNCATE_AND_LOAD, SCD1, SCD2, SCD3, FULL_SNAPSHOT_CDC (FULL_SNAPSHOT_CDC_NO_PK was removed in v1.4.0 -- snapshot CDC requires real primary_keys)',
    source_inputs_json     STRING COMMENT 'JSON array of upstream inputs: {{input_name, table, is_streaming, watermark, decrypted_columns}}',
    transformation_sql     STRING NOT NULL COMMENT 'Native Spark SQL defining the transformation (joins, range-joins, aggregations, UNION/UNION ALL)',
    target_config_json     STRING COMMENT 'JSON: storage_format, partition_columns/liquid_clustering_columns, table_properties, auto_ttl, encrypted_columns (output-only; each entry may declare source_data_type), empty_target_if_source_empty, and every CDC setting -- see ingestion_flow_spec.target_config_json',
    dq_config_json         STRING COMMENT 'JSON: {{rules: [{{rule_id, expression, action}}], quarantine_table, record_id_column}}',
    governance_tags_json    STRING COMMENT 'JSON: {{column_tags: [...], table_tags: {{...}}}}',
    is_active               BOOLEAN NOT NULL COMMENT 'Soft-disable flag',
    created_at              TIMESTAMP NOT NULL COMMENT 'Row creation timestamp (UTC)',
    updated_at              TIMESTAMP NOT NULL COMMENT 'Last upsert timestamp (UTC)',
    last_processed_cdc_version BIGINT COMMENT 'Watermark (Phase 10, post_deployment.py::capture_all_scd_change_counts): Delta commit version of this flow''s target table already reflected in a prior SCD/CDC change-count capture. NULL means never captured (next capture reads table_changes from version 0). Never set by onboarding -- advanced only by the post-deployment governance job after a successful capture.',
    CONSTRAINT transformation_flow_spec_pk PRIMARY KEY (flow_step_id)
)
USING DELTA
CLUSTER BY (dataflow_group_id, dataflow_id)
COMMENT 'One row per metadata-driven Silver/Gold transformation flow'
{render_table_properties_clause(table_properties)}
"""


def get_onboarding_audit_log_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.onboarding_audit_log (
    audit_event_id       STRING NOT NULL COMMENT 'Unique identifier for this audit event (UUID)',
    dataflow_group_id     STRING COMMENT 'Dataflow group this onboarding action targeted',
    action_type           STRING NOT NULL COMMENT 'One of: CREATE, UPDATE, VALIDATE_ONLY',
    environment            STRING COMMENT 'Target environment resolved at onboarding time',
    onboarded_by           STRING COMMENT 'User or service principal that triggered onboarding',
    onboarded_at           TIMESTAMP NOT NULL COMMENT 'Timestamp the onboarding action was executed (UTC)',
    spec_version           STRING COMMENT 'Version/hash of the onboarding spec payload',
    client_context_json    STRING COMMENT 'JSON: client IP, hostname, user agent, CLI/SDK version, Git commit SHA',
    status                 STRING NOT NULL COMMENT 'One of: SUCCESS, FAILED',
    error_message          STRING COMMENT 'Error detail when status = FAILED',
    raw_spec_payload       STRING COMMENT 'Raw, template-substituted JSON spec payload as submitted',
    CONSTRAINT onboarding_audit_log_pk PRIMARY KEY (audit_event_id)
)
USING DELTA
CLUSTER BY (onboarded_at, dataflow_group_id)
COMMENT 'Perception / audit trail for metadata onboarding actions'
{render_table_properties_clause(table_properties)}
"""


def get_reconciliation_flow_spec_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.reconciliation_flow_spec (
    reconciliation_id      STRING NOT NULL COMMENT 'Unique identifier for this reconciliation flow',
    dataflow_group_id      STRING COMMENT 'Parent dataflow group identifier, if any',
    source_config_json     STRING NOT NULL COMMENT 'JSON: {{type: table (Delta tables ONLY as of v1.3.0 -- file/sink were dropped; read such output into a Delta table first), table, read_mode, task_run_id_column, filter_condition, data_standardization_sql, hash_precomputed}} for the baseline dataset',
    target_configs_json    STRING NOT NULL COMMENT 'JSON array of target objects: [{{target_id, type (table only), table, read_mode, task_run_id_column, filter_condition, data_standardization_sql, hash_precomputed, comparison_direction, append_target_table}}] -- one reconciliation flow can compare its source against multiple targets',
    match_keys_json        STRING NOT NULL COMMENT 'JSON array of column names identifying the same logical record across both datasets',
    compare_columns_json    STRING COMMENT 'JSON array of column names compared for drift once matched by key; null/empty means key-presence-only matching',
    transform_sql           STRING COMMENT 'Optional SQL reshaping the missing/unmatched record set before it is appended to a target-specific append_target_table (only when source and target schemas differ)',
    error_handling_json     STRING COMMENT 'JSON: {{on_failure: "fail"|"warn"}}',
    logging_config_json     STRING COMMENT 'JSON: {{run_log_capture: bool, mismatch_log_capture: bool}}, both default true -- when false, skips writing reconciliation_run_log/reconciliation_mismatch_log respectively for this flow (reconciliation_result is always written regardless). This is the ONBOARDED per-flow layer; it is overridden at run time by the recon_run_log_capture/recon_mismatch_log job parameters, which are tri-state (unset defers to this column). See onboarding/spec_validator.py and reconciliation/appender.py::resolve_log_capture_flags.',
    two_tier_verification   BOOLEAN COMMENT 'Default true when NULL. Gates the Phase 1 early-out: a cheap per-side (row_count, bit_xor of __framework_hash_key, bit_xor of __framework_hash_value) fingerprint that short-circuits the whole comparison when both sides match, leaving the full hash-key join and column-level discrepancy mapping (Phase 2) to run only on a reported difference. Set false for a flow that cannot tolerate the XOR fold''s documented pair-cancellation property -- see reconciliation/matcher.py.',
    is_active               BOOLEAN NOT NULL COMMENT 'Soft-disable flag',
    created_at              TIMESTAMP NOT NULL COMMENT 'Row creation timestamp (UTC)',
    updated_at              TIMESTAMP NOT NULL COMMENT 'Last upsert timestamp (UTC)',
    CONSTRAINT reconciliation_flow_spec_pk PRIMARY KEY (reconciliation_id)
)
USING DELTA
CLUSTER BY (reconciliation_id)
COMMENT 'One row per metadata-driven, Delta-table-to-Delta-table, possibly multi-target reconciliation flow (file/sink dataset types were dropped in v1.3.0 -- both sides must share one hash construction, one schema, and one restartability ledger, which a raw file/sink location cannot offer)'
{render_table_properties_clause(table_properties)}
"""


def get_reconciliation_run_log_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.reconciliation_run_log (
    run_id                    STRING NOT NULL COMMENT 'Unique identifier for this reconciliation run (UUID)',
    reconciliation_id          STRING NOT NULL COMMENT 'Reconciliation flow this run executed',
    target_id                  STRING NOT NULL COMMENT 'Which target_configs[] entry this row reports on -- one row per target per run',
    source_batch_fingerprint   STRING NOT NULL COMMENT 'sha256 of the unmatched-record set -- reruns with an identical fingerprint are idempotent no-ops (batch read_mode only). NOTE: v1.3.0 changed the framework''s canonical hash construction (per-column trim/lower normalization and the __NULL__ sentinel -- see cdc/hashing.py), so fingerprints written before the upgrade never match one written after; the first post-upgrade run therefore cannot short-circuit to SKIPPED_ALREADY_PROCESSED and re-evaluates instead.',
    source_record_count        BIGINT COMMENT 'Row count read from the source dataset',
    target_record_count        BIGINT COMMENT 'Row count read from this target dataset',
    matched_count               BIGINT COMMENT 'Records present and consistent in both datasets',
    missing_in_target_count     BIGINT COMMENT 'Records present in source, absent/drifted in this target (source_to_target direction)',
    missing_in_source_count     BIGINT COMMENT 'Records present in this target, absent from source (target_to_source direction, audit-only)',
    value_drift_count           BIGINT COMMENT 'Records matched by key but differing in one or more compared columns',
    appended_count              BIGINT COMMENT 'source_to_target misses actually appended this run (0 if this fingerprint was already processed)',
    failed_count                 BIGINT COMMENT 'Records that could not be compared/appended due to an error',
    status                     STRING NOT NULL COMMENT 'One of: SUCCESS, FAILED, SKIPPED_ALREADY_PROCESSED',
    error_message              STRING COMMENT 'Error detail when status = FAILED',
    task_run_id                STRING COMMENT 'Parent job run id ({{job.run_id}}/{{job.parameters.task_run_id}}), when this reconciliation ran as a task of a parent job -- null for a standalone run. Threaded through so every log row from one orchestrated run can be correlated. As of v1.3.0 it ALSO narrows the read of any side declaring a task_run_id_column -- with no task_run_id_column configured it stays correlation-only, exactly as before. (v1.4.0: reconciliation is triggered-only, so that narrowing is now unconditional; the recon_mode column that used to gate it is dropped from this DDL.) Only written when logging_config.run_log_capture is true (default) -- see onboarding/spec_validator.py',
    run_at                     TIMESTAMP NOT NULL COMMENT 'Timestamp this run executed (UTC)',
    CONSTRAINT reconciliation_run_log_pk PRIMARY KEY (run_id)
)
USING DELTA
CLUSTER BY (reconciliation_id, run_at)
COMMENT 'Restartability/idempotency ledger + metrics history for reconciliation runs, one row per target per run -- conditionally written when logging_config.run_log_capture is true (default); see reconciliation/appender.py and reconciliation_result for the always-written summary'
{render_table_properties_clause(table_properties)}
"""


def get_reconciliation_mismatch_log_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.reconciliation_mismatch_log (
    mismatch_id                 STRING NOT NULL COMMENT 'Unique identifier for this mismatch row (UUID)',
    run_id                      STRING NOT NULL COMMENT 'reconciliation_run_log.run_id this mismatch was detected during',
    reconciliation_id           STRING NOT NULL COMMENT 'Reconciliation flow this mismatch belongs to',
    target_id                   STRING NOT NULL COMMENT 'Which target_configs[] entry this mismatch was detected against',
    match_key_values_json       STRING NOT NULL COMMENT 'JSON object of match_keys column -> value identifying the offending record',
    mismatch_type                STRING NOT NULL COMMENT 'One of: MISSING_IN_TARGET, MISSING_IN_SOURCE, VALUE_DRIFT',
    differing_columns_json       STRING COMMENT 'JSON array of {{column, source_value, target_value}} -- populated only for VALUE_DRIFT',
    source_hash_value            STRING COMMENT '__framework_hash_value on the source side, if available',
    target_hash_value            STRING COMMENT '__framework_hash_value on the target side, if available',
    task_run_id                  STRING COMMENT 'Same parent job run id as the owning reconciliation_run_log row, if any -- see that table''s column comment. Only written when logging_config.mismatch_log_capture is true (default) -- see onboarding/spec_validator.py',
    detected_at                  TIMESTAMP NOT NULL COMMENT 'Timestamp this mismatch was detected (UTC)',
    CONSTRAINT reconciliation_mismatch_log_pk PRIMARY KEY (mismatch_id)
)
USING DELTA
CLUSTER BY (reconciliation_id, target_id, run_id)
COMMENT 'Per-record mismatch detail backing reconciliation_run_log''s aggregate counts -- conditionally written when logging_config.mismatch_log_capture is true (default); see reconciliation/mismatch_logging.py'
{render_table_properties_clause(table_properties)}
"""


def get_reconciliation_result_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.reconciliation_result (
    result_id                  STRING NOT NULL COMMENT 'Unique identifier for this result row (UUID)',
    run_id                      STRING NOT NULL COMMENT 'reconciliation_run_log.run_id this result summarizes, whether or not that row was itself written (see logging_config.run_log_capture)',
    reconciliation_id           STRING NOT NULL COMMENT 'Reconciliation flow this run executed',
    target_id                   STRING NOT NULL COMMENT 'Which target_configs[] entry this row reports on -- one row per target per run',
    task_run_id                 STRING COMMENT 'Parent job run id ({{job.run_id}}/{{job.parameters.task_run_id}}), when this reconciliation ran as a task of a parent job -- null for a standalone run',
    status                      STRING NOT NULL COMMENT 'One of: SUCCESS, FAILED, SKIPPED_ALREADY_PROCESSED',
    matched_count                BIGINT COMMENT 'Records present and consistent in both datasets',
    missing_in_target_count      BIGINT COMMENT 'Records present in source, absent/drifted in this target (source_to_target direction)',
    missing_in_source_count      BIGINT COMMENT 'Records present in this target, absent from source (target_to_source direction, audit-only)',
    value_drift_count            BIGINT COMMENT 'Records matched by key but differing in one or more compared columns',
    run_at                      TIMESTAMP NOT NULL COMMENT 'Timestamp this run executed (UTC)',
    CONSTRAINT reconciliation_result_pk PRIMARY KEY (result_id)
)
USING DELTA
CLUSTER BY (reconciliation_id, run_at)
COMMENT 'Always-populated pass/fail summary for every reconciliation run, independent of logging_config -- deliberately lighter than reconciliation_run_log (no source/target row counts, no fingerprint, no error detail) so it stays cheap to write even for a high-frequency continuous flow; see reconciliation/appender.py'
{render_table_properties_clause(table_properties)}
"""


def get_observability_config_ddl(control_schema: str, table_properties: Dict[str, str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {control_schema}.observability_config (
    config_id            STRING NOT NULL COMMENT 'Unique identifier for this config row (UUID)',
    dataflow_group_id     STRING NOT NULL COMMENT 'Parent dataflow group identifier this destination applies to (same scoping as ingestion_flow_spec/transformation_flow_spec/reconciliation_flow_spec), or literal * for a global fallback applied to every group with no group-specific row for the same destination_id',
    destination_id         STRING NOT NULL COMMENT 'Unique identifier for this destination, e.g. dest-dbx-prod-volume, dest-datadog-otlp-http',
    enabled                 BOOLEAN NOT NULL COMMENT 'Active/inactive flag -- disabled rows are skipped by ConfigLoader without deleting the row',
    destination_type        STRING NOT NULL COMMENT 'One of: DATABRICKS_VOLUME, OTLP_CONSUMER',
    mode                    STRING COMMENT 'triggered (default when NULL -- provisioned before v1.3.0) or continuous. triggered destinations are served by the bounded post-update engine (notebooks/08_observability); continuous destinations by the always-on streaming pipeline (notebooks/06_observability_streaming), whose event_log_tables live in destination_config_json.',
    destination_config_json STRING NOT NULL COMMENT 'JSON object: volume_path/compression/file_format for DATABRICKS_VOLUME; endpoint/protocol/compression/resource_attributes for OTLP_CONSUMER; plus event_log_tables (a non-empty array of fully-qualified catalog.schema.event_log_table names) for either type when mode is continuous -- the streaming pipeline has no upstream task to resolve a pipeline from, so it must be told what to stream',
    auth_config_json         STRING COMMENT 'JSON object: {{type: BEARER_TOKEN|API_KEY|BASIC_AUTH|NONE, credentials: {{...}}}} -- credential values are env:<VAR_NAME> or secret:<scope>:<key> references, never literal secrets. Null/omitted for DATABRICKS_VOLUME (uses native UC permissions, no auth needed)',
    retry_config_json         STRING COMMENT 'JSON object: {{max_attempts, backoff_multiplier, timeout_ms}} -- OTLP_CONSUMER only; defaults applied by DestinationDispatcher when null',
    created_at                TIMESTAMP NOT NULL COMMENT 'Row creation timestamp (UTC)',
    updated_at                TIMESTAMP NOT NULL COMMENT 'Last upsert timestamp (UTC)',
    CONSTRAINT observability_config_pk PRIMARY KEY (config_id)
)
USING DELTA
CLUSTER BY (dataflow_group_id, destination_id)
COMMENT 'Per-dataflow-group (or global "*" fallback) telemetry destination configuration for the DLT observability engine, populated by onboarding/metadata_upsert.py::upsert_observability_config from the same onboarding spec as every other control table -- see observability/config_loader.py for read access'
{render_table_properties_clause(table_properties)}
"""


def get_all_control_table_ddls(control_schema: str, table_properties: Dict[str, str]) -> List[Tuple[str, str]]:
    """Return ``(description, ddl)`` pairs for all control tables, in creation order."""
    return [
        (f"create table {control_schema}.dataflow_group_spec", get_dataflow_group_spec_ddl(control_schema, table_properties)),
        (f"create table {control_schema}.ingestion_flow_spec", get_ingestion_flow_spec_ddl(control_schema, table_properties)),
        (
            f"create table {control_schema}.transformation_flow_spec",
            get_transformation_flow_spec_ddl(control_schema, table_properties),
        ),
        (f"create table {control_schema}.onboarding_audit_log", get_onboarding_audit_log_ddl(control_schema, table_properties)),
        (
            f"create table {control_schema}.reconciliation_flow_spec",
            get_reconciliation_flow_spec_ddl(control_schema, table_properties),
        ),
        (
            f"create table {control_schema}.reconciliation_run_log",
            get_reconciliation_run_log_ddl(control_schema, table_properties),
        ),
        (
            f"create table {control_schema}.reconciliation_mismatch_log",
            get_reconciliation_mismatch_log_ddl(control_schema, table_properties),
        ),
        (
            f"create table {control_schema}.reconciliation_result",
            get_reconciliation_result_ddl(control_schema, table_properties),
        ),
        (f"create table {control_schema}.observability_config", get_observability_config_ddl(control_schema, table_properties)),
    ]


def get_preflight_function_ddl(control_schema: str, onboarding_spec_schema_json: str) -> str:
    """Build the ``CREATE OR REPLACE FUNCTION`` statement for ``preflight_check_onboarding_spec``,
    a Unity Catalog Python Function callable directly via SQL -- by Genie, a Mosaic AI Agent, or
    any MCP tool-calling loop -- with no Python host process required.

    This is "Option B" from ``onboarding/uc_spec_preflight.py``'s own module docstring: a
    **structural-only** subset of that module's full preflight tool. The UC Python function
    sandbox cannot import this repo's installed package or reach a live Spark session, so
    rather than hand-duplicating ``spec_validator.py``'s ~1000 lines of business rules inside a
    SQL string (exactly what that module's docstring warns against), the function body instead
    validates the parsed spec against ``onboarding_spec.schema.json`` --
    the same committed, `jsonschema`-checked JSON Schema mirror of the validator that
    ``onboarding_templates/`` and ``docs/01_control_metadata_schema.md`` already point to as the
    machine-readable spec shape. ``onboarding_spec_schema_json`` is the exact text of that file,
    read by the caller, base64-encoded, and embedded into the deployed function body as a single
    line -- deliberately *not* embedded as a literal multi-KB string spanning hundreds of lines:
    the SQL ``AS $$ ... $$`` dollar-quoted body and a literal-embedded Python raw string are two
    independent, uncontrolled-length-text-sensitive layers, and a large JSON document (backslash
    escapes, arbitrary line count) round-tripping through both intact is not a safe assumption to
    build on. Base64 has no characters either layer treats specially, so this sidesteps that
    class of bug entirely rather than trying to prove the schema text is "safe" to embed raw. The
    two never drift silently out of sync -- redeploying this DDL after editing the schema file
    picks up the change automatically.

    For the complete tool (live Unity Catalog existence checks, real ``spec_validator.py``
    parity, ``transformation_sql``/``transform_sql`` syntax checking), call
    ``NextGen_Metadata_Framework.lakeflow_framework.onboarding.uc_spec_preflight.preflight_check_onboarding_spec``
    from a Python/Spark host process instead (Option A) -- this UC function is deliberately the
    lighter-weight, SQL/Genie-reachable sibling of that tool, not a replacement for it.

    Parameters
    ----------
    control_schema:
        Fully-qualified target schema, e.g. ``"metaflow.config"``.
    onboarding_spec_schema_json:
        Full text of ``onboarding_templates/onboarding_spec.schema.json`` (any valid UTF-8 JSON
        text works -- it is base64-encoded before embedding, so no escaping constraints apply).
    """
    schema_b64 = base64.b64encode(onboarding_spec_schema_json.encode("utf-8")).decode("ascii")

    function_name = f"{control_schema}.preflight_check_onboarding_spec"
    python_body = '''import base64
import json
import yaml
from jsonschema import Draft202012Validator

_SCHEMA_B64 = "''' + schema_b64 + '''"
_SCHEMA = json.loads(base64.b64decode(_SCHEMA_B64).decode("utf-8"))
_VALIDATOR = Draft202012Validator(_SCHEMA)
_NOTE = (
    "Structural-only check (Unity Catalog Python function sandbox) against "
    "onboarding_spec.schema.json (JSON Schema draft 2020-12) -- no live Unity Catalog "
    "existence checks, no transformation_sql/transform_sql syntax checking, and the "
    "documented schema-vs-validator gaps in that schema file's own $comment still apply "
    "(cross-array uniqueness constraints, live EXPLAIN-based SQL checks, etc.). For the "
    "complete tool, call NextGen_Metadata_Framework.lakeflow_framework.onboarding."
    "uc_spec_preflight.preflight_check_onboarding_spec from a Python/Spark host process."
)

if not isinstance(spec_json_or_yaml_text, str) or not spec_json_or_yaml_text.strip():
    return json.dumps({
        "valid": False,
        "validation_errors": ["spec_json_or_yaml_text: is required but was missing or empty"],
        "existence_checks": [],
        "summary": "No spec text was provided -- nothing to check.",
        "note": _NOTE,
    })

catalog_name = catalog.strip() if isinstance(catalog, str) else ""
substituted = spec_json_or_yaml_text.replace("{{catalog}}", catalog_name)

parsed = None
parse_error = None
try:
    parsed = json.loads(substituted)
except Exception as json_exc:
    try:
        parsed = yaml.safe_load(substituted)
    except Exception as yaml_exc:
        parse_error = f"neither valid JSON ({json_exc}) nor valid YAML ({yaml_exc})"

if parse_error is not None or not isinstance(parsed, dict):
    return json.dumps({
        "valid": False,
        "validation_errors": [f"Spec text could not be parsed: {parse_error or 'top-level value is not a JSON/YAML object'}"],
        "existence_checks": [],
        "summary": "Parse error -- fix the syntax before onboarding.",
        "note": _NOTE,
    })

schema_errors = sorted(_VALIDATOR.iter_errors(parsed), key=lambda e: list(e.path))
validation_errors = [f"{'/'.join(str(p) for p in e.path) or '<root>'}: {e.message}" for e in schema_errors]
is_valid = len(validation_errors) == 0

return json.dumps({
    "valid": is_valid,
    "validation_errors": validation_errors,
    "existence_checks": [],
    "summary": (
        "Spec is schema-valid (structural-only check)."
        if is_valid
        else f"{len(validation_errors)} structural validation error(s) found -- see validation_errors."
    ),
    "note": _NOTE,
})'''

    return (
        f"CREATE OR REPLACE FUNCTION {function_name}(\n"
        "  spec_json_or_yaml_text STRING COMMENT 'Full onboarding spec, as JSON or YAML text (may contain an unresolved {{catalog}} token)',\n"
        "  catalog STRING COMMENT 'Target Unity Catalog name substituted for any {{catalog}} token in the spec text'\n"
        ")\n"
        "RETURNS STRING\n"
        "LANGUAGE PYTHON\n"
        "COMMENT 'Structural preflight validation for a Metaflow onboarding spec against onboarding_spec.schema.json -- callable by Genie/MCP tool-calling agents before onboarding. Structural-only: no live Unity Catalog existence checks. For the complete tool call uc_spec_preflight.preflight_check_onboarding_spec from Python instead.'\n"
        "ENVIRONMENT (dependencies = '[\"jsonschema==4.23.0\", \"pyyaml==6.0.2\"]', environment_version = 'None')\n"
        "AS $$\n"
        f"{python_body}\n"
        "$$"
    )
