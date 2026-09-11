"""Unit tests for onboarding/uc_spec_preflight.py.

Consistent with test_spec_validator.py's own convention (see its module docstring),
``_acquire_spark_session`` is patched instead of exercising a real Spark/Databricks Connect
session: patched to ``None`` for specs with no ``transformation_sql``/``transform_sql`` at
all (safe -- spec_validator never touches Spark for those), and to a ``MagicMock`` whose
``.sql(...).collect()`` returns ``[]`` (an empty EXPLAIN plan -- never a "planning error")
for specs that do, so the Spark-backed SQL-syntax check is a deterministic no-op rather than
a source of test flakiness or a spurious validation error.

``_get_workspace_client`` is patched to a ``MagicMock`` configured to raise
``databricks.sdk.errors.NotFound`` for a caller-chosen set of "missing" names and to return a
plain dummy object for everything else -- this repo has no prior WorkspaceClient-mocking
precedent to follow (``tests/conftest.py``'s own ``table_exists``/``volume_exists`` fixtures
call the real SDK against a live workspace), so this mirrors the closest established pattern
in this repo instead: patching one well-named seam with ``unittest.mock`` (see
``test_asn1_partition_decoder.py``'s ``patch("module.path.symbol", ...)`` usage).
"""

import json
from unittest.mock import MagicMock, patch

from databricks.sdk.errors import NotFound

from flowx.lakeflow_framework.onboarding.uc_spec_preflight import (
    preflight_check_onboarding_spec,
)

MODULE = "flowx.lakeflow_framework.onboarding.uc_spec_preflight"


def _no_spark():
    """Patch context: no Spark session available at all (safe for specs with no SQL)."""
    return patch(f"{MODULE}._acquire_spark_session", return_value=None)


def _stub_spark():
    """Patch context: a Spark stand-in whose EXPLAIN always resolves cleanly (empty plan)."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = []
    return patch(f"{MODULE}._acquire_spark_session", return_value=mock_spark)


def _mock_workspace_client(missing: set):
    """Patch context: a WorkspaceClient stand-in where every (kind, name) pair in `missing`
    raises NotFound, and everything else succeeds."""
    client = MagicMock()

    def _catalogs_get(name, **kwargs):
        if ("catalog", name) in missing:
            raise NotFound(f"catalog '{name}' not found")
        return MagicMock()

    def _schemas_get(full_name, **kwargs):
        if ("schema", full_name) in missing:
            raise NotFound(f"schema '{full_name}' not found")
        return MagicMock()

    def _tables_get(full_name, **kwargs):
        if ("table", full_name) in missing:
            raise NotFound(f"table '{full_name}' not found")
        return MagicMock()

    def _volumes_read(name, **kwargs):
        if ("volume", name) in missing:
            raise NotFound(f"volume '{name}' not found")
        return MagicMock()

    client.catalogs.get.side_effect = _catalogs_get
    client.schemas.get.side_effect = _schemas_get
    client.tables.get.side_effect = _tables_get
    client.volumes.read.side_effect = _volumes_read
    return patch(f"{MODULE}._get_workspace_client", return_value=client)


def _valid_ingestion_spec():
    return {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            {
                "dataflow_id": "df_test",
                "source_type": "autoloader",
                "target_catalog": "poc",
                "target_schema": "bronze_test",
                "target_table": "test_raw",
                "target_type": "streaming_table",
                "source_config": {
                    "path": "/Volumes/poc/landing/raw_zone/incoming/",
                    "format": "csv",
                    "schema_location": "/Volumes/poc/landing/_schemas/test_raw/",
                },
                "target_config": {"cdc_load_strategy": "APPEND"},
                "dq_config": {},
                "governance_tags": {},
            }
        ],
    }


def test_valid_spec_reports_valid_true_with_no_errors():
    spec = _valid_ingestion_spec()
    with _no_spark(), _mock_workspace_client(missing=set()):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(spec), "poc"))

    assert report["valid"] is True
    assert report["validation_errors"] == []
    assert not any(check["status"] == "MISSING_REQUIRED" for check in report["existence_checks"])
    kinds_and_statuses = {(c["kind"], c["path"]): c["status"] for c in report["existence_checks"]}
    assert kinds_and_statuses[("table", "poc.bronze_test.test_raw")] == "EXISTS"
    assert kinds_and_statuses[("schema", "poc.bronze_test")] == "EXISTS"
    assert kinds_and_statuses[("catalog", "poc")] == "EXISTS"
    assert kinds_and_statuses[("volume", "poc.landing.raw_zone")] == "EXISTS"
    assert kinds_and_statuses[("volume", "poc.landing._schemas")] == "EXISTS"


def test_valid_spec_reports_new_targets_as_will_be_created_not_an_error():
    """A target table/schema/catalog that doesn't exist yet is informational, never an error --
    onboarding routinely creates brand-new targets."""
    spec = _valid_ingestion_spec()
    missing = {
        ("catalog", "poc"),
        ("schema", "poc.bronze_test"),
        ("table", "poc.bronze_test.test_raw"),
        ("volume", "poc.landing.raw_zone"),
        ("volume", "poc.landing._schemas"),
    }
    with _no_spark(), _mock_workspace_client(missing=missing):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(spec), "poc"))

    assert report["valid"] is True
    assert report["validation_errors"] == []
    statuses = {c["status"] for c in report["existence_checks"]}
    assert statuses == {"WILL_BE_CREATED"}


def test_invalid_spec_reports_valid_false_with_structural_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            {
                "dataflow_id": "df_bad",
                "source_type": "autoloadr",  # typo -- not an allowed source_type
                "target_catalog": "poc",
                "target_schema": "bronze_test",
                "target_table": "test_raw",
                "target_type": "streaming_table",
                "source_config": {"path": "/x/", "format": "csv", "schema_location": "/y/", "capture_technical_metadata": "yes"},
                "target_config": {"cdc_load_strategy": "APPEND"},
            }
        ],
    }
    with _no_spark(), _mock_workspace_client(missing=set()):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(spec), "poc"))

    assert report["valid"] is False
    assert any("source_type" in e and "autoloadr" in e for e in report["validation_errors"])
    assert any("capture_technical_metadata" in e and "boolean" in e for e in report["validation_errors"])


def test_missing_dataflow_group_id_is_reported_as_invalid():
    spec = {"ingestion_flows": []}
    with _no_spark(), _mock_workspace_client(missing=set()):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(spec), "poc"))

    assert report["valid"] is False
    assert any("dataflow_group_id" in e for e in report["validation_errors"])


def test_transformation_flow_missing_source_table_is_missing_required():
    """The one hard-failure case: a transformation flow's source_inputs.table that neither
    already exists nor is produced by another flow in this same spec."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "transformation_flows": [
            {
                "flow_step_id": "ts_test",
                "dataflow_id": "df_test",
                "target_catalog": "poc",
                "target_schema": "silver_test",
                "target_table": "test_silver",
                "target_type": "streaming_table",
                "source_inputs": [{"input_name": "test_input", "table": "poc.bronze_test.does_not_exist", "is_streaming": True}],
                "transformation_sql": "SELECT * FROM test_input",
                "target_config": {"cdc_load_strategy": "APPEND"},
                "dq_config": {},
                "governance_tags": {},
            }
        ],
    }
    missing = {("table", "poc.bronze_test.does_not_exist")}
    with _stub_spark(), _mock_workspace_client(missing=missing):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(spec), "poc"))

    assert report["valid"] is False
    assert report["validation_errors"] == []
    matches = [c for c in report["existence_checks"] if c["kind"] == "table" and c["path"] == "poc.bronze_test.does_not_exist"]
    assert len(matches) == 1
    assert matches[0]["status"] == "MISSING_REQUIRED"


def test_transformation_flow_source_table_created_earlier_in_same_spec_is_not_missing_required():
    """A transformation flow's source_inputs.table that IS another flow's own target in this
    same spec resolves polymorphically within the pipeline graph -- not existing in Unity
    Catalog yet (it hasn't run yet) is completely normal, never an error."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            {
                "dataflow_id": "df_raw",
                "source_type": "autoloader",
                "target_catalog": "poc",
                "target_schema": "bronze_test",
                "target_table": "raw",
                "target_type": "streaming_table",
                "source_config": {"path": "/Volumes/poc/landing/x/", "format": "csv", "schema_location": "/Volumes/poc/landing/_schemas/raw/"},
                "target_config": {"cdc_load_strategy": "APPEND"},
                "dq_config": {},
                "governance_tags": {},
            }
        ],
        "transformation_flows": [
            {
                "flow_step_id": "ts_test",
                "dataflow_id": "df_raw",
                "target_catalog": "poc",
                "target_schema": "silver_test",
                "target_table": "test_silver",
                "target_type": "streaming_table",
                "source_inputs": [{"input_name": "test_input", "table": "poc.bronze_test.raw", "is_streaming": True}],
                "transformation_sql": "SELECT * FROM test_input",
                "target_config": {"cdc_load_strategy": "APPEND"},
                "dq_config": {},
                "governance_tags": {},
            }
        ],
    }
    # Nothing in Unity Catalog exists yet at all -- this is a brand-new onboarding.
    missing = {
        ("catalog", "poc"),
        ("schema", "poc.bronze_test"),
        ("schema", "poc.silver_test"),
        ("table", "poc.bronze_test.raw"),
        ("table", "poc.silver_test.test_silver"),
        ("volume", "poc.landing.x"),
        ("volume", "poc.landing._schemas"),
    }
    with _stub_spark(), _mock_workspace_client(missing=missing):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(spec), "poc"))

    assert report["valid"] is True
    assert report["validation_errors"] == []
    raw_table_check = next(c for c in report["existence_checks"] if c["kind"] == "table" and c["path"] == "poc.bronze_test.raw")
    assert raw_table_check["status"] == "WILL_BE_CREATED"
    assert not any(c["status"] == "MISSING_REQUIRED" for c in report["existence_checks"])


def test_yaml_spec_text_is_parsed_and_validated_same_as_json():
    yaml_text = """
dataflow_group_id: dfg_test
ingestion_flows:
  - dataflow_id: df_test
    source_type: autoloader
    target_catalog: poc
    target_schema: bronze_test
    target_table: test_raw
    target_type: streaming_table
    source_config:
      path: /Volumes/poc/landing/raw_zone/incoming/
      format: csv
      schema_location: /Volumes/poc/landing/_schemas/test_raw/
    target_config:
      cdc_load_strategy: APPEND
"""
    with _no_spark(), _mock_workspace_client(missing=set()):
        report = json.loads(preflight_check_onboarding_spec(yaml_text, "poc"))

    assert report["valid"] is True
    assert report["validation_errors"] == []


def test_malformed_text_reports_invalid_with_parse_error_not_a_crash():
    with _no_spark(), _mock_workspace_client(missing=set()):
        report = json.loads(preflight_check_onboarding_spec("{not valid json or yaml: [", "poc"))

    assert report["valid"] is False
    assert report["existence_checks"] == []
    assert len(report["validation_errors"]) == 1
    assert "neither valid JSON" in report["validation_errors"][0]


def test_empty_spec_text_reports_invalid_without_crashing():
    report = json.loads(preflight_check_onboarding_spec("", "poc"))
    assert report["valid"] is False
    assert "spec_json_or_yaml_text" in report["validation_errors"][0]


def test_missing_catalog_argument_reports_invalid_without_crashing():
    report = json.loads(preflight_check_onboarding_spec(json.dumps(_valid_ingestion_spec()), ""))
    assert report["valid"] is False
    assert "catalog" in report["validation_errors"][0]


def test_catalog_placeholder_token_is_substituted_before_parsing():
    spec = _valid_ingestion_spec()
    spec["ingestion_flows"][0]["target_catalog"] = "{{catalog}}"
    spec["ingestion_flows"][0]["source_config"]["path"] = "/Volumes/{{catalog}}/landing/raw_zone/incoming/"
    spec_text = json.dumps(spec)

    with _no_spark(), _mock_workspace_client(missing=set()):
        report = json.loads(preflight_check_onboarding_spec(spec_text, "poc"))

    assert report["valid"] is True
    paths = {c["path"] for c in report["existence_checks"]}
    assert "poc.bronze_test.test_raw" in paths
    assert not any("{{catalog}}" in path for path in paths)


def test_no_workspace_client_available_skips_existence_checks_without_crashing():
    spec = _valid_ingestion_spec()
    with _no_spark(), patch(f"{MODULE}._get_workspace_client", return_value=None):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(spec), "poc"))

    assert report["existence_checks"] == []
    assert "existence checks were skipped" in report["summary"]
    # Structural validation is independent of Unity Catalog reachability.
    assert report["validation_errors"] == []


# ---------------------------------------------------------------------------------------------
# append_schema -- the healing append's shape versus append_target_table's existing columns
# ---------------------------------------------------------------------------------------------


def _mock_workspace_client_with_columns(columns_by_table):
    """Like ``_mock_workspace_client`` (nothing missing) but ``tables.get(name).columns`` returns
    real column objects for the tables named in ``columns_by_table`` -- which is what
    ``_table_columns`` reads for the append_schema check."""
    from types import SimpleNamespace

    client = MagicMock()

    def _tables_get(full_name, **kwargs):
        if full_name in columns_by_table:
            return SimpleNamespace(columns=[SimpleNamespace(name=n) for n in columns_by_table[full_name]])
        return MagicMock()

    client.tables.get.side_effect = _tables_get
    return patch(f"{MODULE}._get_workspace_client", return_value=client)


def _job_mode_healing_recon_spec(transform_sql=None):
    """A job-mode healing reconciliation flow -- job mode so the V-CYC placement rules stay out of
    the way and the append_schema check is what the test is actually observing."""
    flow = {
        "reconciliation_id": "recon_heal",
        "source_config": {"type": "table", "table": "poc.staging.customer_batch"},
        "target_configs": [
            {
                "target_id": "bronze_customer",
                "type": "table",
                "table": "poc.bronze.customer",
                "append_target_table": "poc.staging.cdc_landing",
            }
        ],
        "match_keys": ["customer_id"],
        "compare_columns": ["status"],
        "error_handling": {"on_failure": "warn"},
    }
    if transform_sql is not None:
        flow["transform_sql"] = transform_sql
    return {"dataflow_group_id": "dfg_test", "reconciliation_flows": [flow]}


#: A flat source, and an append target with a completely different (Debezium-envelope) shape.
_APPEND_SHAPE_COLUMNS = {
    "poc.staging.customer_batch": {"customer_id", "status", "sys_update_date"},
    "poc.staging.cdc_landing": {"destination", "key", "value", "operation"},
}


def test_append_schema_reports_a_mismatch_when_the_miss_set_is_appended_as_is():
    """Without ``transform_sql`` the appended shape IS the source's columns (plus the always
    synthesized ``__framework_hash_key``), so an ``append_target_table`` lacking them is a
    genuine ``SCHEMA_MISMATCH`` -- the pre-existing behaviour, pinned so v1.7.11's new branch
    cannot quietly swallow the real finding too."""
    with _no_spark(), _mock_workspace_client_with_columns(_APPEND_SHAPE_COLUMNS):
        report = json.loads(preflight_check_onboarding_spec(json.dumps(_job_mode_healing_recon_spec()), "poc"))

    append_checks = [c for c in report["existence_checks"] if c["kind"] == "append_schema"]
    assert len(append_checks) == 1, append_checks
    assert append_checks[0]["status"] == "SCHEMA_MISMATCH"
    assert "__framework_hash_key" in append_checks[0]["note"]
    assert "customer_id" in append_checks[0]["note"]


def test_append_schema_defers_to_transform_sql_instead_of_reporting_a_false_mismatch():
    """v1.7.11: a flow that reshapes its miss set with ``transform_sql`` appends that SQL's SELECT
    list, not the source's columns -- so comparing the two flagged every correctly written
    reshaping flow (UC3's batch lane heals a flat staging table into the 9-column Debezium
    landing table). The check must say the shape is defined elsewhere rather than judge it."""
    sql = (
        "SELECT 'topic' AS destination, CAST(customer_id AS STRING) AS `key`, status AS `value`, "
        "'read' AS operation FROM _reconciliation_unmatched_records"
    )
    with _stub_spark(), _mock_workspace_client_with_columns(_APPEND_SHAPE_COLUMNS):
        report = json.loads(
            preflight_check_onboarding_spec(json.dumps(_job_mode_healing_recon_spec(transform_sql=sql)), "poc")
        )

    assert report["valid"] is True
    assert report["validation_errors"] == []
    append_checks = [c for c in report["existence_checks"] if c["kind"] == "append_schema"]
    assert len(append_checks) == 1, append_checks
    assert append_checks[0]["status"] == "SHAPE_DEFINED_BY_TRANSFORM_SQL"
    assert append_checks[0]["path"] == "poc.staging.cdc_landing"
    assert "transform_sql" in append_checks[0]["note"]
    assert not any(c["status"] == "SCHEMA_MISMATCH" for c in report["existence_checks"])
