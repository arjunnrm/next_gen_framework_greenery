"""Unit tests for control_plane/ddl_definitions.py's observability_config DDL -- pure string
building, no Spark session."""

from NextGen_Metadata_Framework.lakeflow_framework.control_plane.ddl_definitions import (
    get_all_control_table_ddls,
    get_observability_config_ddl,
)


def test_observability_config_ddl_creates_expected_table_with_pk():
    ddl = get_observability_config_ddl("poc.config", {})
    assert "CREATE TABLE IF NOT EXISTS poc.config.observability_config" in ddl
    assert "CONSTRAINT observability_config_pk PRIMARY KEY (config_id)" in ddl
    for column in (
        "config_id", "dataflow_group_id", "destination_id", "enabled", "destination_type",
        "destination_config_json", "auth_config_json", "retry_config_json", "created_at", "updated_at",
    ):
        assert column in ddl


def test_observability_config_ddl_applies_table_properties():
    ddl = get_observability_config_ddl("poc.config", {"delta.autoOptimize.optimizeWrite": "true"})
    assert "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')" in ddl


def test_all_control_table_ddls_includes_observability_config():
    ddls = get_all_control_table_ddls("poc.config", {})
    descriptions = [description for description, _ in ddls]
    assert "create table poc.config.observability_config" in descriptions
