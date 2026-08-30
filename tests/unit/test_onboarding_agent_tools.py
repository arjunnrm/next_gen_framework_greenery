"""Unit tests for onboarding agent tools (validate_json, onboard_entity, get_catalog_schema_parameters)."""

import json
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.agent_tools import (
    validate_json,
    onboard_entity,
    get_catalog_schema_parameters,
)


def test_validate_json_valid_spec():
    valid_spec = json.dumps({
        "dataflow_group_id": "dfg_orders_cdc",
        "ingestion_flows": [
            {
                "dataflow_id": "df_raw_orders",
                "source_type": "autoloader",
                "target_catalog": "{{catalog}}",
                "target_schema": "bronze",
                "target_table": "raw_orders",
                "target_type": "streaming_table",
                "source_config": {
                    "path": "/Volumes/{{catalog}}/landing/orders",
                    "format": "csv"
                },
                "target_config": {
                    "cdc_load_strategy": "APPEND"
                },
                "governance_tags": {
                    "table_tags": {
                        "cost_center": "CC-1234-ORDERS",
                        "classification": "internal",
                        "sla": "bronze_hourly"
                    }
                }
            }
        ]
    })
    result = validate_json(valid_spec, catalog="poc", env="dev", strict_mode=True)
    assert result["valid"] is True
    assert result["error_count"] == 0


def test_validate_json_invalid_syntax():
    invalid_syntax = "{ dataflow_group_id: broken json "
    result = validate_json(invalid_syntax)
    assert result["valid"] is False
    assert result["error_count"] > 0
    assert "syntax_error" in result["errors"][0]


def test_validate_json_invalid_cdc_strategy_in_ingestion():
    # SCD3 is transformation-only
    invalid_spec = json.dumps({
        "dataflow_group_id": "dfg_invalid_scd3",
        "ingestion_flows": [
            {
                "dataflow_id": "df_test",
                "source_type": "autoloader",
                "target_catalog": "poc",
                "target_schema": "bronze",
                "target_table": "raw_test",
                "target_type": "streaming_table",
                "source_config": { "path": "/Volumes/poc/test/data" },
                "target_config": {
                    "cdc_load_strategy": "SCD3"
                }
            }
        ]
    })
    result = validate_json(invalid_spec, catalog="poc", env="dev")
    assert result["valid"] is False
    assert any("SCD3" in err for err in result["errors"])


def test_onboard_entity_create_and_dry_run():
    valid_spec = json.dumps({
        "dataflow_group_id": "dfg_orders_cdc",
        "ingestion_flows": [
            {
                "dataflow_id": "df_raw_orders",
                "source_type": "autoloader",
                "target_catalog": "poc",
                "target_schema": "bronze",
                "target_table": "raw_orders",
                "target_type": "streaming_table",
                "source_config": { "path": "/Volumes/poc/test/data", "format": "csv" },
                "target_config": { "cdc_load_strategy": "APPEND" }
            }
        ]
    })
    # Dry run
    dry_run_res = onboard_entity(valid_spec, action_type="create", catalog="poc", environment="dev", dry_run=True)
    assert dry_run_res["success"] is True, f"Failed with: {dry_run_res.get('message')}"
    assert dry_run_res["action_performed"] == "DRY_RUN"
    assert "dataflow_group_spec" in dry_run_res["affected_tables"][0]

    # Live create
    live_res = onboard_entity(valid_spec, action_type="create", catalog="poc", environment="dev", dry_run=False)
    assert live_res["success"] is True
    assert live_res["action_performed"] == "CREATE"


def test_get_catalog_schema_parameters():
    meta = get_catalog_schema_parameters(
        catalog_name="poc",
        schema_name="silver_finance",
        table_name="orders_dim"
    )
    assert meta["catalog"] == "poc"
    assert meta["schema"] == "silver_finance"
    assert len(meta["tables"]) == 1
    assert meta["tables"][0]["table_name"] == "orders_dim"
    assert len(meta["tables"][0]["columns"]) > 0
    assert "cost_center" in meta["tables"][0]["tags"]
