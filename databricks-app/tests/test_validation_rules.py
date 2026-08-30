import pytest
from server.core.registry import RegistryManager
from server.core.validator import SpecValidator


def test_one_flow_array_required_rule():
    reg = RegistryManager()
    val = SpecValidator(reg)

    # Empty spec with no flows should fail
    empty_doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test"}},
        "ingestion_flows": [],
        "transformation_flows": [],
        "reconciliation_flows": []
    }
    res = val.validate_spec(empty_doc)
    assert not res["ok"]
    rule_ids = [e["rule_id"] for e in res["errors"]]
    assert "one_flow_array_required" in rule_ids

    # Adding an ingestion flow makes it pass this rule
    valid_doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test"}},
        "ingestion_flows": [{"v": {"dataflow_id": "df_1", "target_catalog": "c", "target_schema": "s", "target_table": "t", "source_type": "autoloader"}}],
        "transformation_flows": [],
        "reconciliation_flows": []
    }
    res2 = val.validate_spec(valid_doc)
    rule_ids2 = [e["rule_id"] for e in res2["errors"]]
    assert "one_flow_array_required" not in rule_ids2


def test_secret_hygiene_rule():
    reg = RegistryManager()
    val = SpecValidator(reg)

    bad_doc = {
        "root": {
            "v": {
                "dataflow_group_id": "dfg_test",
                "private_key": "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwgg..."
            }
        },
        "ingestion_flows": [{"v": {"dataflow_id": "df_1"}}],
        "transformation_flows": [],
        "reconciliation_flows": []
    }
    res = val.validate_spec(bad_doc)
    assert not res["ok"]
    assert any(e["rule_id"] == "secret_hygiene_violation" for e in res["errors"])


def test_iceberg_batch_table_only_rule():
    reg = RegistryManager()
    val = SpecValidator(reg)

    # Streaming table with iceberg should fail
    bad_doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test"}},
        "ingestion_flows": [{
            "v": {
                "dataflow_id": "df_1",
                "target_type": "streaming_table",
                "target_config.storage_format": "iceberg"
            }
        }],
        "transformation_flows": [],
        "reconciliation_flows": []
    }
    res = val.validate_spec(bad_doc)
    assert any(e["rule_id"] == "iceberg_batch_table_only" for e in res["errors"])


def test_scd_requires_primary_keys_rule():
    reg = RegistryManager()
    val = SpecValidator(reg)

    # SCD1 without primary_keys should fail
    bad_doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test"}},
        "ingestion_flows": [{
            "v": {
                "dataflow_id": "df_1",
                "target_config.cdc_load_strategy": "SCD1"
            }
        }],
        "transformation_flows": [],
        "reconciliation_flows": []
    }
    res = val.validate_spec(bad_doc)
    assert any(e["rule_id"] == "scd_requires_primary_keys" for e in res["errors"])
