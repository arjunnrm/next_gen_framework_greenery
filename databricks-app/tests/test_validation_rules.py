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


def _flow_doc(v, kvs=None, kind="ingestion_flows"):
    doc = {
        "root": {"v": {"dataflow_group_id": "dfg_test"}},
        "ingestion_flows": [],
        "transformation_flows": [],
        "reconciliation_flows": [],
    }
    flow = {"v": v}
    if kvs:
        flow["kvs"] = kvs
    doc[kind] = [flow]
    return doc


def test_full_snapshot_cdc_requires_primary_keys():
    """FULL_SNAPSHOT_CDC needs a key too, as of v1.4.0.

    apply_changes_from_snapshot diffs successive snapshots on a key and the framework no longer
    manufactures one, so spec_validator.py requires primary_keys for every CDC-dispatched
    strategy. The app's rule listed only the SCDs, which let a keyless snapshot flow pass the
    builder and fail onboarding.
    """
    val = SpecValidator(RegistryManager())
    res = val.validate_spec(_flow_doc({
        "dataflow_id": "df_1",
        "target_config.cdc_load_strategy": "FULL_SNAPSHOT_CDC",
    }))
    assert any(e["rule_id"] == "scd_requires_primary_keys" for e in res["errors"])


def test_columns_to_exclude_rejected_on_full_snapshot_cdc():
    """columns_to_exclude is comparison-only, and FULL_SNAPSHOT_CDC has no comparison set.

    The framework scopes it to {SCD1, SCD2, SCD3}; the app's rule fired only on APPEND and
    TRUNCATE_AND_LOAD, so the third rejected strategy slipped through.
    """
    val = SpecValidator(RegistryManager())
    res = val.validate_spec(_flow_doc({
        "dataflow_id": "df_1",
        "target_config.cdc_load_strategy": "FULL_SNAPSHOT_CDC",
        "target_config.primary_keys": ["id"],
        "target_config.columns_to_exclude": ["batch_ts"],
    }))
    assert any(e["rule_id"] == "columns_to_exclude_cdc_only" for e in res["errors"])


def test_cdc_operation_column_rejected_on_scd3():
    """SCD3 is a current/previous pivot with no delete path at all."""
    val = SpecValidator(RegistryManager())
    res = val.validate_spec(_flow_doc({
        "flow_step_id": "ts_1",
        "dataflow_id": "df_1",
        "target_config.cdc_load_strategy": "SCD3",
        "target_config.primary_keys": ["id"],
        "target_config.columns_to_check": ["status"],
        "target_config.cdc_operation_column": "op",
    }, kind="transformation_flows"))
    assert any(e["rule_id"] == "cdc_operation_column_scope" for e in res["errors"])


def test_cdc_operation_column_allowed_on_full_snapshot_cdc():
    """...but FULL_SNAPSHOT_CDC does accept it -- flagged rows are filtered out of the snapshot."""
    val = SpecValidator(RegistryManager())
    res = val.validate_spec(_flow_doc({
        "dataflow_id": "df_1",
        "target_config.cdc_load_strategy": "FULL_SNAPSHOT_CDC",
        "target_config.primary_keys": ["id"],
        "target_config.cdc_operation_column": "op",
        "target_config.cdc_operation_mapping.delete_values": ["D"],
    }))
    assert not any(e["rule_id"] == "cdc_operation_column_scope" for e in res["errors"])


def test_kafka_sink_requires_kafka_options():
    """A kafka sink carries its connection details in kafka_options, never in a path."""
    val = SpecValidator(RegistryManager())
    res = val.validate_spec(_flow_doc({
        "dataflow_id": "df_1",
        "target_type": "external_sink",
        "target_config.cdc_load_strategy": "APPEND",
        "target_config.sink_config.format": "kafka",
    }))
    assert any(e["rule_id"] == "kafka_sink_requires_options" for e in res["errors"])


def test_kafka_options_supplied_as_kv_satisfies_the_rule():
    """kv widgets live in `kvs`, not `v`.

    Flow rule contexts used to expose `v` alone, so a rule referencing a kv path resolved to
    None whether it was filled in or not -- an `empty` guard fired on a correctly-populated
    flow. This asserts the populated case is accepted, which is the half a missing-field test
    cannot cover.
    """
    val = SpecValidator(RegistryManager())
    res = val.validate_spec(_flow_doc(
        {
            "dataflow_id": "df_1",
            "target_type": "external_sink",
            "target_config.cdc_load_strategy": "APPEND",
            "target_config.sink_config.format": "kafka",
        },
        kvs={"target_config.sink_config.kafka_options": [
            ["kafka.bootstrap.servers", "broker:9092"], ["topic", "events"],
        ]},
    ))
    assert not any(e["rule_id"] == "kafka_sink_requires_options" for e in res["errors"])
