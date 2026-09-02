"""Build the golden example set for the metaflow-onboarding skill.

Every example here is validated against the REAL framework validator before being written.
An example that does not pass is a bug in the example, not in the validator.
"""
import json
import sys

sys.path.insert(0, "src")
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import validate_spec

EXAMPLES = {}

EXAMPLES["01_minimal_autoloader_append"] = {
    "_use_when": "Simplest possible ingestion: land CSV files into a Bronze streaming table, append-only.",
    "dataflow_group_id": "dfg_sales_ingest",
    "ingestion_flows": [
        {
            "dataflow_id": "df_sales_orders_ingest",
            "source_type": "autoloader",
            "source_config": {
                "path": "/Volumes/main/landing/sales/orders/",
                "format": "csv",
                "schema_location": "/Volumes/main/landing/_schemas/sales_orders/",
                "reader_options": {"header": "true", "delimiter": ","},
            },
            "target_catalog": "main",
            "target_schema": "bronze",
            "target_table": "sales_orders",
            "target_type": "streaming_table",
            "target_config": {"cdc_load_strategy": "APPEND"},
        }
    ],
}

EXAMPLES["02_scd2_with_dq_and_tags"] = {
    "_use_when": "SCD2 history on a keyed source, with data-quality rules and governance tags.",
    "dataflow_group_id": "dfg_customer_master",
    "ingestion_flows": [
        {
            "dataflow_id": "df_customer_ingest",
            "source_type": "autoloader",
            "source_config": {
                "path": "/Volumes/main/landing/crm/customer/",
                "format": "json",
                "schema_location": "/Volumes/main/landing/_schemas/customer/",
                "capture_technical_metadata": True,
                "column_normalization": {"enabled": True, "case": "lower"},
            },
            "target_catalog": "main",
            "target_schema": "silver",
            "target_table": "customer_history",
            "target_type": "streaming_table",
            "target_config": {
                "cdc_load_strategy": "SCD2",
                "primary_keys": ["customer_id"],
                "sequence_by_column": "updated_at",
                "columns_to_check": ["email", "status"],
            },
            "dq_config": {
                "rules": [
                    {"rule_id": "customer_id_not_null", "expression": "customer_id IS NOT NULL", "action": "quarantine"},
                    {"rule_id": "email_present", "expression": "email IS NOT NULL", "action": "warn"},
                ],
                "quarantine_table": "customer_history_quarantine",
                "record_id_column": "customer_id",
            },
            "governance_tags": {
                "table_tags": {"cost_center": "CC-1042", "classification": "confidential", "sla": "daily"},
                "column_tags": [{"column": "email", "tags": {"pii": "true"}}],
            },
        }
    ],
}

EXAMPLES["03_transformation_join"] = {
    "_use_when": "Join two upstream tables into a Gold materialized view. Transformation flows read from source_inputs[], never source_config.",
    "dataflow_group_id": "dfg_sales_gold",
    "transformation_flows": [
        {
            "flow_step_id": "tf_orders_enriched",
            "dataflow_id": "df_orders_enriched",
            "source_inputs": [
                {"input_name": "orders", "table": "main.silver.sales_orders", "is_streaming": False},
                {"input_name": "customers", "table": "main.silver.customer_history", "is_streaming": False},
            ],
            "transformation_sql": (
                "SELECT o.order_id, o.order_total, c.customer_id, c.email "
                "FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
            ),
            "target_catalog": "main",
            "target_schema": "gold",
            "target_table": "orders_enriched",
            "target_type": "materialized_view",
            "target_config": {"cdc_load_strategy": "APPEND"},
        }
    ],
}

EXAMPLES["04_zerobus_scd1"] = {
    "_use_when": "Stream from an existing Delta table (e.g. Zerobus-landed) and keep only the latest row per key.",
    "dataflow_group_id": "dfg_events_current",
    "ingestion_flows": [
        {
            "dataflow_id": "df_device_events_ingest",
            "source_type": "zerobus",
            "source_config": {
                "source_catalog": "main",
                "source_schema": "landing",
                "source_table": "device_events_raw",
            },
            "target_catalog": "main",
            "target_schema": "silver",
            "target_table": "device_events_current",
            "target_type": "streaming_table",
            "target_config": {
                "cdc_load_strategy": "SCD1",
                "primary_keys": ["device_id"],
                "sequence_by_column": "event_ts",
            },
        }
    ],
}

EXAMPLES["05_reconciliation_in_pipeline"] = {
    "_use_when": "Compare a source and a target inside the pipeline DAG (v1.5.0+). execution_mode 'pipeline' needs a streaming-capable source; use 'pipeline_audit_only' for a batch read.",
    "dataflow_group_id": "dfg_finance_recon",
    "ingestion_flows": [
        {
            "dataflow_id": "df_ledger_ingest",
            "source_type": "autoloader",
            "source_config": {
                "path": "/Volumes/main/landing/finance/ledger/",
                "format": "parquet",
                "schema_location": "/Volumes/main/landing/_schemas/ledger/",
            },
            "target_catalog": "main",
            "target_schema": "bronze",
            "target_table": "ledger",
            "target_type": "streaming_table",
            "target_config": {"cdc_load_strategy": "APPEND"},
        }
    ],
    "reconciliation_flows": [
        {
            "reconciliation_id": "rf_ledger_vs_source",
            "dataflow_group_id": "dfg_finance_recon",
            "execution_mode": "pipeline_audit_only",
            "publish_schema": "recon",
            "match_keys": ["txn_id"],
            "compare_columns": ["amount", "currency"],
            "source_config": {
                "type": "table",
                "table": "main.bronze.ledger",
                "read_mode": "batch",
            },
            "target_configs": [
                {
                    "target_id": "gold_ledger",
                    "type": "table",
                    "table": "main.gold.ledger_final",
                    "append_target_table": "main.landing.ledger_replay",
                }
            ],
            "error_handling": {"on_failure": "warn"},
            "logging_config": {"run_log_capture": True, "mismatch_log_capture": True},
        }
    ],
}


def main():
    ok = True
    for name, spec in EXAMPLES.items():
        payload = {k: v for k, v in spec.items() if k != "_use_when"}
        *_, errors = validate_spec(None, payload)
        status = "OK" if not errors else f"{len(errors)} ERRORS"
        print(f"{name}: {status}")
        for e in errors:
            ok = False
            print("    -", e[:220])
    if not ok:
        sys.exit(1)
    out = "agent_skills/reference/golden_specs.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(EXAMPLES, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print("\nwrote", out)


if __name__ == "__main__":
    main()
