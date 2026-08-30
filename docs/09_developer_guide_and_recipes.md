# 🛠️ Metaflow — Developer Step-by-Step Guide & Recipes

> **Audience**: Data engineers building, validating, and deploying data pipelines using the Metaflow framework.

---

## 1. Prerequisites & Environment Setup

### Required Tooling
- **Databricks CLI**: v0.200+ ([Install Guide](https://docs.databricks.com/dev-tools/cli/install.html))
- **uv**: Modern fast Python package manager ([Install Guide](https://docs.astral.sh/uv/getting-started/installation/))
- **Python 3.12**: Core runtime environment

### Authentication
```bash
# Verify authentication against target Databricks workspace
databricks auth login
databricks current-user me
```

---

## 2. The 11-Step Pipeline Onboarding Walkthrough

Follow these sequential steps to onboard a new end-to-end pipeline:

### Step 1: Create Onboarding Spec File
Create a new file in `onboarding_templates/my_pipeline.json` (or `.yaml`). Declare `dataflow_group_id`:
```json
{
  "dataflow_group_id": "dfg_ecommerce_orders_pipeline",
  "pipeline_parameters": {
    "target_region": "NORTH_AMERICA"
  },
  "ingestion_flows": [],
  "transformation_flows": [],
  "reconciliation_flows": []
}
```

### Step 2: Define Bronze Ingestion Flow
Add an ingestion flow to read raw landing files:
```json
{
  "dataflow_id": "df_raw_orders",
  "source_type": "autoloader",
  "target_catalog": "{{catalog}}",
  "target_schema": "bronze_ecommerce",
  "target_table": "raw_orders",
  "target_type": "streaming_table",
  "source_config": {
    "path": "/Volumes/{{catalog}}/landing/orders",
    "format": "csv",
    "reader_options": { "header": "true" },
    "capture_technical_metadata": true
  },
  "target_config": {
    "cdc_load_strategy": "APPEND"
  }
}
```

### Step 3: Define Silver Transformation Flow & CDC Strategy
Add a transformation flow to clean data and apply SCD1 merge:
```json
{
  "dataflow_id": "tf_orders_cleaned",
  "flow_step_id": "step_orders_scd1",
  "source_inputs": [
    { "table": "{{catalog}}.bronze_ecommerce.raw_orders", "alias": "ord" }
  ],
  "transformation_sql": "SELECT ord.order_id, ord.customer_id, cast(ord.amount as double) as amount, ord.order_status, ord.order_date FROM ord",
  "target_catalog": "{{catalog}}",
  "target_schema": "silver_ecommerce",
  "target_table": "orders_dim",
  "target_type": "streaming_table",
  "target_config": {
    "cdc_load_strategy": "SCD1",
    "primary_keys": ["order_id"],
    "sequence_by_column": "order_date",
    "compute_hash_key": true
  }
}
```

### Step 4: Add Data Quality Expectations & Quarantine
Add `dq_config` with quarantine routing to isolate corrupted rows:
```json
"dq_config": {
  "quarantine_table": "orders_dim_quarantine",
  "record_id_column": "order_id",
  "rules": [
    { "name": "valid_order_id", "sql_condition": "order_id IS NOT NULL", "action": "fail" },
    { "name": "positive_amount", "sql_condition": "amount > 0", "action": "quarantine" }
  ]
}
```

### Step 5: Add Governance Tags
Assign metadata attribution to target tables:
```json
"governance_tags": {
  "table_tags": {
    "cost_center": "CC-ECOMMERCE",
    "classification": "confidential",
    "sla": "silver_hourly"
  }
}
```

### Step 6: Validate Your Spec
Run the preflight validator tool or CLI command:
```powershell
uv run pytest tests/unit/test_spec_validator.py -k "my_pipeline"
```

### Step 7: Build the Wheel Package
```powershell
python scripts/bump_and_build.py
```

### Step 8: Deploy via Databricks Asset Bundles (DABs)
```bash
databricks bundle deploy -t dev
```

### Step 9: Onboard Spec to Control Tables
Trigger the reusable onboarding job:
```bash
databricks bundle run onboarding_job \
  --params spec_file_path=/Workspace/.../my_pipeline.json,catalog=poc,env=dev,action_type=CREATE
```

### Step 10: Run the Lakeflow Pipeline
Trigger the Lakeflow Declarative Pipeline for your `dataflow_group_id`:
```bash
databricks pipelines start --pipeline-id <pipeline_id>
```

### Step 11: Verify Target Tables & Metrics
Query target tables in SQL Editor:
```sql
SELECT * FROM poc.silver_ecommerce.orders_dim LIMIT 10;
SELECT * FROM poc.silver_ecommerce.orders_dim_quarantine LIMIT 10;
```

---

## 3. Production Copy-Paste Recipes

### Recipe A: Auto Loader JSON with Schema Evolution & Rescued Data
```json
{
  "dataflow_id": "df_iot_telemetry",
  "source_type": "autoloader",
  "target_catalog": "{{catalog}}",
  "target_schema": "bronze_iot",
  "target_table": "sensor_telemetry",
  "target_type": "streaming_table",
  "source_config": {
    "path": "/Volumes/{{catalog}}/iot/landing",
    "format": "json",
    "schema_evolution_mode": "rescue",
    "capture_technical_metadata": true
  },
  "target_config": { "cdc_load_strategy": "APPEND" }
}
```

### Recipe B: SCD2 Dimension with AES Encryption
```json
{
  "dataflow_id": "tf_customer_scd2",
  "flow_step_id": "step_cust_scd2",
  "source_inputs": [{ "table": "{{catalog}}.bronze_crm.raw_customers", "alias": "c" }],
  "transformation_sql": "SELECT c.customer_id, c.email, c.ssn, c.updated_at FROM c",
  "target_catalog": "{{catalog}}",
  "target_schema": "silver_crm",
  "target_table": "customer_scd2_dim",
  "target_type": "streaming_table",
  "target_config": {
    "cdc_load_strategy": "SCD2",
    "primary_keys": ["customer_id"],
    "sequence_by_column": "updated_at",
    "encrypted_columns": [
      {
        "column_name": "ssn",
        "algorithm": "AES",
        "mode": "GCM",
        "secret": {
          "secret_catalog": "poc",
          "secret_schema": "security",
          "secret_key": "pii_aes_gcm_key"
        }
      }
    ]
  }
}
```

### Recipe C: Native Delta Lake Egress Sink
```json
{
  "dataflow_id": "tf_export_partner",
  "flow_step_id": "step_export_partner",
  "source_inputs": [{ "table": "{{catalog}}.silver_crm.customer_scd2_dim", "alias": "src" }],
  "transformation_sql": "SELECT src.customer_id, src.email FROM src WHERE src.__end_at IS NULL",
  "target_type": "sink",
  "target_config": {
    "sink_config": {
      "format": "delta",
      "path": "/Volumes/partner_catalog/drops/customer_export",
      "mode": "append"
    }
  }
}
```

---

## 4. Local Testing & Verification Patterns

Run fast unit tests without requiring a remote cluster:
```powershell
# Run all unit tests
uv run pytest tests/unit/ -v

# Test specific onboarding spec validation
uv run pytest tests/unit/test_spec_validator.py -v
```
