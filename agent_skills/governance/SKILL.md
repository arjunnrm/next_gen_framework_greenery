---
name: flowx-governance
description: Enforces standardized pipeline/workflow naming conventions, mandatory Unity Catalog resource tagging, and task/job-level observability naming standards across the Metaflow framework.
---

# 🛡️ Metaflow Governance, Tagging & Observability Standards Skill

This skill enforces enterprise governance policies, naming conventions, metadata attribution, and observability standards for all Metaflow data pipelines.

---

## 1. Standardized Naming Conventions

All pipelines, flows, control identifiers, and workflow tasks MUST adhere to the following naming schemas:

### 1.1 Pipeline & Group Identifiers (`dataflow_group_id`)
- **Format**: `dfg_<domain>_<entity>_<purpose>`
- **Rules**: Lowercase alphanumeric with underscores. Must start with `dfg_`.
- **Examples**:
  - `dfg_finance_transactions_cdc`
  - `dfg_telecom_cdr_ingest`
  - `dfg_ecommerce_orders_curation`

### 1.2 Flow Identifiers (`dataflow_id` & `flow_step_id`)
- **Ingestion Flows**: `df_<entity>_bronze_ingest` (e.g. `df_raw_orders_bronze_ingest`)
- **Transformation Flows**: `tf_<entity>_<layer>_<transform_type>` (e.g. `tf_orders_silver_scd1_enrichment`)
- **Flow Step Identifiers**: `step_<entity>_<operation>` (e.g. `step_orders_join_customers`)
- **Reconciliation Flows**: `rf_<entity>_<source>_vs_<target>` (e.g. `rf_orders_bronze_vs_silver`)

### 1.3 Target Table & Schema Conventions
- **Bronze Schema**: `bronze_<domain>` | Table: `<entity>_raw` (e.g. `poc.bronze_finance.transactions_raw`)
- **Silver Schema**: `silver_<domain>` | Table: `<entity>_dim` / `<entity>_fact` (e.g. `poc.silver_finance.transactions_dim`)
- **Gold Schema**: `gold_<domain>` | Table: `<entity>_agg` / `<entity>_report` (e.g. `poc.gold_finance.monthly_settlement_agg`)
- **Quarantine Tables**: `<target_table>_quarantine` (e.g. `poc.silver_finance.transactions_dim_quarantine`)

### 1.4 Databricks Workflow Task Keys
Databricks Multi-Task Jobs MUST use standard task keys in sequence:
1. `run_pipeline_update`: Executes the Lakeflow Declarative Pipeline update.
2. `apply_governance_tags`: Post-deployment task executing `04_apply_governance_and_egress.py` to set UC tags.
3. `observability_export`: Downstream task executing `08_dlt_observability_engine.py` to extract event logs and post OTel telemetry.

---

## 2. Mandatory Resource Tagging & Metadata Attribution

Every onboarding specification submitted to production MUST contain the following `governance_tags`:

### 2.1 Mandatory Table Tags (`governance_tags.table_tags`)

| Tag Key | Allowed Values / Format | Description | Required Tier |
|---|---|---|---|
| `cost_center` | `CC-[0-9]{4}-[A-Z0-9_]+` (e.g. `"CC-9041-FINANCE"`) | Financial chargeback cost center code. | **Mandatory** |
| `data_owner` | Valid email address (`user@company.com`) | Team or individual responsible for data asset. | **Mandatory** |
| `classification` | `"public"`, `"internal"`, `"confidential"`, `"restricted"` | Data sensitivity classification level. | **Mandatory** |
| `sla` | `"bronze_hourly"`, `"silver_daily"`, `"gold_realtime"`, `"batch_daily"` | Service Level Agreement for pipeline freshness. | **Mandatory** |
| `environment` | `"dev"`, `"stage"`, `"prod"` | Target deployment tier. | **Mandatory** |
| `retention_tier` | `"30_days"`, `"90_days"`, `"1_year"`, `"7_years"`, `"indefinite"` | Data lifecycle retention policy. | **Mandatory** |

### 2.2 Mandatory Column Tags (`governance_tags.column_tags`)
For any column containing PII, PCI, or encrypted fields:
- `pii_type`: `"email"`, `"ssn"`, `"phone"`, `"name"`, `"address"`, `"pci"`, `"account_number"`
- `security_tier`: `"confidential"`, `"restricted"`

### Example Spec Governance Fragment:
```json
"governance_tags": {
  "table_tags": {
    "cost_center": "CC-9041-FINANCE",
    "data_owner": "finance-data-platform@company.com",
    "classification": "restricted",
    "sla": "silver_hourly",
    "environment": "prod",
    "retention_tier": "7_years"
  },
  "column_tags": {
    "customer_ssn": {
      "pii_type": "ssn",
      "security_tier": "restricted"
    },
    "credit_card_hash": {
      "pii_type": "pci",
      "security_tier": "restricted"
    }
  }
}
```

---

## 3. Observability & Telemetry Naming Standards

### 3.1 OpenTelemetry Resource & Service Naming
- **Service Name (`service.name`)**: `flowx.<environment>.<domain>.<dataflow_group_id>`
  - Example: `flowx.prod.finance.dfg_transactions_cdc`
- **Destination Identifiers (`destination_id`)**:
  - Volume Destination: `dest-volume-<domain>-archive`
  - OTLP Endpoint Destination: `dest-otlp-<collector_name>`

### 3.2 Metric Signature Standards
- `lakeflow.pipeline.execution_duration_ms`: Total execution time of pipeline update.
- `lakeflow.pipeline.output_rows_total`: Total count of records materialized.
- `lakeflow.pipeline.quarantined_rows_total`: Total count of records routed to quarantine.
- `lakeflow.pipeline.reconciliation_mismatches_total`: Count of reconciliation drift records.

### 3.3 Log Record Signature Standards
In-pipeline JSON logs emitted to Databricks event log or OTel collector MUST adhere to:
```json
{
  "timestamp": "2026-08-29T12:00:00.000Z",
  "level": "INFO",
  "service_name": "flowx.prod.finance.dfg_transactions_cdc",
  "dataflow_group_id": "dfg_transactions_cdc",
  "dataflow_id": "df_raw_transactions",
  "flow_step_id": "step_cdc_merge",
  "event_type": "MICROBATCH_COMPLETED",
  "metrics": {
    "input_rows": 15000,
    "output_rows": 14950,
    "quarantined_rows": 50
  }
}
```
