# 🛡️ FlowX — Data Quality & Governance

> **Audience**: Data governance officers, data quality engineers, and compliance leads responsible for data contracts, quarantine routing, and Unity Catalog metadata attribution.

---

## 1. Data Quality Framework Overview

FlowX implements a declarative Data Quality (DQ) engine built on top of [Lakeflow Expectations](https://docs.databricks.com/aws/en/dlt/expectations) with automatic, zero-data-loss quarantine routing.

### Data Quality Actions

| Action | Behavior | Target Impact | Pipeline Status |
|---|---|---|---|
| `"warn"` | Logs violation metrics to DLT event log; allows invalid records through. | Target table receives record | Pipeline continues |
| `"drop"` | Silently discards invalid records; logs drop count to event log. | Record is dropped | Pipeline continues |
| `"fail"` | Aborts pipeline execution immediately upon detecting an invalid record. | Target table transaction rolls back | Pipeline fails |
| `"quarantine"` | Routes invalid records to a dedicated quarantine Delta table with diagnostic metadata; allows valid records into the target table. | Target table receives valid rows only; Quarantine receives invalid rows | Pipeline continues |

---

## 2. Quarantine Routing Architecture

When one or more rules declare `action: "quarantine"`, FlowX dynamically forks the stream:

```
                               ┌──────────────────────────────┐
                               │       INCOMING RECORDS       │
                               └──────────────┬───────────────┘
                                              │
                                              ▼
                               ┌──────────────────────────────┐
                               │   DQ EXPECTATION EVALUATION  │
                               └──────────────┬───────────────┘
                                              │
                       ┌──────────────────────┴──────────────────────┐
                       │                                             │
                       ▼ Passes All Rules                            ▼ Fails ≥ 1 Rule
        ┌──────────────────────────────┐              ┌──────────────────────────────┐
        │      CLEAN TARGET TABLE      │              │      QUARANTINE TABLE        │
        │    poc.silver.orders_clean   │              │   poc.silver.orders_quarantine
        │                              │              │                              │
        │ • Valid business records     │              │ • Raw payload preserved      │
        │ • Standard CDC merge         │              │ • Failed rule array added    │
        │                              │              │ • Ingestion timestamp added  │
        └──────────────────────────────┘              └──────────────────────────────┘
```

### Quarantine Configuration
```json
"dq_config": {
  "quarantine_table": "orders_quarantine",
  "record_id_column": "order_id",
  "rules": [
    {
      "name": "valid_order_id",
      "sql_condition": "order_id IS NOT NULL AND trim(order_id) != ''",
      "action": "fail"
    },
    {
      "name": "positive_amount",
      "sql_condition": "amount > 0",
      "action": "quarantine"
    },
    {
      "name": "valid_email_format",
      "sql_condition": "email LIKE '%@%.%'",
      "action": "quarantine"
    },
    {
      "name": "known_region",
      "sql_condition": "region IN ('NA', 'EMEA', 'APAC', 'LATAM')",
      "action": "warn"
    }
  ]
}
```

### Diagnostic Metadata Injected in Quarantine Table
Records entering the quarantine table are automatically enriched with diagnostic audit fields:
- `__framework_quarantine_timestamp_utc`: Exact UTC timestamp when the record entered quarantine.
- `__framework_quarantine_failed_rules`: `ARRAY<STRING>` containing the names of all rules that evaluated to false for this specific record (e.g. `["positive_amount", "valid_email_format"]`).
- `__framework_quarantine_source_dataflow_id`: Origin flow identifier.

---

## 3. Unity Catalog Governance & Tagging

FlowX provides declarative Unity Catalog metadata attribution for both tables and individual columns.

### Governance Configuration Schema
```json
"governance_tags": {
  "table_tags": {
    "cost_center": "CC-9041-FINANCE",
    "data_owner": "data-engineering@company.com",
    "classification": "restricted",
    "sla": "silver_hourly",
    "retention_tier": "7_years"
  },
  "column_tags": {
    "customer_ssn": {
      "pii_type": "ssn",
      "security_tier": "confidential"
    },
    "credit_card_number": {
      "pii_type": "pci",
      "security_tier": "restricted"
    }
  }
}
```

### Decoupled Post-Deployment Tag Application
Because Databricks Lakeflow disallows executing DDL statements (`ALTER TABLE ... SET TAGS`) during streaming DAG execution, tag attribution is decoupled into a dedicated post-deployment job task:
1. Pipeline compiles and materializes target Delta tables.
2. The post-deployment task `notebooks/04_governance/04_apply_governance_and_egress.py` runs immediately following the pipeline update.
3. It queries `governance_tags_json` from the control tables and executes idempotent `ALTER TABLE <catalog>.<schema>.<table> SET TAGS (...)` DDL commands.

---

## 4. Attribute-Based Access Control (ABAC) Integration

FlowX integrates seamlessly with Unity Catalog Row Filters and Column Masks to enforce fine-grained access control:
- **Column Masks**: Automatically mask sensitive plaintext columns (e.g. hashing SSNs for non-privileged users) via UC User-Defined Functions.
- **Row Filters**: Enforce row-level tenant or geographic isolation (e.g. `region = current_user_region()`).
- Learn more in [Official Unity Catalog Row Filters & Column Masks Guide](https://docs.databricks.com/data-governance/unity-catalog/row-and-column-filters).
