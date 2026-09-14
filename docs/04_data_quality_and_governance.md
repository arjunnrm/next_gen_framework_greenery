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
4. **`target_type: "sink"` flows are skipped** (v1.7.5). A pure sink is a `dlt.create_sink` + `@dlt.append_flow` with **no persisted dataset** — its `target_table` names the sink, not a table — so there is nothing to `ALTER`. Before v1.7.5 the loop tagged it anyway, raised `TABLE_OR_VIEW_NOT_FOUND`, and because the group-level loop has no per-flow isolation, **every other flow's tags in the group went unapplied too**. Found live on UC6's first green pipeline update. `external_sink` is *not* skipped: it materializes a real main table first and only additionally exports it, so its tags apply as normal. A `governance_tags` block on a `sink` flow is therefore accepted but inert; put table tags on the flow that produces the data the sink reads.

#### Tagging several dataflow groups in one task (v0.0.7)

The `dataflow_group_id` widget accepts **either a single id or a comma-separated list**:

```yaml
base_parameters:
  catalog: ${var.catalog}
  dataflow_group_id: dfg_uc3_excalibur_streaming_cdc,dfg_uc3_excalibur_batch_recon
  apply_abac: "true"
```

A use case whose groups previously needed one chained task each is now one task — `resources/uc3/uc3_governance_job.yml` collapsed its `tag_streaming_cdc -> tag_batch_recon` pair into a single `tag_uc3_groups` task this way. Adding a further group is an edit to the comma-separated string, not a new task.

A single id is simply the one-element case, so **every pre-existing single-id job definition keeps working unchanged** — no `resources/*.yml` needed editing for this change. Blank entries (a trailing comma) and duplicate ids are dropped, and ordering is preserved.

**Each group is isolated.** If one group fails, the remaining groups are still attempted and the task then fails at the end naming every group that failed, with a `N of M group(s)` count. This matters because the alternative — stopping at the first failure — would make one task carrying N groups strictly *worse* than N separate tasks: a single bad group would silently deny its tags to every group listed after it, which is the same class of silent-gap failure that item 4 above describes at the flow level.

> **Note — this is group-level isolation only.** Within a single group, `apply_all_governance_tags` still has no per-flow isolation, so the item-4 caveat continues to apply inside each group.

---

## 4. Attribute-Based Access Control (ABAC) Integration

FlowX integrates seamlessly with Unity Catalog Row Filters and Column Masks to enforce fine-grained access control:
- **Column Masks**: Automatically mask sensitive plaintext columns (e.g. hashing SSNs for non-privileged users) via UC User-Defined Functions.
- **Row Filters**: Enforce row-level tenant or geographic isolation (e.g. `region = current_user_region()`).
- Learn more in [Official Unity Catalog Row Filters & Column Masks Guide](https://docs.databricks.com/data-governance/unity-catalog/row-and-column-filters).
