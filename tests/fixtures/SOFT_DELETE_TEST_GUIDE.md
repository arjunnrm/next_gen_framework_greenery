# Soft-Delete Feature: Testing Guide

This guide helps you test the soft-delete feature end-to-end with sample data.

## 📋 Files in This Directory

```
tests/fixtures/
├── soft_delete_test_spec.json         # Onboarding spec (ingestion only)
├── soft_delete_sample_data.py         # Data generator script
├── bronze_customers.json              # Sample Bronze table data (10 records)
├── customer_deletion_keys_run1.json    # Key file for Run 1 (2 deletions)
├── customer_deletion_keys_run2.json    # Key file for Run 2 (3 deletions - incremental)
└── SOFT_DELETE_TEST_GUIDE.md           # This file
```

---

## 🚀 Quick Start

### Step 1: Generate Sample Data

```bash
cd tests/fixtures/
python soft_delete_sample_data.py
```

**Output:**
```
✓ Created: bronze_customers.json
✓ Created: customer_deletion_keys_run1.json
✓ Created: customer_deletion_keys_run2.json
```

### Step 2: Review the Configuration

Open `soft_delete_test_spec.json` and note:
- Single ingestion flow: `df_customers_ingest`
- Target: `main.bronze_test.customers`
- Soft-delete config enabled
- Key file path: `/Volumes/main/test/deletes/customer_deletion_keys.parquet`

### Step 3: Review Sample Data

```bash
# See Bronze table structure
cat bronze_customers.json

# See key file for Run 1
cat customer_deletion_keys_run1.json

# See key file for Run 2 (incremental)
cat customer_deletion_keys_run2.json
```

---

## 📊 Sample Data Overview

### Bronze Table: `customers`

**10 customer records:**

| customer_id | name | email | phone | status | created_date |
|---|---|---|---|---|---|
| CUST001 | Alice Johnson | alice@example.com | 555-0001 | active | 2024-01-15 |
| CUST002 | Bob Smith | bob@example.com | 555-0002 | active | 2024-01-16 |
| **CUST003** | **Charlie Brown** | charlie@example.com | 555-0003 | **inactive** | 2024-01-17 |
| CUST004 | Diana Prince | diana@example.com | 555-0004 | active | 2024-01-18 |
| CUST005 | Eve Wilson | eve@example.com | 555-0005 | active | 2024-01-19 |
| CUST006 | Frank Miller | frank@example.com | 555-0006 | active | 2024-01-20 |
| **CUST007** | **Grace Lee** | grace@example.com | 555-0007 | **suspended** | 2024-01-21 |
| CUST008 | Henry Zhang | henry@example.com | 555-0008 | active | 2024-01-22 |
| CUST009 | Iris Kim | iris@example.com | 555-0009 | active | 2024-01-23 |
| CUST010 | Jack Davis | jack@example.com | 555-0010 | active | 2024-01-24 |

---

## 🔑 Key Files

### Key File Schema

Both key files have identical schema:
```json
[
  {
    "customer_id": "CUST001",
    "is_marked_deleted": false
  },
  ...
]
```

**Column meanings:**
- `customer_id`: Primary key to match against Bronze table
- `is_marked_deleted`: Boolean flag (true = soft-delete this record)

---

### Run 1: Initial Soft-Delete

**File:** `customer_deletion_keys_run1.json`

**Marks for deletion:** 2 records
- `CUST003` (Charlie Brown) - inactive customer
- `CUST007` (Grace Lee) - suspended customer

**Expected result after soft-delete:**
```json
{
  "input_records": 10,
  "soft_deleted": 2,
  "active": 8,
  "is_deleted_records": ["CUST003", "CUST007"]
}
```

---

### Run 2: Incremental Soft-Delete

**File:** `customer_deletion_keys_run2.json`

**Marks for deletion:** 3 records (1 new)
- `CUST003` (already deleted in Run 1)
- `CUST007` (already deleted in Run 1)
- `CUST005` (NEW - Eve Wilson, active customer)

**Expected result after soft-delete:**
```json
{
  "input_records": 10,
  "soft_deleted": 3,
  "active": 7,
  "is_deleted_records": ["CUST003", "CUST005", "CUST007"],
  "new_deletions": ["CUST005"],
  "note": "CUST003 and CUST007 already deleted - idempotent operation"
}
```

---

## ✅ Testing Scenarios

### Scenario 1: Initial Soft-Delete (Run 1)

**Test:** Apply soft-delete with `customer_deletion_keys_run1.json`

**Setup:**
1. Create Bronze table with 10 customer records
2. Create key file with 2 deletion marks (CUST003, CUST007)

**Action:**
```python
from flowx.lakeflow_framework.soft_delete.processor import apply_soft_deletes

result = apply_soft_deletes(
    spark=spark,
    catalog="main",
    schema="bronze_test",
    table="customers",
    key_file_path="/Volumes/main/test/deletes/customer_deletion_keys_run1.parquet",
    primary_keys=["customer_id"],
    key_file_deleted_indicator_column="is_marked_deleted",
    flow_id="df_customers_ingest",
)
```

**Verification:**
```sql
-- Verify soft-deleted rows
SELECT customer_id, name, is_deleted
FROM main.bronze_test.customers
WHERE is_deleted = true
ORDER BY customer_id;

-- Expected: 2 rows (CUST003, CUST007)

-- Verify active rows
SELECT customer_id, name, is_deleted
FROM main.bronze_test.customers
WHERE is_deleted IS NULL OR is_deleted = false
ORDER BY customer_id;

-- Expected: 8 rows (CUST001, CUST002, CUST004, CUST005, CUST006, CUST008, CUST009, CUST010)
```

**Assert:**
```python
assert result["status"] == "success"
assert result["rows_marked_for_deletion"] == 2
assert result["rows_updated"] >= 2
```

---

### Scenario 2: Idempotency Test (Run 1 Again)

**Test:** Apply soft-delete again with same key file

**Setup:** Same as Run 1

**Action:**
```python
result2 = apply_soft_deletes(
    spark=spark,
    catalog="main",
    schema="bronze_test",
    table="customers",
    key_file_path="/Volumes/main/test/deletes/customer_deletion_keys_run1.parquet",
    primary_keys=["customer_id"],
    key_file_deleted_indicator_column="is_marked_deleted",
)
```

**Verification:**
```sql
-- Should be identical to Run 1
SELECT COUNT(*) as soft_deleted_count
FROM main.bronze_test.customers
WHERE is_deleted = true;

-- Expected: 2 (no change)
```

**Assert:**
```python
# Same as before
assert result2["rows_updated"] == result["rows_updated"]
```

---

### Scenario 3: Incremental Soft-Delete (Run 2)

**Test:** Apply soft-delete with updated key file (new deletions)

**Setup:** Same Bronze table (with Run 1 soft-deletes applied)

**Action:**
```python
result3 = apply_soft_deletes(
    spark=spark,
    catalog="main",
    schema="bronze_test",
    table="customers",
    key_file_path="/Volumes/main/test/deletes/customer_deletion_keys_run2.parquet",
    primary_keys=["customer_id"],
    key_file_deleted_indicator_column="is_marked_deleted",
)
```

**Verification:**
```sql
-- Now 3 soft-deleted rows
SELECT customer_id, name, is_deleted
FROM main.bronze_test.customers
WHERE is_deleted = true
ORDER BY customer_id;

-- Expected: 3 rows (CUST003, CUST005, CUST007)
```

**Assert:**
```python
assert result3["status"] == "success"
assert result3["rows_marked_for_deletion"] == 3
assert result3["rows_updated"] >= 3  # At least 3 soft-deleted total
```

---

### Scenario 4: Transformation Filtering (Optional)

**Test:** Create transformation that filters soft-deleted records

**Action:**
```python
# Create Silver table from Bronze, excluding soft-deleted
spark.sql("""
    CREATE OR REPLACE TABLE main.silver_test.customers_active AS
    SELECT 
        customer_id,
        name,
        email,
        status
    FROM main.bronze_test.customers
    WHERE is_deleted IS NULL OR is_deleted = false
""")
```

**Verification:**
```sql
-- Silver table should have only 7 active customers (after Run 2)
SELECT COUNT(*) as active_count FROM main.silver_test.customers_active;
-- Expected: 7

-- Verify which customers are in Silver
SELECT customer_id, name FROM main.silver_test.customers_active ORDER BY customer_id;
-- Expected: CUST001, CUST002, CUST004, CUST006, CUST008, CUST009, CUST010
-- (CUST003, CUST005, CUST007 are excluded)
```

---

## 📝 Configuration Details

### Ingestion Flow: `df_customers_ingest`

```json
{
  "dataflow_id": "df_customers_ingest",
  "source_type": "autoloader",
  "target_table": "customers",
  "target_type": "streaming_table",
  "target_config": {
    "cdc_load_strategy": "APPEND",
    "soft_delete_config": {
      "enabled": true,
      "key_file_path": "/Volumes/main/test/deletes/customer_deletion_keys.parquet",
      "primary_keys": ["customer_id"],
      "key_file_deleted_indicator_column": "is_marked_deleted"
    }
  }
}
```

**Key aspects:**
- Single primary key: `customer_id`
- CDC strategy: `APPEND` (standard ingestion, no updates/deletes from source)
- Soft-delete is additional layer on top of CDC

**Data Quality (DQ):**
- Drops rows with null `customer_id`
- Quarantines invalid email formats
- Quarantine table: `customers_quarantine`

**Governance:**
- PII tags on `email` and `phone` columns
- Domain: `customer`
- Sensitivity: `medium`

---

## 🧪 Test Execution Commands

### Unit Tests (Existing)
```bash
pytest tests/unit/test_soft_delete_processor.py -v
```

### Integration Tests (Existing)
```bash
pytest tests/integration/test_soft_delete_e2e.py -v
```

### Manual Testing with Sample Data
```python
# In a Databricks notebook or Python script:

import json
from pathlib import Path
from flowx.lakeflow_framework.soft_delete.processor import apply_soft_deletes

# Load sample data
test_fixtures = Path("tests/fixtures")
with open(test_fixtures / "bronze_customers.json") as f:
    bronze_data = json.load(f)

with open(test_fixtures / "customer_deletion_keys_run1.json") as f:
    key_data_run1 = json.load(f)

# Create Bronze table
spark.createDataFrame(bronze_data).write.format("delta").mode("overwrite").saveAsTable("main.bronze_test.customers")

# Apply soft-deletes
result = apply_soft_deletes(
    spark=spark,
    catalog="main",
    schema="bronze_test",
    table="customers",
    key_file_path="/path/to/customer_deletion_keys_run1.parquet",
    primary_keys=["customer_id"],
    key_file_deleted_indicator_column="is_marked_deleted",
)

print(result)
# Expected output:
# {
#     'status': 'success',
#     'flow_id': None,
#     'key_file_path': '...',
#     'target_table': 'main.bronze_test.customers',
#     'rows_in_key_file': 10,
#     'rows_marked_for_deletion': 2,
#     'rows_updated': 2
# }
```

---

## 🔍 Verification Checklist

- [ ] Sample data files generated successfully
- [ ] Configuration spec validates without errors
- [ ] Bronze table created with 10 records
- [ ] Key file created with deletion marks
- [ ] Soft-delete processor executes successfully
- [ ] `is_deleted` column created in Bronze table
- [ ] 2 records marked as deleted (Run 1)
- [ ] 3 records marked as deleted (Run 2)
- [ ] No changes on third run (idempotency)
- [ ] Transformations can filter soft-deleted records
- [ ] All unit tests pass
- [ ] All integration tests pass

---

## 📌 Key Points

✅ **Single primary key:** `customer_id` matches Bronze table column  
✅ **Deletion indicator:** `is_marked_deleted` is boolean column in key file  
✅ **Idempotent:** Running twice with same key file = same result  
✅ **Incremental:** New deletions added on each run  
✅ **Non-destructive:** Records marked, not physically deleted  
✅ **Read-only:** Key file never modified by framework  
✅ **Observable:** Result metadata for logging  

---

## 🆘 Troubleshooting

### Issue: Key file not found

**Solution:**
```python
# Verify path exists
import os
assert os.path.exists("/Volumes/main/test/deletes/customer_deletion_keys.parquet")
```

### Issue: Column mismatch

**Check:**
```python
# Verify key file has is_marked_deleted column
key_df = spark.read.parquet("/path/to/key/file.parquet")
assert "is_marked_deleted" in key_df.columns
assert "customer_id" in key_df.columns
```

### Issue: No rows soft-deleted

**Check:**
```python
# Verify key file has rows with is_marked_deleted = true
key_df = spark.read.parquet("/path/to/key/file.parquet")
marked = key_df.filter("is_marked_deleted = true").count()
print(f"Rows marked for deletion: {marked}")
assert marked > 0
```

---

## 📚 Related Documentation

- [Soft-Delete Design](../../../.claude/projects/*/memory/SOFTDELETE_DESIGN_SUMMARY.md)
- [Implementation Details](../../../.claude/projects/*/memory/SOFTDELETE_IMPLEMENTATION_COMPLETE.md)
- [Processor Module](../../../src/flowx/lakeflow_framework/soft_delete/processor.py)
- [Unit Tests](../test_soft_delete_processor.py)
- [Integration Tests](../test_soft_delete_e2e.py)

---

## ✨ Summary

This test guide provides a complete, ready-to-use setup for validating the soft-delete feature:

1. **Configuration:** Single ingestion flow with soft-delete enabled
2. **Data:** 10 customer records + 2 key files (initial + incremental)
3. **Scenarios:** Initial delete, idempotency, incremental update, transformation filtering
4. **Verification:** SQL queries and Python assertions

Use this as a reference for testing soft-delete in your own pipelines! 🚀
