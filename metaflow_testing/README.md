# 🧪 Metaflow Testing Suite

This directory contains onboarding specifications, test data configurations, and the comprehensive Master Testing Plan for verifying the NextGen Metadata Framework (Metaflow) on Databricks Lakeflow Declarative Pipelines.

---

## 📑 Core Testing Artifacts

* **Master Testing Plan**: [`TESTING_PLAN.md`](file:///c:/Databricks/NextGen_Metadata_Framework/metaflow_testing/TESTING_PLAN.md) — 12-pillar test matrix, end-to-end execution pipeline, 40+ test cases in tables, supporting resources, and verification queries.
* **Scenario 001 Spec**: [`001_zip_file_onboarding.json`](file:///c:/Databricks/NextGen_Metadata_Framework/metaflow_testing/001_zip_file_onboarding.json) — 4 ZIP Ingestion Flows, 4-Way Inner Join Transformation, and Plain ZIP External Sink.
* **Scenario 002 Spec**: [`002_zerobus_data_load.json`](file:///c:/Databricks/NextGen_Metadata_Framework/metaflow_testing/002_zerobus_data_load.json) — Zerobus Delta Streaming Ingestion to Bronze.
* **Scenario 003 Spec**: [`003_autoload_recon_append.json`](file:///c:/Databricks/NextGen_Metadata_Framework/metaflow_testing/003_autoload_recon_append.json) — Auto Loader CSV Batch Ingestion, Cross-Dataset Reconciliation, and Automated Self-Healing.
* **Scenario 100 Spec**: [`100_zipcsv_onbaording.json`](file:///c:/Databricks/NextGen_Metadata_Framework/metaflow_testing/100_zipcsv_onbaording.json) — SCD1 CDC Ingestion, Quarantine DQ Routing, Archive Retention, and OpenTelemetry Observability Export.

---

## 🚀 Quick Run Guide

Deploy and execute all test scenarios with Databricks Asset Bundles (DAB):

```bash
# Deploy bundle
databricks bundle deploy --target dev

# Run test scenario workflows
databricks bundle run metaflow_test_001_job --target dev
databricks bundle run metaflow_test_002_003_job --target dev
databricks bundle run metaflow_test_100_job --target dev

# Run automated verification assertions
pytest tests/integration/ -v
```
