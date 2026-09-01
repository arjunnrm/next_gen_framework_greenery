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

---

## 📚 Sample reference suite (`metaflow_sample`)

Six self-contained **reference** jobs (not tests) under [`resources/sample_jobs/`](file:///c:/Databricks/NextGen_Metadata_Framework/resources/sample_jobs/), with specs in [`samples/`](file:///c:/Databricks/NextGen_Metadata_Framework/metaflow_testing/samples/). Every asset - landing files, `_schemas` checkpoints, targets, quarantine tables, recon datasets, exports, observability output, and a copy of each job's own spec (`sample_configs/`) - is isolated in the single UC schema **`metaflow.metaflow_sample`**. Seeds slice the Databricks `samples` catalog deterministically and fall back to inline literals when it isn't shared into the workspace.

| Job | Shows |
|---|---|
| `metaflow_sample_01_multi_scd_job` | SCD1 + SCD2 + FULL_SNAPSHOT_CDC (strictly additive iterations) + an SCD3 transformation joining two of the group's own tables |
| `metaflow_sample_02_zip_ingestion_job` | Glob-filtered ZIP extraction (`source_zip_handling`) with quarantine DQ routing |
| `metaflow_sample_03_multi_table_recon_job` | Two concurrent loads + in-DAG reconciliation (`pipeline_audit_only`) with controlled drift, run/mismatch logging, and a volume observability export task |
| `metaflow_sample_04_export_encrypt_zip_job` | Two `pgp_zip` sinks staging CSV (`staged_file_format`, v1.6.0) into AES-256 password-protected ZIP exports via `post_export_archive.secret` |
| `metaflow_sample_05_encrypted_ingestion_job` | Ingesting AES-256 password-protected ZIPs decrypted on the fly via `pre_extraction_decryption.secret_passphrase` |
| `metaflow_sample_06_asn1_tap3_job` | `source_type: "asn1"` BER decoding against the **real GSMA TAP release 3.10** module (`BT_Testing/TAP.310.asn1`, PDU `Notification`), undecodable payloads quarantined |

### Seeding is one job, and it runs first

No sample job lands data of its own. Every fixture the suite consumes is produced by the single **`metaflow_sample_seed_job`** ([`resources/sample_jobs/metaflow_sample_seed_job.yml`](file:///c:/Databricks/NextGen_Metadata_Framework/resources/sample_jobs/metaflow_sample_seed_job.yml)) - one chain of three iteration tasks per sample, six chains in parallel, behind a serial `provision_sample_schema` root that creates the schema and its four Volumes before anything fans out (UC's `CREATE ... IF NOT EXISTS` is idempotent in intent but not atomic, so six concurrent creates genuinely race). Iterations inside a chain stay strictly ordered because several seeds are cumulative by design.

```bash
# 1. Seed everything, once: 6 samples x 3 iterations.
databricks bundle run metaflow_sample_seed_job -t dev_metaflow -p dev_metaflow

# 2. Then any sample job, in any order.
databricks bundle run metaflow_sample_01_multi_scd_job -t dev_metaflow -p dev_metaflow
```

Each sample job is then just `setup_control_tables` -> `onboard_sample_NN` -> `run_pipeline` -> `store_sample_config` (Sample 03 adds `observability_export`), onboarding strictly through the generic `onboarding_job`, and storing its spec JSON into the one Volume `/Volumes/metaflow/metaflow_sample/sample_configs/` as its last task.

Because all three iterations are seeded before a pipeline first runs, that **one** update ingests them together - which is why each sample job runs its pipeline once, not three times. The end state is unchanged (`apply_changes` still sequences SCD1/SCD2 versions within the single batch); what is no longer observable is the update-by-update progression. To watch a sample evolve, run the seed job's chain one iteration at a time with the sample's pipeline in between.

**UC secret prerequisite (Samples 04 & 05 only, one secret serves both):** the Unity Catalog secret `metaflow.metaflow_sample.sample_zip_passkey` must exist before the seed job's Sample 05 chain runs (both seed notebooks fail fast with a clear message if it doesn't; the other four chains are unaffected). Provision it once per workspace - a manual, admin-audited step external to the bundle; any non-empty string works, `pyzipper` derives the AES key from the passphrase - following the same `databricks secrets put-secret` convention the repo already documents for `security.*` secrets (see `resources/bt_tests/metaflow_test_ing_005_pgp_decrypt_job.yml` and the UC three-level model resolved via `dbutils.secrets.get(catalog=, schema=, key=)`), and grant the running principal `READ SECRET` on it. Samples 01, 02, 03 and 06 have **no** manual prerequisite.
