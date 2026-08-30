<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_app_docs.py; edit the source it derives from. -->

# Archive
!!! warning "Superseded material"
    Nothing on these pages is maintained. They are kept so the reasoning
    behind past decisions stays available and so old links keep resolving.
    For current behaviour use the **Architecture** and **JSON reference**
    sections.

## Superseded guides
Design and feature notes replaced by the Architecture and JSON reference sections. Kept for provenance — where they disagree with the living docs, the living docs are correct.

| Document | |
|---|---|
| [Control Metadata Schema Reference](legacy_docs/01_control_metadata_schema.md) | `01_control_metadata_schema` |
| [CDC / Load Strategies Reference](legacy_docs/02_cdc_load_strategies.md) | `02_cdc_load_strategies` |
| [Engine Execution Flow](legacy_docs/03_engine_execution_flow.md) | `03_engine_execution_flow` |
| [Onboarding & Validation](legacy_docs/04_onboarding_validation.md) | `04_onboarding_validation` |
| [Deployment Guide](legacy_docs/05_deployment_guide.md) | `05_deployment_guide` |
| [Governance Integration](legacy_docs/06_governance_integration.md) | `06_governance_integration` |
| [Reconciliation](legacy_docs/07_reconciliation.md) | `07_reconciliation` |
| [Test Pipeline 1: Volume -> SCD1/SCD2 (Customer 360)](legacy_docs/08_test_pipeline_1_volume_scd.md) | `08_test_pipeline_1_volume_scd` |
| [Onboarding: YAML and JSON Format Parity](legacy_docs/09_onboarding_yaml_json.md) | `09_onboarding_yaml_json` |
| [Test Pipeline 2: Zerobus Stream -> Encrypted SCD1/SCD2 (Account Events)](legacy_docs/10_test_pipeline_2_streaming_cdc.md) | `10_test_pipeline_2_streaming_cdc` |
| [ZIP Ingestion Pipeline](legacy_docs/12_zip_ingestion_pipeline.md) | `12_zip_ingestion_pipeline` |
| [ASN.1 Ingestion, DQ, and Quarantine](legacy_docs/13_asn1_dq_quarantine.md) | `13_asn1_dq_quarantine` |
| [Core Functionality Verification Suite](legacy_docs/14_core_functionality_verification.md) | `14_core_functionality_verification` |
| [Engine Refactor: Shared Flow Registration](legacy_docs/15_engine_refactor.md) | `15_engine_refactor` |
| [Encryption and Secrets](legacy_docs/16_encryption_and_secrets.md) | `16_encryption_and_secrets` |
| [Onboarding Template Field Reference](legacy_docs/17_onboarding_template_reference.md) | `17_onboarding_template_reference` |
| [Test Pipeline: SCD1 Wide-Table Column Exclusion (Customer Master)](legacy_docs/18_test_pipeline_scd1_wide_table_column_exclusion.md) | `18_test_pipeline_scd1_wide_table_column_exclusion` |
| [BT_Group Test Suite: UC001-UC005](legacy_docs/19_bt_group_test_suite.md) | `19_bt_group_test_suite` |
| [Exhaustive Encryption, Decryption, and Governance Tags Test Suite](legacy_docs/20_crypto_abac_exhaustive_test_suite.md) | `20_crypto_abac_exhaustive_test_suite` |
| [Auto TTL Row Expiration](legacy_docs/21_auto_ttl_row_expiration.md) | `21_auto_ttl_row_expiration` |
| [Ingestion: PGP/ZIP Archives, ASN.1 Distributed Decode, JSON Flattening, SQL Standardization](legacy_docs/22_ingestion_pgp_zip_json_standardization.md) | `22_ingestion_pgp_zip_json_standardization` |
| [Lakeflow Sinks: `target_type: "sink"` and `"external_sink"`](legacy_docs/23_lakeflow_sinks.md) | `23_lakeflow_sinks` |
| [DLT Observability Module](legacy_docs/25_dlt_observability_module.md) | `25_dlt_observability_module` |
| [DLT Observability: Testing & Validation Runbook](legacy_docs/26_dlt_observability_testing_runbook.md) | `26_dlt_observability_testing_runbook` |
| [DLT Observability: Onboarding Template & Attribute Reference](legacy_docs/27_dlt_observability_onboarding_reference.md) | `27_dlt_observability_onboarding_reference` |
| [Bronze Column Normalization & Explicit Schema Configuration](legacy_docs/28_ingestion_schema_config.md) | `28_ingestion_schema_config` |
| [Test Pipeline: Continuous Event-Log → OTel Streaming Export](legacy_docs/29_test_pipeline_otel_streaming.md) | `29_test_pipeline_otel_streaming` |
| [Test Pipeline: Reconciliation Feature Test (Scenario 004)](legacy_docs/30_test_pipeline_recon_features.md) | `30_test_pipeline_recon_features` |

## Test case catalogue
Point-in-time verification runbooks, one per test case, recording what was exercised and what the expected outcome was.

### Ingestion

[tc_ing_001](legacy_docs/31_tc_ing_001.md), [tc_ing_002](legacy_docs/32_tc_ing_002.md), [tc_ing_003](legacy_docs/33_tc_ing_003.md), [tc_ing_004](legacy_docs/34_tc_ing_004.md), [tc_ing_005](legacy_docs/35_tc_ing_005.md), [tc_ing_006](legacy_docs/36_tc_ing_006.md), [tc_ing_007](legacy_docs/37_tc_ing_007.md), [tc_ing_008](legacy_docs/38_tc_ing_008.md), [tc_ing_009](legacy_docs/39_tc_ing_009.md)

### CDC / snapshot

[tc_cdc_001](legacy_docs/40_tc_cdc_001.md), [tc_cdc_002](legacy_docs/41_tc_cdc_002.md), [tc_cdc_003](legacy_docs/42_tc_cdc_003.md), [tc_cdc_004](legacy_docs/43_tc_cdc_004.md), [tc_cdc_005](legacy_docs/44_tc_cdc_005.md), [tc_cdc_006](legacy_docs/45_tc_cdc_006.md), [tc_cdc_007](legacy_docs/46_tc_cdc_007.md)

### Transformation

[tc_trf_001](legacy_docs/47_tc_trf_001.md), [tc_trf_002](legacy_docs/48_tc_trf_002.md), [tc_trf_003](legacy_docs/49_tc_trf_003.md)

### Data quality

[tc_dq_001](legacy_docs/50_tc_dq_001.md), [tc_dq_002](legacy_docs/51_tc_dq_002.md), [tc_dq_003](legacy_docs/52_tc_dq_003.md), [tc_dq_004](legacy_docs/53_tc_dq_004.md)

### Security & crypto

[tc_sec_001](legacy_docs/54_tc_sec_001.md), [tc_sec_002](legacy_docs/55_tc_sec_002.md), [tc_sec_003](legacy_docs/56_tc_sec_003.md)

### Governance

[tc_gov_001](legacy_docs/57_tc_gov_001.md), [tc_gov_002](legacy_docs/58_tc_gov_002.md)

### Reconciliation

[tc_rec_002](legacy_docs/59_tc_rec_002.md), [tc_rec_003](legacy_docs/60_tc_rec_003.md)

### Sinks & egress

[tc_snk_001](legacy_docs/61_tc_snk_001.md), [tc_snk_002](legacy_docs/62_tc_snk_002.md), [tc_snk_003](legacy_docs/63_tc_snk_003.md)

### Observability

[tc_obs_001](legacy_docs/64_tc_obs_001.md), [tc_obs_003](legacy_docs/65_tc_obs_003.md)

### Filtering

[tc_flt_002](legacy_docs/66_tc_flt_002.md)

### Parameters

[tc_prm_001](legacy_docs/67_tc_prm_001.md), [tc_prm_004](legacy_docs/68_tc_prm_004.md), [tc_prm_005](legacy_docs/69_tc_prm_005.md), [tc_prm_006](legacy_docs/70_tc_prm_006.md)
