# 🚀 Metaflow

A metadata-driven, enterprise-grade data ingestion, transformation, reconciliation, and observability framework built on **Databricks Lakeflow Pipelines (Delta Live Tables)**, **Unity Catalog**, and **Databricks Asset Bundles (DABs)**.

---

## 📁 Repository Directory Structure

```
flowx/
├── src/                                  # Canonical Framework Source Code
│   └── flowx/
│       └── lakeflow_framework/           # 15 Core Framework Submodules
│           ├── archive/                  # PGP & AES-256 ZIP handling & ingestion
│           ├── asn1/                     # ASN.1 BER/DER binary CDR decoding
│           ├── cdc/                      # SCD1/2/3, Append, Truncate, Snapshot CDC
│           ├── control_plane/            # Metadata repository & schema provisioner
│           ├── crypto/                   # AES column encryption, SHA-256 hashing, PGP
│           ├── dq/                       # Data Quality expectations & quarantine
│           ├── engine/                   # Dynamic Lakeflow flow & sink registration
│           ├── governance/               # Unity Catalog tag propagation & ABAC
│           ├── ingestion/                # Auto Loader, JSON flattening, column norm
│           ├── observability/            # OTel payload builder, streaming OTel sinks
│           ├── onboarding/               # Spec validator, loader & bulk onboarding
│           ├── reconciliation/           # Cross-dataset matching & drift detection
│           ├── storage/                  # Column ordering & table properties
│           ├── transformation/           # Dynamic SQL & parameter substitution
│           └── exceptions.py             # Central domain exceptions hierarchy
│
├── resources/                            # Databricks Asset Bundle (DAB) Resources
│   ├── flowx_app/                     # Onboarding App + the UC Volume it writes specs to
│   ├── flowx_config_jobs/             # onboarding_job (one spec) + bulk (a whole spec_dir)
│   ├── observability/                    # DLT observability export job + OTEL streaming pipeline
│   ├── bt_tests/                         # Tests on real BT fixtures (geneva, ASN.1, PGP)
│   ├── feature_tests/                    # TC-* feature/regression corpus (job + pipeline per case)
│   ├── sample_jobs/                      # flowx_sample reference suite: 6 jobs + 6 pipelines + 1 common seed job
│   └── stability_tests/                  # Reserved for STABILITY_TEST_PLAN.md's A1-G4 (empty)
│
├── flowx_testing/                     # Comprehensive Testing Suite Specs
│   ├── *.json                            # 48 end-to-end test configuration specs
│   ├── TESTING_PLAN.md                   # Formal multi-phase test execution plan
│   └── TESTING_STATUS.md                 # Real-time test execution matrix
│
├── flowx-onboarding-app-old/          # Onboarding Web UI Application (Reference)
├── onboarding_templates/                 # JSON Schemas and YAML Onboarding Templates
├── notebooks/                            # Interactive Databricks Notebooks (00-08)
├── sample_data/                          # Synthetic test datasets & fixtures
├── scripts/                              # Wheel build & UC volume upload utilities
├── docs/                                 # 12 Modular Architecture & Reference Specs
├── enhancement_logs/                     # Enhancement logs & release audits
├── tests/                                # Pytest Unit & Integration Test Suites
│   ├── unit/                             # Fast local unit test suites
│   └── integration/                      # Databricks Connect integration tests
├── databricks.yml                        # Root DAB deployment configuration
├── pyproject.toml                        # Project dependencies and build system
└── RELEASE_NOTES.md                      # Release notes & semantic version history
```

---

## ⚡ Key Capabilities

1. **Declarative Metadata Onboarding**: Onboard dataflows via JSON/YAML specifications with comprehensive JSON Schema validation and pre-flight Unity Catalog authorization checks.
2. **Exhaustive CDC Load Strategies**: Native support for `APPEND`, `TRUNCATE_AND_LOAD`, `SCD1`, `SCD2`, `SCD3`, and `FULL_SNAPSHOT_CDC` — all built on Databricks-native `apply_changes` / `apply_changes_from_snapshot`. (`FULL_SNAPSHOT_CDC_NO_PK` and the surrogate-key engine it depended on were removed in v1.4.0; a keyless source uses `TRUNCATE_AND_LOAD`.)
3. **Enterprise Security & Cryptography**: Column-level AES-GCM encryption, SHA-256 deterministic hashing, PGP decryption/signing, and automated UC table/column tagging.
4. **Binary & Complex Ingestion**: High-throughput ASN.1 BER/DER CDR decoding via PySpark `mapInPandas`, nested JSON recursive flattening, and encrypted ZIP decompression.
5. **Reconciliation & Self-Healing**: Continuous and triggered cross-dataset reconciliation with two-tier diffing (Phase 1 fast hash fingerprint + Phase 2 value drift classification).
6. **OpenTelemetry (OTel) Observability**: Real-time DLT Event Log extraction and structured dispatch to Unity Catalog Volumes and external OTel collectors.

---

## 🛠️ Getting Started & Build Commands

### 1. Build and Publish Framework Wheel
The framework wheel is published to a Unity Catalog Volume to guarantee safe concurrent pipeline updates:
```bash
# Build wheel locally
python scripts/bump_and_build.py

# Build and upload to Unity Catalog Volume
python scripts/build_and_upload_wheel.py --profile dev_flowx --catalog flowx
```

### 2. Deploy via Databricks Asset Bundles (DABs)
```bash
# Deploy development bundle
databricks bundle deploy --target dev_flowx -p dev_flowx

# Run a test job
databricks bundle run flowx_test_cdc_003_scd1_job -p dev_flowx
```

### 3. Run Automated Tests
```bash
# Run local unit tests
.venv/Scripts/pytest.exe tests/unit/

# Run specific unit test suite
.venv/Scripts/pytest.exe tests/unit/test_spec_validator.py -v
```

---

## 📚 Documentation

For exhaustive architecture guides, developer cookbooks, and parameter references, see the [`docs/`](docs/) directory:
- [📖 Master Reference Index](docs/00_master_reference_index.md)
- [🏗️ Platform Architecture](docs/01_platform_architecture.md)
- [📥 Ingestion & Sources](docs/02_ingestion_and_sources.md)
- [🔄 Transformation & CDC Engine](docs/03_transformation_and_cdc.md)
- [🛡️ Data Quality & Governance](docs/04_data_quality_and_governance.md)
- [🔐 Security & Cryptography](docs/05_security_and_cryptography.md)
- [⚖️ Reconciliation Engine](docs/07_reconciliation_engine.md)
- [📊 Observability & Telemetry](docs/08_observability_and_telemetry.md)
- [🔐 Canonical Deterministic Hashing](docs/11_hashing_and_determinism.md)
- [🧩 Module Permutation Matrix](docs/12_module_permutation_matrix.md)
