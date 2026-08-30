# 🧹 Metaflow Codebase Audit & Dead-Code Deprecation Manifest

> **Purpose**: Formal static analysis and deprecation manifest detailing orphaned modules, dead code paths, obsolete configuration templates, and superseded documentation files across the repository.

---

## 1. Audit Scope & Static Analysis Methodology

Static analysis was performed across all directories within `NextGen_Metadata_Framework`, evaluating:
- AST symbol call graphs and import statements across `src/`, `notebooks/`, `tests/`, and `resources/`.
- Dependency references in `pyproject.toml` and `databricks.yml`.
- Direct file references across all test specifications and documentation.

### Standard Exclusion Filter Applied
The following transient and cache directories were excluded from the analysis:
- `.git/`, `.venv/`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`, `dist/archive/`.

---

## 2. Deprecation & Cleanup Manifest

| Target File / Path | Category | Status | Dependencies / References | Impact Assessment | Action Taken |
|---|---|---|---|---|---|
| `src/NextGen_Metadata_Framework/main.py` | Python Source | **Safe to Delete** | Formerly `pyproject.toml:31` | Leftover DAB init template querying sample NYC taxi data (`samples.nyctaxi.trips`). Zero internal framework usage. | **Deleted**; removed `[project.scripts]` from `pyproject.toml`. |
| `src/NextGen_Metadata_Framework/taxis.py` | Python Source | **Safe to Delete** | Formerly `main.py:3` | Leftover DAB init helper querying `samples.nyctaxi.trips`. Zero internal framework usage. | **Deleted**. |
| `notebooks/07_verification/` | Notebooks Dir | **Safe to Delete** | None | Empty directory. Verification suites migrated to unit/integration tests. | **Removed directory**. |
| `docv2/` (5 files) | Documentation | **Deprecated / Consolidated** | Internal documentation | All content consolidated and unified into canonical `docs/00_master_reference_index.md` through `docs/10_multi_role_faqs.md`. | **Merged into `docs/` and directory purged**. |
| `docs/01_control_metadata_schema.md` ... `docs/30_test_pipeline_recon_features.md` (28 files) | Documentation | **Deprecated / Archived** | `docs/README.md` | Legacy numbered deep-dive docs consolidated into the 10 domain modular docs in `docs/`. | **Archived** to `docs/archive/legacy_docs/`. |
| `docs/31_tc_ing_001.md` ... `docs/70_tc_prm_006.md` (39 files) | Documentation | **Deprecated / Archived** | `metaflow_testing/` | Legacy per-test markdown runbooks superseded by active specs in `metaflow_testing/*.json` and `metaflow_testing/TESTING_PLAN.md`. | **Archived** to `docs/archive/legacy_docs/`. |
| `test_specs/` (`spec_12_config_validation_negative.json`, `spec_22_optional_fields_df_customer_ingest.json`) | Test Fixtures | **Retained (Unit Test Mocks)** | `tests/unit/test_config_validation_negative_spec.py`, `tests/unit/test_optional_fields_df_customer_ingest_spec.py` | Required by static unit tests that assert on negative validation errors and optional field defaults. | **Retained** for unit testing. |

---

## 3. Step-by-Step Cleanup Commands

### Option A: Automated Python Execution (Recommended)
Run the automated cleanup script:
```powershell
python scripts/cleanup_dead_code.py
```

### Option B: Manual Step-by-Step PowerShell Commands
```powershell
# 1. Remove dead template scaffold files
Remove-Item -Path "src/NextGen_Metadata_Framework/main.py" -Force
Remove-Item -Path "src/NextGen_Metadata_Framework/taxis.py" -Force

# 2. Remove empty verification directory
Remove-Item -Path "notebooks/07_verification" -Recurse -Force -ErrorAction SilentlyContinue

# 3. Remove superseded docv2 directory
Remove-Item -Path "docv2" -Recurse -Force -ErrorAction SilentlyContinue

# 4. Create archive directory and move legacy markdown files
New-Item -ItemType Directory -Path "docs/archive/legacy_docs" -Force
Get-ChildItem -Path "docs" -Filter "*_tc_*.md" | Move-Item -Destination "docs/archive/legacy_docs" -Force
```

---

## 4. Post-Cleanup Verification Checklist
- [x] Run unit tests to confirm zero regressions: `uv run pytest tests/unit/ -v`
- [x] Build wheel package to verify clean build: `python scripts/bump_and_build.py`
- [x] Verify that `docs/` is clean, containing only the 10 domain modules, master reference index, and README.
