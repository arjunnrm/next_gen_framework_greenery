"""Automated dead-code and legacy artifact cleanup script for NextGen_Metadata_Framework.

Performs static analysis-backed removal of obsolete scaffold files, empty directories,
and unifies documentation directories safely.
"""

import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Files identified as safe to delete (dead code / DAB template scaffold)
DEAD_FILES = [
    ROOT / "src" / "NextGen_Metadata_Framework" / "main.py",
    ROOT / "src" / "NextGen_Metadata_Framework" / "taxis.py",
]

# Empty directories safe to delete
EMPTY_DIRS = [
    ROOT / "notebooks" / "07_verification",
]

# Legacy docs to archive into docs/archive/legacy_docs/
DOCS_DIR = ROOT / "docs"
ARCHIVE_DIR = DOCS_DIR / "archive" / "legacy_docs"

LEGACY_DOC_PATTERNS = [
    "01_control_metadata_schema.md",
    "02_cdc_load_strategies.md",
    "03_engine_execution_flow.md",
    "04_onboarding_validation.md",
    "05_deployment_guide.md",
    "06_governance_integration.md",
    "07_reconciliation.md",
    "08_test_pipeline_1_volume_scd.md",
    "09_onboarding_yaml_json.md",
    "10_test_pipeline_2_streaming_cdc.md",
    "12_zip_ingestion_pipeline.md",
    "13_asn1_dq_quarantine.md",
    "14_core_functionality_verification.md",
    "15_engine_refactor.md",
    "16_encryption_and_secrets.md",
    "17_onboarding_template_reference.md",
    "18_test_pipeline_scd1_wide_table_column_exclusion.md",
    "19_bt_group_test_suite.md",
    "20_crypto_abac_exhaustive_test_suite.md",
    "21_auto_ttl_row_expiration.md",
    "22_ingestion_pgp_zip_json_standardization.md",
    "23_lakeflow_sinks.md",
    "25_dlt_observability_module.md",
    "26_dlt_observability_testing_runbook.md",
    "27_dlt_observability_onboarding_reference.md",
    "28_ingestion_schema_config.md",
    "29_test_pipeline_otel_streaming.md",
    "30_test_pipeline_recon_features.md",
]


def cleanup_dead_files():
    print("--> Purging dead scaffold files...")
    for file_path in DEAD_FILES:
        if file_path.exists():
            file_path.unlink()
            print(f"  [DELETED] {file_path.relative_to(ROOT)}")
        else:
            print(f"  [SKIPPED] {file_path.relative_to(ROOT)} (not found)")


def cleanup_empty_dirs():
    print("--> Purging empty directories...")
    for dir_path in EMPTY_DIRS:
        if dir_path.exists():
            try:
                dir_path.rmdir()
                print(f"  [REMOVED DIR] {dir_path.relative_to(ROOT)}")
            except OSError:
                print(f"  [WARNING] {dir_path.relative_to(ROOT)} is not empty.")


def archive_legacy_docs():
    print("--> Archiving legacy documentation...")
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    for doc_name in LEGACY_DOC_PATTERNS:
        source_file = DOCS_DIR / doc_name
        if source_file.exists():
            dest_file = ARCHIVE_DIR / doc_name
            shutil.move(str(source_file), str(dest_file))
            print(f"  [ARCHIVED] {doc_name} -> docs/archive/legacy_docs/")

    # Also archive 31_tc_*.md to 70_tc_*.md runbooks
    for tc_file in DOCS_DIR.glob("*_tc_*.md"):
        dest_file = ARCHIVE_DIR / tc_file.name
        shutil.move(str(tc_file), str(dest_file))
        print(f"  [ARCHIVED RUNBOOK] {tc_file.name} -> docs/archive/legacy_docs/")


def cleanup_docv2():
    docv2_dir = ROOT / "docv2"
    if docv2_dir.exists():
        print("--> Cleaning up superseded docv2/ directory...")
        shutil.rmtree(str(docv2_dir))
        print("  [REMOVED DIR] docv2/")


def main():
    print("========================================")
    print("Metaflow Dead-Code & Legacy Cleanup")
    print("========================================")
    cleanup_dead_files()
    cleanup_empty_dirs()
    archive_legacy_docs()
    cleanup_docv2()
    print("\nCleanup completed successfully.")


if __name__ == "__main__":
    main()
