"""Post-deployment verification for spec_25 (source_config.source_zip_handling.
pre_extraction_decryption, type='pgp') -- the first live pipeline run to ever exercise the
framework's ingestion-side PGP-decrypt-then-unzip path.

Before this scenario existed, a repo-wide grep of test_specs/*.json found zero specs using
pre_extraction_decryption -- a real coverage gap given the project's heavy emphasis on
distributed PGP+ZIP ingestion handling (onboarding_templates/pipeline_onboarding_template.json
documents the config shape via its df_template_asn1_ingest example, but that template was
never itself deployed). This test proves the whole real-world shape actually works end to
end against a live workspace: a plaintext CSV was AES-256-password-protected-ZIPped, then the
whole ZIP was PGP-encrypted for a dedicated test keypair (private key provisioned as UC secret
poc.security.ingestion_pgp_test_private_key, ZIP password as
poc.security.ingestion_pgp_test_zip_password) and uploaded to
/Volumes/poc/landing/pgp_decrypt_test_zone/incoming/pgp_decrypt_test_batch.zip.pgp.
ingestion/readers.py::_apply_source_zip_handling decrypts it (crypto/pgp.py::pgp_decrypt),
hands the now-plain ZIP to archive/zip_utils.py::extract_encrypted_zip (AES password from the
second secret), and Auto Loader ingests the extracted CSV -- all inside one Lakeflow
Declarative Pipeline update, no separate job task, no driver-side collection.

Run ``databricks bundle run pgp_zip_decrypt_test_job --target dev`` (see
resources/pgp_zip_decrypt_test_job.yml) before running these -- these tests assert against
tables/Volume paths *already materialized* by that real run; Lakeflow Declarative Pipelines
cannot run locally (see the note in tests/conftest.py and the analogous docstring in
tests/integration/test_pipeline1_scd.py).

Known-plaintext fixture content (see test_specs/spec_25_ingestion_pgp_zip_decrypt.json's
source_description for how it was built) -- five rows, record_id PGP-001..PGP-005.
"""

CATALOG = "poc"
TARGET_TABLE = f"{CATALOG}.bronze_pgp_test.decrypted_records"

EXPECTED_RECORDS = {
    "PGP-001": {"customer_name": "Ada Lovelace", "amount": 120.50, "region": "US-EAST"},
    "PGP-002": {"customer_name": "Grace Hopper", "amount": 75.25, "region": "US-WEST"},
    "PGP-003": {"customer_name": "Alan Turing", "amount": 300.00, "region": "EU-CENTRAL"},
    "PGP-004": {"customer_name": "Katherine Johnson", "amount": 45.10, "region": "US-EAST"},
    "PGP-005": {"customer_name": "Margaret Hamilton", "amount": 999.99, "region": "US-WEST"},
}


def test_target_table_exists(table_exists):
    assert table_exists(TARGET_TABLE), f"expected {TARGET_TABLE} to exist after a real pipeline run"


def test_all_five_pgp_decrypted_records_landed(spark):
    """Proves the PGP-decrypt-then-unzip path actually ran: the target table would be empty
    or missing entirely if pre_extraction_decryption had failed (raising ArchiveError) or
    silently no-op'd (e.g. Auto Loader finding no files because extraction never happened)."""
    rows = spark.table(TARGET_TABLE).collect()
    record_ids = {row["record_id"] for row in rows}
    assert record_ids == set(EXPECTED_RECORDS), (
        f"expected exactly the 5 known-plaintext record_ids from the PGP+ZIP fixture, got {sorted(record_ids)}"
    )


def test_decrypted_content_matches_known_plaintext_fixture(spark):
    """Byte-for-byte (well, field-for-field) proof that decryption didn't corrupt or
    truncate the payload -- compares every landed row against the exact plaintext CSV values
    encrypted into the fixture (see test_specs/spec_25_ingestion_pgp_zip_decrypt.json)."""
    rows = {row["record_id"]: row for row in spark.table(TARGET_TABLE).collect()}
    for record_id, expected in EXPECTED_RECORDS.items():
        assert record_id in rows, f"missing record {record_id}"
        row = rows[record_id]
        assert row["customer_name"] == expected["customer_name"]
        assert abs(row["amount"] - expected["amount"]) < 0.01
        assert row["region"] == expected["region"]


def test_extracted_zip_landed_in_target_volume_path(volume_exists, volume_file_names):
    """Confirms the intermediate step actually happened: pre_extraction_decryption produced a
    plain (now-decrypted) ZIP that extract_encrypted_zip then unpacked into
    target_volume_path -- if PGP decryption had failed, this directory would never be
    populated at all (ArchiveError aborts before extraction, see readers.py's
    _apply_source_zip_handling)."""
    extract_dir = "/Volumes/poc/landing/pgp_decrypt_test_zone/extracted"
    assert volume_exists(extract_dir), f"expected {extract_dir} to exist after a real pipeline run"
    names = volume_file_names(extract_dir)
    assert any(n.endswith(".csv") for n in names), f"expected the decrypted+unzipped CSV under {extract_dir}, got: {names}"


def test_source_pgp_encrypted_file_still_present(volume_file_names):
    """spec_25 sets delete_source_after_extract: false (unlike the other source_zip_handling
    examples in this repo, which default to true) specifically so this scenario's fixture can
    be re-run/re-verified without re-uploading it via `databricks fs cp` every time -- this
    assertion locks that deliberate choice in as a real regression guard."""
    names = volume_file_names("/Volumes/poc/landing/pgp_decrypt_test_zone/incoming")
    assert "pgp_decrypt_test_batch.zip.pgp" in names, f"expected the source PGP+ZIP fixture to remain, got: {names}"
