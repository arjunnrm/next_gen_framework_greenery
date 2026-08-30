"""Post-deployment verification for Test Pipeline 2 (Zerobus stream -> encrypted SCD1/SCD2, spec_08).

Like test_pipeline1_scd.py, these assert against tables already materialized by a real
``databricks bundle run sample_pipelines_job`` pass. Batch-2-dependent assertions
auto-skip if only batch1 has been seeded/processed so far.
"""

import pytest

CATALOG = "poc"
BRONZE_SCHEMA = f"{CATALOG}.bronze_account_ops"
SILVER_SCHEMA = f"{CATALOG}.silver_account_ops"


def _table_exists(spark, qualified_table: str) -> bool:
    try:
        spark.table(qualified_table)
        return True
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture()
def batch2_has_run(spark):
    rows = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd1").filter("account_id = 'A006'").count()
    return rows > 0


def test_all_five_target_tables_exist(spark):
    for table in [
        "dim_account_profile_scd1",
        "dim_account_balance_scd1",
        "dim_account_status_scd1",
        "dim_account_profile_scd2",
        "dim_account_contact_pref_scd2",
    ]:
        assert _table_exists(spark, f"{SILVER_SCHEMA}.{table}"), f"expected table {table} to exist after pipeline run"


def test_bronze_ssn_and_email_are_encrypted_not_plaintext(spark):
    row = spark.table(f"{BRONZE_SCHEMA}.raw_account_events").filter("account_id = 'A001'").collect()[0]
    assert row["ssn"] != "111-11-1111", "ssn must be ciphertext at rest, not the original plaintext"
    assert row["email"] != "acme@example.com", "email must be ciphertext at rest, not the original plaintext"


def test_scd1_reflects_batch1_state_before_batch2(spark):
    df = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd1")
    assert df.count() >= 5
    row = df.filter("account_id = 'A004'").collect()[0]
    assert row["status"] == "SUSPENDED"


def test_scd1_balance_update_applied(spark, batch2_has_run):
    if not batch2_has_run:
        pytest.skip("batch2 not yet seeded/run")
    row = spark.table(f"{SILVER_SCHEMA}.dim_account_balance_scd1").filter("account_id = 'A001'").collect()[0]
    assert float(row["balance"]) == 15000.00, "SCD1 must reflect the batch2 balance update, not the original"


def test_scd1_delete_removes_the_row(spark, batch2_has_run):
    if not batch2_has_run:
        pytest.skip("batch2 not yet seeded/run")
    rows = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd1").filter("account_id = 'A002'").collect()
    assert len(rows) == 0, "apply_as_deletes must remove A002 (op='D' in batch2) from the SCD1 target"


def test_scd1_new_insert_is_present(spark, batch2_has_run):
    if not batch2_has_run:
        pytest.skip("batch2 not yet seeded/run")
    rows = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd1").filter("account_id = 'A006'").collect()
    assert len(rows) == 1
    assert rows[0]["account_name"] == "Zeta Partners"


def test_scd2_delete_closes_current_version_without_reopening(spark, batch2_has_run):
    if not batch2_has_run:
        pytest.skip("batch2 not yet seeded/run")
    history = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd2").filter("account_id = 'A002'").collect()
    open_versions = [r for r in history if r["__END_AT"] is None]
    assert len(open_versions) == 0, "a deleted key must have no open (current) SCD2 version"


def test_scd2_balance_change_opens_new_version(spark, batch2_has_run):
    if not batch2_has_run:
        pytest.skip("batch2 not yet seeded/run")
    history = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd2").filter("account_id = 'A001'").collect()
    assert len(history) == 2, "A001's balance change must open a second SCD2 version"


def test_scd2_encrypted_columns_are_passthrough_not_tracked(spark, batch2_has_run):
    """ssn/email are NOT in columns_to_check -- encrypting them (non-deterministic GCM,
    different ciphertext per call) must never by itself open a spurious new SCD2 version."""
    if not batch2_has_run:
        pytest.skip("batch2 not yet seeded/run")
    # A004's tracked columns (balance, status) never change between batch1 and batch2 --
    # if encryption randomness leaked into change-detection, this would incorrectly show 2+.
    history = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd2").filter("account_id = 'A004'").collect()
    assert len(history) == 1


def test_contact_pref_scd2_isolated_from_profile_scd2(spark, batch2_has_run):
    """A003's contact_channel change (SMS -> EMAIL) must open a version in the contact-pref
    table but not the profile SCD2 table (contact_channel isn't tracked there)."""
    if not batch2_has_run:
        pytest.skip("batch2 not yet seeded/run")
    contact_history = spark.table(f"{SILVER_SCHEMA}.dim_account_contact_pref_scd2").filter("account_id = 'A003'").collect()
    profile_history = spark.table(f"{SILVER_SCHEMA}.dim_account_profile_scd2").filter("account_id = 'A003'").collect()
    assert len(contact_history) == 2
    assert len(profile_history) == 1
