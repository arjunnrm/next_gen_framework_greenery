"""Post-deployment verification for Test Pipeline 1 (Volume -> SCD1/SCD2, spec_07).

These tests assert against tables *already materialized* by a real
``databricks bundle run sample_pipelines_job`` pass -- they do not themselves trigger the
pipeline (Lakeflow Declarative Pipelines cannot run locally; see
docs/06_governance_integration.md and the note in tests/conftest.py). Run the job (day1
seed data, then again after dropping sample_customer_profile_day2.csv into
customer_ops_raw_zone/customer_profile/) before running these.
"""

import pytest

CATALOG = "poc"
SILVER_SCHEMA = f"{CATALOG}.silver_customer_ops"


def _table_exists(spark, qualified_table: str) -> bool:
    try:
        spark.table(qualified_table)
        return True
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture()
def day2_has_run(spark):
    """Skip day2-dependent assertions if the pipeline has only ever seen day1 data."""
    rows = spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd1").filter("customer_id = 'C006'").count()
    return rows > 0


def test_all_five_target_tables_exist(spark):
    for table in [
        "dim_customer_profile_scd1",
        "dim_customer_tier_scd1",
        "dim_customer_region_scd1",
        "dim_customer_profile_scd2",
        "dim_customer_contact_scd2",
    ]:
        assert _table_exists(spark, f"{SILVER_SCHEMA}.{table}"), f"expected table {table} to exist after pipeline run"


def test_scd1_has_exactly_one_row_per_customer_no_history(spark):
    df = spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd1")
    total = df.count()
    distinct_customers = df.select("customer_id").distinct().count()
    assert total == distinct_customers, "SCD1 must never keep more than one row per business key"


def test_scd1_reflects_latest_attribute_values_after_day2(spark, day2_has_run):
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    row = spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd1").filter("customer_id = 'C001'").collect()[0]
    assert row["tier"] == "PLATINUM", "SCD1 must overwrite with the latest value, not keep the original"


def test_scd1_new_customer_inserted_on_day2(spark, day2_has_run):
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    row = spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd1").filter("customer_id = 'C006'").collect()
    assert len(row) == 1
    assert row[0]["tier"] == "BRONZE"


def test_scd2_history_has_two_versions_for_changed_customer(spark, day2_has_run):
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    history = (
        spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd2")
        .filter("customer_id = 'C001'")
        .orderBy("__START_AT")
        .collect()
    )
    assert len(history) == 2, "C001's tier change (GOLD -> PLATINUM) must open a new SCD2 version"
    assert history[0]["tier"] == "GOLD"
    assert history[1]["tier"] == "PLATINUM"
    assert history[0]["__END_AT"] is not None, "the closed (non-current) version must have __END_AT set"
    assert history[1]["__END_AT"] is None, "the current version must have __END_AT null"


def test_scd2_unchanged_customer_has_exactly_one_version(spark, day2_has_run):
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    # C004's tracked columns (city, country, tier) never change between day1 and day2.
    history = spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd2").filter("customer_id = 'C004'").collect()
    assert len(history) == 1


def test_scd2_reporting_view_exposes_valid_from_valid_to_is_current(spark, day2_has_run):
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    report = (
        spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd2_current")
        .filter("customer_id = 'C001'")
        .collect()
    )
    current_rows = [r for r in report if r["is_current"]]
    assert len(current_rows) == 1
    assert current_rows[0]["tier"] == "PLATINUM"
    assert current_rows[0]["valid_to"] is None
    assert current_rows[0]["valid_from"] is not None


def test_contact_scd2_isolates_email_history_from_profile_scd2(spark, day2_has_run):
    """C003's email change should open a new version in the *contact* SCD2 table but not
    the profile SCD2 table (email isn't in dim_customer_profile_scd2's columns_to_check)."""
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    contact_history = spark.table(f"{SILVER_SCHEMA}.dim_customer_contact_scd2").filter("customer_id = 'C003'").collect()
    profile_history = spark.table(f"{SILVER_SCHEMA}.dim_customer_profile_scd2").filter("customer_id = 'C003'").collect()
    assert len(contact_history) == 2, "email change must open a new contact-history version"
    assert len(profile_history) == 1, "email is not a tracked column for the profile SCD2 table"
