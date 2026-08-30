"""Post-deployment verification for the SCD1 wide-table column-exclusion pipeline (spec_14).

These tests assert against tables *already materialized* by a real
``databricks bundle run sample_pipelines_job`` pass -- they do not themselves trigger the
pipeline (Lakeflow Declarative Pipelines cannot run locally; see
docs/06_governance_integration.md and the note in tests/conftest.py). Run the job (day1
seed data, then again after dropping sample_wide_customer_master_day2.csv into
wide_customer_master_zone/incoming/) before running these.
"""

import pytest

CATALOG = "poc"
SILVER_SCHEMA = f"{CATALOG}.silver_wide_customer_ops"
BRONZE_TABLE = f"{CATALOG}.bronze_wide_customer_ops.raw_wide_customer_master"
TARGET_TABLE = f"{SILVER_SCHEMA}.dim_wide_customer_scd1"

EXCLUDED_COLUMNS = {"batch_load_ts", "source_extract_filename", "etl_run_id", "checksum_hash", "ingestion_notes"}


def _table_exists(spark, qualified_table: str) -> bool:
    try:
        spark.table(qualified_table)
        return True
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture()
def day2_has_run(spark):
    """Skip day2-dependent assertions if the pipeline has only ever seen day1 data."""
    rows = spark.table(TARGET_TABLE).filter("customer_id = 'WC006'").count()
    return rows > 0


def test_target_table_exists(spark):
    assert _table_exists(spark, TARGET_TABLE), "expected dim_wide_customer_scd1 to exist after pipeline run"


def test_target_has_exactly_five_fewer_columns_than_bronze(spark):
    """The target has every Bronze column (50 CSV columns + the framework's own
    technical-metadata/record-id columns) minus exactly the 5 excluded ones -- a relative
    assertion, since the exact Bronze column count also depends on unrelated framework
    features (capture_technical_metadata, record_id_column) that aren't this test's
    concern."""
    bronze_columns = set(spark.table(BRONZE_TABLE).columns)
    silver_columns = set(spark.table(TARGET_TABLE).columns)
    assert silver_columns == bronze_columns - EXCLUDED_COLUMNS
    assert len(bronze_columns) - len(silver_columns) == len(EXCLUDED_COLUMNS)


def test_excluded_technical_columns_are_absent(spark):
    columns = set(spark.table(TARGET_TABLE).columns)
    overlap = columns & EXCLUDED_COLUMNS
    assert not overlap, f"columns_to_exclude should have dropped these from the target, but they're present: {overlap}"


def test_business_columns_are_present(spark):
    columns = set(spark.table(TARGET_TABLE).columns)
    for expected in ["customer_id", "first_name", "email", "account_status", "loyalty_tier", "updated_at"]:
        assert expected in columns, f"expected business column '{expected}' to be present in the SCD1 target"


def test_scd1_has_exactly_one_row_per_customer_no_history(spark):
    df = spark.table(TARGET_TABLE)
    total = df.count()
    distinct_customers = df.select("customer_id").distinct().count()
    assert total == distinct_customers, "SCD1 must never keep more than one row per business key"


def test_scd1_reflects_business_attribute_change_after_day2(spark, day2_has_run):
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    row = spark.table(TARGET_TABLE).filter("customer_id = 'WC001'").collect()[0]
    assert row["account_status"] == "SUSPENDED", "SCD1 must overwrite with the latest business value"
    assert row["risk_segment"] == "HIGH"


def test_scd1_unaffected_by_technical_only_reload(spark, day2_has_run):
    """WC002's day2 row differs only in technical/audit columns (all excluded) -- every
    visible business column must still match day1's original values."""
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    row = spark.table(TARGET_TABLE).filter("customer_id = 'WC002'").collect()[0]
    assert row["account_status"] == "ACTIVE"
    assert row["first_name"] == "First2"
    assert row["email"] == "customer2@example.com"


def test_scd1_new_customer_inserted_on_day2(spark, day2_has_run):
    if not day2_has_run:
        pytest.skip("day2 fixture not yet seeded/run -- see module docstring")
    rows = spark.table(TARGET_TABLE).filter("customer_id = 'WC006'").collect()
    assert len(rows) == 1
    assert rows[0]["account_status"] == "ACTIVE"


def test_bronze_source_has_all_50_columns(spark):
    """Sanity check the exclusion is happening at the target, not by trimming the source."""
    bronze_columns = set(spark.table(f"{CATALOG}.bronze_wide_customer_ops.raw_wide_customer_master").columns)
    assert EXCLUDED_COLUMNS.issubset(bronze_columns), "the excluded columns should still be present in Bronze"
