"""Post-deployment verification for the transformation_sql UNION ALL proof (spec_26).

Closes a real test-coverage gap: `transformation_sql` combining two `source_inputs[]` via
`UNION`/`UNION ALL` had never been exercised against a real, live pipeline targeting this
project's main `poc` catalog before this scenario -- a repo-wide grep found exactly one
prior `UNION` usage anywhere (`test_specs/spec_16_bt_uc002_archive_autoloader_egress.json`,
part of a separate, untouched legacy `BT_Group`-catalog suite), and it wasn't a dedicated
test of the mechanic itself. This asserts the specific streaming+streaming case: two
Auto-Loader-ingested Bronze tables (`bronze_union_test.orders_region_a/b`, both
`is_streaming: true` as `source_inputs`) combined by one transformation flow's
`SELECT ... FROM orders_region_a UNION ALL SELECT ... FROM orders_region_b` into
`silver_union_test.orders_combined`.

These tests assert against a table *already materialized* by a real
``databricks bundle run transformation_union_test_job`` pass -- they do not themselves
trigger the pipeline (Lakeflow Declarative Pipelines cannot run locally; see
docs/06_governance_integration.md and the note in tests/conftest.py).
"""

CATALOG = "poc"
SILVER_SCHEMA = f"{CATALOG}.silver_union_test"
COMBINED_TABLE = f"{SILVER_SCHEMA}.orders_combined"

# Matches sample_data/sample_orders_region_a.csv / sample_orders_region_b.csv exactly --
# deliberately unequal sizes (5 vs 6) so a passing row-count/id-set assertion can't be
# explained by coincidence (e.g. accidentally reading only one side twice).
REGION_A_ORDER_IDS = {"ORD-A-001", "ORD-A-002", "ORD-A-003", "ORD-A-004", "ORD-A-005"}
REGION_B_ORDER_IDS = {"ORD-B-001", "ORD-B-002", "ORD-B-003", "ORD-B-004", "ORD-B-005", "ORD-B-006"}


def test_bronze_tables_for_both_regions_exist(table_exists):
    assert table_exists(f"{CATALOG}.bronze_union_test.orders_region_a")
    assert table_exists(f"{CATALOG}.bronze_union_test.orders_region_b")


def test_combined_table_exists(table_exists):
    assert table_exists(COMBINED_TABLE), f"expected UNION ALL output table {COMBINED_TABLE} to exist after pipeline run"


def test_combined_row_count_equals_sum_of_both_sources(spark):
    """The core UNION ALL proof: no rows lost, none duplicated, none deduplicated away --
    UNION ALL (not UNION) must preserve every row from both branches verbatim."""
    df = spark.table(COMBINED_TABLE)
    assert df.count() == len(REGION_A_ORDER_IDS) + len(REGION_B_ORDER_IDS)


def test_combined_table_contains_every_order_id_from_both_regions(spark):
    df = spark.table(COMBINED_TABLE)
    order_ids = {row["order_id"] for row in df.select("order_id").collect()}
    assert order_ids == REGION_A_ORDER_IDS | REGION_B_ORDER_IDS


def test_combined_table_preserves_the_region_column_from_each_branch(spark):
    """Confirms rows genuinely came from both UNION ALL branches (not just one source
    read twice) -- each branch's own `region` column value survives into the output."""
    df = spark.table(COMBINED_TABLE)
    region_a_rows = df.filter("region = 'region_a'").collect()
    region_b_rows = df.filter("region = 'region_b'").collect()
    assert {r["order_id"] for r in region_a_rows} == REGION_A_ORDER_IDS
    assert {r["order_id"] for r in region_b_rows} == REGION_B_ORDER_IDS


def test_combined_table_has_no_unexpected_extra_or_missing_rows(spark):
    """Belt-and-suspenders on the row-count/id-set checks above: the combined table's
    schema-identical columns (order_ts, customer_id, product_id, quantity, unit_price)
    are all non-null for every row -- a UNION ALL gone wrong (e.g. a column-order mismatch
    silently coercing values into the wrong column) would routinely show up as nulls here."""
    df = spark.table(COMBINED_TABLE)
    for column in ("order_ts", "customer_id", "product_id", "quantity", "unit_price", "region"):
        null_count = df.filter(df[column].isNull()).count()
        assert null_count == 0, f"column '{column}' had {null_count} unexpected null(s) in {COMBINED_TABLE}"
