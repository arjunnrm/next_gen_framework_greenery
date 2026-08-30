"""Post-deployment verification for the ZIP ingestion pipeline (sales_enrichment.json).

Run ``databricks bundle run sample_pipelines_job`` (task ``run_zip_ingestion_sales_enrichment``)
before running these.
"""

CATALOG = "poc"
OUTPUT_TABLE = f"{CATALOG}.silver_sales_ops.enriched_sales_orders"


def test_output_table_has_all_orders_from_both_branches(spark):
    df = spark.table(OUTPUT_TABLE)
    order_ids = {row["order_id"] for row in df.select("order_id").collect()}
    assert order_ids == {"SO-E001", "SO-E002", "SO-E003", "SO-W001", "SO-W002"}


def test_join_enriched_every_row_with_customer_and_product_attributes(spark):
    df = spark.table(OUTPUT_TABLE)
    rows = df.collect()
    assert len(rows) == 5
    for row in rows:
        assert row["customer_name"] is not None
        assert row["product_name"] is not None
        assert row["line_total"] is not None


def test_line_total_computed_correctly(spark):
    row = spark.table(OUTPUT_TABLE).filter("order_id = 'SO-E001'").collect()[0]
    # SO-E001: product P100 (Widget, $9.99) x quantity 3
    assert abs(row["line_total"] - 29.97) < 0.01


def test_output_zip_was_created_in_egress_volume(volume_file_names):
    names = volume_file_names(f"/Volumes/{CATALOG}/egress/zip_ingestion_output")
    assert "enriched_sales_orders.zip" in names, f"expected output archive in listing, got: {names}"


def test_extracted_temp_files_were_cleaned_up(volume_exists, volume_file_names):
    extract_dir = f"/Volumes/{CATALOG}/landing/zip_ingestion_zone/extracted"
    if not volume_exists(extract_dir):
        return
    leftover_csvs = [f for f in volume_file_names(extract_dir) if f.endswith(".csv")]
    assert not leftover_csvs, f"extracted temp files were not cleaned up: {leftover_csvs}"
