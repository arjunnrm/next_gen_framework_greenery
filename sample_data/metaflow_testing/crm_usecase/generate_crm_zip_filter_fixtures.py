"""Generates the CSV fixtures for TC-ING-001 (Selective Glob Pattern ZIP Extraction).

Run locally (no Databricks/Spark dependency): ``python generate_crm_zip_filter_fixtures.py``.

Produces two plain CSVs under this same directory:

* ``customer_data.csv`` -- 100 rows. Zipped on-cluster (by
  ``notebooks/00_seed_sample_data/03_seed_ing_001_zip_filter_data.py``) into
  ``customer_data_20260828.zip``, which the onboarding spec's
  ``source_zip_handling.zip_file_pattern: "customer_*.zip"`` IS expected to match and extract.
* ``vendor_feed.csv`` -- 50 rows. Zipped into ``vendor_feed_20260828.zip``, which the same glob
  pattern must NOT match -- proving the framework's ZIP filtering ignores non-matching archives
  entirely (left untouched in the incoming Volume, never extracted, never ingested).

See metaflow_testing/010_ing_001_zip_filter.json and docs/31_tc_ing_001.md.
"""

import csv
import os

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

REGIONS = ["US", "EU", "APAC", "LATAM"]
VENDOR_CATEGORIES = ["LOGISTICS", "PACKAGING", "RAW_MATERIALS", "IT_SERVICES"]


def _write_csv(filename: str, fieldnames: list, rows: list) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} row(s) to {path}")


def main() -> None:
    customer_rows = [
        {
            "customer_id": f"CUST{i:04d}",
            "customer_name": f"Customer {i:04d} Inc",
            "region": REGIONS[i % len(REGIONS)],
            "signup_date": "2026-08-28",
        }
        for i in range(1, 101)
    ]
    _write_csv("customer_data.csv", ["customer_id", "customer_name", "region", "signup_date"], customer_rows)

    vendor_rows = [
        {
            "vendor_id": f"VEND{i:04d}",
            "vendor_name": f"Vendor {i:04d} LLC",
            "category": VENDOR_CATEGORIES[i % len(VENDOR_CATEGORIES)],
            "feed_date": "2026-08-28",
        }
        for i in range(1, 51)
    ]
    _write_csv("vendor_feed.csv", ["vendor_id", "vendor_name", "category", "feed_date"], vendor_rows)


if __name__ == "__main__":
    main()
