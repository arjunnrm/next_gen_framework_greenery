"""Generates the ZIP ingestion pipeline's sample input archives under sample_data/zip_ingestion/.

Run locally (no Databricks/Spark dependency): ``python sample_data/generate_zip_ingestion_fixtures.py``.
Produces:

* Four valid input ZIPs (the "4 ZIP files" ingestion batch): two regional sales extracts
  plus two reference datasets (customers, products) joined against them.
* Two deliberately-broken fixtures used only by negative-path tests, never onboarded as
  part of the real batch: a malformed (truncated, non-ZIP) archive, and a byte-identical
  duplicate of one of the valid archives under a different name.

See docs/12_zip_ingestion_pipeline.md for how these fixtures are used.
"""

import csv
import io
import os
import zipfile

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zip_ingestion")


def _write_csv_zip(zip_path: str, csv_filename: str, header: list, rows: list) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(csv_filename, buffer.getvalue())
    print(f"Wrote {zip_path}")


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    _write_csv_zip(
        os.path.join(OUTPUT_DIR, "zip_sales_branch_east.zip"),
        "sales_branch_east.csv",
        ["order_id", "product_id", "customer_id", "quantity", "branch"],
        [
            ["SO-E001", "P100", "C001", "3", "EAST"],
            ["SO-E002", "P101", "C002", "1", "EAST"],
            ["SO-E003", "P100", "C003", "5", "EAST"],
        ],
    )
    _write_csv_zip(
        os.path.join(OUTPUT_DIR, "zip_sales_branch_west.zip"),
        "sales_branch_west.csv",
        ["order_id", "product_id", "customer_id", "quantity", "branch"],
        [
            ["SO-W001", "P102", "C004", "2", "WEST"],
            ["SO-W002", "P101", "C001", "4", "WEST"],
        ],
    )
    _write_csv_zip(
        os.path.join(OUTPUT_DIR, "zip_ref_customers.zip"),
        "customers.csv",
        ["customer_id", "customer_name", "region"],
        [
            ["C001", "Acme Corp", "EAST"],
            ["C002", "Beta LLC", "EAST"],
            ["C003", "Gamma Inc", "EAST"],
            ["C004", "Delta Co", "WEST"],
        ],
    )
    _write_csv_zip(
        os.path.join(OUTPUT_DIR, "zip_ref_products.zip"),
        "products.csv",
        ["product_id", "product_name", "unit_price"],
        [
            ["P100", "Widget", "9.99"],
            ["P101", "Gadget", "19.99"],
            ["P102", "Gizmo", "49.99"],
        ],
    )

    # Negative-path fixtures -- never part of the real 4-ZIP batch.
    malformed_path = os.path.join(OUTPUT_DIR, "zip_malformed.zip")
    with open(malformed_path, "wb") as handle:
        handle.write(b"This is not a ZIP file, just plain bytes with a misleading extension.")
    print(f"Wrote {malformed_path} (deliberately not a valid ZIP)")

    duplicate_path = os.path.join(OUTPUT_DIR, "zip_sales_branch_east_duplicate.zip")
    with open(os.path.join(OUTPUT_DIR, "zip_sales_branch_east.zip"), "rb") as source, open(duplicate_path, "wb") as dest:
        dest.write(source.read())
    print(f"Wrote {duplicate_path} (byte-identical duplicate of zip_sales_branch_east.zip)")

    empty_path = os.path.join(OUTPUT_DIR, "zip_empty.zip")
    with zipfile.ZipFile(empty_path, "w"):
        pass
    print(f"Wrote {empty_path} (valid ZIP, zero members)")


if __name__ == "__main__":
    main()
