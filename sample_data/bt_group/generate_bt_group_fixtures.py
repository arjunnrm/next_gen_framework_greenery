"""Generates every plain-text (CSV/JSON) fixture for the BT_Group UC001-UC005 test suite.

Run locally (no Databricks/Spark dependency): ``python sample_data/bt_group/generate_bt_group_fixtures.py``.

Binary fixtures (the 4 UC002 ZIP archives, the 2 UC003 ASN.1-encoded CDR files) are
deliberately NOT produced here -- they're generated directly on-cluster by
``notebooks/00_seed_sample_data/01_seed_bt_group_data.py`` at seed time, same reasoning as
the existing ``00_seed_sample_data.py``: Databricks Workspace Files import silently
extracts/mangles pre-built ``.zip``/binary uploads synced through the bundle, so those two
fixture kinds must be built straight into the Volume on the cluster, never round-tripped
through workspace sync as a file.

See docs/19_bt_group_test_suite.md for how each fixture maps to its use case.
"""

import csv
import io
import json
import os

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))


def _write_csv(filename: str, header: list, rows: list) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Wrote {path} ({len(rows)} rows)")


def _write_json_lines(filename: str, records: list) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")
    print(f"Wrote {path} ({len(records)} records)")


# ---------------------------------------------------------------------------
# UC001: 3 Zerobus-style streaming producer tables (SCD1 / SCD2 / Append)
# ---------------------------------------------------------------------------

_write_csv(
    "uc001_src_product_catalog_batch1.csv",
    ["product_id", "product_name", "category", "unit_price", "status", "updated_at"],
    [
        ["PC001", "Wireless Mouse", "Electronics", "24.99", "ACTIVE", "2026-08-20T09:00:00.000+0000"],
        ["PC002", "Mechanical Keyboard", "Electronics", "89.99", "ACTIVE", "2026-08-20T09:00:00.000+0000"],
        ["PC003", "USB-C Hub", "Electronics", "34.50", "ACTIVE", "2026-08-20T09:00:00.000+0000"],
        ["PC004", "Standing Desk", "Furniture", "349.00", "ACTIVE", "2026-08-20T09:00:00.000+0000"],
        ["PC005", "Office Chair", "Furniture", "199.99", "DISCONTINUED", "2026-08-20T09:00:00.000+0000"],
    ],
)
_write_csv(
    "uc001_src_product_catalog_batch2.csv",
    ["product_id", "product_name", "category", "unit_price", "status", "updated_at"],
    [
        ["PC001", "Wireless Mouse", "Electronics", "19.99", "ACTIVE", "2026-08-21T09:00:00.000+0000"],  # price drop
        ["PC006", "4K Webcam", "Electronics", "59.99", "ACTIVE", "2026-08-21T09:05:00.000+0000"],  # new insert
    ],
)

_write_csv(
    "uc001_src_customer_master_batch1.csv",
    ["customer_id", "customer_name", "segment", "region", "tier", "updated_at"],
    [
        ["CM001", "Acme Corp", "ENTERPRISE", "US-EAST", "GOLD", "2026-08-20T09:00:00.000+0000"],
        ["CM002", "Beta LLC", "SMB", "US-WEST", "SILVER", "2026-08-20T09:00:00.000+0000"],
        ["CM003", "Gamma Inc", "ENTERPRISE", "EU-CENTRAL", "PLATINUM", "2026-08-20T09:00:00.000+0000"],
        ["CM004", "Delta Co", "SMB", "US-EAST", "BRONZE", "2026-08-20T09:00:00.000+0000"],
        ["CM005", "Epsilon Ltd", "MIDMARKET", "APAC", "SILVER", "2026-08-20T09:00:00.000+0000"],
    ],
)
_write_csv(
    "uc001_src_customer_master_batch2.csv",
    ["customer_id", "customer_name", "segment", "region", "tier", "updated_at"],
    [
        ["CM002", "Beta LLC", "SMB", "US-WEST", "GOLD", "2026-08-21T09:00:00.000+0000"],  # tier change -> tracked, opens v2
        ["CM003", "Gamma Inc", "ENTERPRISE", "EU-WEST", "PLATINUM", "2026-08-21T09:00:00.000+0000"],  # region only -> untracked, stays v1
        ["CM006", "Zeta Partners", "ENTERPRISE", "US-EAST", "GOLD", "2026-08-21T09:10:00.000+0000"],  # new insert
    ],
)

_write_csv(
    "uc001_src_order_events_batch1.csv",
    ["event_id", "order_id", "customer_id", "product_id", "quantity", "event_ts", "event_type"],
    [
        ["EVT-0001", "SO-1001", "CM001", "PC001", "2", "2026-08-20T10:00:00.000+0000", "ORDER_PLACED"],
        ["EVT-0002", "SO-1002", "CM002", "PC002", "1", "2026-08-20T10:05:00.000+0000", "ORDER_PLACED"],
        ["EVT-0003", "SO-1003", "CM003", "PC004", "1", "2026-08-20T10:10:00.000+0000", "ORDER_PLACED"],
        ["EVT-0004", "SO-1001", "CM001", "PC001", "2", "2026-08-20T11:00:00.000+0000", "ORDER_SHIPPED"],
        ["EVT-0005", "SO-1004", "CM004", "PC003", "3", "2026-08-20T11:15:00.000+0000", "ORDER_PLACED"],
        ["EVT-0006", "SO-1005", "CM005", "PC005", "1", "2026-08-20T11:30:00.000+0000", "ORDER_PLACED"],
    ],
)
_write_csv(
    "uc001_src_order_events_batch2.csv",
    ["event_id", "order_id", "customer_id", "product_id", "quantity", "event_ts", "event_type"],
    [
        ["EVT-0007", "SO-1006", "CM006", "PC006", "5", "2026-08-21T09:20:00.000+0000", "ORDER_PLACED"],
        ["EVT-0008", "SO-1002", "CM002", "PC002", "1", "2026-08-21T09:25:00.000+0000", "ORDER_DELIVERED"],
    ],
)

# ---------------------------------------------------------------------------
# UC004: reconciliation source -- 3 rows overlap UC001's product catalog (should be
# excluded from the drift audit), 2 rows don't exist there at all (should appear in it).
# ---------------------------------------------------------------------------

_write_csv(
    "uc004_reconciliation_source.csv",
    ["product_id", "product_name", "category", "unit_price", "extract_date"],
    [
        ["PC001", "Wireless Mouse", "Electronics", "24.99", "2026-08-20"],  # matches UC001 -> excluded
        ["PC002", "Mechanical Keyboard", "Electronics", "89.99", "2026-08-20"],  # matches UC001 -> excluded
        ["PC003", "USB-C Hub", "Electronics", "34.50", "2026-08-20"],  # matches UC001 -> excluded
        ["PC101", "Bluetooth Speaker", "Electronics", "44.99", "2026-08-20"],  # NOT in UC001 -> drift
        ["PC102", "Laptop Stand", "Accessories", "27.50", "2026-08-20"],  # NOT in UC001 -> drift
    ],
)

# ---------------------------------------------------------------------------
# UC005: schema evolution -- batch1 (baseline), batch2 (type-mismatch, safely rescued,
# auto-seeded/live-tested), batch3 (a genuinely new column -- addNewColumns mode
# correctly halts the stream pending restart; manual/documented demo only, see
# docs/19_bt_group_test_suite.md, NOT auto-seeded).
# ---------------------------------------------------------------------------

_write_json_lines(
    "uc005_schema_evolution_batch1.json",
    [
        {"device_id": "DEV-01", "event_ts": "2026-08-20T09:00:00.000Z", "reading": 21.5, "unit": "C"},
        {"device_id": "DEV-02", "event_ts": "2026-08-20T09:01:00.000Z", "reading": 19.8, "unit": "C"},
        {"device_id": "DEV-03", "event_ts": "2026-08-20T09:02:00.000Z", "reading": 22.1, "unit": "C"},
        {"device_id": "DEV-01", "event_ts": "2026-08-20T09:05:00.000Z", "reading": 21.7, "unit": "C"},
    ],
)
_write_json_lines(
    "uc005_schema_evolution_batch2_rescued_type_mismatch.json",
    [
        {"device_id": "DEV-02", "event_ts": "2026-08-21T09:00:00.000Z", "reading": 20.1, "unit": "C"},
        # 'reading' is a string here, not the numeric type inferred from batch1 -- Auto
        # Loader rescues this row's mismatched field into _rescued_data instead of
        # failing the stream or silently dropping data.
        {"device_id": "DEV-03", "event_ts": "2026-08-21T09:01:00.000Z", "reading": "SENSOR_ERROR", "unit": "C"},
        {"device_id": "DEV-04", "event_ts": "2026-08-21T09:02:00.000Z", "reading": 18.9, "unit": "C"},
    ],
)
_write_json_lines(
    "uc005_schema_evolution_batch3_new_column.json",
    [
        # firmware_version never appeared in batch1/batch2 -- under schema_evolution_mode
        # "addNewColumns" this is expected to halt the stream (UnknownFieldException)
        # until the pipeline is restarted to adopt the new column. Manual demo only.
        {"device_id": "DEV-05", "event_ts": "2026-08-22T09:00:00.000Z", "reading": 23.4, "unit": "C", "firmware_version": "2.1.0"},
    ],
)

print("Done.")
