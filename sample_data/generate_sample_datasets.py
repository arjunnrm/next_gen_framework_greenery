"""Regenerate the framework's 10-record sample datasets, one per test-case scenario.

Run standalone (``python generate_sample_datasets.py``) from any Python environment --
it only needs the standard library for the CSV/JSON artifacts. Two *optional* artifacts
additionally demonstrate the ASN.1 and encrypted-ZIP capabilities for real:

* A genuine BER-encoded CDR binary batch (requires ``asn1tools``).
* An AES-256 password-encrypted ZIP of that batch (requires ``pyzipper``).

Both are best-effort: if the optional dependency is missing, the script logs a warning
and continues rather than failing the whole run -- the plain CSV/JSON/decoded-JSON
artifacts (used directly by the six `04_test_specs/*.json` pipelines) are always produced.
"""

import csv
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("generate_sample_datasets")

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))


def _write_csv(filename: str, fieldnames: list, rows: list) -> None:
    """Write ``rows`` (list of dicts) to ``filename`` as CSV, with clear error context."""
    path = os.path.join(OUTPUT_DIR, filename)
    try:
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info("Wrote %d row(s) to %s", len(rows), path)
    except OSError as exc:
        raise RuntimeError(f"Failed to write CSV sample data to '{path}': {exc}") from exc


def _write_ndjson(filename: str, rows: list) -> None:
    """Write ``rows`` (list of dicts) to ``filename`` as newline-delimited JSON."""
    path = os.path.join(OUTPUT_DIR, filename)
    try:
        with open(path, "w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        logger.info("Wrote %d record(s) to %s", len(rows), path)
    except OSError as exc:
        raise RuntimeError(f"Failed to write NDJSON sample data to '{path}': {exc}") from exc


def generate_finance_datasets() -> None:
    """spec_01 (quarantine) + spec_04 (stream-stream join / SCD2) input fixtures."""
    try:
        raw_txn_rows = [
            {"txn_id": "TXN1001", "txn_ts": "2026-08-20 08:15:00", "amount": "250.75", "currency_code": "USD", "ssn_raw": "123-45-6789", "account_id": "ACC001", "order_id": "ORD5001"},
            {"txn_id": "TXN1002", "txn_ts": "2026-08-20 09:02:11", "amount": "89.99", "currency_code": "USD", "ssn_raw": "234-56-7890", "account_id": "ACC002", "order_id": "ORD5002"},
            {"txn_id": "TXN1003", "txn_ts": "2026-08-20 09:45:30", "amount": "-15.00", "currency_code": "USD", "ssn_raw": "345-67-8901", "account_id": "ACC003", "order_id": "ORD5003"},
            {"txn_id": "TXN1004", "txn_ts": "2026-08-20 10:10:05", "amount": "1200.00", "currency_code": "EUR", "ssn_raw": "456-78-9012", "account_id": "ACC004", "order_id": "ORD5004"},
            {"txn_id": "", "txn_ts": "2026-08-20 10:30:45", "amount": "75.50", "currency_code": "USD", "ssn_raw": "567-89-0123", "account_id": "ACC005", "order_id": "ORD5005"},
            {"txn_id": "TXN1006", "txn_ts": "2026-08-20 11:05:00", "amount": "42.10", "currency_code": "", "ssn_raw": "678-90-1234", "account_id": "ACC006", "order_id": "ORD5006"},
            {"txn_id": "TXN1007", "txn_ts": "2026-08-20 11:47:22", "amount": "310.25", "currency_code": "USD", "ssn_raw": "789-01-2345", "account_id": "ACC007", "order_id": "ORD5007"},
            {"txn_id": "TXN1008", "txn_ts": "2026-08-20 12:12:12", "amount": "-5.25", "currency_code": "GBP", "ssn_raw": "890-12-3456", "account_id": "ACC008", "order_id": "ORD5008"},
            {"txn_id": "TXN1009", "txn_ts": "2026-08-20 13:00:00", "amount": "999.99", "currency_code": "USD", "ssn_raw": "901-23-4567", "account_id": "ACC009", "order_id": "ORD5009"},
            {"txn_id": "TXN1010", "txn_ts": "2026-08-20 13:33:33", "amount": "150.00", "currency_code": "USD", "ssn_raw": "012-34-5678", "account_id": "ACC010", "order_id": "ORD5010"},
        ]
        _write_csv(
            "sample_raw_txn.csv",
            ["txn_id", "txn_ts", "amount", "currency_code", "ssn_raw", "account_id", "order_id"],
            raw_txn_rows,
        )

        raw_orders_rows = [
            {"order_id": f"ORD{5000 + i}", "order_ts": ts, "account_id": f"ACC{i:03d}"}
            for i, ts in enumerate(
                [
                    "2026-08-20 08:20:00", "2026-08-20 09:10:00", "2026-08-20 09:50:00", "2026-08-20 10:15:00",
                    "2026-08-20 10:35:00", "2026-08-20 11:08:00", "2026-08-20 11:50:00", "2026-08-20 12:15:00",
                    "2026-08-20 13:03:00", "2026-08-20 13:36:00",
                ],
                start=1,
            )
        ]
        _write_csv("sample_raw_orders.csv", ["order_id", "order_ts", "account_id"], raw_orders_rows)

        dim_accounts_rows = [
            {"account_id": "ACC001", "account_name": "Alpha Retail Ltd", "country": "US"},
            {"account_id": "ACC002", "account_name": "Beta Manufacturing", "country": "US"},
            {"account_id": "ACC003", "account_name": "Gamma Logistics", "country": "CA"},
            {"account_id": "ACC004", "account_name": "Delta Consulting", "country": "DE"},
            {"account_id": "ACC005", "account_name": "Epsilon Foods", "country": "US"},
            {"account_id": "ACC006", "account_name": "Zeta Health", "country": "US"},
            {"account_id": "ACC007", "account_name": "Eta Energy", "country": "GB"},
            {"account_id": "ACC008", "account_name": "Theta Media", "country": "US"},
            {"account_id": "ACC009", "account_name": "Iota Software", "country": "US"},
            {"account_id": "ACC010", "account_name": "Kappa Finance", "country": "US"},
        ]
        _write_csv("sample_dim_accounts.csv", ["account_id", "account_name", "country"], dim_accounts_rows)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to generate finance sample datasets: %s", exc)
        raise


def generate_mainframe_snapshot_datasets() -> None:
    """spec_03 (FULL_SNAPSHOT_CDC) two-day snapshot pair, diffed on customer_name.

    Day-2 vs. Day-1, verified against the committed fixtures: **2 updates** (John Smith
    ACTIVE->INACTIVE, Fatima Ali Dallas->Fort Worth), 1 delete (Wei Zhang), 1 insert
    (Noah Kim). Both days hold 10 rows. The two updates are the point of the fixture: with
    customer_name as the declared key they are UPDATEs in place, where the removed
    FULL_SNAPSHOT_CDC_NO_PK strategy hashed the whole payload and so reported each as a
    delete plus an insert -- the same customer under two identities.
    """
    try:
        day1_rows = [
            {"customer_name": "John Smith", "customer_city": "Chicago", "customer_status": "ACTIVE"},
            {"customer_name": "Maria Garcia", "customer_city": "Houston", "customer_status": "ACTIVE"},
            {"customer_name": "Wei Zhang", "customer_city": "Seattle", "customer_status": "ACTIVE"},
            {"customer_name": "Fatima Ali", "customer_city": "Dallas", "customer_status": "ACTIVE"},
            {"customer_name": "Robert Brown", "customer_city": "Boston", "customer_status": "ACTIVE"},
            {"customer_name": "Aiko Tanaka", "customer_city": "Denver", "customer_status": "ACTIVE"},
            {"customer_name": "Carlos Rivera", "customer_city": "Miami", "customer_status": "ACTIVE"},
            {"customer_name": "Emma Wilson", "customer_city": "Austin", "customer_status": "ACTIVE"},
            {"customer_name": "Liam O'Brien", "customer_city": "Portland", "customer_status": "ACTIVE"},
            {"customer_name": "Sofia Rossi", "customer_city": "Phoenix", "customer_status": "ACTIVE"},
        ]
        # day2: John Smith updated (status), Fatima Ali updated (city), Wei Zhang deleted, Noah Kim inserted.
        day2_rows = [row for row in day1_rows if row["customer_name"] not in ("John Smith", "Wei Zhang", "Fatima Ali")]
        day2_rows.insert(0, {"customer_name": "John Smith", "customer_city": "Chicago", "customer_status": "INACTIVE"})
        day2_rows.insert(2, {"customer_name": "Fatima Ali", "customer_city": "Fort Worth", "customer_status": "ACTIVE"})
        day2_rows.append({"customer_name": "Noah Kim", "customer_city": "San Jose", "customer_status": "ACTIVE"})

        fieldnames = ["customer_name", "customer_city", "customer_status"]
        _write_csv("sample_mainframe_customer_master_day1.csv", fieldnames, day1_rows)
        _write_csv("sample_mainframe_customer_master_day2.csv", fieldnames, day2_rows)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to generate mainframe snapshot sample datasets: %s", exc)
        raise


def generate_fx_rate_dataset() -> None:
    """spec_05 (TRUNCATE_AND_LOAD materialized view) input, including one invalid (negative) rate."""
    try:
        rows = [
            {"currency_code": "USD", "rate_date": "2026-08-23", "exchange_rate": "1.0"},
            {"currency_code": "EUR", "rate_date": "2026-08-23", "exchange_rate": "0.92"},
            {"currency_code": "GBP", "rate_date": "2026-08-23", "exchange_rate": "0.78"},
            {"currency_code": "JPY", "rate_date": "2026-08-23", "exchange_rate": "147.35"},
            {"currency_code": "CAD", "rate_date": "2026-08-23", "exchange_rate": "1.36"},
            {"currency_code": "AUD", "rate_date": "2026-08-23", "exchange_rate": "1.49"},
            {"currency_code": "CHF", "rate_date": "2026-08-23", "exchange_rate": "0.88"},
            {"currency_code": "INR", "rate_date": "2026-08-23", "exchange_rate": "83.12"},
            {"currency_code": "CNY", "rate_date": "2026-08-23", "exchange_rate": "7.15"},
            {"currency_code": "BRL", "rate_date": "2026-08-23", "exchange_rate": "-0.10"},
        ]
        _write_csv("sample_fx_rates.csv", ["currency_code", "rate_date", "exchange_rate"], rows)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to generate FX rate sample dataset: %s", exc)
        raise


def generate_iot_telemetry_dataset() -> None:
    """spec_06 (unified ingestion + external-sink egress) input: one device over the heavy-usage threshold."""
    try:
        rows = [
            {"device_id": "dev-001", "country": "US", "event_ts": "2026-08-23T10:05:00Z", "usage_bytes": 2000000000},
            {"device_id": "dev-001", "country": "US", "event_ts": "2026-08-23T10:15:00Z", "usage_bytes": 2200000000},
            {"device_id": "dev-001", "country": "US", "event_ts": "2026-08-23T10:25:00Z", "usage_bytes": 1500000000},
            {"device_id": "dev-002", "country": "US", "event_ts": "2026-08-23T10:10:00Z", "usage_bytes": 500000000},
            {"device_id": "dev-002", "country": "US", "event_ts": "2026-08-23T10:40:00Z", "usage_bytes": 300000000},
            {"device_id": "dev-003", "country": "DE", "event_ts": "2026-08-23T10:05:00Z", "usage_bytes": 6000000000},
            {"device_id": None, "country": "US", "event_ts": "2026-08-23T10:20:00Z", "usage_bytes": 100000000},
            {"device_id": "dev-005", "country": "US", "event_ts": "2026-08-23T10:12:00Z", "usage_bytes": 700000000},
            {"device_id": "dev-006", "country": "US", "event_ts": "2026-08-23T10:50:00Z", "usage_bytes": 900000000},
            {"device_id": "dev-006", "country": "US", "event_ts": "2026-08-23T11:05:00Z", "usage_bytes": 950000000},
        ]
        _write_ndjson("sample_iot_telemetry.json", rows)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to generate IoT telemetry sample dataset: %s", exc)
        raise


def _build_cdr_records() -> list:
    """The 10 CDR records shared by the decoded-JSON fixture and the optional ASN.1/ZIP artifacts."""
    # Keys are camelCase to match telecom_cdr.asn's ASN.1 identifiers exactly -- ASN.1
    # (X.680) identifiers don't allow underscores; see that file's header comment for how
    # this was discovered.
    return [
        {"imsi": "310150123456789", "msisdn": "14155550101", "regionCode": "US-CA", "callDurationSeconds": 125, "cellId": "CELL-0001"},
        {"imsi": "310150123456790", "msisdn": "14155550102", "regionCode": "US-CA", "callDurationSeconds": 340, "cellId": "CELL-0002"},
        {"imsi": "310150123456791", "msisdn": "14155550103", "regionCode": "US-NY", "callDurationSeconds": 58, "cellId": "CELL-0003"},
        {"imsi": None, "msisdn": "14155550104", "regionCode": "US-NY", "callDurationSeconds": 200, "cellId": "CELL-0004"},
        {"imsi": "310150123456793", "msisdn": "14155550105", "regionCode": "US-TX", "callDurationSeconds": -15, "cellId": "CELL-0005"},
        {"imsi": "310150123456794", "msisdn": "14155550106", "regionCode": "US-TX", "callDurationSeconds": 410, "cellId": "CELL-0006"},
        {"imsi": "310150123456795", "msisdn": "14155550107", "regionCode": "US-FL", "callDurationSeconds": 95, "cellId": "CELL-0007"},
        {"imsi": "310150123456796", "msisdn": "14155550108", "regionCode": "US-FL", "callDurationSeconds": 272, "cellId": "CELL-0008"},
        {"imsi": "310150123456797", "msisdn": "14155550109", "regionCode": "US-WA", "callDurationSeconds": 18, "cellId": "CELL-0009"},
        {"imsi": "310150123456798", "msisdn": "14155550110", "regionCode": "US-WA", "callDurationSeconds": 530, "cellId": "CELL-0010"},
    ]


def generate_telecom_cdr_decoded_dataset() -> None:
    """spec_02 (ASN.1 ingestion) fixture: the *decoded* representation, always produced (no optional deps)."""
    try:
        rows = [dict(record, _asn1_decode_error=None) for record in _build_cdr_records()]
        _write_ndjson("sample_telecom_cdr_decoded.json", rows)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to generate decoded telecom CDR sample dataset: %s", exc)
        raise


def generate_telecom_cdr_encoded_artifacts() -> None:
    """Optional: real BER-encoded CDR binary batch + AES-256 encrypted ZIP, for end-to-end testing.

    Best-effort: skips (with a warning, not a failure) when ``asn1tools`` and/or
    ``pyzipper`` aren't installed in the current environment.
    """
    records = _build_cdr_records()
    schema_path = os.path.join(OUTPUT_DIR, "asn1_schema", "telecom_cdr.asn")
    encoded_dir = os.path.join(OUTPUT_DIR, "encoded")
    binary_path = os.path.join(encoded_dir, "sample_cdr_batch.ber")

    try:
        import asn1tools
    except ImportError:
        logger.warning("asn1tools not installed -- skipping real BER-encoded CDR batch generation.")
        return

    try:
        os.makedirs(encoded_dir, exist_ok=True)
        compiled = asn1tools.compile_files(schema_path, "ber")
        with open(binary_path, "wb") as handle:
            for record in records:
                # Records missing imsi or with an invalid duration are intentionally skipped
                # from the binary batch -- they exist only to exercise the framework's DQ
                # rules against the *decoded* fixture, not to be valid wire-format CDRs.
                if record["imsi"] is None or record["callDurationSeconds"] < 0:
                    continue
                handle.write(compiled.encode("CallDetailRecord", record))
        logger.info("Wrote real BER-encoded CDR batch to %s", binary_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to generate BER-encoded CDR batch (continuing without it): %s", exc)
        return

    try:
        import pyzipper
    except ImportError:
        logger.warning("pyzipper not installed -- skipping encrypted ZIP generation for the CDR batch.")
        return

    try:
        zip_path = os.path.join(encoded_dir, "cdr_batch.zip")
        with pyzipper.AESZipFile(zip_path, "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as archive:
            archive.setpassword(b"sample-passphrase-change-me")
            archive.setencryption(pyzipper.WZ_AES, nbits=256)
            archive.write(binary_path, arcname="sample_cdr_batch.ber")
        logger.info("Wrote AES-256 encrypted sample archive to %s (passphrase: 'sample-passphrase-change-me')", zip_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to generate encrypted ZIP for the CDR batch (continuing without it): %s", exc)


def main() -> None:
    generators = [
        generate_finance_datasets,
        generate_mainframe_snapshot_datasets,
        generate_fx_rate_dataset,
        generate_iot_telemetry_dataset,
        generate_telecom_cdr_decoded_dataset,
        generate_telecom_cdr_encoded_artifacts,
    ]
    failures = []
    for generator in generators:
        try:
            generator()
        except Exception as exc:  # noqa: BLE001
            failures.append((generator.__name__, str(exc)))

    if failures:
        logger.error("%d generator(s) failed: %s", len(failures), failures)
        raise SystemExit(1)
    logger.info("All sample datasets generated successfully in %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
