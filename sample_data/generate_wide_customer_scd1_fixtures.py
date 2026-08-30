"""Generates the wide-table SCD1 column-exclusion pipeline's sample CSVs.

Run locally (no Databricks/Spark dependency): ``python sample_data/generate_wide_customer_scd1_fixtures.py``.
Produces two 50-column CSVs -- ``sample_wide_customer_master_day1.csv`` (5 customers) and
``sample_wide_customer_master_day2.csv`` (one business-attribute change, one
technical-columns-only reload, one new customer) -- proving ``target_config.columns_to_exclude``
(see docs/18_test_pipeline_scd1_wide_table_column_exclusion.md) drops the 5 technical/audit
columns from the SCD1 target regardless of how often they change, while every business
column still flows through untouched via a plain ``SELECT *``.

45 business/key columns + 5 technical/audit columns = 50 total, matching the
``columns_to_exclude`` example already used in ``onboarding_templates/pipeline_onboarding_template.json``.
"""

import csv
import os

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

BUSINESS_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "secondary_email",
    "phone",
    "address_line1",
    "address_line2",
    "city",
    "state_province",
    "postal_code",
    "country",
    "date_of_birth",
    "gender",
    "marital_status",
    "nationality",
    "tax_id",
    "occupation",
    "employer_name",
    "annual_income",
    "credit_score",
    "account_open_date",
    "account_type",
    "account_status",
    "branch_code",
    "region_code",
    "relationship_manager",
    "segment_code",
    "risk_segment",
    "kyc_status",
    "kyc_verified_date",
    "preferred_language",
    "preferred_channel",
    "marketing_opt_in",
    "loyalty_tier",
    "loyalty_points",
    "lifetime_value",
    "last_purchase_date",
    "churn_risk_score",
    "satisfaction_score",
    "referral_source",
    "emergency_contact_name",
    "emergency_contact_phone",
    "device_type",
    "updated_at",  # sequence_by_column -- a business-meaningful "last changed" timestamp
]

# Technical/audit columns: about the ETL run, not the customer. Excluded from the SCD1
# target via target_config.columns_to_exclude -- see spec_14_wide_table_scd1_column_exclusion.json.
TECHNICAL_COLUMNS = [
    "batch_load_ts",
    "source_extract_filename",
    "etl_run_id",
    "checksum_hash",
    "ingestion_notes",
]

ALL_COLUMNS = BUSINESS_COLUMNS + TECHNICAL_COLUMNS
assert len(ALL_COLUMNS) == 50, f"expected exactly 50 columns, got {len(ALL_COLUMNS)}"


def _base_customer(customer_id: str, seq: int) -> dict:
    return {
        "customer_id": customer_id,
        "first_name": f"First{seq}",
        "last_name": f"Last{seq}",
        "email": f"customer{seq}@example.com",
        "secondary_email": f"customer{seq}.alt@example.com",
        "phone": f"+1-555-01{seq:02d}",
        "address_line1": f"{100 + seq} Main Street",
        "address_line2": "",
        "city": "Springfield",
        "state_province": "IL",
        "postal_code": f"627{seq:02d}",
        "country": "US",
        "date_of_birth": "1985-06-15",
        "gender": "U",
        "marital_status": "SINGLE",
        "nationality": "US",
        "tax_id": f"TAX-{seq:06d}",
        "occupation": "Engineer",
        "employer_name": "Acme Corp",
        "annual_income": str(50000 + seq * 1000),
        "credit_score": str(650 + seq),
        "account_open_date": "2020-01-10",
        "account_type": "CHECKING",
        "account_status": "ACTIVE",
        "branch_code": "BR-EAST-01",
        "region_code": "US-EAST",
        "relationship_manager": "RM-100",
        "segment_code": "RETAIL",
        "risk_segment": "LOW",
        "kyc_status": "VERIFIED",
        "kyc_verified_date": "2020-01-12",
        "preferred_language": "en",
        "preferred_channel": "EMAIL",
        "marketing_opt_in": "true",
        "loyalty_tier": "SILVER",
        "loyalty_points": str(100 * seq),
        "lifetime_value": str(1000 * seq),
        "last_purchase_date": "2026-08-01",
        "churn_risk_score": "0.10",
        "satisfaction_score": "4.2",
        "referral_source": "WEB",
        "emergency_contact_name": f"Contact{seq}",
        "emergency_contact_phone": f"+1-555-02{seq:02d}",
        "device_type": "MOBILE",
        "updated_at": "2026-08-20T09:00:00.000+0000",
        "batch_load_ts": "2026-08-20T10:00:00.000+0000",
        "source_extract_filename": "wide_customer_master_2026-08-20.csv",
        "etl_run_id": "RUN-2026-08-20-001",
        "checksum_hash": f"sha256:day1{seq:04d}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "ingestion_notes": "Nightly full extract, no manual overrides",
    }


def _write_csv(path: str, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ALL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Wrote {path} ({len(rows)} rows, {len(ALL_COLUMNS)} columns)")


def main() -> None:
    day1_rows = [_base_customer(f"WC00{i}", i) for i in range(1, 6)]  # WC001..WC005
    _write_csv(os.path.join(OUTPUT_DIR, "sample_wide_customer_master_day1.csv"), day1_rows)

    day2_rows = [dict(row) for row in day1_rows]

    # WC001: a genuine business-attribute change -- account_status flips, updated_at
    # advances, and (as every real re-extract naturally would) the technical columns
    # differ too.
    wc001 = day2_rows[0]
    wc001["account_status"] = "SUSPENDED"
    wc001["risk_segment"] = "HIGH"
    wc001["updated_at"] = "2026-08-21T09:00:00.000+0000"

    # WC002: zero business-column changes -- but the technical/audit columns still
    # differ (new run id, new checksum, updated_at bumped by the source's own refresh),
    # exactly as a routine nightly re-extract would produce even for untouched rows.
    # Proves columns_to_exclude keeps the SCD1 target's visible data unaffected by
    # technical churn.
    wc002 = day2_rows[1]
    wc002["updated_at"] = "2026-08-21T09:00:00.000+0000"

    # WC003, WC004, WC005: entirely unchanged, not re-included in day2's batch at all --
    # SCD1 keeps their day1 row as-is (nothing to overwrite it with).
    day2_rows = [wc001, wc002]

    # WC006: brand new customer inserted on day2.
    wc006 = _base_customer("WC006", 6)
    wc006["updated_at"] = "2026-08-21T09:05:00.000+0000"
    day2_rows.append(wc006)

    for row in day2_rows:
        row["batch_load_ts"] = "2026-08-21T10:00:00.000+0000"
        row["source_extract_filename"] = "wide_customer_master_2026-08-21.csv"
        row["etl_run_id"] = "RUN-2026-08-21-001"
        row["checksum_hash"] = f"sha256:day2{row['customer_id']}bbbbbbbbbbbbbbbbbbbbbbbb"
        row["ingestion_notes"] = "Nightly full extract, no manual overrides"

    _write_csv(os.path.join(OUTPUT_DIR, "sample_wide_customer_master_day2.csv"), day2_rows)


if __name__ == "__main__":
    main()
