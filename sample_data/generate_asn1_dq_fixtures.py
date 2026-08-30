"""Generates the 5 ASN.1 (BER-encoded) sample CDR files for the DQ/quarantine pipeline.

Run locally: ``python sample_data/generate_asn1_dq_fixtures.py`` (requires ``asn1tools``,
already a declared project dependency -- see pyproject.toml).

Produces, under sample_data/asn1_cdr_v2/:

* cdr_001.ber / cdr_002.ber -- valid records, no DQ rule violations.
* cdr_003.ber -- decodable, but violates ``dq_call_duration_non_negative``
  (``callDurationSeconds = -10``).
* cdr_004.ber -- decodable, but violates ``dq_imsi_present`` (empty ``imsi``).
* cdr_005.ber -- deliberately malformed (not valid BER at all), exercising the
  *schema/decode* failure path (``_asn1_decode_error``) rather than a DQ rule.

See docs/13_asn1_dq_quarantine.md for how these route through the pipeline.
"""

import os

import asn1tools

SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "asn1_schema")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "asn1_cdr_v2")


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    compiled = asn1tools.compile_files(os.path.join(SCHEMA_DIR, "telecom_cdr_v2.asn"), "ber")

    records = {
        "cdr_001.ber": {
            "recordId": "REC-001",
            "imsi": "310150123456789",
            "msisdn": "15551230001",
            "regionCode": "US-EAST",
            "callDurationSeconds": 120,
            "cellId": "CELL-1001",
        },
        "cdr_002.ber": {
            "recordId": "REC-002",
            "imsi": "310150123456790",
            "msisdn": "15551230002",
            "regionCode": "US-WEST",
            "callDurationSeconds": 45,
            "cellId": "CELL-2002",
        },
        "cdr_003.ber": {  # DQ violation: negative call duration
            "recordId": "REC-003",
            "imsi": "310150123456791",
            "msisdn": "15551230003",
            "regionCode": "US-EAST",
            "callDurationSeconds": -10,
            "cellId": "CELL-1002",
        },
        "cdr_004.ber": {  # DQ violation: empty imsi
            "recordId": "REC-004",
            "imsi": "",
            "msisdn": "15551230004",
            "regionCode": "EU-CENTRAL",
            "callDurationSeconds": 30,
            "cellId": "CELL-3003",
        },
    }

    for filename, record in records.items():
        encoded = compiled.encode("CallDetailRecordV2", record)
        output_path = os.path.join(OUTPUT_DIR, filename)
        with open(output_path, "wb") as handle:
            handle.write(encoded)
        print(f"Wrote {output_path} ({len(encoded)} bytes)")

    malformed_path = os.path.join(OUTPUT_DIR, "cdr_005.ber")
    with open(malformed_path, "wb") as handle:
        handle.write(b"\x30\x99\xffnot a valid BER-encoded CallDetailRecordV2 at all\x00\x01\x02")
    print(f"Wrote {malformed_path} (deliberately malformed -- exercises _asn1_decode_error)")


if __name__ == "__main__":
    main()
