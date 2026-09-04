"""Generates the 3 BER-encoded GSM CDR sample fixtures for TC-ING-004 (ASN.1 Binary Telecom
CDR Decoding, flowx_testing/013_ing_004_asn1_decode.json).

Run locally: ``python sample_data/generate_gsm_cdr_fixtures.py`` (requires ``asn1tools``,
already a declared project dependency -- see pyproject.toml). Mirrors
``sample_data/generate_asn1_dq_fixtures.py``'s structure, but this test case is specifically
about proving a *clean* decode (imsi/callDurationSeconds populated, zero
``_asn1_decode_error`` rows) -- unlike that scenario's deliberately mixed-quality batch, all
3 records here are valid, decodable ``GsmCallDetailRecord`` instances.

Produces, under sample_data/asn1_cdr_gsm/:

* gsm_cdr_001.ber / gsm_cdr_002.ber / gsm_cdr_003.ber -- all valid, no DQ rule violations,
  no decode errors.

These files are for local inspection/regression only -- the real fixtures the pipeline
actually reads are generated **directly on-cluster** at seed time by
``notebooks/00_seed_sample_data/03_seed_asn1_gsm_cdr_fixture.py`` (binary files don't survive
Databricks Workspace Files sync reliably; see docs/13_asn1_dq_quarantine.md's identical
rationale), using this exact same schema and record set.

See docs/34_tc_ing_004.md for how these route through the pipeline.
"""

import os

import asn1tools

SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "asn1_schema")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "asn1_cdr_gsm")


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    compiled = asn1tools.compile_files(os.path.join(SCHEMA_DIR, "gsm_cdr.asn"), "ber")

    records = {
        "gsm_cdr_001.ber": {
            "imsi": "310150555000001",
            "imei": "490154203237518",
            "callDurationSeconds": 180,
            "cellId": "CELL-GSM-001",
            "roamingFlag": False,
        },
        "gsm_cdr_002.ber": {
            "imsi": "310150555000002",
            "imei": "490154203237519",
            "callDurationSeconds": 42,
            "cellId": "CELL-GSM-002",
            "roamingFlag": True,
        },
        "gsm_cdr_003.ber": {
            "imsi": "310150555000003",
            "imei": "490154203237520",
            "callDurationSeconds": 305,
            "cellId": "CELL-GSM-003",
            "roamingFlag": False,
        },
    }

    for filename, record in records.items():
        encoded = compiled.encode("GsmCallDetailRecord", record)
        output_path = os.path.join(OUTPUT_DIR, filename)
        with open(output_path, "wb") as handle:
            handle.write(encoded)
        print(f"Wrote {output_path} ({len(encoded)} bytes)")


if __name__ == "__main__":
    main()
