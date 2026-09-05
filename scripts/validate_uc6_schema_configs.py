"""Validate UC6's schema_config files against the real sample source files.

A schema_config for a headerless delimited feed maps POSITIONS (``_c0.._cN``) to names, so
every error it can contain is an off-by-one or a duplicate -- both of which are invisible on
inspection and produce silently misaligned columns rather than a failure. (One of each was
caught by an earlier run of this script during the UC6 build.)

Checks, per source:
  1. the declared column count equals the sample file's actual field count;
  2. ``source_name`` values are exactly ``_c0.._c<n-1>``, with no gaps or repeats;
  3. no two columns share a ``target_name`` (Spark resolves case-insensitively, so the
     comparison is case-folded);
  4. every business-critical column named in the build contract resolves to a sample value
     that actually looks like that kind of data.

Run: python scripts/validate_uc6_schema_configs.py
"""

import gzip
import json
import os
import re
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(REPO_ROOT, "onboarding", "uc6", "schema_configs")
SAMPLE_DIR = os.path.join(REPO_ROOT, "docs", "UC6", "sample_bundle", "uc_6", "raw")

#: source key -> (schema_config file, sample file, delimiter, has_header)
SOURCES = {
    "css_account_address": ("uc6_css_account_address.json", "CSS_account_address_20250127_00000008.dat.gz", "|", False),
    "css_subscription": ("uc6_css_subscription.json", "CSS_subscription_20250127_00000009.dat.gz", "|", False),
    "css_account": ("uc6_css_account.json", "CSS_account_20250127_00000008.dat.gz", "|", False),
    "jt_customer": ("uc6_jt_customer.json", "CM_JT_Customer_Details_20250127_12345678.dat.gz", "|", False),
    "excalibur_address": ("uc6_excalibur_address.json", "CM_EXCALIBUR_ADDRESS_20250127_00000048.dat.gz", ",", False),
}

#: target_name -> a predicate the sampled value must satisfy in at least one row. These are the
#: columns the transformation SQL actually keys on, so a silent position shift in any of them
#: changes results rather than erroring.
SPOT_CHECKS = {
    "msisdn": lambda v: re.fullmatch(r"0\d{9,14}", v or "") is not None,
    "postcode": lambda v: re.fullmatch(r"[A-Z]{1,2}\d[\dA-Z]? ?\d[A-Z]{2}", (v or "").strip().upper()) is not None,
    "adr_post_code": lambda v: re.fullmatch(r"[A-Z]{1,2}\d[\dA-Z]? ?\d[A-Z]{2}", (v or "").strip().upper()) is not None,
    "dateofbirth": lambda v: re.fullmatch(r"(\d{4}-\d{2}-\d{2}|\d{8})", (v or "").strip()) is not None,
}


def _rows(sample_path, delimiter, has_header):
    with gzip.open(sample_path, "rt", encoding="utf-8", errors="replace") as handle:
        lines = [line for line in handle.read().splitlines() if line.strip()]
    if has_header:
        lines = lines[1:]
    return [line.split(delimiter) for line in lines]


def _check(source_key, failures):
    config_name, sample_name, delimiter, has_header = SOURCES[source_key]
    columns = json.load(open(os.path.join(CONFIG_DIR, config_name), encoding="utf-8"))["columns"]
    rows = _rows(os.path.join(SAMPLE_DIR, sample_name), delimiter, has_header)

    field_counts = {len(row) for row in rows}
    if len(field_counts) != 1:
        failures.append(f"{source_key}: sample file has ragged rows ({sorted(field_counts)} fields)")
    actual = max(field_counts)
    if len(columns) != actual:
        failures.append(f"{source_key}: schema_config declares {len(columns)} columns, sample file has {actual} fields")

    expected_names = [f"_c{index}" for index in range(len(columns))]
    if [column["source_name"] for column in columns] != expected_names:
        failures.append(f"{source_key}: source_name values are not exactly _c0.._c{len(columns) - 1} in order")

    targets = [column["target_name"].casefold() for column in columns]
    duplicates = sorted({name for name in targets if targets.count(name) > 1})
    if duplicates:
        failures.append(f"{source_key}: duplicate target_name(s) {duplicates} -- Spark resolves names case-insensitively")

    for index, column in enumerate(columns):
        predicate = SPOT_CHECKS.get(column["target_name"])
        if predicate is None or index >= actual:
            continue
        values = [row[index].strip() for row in rows if index < len(row) and row[index].strip()]
        if not values:
            failures.append(f"{source_key}: '{column['target_name']}' (_c{index}) is empty in every sample row")
        elif not any(predicate(value) for value in values):
            failures.append(
                f"{source_key}: '{column['target_name']}' (_c{index}) holds {values[:3]!r}, which does not "
                f"look like that column -- likely an off-by-one against the sample file"
            )


def main():
    failures = []
    for source_key in SOURCES:
        _check(source_key, failures)

    # The EA request file is the one headered source and needs its own check: it is
    # GPG-encrypted, so decrypting it proves the passphrase and the pipeline's inbound path.
    ea_path = os.path.join(SAMPLE_DIR, "EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg")
    try:
        from flowx.lakeflow_framework.crypto.pgp import pgp_decrypt_symmetric

        header = gzip.decompress(
            pgp_decrypt_symmetric(open(ea_path, "rb").read(), "EA-POC-Sample-2026!")
        ).decode().splitlines()[0]
        expected = "targetAreaID|osapr|postcode|org_name|department|po_box|sub_bld_name|bld_name|bld_number|thfare2|thfare1|dbl_dep_loc|dep_loc|town"
        if header != expected:
            failures.append(f"ea_request: header row is {header!r}, expected {expected!r}")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"ea_request: could not decrypt/read the sample ({exc})")

    if failures:
        print("FAILED:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print(f"OK: {len(SOURCES)} schema_config file(s) agree with the sample data, plus the EA request header.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
