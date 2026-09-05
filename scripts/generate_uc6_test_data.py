"""Generate an AUGMENTED UC6 fixture that actually exercises the business rules.

Why this exists
---------------
The supplied `uc_6_poc_bundle.zip` is sanitised synthetic data, and two independent facts make
it unable to prove UC6's logic (both verified, both recorded in
docs/UC6/FRAMEWORK_CAPABILITY_MAP.md section 3.4):

1. **No postcode overlap.** The EA request file's postcodes (AB12 3CD, EF45 6GH, IJ78 9KL,
   MN10 2OP) appear in none of the five EE address sources.
2. **No CSS join-key overlap.** CSS account ids (~2873-3051), subscription ids (~3296-3715) and
   address customer ids (~46649141) are disjoint under every candidate join column, so the CSS
   three-way join yields zero rows regardless of postcodes.

So a run over the supplied bundle produces ZERO matches, and every OSAPR row lands as
'Bad OSAPR' or 'Single Addr'. That is a legitimate test of the no-match path, and it is kept --
but it cannot demonstrate the acceptance criterion the brief actually asks for ("spot-check
STATUS values Found / Not Found / Bad OSAPR / Single Addr").

This generator therefore writes a SECOND fixture, in the same formats and filename conventions,
engineered so that every branch of the decision table is hit by at least one row. The supplied
bundle is never modified.

Designed cases
--------------
| targetAreaID | Expected STATUS | Why |
|---|---|---|
| AREA_FOUND    | Found       | 2 osaprs, both matching an EE address above threshold |
| AREA_NOTFOUND | Not Found   | 2 osaprs, only ONE matches -- so the area fails the >1 rule |
| AREA_BADOSAPR | Bad OSAPR   | 2 osaprs, postcode matches but address text does not |
| AREA_SINGLE   | Single Addr | exactly 1 osapr in the area at all |

The telephone list must then contain ONLY AREA_FOUND's MSISDNs: AREA_NOTFOUND is withheld by
the privacy safeguard even though one of its addresses genuinely matched.

Usage
-----
    python scripts/generate_uc6_test_data.py [--out <dir>] [--passphrase <pw>]

Writes the six source files (gzip, plus GPG-symmetric for the EA request) into
<out>/raw/, defaulting to docs/UC6/test_fixture/uc_6/raw/.
"""

import argparse
import gzip
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

DEFAULT_OUT = os.path.join(REPO_ROOT, "docs", "UC6", "test_fixture", "uc_6")
DEFAULT_PASSPHRASE = "EA-POC-Sample-2026!"

EA_HEADER = ("targetAreaID|osapr|postcode|org_name|department|po_box|sub_bld_name|bld_name|"
             "bld_number|thfare2|thfare1|dbl_dep_loc|dep_loc|town")

#: (targetAreaID, osapr, postcode, sub_bld, bld_name, bld_no, thfare, town)
EA_ROWS = [
    # AREA_FOUND: two osaprs, both will match an EE address exactly.
    ("AREA_FOUND", "3040045625", "AB12 3CD", "", "ROSE COTTAGE", "22", "CYPRESS ROAD", "SAMPLECITY"),
    ("AREA_FOUND", "3040045626", "AB12 3CD", "FLAT 1", "WILLOW HOUSE", "14", "OAK STREET", "SAMPLECITY"),
    # AREA_NOTFOUND: two osaprs, only the first matches -> area has 1 matched address -> withheld.
    ("AREA_NOTFOUND", "3040045630", "EF45 6GH", "", "MAPLE BUILDING", "5", "BIRCH AVENUE", "TESTBURGH"),
    ("AREA_NOTFOUND", "3040045631", "EF45 6GH", "UNIT 3", "CEDAR HOUSE", "7", "ELM ROAD", "TESTBURGH"),
    # AREA_BADOSAPR: postcode matches an EE row, but the address text shares no tokens.
    ("AREA_BADOSAPR", "3040045640", "IJ78 9KL", "", "QUARTZ TOWER", "101", "OBSIDIAN PARADE", "MOCKHAM"),
    ("AREA_BADOSAPR", "3040045641", "IJ78 9KL", "", "GRANITE LODGE", "103", "BASALT CRESCENT", "MOCKHAM"),
    # AREA_SINGLE: exactly one osapr in the whole area.
    ("AREA_SINGLE", "3040045650", "MN10 2OP", "APARTMENT 2B", "KINGFISHER COURT", "18", "HERON WAY", "FAKEFORD"),
]

#: EE-side addresses. (postcode, bld_name, bld_no, thfare, sub_bld, town, msisdn)
#: AREA_FOUND's two addresses match exactly; AREA_NOTFOUND's first matches, its second does not
#: (deliberately absent); AREA_BADOSAPR's postcode is present but the address text is unrelated.
CSS_MATCHES = [
    ("AB12 3CD", "ROSE COTTAGE", "22", "CYPRESS ROAD", "", "SAMPLECITY", "07700900001"),
    ("AB12 3CD", "WILLOW HOUSE", "14", "OAK STREET", "FLAT 1", "SAMPLECITY", "07700900002"),
    ("EF45 6GH", "MAPLE BUILDING", "5", "BIRCH AVENUE", "", "TESTBURGH", "07700900003"),
    ("IJ78 9KL", "SANDSTONE WHARF", "900", "MARBLE ESPLANADE", "", "MOCKHAM", "07700900004"),
    ("MN10 2OP", "KINGFISHER COURT", "18", "HERON WAY", "APARTMENT 2B", "FAKEFORD", "07700900005"),
]


def _pad(values, width):
    """Pad a row out to the source feed's real field count, so the fixture matches the
    schema_config files exactly (which are themselves asserted against the supplied bundle)."""
    return list(values) + [""] * (width - len(values))


def _ea_file_text():
    lines = [EA_HEADER]
    for area, osapr, postcode, sub_bld, bld_name, bld_no, thfare, town in EA_ROWS:
        lines.append("|".join([
            area, osapr, postcode, "", "", "", sub_bld, bld_name, bld_no, "", thfare, "", "", town,
        ]))
    return "\n".join(lines) + "\n"


def _css_files():
    """CSS is a three-way join, so the SAME customerid must appear in all three files --
    the defect that makes the supplied bundle's CSS path yield zero rows."""
    accounts, subscriptions, addresses = [], [], []
    for index, (postcode, bld, no, thfare, sub_bld, town, msisdn) in enumerate(CSS_MATCHES):
        customerid = "90000%03d" % index

        account = _pad(["S", customerid, "ACC%05d" % index], 56)
        account[3] = ""                      # org_name
        account[7] = "Test"                  # first_name
        account[8] = "Customer%d" % index    # last_name
        account[11] = "1980-01-15"           # dateofbirth -> age > 17
        account[14] = "LC"                   # customerbusinessunitcode -> not 'BS'
        accounts.append("|".join(account))

        subscription = _pad(["S", "SUB%05d" % index], 47)
        subscription[4] = customerid
        subscription[5] = "ACC%05d" % index
        subscription[9] = msisdn
        subscriptions.append("|".join(subscription))

        address = _pad(["I", "ADDR%05d" % index, customerid], 19)
        address[5] = sub_bld       # building_name1
        address[7] = ""            # po_box -- empty, so these rows are matchable
        address[8] = bld           # building_name
        address[9] = thfare        # street1
        address[13] = town         # county (this feed's town-ish column)
        address[14] = no           # building_number
        address[15] = postcode
        addresses.append("|".join(address))

    return accounts, subscriptions, addresses


def _jt_file():
    """One PAYG customer, deliberately UNDER 17, to prove the age filter genuinely excludes."""
    row = _pad(["JT00001"], 34)
    row[2], row[3], row[5] = "Mr.", "Too", "Young"
    row[8], row[9] = "22", "CYPRESS ROAD"
    row[13], row[15] = "SAMPLECITY", "AB12 3CD"
    row[30] = "20150101"   # dateofbirth -> ~11 years old, must be filtered out
    row[31] = "07700900999"
    return ["|".join(row)]


def _excalibur_file():
    """One EE/TMUK row on AREA_BADOSAPR's postcode with unrelated address text, so that area
    matches on postcode but scores below threshold."""
    row = _pad(["EXC00001"], 43)
    row[4] = "07700900006"
    row[18] = "PORPHYRY WALK"
    row[19] = "SLATE QUARTER"
    row[20] = "404"
    row[21] = "FLINT HOUSE"
    row[32] = "IJ78 9KL"
    return [",".join(row)]


def _write_gzip(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--passphrase", default=DEFAULT_PASSPHRASE)
    args = parser.parse_args()

    raw_dir = os.path.join(args.out, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    accounts, subscriptions, addresses = _css_files()
    written = [
        _write_gzip(os.path.join(raw_dir, "CSS_account_20260820_00000001.dat.gz"), "\n".join(accounts) + "\n"),
        _write_gzip(os.path.join(raw_dir, "CSS_subscription_20260820_00000001.dat.gz"), "\n".join(subscriptions) + "\n"),
        _write_gzip(os.path.join(raw_dir, "CSS_account_address_20260820_00000001.dat.gz"), "\n".join(addresses) + "\n"),
        _write_gzip(os.path.join(raw_dir, "CM_JT_Customer_Details_20260820_00000001.dat.gz"), "\n".join(_jt_file()) + "\n"),
        _write_gzip(os.path.join(raw_dir, "CM_EXCALIBUR_ADDRESS_20260820_00000001.dat.gz"), "\n".join(_excalibur_file()) + "\n"),
    ]

    # The EA request is gzip-then-GPG-symmetric, exactly as the real feed arrives.
    from flowx.lakeflow_framework.crypto.pgp import pgp_encrypt_symmetric

    ea_path = os.path.join(raw_dir, "EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg")
    with open(ea_path, "wb") as handle:
        handle.write(pgp_encrypt_symmetric(gzip.compress(_ea_file_text().encode("utf-8")), args.passphrase))
    written.append(ea_path)

    for path in written:
        print("wrote %s (%d bytes)" % (os.path.relpath(path, REPO_ROOT), os.path.getsize(path)))
    print("\nExpected results:")
    print("  OSAPR   -> AREA_FOUND=Found x2, AREA_NOTFOUND=Not Found x2, "
          "AREA_BADOSAPR=Bad OSAPR x2, AREA_SINGLE=Single Addr x1")
    print("  TELEPHONE -> AREA_FOUND only (2 MSISDNs); AREA_NOTFOUND withheld by the privacy rule")
    return 0


if __name__ == "__main__":
    sys.exit(main())
