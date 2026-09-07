"""Offline verification of UC6's transformation SQL -- runs the SPEC's own SQL, not a copy.

Why this is not a pytest in tests/unit: this repo's tests/conftest.py eagerly builds a
DatabricksSession, and databricks-connect patches pyspark to REFUSE a local SparkSession
outright. So `tests/unit` cannot execute SQL without a live workspace -- which is exactly why
its offline baseline sits at ~126 failures. Rather than add a 127th, this runs standalone:

  1. parse every transformation_sql with sqlglot's SPARK dialect -- proves it is syntactically
     valid Spark SQL, and surfaces the table references actually used;
  2. transpile to DuckDB and execute against both fixtures -- proves the LOGIC (the four
     status branches, the privacy rule, the age filter) produces the expected rows.

Requires: pip install sqlglot duckdb pandas pgpy  (none is a framework runtime dependency).

    python scripts/verify_uc6_business_rules.py

CAVEAT, stated plainly: DuckDB is not Spark. This is strong evidence for the decision table
(plain relational algebra) and weak for Spark-specific functions -- and it has already been
wrong once, via a sqlglot bug that mis-parenthesised months_between(...)/12 and silently let an
under-17 customer through. The spec now parenthesises that expression explicitly. A real
pipeline run remains the authority; this is the best available offline check, not a substitute.

  1. parse every transformation_sql with sqlglot's SPARK dialect -- proves it is syntactically
     valid Spark SQL, and surfaces the column/table references actually used;
  2. transpile to DuckDB and execute against the generated fixture -- proves the LOGIC
     (the four status branches, the privacy rule, the age filter) produces expected rows.

DuckDB is not Spark, so this is corroboration, not a substitute for a pipeline run. It is
strong on the decision table (plain relational algebra) and weak on Spark-specific functions.
"""

import gzip
import json
import os
import sys

import duckdb
import sqlglot
from sqlglot import exp

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEC = os.path.join(REPO, "BT_Usecase/UC6/onboarding/uc6_ea_flood_warning.json")
FIXTURE = os.path.join(REPO, "BT_Usecase/UC6/data/test_fixture")
SUPPLIED = os.path.join(REPO, "BT_Usecase/UC6/data/sample_bundle")
PW = "EA-POC-Sample-2026!"

SOURCES = {
    "uc6_ea_request": ("EE_", "|", True, None),
    "uc6_css_account": ("CSS_account_2", "|", False, "uc6_css_account.json"),
    "uc6_css_account_address": ("CSS_account_address_", "|", False, "uc6_css_account_address.json"),
    "uc6_css_subscription": ("CSS_subscription_", "|", False, "uc6_css_subscription.json"),
    "uc6_jt_customer": ("CM_JT_Customer_Details_", "|", False, "uc6_jt_customer.json"),
    "uc6_excalibur_address": ("CM_EXCALIBUR_ADDRESS_", ",", False, "uc6_excalibur_address.json"),
}


def substitute(sql, params):
    for key, value in params.items():
        literal = str(value) if isinstance(value, (int, float)) else "'%s'" % value
        sql = sql.replace("${%s}" % key, literal)
    return sql


def load_rows(raw_dir, prefix, delimiter, has_header, schema_config):
    matches = sorted(f for f in os.listdir(raw_dir) if f.startswith(prefix))
    if not matches:
        return None, None
    path = os.path.join(raw_dir, matches[0])
    if path.endswith(".gpg"):
        sys.path.insert(0, os.path.join(REPO, "src"))
        from flowx.lakeflow_framework.crypto.pgp import pgp_decrypt_symmetric
        text = gzip.decompress(pgp_decrypt_symmetric(open(path, "rb").read(), PW)).decode()
    else:
        text = gzip.decompress(open(path, "rb").read()).decode("utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    if has_header:
        cols = lines[0].split(delimiter)
        rows = [line.split(delimiter) for line in lines[1:]]
    else:
        cfg = json.load(open(os.path.join(REPO, "BT_Usecase/UC6/onboarding/schema_configs", schema_config), encoding="utf-8"))
        cols = [c["target_name"] for c in cfg["columns"]]
        rows = [line.split(delimiter) for line in lines]
    width = len(cols)
    rows = [(r + [""] * width)[:width] for r in rows]
    return cols, rows


def register(con, raw_dir):
    for table, (prefix, delimiter, has_header, schema_config) in SOURCES.items():
        cols, rows = load_rows(raw_dir, prefix, delimiter, has_header, schema_config)
        if cols is None:
            continue
        cols = cols + ["__framework_ingestion_timestamp_utc"]
        rows = [r + [str(i)] for i, r in enumerate(rows)]
        ddl = ", ".join('"%s" VARCHAR' % c for c in cols)
        con.execute('CREATE OR REPLACE TABLE "%s" (%s)' % (table, ddl))
        if rows:
            placeholders = ", ".join(["?"] * len(cols))
            con.executemany('INSERT INTO "%s" VALUES (%s)' % (table, placeholders), rows)


def run(raw_dir, spec, label):
    con = duckdb.connect()
    register(con, raw_dir)
    produced = {}
    for flow in spec["transformation_flows"]:
        sql = substitute(flow["transformation_sql"], spec["pipeline_parameters"])
        for src in flow["source_inputs"]:
            bare = src["table"].split(".")[-1]
            con.execute('CREATE OR REPLACE VIEW "%s" AS SELECT * FROM "%s"' % (src["input_name"], bare))
        duck_sql = sqlglot.transpile(sql, read="spark", write="duckdb")[0]
        con.execute('CREATE OR REPLACE TABLE "%s" AS %s' % (flow["target_table"], duck_sql))
        produced[flow["target_table"]] = con.execute('SELECT * FROM "%s"' % flow["target_table"]).fetchdf()
    print("\n===== %s =====" % label)
    for name, frame in produced.items():
        print("  %-24s %d row(s)" % (name, len(frame)))
    return produced


def main():
    spec = json.load(open(SPEC, encoding="utf-8"))

    print("===== 1. Spark-dialect parse of every transformation_sql =====")
    for flow in spec["transformation_flows"]:
        sql = substitute(flow["transformation_sql"], spec["pipeline_parameters"])
        tree = sqlglot.parse_one(sql, dialect="spark")
        tables = sorted({t.name for t in tree.find_all(exp.Table)})
        print("  OK  %-26s refs=%s" % (flow["flow_step_id"], tables))

    failures = []

    augmented = run(FIXTURE, spec, "AUGMENTED FIXTURE (generated)")
    osapr = augmented["uc6_osapr_output"]
    by_area = osapr.groupby("targetAreaID")["status"].apply(lambda s: set(s)).to_dict()
    print("\n  OSAPR statuses by area:")
    for area in sorted(by_area):
        print("    %-16s %s" % (area, sorted(by_area[area])))
    # Per-OSAPR expectations, keyed by (targetAreaID, osapr) -- an AREA can legitimately hold
    # more than one status, since STATUS is decided per address, not per area. AREA_NOTFOUND is
    # exactly that case: its first osapr matched (strength 100) but its area has only one
    # matched address, so it is withheld as 'Not Found'; its second never matched at all, so it
    # is 'Bad OSAPR'. Asserting a single status per area would have been asserting the wrong rule.
    expected_rows = {
        ("AREA_FOUND", "3040045625"): ("Found", 1),
        ("AREA_FOUND", "3040045626"): ("Found", 1),
        ("AREA_NOTFOUND", "3040045630"): ("Not Found", 0),
        ("AREA_NOTFOUND", "3040045631"): ("Bad OSAPR", 0),
        ("AREA_BADOSAPR", "3040045640"): ("Bad OSAPR", 0),
        ("AREA_BADOSAPR", "3040045641"): ("Bad OSAPR", 0),
        ("AREA_SINGLE", "3040045650"): ("Single Addr", 0),
    }
    actual_rows = {(r["targetAreaID"], r["osapr"]): (r["status"], int(r["count"])) for _, r in osapr.iterrows()}
    if actual_rows != expected_rows:
        for key in sorted(set(expected_rows) | set(actual_rows)):
            if expected_rows.get(key) != actual_rows.get(key):
                failures.append("OSAPR %s: expected %s, got %s" % (key, expected_rows.get(key), actual_rows.get(key)))
    if {s for s, _ in expected_rows.values()} != {"Found", "Not Found", "Bad OSAPR", "Single Addr"}:
        failures.append("the fixture must exercise all four status branches")

    tel = augmented["uc6_telephone_output"]
    tel_areas = set(tel["targetAreaID"]) if len(tel) else set()
    tel_numbers = set(tel["telephone"]) if len(tel) else set()
    print("\n  TELEPHONE areas: %s" % sorted(tel_areas))
    print("  TELEPHONE numbers: %s" % sorted(tel_numbers))
    if tel_areas != {"AREA_FOUND"}:
        failures.append("TELEPHONE: expected only AREA_FOUND, got %s" % sorted(tel_areas))
    if "07700900003" in tel_numbers:
        failures.append("TELEPHONE: AREA_NOTFOUND's MSISDN leaked past the privacy rule")
    if "07700900999" in tel_numbers:
        failures.append("TELEPHONE: the under-17 JT customer was not filtered out")

    supplied = run(SUPPLIED, spec, "SUPPLIED POC BUNDLE (as shipped)")
    if len(supplied["uc6_matched_address"]) != 0:
        failures.append("SUPPLIED: expected 0 matches")
    supplied_statuses = set(supplied["uc6_osapr_output"]["status"]) if len(supplied["uc6_osapr_output"]) else set()
    print("\n  supplied-bundle statuses: %s" % sorted(supplied_statuses))
    if not supplied_statuses <= {"Bad OSAPR", "Single Addr"}:
        failures.append("SUPPLIED: unexpected statuses %s" % supplied_statuses)

    print("\n" + "=" * 60)
    if failures:
        print("FAILURES:")
        for f in failures:
            print("  - %s" % f)
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
