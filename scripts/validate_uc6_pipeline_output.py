"""Validate the UC6 pipeline's REAL output -- the gold tables and the four exported files --
against the business-rule specification.

Why this exists, stated plainly: until now every UC6 "verification" has been offline.
scripts/verify_uc6_business_rules.py transpiles the spec's Spark SQL to DuckDB and runs it
against fixtures -- and it has already been wrong once, via a sqlglot bug that mis-parenthesised
months_between(...)/12 and silently let an under-17 customer through. DuckDB is corroboration,
not authority. The authority is what the Lakeflow pipeline actually wrote: the rows in
flowx.gold.uc6_osapr_output / uc6_telephone_output, and the bytes of the four files the pgp_zip
sink placed under /Volumes/flowx/staging/uc_6/output/. This script reads those and nothing else.

Run it AFTER a successful pipeline update (never mid-update -- see flowx_testing/TESTING_PLAN.md
section 0). It does not deploy, does not trigger anything, and only issues SELECTs and reads.

    python scripts/validate_uc6_pipeline_output.py                  # augmented fixture landed
    python scripts/validate_uc6_pipeline_output.py --expect-empty   # supplied zero-match bundle

Requires: pip install databricks-sdk pgpy   (pgpy via the framework's own crypto.pgp helper,
so the .gpg files are opened by exactly the code path a downstream supplier would rely on).
Authentication is the 'metaflow_v7' CLI profile; the PGP passphrase comes from the env var
UC6_PGP_PASSPHRASE and defaults to the documented synthetic POC passphrase.

Checks (each prints PASS/FAIL with actual vs expected; exit 0 only if every one passes):

  C1  gold uc6_osapr_output: exactly 7 rows, and every (targetAreaID, osapr) -> (status, count)
      matches the decision table -- all four status branches exercised.
  C2  gold uc6_telephone_output: targetAreaID set == {AREA_FOUND}, exactly 2 telephones, and
      the two must-be-absent numbers (privacy rule, under-17 age filter) are absent.
  C3  exactly four files in the output volume, one per role, names matching the interface
      shape EE_<yyyy-mm-dd>-[LEIDOS_]{TELEPHONE|OSAPR}_<NofM>.csv.gz[.gpg].
  C4  each .gpg decrypts (symmetric, framework helper) and gunzips; each .gz gunzips.
  C5  header row is exactly the exported columns, pipe-delimited, LF-terminated, every data
      row the same width.
  C6  file rows == the corresponding gold table's rows on the exported columns (set AND count).
  C7  LEIDOS_TELEPHONE content == TELEPHONE content, LEIDOS_OSAPR == OSAPR -- the two
      envelopes must carry identical rows; only the encryption differs.

--expect-empty (the SUPPLIED POC bundle, which matches nothing): C1 instead asserts the osapr
statuses are a subset of {Bad OSAPR, Single Addr}; C2 asserts zero telephone rows. Because the
pgp_zip sink writes NO archive for an empty micro-batch (write() discards a zero-row staged
part, commit() returns before archiving), the two TELEPHONE files are expected to be ABSENT
in this mode -- a telephone file that is present can only be stale output from an earlier run,
and is reported as such. C6/C7 still hold for whatever is present.

Exit codes: 0 all checks passed; 1 at least one check failed; 2 the workspace could not be
read at all (auth, warehouse, volume) -- an infrastructure problem, not a verdict on the data.
"""

import argparse
import csv
import gzip
import io
import json
import os
import re
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_PROFILE = "metaflow_v7"
DEFAULT_WAREHOUSE_ID = "3942c176e8c4dee8"
DEFAULT_CATALOG = "flowx"
DEFAULT_OUTPUT_DIR = "/Volumes/flowx/staging/uc_6/output/"
PASSPHRASE = os.environ.get("UC6_PGP_PASSPHRASE", "EA-POC-Sample-2026!")

# Per-OSAPR expectations, keyed by (targetAreaID, osapr) -- identical to the offline harness.
# An AREA can legitimately hold more than one status, since STATUS is decided per address, not
# per area: AREA_NOTFOUND's first osapr matched but its area has only one matched address, so it
# is withheld as 'Not Found'; its second never matched at all, so it is 'Bad OSAPR'.
EXPECTED_OSAPR = {
    ("AREA_FOUND", "3040045625"): ("Found", 1),
    ("AREA_FOUND", "3040045626"): ("Found", 1),
    ("AREA_NOTFOUND", "3040045630"): ("Not Found", 0),
    ("AREA_NOTFOUND", "3040045631"): ("Bad OSAPR", 0),
    ("AREA_BADOSAPR", "3040045640"): ("Bad OSAPR", 0),
    ("AREA_BADOSAPR", "3040045641"): ("Bad OSAPR", 0),
    ("AREA_SINGLE", "3040045650"): ("Single Addr", 0),
}
EXPECTED_TELEPHONE_AREAS = {"AREA_FOUND"}
EXPECTED_TELEPHONE_COUNT = 2
# Numbers whose PRESENCE is a defect, in every mode.
TELEPHONE_MUST_BE_ABSENT = {
    "07700900003": "AREA_NOTFOUND's MSISDN -- must be withheld by the privacy rule (only one matched address in its area)",
    "07700900999": "the under-17 JT customer -- must be removed by the age filter",
}
EMPTY_MODE_ALLOWED_STATUSES = {"Bad OSAPR", "Single Addr"}

TELEPHONE_COLUMNS = ["targetAreaID", "telephone"]
OSAPR_COLUMNS = ["targetAreaID", "osapr", "count", "status"]

# The four egress roles. Name shapes come from the spec's export_file_name_format
# (EE_${export_file_date}-<ROLE>_${export_file_sequence}) plus the pgp_zip sink's suffixing:
# <name>.csv.gz for archive_format gzip, and .gpg appended when pgp_encryption is enabled.
# '-TELEPHONE_' is anchored on the hyphen so it cannot also match '-LEIDOS_TELEPHONE_'.
_DATE = r"(?P<date>\d{4}-\d{2}-\d{2})"
_SEQ = r"(?P<seq>\d+[Oo][Ff]\d+)"
FILE_ROLES = {
    "LEIDOS_TELEPHONE": {
        "regex": re.compile(r"^EE_%s-LEIDOS_TELEPHONE_%s\.csv\.gz$" % (_DATE, _SEQ)),
        "encrypted": False,
        "table": "uc6_telephone_output",
        "columns": TELEPHONE_COLUMNS,
    },
    "LEIDOS_OSAPR": {
        "regex": re.compile(r"^EE_%s-LEIDOS_OSAPR_%s\.csv\.gz$" % (_DATE, _SEQ)),
        "encrypted": False,
        "table": "uc6_osapr_output",
        "columns": OSAPR_COLUMNS,
    },
    "TELEPHONE": {
        "regex": re.compile(r"^EE_%s-TELEPHONE_%s\.csv\.gz\.gpg$" % (_DATE, _SEQ)),
        "encrypted": True,
        "table": "uc6_telephone_output",
        "columns": TELEPHONE_COLUMNS,
    },
    "OSAPR": {
        "regex": re.compile(r"^EE_%s-OSAPR_%s\.csv\.gz\.gpg$" % (_DATE, _SEQ)),
        "encrypted": True,
        "table": "uc6_osapr_output",
        "columns": OSAPR_COLUMNS,
    },
}
# (plain Leidos file, encrypted Fujitsu-equivalent file) -- must carry identical rows.
EQUIVALENT_PAIRS = [("LEIDOS_TELEPHONE", "TELEPHONE"), ("LEIDOS_OSAPR", "OSAPR")]
TELEPHONE_ROLES = {"LEIDOS_TELEPHONE", "TELEPHONE"}


# ---------------------------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------------------------

class Report:
    """Collects PASS/FAIL results; every check prints its actual and expected value so a FAIL
    is diagnosable from the transcript alone, without re-running anything."""

    def __init__(self):
        self.results = []

    def check(self, check_id, label, ok, actual, expected):
        self.results.append((check_id, label, bool(ok)))
        print("  %s  %s  %s" % ("PASS" if ok else "FAIL", check_id, label))
        print("          actual:   %s" % _fmt(actual))
        print("          expected: %s" % _fmt(expected))
        return bool(ok)

    def failed(self):
        return [(cid, label) for cid, label, ok in self.results if not ok]


def _fmt(value):
    if isinstance(value, (set, frozenset)):
        return json.dumps(sorted(_jsonable(v) for v in value))
    if isinstance(value, dict):
        return json.dumps({str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))})
    return json.dumps(_jsonable(value), default=str)


def _jsonable(value):
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, (set, frozenset)):
        return sorted(_jsonable(v) for v in value)
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return value


# ---------------------------------------------------------------------------------------------
# workspace access -- SELECTs and reads only
# ---------------------------------------------------------------------------------------------

def connect(profile):
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=profile)


def query(w, warehouse_id, catalog, sql):
    """Run one SELECT on the SQL warehouse and return (column_names, rows) with every cell as a
    string ('' for NULL) -- the same shape the CSV export has, so C6 compares like with like."""
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

    response = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        catalog=catalog,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    while response.status is not None and response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        response = w.statement_execution.get_statement(response.statement_id)
    state = response.status.state if response.status is not None else None
    if state != StatementState.SUCCEEDED:
        error = response.status.error.message if response.status is not None and response.status.error else "(no message)"
        raise RuntimeError("statement %s ended %s: %s\n  SQL: %s" % (response.statement_id, state, error, sql))

    columns = [c.name for c in response.manifest.schema.columns]
    rows = list(response.result.data_array or [])
    chunk = response.result
    while chunk is not None and chunk.next_chunk_index is not None:
        chunk = w.statement_execution.get_statement_result_chunk_n(response.statement_id, chunk.next_chunk_index)
        rows.extend(chunk.data_array or [])
    return columns, [["" if v is None else str(v) for v in row] for row in rows]


def list_output_files(w, output_dir):
    """Regular files directly under the output volume path. The sink's own _staging/ directory
    lives alongside the exports and is skipped -- it is scaffolding, not an export."""
    entries = []
    for entry in w.files.list_directory_contents(output_dir):
        if entry.is_directory:
            continue
        entries.append((entry.name, entry.path, entry.file_size))
    return sorted(entries)


def download(w, path):
    return w.files.download(path).contents.read()


def decode_export(raw, encrypted):
    """Reverse the sink's envelope: [PGP symmetric] -> gzip -> UTF-8 text. Raises on any layer."""
    payload = raw
    if encrypted:
        sys.path.insert(0, os.path.join(REPO, "src"))
        from flowx.lakeflow_framework.crypto.pgp import pgp_decrypt_symmetric
        payload = pgp_decrypt_symmetric(raw, PASSPHRASE)
    return gzip.decompress(payload).decode("utf-8")


def parse_pipe_csv(text):
    """(header, rows) via the csv module with delimiter '|' -- the exact inverse of the sink's
    csv.DictWriter, so any quoting it applied is undone rather than split naively."""
    reader = csv.reader(io.StringIO(text, newline=""), delimiter="|")
    lines = [row for row in reader if row]
    if not lines:
        return [], []
    return lines[0], lines[1:]


# ---------------------------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------------------------

def check_gold_osapr(report, columns, rows, expect_empty):
    actual = {}
    duplicates = []
    idx = {c: i for i, c in enumerate(columns)}
    for row in rows:
        key = (row[idx["targetAreaID"]], row[idx["osapr"]])
        if key in actual:
            duplicates.append(key)
        actual[key] = (row[idx["status"]], int(row[idx["count"]]) if row[idx["count"]] != "" else None)

    if expect_empty:
        statuses = {status for status, _ in actual.values()}
        report.check(
            "C1", "gold uc6_osapr_output statuses (supplied zero-match bundle) are a subset of the no-match statuses",
            statuses <= EMPTY_MODE_ALLOWED_STATUSES and not duplicates,
            {"rows": len(rows), "statuses": statuses, "duplicate_keys": duplicates},
            {"statuses_subset_of": EMPTY_MODE_ALLOWED_STATUSES, "duplicate_keys": []},
        )
        return

    report.check(
        "C1", "gold uc6_osapr_output row count == %d" % len(EXPECTED_OSAPR),
        len(rows) == len(EXPECTED_OSAPR), len(rows), len(EXPECTED_OSAPR),
    )
    diff = {}
    for key in sorted(set(EXPECTED_OSAPR) | set(actual)):
        if EXPECTED_OSAPR.get(key) != actual.get(key):
            diff["%s/%s" % key] = {"expected": EXPECTED_OSAPR.get(key), "actual": actual.get(key)}
    ok = not diff and not duplicates
    report.check(
        "C1", "gold uc6_osapr_output every (targetAreaID, osapr) -> (status, count) matches the decision table",
        ok,
        {"mismatches": diff, "duplicates": duplicates, "keys_compared": len(actual)},
        {"mismatches": {}, "duplicates": [], "keys_compared": len(EXPECTED_OSAPR)},
    )


def check_gold_telephone(report, columns, rows, expect_empty):
    idx = {c: i for i, c in enumerate(columns)}
    areas = {row[idx["targetAreaID"]] for row in rows}
    numbers = {row[idx["telephone"]] for row in rows}

    if expect_empty:
        report.check("C2", "gold uc6_telephone_output has zero rows (supplied zero-match bundle)", len(rows) == 0,
                     {"rows": len(rows), "areas": areas}, {"rows": 0, "areas": set()})
    else:
        report.check("C2", "gold uc6_telephone_output targetAreaID set", areas == EXPECTED_TELEPHONE_AREAS,
                     areas, EXPECTED_TELEPHONE_AREAS)
        report.check("C2", "gold uc6_telephone_output row count == %d" % EXPECTED_TELEPHONE_COUNT,
                     len(rows) == EXPECTED_TELEPHONE_COUNT, len(rows), EXPECTED_TELEPHONE_COUNT)

    for number, reason in TELEPHONE_MUST_BE_ABSENT.items():
        report.check("C2", "gold uc6_telephone_output must NOT contain %s: %s" % (number, reason),
                     number not in numbers, "present" if number in numbers else "absent", "absent")


def classify_files(entries):
    """Map each listed file to its role; returns (role -> [(name, path, size)], unmatched names)."""
    by_role = {role: [] for role in FILE_ROLES}
    unmatched = []
    for name, path, size in entries:
        for role, spec in FILE_ROLES.items():
            if spec["regex"].match(name):
                by_role[role].append((name, path, size))
                break
        else:
            unmatched.append(name)
    return by_role, unmatched


def check_file_inventory(report, entries, by_role, unmatched, expect_empty):
    names = [name for name, _, _ in entries]
    if expect_empty:
        # An empty gold table yields no archive at all (pgp_zip_sink.write() drops the zero-row
        # staged part; commit() returns before archiving), so the telephone files must be absent.
        expected_roles = {role for role in FILE_ROLES if role not in TELEPHONE_ROLES}
    else:
        expected_roles = set(FILE_ROLES)
    expected_total = len(expected_roles)

    report.check("C3", "output volume holds exactly %d export file(s), all with a recognised name shape" % expected_total,
                 len(entries) == expected_total and not unmatched,
                 {"count": len(entries), "names": names, "unmatched_names": unmatched},
                 {"count": expected_total, "unmatched_names": []})
    for role, spec in FILE_ROLES.items():
        found = [name for name, _, _ in by_role[role]]
        if role in expected_roles:
            report.check("C3", "exactly one %s file matching %s" % (role, spec["regex"].pattern),
                         len(found) == 1, found, "one file")
        else:
            report.check("C3", "no %s file (empty gold table -> sink writes no archive; a file here is stale output "
                               "from an earlier run -- clear the output directory)" % role,
                         len(found) == 0, found, [])


def check_decode(report, w, by_role):
    """C4: download and unwrap each file. Returns role -> decoded text (only for successes)."""
    decoded = {}
    for role, spec in FILE_ROLES.items():
        for name, path, size in by_role[role]:
            step = "decrypt+gunzip" if spec["encrypted"] else "gunzip"
            try:
                raw = download(w, path)
                text = decode_export(raw, spec["encrypted"])
            except Exception as exc:  # any layer: transport, PGP, gzip, UTF-8
                report.check("C4", "%s %s: %s" % (role, name, step),
                             False, "%s: %s" % (type(exc).__name__, exc), "clean decode")
                continue
            report.check("C4", "%s %s: %s" % (role, name, step),
                         True, {"bytes_on_volume": size, "decoded_chars": len(text)}, "clean decode")
            decoded[role] = text
    return decoded


def check_layout(report, decoded):
    """C5: header, delimiter, LF terminator, uniform width. Returns role -> data rows."""
    parsed = {}
    for role, text in decoded.items():
        spec = FILE_ROLES[role]
        header, rows = parse_pipe_csv(text)
        expected_header = spec["columns"]
        widths = {len(r) for r in rows}
        problems = []
        if header != expected_header:
            problems.append("header %r" % header)
        if "\r" in text:
            problems.append("CR present (expected LF-only line terminator)")
        if not text.endswith("\n"):
            problems.append("no trailing LF")
        if widths - {len(expected_header)}:
            problems.append("row widths %s" % sorted(widths))
        report.check("C5", "%s header %s, pipe-delimited, LF-terminated, %d columns wide" %
                     (role, "|".join(expected_header), len(expected_header)),
                     not problems,
                     problems if problems else {"header": header, "data_rows": len(rows)},
                     {"header": expected_header, "line_terminator": "LF"})
        if header == expected_header:
            parsed[role] = rows
    return parsed


def check_file_vs_gold(report, parsed, by_role, gold):
    """C6: rows in the file == rows in the gold table it exports (set AND count equality)."""
    for role, spec in FILE_ROLES.items():
        gold_columns, gold_rows = gold[spec["table"]]
        idx = [gold_columns.index(c) for c in spec["columns"]]
        gold_tuples = [tuple(row[i] for i in idx) for row in gold_rows]
        if not by_role[role]:
            # Only defensible when the gold table is empty -- the sink writes nothing for zero rows.
            report.check("C6", "%s absent -- defensible only if %s is empty" % (role, spec["table"]),
                         len(gold_tuples) == 0, {"gold_rows": len(gold_tuples)}, {"gold_rows": 0})
            continue
        if role not in parsed:
            report.check("C6", "%s rows == gold %s rows" % (role, spec["table"]), False,
                         "file could not be decoded/parsed (see C4/C5)", "%d row(s)" % len(gold_tuples))
            continue
        file_tuples = [tuple(r) for r in parsed[role]]
        only_file = sorted(set(file_tuples) - set(gold_tuples))
        only_gold = sorted(set(gold_tuples) - set(file_tuples))
        ok = not only_file and not only_gold and len(file_tuples) == len(gold_tuples)
        report.check("C6", "%s rows == gold %s rows on %s" % (role, spec["table"], "|".join(spec["columns"])),
                     ok,
                     {"file_rows": len(file_tuples), "gold_rows": len(gold_tuples),
                      "only_in_file": only_file, "only_in_gold": only_gold},
                     {"only_in_file": [], "only_in_gold": [], "row_counts_equal": True})


def check_pairs(report, decoded, parsed, by_role):
    """C7: the Leidos (plain) and Fujitsu-equivalent (GPG) files carry identical rows."""
    for plain_role, gpg_role in EQUIVALENT_PAIRS:
        plain_present, gpg_present = bool(by_role[plain_role]), bool(by_role[gpg_role])
        label = "%s content == %s content" % (plain_role, gpg_role)
        if not plain_present and not gpg_present:
            report.check("C7", label + " (both absent -- consistent with an empty gold table)", True,
                         "neither produced", "both present or both absent")
            continue
        if plain_present != gpg_present:
            report.check("C7", label, False,
                         {"%s_present" % plain_role: plain_present, "%s_present" % gpg_role: gpg_present},
                         "both present or both absent")
            continue
        if plain_role not in parsed or gpg_role not in parsed:
            report.check("C7", label, False, "one side could not be decoded/parsed (see C4/C5)", "identical rows")
            continue
        plain_rows, gpg_rows = sorted(map(tuple, parsed[plain_role])), sorted(map(tuple, parsed[gpg_role]))
        ok = plain_rows == gpg_rows
        report.check("C7", label, ok,
                     {"rows_identical": ok,
                      "byte_identical": decoded[plain_role] == decoded[gpg_role],
                      "plain_rows": len(plain_rows), "gpg_rows": len(gpg_rows),
                      "only_in_plain": sorted(set(plain_rows) - set(gpg_rows)),
                      "only_in_gpg": sorted(set(gpg_rows) - set(plain_rows))},
                     {"rows_identical": True})


# ---------------------------------------------------------------------------------------------

def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--expect-empty", action="store_true",
                        help="the SUPPLIED zero-match bundle was loaded: no Found/Not Found rows, no telephones, "
                             "and therefore no TELEPHONE export files")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="databricks CLI profile (default %(default)s)")
    parser.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID, help="SQL warehouse id (default %(default)s)")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="UC catalog holding the gold schema (default %(default)s)")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="volume path of the exports (default %(default)s)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    report = Report()
    mode = "SUPPLIED ZERO-MATCH BUNDLE (--expect-empty)" if args.expect_empty else "AUGMENTED FIXTURE"
    print("UC6 output validation -- profile=%s warehouse=%s catalog=%s\n  output=%s\n  mode=%s"
          % (args.profile, args.warehouse_id, args.catalog, args.output_dir, mode))

    try:
        w = connect(args.profile)
        print("\n===== gold tables =====")
        gold = {}
        for table, columns in (("uc6_osapr_output", OSAPR_COLUMNS), ("uc6_telephone_output", TELEPHONE_COLUMNS)):
            sql = "SELECT %s FROM %s.gold.%s" % (", ".join("`%s`" % c for c in columns), args.catalog, table)
            gold[table] = query(w, args.warehouse_id, args.catalog, sql)
            print("  %-22s %d row(s)" % (table, len(gold[table][1])))
        print("\n===== output volume =====")
        entries = list_output_files(w, args.output_dir)
        for name, _, size in entries:
            print("  %-56s %s bytes" % (name, size))
        if not entries:
            print("  (no files)")
    except Exception as exc:
        # Infrastructure failure, not a validation verdict -- distinct exit code so a CI wrapper
        # does not read 'could not connect' as 'the pipeline output is wrong'.
        print("\nERROR: could not read the workspace: %s: %s" % (type(exc).__name__, exc))
        return 2

    print("\n===== C1 gold osapr =====")
    check_gold_osapr(report, *gold["uc6_osapr_output"], expect_empty=args.expect_empty)
    print("\n===== C2 gold telephone =====")
    check_gold_telephone(report, *gold["uc6_telephone_output"], expect_empty=args.expect_empty)
    print("\n===== C3 file inventory =====")
    by_role, unmatched = classify_files(entries)
    check_file_inventory(report, entries, by_role, unmatched, args.expect_empty)
    print("\n===== C4 envelope: decrypt / gunzip =====")
    decoded = check_decode(report, w, by_role)
    print("\n===== C5 layout: header / delimiter / terminator =====")
    parsed = check_layout(report, decoded)
    print("\n===== C6 file rows vs gold rows =====")
    check_file_vs_gold(report, parsed, by_role, gold)
    print("\n===== C7 Leidos vs Fujitsu-equivalent =====")
    check_pairs(report, decoded, parsed, by_role)

    failed = report.failed()
    print("\n" + "=" * 72)
    if failed:
        print("FAILURES:")
        for cid, label in failed:
            print("  - %s %s" % (cid, label))
        print("UC6 OUTPUT VALIDATION: %d FAILED" % len(failed))
        return 1
    print("UC6 OUTPUT VALIDATION: ALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
