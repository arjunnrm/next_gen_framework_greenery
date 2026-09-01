"""Post-deployment verification for Metaflow test scenario 003 ("Autoload batch +
reconciliation") -- ``metaflow_testing/003_autoload_recon_append.json``, ``dataflow_group_id
dfg_metaflow_003_autoload_recon``.

This scenario's ingestion flow (``df_excalibur_autoload_ingest``) lands a fixed CSV batch
(``autoload_batch1.csv``, seeded by
``notebooks/00_seed_sample_data/02_seed_metaflow_testing_data.py``) into
``metaflow.bronze_excalibur.autoload_bronze`` via Auto Loader, ``cdc_load_strategy: APPEND``.
That batch deliberately includes ``C006``/``C007`` -- customers *absent* from scenario 002's
``zerobus_source_bus`` (and therefore from ``zerobus_bronze``) at the time both pipelines first
ran, per ``resources/feature_tests/metaflow_test_002_003_job.yml``'s task order (``run_002_pipeline`` before
``run_003_pipeline`` before ``run_003_reconciliation``).

**The reconciliation flow this scenario exists to exercise**
(``recon_excalibur_autoload_vs_zerobus``, run standalone by
``notebooks/05_reconciliation/05_reconciliation_engine.py`` -- reconciliation is not a Lakeflow
Declarative Pipeline flow type) compares ``autoload_bronze`` (source) against
``zerobus_bronze`` (``target_configs[0]``, ``target_id: "zerobus_bronze_target"``) on
``match_keys: ["customer_id"]`` / ``compare_columns: ["customer_name", "status"]``,
``comparison_direction: "source_to_target"``. C006/C007 are classified ``MISSING_IN_TARGET``.

**Why this scenario's single most important assertion is about
``zerobus_source_bus``, not about ``zerobus_bronze`` or the run log.** A shallow reading of
"reconciliation appends missing records" would expect the correction to land in
``zerobus_bronze`` -- the table actually being compared against. It does not: this flow's
``target_configs[0].append_target_table`` is
``metaflow.Excalibur_usecase.zerobus_source_bus``, the Zerobus **source bus table itself**.
That is the entire point of this scenario -- appending to the bronze *copy* would fix this
one comparison and nothing else; appending to the upstream *source* means the correction is
now indistinguishable from data that arrived at the bus normally, so the next time scenario
002's Zerobus stream reads that bus, it picks up C006/C007 as a completely ordinary new batch,
no special-casing required anywhere downstream. Proving *that* -- not just "some table
somewhere now has 7 rows" -- is what ``test_zerobus_source_bus_received_the_missing_records_
via_reconciliation_feedback`` below is for.

That test also confirms the flow's ``transform_sql`` (``SELECT customer_id, customer_name,
status, current_timestamp() AS updated_at FROM _reconciliation_unmatched_records``) genuinely
ran: ``autoload_bronze`` itself has no ``updated_at`` column at all (see
``test_metaflow_002_zerobus_data_load.py``'s module docstring for the column's real origin), so
a populated, non-null ``updated_at`` on the appended C006/C007 rows can only have come from
this reshape -- and it is written via ``mergeSchema=true``
(``reconciliation/appender.py::append_missing_records``), which is *also* why
``zerobus_source_bus``'s five original rows now show a NULL ``updated_at`` instead of lacking
the column: schema evolution backfills existing rows with NULL rather than leaving the column
partial.

The ``reconciliation_run_log`` assertions below check metric *relationships* every SUCCESS run
must satisfy, not a hardcoded row count or hardcoded ``target_record_count`` -- re-running
``databricks bundle run metaflow_test_002_003_job`` appends another log row every time (and,
per the module docstring above, can also grow ``zerobus_bronze`` itself once scenario 002 has
re-streamed past this correction), so a test asserting "exactly 1 run row" or "target_record_
count == 5 forever" would start failing on the very next end-to-end run. See
``tests/conftest.py`` and ``tests/integration/test_reconciliation.py``'s module/test docstrings
for this project's established rationale for relative-invariant assertions over an
accumulating log table.

Run ``databricks bundle run metaflow_test_002_003_job --target dev`` (see
``resources/feature_tests/metaflow_test_002_003_job.yml``) before running these -- these tests assert
against tables *already materialized* by that real run; Lakeflow Declarative Pipelines cannot
run locally (see the note in ``tests/conftest.py``).
"""

CATALOG = "metaflow"
CONTROL_SCHEMA = f"{CATALOG}.config"
AUTOLOAD_BRONZE = f"{CATALOG}.bronze_excalibur.autoload_bronze"
ZEROBUS_SOURCE_BUS = f"{CATALOG}.Excalibur_usecase.zerobus_source_bus"

RECONCILIATION_ID = "recon_excalibur_autoload_vs_zerobus"
TARGET_ID = "zerobus_bronze_target"

BUSINESS_COLUMNS = {"customer_id", "customer_name", "status"}

# autoload_bronze is fed by exactly one already-processed CSV file (autoload_batch1.csv);
# Auto Loader's own checkpoint means re-running this job never reprocesses that same file, so
# -- unlike zerobus_bronze in the companion test file -- this table's row count and row set
# are genuinely stable across reruns, not just a floor.
EXPECTED_ROWS = {
    "C001": ("Ava Thompson", "ACTIVE"),
    "C002": ("Noah Martinez", "ACTIVE"),
    "C003": ("Liam Chen", "ACTIVE"),
    "C004": ("Emma Wilson", "INACTIVE"),
    "C005": ("Olivia Brown", "ACTIVE"),
    "C006": ("William Davis", "ACTIVE"),
    "C007": ("Sophia Garcia", "ACTIVE"),
}


# ---------------------------------------------------------------------------
# autoload_bronze: existence + shape
# ---------------------------------------------------------------------------


def test_autoload_bronze_exists(table_exists):
    """Confirms the Auto Loader ingestion flow actually registered and materialized a real
    table -- not just that onboarding parsed the spec."""
    assert table_exists(AUTOLOAD_BRONZE), f"expected {AUTOLOAD_BRONZE} to exist after a real pipeline run"


def test_autoload_bronze_row_count_and_content(spark):
    """Exactly 7 rows (C001-C007), including the 2 (C006/C007) deliberately absent from
    zerobus_bronze -- this exact gap is what gives the reconciliation flow below something to
    detect and act on."""
    df = spark.table(AUTOLOAD_BRONZE)
    count = df.count()
    assert count == 7, f"expected exactly 7 rows in {AUTOLOAD_BRONZE} (a single already-processed CSV batch), got {count}"

    columns = set(df.columns)
    assert BUSINESS_COLUMNS <= columns, f"{AUTOLOAD_BRONZE} missing expected business column(s): {BUSINESS_COLUMNS - columns}"

    rows = {row["customer_id"]: (row["customer_name"], row["status"]) for row in df.collect()}
    assert set(rows) == set(EXPECTED_ROWS), f"expected exactly {set(EXPECTED_ROWS)}, got {set(rows)}"
    for customer_id, expected in EXPECTED_ROWS.items():
        assert rows[customer_id] == expected, f"expected {customer_id} to be {expected}, got {rows[customer_id]}"


# ---------------------------------------------------------------------------
# reconciliation_run_log: relative invariants across every SUCCESS run
# ---------------------------------------------------------------------------


def test_reconciliation_run_log_metrics_hold_for_every_success_run(spark):
    """Checks metric *relationships* every SUCCESS run of this reconciliation flow/target must
    satisfy, not hardcoded absolute counts tied to one specific run -- see module docstring for
    why (re-running the job appends another log row, and can also grow zerobus_bronze itself
    once scenario 002 re-streams past this correction, so a brittle absolute assertion here
    would break on the very next end-to-end run).

    ``source_record_count`` is the one exception, matching ``test_reconciliation.py``'s own
    precedent for its analogous baseline flow: autoload_bronze is fed by a single, already-
    processed CSV batch with no reconciliation-driven growth of its own (nothing ever appends
    *into* autoload_bronze), so it must stay stable at 7 across every run regardless of
    history.
    """
    runs = (
        spark.table(f"{CONTROL_SCHEMA}.reconciliation_run_log")
        .filter(f"reconciliation_id = '{RECONCILIATION_ID}' AND target_id = '{TARGET_ID}' AND status = 'SUCCESS'")
        .orderBy("run_at")
        .collect()
    )
    assert len(runs) >= 1, f"expected at least one SUCCESS run for {RECONCILIATION_ID}/{TARGET_ID}"

    for run in runs:
        assert run["source_record_count"] == 7, (
            f"expected source_record_count == 7 (autoload_bronze's stable row count) on every run, "
            f"got {run['source_record_count']} for run_id={run['run_id']}"
        )
        # matched + missing_in_target (drift+missing, source_to_target) must account for every
        # autoload_bronze record exactly once -- autoload_bronze has no duplicate customer_id
        # keys, so this holds precisely, not just as an upper bound.
        assert run["matched_count"] + run["missing_in_target_count"] == run["source_record_count"], (
            f"matched_count ({run['matched_count']}) + missing_in_target_count "
            f"({run['missing_in_target_count']}) should equal source_record_count "
            f"({run['source_record_count']}) for run_id={run['run_id']}"
        )
        # value_drift_count is a subset of missing_in_target_count by construction
        # (matcher.py's missing_in_target_count is defined as the union of MISSING_IN_TARGET +
        # VALUE_DRIFT) -- a structural invariant, not merely true of today's data.
        assert run["value_drift_count"] <= run["missing_in_target_count"]
        # comparison_direction is source_to_target-only for this target, but matcher.py always
        # computes all four categories in one pass (see appender.py's "Direction gating" note)
        # -- missing_in_source_count == 0 here reflects that every zerobus_bronze row has a
        # matching autoload_bronze counterpart, on every run so far.
        assert run["missing_in_source_count"] == 0
        # A run only appends when it found something to append, and never more than it found --
        # true whether this run actually appended (first time seeing this miss-set fingerprint)
        # or was a SKIPPED_ALREADY_PROCESSED no-op counted as 0.
        assert run["appended_count"] <= run["missing_in_target_count"]


# ---------------------------------------------------------------------------
# zerobus_source_bus: the actual point of this whole scenario
# ---------------------------------------------------------------------------


def test_zerobus_source_bus_received_the_missing_records_via_reconciliation_feedback(spark):
    """The single assertion that actually proves this scenario's design point: reconciliation
    appends its correction into the Zerobus *source* bus table
    (``metaflow.Excalibur_usecase.zerobus_source_bus``), not merely into ``zerobus_bronze``'s
    bronze copy -- see module docstring for why that distinction is the entire reason this
    scenario exists (it's what lets the *next* Zerobus stream read pick C006/C007 up as
    ordinary new data, no special-casing required).

    Three things all have to be true at once for this to be a genuine pass, not a coincidence:

    1. C006 ("William Davis")/C007 ("Sophia Garcia") are present in ``zerobus_source_bus``
       with a non-null ``updated_at`` -- proving ``transform_sql`` actually ran (autoload_
       bronze, the only place these rows could have come from, has no ``updated_at`` column at
       all, so a populated value could only have come from the flow's own
       ``current_timestamp() AS updated_at`` projection).
    2. C001-C005 -- ``zerobus_source_bus``'s original rows -- are still present and otherwise
       untouched, with ``updated_at`` still NULL (schema evolution backfills existing rows with
       NULL rather than mutating them) -- proving the append genuinely *added* rows rather than,
       say, a bug that rewrote/merged the whole table and coincidentally left 7 rows behind.
    3. Each of C006/C007 appears exactly once -- proving the fingerprint-based idempotency
       ``reconciliation/appender.py`` documents (a duplicate append on a rerun against an
       unchanged miss-set would be the "real, narrow correctness gap" that same module's
       docstring warns is only *safe* under append-only assumptions, not guaranteed-impossible
       by magic).
    """
    df = spark.table(ZEROBUS_SOURCE_BUS)
    rows = {row["customer_id"]: row for row in df.collect()}

    for customer_id, expected_name in (("C006", "William Davis"), ("C007", "Sophia Garcia")):
        assert customer_id in rows, f"expected {customer_id} to have been appended into {ZEROBUS_SOURCE_BUS}"
        row = rows[customer_id]
        assert row["customer_name"] == expected_name, (
            f"expected {customer_id}'s customer_name to be {expected_name!r}, got {row['customer_name']!r}"
        )
        assert row["status"] == "ACTIVE", f"expected {customer_id}'s status to be ACTIVE, got {row['status']}"
        assert row["updated_at"] is not None, (
            f"expected {customer_id}'s updated_at to be populated by transform_sql's "
            "current_timestamp() projection, got NULL"
        )

    for customer_id in ("C001", "C002", "C003", "C004", "C005"):
        assert customer_id in rows, f"expected original seed row {customer_id} to still be present in {ZEROBUS_SOURCE_BUS}"
        assert rows[customer_id]["updated_at"] is None, (
            f"expected {customer_id} (never touched by reconciliation) to still have a NULL updated_at, "
            f"got {rows[customer_id]['updated_at']}"
        )

    dupes = (
        df.filter("customer_id IN ('C006', 'C007')")
        .groupBy("customer_id")
        .count()
        .filter("count > 1")
        .collect()
    )
    assert not dupes, f"reconciliation must never duplicate its appended correction rows on rerun, found: {dupes}"
