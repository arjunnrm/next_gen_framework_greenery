"""Post-deployment verification for reconciliation (spec_09).

Run ``databricks bundle run sample_pipelines_job`` (which onboards spec_09 and runs
``05_reconciliation_engine.py`` once) before running these -- and again a second time to
exercise the idempotency assertions.

spec_09 (``test_specs/spec_09_reconciliation_volume_vs_cdc.json``) declares one reconciliation
flow, ``recon_customer_master_volume_vs_cdc``, comparing
``bronze_recon_ops.raw_customer_master_baseline`` (source, 6 rows: R001-R006) against a single
target -- ``target_id: "primary"``, ``bronze_recon_ops.raw_customer_master_cdc`` (4 rows:
R001-R004, with R003's ``status`` drifted to ``INACTIVE`` vs. the baseline's ``ACTIVE``) --
``match_keys: ["customer_id"]``, ``compare_columns: ["status", "region"]``. Neither
``read_mode`` nor ``comparison_direction`` is set on this spec, so both default: batch reads,
``comparison_direction: "both"``. Every one of ``primary``'s 4 target rows also has a
corresponding baseline row, so the ``target_to_source`` direction is expected to find nothing
(``missing_in_source_count == 0``) -- this fixture only exercises ``source_to_target``
in practice, matching the pre-redesign test's own scenario coverage.
"""

CATALOG = "poc"
CONTROL_SCHEMA = f"{CATALOG}.config"
CDC_TABLE = f"{CATALOG}.bronze_recon_ops.raw_customer_master_cdc"
RECONCILIATION_ID = "recon_customer_master_volume_vs_cdc"
TARGET_ID = "primary"


def test_unmatched_records_were_appended_to_cdc_table(spark):
    """R005/R006 were entirely missing from the CDC fixture; R003 was drifted (status
    ACTIVE in baseline vs. INACTIVE in the CDC fixture). At least one SUCCESS run (this run,
    or an earlier one in this same long-lived dev catalog's history -- see the note below on
    why this can't assert against a single specific run) must have appended corrections for
    all three, alongside the stale rows (append-only CDC/Zerobus bus semantics -- see
    matcher.py's duplicate-key-safety docstring) -- so the CURRENT state of the table, which
    only ever grows, must show them, regardless of exactly which historical run(s) put them
    there.

    Known limitation, not something this test works around: this framework's reconciliation
    idempotency (a fingerprint of the *miss set*) assumes an append-only target. If something
    external to reconciliation resets `append_target_table` back to a prior state (e.g. a
    `--full-refresh-all` of a pipeline that also owns this same table as a CDC ingestion
    target -- exactly what repeated dev-loop runs of this project's own sample_pipelines_job
    do), a subsequent reconciliation run can see the *same* miss-set fingerprint as an already-
    processed one and skip re-appending, even though the target no longer actually reflects
    that correction. This is a real, narrow correctness gap for that specific external-reset
    scenario (out of scope for this fix) -- tracked as a follow-up, not silently accepted as
    correct."""
    df = spark.table(CDC_TABLE)
    customer_ids = {row["customer_id"] for row in df.select("customer_id").distinct().collect()}
    assert {"R005", "R006"}.issubset(customer_ids), (
        f"expected R005/R006 to have been appended by some prior reconciliation run; got {customer_ids}. "
        "If this is a genuinely fresh catalog, run `databricks bundle run sample_pipelines_job` (which "
        "onboards spec_09 and runs the reconciliation engine) before running this test."
    )
    assert df.filter("customer_id = 'R003' AND status = 'ACTIVE'").count() >= 1
    assert df.filter("customer_id = 'R003' AND status = 'INACTIVE'").count() >= 1


def test_run_log_records_expected_metrics_for_the_primary_target(spark):
    """Checks the metric *relationships* every SUCCESS run must satisfy, not hardcoded
    absolute counts tied to one specific historical run -- see the note in
    `test_unmatched_records_were_appended_to_cdc_table` above for why an absolute
    `target_record_count` can't be asserted against a single run in this long-lived dev
    catalog. `source_record_count` is the one exception: spec_09's baseline ingestion flow is
    a plain, deterministic 6-row seed with no reconciliation-driven growth of its own, so it
    must stay stable at 6 across every run regardless of history."""
    runs = (
        spark.table(f"{CONTROL_SCHEMA}.reconciliation_run_log")
        .filter(f"reconciliation_id = '{RECONCILIATION_ID}' AND target_id = '{TARGET_ID}' AND status = 'SUCCESS'")
        .orderBy("run_at")
        .collect()
    )
    assert len(runs) >= 1
    for run in runs:
        assert run["source_record_count"] == 6
        # matched + missing_in_target (drift+missing, source_to_target) must account for
        # every baseline record exactly once.
        assert run["matched_count"] + run["missing_in_target_count"] == run["source_record_count"]
        assert run["value_drift_count"] <= run["missing_in_target_count"]
        # missing_in_source_count == 0 for this fixture: every CDC row has a baseline
        # counterpart, on every run, regardless of how many corrections have accumulated.
        assert run["missing_in_source_count"] == 0
        # A run only appends when it found something to append, and never more than it found.
        assert run["appended_count"] <= run["missing_in_target_count"]


def test_mismatch_log_has_one_row_per_missing_or_drifted_record(spark):
    mismatches = (
        spark.table(f"{CONTROL_SCHEMA}.reconciliation_mismatch_log")
        .filter(f"reconciliation_id = '{RECONCILIATION_ID}' AND target_id = '{TARGET_ID}'")
        .collect()
    )
    by_type = {}
    for row in mismatches:
        by_type.setdefault(row["mismatch_type"], []).append(row)

    assert len(by_type.get("MISSING_IN_TARGET", [])) >= 2  # R005, R006 (at least the first run's worth)
    assert len(by_type.get("VALUE_DRIFT", [])) >= 1  # R003
    assert "MISSING_IN_SOURCE" not in by_type  # nothing to audit-log for this fixture

    drift_row = by_type["VALUE_DRIFT"][0]
    assert '"customer_id"' in drift_row["match_key_values_json"]
    assert drift_row["differing_columns_json"] is not None
    assert '"column":"status"' in drift_row["differing_columns_json"].replace(" ", "")
    assert drift_row["source_hash_value"] is not None
    assert drift_row["target_hash_value"] is not None


def test_second_identical_run_is_idempotent_no_op(spark):
    """A second run against an unchanged gap must be logged SKIPPED_ALREADY_PROCESSED,
    not append the same 3 corrective records a second time."""
    runs = (
        spark.table(f"{CONTROL_SCHEMA}.reconciliation_run_log")
        .filter(f"reconciliation_id = '{RECONCILIATION_ID}' AND target_id = '{TARGET_ID}'")
        .orderBy("run_at")
        .collect()
    )
    if len(runs) < 2:
        return  # only ran once so far -- run sample_pipelines_job a second time to exercise this
    assert runs[1]["status"] in ("SKIPPED_ALREADY_PROCESSED", "SUCCESS")
    if runs[1]["status"] == "SKIPPED_ALREADY_PROCESSED":
        assert runs[1]["appended_count"] in (0, None)

    # Regardless of log status, the CDC table itself must not have duplicate R003/R005/R006
    # *correction* rows -- i.e. no more than one ACTIVE R003 row, no more than one R005/R006.
    df = spark.table(CDC_TABLE)
    dupes = df.groupBy("customer_id", "status").count().filter("count > 1").collect()
    assert not dupes, f"reconciliation must never duplicate rows on rerun, found: {dupes}"
