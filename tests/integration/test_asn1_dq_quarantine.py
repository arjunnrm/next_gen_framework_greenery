"""Post-deployment verification for the ASN.1 DQ/quarantine pipeline (spec_11).

Run ``databricks bundle run sample_pipelines_job`` (task
``run_asn1_cdr_dq_quarantine_pipeline``) before running these.
"""

CATALOG = "poc"
MAIN_TABLE = f"{CATALOG}.bronze_telecom_v2.raw_cdr_v2"
QUARANTINE_TABLE = f"{CATALOG}.bronze_telecom_v2.raw_cdr_v2_quarantine"


def test_valid_records_land_in_main_table(spark):
    df = spark.table(MAIN_TABLE)
    record_ids = {row["recordId"] for row in df.select("recordId").collect()}
    assert record_ids == {"REC-001", "REC-002"}


def test_main_table_has_no_quarantine_process_columns(spark):
    df = spark.table(MAIN_TABLE)
    for leaked_column in ("__framework_dq_quarantine_flag", "__framework_dq_failed_rule_ids", "__framework_dq_failure_reasons"):
        assert leaked_column not in df.columns


def test_three_records_are_quarantined(spark):
    df = spark.table(QUARANTINE_TABLE)
    assert df.count() == 3


def test_negative_call_duration_record_quarantined_with_correct_reason(spark):
    row = spark.table(QUARANTINE_TABLE).filter("recordId = 'REC-003'").collect()[0]
    assert "dq_call_duration_non_negative" in row["__framework_dq_failed_rule_ids"]
    assert any("dq_call_duration_non_negative" in reason for reason in row["__framework_dq_failure_reasons"])


def test_empty_imsi_record_quarantined_with_correct_reason(spark):
    row = spark.table(QUARANTINE_TABLE).filter("recordId = 'REC-004'").collect()[0]
    assert "dq_imsi_present" in row["__framework_dq_failed_rule_ids"]


def test_malformed_record_quarantined_via_decode_error_rule(spark):
    df = spark.table(QUARANTINE_TABLE)
    decode_failures = df.filter("array_contains(__framework_dq_failed_rule_ids, 'dq_asn1_decode_ok')").collect()
    assert len(decode_failures) == 1
    assert decode_failures[0]["_asn1_decode_error"] is not None


def test_quarantine_metadata_is_fully_populated(spark):
    rows = spark.table(QUARANTINE_TABLE).collect()
    for row in rows:
        assert row["__framework_quarantine_validated_at"] is not None
        assert row["__framework_pipeline_run_id"] is not None
        assert row["__framework_source_file_name"] is not None
        # __framework_record_id is expected to be null only for the fully-malformed decode failure
        # (record_id itself failed to decode); every DQ-rule-violation row must have it.
        if "dq_asn1_decode_ok" not in row["__framework_dq_failed_rule_ids"]:
            assert row["__framework_record_id"] is not None
