"""Unit test proving spec_12_config_validation_negative.json -- the real, on-disk
reference example of a broken onboarding spec -- is caught exhaustively by
spec_validator.py. Pure Python: this spec has no transformation_flows, so
validate_spec never touches Spark (spark=None is a safe sentinel; see
tests/unit/test_spec_validator.py's module docstring for the same reasoning).

This is deliberately NOT wired into resources/sample_pipelines_job.yml as a job task --
see the note in that file for why a task that's *supposed* to fail would pollute the
job's pass/fail signal. This test is the actual verification.
"""

import json
from pathlib import Path

from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec

SPEC_PATH = Path(__file__).resolve().parents[2] / "test_specs" / "spec_12_config_validation_negative.json"


def _load_spec() -> dict:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def test_spec_file_exists_on_disk():
    assert SPEC_PATH.exists(), f"missing {SPEC_PATH}"


def test_every_deliberate_problem_is_caught():
    spec = _load_spec()
    _, _, _, _, errors = validate_spec(None, spec)

    # One assertion per deliberately-planted problem -- proves the validator's exhaustive
    # (collect-everything, not fail-fast) behavior on a real, representative bad spec.
    joined = "\n".join(errors)
    assert "dataflow_group_id: is required" in joined
    assert "source_type: has invalid value 'autoloadr'" in joined
    assert "target_catalog: is required" in joined
    assert "target_schema: is required" in joined
    assert "target_table: is required" in joined
    assert "capture_technical_metadata" in joined and "boolean" in joined
    assert "archive_path: is required" in joined  # clean_source=archive requires it
    assert "retention_days" in joined and "integer" in joined
    assert "cdc_operation_mapping: is required" in joined  # cdc_operation_column set without it
    assert "dq_config.rules[0].rule_id: is required" in joined
    assert "storage_format" in joined and "iceberg" in joined and "batch_table" in joined
    assert "column_tags[0].column: is required" in joined


def test_error_count_matches_expected_problem_count():
    spec = _load_spec()
    _, _, _, _, errors = validate_spec(None, spec)
    # 12 deliberately-planted problems, see test_every_deliberate_problem_is_caught.
    # An exact count (not just >=) guards against the validator silently swallowing or
    # duplicating a finding as the spec/validator evolve.
    assert len(errors) == 12, f"expected exactly 12 validation errors, got {len(errors)}:\n" + "\n".join(errors)
