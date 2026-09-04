"""Unit test proving spec_22_optional_fields_df_customer_ingest.json -- a real, on-disk
reproduction of a user-reported onboarding spec -- no longer blocks on fields that don't
need to be required, except the ones that genuinely cannot be defaulted.

Original report: onboarding an autoloader flow (dataflow_id df_customer_ingest) with no
source_config.schema_location, an empty target_config.auto_ttl block, and an
encrypted_columns entry with neither column_name nor a secret reference populated produced
several "is required" errors. Fixed via spec_validator.py:

  * source_config.schema_location -- auto-derived from target_catalog/target_table
    (/Volumes/<catalog>/landing/_schemas/<table>/) when omitted. No error.
  * target_config.auto_ttl.timestamp_column / .expire_in_days -- Auto TTL is opt-in; an
    incomplete block just means TTL isn't applied for this flow (see
    storage/table_properties.py::build_auto_ttl_kwarg's matching soft-skip). No error.
  * target_config.encrypted_columns[0].column_name / .secret -- deliberately NOT relaxed:
    there is no safe default for which column to encrypt or which Unity Catalog secret to
    use, and silently skipping either would leave PII unencrypted with no indication. These
    two still correctly report as required (v2 schema: secret_scope's old "security"
    convention default is retired along with classic scopes -- a Unity Catalog
    catalog.schema.key reference has no comparably safe universal default, so the whole
    ``secret`` block is required outright rather than partially defaulted).

Pure Python: this spec has no transformation_flows, so validate_spec never touches Spark
(spark=None is a safe sentinel; see tests/unit/test_spec_validator.py's module docstring).
"""

import json
from pathlib import Path

from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec

SPEC_PATH = Path(__file__).resolve().parents[2] / "test_specs" / "spec_22_optional_fields_df_customer_ingest.json"


def _load_spec() -> dict:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def test_spec_file_exists_on_disk():
    assert SPEC_PATH.exists(), f"missing {SPEC_PATH}"


def test_previously_blocking_optional_fields_no_longer_error():
    spec = _load_spec()
    _, _, _, _, errors = validate_spec(None, spec)
    joined = "\n".join(errors)
    assert "schema_location" not in joined
    assert "auto_ttl.timestamp_column" not in joined
    assert "auto_ttl.expire_in_days" not in joined


def test_column_name_and_secret_still_correctly_required():
    spec = _load_spec()
    _, _, _, _, errors = validate_spec(None, spec)
    joined = "\n".join(errors)
    assert "encrypted_columns[0].column_name: is required" in joined
    assert "encrypted_columns[0].secret: is required" in joined


def test_error_count_is_exactly_the_two_irreducible_fields():
    spec = _load_spec()
    _, _, _, _, errors = validate_spec(None, spec)
    assert len(errors) == 2, f"expected exactly 2 remaining errors, got {len(errors)}:\n" + "\n".join(errors)


def test_schema_location_default_is_persisted_onto_the_flow():
    """validate_spec mutates the flow dicts in place -- the derived default must be what
    actually gets written to the control table, not just what silences the validator."""
    spec = _load_spec()
    ingestion_flows, _, _, _, _ = validate_spec(None, spec)
    flow = ingestion_flows[0]
    assert flow["source_config"]["schema_location"] == "/Volumes/flowx/landing/_schemas/customer_raw/"
