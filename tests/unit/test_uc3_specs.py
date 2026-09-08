"""Offline-safe assertions about UC3's two onboarding specs (Excalibur streaming CDC and
batch-load + reconciliation).

Mirrors tests/unit/test_uc6_spec.py: no SQL is executed here (databricks-connect refuses a
local SparkSession, see that module's docstring). What IS pinned is every mistake the
2026-09-08 review actually found in these files:

* the batch-recon spec hard-coded the catalog ``br_digital_poc``, which is neither bundle
  target's catalog (``bt_digital_poc`` / ``flowx``), so its flows and reconciliations wrote to a
  different catalog than the pipeline resource and control tables they were onboarded with;
* ``table_tags.domain`` moved from ``crm`` to the governed value ``Customer 360``, and two
  ``info_type`` values were the case-variant ``EIN number`` that the governed policy rejects;
* both specs must pass BOTH gates -- the Python validator and the JSON schema.
"""

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
ONBOARDING = REPO_ROOT / "BT_Usecase" / "UC3" / "onboarding"
SPEC_PATHS = {
    "streaming_cdc": ONBOARDING / "uc3_excalibur_streaming_cdc.json",
    "batch_recon": ONBOARDING / "uc3_excalibur_batch_recon.json",
}
SCHEMA_PATH = REPO_ROOT / "onboarding_templates" / "onboarding_spec.schema.json"
GOVERNED_TAGS_SQL = ONBOARDING / "uc3_governed_tags.sql"

pytestmark = pytest.mark.skipif(
    not all(p.exists() for p in SPEC_PATHS.values()),
    reason="UC3 specs are a parallel workstream's in-flight work and may be absent from a fresh clone",
)


@pytest.fixture(scope="module", params=sorted(SPEC_PATHS))
def spec(request):
    return json.loads(SPEC_PATHS[request.param].read_text(encoding="utf-8"))


def _flows(spec):
    for key in ("ingestion_flows", "transformation_flows"):
        yield from spec.get(key) or []


def _string_values(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key.startswith("_"):
                continue  # author comments (_about) are prose, not configuration
            yield from _string_values(value)
    elif isinstance(node, list):
        for value in node:
            yield from _string_values(value)
    elif isinstance(node, str):
        yield node


def test_spec_passes_the_framework_validator(spec):
    from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec

    resolved = json.loads(json.dumps(spec).replace("{{catalog}}", "flowx"))
    errors = validate_spec(None, resolved)[-1]
    assert errors == [], errors


def test_spec_passes_the_json_schema(spec):
    jsonschema = pytest.importorskip("jsonschema")

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    errors = list(jsonschema.Draft202012Validator(schema).iter_errors(spec))
    assert errors == [], [f"{list(e.path)}: {e.message}" for e in errors]


def test_no_functional_field_hard_codes_a_catalog(spec):
    """``{{catalog}}`` is a raw-text substitution applied to the whole document before parsing,
    so it is legal in every position -- including reconciliation table names, pipeline
    parameters and the observability volume path. A literal catalog name binds the spec to one
    workspace and, here, to one that no bundle target deploys to."""
    offenders = [v for v in _string_values(spec) if "br_digital_poc" in v or "bt_digital_poc" in v]
    assert offenders == [], offenders


def test_every_table_tag_domain_is_the_governed_customer_360_value(spec):
    domains = {flow["governance_tags"]["table_tags"]["domain"] for flow in _flows(spec)}
    assert domains == {"Customer 360"}, domains


def _governed_tags_sql_without_comments():
    """The SQL file's ``--`` comments legitimately mention rejected spellings (they document the
    defects that were fixed), so only the statements themselves are the allowed-value authority."""
    lines = GOVERNED_TAGS_SQL.read_text(encoding="utf-8").splitlines()
    return "\n".join(line.split("--", 1)[0] for line in lines)


def test_column_tag_values_match_the_governed_policy_spellings(spec):
    """Governed-tag values are case- and space-sensitive; ``SET TAGS`` with an unlisted spelling
    fails the whole group's tagging. The SQL file is the allowed-value authority."""
    sql = _governed_tags_sql_without_comments()
    assert "'EIN Number'" in sql and "'EIN number'" not in sql
    info_types = {
        tag["tags"]["info_type"]
        for flow in _flows(spec)
        for tag in flow.get("governance_tags", {}).get("column_tags", [])
        if "info_type" in tag["tags"]
    }
    assert "EIN number" not in info_types, info_types
    for value in info_types:
        assert f"'{value}'" in sql, f"info_type {value!r} is not an ALLOWED_VALUE in {GOVERNED_TAGS_SQL.name}"


def test_governed_domain_policy_lists_customer_360():
    sql = _governed_tags_sql_without_comments()
    assert "ALLOWED_VALUES ('Customer 360')" in sql


def test_zerobus_flows_do_not_restate_the_capture_technical_metadata_default():
    """On a Delta-table (zerobus) source the four file-provenance columns land NULL; the flag's
    default is already true, so restating it only adds noise. Pinned so the trimmed shape stays,
    and so the three keys the zerobus reader actually needs are always present."""
    spec = json.loads(SPEC_PATHS["streaming_cdc"].read_text(encoding="utf-8"))
    for flow in spec["ingestion_flows"]:
        assert flow["source_type"] == "zerobus"
        assert "capture_technical_metadata" not in flow["source_config"], flow["dataflow_id"]
        for key in ("source_catalog", "source_schema", "source_table"):
            assert flow["source_config"][key], (flow["dataflow_id"], key)
