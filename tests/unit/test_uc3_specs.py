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


# ---------------------------------------------------------------------------------------------
# The batch lane (v0.0.7) -- the Lakeflow Connect Oracle query-based connector
#
# Every assertion below pins a mistake that was actually made, most of them caught only at
# pipeline runtime after validating clean through BOTH spec gates.
# ---------------------------------------------------------------------------------------------

#: dataflow_id -> (UC table, business column count)
_UC3_BATCH_FLOWS = {
    "df_uc3_physical_device_batch_load": ("physical_device", 31),
    "df_uc3_customer_batch_load": ("customer", 90),
    "df_uc3_subscriber_batch_load": ("subscriber", 131),
}
#: reconciliation_id -> the bronze twin's SCD type, which decides whether a filter is required
_UC3_RECON_FLOWS = {
    "rf_uc3_physical_device_batch_vs_bronze": 1,
    "rf_uc3_customer_batch_vs_bronze": 2,
    "rf_uc3_subscriber_batch_vs_bronze": 1,
}
HEAL_LANDING_TABLE = "{{catalog}}.staging.oracle_excalibur_cdc"
LANDING_TABLE_COLUMNS = [
    "destination", "target_table", "key", "value", "operation",
    "source_position", "idempotency_key", "partition", "headers",
]


def _batch_spec():
    return json.loads(SPEC_PATHS["batch_recon"].read_text(encoding="utf-8"))


def test_batch_flows_are_transformation_flows_not_ingestion_flows():
    """An ingestion flow cannot read these tables AT ALL. source_plane's
    _requests_from_ingestion_rows plans every ingestion source request want_stream=True
    unconditionally ("there is no batch ingestion reader to fall back to") and _execute_reader
    hard-rejects a batch bind of an ingestion identity, so the source_type zerobus draft of this
    lane failed on every update with "Failed to resolve flow: '_<table>_batch_staged'"."""
    spec = _batch_spec()
    assert not spec.get("ingestion_flows"), spec.get("ingestion_flows")
    flows = spec["transformation_flows"]
    assert len(flows) == 3, [f["dataflow_id"] for f in flows]
    assert sorted(f["dataflow_id"] for f in flows) == sorted(_UC3_BATCH_FLOWS)


def test_every_batch_flow_reads_its_lakeflow_connect_table_as_a_batch_input():
    """The Lakeflow Connect destinations are MERGE-written (scd_type SCD_TYPE_1, confirmed by
    MERGE at v2 in their Delta history). Delta refuses a streaming read of a MERGE-written table
    (DELTA_SOURCE_TABLE_IGNORE_CHANGES) and this framework refuses skipChangeCommits, so
    is_streaming must be the BOOLEAN False. The string "false" is truthy in Python and would plan
    exactly the streaming read that fails."""
    for flow in _batch_spec()["transformation_flows"]:
        table, _ = _UC3_BATCH_FLOWS[flow["dataflow_id"]]
        inputs = flow["source_inputs"]
        assert len(inputs) == 1, flow["dataflow_id"]
        source = inputs[0]
        assert source["table"] == "{{catalog}}.oracle_excalibur_batch." + table, flow["dataflow_id"]
        assert source["is_streaming"] is False, (flow["dataflow_id"], repr(source["is_streaming"]))


def test_batch_targets_are_full_snapshot_materialized_views():
    """A full recompute per update, not a CDC collapse: the connector table already holds exactly
    one row per key (live: 30/77/55, distinct-PK == rows), so apply_changes would be redundant.
    Declaring primary_keys or sequence_by_column, or target_type streaming_table, would each force
    the streaming read ruled out above (flow_generators derives is_streaming from target_type)."""
    for flow in _batch_spec()["transformation_flows"]:
        assert flow["target_type"] == "materialized_view", flow["dataflow_id"]
        target_config = flow["target_config"]
        assert target_config["cdc_load_strategy"] == "TRUNCATE_AND_LOAD", flow["dataflow_id"]
        assert "primary_keys" not in target_config, flow["dataflow_id"]
        assert "sequence_by_column" not in target_config, flow["dataflow_id"]


def test_no_batch_flow_sequences_by_a_per_query_constant():
    """An earlier draft sequenced CDC by a struct whose tiebreaker was current_timestamp(). Spark
    evaluates it ONCE PER QUERY, so it is constant across every row of a micro-batch and breaks no
    tie whatsoever. transform_sql legitimately calls it once, in heal_params, where a single
    per-run heal stamp is exactly what is wanted; that is not a row ordering."""
    spec = _batch_spec()
    for flow in spec["transformation_flows"]:
        assert "current_timestamp()" not in flow["transformation_sql"].lower(), flow["dataflow_id"]
        assert "sequence_by_column" not in flow["target_config"], flow["dataflow_id"]
    assert "data_standardization_sql" not in json.dumps(spec)


def test_batch_columns_are_cast_to_the_bronze_side_types():
    """cdc/hashing.py normalises every compare column with trim(lower(cast(col AS STRING))), so
    the batch lane must cast to the SAME types the streaming lane writes to bronze. A
    decimal(9,0) 100002 and a double 100002.0 render as different strings and hash differently,
    which would report every row as VALUE_DRIFT while both lanes were in fact identical."""
    import re

    cast = re.compile(r"CAST\((?:t\.`[A-Z0-9_]+`|NULL) AS (?:DOUBLE|TIMESTAMP|STRING)\)")
    for flow in _batch_spec()["transformation_flows"]:
        _, expected_columns = _UC3_BATCH_FLOWS[flow["dataflow_id"]]
        sql = flow["transformation_sql"]
        upper = sql.upper()
        for banned in ("DECIMAL(", "CHAR(", "VARCHAR("):
            assert banned not in upper, (flow["dataflow_id"], banned)
        assert len(cast.findall(sql)) == expected_columns, flow["dataflow_id"]


def test_the_scd2_reconciliation_target_filters_to_current_rows_only():
    """bronze.customer is written SCD2 and holds EVERY historical version of a key (live: 40 rows,
    30 current). reconciliation/matcher.py collapses duplicate keys with MATCHED beating
    VALUE_DRIFT beating MISSING -- "a key counts as matched as soon as *any* target row matches"
    -- so an unfiltered SCD2 target lets a stale CLOSED version silently mask real drift on the
    current row, and that drives live heal writes. The SCD1 targets must NOT carry the filter:
    they have no __END_AT column to reference."""
    for flow in _batch_spec()["reconciliation_flows"]:
        scd_type = _UC3_RECON_FLOWS[flow["reconciliation_id"]]
        target = flow["target_configs"][0]
        if scd_type == 2:
            assert target.get("filter_condition") == "__END_AT IS NULL", flow["reconciliation_id"]
        else:
            assert "filter_condition" not in target, flow["reconciliation_id"]


def test_every_reconciliation_heals_into_the_debezium_landing_table():
    """The heal must re-enter through the SAME CDC engine as a real change, so it appends to the
    multiplexed landing table rather than writing bronze directly, and never to the obsolete
    pre-split staging.<table>_stream tables.

    execution_mode is pipeline_audit_only, so the L5 heal lane is NOT a node in the pipeline
    graph and healing runs as the three 05_reconciliation_engine.py job tasks in
    005_lfj_uc3_excalibur_batch_recon. That is forced by the source, not preferred: "pipeline" is
    the only mode that registers the heal lane, and it STREAMS its source. The pre-v0.0.7 CSV lane
    could use it because Auto Loader is a streaming reader feeding an APPEND streaming table. The
    connector table cannot be streamed (MERGE-written), the snapshot MV cannot (TRUNCATE_AND_LOAD
    is in G-STREAM's rejection set), and an APPEND copy of the MV cannot bridge the two, because
    target_type streaming_table forces is_streaming=True on the staged view while that view's own
    input must stay batch -- proven at runtime as "View '_<table>_batch_events_staged' is not a
    streaming view and must be referenced using read". Do not retry that shape."""
    spec = _batch_spec()
    assert sorted(f["reconciliation_id"] for f in spec["reconciliation_flows"]) == sorted(_UC3_RECON_FLOWS)
    for flow in spec["reconciliation_flows"]:
        assert flow["execution_mode"] == "pipeline_audit_only", flow["reconciliation_id"]
        assert flow["target_configs"][0]["append_target_table"] == HEAL_LANDING_TABLE, flow["reconciliation_id"]
    stale = [v for v in _string_values(spec) if v.startswith("{{catalog}}.staging.") and v.endswith("_stream")]
    assert not stale, stale


def test_every_transform_sql_emits_exactly_the_landing_table_columns():
    """The heal append runs with mergeSchema=true, so a misspelt alias silently ADDS a column to
    the landing table instead of failing. Neither spec gate catches it: _validate_sql_syntax
    returns immediately when spark is None, which is how both gates and these tests call it."""
    sqlglot = pytest.importorskip("sqlglot")
    for flow in _batch_spec()["reconciliation_flows"]:
        tree = sqlglot.parse_one(flow["transform_sql"], read="databricks")
        aliases = [e.alias_or_name for e in tree.expressions]
        assert aliases == LANDING_TABLE_COLUMNS, (flow["reconciliation_id"], aliases)


def test_every_transform_sql_preserves_oracle_uppercase_field_names():
    """from_json field matching is CASE-SENSITIVE and Debezium emits Oracle's UPPERCASE names. A
    lowercase envelope parses every payload column to NULL, primary keys included, while row
    counts, op codes and delete flags all still look perfect."""
    for flow in _batch_spec()["reconciliation_flows"]:
        sql = flow["transform_sql"]
        assert "'CUSTOMER_ID'" in sql, flow["reconciliation_id"]
        assert "'customer_id'" not in sql, flow["reconciliation_id"]
