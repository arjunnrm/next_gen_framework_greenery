"""Offline-safe assertions about UC6's onboarding spec and fixtures.

Deliberately contains NO SQL execution. This repo's `tests/conftest.py` eagerly builds a
`DatabricksSession`, and databricks-connect patches pyspark to refuse a local SparkSession, so
anything needing Spark cannot run offline here -- that is the whole reason `tests/unit`'s
offline baseline sits at ~126 failures. Executing UC6's transformation SQL therefore lives in
`scripts/verify_uc6_business_rules.py` (sqlglot parse + DuckDB execution), which runs
standalone and asserts the four OSAPR status branches, the telephone privacy rule and the age
filter against both fixtures.

What IS asserted here is everything that can be checked without a query engine, and every one
of these pins a mistake actually made during the build:

* the CSS account glob really does exclude the account_address file (they have 56 vs 19 fields);
* the schema_config files line up with the real sample data, positionally;
* thresholds are pipeline parameters, and no `${param}` is wrapped in the author's own quotes;
* the spec passes the framework's own validator AND the JSON schema.
"""

import fnmatch
import gzip
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = REPO_ROOT / "BT_Usecase" / "UC6" / "onboarding" / "uc6_ea_flood_warning.json"
SCHEMA_PATH = REPO_ROOT / "onboarding_templates" / "onboarding_spec.schema.json"
CONFIG_DIR = REPO_ROOT / "BT_Usecase" / "UC6" / "onboarding" / "schema_configs"
SUPPLIED_RAW = REPO_ROOT / "BT_Usecase" / "UC6" / "data" / "sample_bundle"

#: schema_config file -> (sample filename glob, delimiter)
SOURCE_FILES = {
    "uc6_css_account.json": ("CSS_account_[0-9]*.dat.gz", "|"),
    "uc6_css_account_address.json": ("CSS_account_address_*.dat.gz", "|"),
    "uc6_css_subscription.json": ("CSS_subscription_*.dat.gz", "|"),
    "uc6_jt_customer.json": ("CM_JT_Customer_Details_*.dat.gz", "|"),
    "uc6_excalibur_address.json": ("CM_EXCALIBUR_ADDRESS_*.dat.gz", ","),
}


@pytest.fixture(scope="module")
def spec():
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------------------
# The spec must pass BOTH gates -- the Python validator and the JSON schema. Since v1.7.2 the
# schema's additionalProperties:false is what actually rejects unknown keys at onboarding, so
# a spec passing only the validator would still be rejected in production.
# ---------------------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------------------
# The glob trap: CSS_account_* also matches CSS_account_address_*.
# ---------------------------------------------------------------------------------------


def test_css_account_glob_excludes_the_address_file():
    names = ["CSS_account_20250127_00000008.dat.gz", "CSS_account_address_20250127_00000008.dat.gz"]

    naive = [n for n in names if fnmatch.fnmatchcase(n, "CSS_account_*.dat.gz")]
    adopted = [n for n in names if fnmatch.fnmatchcase(n, "CSS_account_[0-9]*.dat.gz")]

    assert len(naive) == 2, "the obvious glob over-matches -- that is the trap this pins"
    assert adopted == ["CSS_account_20250127_00000008.dat.gz"]


def test_spec_uses_the_narrow_css_account_glob(spec):
    flows = {flow["dataflow_id"]: flow for flow in spec["ingestion_flows"]}
    assert flows["df_uc6_css_account_ingest"]["source_config"]["file_pattern"] == "CSS_account_[0-9]*.dat.gz"


def test_every_ingestion_pattern_matches_exactly_one_supplied_file(spec):
    if not SUPPLIED_RAW.exists():
        pytest.skip("supplied sample bundle not present")
    names = [path.name for path in SUPPLIED_RAW.iterdir() if path.is_file()]

    for flow in spec["ingestion_flows"]:
        source_config = flow["source_config"]
        # The EA request is matched by its zip_file_pattern in raw/ (its file_pattern applies
        # to the DECRYPTED name, which only exists after extraction).
        pattern = (
            source_config["source_zip_handling"]["zip_file_pattern"]
            if "source_zip_handling" in source_config
            else source_config["file_pattern"]
        )
        matched = [n for n in names if fnmatch.fnmatchcase(n, pattern)]
        assert len(matched) == 1, f"{flow['dataflow_id']}: {pattern!r} matched {matched}"


# ---------------------------------------------------------------------------------------
# schema_config files are POSITIONAL, so every error they can hold is an off-by-one or a
# duplicate -- both silent. One of each was made and caught during this build.
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("config_name", sorted(SOURCE_FILES))
def test_schema_config_matches_the_sample_file_width(config_name):
    if not SUPPLIED_RAW.exists():
        pytest.skip("supplied sample bundle not present")
    pattern, delimiter = SOURCE_FILES[config_name]
    path = sorted(SUPPLIED_RAW.glob(pattern))[0]
    text = gzip.decompress(path.read_bytes()).decode("utf-8", errors="replace")
    widths = {len(line.split(delimiter)) for line in text.splitlines() if line.strip()}

    columns = json.loads((CONFIG_DIR / config_name).read_text(encoding="utf-8"))["columns"]
    assert len(widths) == 1, f"{config_name}: sample file has ragged rows {sorted(widths)}"
    assert len(columns) == widths.pop(), config_name


@pytest.mark.parametrize("config_name", sorted(SOURCE_FILES))
def test_schema_config_source_names_are_contiguous_and_targets_unique(config_name):
    columns = json.loads((CONFIG_DIR / config_name).read_text(encoding="utf-8"))["columns"]

    assert [c["source_name"] for c in columns] == [f"_c{i}" for i in range(len(columns))]
    targets = [c["target_name"].casefold() for c in columns]
    duplicates = sorted({t for t in targets if targets.count(t) > 1})
    assert not duplicates, f"{config_name}: duplicate target_name(s) {duplicates}"


def test_excalibur_is_configured_comma_delimited(spec):
    """The brief says pipe for every source; this feed has 43 comma-separated fields and no
    pipes at all. Configuring it as pipe yields a one-column table and breaks the path silently."""
    flows = {flow["dataflow_id"]: flow for flow in spec["ingestion_flows"]}
    assert flows["df_uc6_excalibur_address_ingest"]["source_config"]["reader_options"]["delimiter"] == ","

    for dataflow_id, flow in flows.items():
        if dataflow_id != "df_uc6_excalibur_address_ingest":
            assert flow["source_config"]["reader_options"]["delimiter"] == "|", dataflow_id


# ---------------------------------------------------------------------------------------
# Parameters and the quoting trap.
# ---------------------------------------------------------------------------------------


def test_thresholds_are_parameters(spec):
    parameters = spec["pipeline_parameters"]
    assert parameters["match_strength_threshold"] == 50
    assert parameters["min_addresses_per_area"] == 1
    assert parameters["min_age_years"] == 17


def test_no_parameter_placeholder_is_wrapped_in_quotes(spec):
    """`WHERE x = '${p}'` renders as ''US'' and is a parse error -- the substituter supplies
    the quotes. This exact mistake shipped in two of this repo's own fixtures before it was
    caught by a live run."""
    import re

    for flow in spec["transformation_flows"]:
        assert not re.search(r"'\$\{[a-z_]+\}'", flow["transformation_sql"]), flow["flow_step_id"]


def test_every_declared_parameter_is_used(spec):
    """An unused parameter reads as a working control that in fact adjusts nothing."""
    sql = " ".join(flow["transformation_sql"] for flow in spec["transformation_flows"])
    sink_config = json.dumps(spec)
    for name in spec["pipeline_parameters"]:
        assert ("${%s}" % name) in sql or ("${%s}" % name) in sink_config, f"unused parameter: {name}"


def test_age_expression_parenthesises_months_between(spec):
    """Regression pin: sqlglot's months_between expansion mis-parenthesises `x / 12`, which
    silently let an under-17 customer into the telephone list during offline verification.
    The explicit parentheses are semantically identical in Spark and immune to that class of bug."""
    for flow in spec["transformation_flows"]:
        sql = flow["transformation_sql"]
        if "months_between" in sql:
            assert "(months_between(" in sql and ")) / 12" in sql, flow["flow_step_id"]


# ---------------------------------------------------------------------------------------
# Naming, tagging and the read-once mandate.
# ---------------------------------------------------------------------------------------


def test_every_table_carries_the_required_tags(spec):
    required = {"use_case", "source_system", "data_classification", "pii", "owner", "environment"}
    for flow in spec["ingestion_flows"] + spec["transformation_flows"]:
        tags = flow["governance_tags"]["table_tags"]
        assert required <= set(tags), flow.get("dataflow_id") or flow["flow_step_id"]
        assert tags["use_case"] == "uc6"
        assert tags["pii"] in {"true", "false"}


def test_no_table_carries_the_technical_uc6_prefix(spec):
    """2026-09-08 user decision: table names carry BUSINESS context -- the source table's own name
    in bronze (``css_account``, ``ea_request``), the flood-warning subject in silver/gold
    (``flood_warning_osapr``) -- never the technical ``uc6_`` use-case prefix. Flow ids, rule ids
    and input aliases may still carry it; those are identifiers, not table names."""
    for flow in spec["ingestion_flows"] + spec["transformation_flows"]:
        assert not flow["target_table"].startswith("uc6_"), flow["target_table"]
        assert "uc6" not in flow["target_table"], flow["target_table"]


def test_bronze_tables_are_named_after_their_source_tables(spec):
    for flow in spec["ingestion_flows"]:
        assert flow["target_table"] == flow["source_table_name"], (flow["dataflow_id"], flow["target_table"])


# ---------------------------------------------------------------------------------------
# 2026-09-08: the six self-reconciliation presence gates were replaced by ONE COUNT(*) gate MV.
# A per-row dq rule cannot fire on zero rows; a groupBy-less count always yields a row, so the
# gate's `row_count > 0` expectation evaluates even over an empty bronze table. The three flows
# that read bronze directly CROSS JOIN the gate, giving the failure an upstream edge.
# ---------------------------------------------------------------------------------------


def test_no_reconciliation_flows_remain(spec):
    assert not spec.get("reconciliation_flows"), "the recon-based presence gates were removed on 2026-09-08"


def test_presence_gate_counts_every_bronze_source_and_fails_on_zero(spec):
    gate = next(f for f in spec["transformation_flows"] if f["flow_step_id"] == "ts_uc6_source_presence_gate")
    assert gate["target_table"] == "flood_warning_source_presence"
    assert gate["target_type"] == "materialized_view"
    assert gate["target_config"]["cdc_load_strategy"] == "TRUNCATE_AND_LOAD"
    bronze_tables = {f"{{{{catalog}}}}.bronze.{f['target_table']}" for f in spec["ingestion_flows"]}
    assert {si["table"] for si in gate["source_inputs"]} == bronze_tables
    assert all(si["is_streaming"] is False for si in gate["source_inputs"])
    rules = gate["dq_config"]["rules"]
    assert rules == [{"rule_id": "uc6_every_source_present", "expression": "row_count > 0", "action": "fail"}]
    # every source appears in the UNION ALL exactly once, each as a groupBy-less count
    for f in spec["ingestion_flows"]:
        assert gate["transformation_sql"].count(f"'{f['target_table']}'") == 1
    assert gate["transformation_sql"].count("count(*)") == len(spec["ingestion_flows"])
    assert "GROUP BY" not in gate["transformation_sql"].upper()


def test_no_flow_restates_a_framework_default(spec):
    """2026-09-08 simplification: the spec says only what differs from the defaults, so a reader
    sees the decisions, not the boilerplate."""
    for flow in spec["ingestion_flows"] + spec["transformation_flows"]:
        assert "storage_format" not in flow.get("target_config", {}), flow.get("flow_step_id") or flow["dataflow_id"]
        assert flow.get("dq_config") != {"rules": []}, flow.get("flow_step_id") or flow["dataflow_id"]
    for flow in spec["ingestion_flows"]:
        assert "capture_technical_metadata" not in flow["source_config"], flow["dataflow_id"]
        assert "mode" not in flow["source_config"].get("reader_options", {}), flow["dataflow_id"]


def test_input_aliases_describe_their_role_and_never_shadow_a_table(spec):
    tables = {f["target_table"] for f in spec["ingestion_flows"] + spec["transformation_flows"]}
    for flow in spec["transformation_flows"]:
        for si in flow["source_inputs"]:
            alias = si["input_name"]
            assert not alias.startswith("uc6_"), alias
            assert alias not in tables, f"{alias} would collide with a table in the pipeline namespace"
            assert alias.startswith("gate_") or alias.endswith("_in") or "_for_" in alias, alias


def test_age_predicates_keep_months_between_parenthesised_for_the_offline_verifier(spec):
    """`floor((months_between(a, b)) / 12)` and `floor(months_between(a, b) / 12)` are the same
    in Spark, but sqlglot's DuckDB rendering of MONTHS_BETWEEN expands to `a + CASE ... END`
    without parentheses, so the second form binds `/ 12` to the CASE alone and every age comes
    out near 140 -- the offline business-rules check then passes an under-17 customer. Keep the
    explicit parentheses so scripts/verify_uc6_business_rules.py stays faithful."""
    paf = next(f for f in spec["transformation_flows"] if f["flow_step_id"] == "ts_uc6_ee_address_paf")
    sql = paf["transformation_sql"]
    assert sql.count("months_between(") == 2
    assert sql.count("floor((months_between(") == 2, "every age predicate must be floor((months_between(...)) / 12)"


def test_every_flow_that_reads_bronze_directly_depends_on_the_gate(spec):
    gate_table = "{{catalog}}.silver.flood_warning_source_presence"
    for flow in spec["transformation_flows"]:
        if flow["flow_step_id"] == "ts_uc6_source_presence_gate":
            continue
        reads_bronze = any(".bronze." in si["table"] for si in flow["source_inputs"])
        depends_on_gate = any(si["table"] == gate_table for si in flow["source_inputs"])
        assert reads_bronze == depends_on_gate, flow["flow_step_id"]
        if reads_bronze:
            gate_alias = next(si["input_name"] for si in flow["source_inputs"] if si["table"] == gate_table)
            assert f"CROSS JOIN (SELECT count(*) AS present_sources FROM {gate_alias}" in flow["transformation_sql"]
            assert "row_count > 0 HAVING count(*) =" in flow["transformation_sql"]


def test_source_input_view_names_are_globally_unique(spec):
    """Every transformation flow's source_inputs registers a @dlt.view in ONE shared namespace,
    so a name reused across flows is a collision the validator rejects."""
    seen = {}
    for flow in spec["transformation_flows"]:
        for source_input in flow["source_inputs"]:
            name = source_input["input_name"]
            assert name not in seen, f"{name} used by both {seen.get(name)} and {flow['flow_step_id']}"
            seen[name] = flow["flow_step_id"]


def test_no_flow_reads_a_raw_path_outside_its_ingestion_boundary(spec):
    """The Single-Read DAG mandate: downstream flows consume tables, never storage paths."""
    for flow in spec["transformation_flows"]:
        for source_input in flow["source_inputs"]:
            assert not source_input["table"].startswith("/Volumes/"), flow["flow_step_id"]
            assert source_input["table"].count(".") == 2, source_input["table"]
