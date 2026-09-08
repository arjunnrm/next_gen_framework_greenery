"""Unknown attributes must be rejected, never silently ignored (v1.7.1).

A key the framework does not read is not inert: the spec onboards, the control-table row is
written and the pipeline runs, doing something other than what the document says. ``dq_config``
misspelled as ``data_quality`` means data quality never runs at all, with no signal anywhere.
These tests pin that behaviour and -- via ``test_allowed_key_sets_match_json_schema`` -- keep the
validator allowlists and the published JSON schema from drifting apart.
"""

import glob
import json

import pytest

from flowx.lakeflow_framework.onboarding.spec_validator import (
    ALLOWED_DQ_CONFIG_KEYS,
    ALLOWED_GOVERNANCE_TAGS_KEYS,
    ALLOWED_INGESTION_FLOW_KEYS,
    ALLOWED_INGESTION_SOURCE_CONFIG_KEYS,
    ALLOWED_RECONCILIATION_FLOW_KEYS,
    ALLOWED_SOURCE_PLANE_KEYS,
    ALLOWED_ROOT_KEYS,
    ALLOWED_SINK_CONFIG_KEYS,
    ALLOWED_SOURCE_INPUT_KEYS,
    ALLOWED_SOURCE_PLANE_KEYS,
    ALLOWED_TARGET_CONFIG_KEYS,
    ALLOWED_TRANSFORMATION_FLOW_KEYS,
    UNKNOWN_KEY_ALIASES,
    reject_unknown_keys,
    validate_spec,
)

SCHEMA_PATH = "onboarding_templates/onboarding_spec.schema.json"


def _schema():
    with open(SCHEMA_PATH, encoding="utf-8") as handle:
        return json.load(handle)


def _minimal_ingestion_spec(**overrides):
    spec = {
        "dataflow_group_id": "dfg_unit_test",
        "ingestion_flows": [
            {
                "dataflow_id": "df_unit_test",
                "source_type": "autoloader",
                "source_config": {
                    "path": "/Volumes/c/s/landing",
                    "format": "csv",
                    "schema_location": "/Volumes/c/s/_schema",
                },
                "target_catalog": "c",
                "target_schema": "s",
                "target_table": "t",
                "target_type": "streaming_table",
                "target_config": {"cdc_load_strategy": "APPEND"},
            }
        ],
    }
    spec.update(overrides)
    return spec


def _errors(spec):
    *_, errors = validate_spec(None, spec)
    return errors


def _unknown_errors(spec):
    return [e for e in _errors(spec) if "not a recognised attribute" in e]


# --------------------------------------------------------------------------- rejection


def test_baseline_minimal_spec_has_no_unknown_key_errors():
    assert _unknown_errors(_minimal_ingestion_spec()) == []


def test_unknown_key_on_source_config_is_rejected():
    spec = _minimal_ingestion_spec()
    spec["ingestion_flows"][0]["source_config"]["totally_made_up_key"] = 1
    errors = _unknown_errors(spec)
    assert len(errors) == 1
    assert "source_config.totally_made_up_key" in errors[0]


def test_unknown_key_on_target_config_is_rejected():
    spec = _minimal_ingestion_spec()
    spec["ingestion_flows"][0]["target_config"]["another_fake"] = True
    assert any("target_config.another_fake" in e for e in _unknown_errors(spec))


def test_unknown_key_on_the_flow_itself_is_rejected():
    spec = _minimal_ingestion_spec()
    spec["ingestion_flows"][0]["data_quality"] = {"rules": []}
    errors = _unknown_errors(spec)
    assert any("data_quality" in e and "Use dq_config." in e for e in errors)


def test_unknown_key_at_spec_root_is_rejected_without_a_dotted_prefix():
    spec = _minimal_ingestion_spec(catalog_name="{{catalog}}")
    errors = _unknown_errors(spec)
    assert any(e.startswith("catalog_name:") for e in errors)


def test_rejection_triggers_on_presence_not_truthiness():
    """A false-valued key is still a statement about a feature that does not exist."""
    spec = _minimal_ingestion_spec()
    spec["ingestion_flows"][0]["source_config"]["infer_schema"] = False
    assert any("infer_schema" in e for e in _unknown_errors(spec))


def test_v1_cdc_config_block_is_rejected_and_names_its_replacement():
    spec = _minimal_ingestion_spec()
    spec["ingestion_flows"][0]["cdc_config"] = {"keys": ["id"]}
    errors = _unknown_errors(spec)
    assert any("cdc_load_strategy" in e for e in errors), errors


# --------------------------------------------------------------------------- exemptions


@pytest.mark.parametrize("comment_key", ["_scenario", "_provenance", "_test_case_note", "_x"])
def test_underscore_prefixed_author_comments_are_allowed(comment_key):
    spec = _minimal_ingestion_spec(**{comment_key: "a note"})
    spec["ingestion_flows"][0][comment_key] = "a note"
    spec["ingestion_flows"][0]["source_config"][comment_key] = "a note"
    assert _unknown_errors(spec) == []


def test_schema_editor_hint_is_allowed():
    spec = _minimal_ingestion_spec()
    spec["$schema"] = "./onboarding_spec.schema.json"
    assert _unknown_errors(spec) == []


# --------------------------------------------------------------------------- messages


def test_near_miss_gets_a_did_you_mean_suggestion():
    errors = []
    reject_unknown_keys({"targe_table": 1}, "flow", errors, {"target_table", "target_schema"})
    assert "Did you mean 'target_table'?" in errors[0]


def test_unrecognisable_key_lists_the_allowed_keys():
    errors = []
    reject_unknown_keys({"zzzzz": 1}, "flow", errors, {"target_table"})
    assert "Allowed keys here: target_table." in errors[0]


@pytest.mark.parametrize("wrong,expected", sorted(UNKNOWN_KEY_ALIASES.items()))
def test_every_alias_produces_its_mapped_guidance(wrong, expected):
    errors = []
    reject_unknown_keys({wrong: 1}, "flow", errors, {"unrelated"})
    assert len(errors) == 1
    assert expected.split(" --")[0].strip(" .") in errors[0]


def test_an_alias_name_is_accepted_where_it_is_genuinely_a_real_key():
    """Aliases are keyed on bare names, but some are real keys in a different container.

    ``file_format`` is wrong under ``source_config`` and correct under
    ``observability[].destination_config``. The allowlist must win over the alias table.
    """
    errors = []
    reject_unknown_keys(
        {"file_format": "JSONL"},
        "observability[0].destination_config",
        errors,
        {"file_format", "volume_path"},
    )
    assert errors == []


def test_observability_template_block_is_not_flagged_by_the_full_validator():
    """The shipped template carries a legitimate destination_config.file_format."""
    with open("onboarding_templates/pipeline_onboarding_template.json", encoding="utf-8") as handle:
        spec = json.load(handle)
    assert any("file_format" in json.dumps(d) for d in spec.get("observability", [])), (
        "template no longer exercises destination_config.file_format; keep a case that does"
    )
    assert _unknown_errors(spec) == []


def test_non_dict_input_is_a_no_op():
    errors = []
    for value in (None, [], "text", 3):
        reject_unknown_keys(value, "flow", errors, {"a"})
    assert errors == []


# --------------------------------------------------------------------------- anti-drift


def test_allowed_key_sets_match_json_schema():
    """The validator allowlists and the published JSON schema are one contract.

    Adding an attribute to one and not the other is the drift this change exists to prevent,
    so it must fail here rather than at pipeline runtime.
    """
    defs = _schema()["$defs"]
    expected = {
        "ingestionFlow": ALLOWED_INGESTION_FLOW_KEYS,
        "ingestionSourceConfig": ALLOWED_INGESTION_SOURCE_CONFIG_KEYS,
        "targetConfig": ALLOWED_TARGET_CONFIG_KEYS,
        "dqConfig": ALLOWED_DQ_CONFIG_KEYS,
        "governanceTags": ALLOWED_GOVERNANCE_TAGS_KEYS,
        "sourceInput": ALLOWED_SOURCE_INPUT_KEYS,
        "sinkConfig": ALLOWED_SINK_CONFIG_KEYS,
        "reconciliationFlow": ALLOWED_RECONCILIATION_FLOW_KEYS,
        # v1.7.3: source_plane gained its first validator (_validate_source_plane), so its
        # allowlist is pinned to the schema like every other one. Before that release the block
        # was in ALLOWED_ROOT_KEYS and then never inspected again -- accepted verbatim, typos
        # and all.
        "sourcePlane": ALLOWED_SOURCE_PLANE_KEYS,
    }
    for def_name, allowed in expected.items():
        assert set(defs[def_name]["properties"]) == allowed, def_name

    assert set(_schema()["properties"]) - {"$schema"} == ALLOWED_ROOT_KEYS
    # transformationFlow additionally accepts source_description, a real control-table column
    # (metadata_upsert.py) that the schema does not currently declare for this flow type.
    assert set(defs["transformationFlow"]["properties"]) <= ALLOWED_TRANSFORMATION_FLOW_KEYS


def test_locked_containers_still_permit_underscore_comments():
    defs = _schema()["$defs"]
    for name in ("ingestionFlow", "ingestionSourceConfig", "targetConfig"):
        assert defs[name]["additionalProperties"] is False
        assert "^_" in defs[name]["patternProperties"]


def test_every_shipped_spec_still_validates_clean_of_unknown_keys():
    """No spec in the repo may regress -- these are the framework worked examples."""
    # 0.0.4 moved the corpus: the live use-case specs now live under BT_Usecase/<UC>/onboarding/
    # (schema_configs/ below them are column maps, not specs, hence the non-recursive glob) and
    # the former flowx_testing/ worked examples under archive/old_json/. Both are still shipped,
    # so both stay under this guard -- the BT_Usecase ones are the specs that actually deploy.
    paths = sorted(
        set(glob.glob("flowx_testing/*.json"))
        | set(glob.glob("BT_Usecase/*/onboarding/*.json"))
        | set(glob.glob("archive/old_json/*.json"))
        | set(glob.glob("resources/**/*.json", recursive=True))
        | {
            "onboarding_templates/pipeline_onboarding_template.json",
            "databricks-app/templates/pipeline_onboarding_template.json",
        }
    )
    checked = 0
    for path in paths:
        try:
            with open(path, encoding="utf-8") as handle:
                spec = json.load(handle)
        except (OSError, ValueError):
            continue
        if not isinstance(spec, dict) or "dataflow_group_id" not in spec:
            continue
        if not any(k in spec for k in ("ingestion_flows", "transformation_flows", "reconciliation_flows")):
            continue
        checked += 1
        assert _unknown_errors(spec) == [], path
    assert checked >= 50, f"expected the shipped spec corpus, only found {checked}"


# --------------------------------------------------------------- source_plane (v1.7.3)
#
# The Single-Read architectural mandate. Until v1.7.3 `source_plane` sat in ALLOWED_ROOT_KEYS
# with NO validator behind it: any shape, any key, any `materialize` value was accepted, written
# to dataflow_group_spec.source_plane_config_json, and only interpreted at pipeline runtime --
# where an unrecognised value silently behaved as the old "auto" default. These tests pin the
# validator that closed that gap.

MATERIALIZE_NEVER_MESSAGE = (
    "materialize='never' is deprecated and prohibited under the Single-Read architectural "
    "mandate. Remove this setting to default to 'always', ensuring base tables are read once "
    "and reused via dlt.read()."
)


def _source_plane_errors(source_plane):
    spec = _minimal_ingestion_spec(source_plane=source_plane)
    return [e for e in _errors(spec) if e.startswith("source_plane")]


def test_materialize_never_is_rejected_with_the_migration_message():
    """The prohibited value is rejected by NAME, with the migration sentence naming 'always'.

    Asserted as an exact string, not a substring: this message is the machine-readable contract
    the v1.7.3 attribute delta publishes for the Databricks App and other automated consumers,
    so a reworded sentence is a breaking change to them even when it still reads correctly.
    """
    errors = _source_plane_errors({"materialize": "never"})
    assert errors == [f"source_plane.materialize: {MATERIALIZE_NEVER_MESSAGE}"]


def test_materialize_never_is_rejected_regardless_of_other_keys():
    """Presence of the prohibited VALUE is the trigger -- a fully-populated, otherwise-valid
    block does not launder it."""
    errors = _source_plane_errors(
        {"materialize": "never", "catalog": "metaflow", "schema": "plane"}
    )
    assert errors == [f"source_plane.materialize: {MATERIALIZE_NEVER_MESSAGE}"]


def test_materialize_never_does_not_fall_through_to_the_generic_enum_error():
    """A withdrawn value gets its migration message INSTEAD of 'not one of [always, auto]'.

    The generic enum error reads as a typo and tells the author nothing about what replaced the
    policy or why it went away -- the same reason cdc_load_strategy checks
    REMOVED_CDC_LOAD_STRATEGIES before its allowed-values test.
    """
    errors = _source_plane_errors({"materialize": "never"})
    assert len(errors) == 1
    assert "not one of" not in errors[0]


@pytest.mark.parametrize("policy", ["always", "auto"])
def test_surviving_materialize_policies_are_accepted(policy):
    """`materialize` is a surviving key, so the rejection must be VALUE-scoped.

    This is why reject_removed_keys() -- which fires on key PRESENCE -- is the wrong helper
    here: pointed at `materialize` it would reject the default `"always"` too. `"auto"` stays
    legal because it is weaker, not prohibited: groups onboarded before the mandate set it
    explicitly, and breaking them buys nothing that the new default does not already deliver.
    """
    assert _source_plane_errors({"materialize": policy}) == []


def test_omitted_materialize_is_accepted_and_defaults_to_always():
    """Silence is legal and means "always" -- the default lives in the engine signature and the
    notebook fallback, not in a validator-injected value."""
    assert _source_plane_errors({"catalog": "metaflow", "schema": "plane"}) == []
    assert _source_plane_errors({}) == []


def test_unrecognised_materialize_value_is_rejected():
    """Before v1.7.3 an unknown value was accepted and silently behaved as "auto" -- the exact
    class of silent-divergence defect the validator exists to catch."""
    errors = _source_plane_errors({"materialize": "sometimes"})
    assert len(errors) == 1
    assert errors[0].startswith("source_plane.materialize:")


def test_unknown_key_on_source_plane_is_rejected():
    errors = _source_plane_errors({"materialize": "always", "materialise": "always"})
    assert len(errors) == 1
    assert "not a recognised attribute" in errors[0]
    assert errors[0].startswith("source_plane.materialise:")


def test_non_dict_source_plane_is_rejected():
    assert _source_plane_errors(["always"]) != []
