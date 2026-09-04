"""The app and the framework must agree about what a valid spec is.

Every other test in this directory checks the app against *itself* — its own registry, its own
rules.json, its own serializer. That leaves the one gap that actually hurts an author: the
builder offering a field, or shipping a template, that `onboarding/spec_validator.py` rejects.
The spec is only ever judged by the framework, so these tests judge the app by the framework
too — they import the real `validate_spec` and run the app's own output through it.

Three things are pinned here:

1. **Every shipped template validates.** A template is a starting point an author is invited to
   save as-is; one that cannot onboard is worse than no template. This caught
   `reconciliation/blank.json` shipping the builder's *decomposed* target keys
   (target_catalog/target_schema/target_table) instead of the canonical three-part `table` the
   framework reads — a shape the two sibling reconciliation templates already used.

2. **Field applicability matches.** For each (strategy x optional attribute) pair, a field the
   builder shows must be a field onboarding accepts. This caught `cdc_operation_column` being
   offered on SCD3, which `strategies_supporting_delete_marker` rejects outright.

3. **No malformed predicate survives in the registry.** A binary operator given one argument
   falls through `evaluate_predicate`'s `len(args) >= 2` guard and silently returns False, so
   the field it guards is invisible for *every* value. That is how the v1.4.0 removal of the
   `FULL_SNAPSHOT_CDC_NO_PK` guard left `{"ne": ["target_config.cdc_load_strategy"]}` behind and
   hid the delete-marker fields entirely.

`spark` is stubbed: SQL planning needs a live session (no Java on a dev box), and the
validator's own contract is that an unresolved reference is tolerated pre-deployment. The stub
returns exactly the plan text this workspace's serverless environment returns for one, so the
SQL path is exercised and only *attribute* problems can fail these tests.
"""

import json
import re
import sys
from pathlib import Path

import pytest

APP_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = APP_ROOT.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from flowx.lakeflow_framework.onboarding import (  # noqa: E402
    spec_validator as sv,
)

TEMPLATES = APP_ROOT / "templates"
REGISTRY = APP_ROOT / "config" / "registry"

# {{catalog}}/{{env}} resolve before the spec is ever parsed, so substitute them the way the
# app does rather than validating placeholder text.
TEMPLATE_VARS = {"catalog": "flowx", "env": "dev"}
_VAR = re.compile(r"\{\{\s*(\w+)\s*\}\}")

FLOW_ARRAY = {
    "ingestion": "ingestion_flows",
    "transformation": "transformation_flows",
    "reconciliation": "reconciliation_flows",
    "observability": "observability",
}


class _StubRow(dict):
    def asDict(self):
        return dict(self)


class _StubDataFrame:
    def collect(self):
        return [_StubRow(plan="Error occurred during query planning: [TABLE_OR_VIEW_NOT_FOUND]")]


class _StubSpark:
    """Returns the plan text serverless returns for a not-yet-deployed view reference.

    `_validate_sql_syntax` tolerates that (only NUM_COLUMNS_MISMATCH / INCOMPATIBLE_COLUMN_TYPE
    are hard failures), which is correct here: a template's `transformation_sql` legitimately
    names `source_inputs[]` views that exist only once the pipeline runs.
    """

    def sql(self, _query):
        return _StubDataFrame()


def _substitute(node):
    if isinstance(node, str):
        return _VAR.sub(lambda m: str(TEMPLATE_VARS.get(m.group(1), m.group(1))), node)
    if isinstance(node, list):
        return [_substitute(x) for x in node]
    if isinstance(node, dict):
        return {k: _substitute(v) for k, v in node.items()}
    return node


def _canonical_pipeline_parameters():
    """The whole-spec template's `pipeline_parameters`.

    Flow-scoped templates legitimately reference `${min_amount}`-style parameters that are
    declared once at spec root; validating a fragment in isolation would report them as
    undefined, which is an artefact of the fragment, not a defect in it.
    """
    with open(TEMPLATES / "pipeline_onboarding_template.json", encoding="utf-8") as f:
        return json.load(f).get("pipeline_parameters", {})


def _as_spec(scope, body):
    if scope == "spec":
        return body
    return {
        "dataflow_group_id": "dfg_template_validation",
        "pipeline_parameters": _canonical_pipeline_parameters(),
        FLOW_ARRAY[scope]: [body],
    }


def _validate(spec):
    *_, errors = sv.validate_spec(_StubSpark(), spec)
    return errors


def _shipped_templates():
    for path in sorted(TEMPLATES.rglob("*.json")):
        if path.name == "index.json":
            continue
        rel = path.relative_to(TEMPLATES).as_posix()
        scope = "spec" if "/" not in rel else rel.split("/")[0]
        yield pytest.param(rel, scope, id=rel)


@pytest.mark.parametrize("rel,scope", _shipped_templates())
def test_every_shipped_template_passes_framework_validation(rel, scope):
    """A template an author can save as-is must be a spec onboarding accepts."""
    with open(TEMPLATES / rel, encoding="utf-8") as f:
        body = _substitute(json.load(f))
    errors = _validate(_as_spec(scope, body))
    assert not errors, "templates/%s is rejected by the framework validator:\n  %s" % (
        rel,
        "\n  ".join(errors),
    )


def test_app_and_framework_template_copies_are_identical():
    """The app ships its own copy of the canonical template; a divergence is a silent fork.

    They drifted once already: the framework's copy pointed `transform_sql` at a view named
    `missing_records`, which nothing creates -- the reconciliation appender registers the miss
    set as `_reconciliation_unmatched_records` (appender.py::UNMATCHED_RECORDS_VIEW_NAME).
    """
    with open(TEMPLATES / "pipeline_onboarding_template.json", encoding="utf-8") as f:
        app_copy = json.load(f)
    framework_path = REPO_ROOT / "onboarding_templates" / "pipeline_onboarding_template.json"
    with open(framework_path, encoding="utf-8") as f:
        framework_copy = json.load(f)
    assert app_copy == framework_copy, (
        "databricks-app/templates/pipeline_onboarding_template.json has diverged from "
        "onboarding_templates/pipeline_onboarding_template.json"
    )


# --- field applicability -------------------------------------------------------------------
#
# What the builder SHOWS, transcribed from the `w:` predicates in web/src/Builder.jsx. Keep in
# step with that file: a mismatch here is the bug this test exists to catch.
STRATEGIES = ["APPEND", "TRUNCATE_AND_LOAD", "SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"]
_ALL_CDC = {"SCD1", "SCD2", "SCD3", "FULL_SNAPSHOT_CDC"}

APP_SHOWS = {
    "primary_keys": _ALL_CDC,
    "sequence_by_column": _ALL_CDC,
    "columns_to_check": {"SCD1", "SCD2", "SCD3"},
    "columns_to_exclude": {"SCD1", "SCD2", "SCD3"},
    "cdc_operation_column": {"SCD1", "SCD2", "FULL_SNAPSHOT_CDC"},
    "generate_hash_columns": _ALL_CDC,
    "auto_ttl": {"APPEND", "TRUNCATE_AND_LOAD"},
}

PROBE = {
    "primary_keys": {"primary_keys": ["id"]},
    "sequence_by_column": {"sequence_by_column": "updated_at"},
    "columns_to_check": {"columns_to_check": ["amount"]},
    "columns_to_exclude": {"columns_to_exclude": ["batch_ts"]},
    "cdc_operation_column": {
        "cdc_operation_column": "op",
        "cdc_operation_mapping": {"delete_values": ["D"]},
    },
    "generate_hash_columns": {"generate_hash_columns": True},
    "auto_ttl": {"auto_ttl": {"timestamp_column": "updated_at", "expire_in_days": 90}},
}


def _probe_flow(strategy, extra):
    target_config = {"cdc_load_strategy": strategy}
    # Supply what the strategy itself requires, so a probe reports on the attribute under test
    # rather than re-reporting a missing key.
    if strategy in _ALL_CDC:
        target_config["primary_keys"] = ["id"]
    if strategy == "SCD3":
        target_config["columns_to_check"] = ["status"]
    target_config.update(extra)
    return {
        "flow_step_id": "ts_applicability_probe",
        "dataflow_id": "df_applicability_probe",
        "target_catalog": "c",
        "target_schema": "s",
        "target_table": "t",
        "target_type": "streaming_table",
        "source_inputs": [{"input_name": "src", "table": "c.s.raw", "is_streaming": False}],
        "transformation_sql": "SELECT id FROM src",
        "target_config": target_config,
    }


@pytest.mark.parametrize("attribute", sorted(APP_SHOWS))
@pytest.mark.parametrize("strategy", STRATEGIES)
def test_builder_never_offers_a_field_onboarding_rejects(attribute, strategy):
    """The builder may narrow the framework, but it must never widen it.

    Hiding a field the framework merely tolerates is a deliberate UX choice (partition_columns
    on a CDC strategy is accepted and then silently ignored at runtime, so the builder hides
    it). Showing a field the framework *rejects* is always a defect: the author fills it in,
    saves, and onboarding refuses the spec.
    """
    if strategy not in APP_SHOWS[attribute]:
        pytest.skip("builder hides %s on %s" % (attribute, strategy))
    spec = {
        "dataflow_group_id": "dfg_probe",
        "transformation_flows": [_probe_flow(strategy, PROBE[attribute])],
    }
    relevant = [e for e in _validate(spec) if attribute in e]
    assert not relevant, (
        "the builder offers target_config.%s on %s, but onboarding rejects it:\n  %s"
        % (attribute, strategy, "\n  ".join(relevant))
    )


# --- registry predicate hygiene ------------------------------------------------------------
BINARY_OPERATORS = {"eq", "ne", "in", "nin", "gt", "gte", "lt", "lte", "matches"}


def _walk_predicates(node, where, found):
    if isinstance(node, dict):
        for key, value in node.items():
            if key in BINARY_OPERATORS and isinstance(value, list) and len(value) < 2:
                found.append((where, key, value))
            _walk_predicates(value, where, found)
    elif isinstance(node, list):
        for item in node:
            _walk_predicates(item, where, found)


def test_no_binary_predicate_is_missing_its_operand():
    """A one-argument binary operator is always-False, not always-True.

    `evaluate_predicate` reaches its comparison operators behind `len(args) >= 2` and falls
    through to `return False` otherwise, so a `visible_when` carrying one hides its field for
    every value of every other field -- silently, with no error anywhere. Deleting the operand
    of a guard (as the FULL_SNAPSHOT_CDC_NO_PK removal did) and leaving the operator behind is
    the way this happens.
    """
    offenders = []
    for path in sorted(REGISTRY.glob("*.json")):
        with open(path, encoding="utf-8") as f:
            _walk_predicates(json.load(f), path.name, offenders)
    assert not offenders, "malformed predicates (a binary operator with <2 operands):\n  " + (
        "\n  ".join("%s: {%r: %r}" % o for o in offenders)
    )
