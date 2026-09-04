"""Schema/Row agreement regression guard for ``onboarding/metadata_upsert.py``.

Two attributes rotted in exactly the same way and neither was caught by a test:

* ``two_tier_verification`` -- present in the reconciliation control-table DDL and in
  ``_RECONCILIATION_FLOW_SPEC_SCHEMA``, validated by ``spec_validator.py``, documented in the
  onboarding spec -- and never set by the ``Row(...)`` literal in
  ``upsert_reconciliation_flow_spec``. An onboarded ``two_tier_verification: false`` therefore
  never reached the control table, and the engine happily read back the column default.
* ``spark_config_json`` -- the group-level twin of the same defect in
  ``upsert_dataflow_group_spec``.

A column can drift out of agreement in either direction, and both directions are silent:

1. the ``StructType`` declares a column the ``Row`` literal never populates -- the value the
   operator onboarded is dropped on the floor; or
2. the ``Row`` literal sets a keyword the ``StructType`` does not declare -- ``createDataFrame``
   raises only at onboarding time, on a live cluster.

So for every flow kind this module upserts, this test asserts that each attribute the validator
knows about maps to a column that appears BOTH in the flow kind's ``StructType`` AND among the
``Row(...)`` keywords of its upsert function.

The mapping (spec key -> control-table column) is hand-maintained -- deliberately, because that
is the artefact a reviewer reads to answer "is this attribute actually persisted?". It is kept
honest by ``test_mapping_is_complete_against_validator_constants``, which asserts it against
``spec_validator``'s own constant lists: every key the validator conditionally requires must be
mapped, and no key the validator *rejects* may be.

Pure static analysis: the ``StructType`` field names and the ``Row(...)`` keyword names are read
out of the module's source with ``ast``, so nothing here imports pyspark or delta.
"""

import ast
import pathlib

import pytest

from flowx.lakeflow_framework.onboarding import spec_validator

# ---------------------------------------------------------------------------
# Source loading (ast only -- importing metadata_upsert would pull in pyspark/delta)
# ---------------------------------------------------------------------------

_MODULE_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "src"
    / "flowx"
    / "lakeflow_framework"
    / "onboarding"
    / "metadata_upsert.py"
)


def _module_tree() -> ast.Module:
    assert _MODULE_PATH.is_file(), f"metadata_upsert.py not found at {_MODULE_PATH}"
    return ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))


_TREE = _module_tree()


def _struct_field_names(schema_constant: str) -> list:
    """Names passed as the first positional arg of every ``StructField(...)`` in the
    module-level assignment to ``schema_constant``."""
    for node in _TREE.body:
        targets = node.targets if isinstance(node, ast.Assign) else []
        if not any(isinstance(t, ast.Name) and t.id == schema_constant for t in targets):
            continue
        names = []
        for call in ast.walk(node.value):
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            if isinstance(func, ast.Name) and func.id == "StructField" and call.args:
                first = call.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    names.append(first.value)
        assert names, f"{schema_constant} declares no StructField columns"
        return names
    raise AssertionError(f"{schema_constant} is not assigned at module level in metadata_upsert.py")


def _row_keyword_names(function_name: str) -> set:
    """Keyword argument names of every ``Row(...)`` call inside ``function_name``."""
    for node in ast.walk(_TREE):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name != function_name:
            continue
        names = set()
        for call in ast.walk(node):
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "Row":
                names.update(kw.arg for kw in call.keywords if kw.arg is not None)
        assert names, f"{function_name} contains no Row(...) keyword arguments"
        return names
    raise AssertionError(f"{function_name} is not defined in metadata_upsert.py")


# ---------------------------------------------------------------------------
# Hand-maintained spec-key -> control-table-column mapping, per flow kind
# ---------------------------------------------------------------------------

# spec key at the dataflow-group level -> dataflow_group_spec column
DATAFLOW_GROUP_ATTRIBUTES = {
    "dataflow_group_id": "dataflow_group_id",
    "pipeline_parameters": "pipeline_parameters_json",
    "spark_config": "spark_config_json",
}

# spec key on an ingestion_flows[] entry -> ingestion_flow_spec column
INGESTION_FLOW_ATTRIBUTES = {
    "dataflow_id": "dataflow_id",
    "source_system": "source_system",
    "source_database": "source_database",
    "source_table_name": "source_table_name",
    "source_description": "source_description",
    "source_type": "source_type",
    "target_catalog": "target_catalog",
    "target_schema": "target_schema",
    "target_table": "target_table",
    "target_type": "target_type",
    "source_config": "source_config_json",
    "target_config": "target_config_json",
    "dq_config": "dq_config_json",
    "governance_tags": "governance_tags_json",
}

# spec key on a transformation_flows[] entry -> transformation_flow_spec column
TRANSFORMATION_FLOW_ATTRIBUTES = {
    "flow_step_id": "flow_step_id",
    "dataflow_id": "dataflow_id",
    "target_catalog": "target_catalog",
    "target_schema": "target_schema",
    "target_table": "target_table",
    "target_type": "target_type",
    "source_inputs": "source_inputs_json",
    "transformation_sql": "transformation_sql",
    "target_config": "target_config_json",
    "dq_config": "dq_config_json",
    "governance_tags": "governance_tags_json",
}

# spec key on a reconciliation_flows[] entry -> reconciliation_flow_spec column
RECONCILIATION_FLOW_ATTRIBUTES = {
    "reconciliation_id": "reconciliation_id",
    "dataflow_group_id": "dataflow_group_id",
    "source_config": "source_config_json",
    "target_configs": "target_configs_json",
    "match_keys": "match_keys_json",
    "compare_columns": "compare_columns_json",
    "transform_sql": "transform_sql",
    "error_handling": "error_handling_json",
    "logging_config": "logging_config_json",
    "two_tier_verification": "two_tier_verification",
    "execution_mode": "execution_mode",
    "publish_schema": "publish_schema",
    "dq_config": "dq_config_json",
}

# flow kind -> (mapping, StructType constant name, upsert function name)
FLOW_KINDS = {
    "dataflow_group": (
        DATAFLOW_GROUP_ATTRIBUTES,
        "_DATAFLOW_GROUP_SPEC_SCHEMA",
        "upsert_dataflow_group_spec",
    ),
    "ingestion_flow": (
        INGESTION_FLOW_ATTRIBUTES,
        "_INGESTION_FLOW_SPEC_SCHEMA",
        "upsert_ingestion_flow_spec",
    ),
    "transformation_flow": (
        TRANSFORMATION_FLOW_ATTRIBUTES,
        "_TRANSFORMATION_FLOW_SPEC_SCHEMA",
        "upsert_transformation_flow_spec",
    ),
    "reconciliation_flow": (
        RECONCILIATION_FLOW_ATTRIBUTES,
        "_RECONCILIATION_FLOW_SPEC_SCHEMA",
        "upsert_reconciliation_flow_spec",
    ),
}


# ---------------------------------------------------------------------------
# The agreement assertions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("flow_kind", sorted(FLOW_KINDS))
def test_every_mapped_attribute_is_declared_in_the_struct_type(flow_kind):
    mapping, schema_constant, _ = FLOW_KINDS[flow_kind]
    declared = set(_struct_field_names(schema_constant))
    missing = sorted({col for col in mapping.values() if col not in declared})
    assert not missing, (
        f"{flow_kind}: {schema_constant} does not declare {missing}. The attribute is validated "
        "by spec_validator.py but has no column to land in."
    )


@pytest.mark.parametrize("flow_kind", sorted(FLOW_KINDS))
def test_every_mapped_attribute_is_set_by_the_row_literal(flow_kind):
    mapping, _, function_name = FLOW_KINDS[flow_kind]
    populated = _row_keyword_names(function_name)
    unset = sorted((spec_key, col) for spec_key, col in mapping.items() if col not in populated)
    assert not unset, (
        f"{flow_kind}: {function_name}'s Row(...) literal never sets "
        + ", ".join(f"{col} (spec key {spec_key!r})" for spec_key, col in unset)
        + ". The column exists and the validator accepts the attribute, so onboarding silently "
        "drops the operator's value and the engine reads back the column default. This is "
        "exactly the two_tier_verification / spark_config_json defect."
    )


@pytest.mark.parametrize("flow_kind", sorted(FLOW_KINDS))
def test_row_literal_sets_no_column_the_struct_type_lacks(flow_kind):
    """The other drift direction: a Row keyword with no StructField fails createDataFrame at
    onboarding time, on a live cluster, with no local signal at all."""
    _, schema_constant, function_name = FLOW_KINDS[flow_kind]
    declared = set(_struct_field_names(schema_constant))
    undeclared = sorted(_row_keyword_names(function_name) - declared)
    assert not undeclared, (
        f"{flow_kind}: {function_name} sets {undeclared}, which {schema_constant} does not "
        "declare -- createDataFrame would reject this row at onboarding time."
    )


# ---------------------------------------------------------------------------
# Named regression cases for the two attributes that actually rotted
# ---------------------------------------------------------------------------


def test_two_tier_verification_is_persisted():
    """REGRESSION: ``two_tier_verification`` was declared in
    ``_RECONCILIATION_FLOW_SPEC_SCHEMA`` and validated by
    ``spec_validator._validate_reconciliation_flows``, but
    ``upsert_reconciliation_flow_spec``'s ``Row(...)`` never set it, so
    ``two_tier_verification: false`` -- the documented opt-out from the XOR-fold Phase 1
    early-out -- never reached ``reconciliation_flow_spec`` and Phase 1 kept running."""
    assert "two_tier_verification" in _struct_field_names("_RECONCILIATION_FLOW_SPEC_SCHEMA")
    assert "two_tier_verification" in _row_keyword_names("upsert_reconciliation_flow_spec"), (
        "upsert_reconciliation_flow_spec must set two_tier_verification=... in its Row literal; "
        "declaring the column is not enough."
    )


def test_spark_config_json_is_persisted():
    """REGRESSION: the group-level twin of the same defect -- ``spark_config_json`` declared in
    ``_DATAFLOW_GROUP_SPEC_SCHEMA`` and in the MERGE's update set, while the ``Row(...)`` in
    ``upsert_dataflow_group_spec`` did not populate it from ``spec['spark_config']``."""
    assert "spark_config_json" in _struct_field_names("_DATAFLOW_GROUP_SPEC_SCHEMA")
    assert "spark_config_json" in _row_keyword_names("upsert_dataflow_group_spec"), (
        "upsert_dataflow_group_spec must set spark_config_json=... in its Row literal."
    )


# ---------------------------------------------------------------------------
# The mapping above is only trustworthy if it cannot silently fall behind
# ---------------------------------------------------------------------------


def test_mapping_is_complete_against_validator_constants():
    """Every reconciliation-flow key the validator conditionally *requires a mode for* is a real,
    persisted attribute and must therefore appear in the mapping -- this is the check that would
    have flagged publish_schema/dq_config had they been added to the validator without a Row
    keyword."""
    required = set(spec_validator.RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE)
    unmapped = sorted(required - set(RECONCILIATION_FLOW_ATTRIBUTES))
    assert not unmapped, (
        "RECONCILIATION_FLOW_ATTRIBUTES is missing "
        f"{unmapped}, which spec_validator.RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE "
        "treats as a real (mode-conditional) attribute."
    )


def test_mapping_carries_no_removed_attribute():
    """A removed attribute is rejected by the validator, so it must never be mapped to a column
    -- mapping one would resurrect it as a persisted field."""
    for constant_name in (
        "REMOVED_RECONCILIATION_FLOW_KEYS",
        "REMOVED_SOURCE_CONFIG_KEYS",
        "REMOVED_TARGET_CONFIG_KEYS",
    ):
        removed = set(getattr(spec_validator, constant_name))
        for flow_kind, (mapping, _schema, _fn) in FLOW_KINDS.items():
            resurrected = sorted(removed & set(mapping))
            assert not resurrected, (
                f"{flow_kind}: {resurrected} appear in spec_validator.{constant_name} (removed, "
                "rejected on presence) yet are mapped to a control-table column."
            )


# ------------------------------------------------------------------------------------------------
# Per-flow dataflow_group_id must be PERSISTED, not silently replaced by the spec's top-level one.
#
# Regression origin: upsert_reconciliation_flow_spec's Row literal read `dataflow_group_id=group_id`
# and never looked at flow["dataflow_group_id"]. V-CYC-6 REQUIRES that key on a pipeline-mode flow
# and its own rejection message offers "another group's, to run inside that group's pipeline
# instead" as a supported choice -- so a user following that instruction had their choice discarded
# and the flow registered into the wrong pipeline. Same defect class as the lost execution_mode.
# ------------------------------------------------------------------------------------------------


def _reconciliation_row_value(keyword_name):
    """Return the AST value node assigned to `keyword_name` in the recon Row(...) literal."""
    for node in ast.walk(_TREE):
        if isinstance(node, ast.FunctionDef) and node.name == "upsert_reconciliation_flow_spec":
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and getattr(call.func, "id", None) == "Row":
                    for kw in call.keywords:
                        if kw.arg == keyword_name:
                            return kw.value
    raise AssertionError(f"Row(...) literal has no {keyword_name}= keyword")


def test_reconciliation_dataflow_group_id_is_not_hardcoded_to_the_spec_group():
    """The Row literal must not assign the bare `group_id` parameter.

    A bare Name node here is exactly the bug: it ignores the flow's own attribute.
    """
    value = _reconciliation_row_value("dataflow_group_id")
    assert not isinstance(value, ast.Name), (
        "upsert_reconciliation_flow_spec sets dataflow_group_id from the bare group_id parameter, "
        "discarding the flow's own dataflow_group_id. A pipeline-mode flow that names another "
        "group would be registered into the wrong pipeline."
    )


def test_reconciliation_dataflow_group_id_reads_the_flow_then_falls_back():
    """It must read flow['dataflow_group_id'] and fall back to the spec's group when absent."""
    value = _reconciliation_row_value("dataflow_group_id")
    rendered = ast.dump(value)
    assert "dataflow_group_id" in rendered and "flow" in rendered, (
        f"expected the flow's own dataflow_group_id to be read; got {rendered}"
    )
    assert isinstance(value, ast.BoolOp) and isinstance(value.op, ast.Or), (
        "expected a `flow.get('dataflow_group_id') or group_id` fallback so an omitted key keeps "
        f"today's behaviour (the spec's own group); got {rendered}"
    )
