"""Every v1.4.0-removed attribute must be REJECTED by onboarding, never silently ignored.

This file exists as one place because the four removals share one failure mode, and it is the
quiet one. A validator that ignores an unknown key lets the spec onboard, writes the control-table
row, and runs the pipeline -- while doing something other than what the document says. For
``normalize_column_names`` and ``generate_surrogate_key`` specifically, ignoring would flip a
data-shaping behaviour from ON to OFF with no signal at all: columns silently stop being renamed,
rows silently stop carrying the key their target table is diffed on.

So each removed attribute is asserted to produce an error naming both the path and the migration.
The "names the replacement" assertions are not decoration -- an error that only says "unknown key"
sends an operator to the source, and the whole point of a removal message is that they should not
have to go there.

Pure Python, no Spark: ``validate_spec`` only touches a session for non-empty
``transformation_sql``, and nothing here has any. See ``test_spec_validator.py``'s docstring.
"""

import pytest

from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec


def _ingestion_flow(target_config=None, source_config=None, **overrides):
    merged_target = {"cdc_load_strategy": "APPEND"}
    merged_target.update(target_config or {})
    merged_source = {
        "path": "/Volumes/poc/landing/x/",
        "format": "csv",
        "schema_location": "/Volumes/poc/landing/_schemas/x/",
    }
    merged_source.update(source_config or {})
    flow = {
        "dataflow_id": "df_test",
        "source_type": "autoloader",
        "target_catalog": "poc",
        "target_schema": "bronze_test",
        "target_table": "test_raw",
        "target_type": "streaming_table",
        "source_config": merged_source,
        "target_config": merged_target,
        "dq_config": {},
        "governance_tags": {},
    }
    flow.update(overrides)
    return flow


def _reconciliation_flow(**overrides):
    flow = {
        "reconciliation_id": "recon_test",
        "source_config": {"type": "table", "table": "poc.bronze_test.baseline"},
        "target_configs": [
            {
                "target_id": "primary",
                "type": "table",
                "table": "poc.bronze_test.final",
                "comparison_direction": "source_to_target",
                "append_target_table": "poc.bronze_test.cdc",
            }
        ],
        "match_keys": ["example_id"],
    }
    flow.update(overrides)
    return flow


def _errors(spec):
    return validate_spec(None, spec)[4]


# --------------------------------------------------------------------------- 2. normalization
def test_normalize_column_names_is_rejected_not_ignored():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_ingestion_flow(source_config={"normalize_column_names": True})],
    }
    matched = [e for e in _errors(spec) if "source_config.normalize_column_names" in e]
    assert matched, "the removed legacy boolean must be reported, not silently ignored"
    assert "column_normalization" in matched[0], "the error must name the replacement"


def test_normalize_column_names_false_is_also_rejected():
    """Presence, not truthiness. A `false` still describes a feature that no longer exists, and
    leaving it in a spec tells the next reader the framework still has the knob."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_ingestion_flow(source_config={"normalize_column_names": False})],
    }
    assert any("source_config.normalize_column_names" in e for e in _errors(spec))


def test_column_normalization_object_still_validates():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _ingestion_flow(source_config={"column_normalization": {"enabled": True, "case": "upper"}})
        ],
    }
    assert _errors(spec) == []


def test_column_normalization_rejects_an_unknown_case():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_ingestion_flow(source_config={"column_normalization": {"case": "title"}})],
    }
    assert any("column_normalization.case" in e for e in _errors(spec))


# ------------------------------------------------------------------------- 3. surrogate keys
@pytest.mark.parametrize(
    "key, value",
    [
        ("generate_surrogate_key", True),
        ("generate_surrogate_key", False),
        ("surrogate_key_columns", ["a", "b"]),
        ("surrogate_key_exclude_columns", ["ingest_ts"]),
    ],
)
def test_target_config_surrogate_key_attributes_are_rejected(key, value):
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [_ingestion_flow(target_config={key: value})]}
    matched = [e for e in _errors(spec) if f"target_config.{key}" in e]
    assert matched, f"{key} must be reported"
    assert "primary_keys" in matched[0], "the error must point at what replaces it"


def test_full_snapshot_cdc_no_pk_strategy_is_rejected_by_name():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_ingestion_flow(target_config={"cdc_load_strategy": "FULL_SNAPSHOT_CDC_NO_PK"})],
    }
    matched = [e for e in _errors(spec) if "cdc_load_strategy" in e]
    assert matched
    # Not the generic "not one of [...]" list: a withdrawn strategy needs to say it was withdrawn
    # and what to use instead, or an author reads it as a typo and goes looking in the source.
    assert "removed in v1.4.0" in matched[0]
    assert "FULL_SNAPSHOT_CDC" in matched[0] and "TRUNCATE_AND_LOAD" in matched[0]


def test_full_snapshot_cdc_still_requires_primary_keys():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_ingestion_flow(target_config={"cdc_load_strategy": "FULL_SNAPSHOT_CDC"})],
    }
    assert any("primary_keys" in e for e in _errors(spec))


def test_full_snapshot_cdc_with_primary_keys_is_valid():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _ingestion_flow(target_config={"cdc_load_strategy": "FULL_SNAPSHOT_CDC", "primary_keys": ["customer_id"]})
        ],
    }
    assert _errors(spec) == []


# ---------------------------------------------------------------------------- 4. recon_mode
@pytest.mark.parametrize("value", ["triggered", "continuous"])
def test_recon_mode_is_rejected_for_both_former_values(value):
    """Including `triggered` -- the value that still describes what happens. It is removed as an
    *attribute*, so a spec asserting the surviving behaviour is still asserting a field that no
    longer exists, and the next person to read it will look for the switch."""
    spec = {"dataflow_group_id": "dfg_test", "reconciliation_flows": [_reconciliation_flow(recon_mode=value)]}
    matched = [e for e in _errors(spec) if "recon_mode" in e]
    assert matched
    assert "triggered-only" in matched[0]


def test_reconciliation_generate_surrogate_key_is_rejected():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_reconciliation_flow(generate_surrogate_key=True)],
    }
    assert any("generate_surrogate_key" in e for e in _errors(spec))


def test_reconciliation_flow_without_the_removed_keys_is_valid():
    spec = {"dataflow_group_id": "dfg_test", "reconciliation_flows": [_reconciliation_flow()]}
    assert _errors(spec) == []


# ------------------------------------------------------------------- 8. source_data_type (added)
def test_source_data_type_is_accepted_on_an_encrypted_column():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _ingestion_flow(
                target_config={
                    "encrypted_columns": [
                        {
                            "column_name": "ssn",
                            "source_data_type": "string",
                            "secret": {
                                "secret_catalog": "poc",
                                "secret_schema": "security",
                                "secret_key": "pii_key",
                            },
                        }
                    ]
                }
            )
        ],
    }
    assert _errors(spec) == []


def test_source_data_type_is_optional():
    """The whole safety property of the new attribute: omitting it must stay valid, because every
    pre-v1.4.0 spec omits it and none of them should need editing."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _ingestion_flow(
                target_config={
                    "encrypted_columns": [
                        {
                            "column_name": "ssn",
                            "secret": {
                                "secret_catalog": "poc",
                                "secret_schema": "security",
                                "secret_key": "pii_key",
                            },
                        }
                    ]
                }
            )
        ],
    }
    assert _errors(spec) == []


def test_source_data_type_must_be_a_string_when_present():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _ingestion_flow(
                target_config={
                    "encrypted_columns": [
                        {
                            "column_name": "ssn",
                            "source_data_type": 42,
                            "secret": {
                                "secret_catalog": "poc",
                                "secret_schema": "security",
                                "secret_key": "pii_key",
                            },
                        }
                    ]
                }
            )
        ],
    }
    assert any("source_data_type" in e for e in _errors(spec))
