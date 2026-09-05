"""Unit tests for onboarding/spec_validator.py -- pure Python, no Spark session touched.

``validate_spec`` only calls Spark (``spark.sql(f"EXPLAIN {sql}")``) when a transformation
flow has a non-empty ``transformation_sql`` -- every case here is ingestion-only or an
otherwise-invalid transformation flow that never reaches that code path, so ``spark=None``
is a safe sentinel throughout. Transformation-SQL-syntax validation itself is covered under
tests/integration/ (it needs a real Spark session).

v2 schema: ``cdc_load_strategy`` and every CDC-related field live inside ``target_config``
(no top-level ``cdc_load_strategy`` field, no separate ``cdc_config``); DQ rules/quarantine
live under ``dq_config``; governance is ``governance_tags`` (tags-only, replacing
``abac_config``); ``source_type`` is ``"autoloader"`` (renamed from ``gcs_autoloader``).
"""

from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec


def _base_ingestion_flow(target_config=None, **overrides):
    """Merges ``target_config`` overrides onto a default ``{"cdc_load_strategy": "APPEND"}``
    rather than replacing it outright, so callers only need to specify what they're
    actually testing."""
    merged_target_config = {"cdc_load_strategy": "APPEND"}
    merged_target_config.update(target_config or {})
    flow = {
        "dataflow_id": "df_test",
        "source_type": "autoloader",
        "target_catalog": "poc",
        "target_schema": "bronze_test",
        "target_table": "test_raw",
        "target_type": "streaming_table",
        "source_config": {"path": "/Volumes/poc/landing/x/", "format": "csv", "schema_location": "/Volumes/poc/landing/_schemas/x/"},
        "target_config": merged_target_config,
        "dq_config": {},
        "governance_tags": {},
    }
    flow.update(overrides)
    return flow


def test_valid_minimal_spec_has_no_errors():
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [_base_ingestion_flow()]}
    ingestion_flows, transformation_flows, _reconciliation_flows, _observability_destinations, errors = validate_spec(None, spec)
    assert errors == []
    assert len(ingestion_flows) == 1
    assert transformation_flows == []


def test_missing_dataflow_group_id_is_reported():
    spec = {"ingestion_flows": [_base_ingestion_flow()]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("dataflow_group_id: is required" in e for e in errors)


def test_empty_flows_is_reported():
    spec = {"dataflow_group_id": "dfg_test"}
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("At least one of 'ingestion_flows'" in e for e in errors)


def test_invalid_source_type_reports_allowed_values():
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [_base_ingestion_flow(source_type="autoloadr")]}
    _, _, _, _, errors = validate_spec(None, spec)
    matches = [e for e in errors if "source_type" in e]
    assert len(matches) == 1
    assert "autoloadr" in matches[0]
    assert "autoloader" in matches[0]  # the allowed-values list should name the correct spelling


def test_depends_on_dataflow_group_ids_is_rejected_not_silently_ignored():
    """v1 field -- orchestration/dependency ordering is a Lakeflow Jobs concern now.

    Until v1.7.1 this key was accepted and simply left unread, which meant a spec could appear
    to declare an ordering the framework never enforced. Unknown-key rejection makes that
    visible: the key is now an error naming where the ordering actually belongs.
    """
    spec = {
        "dataflow_group_id": "dfg_test",
        "depends_on_dataflow_group_ids": ["dfg_test"],
        "ingestion_flows": [_base_ingestion_flow()],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any(
        e.startswith("depends_on_dataflow_group_ids:") and "Lakeflow Jobs" in e for e in errors
    ), errors


def test_scd3_rejected_on_ingestion_flow_scd3_is_transformation_only():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(target_config={"cdc_load_strategy": "SCD3", "primary_keys": ["id"], "columns_to_check": ["status"]})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("cdc_load_strategy" in e and "SCD3" in e for e in errors)


def test_boolean_field_rejects_quoted_string_true():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(source_config={"path": "/x/", "format": "csv", "schema_location": "/y/", "capture_technical_metadata": "yes"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("capture_technical_metadata" in e and "boolean" in e for e in errors)


def test_scd2_without_columns_to_check_has_no_error_columns_to_check_is_optional():
    """v2 behavior change: columns_to_check is comparison-only and optional -- empty/absent
    means 'compare all applicable columns', not a required field."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_config={"cdc_load_strategy": "SCD2", "primary_keys": ["id"], "sequence_by_column": "updated_at"})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_scd1_with_valid_target_config_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_config={"cdc_load_strategy": "SCD1", "primary_keys": ["id"], "sequence_by_column": "updated_at"})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_scd1_sequence_by_column_is_optional():
    """v2 behavior change: sequence_by_column is optional for SCD1/SCD2/SCD3 -- falls back
    to __framework_ingestion_timestamp_utc at runtime (cdc/scd.py)."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(target_config={"cdc_load_strategy": "SCD1", "primary_keys": ["id"]})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_cdc_operation_column_valid_on_scd1_with_full_mapping():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_config={
                    "cdc_load_strategy": "SCD1",
                    "primary_keys": ["id"],
                    "sequence_by_column": "updated_at",
                    "cdc_operation_column": "op",
                    "cdc_operation_mapping": {"delete_values": ["D"]},
                }
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_cdc_operation_column_optional_even_with_primary_keys():
    """Non-regression: cdc_operation_column/mapping stay fully optional regardless of
    primary_keys -- a source can have a real PK with no explicit delete-marker column."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_config={"cdc_load_strategy": "SCD1", "primary_keys": ["id"], "sequence_by_column": "updated_at"})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []
    assert not any("cdc_operation" in e for e in errors)


def test_cdc_operation_column_rejected_on_append_strategy():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_config={"cdc_load_strategy": "APPEND", "cdc_operation_column": "op", "cdc_operation_mapping": {"delete_values": ["D"]}}
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("only meaningful for cdc_load_strategy in" in e for e in errors)


def test_cdc_operation_mapping_missing_delete_values_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_config={
                    "cdc_load_strategy": "SCD1",
                    "primary_keys": ["id"],
                    "sequence_by_column": "updated_at",
                    "cdc_operation_column": "op",
                    "cdc_operation_mapping": {},
                }
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("cdc_operation_mapping.delete_values: is required" in e for e in errors)


def test_cdc_operation_column_and_mapping_both_omitted_on_scd2_has_no_errors():
    """Same non-regression as SCD1 above, pinned for SCD2 too: cdc_operation_column/mapping
    stay fully optional -- the `if ... is not None or ... is not None:` gate in
    _validate_target_config only requires either at all once one of them is provided."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_config={"cdc_load_strategy": "SCD2", "primary_keys": ["id"], "sequence_by_column": "updated_at"})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []
    assert not any("cdc_operation" in e for e in errors)


# --- source_zip_handling.pre_extraction_decryption reshape coverage ------------------------
#
# `secret` was removed as a top-level source_zip_handling field; the ZIP archive's own AES
# password now lives at pre_extraction_decryption.secret_passphrase (a 3-level UC secret ref),
# independent of and combinable with pre_extraction_decryption.type (now itself optional,
# previously required whenever pre_extraction_decryption was present at all).


def _zip_handling_source_config(**overrides):
    config = {
        "enabled": True,
        "source_zip_path": "/Volumes/poc/landing/incoming/",
        "zip_file_pattern": "*.zip",
        "target_volume_path": "/Volumes/poc/landing/extracted/",
    }
    config.update(overrides)
    return {"path": "/x/", "format": "csv", "schema_location": "/y/", "source_zip_handling": config}


def test_source_zip_handling_pre_extraction_decryption_omitted_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(source_config=_zip_handling_source_config())],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_source_zip_handling_pre_extraction_decryption_empty_dict_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(source_config=_zip_handling_source_config(pre_extraction_decryption={}))
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_source_zip_handling_secret_passphrase_only_no_type_has_no_errors():
    """secret_passphrase present with no 'type' sibling: 'just a password-protected ZIP, no
    outer decryption layer' -- must validate cleanly, and no type-only error (e.g. a bogus
    "private_key_secret required" from the pgp branch) should ever fire since that branch
    only runs when 'type' is present."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config=_zip_handling_source_config(
                    pre_extraction_decryption={
                        "secret_passphrase": {"secret_catalog": "poc", "secret_schema": "security", "secret_key": "zip_password"}
                    }
                )
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_source_zip_handling_type_only_no_secret_passphrase_has_no_errors():
    """type='pgp' alone, no secret_passphrase -- unchanged v1-style behavior: outer PGP
    decryption layer, unprotected inner ZIP."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config=_zip_handling_source_config(
                    pre_extraction_decryption={
                        "type": "pgp",
                        "private_key_secret": {"secret_catalog": "poc", "secret_schema": "security", "secret_key": "pgp_private_key"},
                    }
                )
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_source_zip_handling_secret_passphrase_incomplete_reports_error():
    """secret_passphrase, when present, is still validated as a genuine 3-level secret ref --
    a partial ref (e.g. missing secret_key) must still be reported even with no 'type'."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config=_zip_handling_source_config(
                    pre_extraction_decryption={"secret_passphrase": {"secret_catalog": "poc", "secret_schema": "security"}}
                )
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("secret_passphrase.secret_key: is required" in e for e in errors)


def test_encrypted_column_invalid_mode_reports_allowed_values():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_config={
                    "encrypted_columns": [
                        {"column_name": "ssn", "mode": "AES128", "secret": {"secret_catalog": "security", "secret_schema": "keys", "secret_key": "k"}}
                    ]
                }
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("mode" in e and "AES128" in e for e in errors)


def test_encrypted_column_requires_secret_ref():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(target_config={"encrypted_columns": [{"column_name": "ssn"}]})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("encrypted_columns[0].secret: is required" in e for e in errors)


def test_encrypted_column_secret_ref_requires_all_three_parts():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_config={"encrypted_columns": [{"column_name": "ssn", "secret": {"secret_catalog": "security"}}]}
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("encrypted_columns[0].secret.secret_schema: is required" in e for e in errors)
    assert any("encrypted_columns[0].secret.secret_key: is required" in e for e in errors)


def test_schema_location_defaults_when_omitted_for_autoloader():
    flow = _base_ingestion_flow(source_config={"path": "/Volumes/poc/landing/x/", "format": "csv"})
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [flow]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []
    assert flow["source_config"]["schema_location"] == "/Volumes/poc/landing/_schemas/test_raw/"


def test_schema_location_explicit_value_is_not_overridden():
    flow = _base_ingestion_flow(
        source_config={"path": "/Volumes/poc/landing/x/", "format": "csv", "schema_location": "/Volumes/poc/custom/_schemas/"}
    )
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [flow]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []
    assert flow["source_config"]["schema_location"] == "/Volumes/poc/custom/_schemas/"


def test_schema_location_still_required_when_target_identity_missing():
    """No target_catalog/target_table to derive a default from -- omitting schema_location
    is still reported, same as before."""
    flow = _base_ingestion_flow(target_catalog=None, source_config={"path": "/x/", "format": "csv"})
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [flow]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("schema_location: is required" in e for e in errors)


def test_file_pattern_accepts_string():
    flow = _base_ingestion_flow(source_config={"path": "/x/", "format": "csv", "schema_location": "/y/", "file_pattern": "orc_*"})
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [flow]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_reader_options_must_be_string_to_string():
    flow = _base_ingestion_flow(
        source_config={"path": "/x/", "format": "csv", "schema_location": "/y/", "reader_options": {"header": True}}
    )
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [flow]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("reader_options" in e for e in errors)


def test_dq_rule_missing_rule_id_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(dq_config={"rules": [{"expression": "x > 0", "action": "warn"}]})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("dq_config.rules[0].rule_id: is required" in e for e in errors)


def _retention_spec(policy):
    return {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config={
                    "path": "/x/",
                    "format": "csv",
                    "schema_location": "/y/",
                    "landing_retention_policy": policy,
                }
            )
        ],
    }


def test_landing_retention_archive_without_archive_path_degrades_to_off():
    """v1.3.0 (E01) behavior change: ``clean_source: "archive"`` with a missing or empty
    ``archive_path`` is no longer a validation error -- it is a documented degrade-to-off
    (the runtime logs a warning and applies no retention action at all). Previously this
    raised ``archive_path: is required``, which made "I haven't picked an archive location
    yet" un-onboardable rather than simply inert."""
    for policy in ({"clean_source": "archive"}, {"clean_source": "archive", "archive_path": ""}):
        _, _, _, _, errors = validate_spec(None, _retention_spec(policy))
        assert not any("archive_path" in e for e in errors), f"unexpected archive_path error for {policy}: {errors}"


def test_landing_retention_delete_does_not_require_archive_path():
    """``clean_source: "delete"`` deletes aged-out files irrespective of any archive location,
    so ``archive_path`` must never be demanded for it (E01)."""
    _, _, _, _, errors = validate_spec(None, _retention_spec({"clean_source": "delete"}))
    assert not any("archive_path" in e for e in errors), errors


def test_landing_retention_zero_retention_days_is_valid():
    """``retention_days: 0`` must pass schema validation (minimum is 0, not 1) -- E01."""
    _, _, _, _, errors = validate_spec(None, _retention_spec({"clean_source": "delete", "retention_days": 0}))
    assert not any("retention_days" in e for e in errors), errors


def test_multiple_problems_are_all_reported_together():
    spec = {
        "ingestion_flows": [
            {
                "dataflow_id": "df_bad_example",
                "source_type": "autoloadr",
                "target_type": "streaming_table",
                "target_config": {"cdc_load_strategy": "APPEND"},
                "source_config": {
                    "capture_technical_metadata": "yes",
                    "landing_retention_policy": {"clean_source": "archive", "retention_days": "seven"},
                },
            }
        ]
    }
    _, _, _, _, errors = validate_spec(None, spec)
    # dataflow_group_id missing, source_type invalid, target_catalog/schema/table missing,
    # capture_technical_metadata not bool, archive_path missing, retention_days not int.
    assert len(errors) >= 6


def test_columns_to_exclude_valid_on_scd1_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_config={
                    "cdc_load_strategy": "SCD1",
                    "primary_keys": ["id"],
                    "sequence_by_column": "updated_at",
                    "columns_to_exclude": ["batch_load_ts", "etl_run_id"],
                }
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_columns_to_exclude_rejected_on_append_strategy():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(target_config={"cdc_load_strategy": "APPEND", "columns_to_exclude": ["batch_load_ts"]})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("columns_to_exclude" in e and "only meaningful for cdc_load_strategy" in e for e in errors)


def test_auto_ttl_valid_on_append_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_config={"cdc_load_strategy": "APPEND", "auto_ttl": {"timestamp_column": "updated_at", "expire_in_days": 90}})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_auto_ttl_missing_timestamp_column_is_optional_not_an_error():
    """auto_ttl is opt-in: an incomplete block is not a validation error, it just means Auto
    TTL won't be applied for this flow (see build_auto_ttl_kwarg's matching soft-skip)."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(target_config={"cdc_load_strategy": "APPEND", "auto_ttl": {"expire_in_days": 90}})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_auto_ttl_non_positive_expire_in_days_is_rejected():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_config={"cdc_load_strategy": "APPEND", "auto_ttl": {"timestamp_column": "updated_at", "expire_in_days": 0}})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("auto_ttl.expire_in_days" in e for e in errors)


def test_auto_ttl_rejected_on_scd1_strategy():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_config={
                    "cdc_load_strategy": "SCD1",
                    "primary_keys": ["id"],
                    "sequence_by_column": "updated_at",
                    "auto_ttl": {"timestamp_column": "updated_at", "expire_in_days": 90},
                }
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("auto_ttl: only supported for cdc_load_strategy in" in e for e in errors)


def test_storage_format_iceberg_rejected_on_streaming_table():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(target_config={"storage_format": "iceberg"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("storage_format" in e and "iceberg" in e and "batch_table" in e for e in errors)


def test_storage_format_iceberg_accepted_on_batch_table():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_type="batch_table", target_config={"cdc_load_strategy": "TRUNCATE_AND_LOAD", "storage_format": "iceberg"})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_table_properties_enable_iceberg_read_uniformity_accepts_bool():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_config={"table_properties": {"enable_iceberg_read_uniformity": True}})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_governance_tags_column_tags_multi_tag_per_column_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                governance_tags={"column_tags": [{"column": "ssn", "tags": {"mask": "PII", "classification": "restricted"}}]}
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_governance_tags_table_tags_multi_tag_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(governance_tags={"table_tags": {"row_filter": "region_restricted", "domain": "finance"}})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_governance_tags_column_tags_missing_column_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(governance_tags={"column_tags": [{"tags": {"mask": "PII"}}]})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("column_tags[0].column: is required" in e for e in errors)


def test_dq_config_quarantine_table_and_record_id_column_accept_strings():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(dq_config={"quarantine_table": "raw_quarantine", "record_id_column": "example_id"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def _base_transformation_flow(target_config=None, **overrides):
    merged_target_config = {"cdc_load_strategy": "APPEND"}
    merged_target_config.update(target_config or {})
    flow = {
        "flow_step_id": "ts_test",
        "dataflow_id": "df_test",
        "target_catalog": "poc",
        "target_schema": "silver_test",
        "target_table": "test_silver",
        "target_type": "streaming_table",
        "source_inputs": [{"input_name": "test_input", "table": "poc.bronze_test.raw", "is_streaming": True}],
        "target_config": merged_target_config,
        "dq_config": {},
        "governance_tags": {},
        # transformation_sql deliberately omitted: validate_spec is called with spark=None
        # in this file, and a non-empty transformation_sql would route into
        # _validate_sql_syntax's spark.sql(...) call. The resulting "transformation_sql:
        # is required" error is expected and ignored by the assertions below.
    }
    flow.update(overrides)
    return flow


def test_duplicate_input_name_across_transformation_flows_is_reported():
    spec = {
        "dataflow_group_id": "dfg_test",
        "transformation_flows": [
            _base_transformation_flow(
                flow_step_id="ts_a",
                target_table="dim_a",
                source_inputs=[{"input_name": "shared_input", "table": "poc.bronze_test.raw", "is_streaming": True}],
            ),
            _base_transformation_flow(
                flow_step_id="ts_b",
                target_table="dim_b",
                source_inputs=[{"input_name": "shared_input", "table": "poc.bronze_test.raw", "is_streaming": True}],
            ),
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    matches = [e for e in errors if "shared_input" in e and "already used by" in e]
    assert len(matches) == 1
    assert "ts_b" in matches[0] and "ts_a" in matches[0]


def test_distinct_input_names_across_transformation_flows_has_no_duplicate_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "transformation_flows": [
            _base_transformation_flow(
                flow_step_id="ts_a",
                target_table="dim_a",
                source_inputs=[{"input_name": "input_a", "table": "poc.bronze_test.raw", "is_streaming": True}],
            ),
            _base_transformation_flow(
                flow_step_id="ts_b",
                target_table="dim_b",
                source_inputs=[{"input_name": "input_b", "table": "poc.bronze_test.raw", "is_streaming": True}],
            ),
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert not any("already used by" in e for e in errors)


def test_transformation_source_input_decrypted_columns_requires_cast_to_type():
    spec = {
        "dataflow_group_id": "dfg_test",
        "transformation_flows": [
            _base_transformation_flow(
                source_inputs=[
                    {
                        "input_name": "test_input",
                        "table": "poc.bronze_test.raw",
                        "is_streaming": True,
                        "decrypted_columns": [
                            {"column_name": "ssn_encrypted", "secret": {"secret_catalog": "security", "secret_schema": "keys", "secret_key": "k"}}
                        ],
                    }
                ]
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("decrypted_columns[0].cast_to_type: is required" in e for e in errors)


def test_transformation_source_input_decrypted_columns_valid_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "transformation_flows": [
            _base_transformation_flow(
                source_inputs=[
                    {
                        "input_name": "test_input",
                        "table": "poc.bronze_test.raw",
                        "is_streaming": True,
                        "decrypted_columns": [
                            {
                                "column_name": "ssn_encrypted",
                                "cast_to_type": "string",
                                "secret": {"secret_catalog": "security", "secret_schema": "keys", "secret_key": "k"},
                            }
                        ],
                    }
                ]
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert not any("decrypted_columns" in e for e in errors)


def _base_reconciliation_flow(**overrides):
    flow = {
        "reconciliation_id": "recon_test",
        "source_config": {"type": "table", "table": "poc.bronze_x.baseline"},
        "target_configs": [
            {"target_id": "primary", "type": "table", "table": "poc.bronze_x.product", "append_target_table": "poc.bronze_x.cdc"}
        ],
        "match_keys": ["id"],
        "compare_columns": ["status"],
        "error_handling": {"on_failure": "fail"},
    }
    flow.update(overrides)
    return flow


def test_reconciliation_flow_alone_satisfies_non_empty_spec_check():
    spec = {"dataflow_group_id": "dfg_test", "reconciliation_flows": [_base_reconciliation_flow()]}
    _, _, reconciliation_flows, _, errors = validate_spec(None, spec)
    assert errors == []
    assert len(reconciliation_flows) == 1


def test_reconciliation_flow_missing_match_keys_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(match_keys=None)],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("match_keys: is required" in e for e in errors)


def test_reconciliation_flow_missing_target_configs_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(target_configs=None)],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("target_configs: is required" in e for e in errors)


def test_reconciliation_flow_target_missing_append_target_table_is_required_when_source_to_target():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _base_reconciliation_flow(target_configs=[{"target_id": "primary", "type": "table", "table": "poc.bronze_x.product"}])
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("append_target_table: is required" in e for e in errors)


def test_reconciliation_flow_target_to_source_only_does_not_require_append_target_table():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _base_reconciliation_flow(
                target_configs=[
                    {"target_id": "primary", "type": "table", "table": "poc.bronze_x.product", "comparison_direction": "target_to_source"}
                ]
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_reconciliation_flow_duplicate_target_id_is_reported():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _base_reconciliation_flow(
                target_configs=[
                    {"target_id": "primary", "type": "table", "table": "poc.bronze_x.product", "append_target_table": "poc.bronze_x.cdc"},
                    {"target_id": "primary", "type": "table", "table": "poc.bronze_x.replica", "append_target_table": "poc.bronze_x.cdc2"},
                ]
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("target_id" in e and "already used by" in e for e in errors)


def test_reconciliation_flow_invalid_on_failure_value_reports_allowed_values():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(error_handling={"on_failure": "retry"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("on_failure" in e and "retry" in e for e in errors)


def test_reconciliation_flow_missing_source_config_table_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(source_config={"type": "table"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("source_config.table: is required" in e for e in errors)


def test_reconciliation_flow_rejects_non_delta_source_type():
    """v1.3.0 (E12d) scope restriction: reconciliation supports Delta tables only, so a
    ``type: "file"`` source is now rejected outright rather than validated for
    ``path``/``format``. The error must name the supported type and say what to do instead."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(source_config={"type": "file"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("allowed values are ['table']" in e for e in errors), errors
    assert any("reconciliation is supported for Delta tables only" in e for e in errors), errors


def test_reconciliation_flow_hash_precomputed_rejected_on_non_table_type():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _base_reconciliation_flow(source_config={"type": "file", "path": "/x/", "format": "csv", "hash_precomputed": True})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("hash_precomputed" in e and "type == 'table'" in e for e in errors)


def test_reconciliation_flow_comparison_direction_invalid_value_reports_allowed_values():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _base_reconciliation_flow(
                target_configs=[
                    {
                        "target_id": "primary",
                        "type": "table",
                        "table": "poc.bronze_x.product",
                        "append_target_table": "poc.bronze_x.cdc",
                        "comparison_direction": "sideways",
                    }
                ]
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("comparison_direction" in e and "sideways" in e for e in errors)


# ---------------------------------------------------------------------------
# execution_mode -- "job" (default, unchanged standalone 05_reconciliation_engine.py job task),
# "pipeline" (register L3+L4 published datasets AND the L5 heal lane inside this flow's own
# dataflow_group_id's Lakeflow pipeline update), or "pipeline_audit_only" (L3+L4 only). See
# ALLOWED_RECONCILIATION_EXECUTION_MODES.
# ---------------------------------------------------------------------------


def test_execution_mode_omitted_defaults_and_has_no_invalid_value_error():
    spec = {"dataflow_group_id": "dfg_test", "reconciliation_flows": [_base_reconciliation_flow()]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert not any("execution_mode" in e for e in errors)


def test_execution_mode_accepts_job_pipeline_and_pipeline_audit_only():
    for mode in ("job", "pipeline", "pipeline_audit_only"):
        spec = {
            "dataflow_group_id": "dfg_test",
            "reconciliation_flows": [_base_reconciliation_flow(execution_mode=mode, dataflow_group_id="dfg_test")],
        }
        _, _, _, _, errors = validate_spec(None, spec)
        assert not any("execution_mode" in e and "has invalid value" in e for e in errors), (mode, errors)


def test_execution_mode_rejects_an_unknown_value():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(execution_mode="streaming")],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any(
        "execution_mode" in e and "has invalid value" in e and "streaming" in e for e in errors
    )


# ---------------------------------------------------------------------------
# V-CYC-1..8 -- cross-array reconciliation/pipeline-placement checks, only meaningful once every
# flow array in the spec is known (_validate_reconciliation_pipeline_placement). Every case below
# uses assert any(...) rather than errors == [] (except the dedicated positive control), because
# several of these specs are deliberately minimal and can carry other, unrelated findings (e.g.
# V-CYC-1's own "does not resolve to any target" alongside a V-CYC-2/V-CYC-5 case) that this file
# does not care about.
# ---------------------------------------------------------------------------


def test_v_cyc_1_pipeline_mode_source_must_resolve_to_an_in_spec_producer():
    """A group-less-looking recon source (nothing in this spec actually produces it) must be
    rejected in pipeline mode -- reading it would silently degrade to an external, always-
    one-update-stale read instead of this update's freshly written rows."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _base_reconciliation_flow(execution_mode="pipeline", dataflow_group_id="dfg_test")
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("does not resolve to any target this dataflow group actually produces" in e for e in errors)


def test_v_cyc_2_append_target_table_is_this_groups_own_target_is_rejected():
    """Appending into a target THIS dataflow group's own ingestion/transformation flow already
    owns would corrupt whatever write contract that flow declared for it."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(target_catalog="poc", target_schema="bronze_x", target_table="cdc")],
        "reconciliation_flows": [
            _base_reconciliation_flow(execution_mode="pipeline", dataflow_group_id="dfg_test")
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("is this dataflow group's own target" in e for e in errors)


def test_v_cyc_3_append_target_table_is_this_groups_own_ingestion_source_is_rejected():
    """Appending corrections back into the same group's own raw zerobus ingestion source races
    the next update's own read of it."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_type="zerobus",
                source_config={"source_catalog": "poc", "source_schema": "bronze_x", "source_table": "cdc"},
            )
        ],
        "reconciliation_flows": [
            _base_reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                target_configs=[
                    {"target_id": "primary", "type": "table", "table": "poc.bronze_x.product", "append_target_table": "poc.bronze_x.cdc"}
                ],
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("raw ingestion source" in e and "corrupting an append-only source" in e for e in errors)


def test_v_cyc_4_append_target_table_is_another_groups_ingestion_source_only_warns(caplog):
    """Cross-GROUP collision: Lakeflow cannot see this loop from either pipeline's own graph, so
    it is a WARNING an operator must confirm is intentional, never a hard error."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_type="zerobus",
                source_config={"source_catalog": "poc", "source_schema": "bronze_x", "source_table": "cdc"},
            )
        ],
        "reconciliation_flows": [
            _base_reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_other",
                target_configs=[
                    {"target_id": "primary", "type": "table", "table": "poc.bronze_x.product", "append_target_table": "poc.bronze_x.cdc"}
                ],
            )
        ],
    }
    with caplog.at_level("WARNING"):
        _, _, _, _, errors = validate_spec(None, spec)
    assert not any("raw ingestion source" in e for e in errors), errors
    assert any("cross-pipeline landing" in record.getMessage() for record in caplog.records)


def test_v_cyc_5_append_target_table_equal_to_own_source_config_table_is_rejected():
    """V-CYC-5 is a PIPELINE-mode rule, so the fixture must declare pipeline mode.

    The append-into-your-own-source loop is a Lakeflow graph cycle: the same update both reads
    that dataset and appends to it. Under ``execution_mode: "job"`` there is no such graph -- the
    standalone engine runs after the update finishes -- so the finding is downgraded to a
    warning there (see ``_append_cycle_finding``). The job-mode half of that contract is pinned
    by the companion test below.
    """
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [
            _base_reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.cdc"},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("must not equal this flow's own source_config.table" in e for e in errors)


def test_v_cyc_5_is_only_a_warning_in_job_mode(caplog):
    """Backward compatibility: the same shape must still ONBOARD under job mode.

    ``flowx_testing/038_rec_003_precomputed_hash.json`` is a shipped, pre-v1.5.0, purely
    job-mode spec that appends into its own comparison target. Because
    ``02_onboarding_engine.py`` raises on any non-empty ``errors`` list, letting V-CYC-5 fire
    unconditionally made that document un-onboardable with no edit by its author -- a real
    regression, caught by tests/unit/test_recon_backward_compatibility.py.
    """
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(source_config={"type": "table", "table": "poc.bronze_x.cdc"})],
    }
    with caplog.at_level("WARNING"):
        _, _, _, _, errors = validate_spec(None, spec)

    assert not any("must not equal this flow's own source_config.table" in e for e in errors), errors
    assert any(
        "must not equal this flow's own source_config.table" in record.getMessage() for record in caplog.records
    ), "job mode must still SURFACE the hazard as a warning, not swallow it silently"


def test_v_cyc_6_dataflow_group_id_is_required_in_pipeline_mode():
    spec = {
        "dataflow_group_id": "dfg_test",
        "reconciliation_flows": [_base_reconciliation_flow(execution_mode="pipeline")],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("dataflow_group_id: is required when execution_mode is" in e for e in errors)


def test_v_cyc_7_merge_cdc_strategy_producer_is_rejected_for_pipeline_mode():
    """SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC dispatch through dlt.apply_changes[_from_snapshot] --
    real MERGE/UPDATE/DELETE writes, not append-only -- so an in-pipeline heal lane appending
    into a comparison built over rows Lakeflow may still rewrite is unsafe."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_catalog="poc",
                target_schema="bronze_x",
                target_table="product",
                target_config={"cdc_load_strategy": "SCD1", "primary_keys": ["id"]},
            )
        ],
        "reconciliation_flows": [
            _base_reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.product"},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("dispatches through dlt.apply_changes" in e for e in errors)


def test_v_cyc_7_truncate_and_load_into_materialized_view_producer_is_rejected_for_pipeline_mode():
    """TRUNCATE_AND_LOAD into a materialized_view is fully refreshed (not append-only) on every
    update, the other non-append-only producer shape pipeline mode must reject."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                target_catalog="poc",
                target_schema="bronze_x",
                target_table="product",
                target_type="materialized_view",
                target_config={"cdc_load_strategy": "TRUNCATE_AND_LOAD"},
            )
        ],
        "reconciliation_flows": [
            _base_reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.product"},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("fully refreshed (not append-only) on every" in e for e in errors)


def test_v_cyc_8_landing_retention_policy_collision_on_shared_path_is_rejected():
    """Two ingestion flows sharing one Auto Loader landing path with different
    landing_retention_policy configs is a latent data-loss race: cloudFiles.cleanSource MOVES or
    DELETES committed landing files, so two competing lifecycle regimes corrupt whichever policy
    runs second."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                dataflow_id="df_a",
                source_config={
                    "path": "/Volumes/poc/landing/x/",
                    "format": "csv",
                    "schema_location": "/Volumes/poc/landing/_schemas/x/",
                    "landing_retention_policy": {"clean_source": "delete", "retention_days": 7},
                },
            ),
            _base_ingestion_flow(
                dataflow_id="df_b",
                target_table="test_raw_2",
                source_config={
                    "path": "/Volumes/poc/landing/x/",
                    "format": "csv",
                    "schema_location": "/Volumes/poc/landing/_schemas/x/",
                    "landing_retention_policy": {"clean_source": "archive", "archive_path": "/Volumes/poc/landing/_archive/"},
                },
            ),
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("landing_retention_policy" in e and "data-loss race" in e for e in errors)


def test_v_cyc_reconciliation_pipeline_mode_fully_valid_flow_has_no_errors():
    """Positive control: a producer with an append-only strategy, a source that resolves to it,
    a dataflow_group_id matching the spec's own, and an append_target_table that collides with
    nothing -- must validate cleanly end to end."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(target_catalog="poc", target_schema="bronze_x", target_table="product")
        ],
        "reconciliation_flows": [
            _base_reconciliation_flow(
                execution_mode="pipeline",
                dataflow_group_id="dfg_test",
                source_config={"type": "table", "table": "poc.bronze_x.product"},
                target_configs=[
                    {
                        "target_id": "primary",
                        "type": "table",
                        "table": "poc.bronze_x.replica",
                        "append_target_table": "poc.bronze_x.other_cdc",
                    }
                ],
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


# ---------------------------------------------------------------------------
# observability[] -- telemetry destinations for the DLT observability engine, validated
# alongside every other top-level flow array from the same onboarding spec (no separate
# observability config file -- see docs/25_dlt_observability_module.md).
# ---------------------------------------------------------------------------


def _base_observability_destination(**overrides):
    destination = {
        "id": "dest_test",
        "enabled": True,
        "type": "DATABRICKS_VOLUME",
        "destination_config": {"volume_path": "/Volumes/poc/observability/logs/"},
    }
    destination.update(overrides)
    return destination


def test_observability_alone_does_not_satisfy_non_empty_spec_check():
    """observability[] is deliberately NOT one of the "at least one flow array" options --
    telemetry with nothing to observe is meaningless."""
    spec = {"dataflow_group_id": "dfg_test", "observability": [_base_observability_destination()]}
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("At least one of" in e for e in errors)


def test_valid_observability_destination_alongside_a_real_flow_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination()],
    }
    _, _, _, observability_destinations, errors = validate_spec(None, spec)
    assert errors == []
    assert len(observability_destinations) == 1


def test_observability_not_a_list_is_reported():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": {"not": "a list"},
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("observability: expected a list" in e for e in errors)


def test_observability_destination_missing_id_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(id=None)],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("observability[0].id: is required" in e for e in errors)


def test_observability_duplicate_destination_id_is_reported():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(id="dup"),
            _base_observability_destination(id="dup"),
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("observability[1].id" in e and "already used by observability[0]" in e for e in errors)


def test_observability_destination_invalid_type_reports_allowed_values():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(type="CARRIER_PIGEON")],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("observability[0].type" in e and "CARRIER_PIGEON" in e for e in errors)


def test_observability_volume_destination_missing_volume_path_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(destination_config={})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("destination_config.volume_path: is required" in e for e in errors)


def test_observability_volume_destination_config_must_be_under_volumes():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(destination_config={"volume_path": "/tmp/not-a-volume/"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("must start with '/Volumes/'" in e for e in errors)


def test_observability_otlp_destination_missing_endpoint_is_required_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(type="OTLP_CONSUMER", destination_config={})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("destination_config.endpoint: is required" in e for e in errors)


def test_observability_otlp_destination_endpoint_must_be_http_url():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(type="OTLP_CONSUMER", destination_config={"endpoint": "ftp://not-http.example.com"})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("must be a full http:// or https:// URL" in e for e in errors)


def test_observability_invalid_compression_reports_allowed_values():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(destination_config={"volume_path": "/Volumes/poc/x/", "compression": "brotli"})
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("destination_config.compression" in e and "brotli" in e for e in errors)


def test_observability_bearer_token_auth_requires_token_credential():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(
                type="OTLP_CONSUMER",
                destination_config={"endpoint": "https://example.com"},
                auth={"type": "BEARER_TOKEN", "credentials": {}},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("auth.credentials.token: is required" in e for e in errors)


def test_observability_literal_credential_value_is_rejected():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(
                type="OTLP_CONSUMER",
                destination_config={"endpoint": "https://example.com"},
                auth={"type": "BEARER_TOKEN", "credentials": {"token": "sk-literal-secret-value"}},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("literal secret values are never allowed here" in e for e in errors)


def test_observability_env_credential_reference_passes():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(
                type="OTLP_CONSUMER",
                destination_config={"endpoint": "https://example.com"},
                auth={"type": "BEARER_TOKEN", "credentials": {"token": "env:MY_TOKEN"}},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_observability_secret_credential_reference_passes():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(
                type="OTLP_CONSUMER",
                destination_config={"endpoint": "https://example.com"},
                auth={"type": "API_KEY", "credentials": {"header_name": "X-Api-Key", "api_key": "secret:my_scope:my_key"}},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_observability_basic_auth_requires_username_and_password():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(
                type="OTLP_CONSUMER",
                destination_config={"endpoint": "https://example.com"},
                auth={"type": "BASIC_AUTH", "credentials": {"username": "env:U"}},
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("auth.credentials.password: is required" in e for e in errors)


def test_observability_invalid_auth_type_reports_allowed_values():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [
            _base_observability_destination(
                type="OTLP_CONSUMER", destination_config={"endpoint": "https://example.com"}, auth={"type": "CARRIER_PIGEON"}
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("observability[0].auth.type" in e and "CARRIER_PIGEON" in e for e in errors)


def test_observability_retry_max_attempts_must_be_at_least_one():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(retry={"max_attempts": 0})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("retry.max_attempts: must be >= 1" in e for e in errors)


def test_observability_retry_backoff_multiplier_must_exceed_one():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(retry={"backoff_multiplier": 1})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("retry.backoff_multiplier: must be a number > 1" in e for e in errors)


def test_observability_timeout_ms_must_be_at_least_one():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(timeout_ms=0)],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("observability[0].timeout_ms: must be >= 1" in e for e in errors)


def test_observability_missing_key_does_not_raise_and_defaults_to_empty():
    spec = {"dataflow_group_id": "dfg_test", "ingestion_flows": [_base_ingestion_flow()]}
    _, _, _, observability_destinations, errors = validate_spec(None, spec)
    assert errors == []
    assert observability_destinations == []


# --- ${param} substitution for path-bearing fields (source_config/target_config) ---


def test_ingestion_source_config_undefined_path_parameter_is_reported():
    spec = {
        "dataflow_group_id": "dfg_test",
        "pipeline_parameters": {},
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config={
                    "path": "/Volumes/poc/landing/${run_date}/",
                    "format": "csv",
                    "schema_location": "/Volumes/poc/landing/_schemas/x/",
                }
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("source_config" in e and "run_date" in e for e in errors)


def test_ingestion_source_config_defined_path_parameter_has_no_errors():
    spec = {
        "dataflow_group_id": "dfg_test",
        "pipeline_parameters": {"run_date": "2026-08-29"},
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config={
                    "path": "/Volumes/poc/landing/${run_date}/",
                    "format": "csv",
                    "schema_location": "/Volumes/poc/landing/_schemas/x/",
                }
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_target_config_sink_path_undefined_parameter_is_reported():
    spec = {
        "dataflow_group_id": "dfg_test",
        "pipeline_parameters": {},
        "ingestion_flows": [
            _base_ingestion_flow(
                target_type="sink",
                target_config={
                    "cdc_load_strategy": "APPEND",
                    "sink_config": {"format": "delta", "path": "/Volumes/poc/egress/${region}/"},
                },
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("target_config" in e and "region" in e for e in errors)


def test_observability_volume_path_parameter_is_deliberately_out_of_scope():
    """observability_config.destination_config.volume_path is NOT wired into path-parameter
    substitution (see transformation/parameters.py's module docstring) -- a ${param} there is
    just inert, unresolved text, not a validation error, since no substitution runs against it
    at all."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "observability": [_base_observability_destination(destination_config={"volume_path": "/Volumes/poc/obs/${env}/"})],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert not any("run_date" in e or "${env}" in e or "undefined parameter" in e for e in errors)


# ---------------------------------------------------------------------------------------------
# source_plane (v1.7.3 Single-Read architectural mandate)
#
# Until v1.7.3 `source_plane` was listed in ALLOWED_ROOT_KEYS and then never inspected again --
# the block was accepted verbatim, so a typo'd key or an unrecognised materialize value onboarded
# cleanly and silently fell back to the engine default. These tests pin its first validator.
# ---------------------------------------------------------------------------------------------

MATERIALIZE_NEVER_MESSAGE = (
    "materialize='never' is deprecated and prohibited under the Single-Read architectural "
    "mandate. Remove this setting to default to 'always', ensuring base tables are read once "
    "and reused via dlt.read()."
)


def _spec_with_source_plane(source_plane):
    return {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow()],
        "source_plane": source_plane,
    }


def test_source_plane_materialize_never_is_rejected_with_the_exact_message():
    """The removed VALUE is reported with its verbatim migration message, not the generic
    "not one of [...]" list -- an author who wrote 'never' needs to know it was deliberately
    withdrawn and what replaces it, not to think they made a typo.
    """
    _, _, _, _, errors = validate_spec(None, _spec_with_source_plane({"materialize": "never"}))
    assert f"source_plane.materialize: {MATERIALIZE_NEVER_MESSAGE}" in errors


def test_source_plane_materialize_never_is_rejected_alongside_other_keys():
    """Presence of the prohibited value is the trigger regardless of what else the block sets --
    catalog/schema being present must not mask it.
    """
    spec = _spec_with_source_plane({"materialize": "never", "catalog": "c", "schema": "s"})
    _, _, _, _, errors = validate_spec(None, spec)
    assert any(MATERIALIZE_NEVER_MESSAGE in e for e in errors)


def test_source_plane_materialize_always_and_auto_are_accepted():
    """'always' is the new default and 'auto' was never prohibited -- neither may be rejected.

    This is the regression guard for the reject_removed_keys() trap: that helper fires on KEY
    PRESENCE, so wiring `materialize` through it would reject these two legal values as well.
    """
    for value in ("always", "auto"):
        _, _, _, _, errors = validate_spec(None, _spec_with_source_plane({"materialize": value}))
        assert not any("materialize" in e for e in errors), (value, errors)


def test_source_plane_unknown_materialize_value_reports_allowed_values():
    _, _, _, _, errors = validate_spec(None, _spec_with_source_plane({"materialize": "sometimes"}))
    assert any("source_plane.materialize" in e and "always" in e and "auto" in e for e in errors)


def test_source_plane_unknown_key_is_rejected():
    """additionalProperties is false for this block in the JSON schema; the validator must agree."""
    _, _, _, _, errors = validate_spec(None, _spec_with_source_plane({"materialise": "always"}))
    assert any("source_plane.materialise" in e and "not a recognised attribute" in e for e in errors)


def test_source_plane_absent_or_empty_is_valid():
    """The block is optional, and an empty one simply takes the defaults."""
    for block in ({}, None):
        spec = _spec_with_source_plane(block)
        if block is None:
            del spec["source_plane"]
        _, _, _, _, errors = validate_spec(None, spec)
        assert errors == [], (block, errors)


# --- v1.7.4: pgp_symmetric + member_format (added for UC6) --------------------------------
#
# Both keys are new spec surface, so both must be simultaneously (a) accepted by the Python
# validator and (b) present in the JSON schema, whose additionalProperties:false is what
# actually rejects unknown keys since v1.7.2. A test that only exercised validate_spec would
# pass while real onboarding still rejected the spec.


def _uc6_passphrase_secret():
    return {"secret_catalog": "flowx", "secret_schema": "config", "secret_key": "pgpkey"}


def test_pre_extraction_decryption_pgp_symmetric_with_passphrase_has_no_errors():
    """UC6's Environment Agency feed: gzip inside a symmetric-PGP envelope."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config=_zip_handling_source_config(
                    zip_file_pattern="EE_*-REQUEST_*.csv.gz.gpg",
                    member_format="gzip",
                    pre_extraction_decryption={
                        "type": "pgp_symmetric",
                        "passphrase_secret": _uc6_passphrase_secret(),
                    },
                )
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert errors == []


def test_pgp_symmetric_without_passphrase_secret_reports_error():
    """The shared passphrase IS the key -- unlike 'pgp', it cannot be optional."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config=_zip_handling_source_config(pre_extraction_decryption={"type": "pgp_symmetric"})
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("passphrase_secret" in error for error in errors), errors


def test_pgp_symmetric_with_a_private_key_reports_error():
    """A passphrase-encrypted OpenPGP message has no recipient keypair -- naming one means the
    author picked the wrong type, and silently ignoring it would decrypt nothing."""
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config=_zip_handling_source_config(
                    pre_extraction_decryption={
                        "type": "pgp_symmetric",
                        "passphrase_secret": _uc6_passphrase_secret(),
                        "private_key_secret": _uc6_passphrase_secret(),
                    }
                )
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("private_key_secret" in error for error in errors), errors


def test_gzip_member_format_with_a_zip_password_reports_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [
            _base_ingestion_flow(
                source_config=_zip_handling_source_config(
                    member_format="gzip",
                    pre_extraction_decryption={"secret_passphrase": _uc6_passphrase_secret()},
                )
            )
        ],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("secret_passphrase" in error for error in errors), errors


def test_unknown_member_format_reports_error():
    spec = {
        "dataflow_group_id": "dfg_test",
        "ingestion_flows": [_base_ingestion_flow(source_config=_zip_handling_source_config(member_format="tar"))],
    }
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("member_format" in error for error in errors), errors


def test_new_v1_7_4_keys_are_present_in_the_json_schema():
    """Guard the half of the contract validate_spec cannot see: since v1.7.2 the JSON schema's
    additionalProperties:false is what rejects unknown keys at onboarding, so a key accepted
    here but missing there is still rejected in production."""
    import json
    from pathlib import Path

    schema_path = Path(__file__).resolve().parents[2] / "onboarding_templates" / "onboarding_spec.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    decryption_types = schema["$defs"]["preExtractionDecryption"]["properties"]["type"]["enum"]
    assert "pgp_symmetric" in decryption_types, decryption_types

    member_formats = schema["$defs"]["sourceZipHandling"]["properties"]["member_format"]["enum"]
    assert set(member_formats) == {"zip", "gzip"}, member_formats
