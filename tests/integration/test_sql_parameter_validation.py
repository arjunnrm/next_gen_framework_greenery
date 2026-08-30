"""Integration tests for spec_validator.py's SQL-syntax validation of ${param} placeholders.

Needs a real Spark session (``EXPLAIN`` runs against it) -- this is exactly the bug class
that shipped in this repo's own spec_04/spec_06 fixtures and was only caught by a real
pipeline run: validating the *raw* transformation_sql text let a redundantly-quoted
placeholder (``'${param}'``, which becomes unparseable ``''US''`` after substitution) pass
onboarding validation, only to fail at pipeline-deployment time instead.
"""

from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import validate_spec


def _transformation_spec(transformation_sql: str, pipeline_parameters: dict) -> dict:
    return {
        "dataflow_group_id": "dfg_test",
        "pipeline_parameters": pipeline_parameters,
        "transformation_flows": [
            {
                "flow_step_id": "ts_test",
                "dataflow_id": "df_test",
                "target_catalog": "poc",
                "target_schema": "silver_test",
                "target_table": "test_target",
                "target_type": "streaming_table",
                "source_inputs": [],
                "transformation_sql": transformation_sql,
                "target_config": {"cdc_load_strategy": "APPEND"},
                "dq_config": {},
                "governance_tags": {},
            }
        ],
    }


def test_correctly_unquoted_placeholder_validates_cleanly(spark):
    spec = _transformation_spec("SELECT 1 AS x WHERE 'a' = ${region}", {"region": "US"})
    _, _, _, _, errors = validate_spec(spark, spec)
    assert errors == []


def test_redundantly_quoted_string_placeholder_is_rejected(spark):
    """This is the exact bug that shipped in spec_04/spec_06: quoting a string
    placeholder yourself, on top of substitute_dynamic_parameters's own quoting, produces
    unparseable SQL. Validation must catch it at onboarding time."""
    spec = _transformation_spec("SELECT 1 AS x WHERE 'a' = '${region}'", {"region": "US"})
    _, _, _, _, errors = validate_spec(spark, spec)
    assert any("transformation_sql" in e and "failed to parse" in e for e in errors)


def test_undefined_placeholder_is_reported_at_validation_time():
    spec = _transformation_spec("SELECT 1 AS x WHERE 'a' = ${undefined_param}", {})
    _, _, _, _, errors = validate_spec(None, spec)
    assert any("undefined parameter" in e for e in errors)


def test_union_all_with_mismatched_column_count_is_rejected(spark):
    """Real bug found live (via test_specs/spec_26_transformation_union_all.json's own build):
    on this workspace's Spark Connect/serverless environment, EXPLAIN never raises an
    exception for a query-planning problem -- a failed plan just returns a DataFrame whose
    text starts with "Error occurred during query planning:" instead. The original
    exception-only validation never collected that DataFrame, so a UNION with mismatched
    column counts previously passed onboarding validation and only failed once actually
    deployed. See spec_validator.py::_validate_sql_syntax's own comment for the full story."""
    spec = _transformation_spec("SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS a", {})
    _, _, _, _, errors = validate_spec(spark, spec)
    assert any("transformation_sql" in e and "NUM_COLUMNS_MISMATCH" in e for e in errors)


def test_union_all_with_incompatible_column_types_is_rejected(spark):
    spec = _transformation_spec('SELECT named_struct("x", 1) AS a UNION ALL SELECT 1 AS a', {})
    _, _, _, _, errors = validate_spec(spark, spec)
    assert any("transformation_sql" in e and "INCOMPATIBLE_COLUMN_TYPE" in e for e in errors)


def test_valid_union_all_validates_cleanly(spark):
    spec = _transformation_spec("SELECT 1 AS a UNION ALL SELECT 2 AS a", {})
    _, _, _, _, errors = validate_spec(spark, spec)
    assert errors == []


def test_reference_to_a_not_yet_deployed_view_is_still_tolerated(spark):
    """The fix for the UNION-mismatch bug above must not regress this: transformation_sql
    legitimately references source_inputs[] view names that only exist once the pipeline
    actually runs, never at onboarding-validation time -- this must still validate cleanly,
    not be treated as a hard "Error occurred during query planning" failure."""
    spec = _transformation_spec("SELECT * FROM some_not_yet_deployed_view", {})
    _, _, _, _, errors = validate_spec(spark, spec)
    assert errors == []
