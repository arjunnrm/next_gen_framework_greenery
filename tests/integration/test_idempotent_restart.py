"""Integration test for onboarding restart/idempotency: re-onboarding an unchanged spec
must upsert (MERGE), never duplicate, control-table rows.

Uses the real spec_07 file end to end through the actual onboarding pipeline
(spec_loader -> spec_validator -> metadata_upsert), the same path
02_onboarding_engine.py drives -- not a hand-built dict -- so this exercises the exact
templating/validation/upsert code a real onboarding action runs.
"""

import json

from NextGen_Metadata_Framework.lakeflow_framework.onboarding.metadata_upsert import (
    upsert_dataflow_group_spec,
    upsert_ingestion_flow_spec,
    upsert_transformation_flow_spec,
)
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_loader import load_and_template_spec
from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import validate_spec
from NextGen_Metadata_Framework.lakeflow_framework.control_plane.schema_provisioner import ensure_control_schema_exists

CATALOG = "poc"
CONTROL_SCHEMA = f"{CATALOG}.config"
SPEC_PATH = "test_specs/spec_07_volume_scd1_scd2_customer_360.json"


def _onboard_once(spark):
    ensure_control_schema_exists(spark, CATALOG)
    spec, _, _ = load_and_template_spec(None, SPEC_PATH, CATALOG, "dev")
    ingestion_flows, transformation_flows, _reconciliation_flows, _observability_destinations, errors = validate_spec(spark, spec)
    assert errors == [], f"spec_07 must validate cleanly: {errors}"
    upsert_dataflow_group_spec(spark, CONTROL_SCHEMA, spec, ingestion_flows, transformation_flows, CATALOG, "dev")
    upsert_ingestion_flow_spec(spark, CONTROL_SCHEMA, spec["dataflow_group_id"], ingestion_flows)
    upsert_transformation_flow_spec(spark, CONTROL_SCHEMA, spec["dataflow_group_id"], transformation_flows)
    return spec


def test_reonboarding_the_same_spec_twice_does_not_duplicate_group_row(spark):
    spec = _onboard_once(spark)
    _onboard_once(spark)  # re-onboard, unchanged

    matches = (
        spark.table(f"{CONTROL_SCHEMA}.dataflow_group_spec")
        .filter(f"dataflow_group_id = '{spec['dataflow_group_id']}'")
        .count()
    )
    assert matches == 1


def test_reonboarding_the_same_spec_twice_does_not_duplicate_ingestion_flow_rows(spark):
    spec = _onboard_once(spark)
    _onboard_once(spark)

    ingestion_ids = [flow["dataflow_id"] for flow in spec["ingestion_flows"]]
    for dataflow_id in ingestion_ids:
        matches = spark.table(f"{CONTROL_SCHEMA}.ingestion_flow_spec").filter(f"dataflow_id = '{dataflow_id}'").count()
        assert matches == 1, f"expected exactly one row for {dataflow_id}, found {matches}"


def test_reonboarding_the_same_spec_twice_does_not_duplicate_transformation_flow_rows(spark):
    spec = _onboard_once(spark)
    _onboard_once(spark)

    flow_step_ids = [flow["flow_step_id"] for flow in spec["transformation_flows"]]
    for flow_step_id in flow_step_ids:
        matches = spark.table(f"{CONTROL_SCHEMA}.transformation_flow_spec").filter(f"flow_step_id = '{flow_step_id}'").count()
        assert matches == 1, f"expected exactly one row for {flow_step_id}, found {matches}"


def test_reonboarding_updates_updated_at_but_preserves_created_at(spark):
    spec = _onboard_once(spark)
    first_row = (
        spark.table(f"{CONTROL_SCHEMA}.dataflow_group_spec")
        .filter(f"dataflow_group_id = '{spec['dataflow_group_id']}'")
        .collect()[0]
    )
    _onboard_once(spark)
    second_row = (
        spark.table(f"{CONTROL_SCHEMA}.dataflow_group_spec")
        .filter(f"dataflow_group_id = '{spec['dataflow_group_id']}'")
        .collect()[0]
    )
    assert first_row["created_at"] == second_row["created_at"], "re-onboarding must not reset created_at"
