"""Integration test for governance/tags.py -- the v2 tags-only governance model.

Replaces test_governance_idempotency.py (deleted -- governance/idempotency.py and its
`governance_applied_log` ledger were removed entirely, see decision 9 in the v2 redesign
plan: tag DDL is naturally idempotent, so no application ledger is needed). Proves, against
a real Delta table:

* multiple tags per column and multiple tags per table apply in one statement each,
* re-applying an identical tag set is a no-op (still correct afterward, no error),
* applying a changed value for an existing tag key overwrites it,
* a failure (unsafe/unknown column) raises AbacApplicationError naming what failed, without
  preventing every other tag in the same call from being attempted (best-effort + collected
  errors).

The live-enforced ``mask`` tag-policy behavior itself (that this workspace actually masks a
column tagged ``mask=PII``) is proven exhaustively in
notebooks/07_verification/07_verify_crypto_abac_exhaustive.py, which runs inside a real DLT
pipeline context; this test only proves governance/tags.py's own DDL-application mechanics.
"""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import AbacApplicationError
from NextGen_Metadata_Framework.lakeflow_framework.governance.tags import apply_governance_tags

CATALOG = "poc"
SCHEMA = "silver_test"
TABLE = "governance_tags_probe"
QUALIFIED_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"


@pytest.fixture()
def probe_table(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(
        f"CREATE OR REPLACE TABLE {QUALIFIED_TABLE} (customer_id STRING, ssn STRING, region STRING) USING DELTA"
    )
    yield QUALIFIED_TABLE
    spark.sql(f"DROP TABLE IF EXISTS {QUALIFIED_TABLE}")


def _column_tags(spark, column: str):
    rows = (
        spark.table(f"{CATALOG}.information_schema.column_tags")
        .filter(f"schema_name = '{SCHEMA}' AND table_name = '{TABLE}' AND column_name = '{column}'")
        .collect()
    )
    return {row["tag_name"]: row["tag_value"] for row in rows}


def _table_tags(spark):
    rows = (
        spark.table(f"{CATALOG}.information_schema.table_tags")
        .filter(f"schema_name = '{SCHEMA}' AND table_name = '{TABLE}'")
        .collect()
    )
    return {row["tag_name"]: row["tag_value"] for row in rows}


def test_multiple_column_and_table_tags_applied_in_one_pass(spark, probe_table):
    apply_governance_tags(
        spark,
        CATALOG,
        SCHEMA,
        TABLE,
        {
            "column_tags": [{"column": "ssn", "tags": {"mask": "PII", "pii_type": "SSN"}}],
            "table_tags": {"domain": "customer_ops", "owner": "data_platform"},
        },
    )
    ssn_tags = _column_tags(spark, "ssn")
    assert ssn_tags.get("mask") == "PII"
    assert ssn_tags.get("pii_type") == "SSN"

    table_tags = _table_tags(spark)
    assert table_tags.get("domain") == "customer_ops"
    assert table_tags.get("owner") == "data_platform"


def test_multiple_columns_each_get_their_own_tags(spark, probe_table):
    apply_governance_tags(
        spark,
        CATALOG,
        SCHEMA,
        TABLE,
        {
            "column_tags": [
                {"column": "ssn", "tags": {"mask": "PII"}},
                {"column": "region", "tags": {"classification": "restricted"}},
            ]
        },
    )
    assert _column_tags(spark, "ssn").get("mask") == "PII"
    assert _column_tags(spark, "region").get("classification") == "restricted"


def test_reapplying_identical_tags_is_idempotent_no_error(spark, probe_table):
    tags = {"column_tags": [{"column": "ssn", "tags": {"mask": "PII"}}], "table_tags": {"domain": "customer_ops"}}
    apply_governance_tags(spark, CATALOG, SCHEMA, TABLE, tags)
    apply_governance_tags(spark, CATALOG, SCHEMA, TABLE, tags)  # must not raise

    assert _column_tags(spark, "ssn").get("mask") == "PII"
    assert _table_tags(spark).get("domain") == "customer_ops"


def test_reapplying_with_changed_value_overwrites_the_tag(spark, probe_table):
    apply_governance_tags(spark, CATALOG, SCHEMA, TABLE, {"table_tags": {"domain": "customer_ops"}})
    apply_governance_tags(spark, CATALOG, SCHEMA, TABLE, {"table_tags": {"domain": "finance"}})

    assert _table_tags(spark).get("domain") == "finance"


def test_unknown_column_raises_abac_application_error_naming_the_failure(spark, probe_table):
    with pytest.raises(AbacApplicationError, match="does_not_exist"):
        apply_governance_tags(
            spark, CATALOG, SCHEMA, TABLE, {"column_tags": [{"column": "does_not_exist", "tags": {"mask": "PII"}}]}
        )


def test_one_failing_column_does_not_block_other_tags_in_the_same_call(spark, probe_table):
    with pytest.raises(AbacApplicationError):
        apply_governance_tags(
            spark,
            CATALOG,
            SCHEMA,
            TABLE,
            {
                "column_tags": [
                    {"column": "does_not_exist", "tags": {"mask": "PII"}},
                    {"column": "ssn", "tags": {"mask": "PII"}},
                ]
            },
        )
    # The failing column raised, but the valid one still applied (best-effort semantics).
    assert _column_tags(spark, "ssn").get("mask") == "PII"
