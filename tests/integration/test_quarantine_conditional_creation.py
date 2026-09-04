"""Post-deployment verification for the quarantine creation/non-creation scenario (spec_27).

Run ``databricks bundle run quarantine_creation_test_job --target dev`` before running these.

Proves both halves of a real bug fix in
``src/flowx/lakeflow_framework/dq/quarantine.py::register_main_and_quarantine_tables``:
the quarantine sibling table is only ever registered when at least one ``dq_config.rules[]``
entry has ``action: "quarantine"`` -- configuring ``dq_config.quarantine_table`` alone (a name,
with no rule actually using that action) must produce **no** quarantine table at all. Before
this fix, any configured ``quarantine_table`` name unconditionally created an always-empty
table, wasting a resource for every flow that happened to name one without meaning to use it.

* Flow A (``df_orders_with_quarantine``): has a real ``action: "quarantine"`` rule
  (``order_amount >= 0``), violated by 2 of its 10 seeded rows (``-25.00``, ``-5.75``) -- its
  quarantine table must exist, and hold exactly those 2 rows.
* Flow B (``df_orders_without_quarantine``): configures the *same shape* ``quarantine_table``
  name, but every rule uses ``action: "drop"``/``"warn"`` only -- its quarantine table must not
  exist as a table at all, even though a name was configured for it.
"""

CATALOG = "poc"
SCHEMA = f"{CATALOG}.bronze_quarantine_test"


def test_flow_a_quarantine_table_exists_and_has_exactly_the_violating_rows(spark):
    df = spark.table(f"{SCHEMA}.orders_with_quarantine_quarantine")
    rows = df.collect()
    assert len(rows) == 2, f"expected exactly 2 quarantined rows, got {len(rows)}"
    order_ids = {row["order_id"] for row in rows}
    assert order_ids == {"Q003", "Q006"}
    for row in rows:
        assert "dq_order_amount_non_negative" in row["__framework_dq_failed_rule_ids"]
        assert row["__framework_quarantine_validated_at"] is not None
        assert row["__framework_pipeline_run_id"] is not None
        assert row["__framework_record_id"] in order_ids


def test_flow_a_main_table_excludes_the_quarantined_rows(spark):
    df = spark.table(f"{SCHEMA}.orders_with_quarantine")
    order_ids = {row["order_id"] for row in df.collect()}
    assert order_ids == {"Q001", "Q002", "Q004", "Q005", "Q007", "Q008", "Q009", "Q010"}
    assert "Q003" not in order_ids
    assert "Q006" not in order_ids


def test_flow_a_main_table_has_no_leftover_quarantine_process_columns(spark):
    df = spark.table(f"{SCHEMA}.orders_with_quarantine")
    for leaked_column in ("__framework_dq_quarantine_flag", "__framework_dq_failed_rule_ids", "__framework_dq_failure_reasons"):
        assert leaked_column not in df.columns


def test_flow_b_quarantine_table_was_never_created(table_exists):
    """The core assertion this whole scenario exists to prove -- see module docstring. Uses the
    `table_exists` fixture (a plain Unity Catalog REST call), not `spark.table(...)`: this
    project's shared pytest-session `spark` fixture is confirmed live to sometimes return a
    stale "exists" answer for a table that's actually gone (see that fixture's docstring in
    tests/conftest.py), which would silently defeat exactly this kind of negative assertion."""
    assert not table_exists(f"{SCHEMA}.orders_without_quarantine_quarantine"), (
        "dq_config.quarantine_table was configured for Flow B, but no rule uses action:'quarantine' -- "
        "no quarantine table should ever have been created. See "
        "dq/quarantine.py::register_main_and_quarantine_tables."
    )


def test_flow_b_main_table_still_has_all_rows_including_the_rule_violating_ones(spark):
    """Flow B's order_amount >= 0 rule uses action:"warn", not "quarantine" -- a warn
    expectation logs a warning but never drops or reroutes the row, so all 10 seeded rows
    (including the 2 that violate the rule) must still be present in the main table."""
    df = spark.table(f"{SCHEMA}.orders_without_quarantine")
    order_ids = {row["order_id"] for row in df.collect()}
    assert order_ids == {f"N{i:03d}" for i in range(1, 11)}
