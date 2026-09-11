"""Unit tests for cdc/comparison_columns.py -- pure Python, no Spark needed.

Explicit user-requested test matrix: empty/populated/overlapping/invalid columns_to_check
and columns_to_exclude combinations.
"""

from flowx.lakeflow_framework.cdc.comparison_columns import (
    FRAMEWORK_TECHNICAL_COLUMNS,
    resolve_comparison_columns,
)

ALL_COLUMNS = ["customer_id", "name", "email", "tier", "__framework_ingestion_timestamp_utc", "__framework_hash_key"]


def test_empty_columns_to_check_compares_all_applicable_columns():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=None)
    assert result == sorted(["name", "email", "tier"])


def test_populated_columns_to_check_scopes_to_exactly_those():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=["name", "tier"], columns_to_exclude=None)
    assert result == sorted(["name", "tier"])


def test_columns_to_exclude_removes_from_the_all_columns_base():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=["email"])
    assert result == sorted(["name", "tier"])


def test_columns_to_exclude_also_removes_from_a_populated_columns_to_check():
    """An author mistake -- naming a column in both columns_to_check and columns_to_exclude
    -- must not silently include it; exclusion always wins."""
    result = resolve_comparison_columns(
        ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=["name", "email"], columns_to_exclude=["email"]
    )
    assert result == ["name"]


def test_primary_keys_never_appear_in_comparison_columns():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id", "name"], columns_to_check=None, columns_to_exclude=None)
    assert "customer_id" not in result
    assert "name" not in result


def test_framework_technical_columns_never_appear_in_comparison_columns():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=None)
    assert not (set(result) & FRAMEWORK_TECHNICAL_COLUMNS)


def test_framework_technical_columns_excluded_even_if_explicitly_requested_in_columns_to_check():
    result = resolve_comparison_columns(
        ALL_COLUMNS, primary_keys=["customer_id"], columns_to_check=["__framework_hash_key", "name"], columns_to_exclude=None
    )
    assert result == ["name"]


def test_no_primary_keys_still_excludes_technical_columns_only():
    result = resolve_comparison_columns(ALL_COLUMNS, primary_keys=None, columns_to_check=None, columns_to_exclude=None)
    assert result == sorted(["customer_id", "name", "email", "tier"])


def test_result_is_sorted_and_deduplicated():
    result = resolve_comparison_columns(
        ["b", "a", "a"], primary_keys=None, columns_to_check=["a", "a", "b"], columns_to_exclude=None
    )
    assert result == ["a", "b"]


def test_all_columns_excluded_returns_empty_list():
    result = resolve_comparison_columns(["customer_id"], primary_keys=["customer_id"], columns_to_check=None, columns_to_exclude=None)
    assert result == []


# ---------------------------------------------------------------------------------------------
# columns_to_exclude has TWO jobs -- pinned 2026-09-11.
#
# For releases the repo contradicted itself about this attribute: cdc/comparison_columns.py and
# onboarding/spec_validator.py both claimed it was "comparison-only in v2" and "never drops it
# from the target table", while cdc/scd.py went on passing it to dlt.apply_changes's
# except_column_list at all three SCD call sites -- which DOES drop the column. docs/
# 00_master_reference_index.md and agent_skills/SKILL.md repeated the wrong claim.
#
# The code was right and the prose was wrong. Live inspection of
# bt_digital_poc.bronze.physical_device / customer / subscriber (2026-09-11) found
# sys_creation_date and sys_update_date genuinely ABSENT -- exactly the two columns those specs
# name in columns_to_exclude. The prose was corrected, not the behaviour, because
# except_column_list is the framework's ONLY "don't store this column at all" mechanism for a
# CDC target: data_standardization_sql is strictly add/replace (a mandatory AS <name> alias),
# column_normalization and schema_config only rename/cast/comment, and columns_to_check scopes
# comparison. Narrowing it would have deleted a capability with no replacement AND silently
# re-added columns to every already-materialized SCD target.
#
# No test asserted the storage half, which is why the contradiction survived. These do.
# ---------------------------------------------------------------------------------------------


def test_build_except_column_list_passes_columns_to_exclude_through():
    """The storage half of the contract. If this starts returning None, every SCD target
    silently GAINS the excluded columns on its next update -- and the only way to drop a
    column from a CDC target is gone."""
    from flowx.lakeflow_framework.cdc.scd import _build_except_column_list

    assert _build_except_column_list({"columns_to_exclude": ["batch_load_ts", "etl_run_id"]}) == [
        "batch_load_ts",
        "etl_run_id",
    ]


def test_build_except_column_list_is_none_when_unset_or_empty():
    """None means "keep every source column" -- apply_changes treats an empty list and None
    differently, so an empty/absent config must not become []."""
    from flowx.lakeflow_framework.cdc.scd import _build_except_column_list

    assert _build_except_column_list({}) is None
    assert _build_except_column_list({"columns_to_exclude": []}) is None
    assert _build_except_column_list({"columns_to_exclude": None}) is None


def test_comparison_and_storage_exclusion_agree_on_the_same_column_list():
    """The two jobs must not fight: a column named in columns_to_exclude is dropped from the
    target AND absent from the comparison basis. If it were dropped from storage but still in
    the hash basis, __framework_hash_value would reference a column the target does not have."""
    from flowx.lakeflow_framework.cdc.comparison_columns import resolve_comparison_columns
    from flowx.lakeflow_framework.cdc.scd import _build_except_column_list

    target_config = {"primary_keys": ["id"], "columns_to_exclude": ["batch_load_ts"]}
    all_columns = ["id", "name", "amount", "batch_load_ts", "__framework_hash_key"]

    dropped = _build_except_column_list(target_config)
    compared = resolve_comparison_columns(
        all_columns,
        primary_keys=target_config["primary_keys"],
        columns_to_check=None,
        columns_to_exclude=target_config["columns_to_exclude"],
    )

    assert "batch_load_ts" in dropped
    assert "batch_load_ts" not in compared
    # And a column that is neither a key nor excluded is compared but never dropped.
    assert "amount" in compared
    assert "amount" not in dropped


def test_no_module_claims_columns_to_exclude_is_comparison_only():
    """Prose regression guard. The 'comparison-only / never drops it from the target table'
    claim was wrong in four places at once and cost a full investigation to untangle. Fail
    loudly if it comes back rather than letting the repo describe a system it does not have."""
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    targets = [
        repo_root / "src" / "flowx" / "lakeflow_framework" / "cdc" / "comparison_columns.py",
        repo_root / "src" / "flowx" / "lakeflow_framework" / "onboarding" / "spec_validator.py",
        repo_root / "docs" / "00_master_reference_index.md",
        repo_root / "agent_skills" / "SKILL.md",
    ]
    # Phrases that assert the FALSE half of the contract. Matched case-insensitively on a
    # whitespace-collapsed copy so a reflow cannot smuggle one back in.
    banned = [
        "never drops it from the target table",
        "never drops them from the target table",
        "neither drops a column from the target table",
        "comparison vs. storage are fully decoupled",
        "is comparison-only now",
    ]
    offenders = []
    for path in targets:
        if not path.exists():
            continue
        text = " ".join(path.read_text(encoding="utf-8").split()).lower()
        for phrase in banned:
            if phrase in text:
                offenders.append(f"{path.name}: {phrase!r}")
    assert offenders == [], (
        "columns_to_exclude ALSO drops columns from the target table (cdc/scd.py passes it to "
        "apply_changes's except_column_list). These files claim otherwise: " + "; ".join(offenders)
    )


# ---------------------------------------------------------------------------------------------
# v0.0.6: an SCD2 flow publishes EXACTLY ONE dataset.
#
# Up to v0.0.5, register_scd2 also called register_scd2_reporting_view, which published
# <target>_current re-labelling __START_AT/__END_AT as valid_from/valid_to/is_current. It was
# removed because it was actively harmful, not merely redundant:
#
#   * it materialized as a MATERIALIZED_VIEW holding a full copy of EVERY row -- history
#     included -- despite a `_current` name promising only current ones. Verified live on
#     bt_digital_poc.bronze.customer_current: 40 rows, of which only 30 were current;
#   * so it doubled storage per SCD2 target and misled any consumer who trusted the name.
#
# The tracking columns CANNOT be renamed in place, which is why no replacement was added:
# dlt.apply_changes exposes no parameter for their names (18 params, none of them), and an SCD2
# target is create_streaming_table + apply_changes with no query body to project through --
# Lakeflow writes those columns itself. Databricks' own SCD2 guidance aliases them in a SELECT,
# never in storage. Deriving valid_from/valid_to in the staged view would be WORSE than the
# __-prefixed names: computed before apply_changes assigns versions, they would not track the
# real version boundaries -- authoritative-looking and wrong.
#
# These tests assert the removal is genuine (per AGENTS.md: a removal needs a test proving the
# removed thing is actually gone, not merely unused).
# ---------------------------------------------------------------------------------------------


def test_register_scd2_reporting_view_is_gone():
    """The removed function must not exist at all. A lingering definition would invite a future
    caller to re-add the duplicate dataset."""
    from flowx.lakeflow_framework.cdc import scd

    assert not hasattr(scd, "register_scd2_reporting_view"), (
        "register_scd2_reporting_view was removed in v0.0.6 -- an SCD2 flow publishes exactly "
        "one dataset (its target). Re-adding it reintroduces a full-copy MATERIALIZED_VIEW under "
        "a misleading `_current` name."
    )


def test_scd_module_registers_no_current_companion_dataset():
    """Source-level guard: no CODE in cdc/scd.py may construct a `<target>_current` name or an
    `is_current` column. Catches a re-introduction that renames the function but keeps the
    behaviour.

    Docstrings are stripped before matching -- the module docstring deliberately *explains* the
    removal and names the removed dataset, so a naive substring search over the whole file would
    flag the explanation itself."""
    import ast
    import inspect

    from flowx.lakeflow_framework.cdc import scd

    tree = ast.parse(inspect.getsource(scd))
    # Drop every docstring node, then unparse back to code-only source.
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    code_only = ast.unparse(tree)

    assert "_current" not in code_only, (
        "cdc/scd.py must not build a `<target>_current` dataset name -- the SCD2 reporting "
        "companion was removed in v0.0.6. Offending code:\n"
        + "\n".join(l for l in code_only.splitlines() if "_current" in l)
    )
    assert "is_current" not in code_only, (
        "cdc/scd.py must not add an `is_current` column -- that was the removed reporting "
        "companion's alias. Consumers derive it with `__END_AT IS NULL` in their own query."
    )


def test_register_scd2_calls_apply_changes_exactly_once_and_publishes_one_table():
    """Behavioural guard: one create_streaming_table + one apply_changes, and NO second dataset.

    Stubs dlt so this stays a pure-Python unit test (no Spark, no pipeline runtime -- see this
    module's other tests for why that matters offline)."""
    import sys
    import types
    from unittest import mock

    created, applied, decorated = [], [], []

    fake_dlt = types.SimpleNamespace(
        create_streaming_table=lambda name, **kw: created.append(name),
        apply_changes=lambda **kw: applied.append(kw),
        # If anything tries to register another dataset, record it and fail the assertion below.
        table=lambda **kw: (decorated.append(kw.get("name")) or (lambda fn: fn)),
        view=lambda **kw: (decorated.append(kw.get("name")) or (lambda fn: fn)),
    )

    with mock.patch.dict(sys.modules, {"dlt": fake_dlt}):
        import importlib

        from flowx.lakeflow_framework.cdc import scd as scd_module

        scd = importlib.reload(scd_module)
        try:
            scd.register_scd2(
                flow_id="df_test",
                source_view="v_staged",
                target_table="customer",
                target_catalog="cat",
                target_schema="bronze",
                target_config={"primary_keys": ["customer_id"], "sequence_by_column": "seq"},
                table_properties={},
            )
        finally:
            importlib.reload(scd_module)  # restore the real dlt-backed module for other tests

    assert created == ["cat.bronze.customer"], created
    assert len(applied) == 1, applied
    assert applied[0]["stored_as_scd_type"] == "2"
    assert decorated == [], (
        "register_scd2 registered an extra dataset (%r) -- an SCD2 flow must publish exactly one "
        "table, its target." % decorated
    )
