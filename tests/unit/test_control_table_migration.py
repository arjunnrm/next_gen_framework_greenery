"""Unit tests for the control-table column-migration path (v1.5.0).

Every statement in ``get_all_control_table_ddls`` is ``CREATE TABLE IF NOT EXISTS``, which is a
no-op against an already-provisioned table -- so a column added to one of those CREATE statements
reaches BRAND-NEW installations only. Verified live on 2026-08-31: ``flowx.config.
reconciliation_flow_spec`` carried none of ``execution_mode`` / ``publish_schema`` /
``dq_config_json``, so no reconciliation flow could be onboarded in pipeline mode on that
workspace at all -- the first write referencing the column failed with ``UNRESOLVED_COLUMN``.

``ADDITIVE_CONTROL_TABLE_COLUMNS`` + ``get_add_column_ddl`` (ddl_definitions) and
``ensure_control_table_columns`` (schema_provisioner) close that gap. These tests are pure Python:
no Spark session, no workspace. The spark stub follows the hand-written duck-typed idiom of
``tests/unit/test_group_metadata_loader.py`` (``_StubSpark``) rather than ``unittest.mock``,
because the real ``spark`` fixture in ``tests/conftest.py`` opens a live Databricks Connect
session that this control-flow test has no need to pay for.

Covers:
  * the exact ``ALTER TABLE ... ADD COLUMNS (...)`` text, and the deliberate ABSENCE of
    ``IF NOT EXISTS`` (Databricks SQL rejects it there with ``PARSE_SYNTAX_ERROR``);
  * single-quote escaping inside a comment;
  * no brace character in any generated statement (the concatenation-vs-f-string trap);
  * ALTER issued only for genuinely missing columns, and nothing at all when all are present;
  * an unreadable table is skipped with a warning, not raised;
  * the concurrent-add race is swallowed, any other failure raises ``FrameworkConfigError``;
  * CONSISTENCY GUARD: every migrated column also exists in that table's CREATE DDL, so a
    migrated workspace and a fresh install converge on the same schema;
  * REVERSE GUARD: the four v1.5.0 ``reconciliation_flow_spec`` columns are listed for migration.

    Read the limit of this guard before relying on it. It is parametrised over a HARD-CODED list
    of the four known v1.5.0 column names, so it proves those four did not fall off the migration
    list -- it does NOT detect a *future* column added to a CREATE DDL and forgotten here, because
    nothing tells the test that column exists. Catching that case would need the CREATE DDL parsed
    as the source of truth and every column in it required to be either pre-v1.0 or on the
    migration list, which is a bigger change than this file. Until then, "add the column to
    ADDITIVE_CONTROL_TABLE_COLUMNS too" remains a review-time responsibility, and the
    ``ADDITIVE_CONTROL_TABLE_COLUMNS`` docstring in ``ddl_definitions.py`` states it.
"""

import re

import pytest

from flowx.lakeflow_framework.control_plane.ddl_definitions import (
    ADDITIVE_CONTROL_TABLE_COLUMNS,
    get_add_column_ddl,
    get_all_control_table_ddls,
)
from flowx.lakeflow_framework.control_plane.schema_provisioner import (
    ensure_control_table_columns,
)
from flowx.lakeflow_framework.exceptions import FrameworkConfigError

CONTROL_CATALOG = "flowx"
CONTROL_SCHEMA = f"{CONTROL_CATALOG}.config"


# --------------------------------------------------------------------------------------------
# Stubs
# --------------------------------------------------------------------------------------------


class _StubTable:
    """Duck-typed stand-in for a ``DataFrame``: only ``.columns`` is ever read."""

    def __init__(self, columns):
        self.columns = list(columns)


class _StubSpark:
    """Minimal ``SparkSession`` stand-in.

    ``tables`` maps fully-qualified name -> column list. A name absent from the map makes
    ``.table()`` raise, simulating a table that cannot be read (missing, or no permission).
    ``sql()`` records every statement; ``sql_error`` (a callable ``statement -> Exception|None``)
    lets a test make one specific ALTER fail.
    """

    def __init__(self, tables, sql_error=None):
        self._tables = dict(tables)
        self._sql_error = sql_error
        self.statements = []

    def table(self, name):
        if name not in self._tables:
            raise Exception(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{name}` cannot be found")
        return _StubTable(self._tables[name])

    def sql(self, statement):
        self.statements.append(statement)
        if self._sql_error is not None:
            error = self._sql_error(statement)
            if error is not None:
                raise error
        return None


def _all_tables_with(columns_by_table):
    """Build a ``_StubSpark`` tables map covering every migrated table."""
    return {
        f"{CONTROL_SCHEMA}.{table}": columns_by_table.get(table, [])
        for table in ADDITIVE_CONTROL_TABLE_COLUMNS
    }


def _fully_migrated_tables():
    """Every migrated table already carrying every one of its additive columns."""
    return {
        f"{CONTROL_SCHEMA}.{table}": ["id"] + [column for column, _type, _comment in columns]
        for table, columns in ADDITIVE_CONTROL_TABLE_COLUMNS.items()
    }


def _all_additive_entries():
    return [
        (table, column, sql_type, comment)
        for table, columns in ADDITIVE_CONTROL_TABLE_COLUMNS.items()
        for column, sql_type, comment in columns
    ]


# --------------------------------------------------------------------------------------------
# (a) get_add_column_ddl text
# --------------------------------------------------------------------------------------------


def test_add_column_ddl_is_exactly_the_expected_statement():
    statement = get_add_column_ddl(CONTROL_SCHEMA, "reconciliation_flow_spec", "execution_mode", "STRING", "A comment.")
    assert statement == (
        "ALTER TABLE flowx.config.reconciliation_flow_spec "
        "ADD COLUMNS (execution_mode STRING COMMENT 'A comment.')"
    )


def test_add_column_ddl_never_says_if_not_exists():
    """Databricks SQL rejects ``ADD COLUMNS IF NOT EXISTS`` with ``PARSE_SYNTAX_ERROR``.

    Verified live on 2026-08-31 against DBR serverless. The clause is accepted for ``ADD
    PARTITION`` and for ``CREATE TABLE``, which is exactly why someone keeps wanting to add it
    here. Idempotence is the caller's job (``ensure_control_table_columns`` skips present columns
    and swallows the concurrent-add race), not the statement's.
    """
    for table, column, sql_type, comment in _all_additive_entries():
        statement = get_add_column_ddl(CONTROL_SCHEMA, table, column, sql_type, comment)
        assert "IF NOT EXISTS" not in statement.upper(), f"{table}.{column} must not use IF NOT EXISTS"
        assert statement.upper().startswith(f"ALTER TABLE {CONTROL_SCHEMA}.{table} ADD COLUMNS (".upper())


# --------------------------------------------------------------------------------------------
# (b) quote escaping
# --------------------------------------------------------------------------------------------


def test_apostrophe_in_comment_is_doubled():
    statement = get_add_column_ddl(
        CONTROL_SCHEMA, "dataflow_group_spec", "publish_schema", "STRING",
        "Defaults to the hosting pipeline's own schema when NULL.",
    )
    assert "pipeline''s own schema" in statement
    # The comment literal must still be a single balanced SQL string: the only unescaped quotes
    # are the opening one after COMMENT and the closing one before the paren.
    assert statement.endswith("when NULL.')")
    assert statement.count("'") % 2 == 0


def test_multiple_apostrophes_are_each_doubled():
    statement = get_add_column_ddl(CONTROL_SCHEMA, "t", "c", "STRING", "it's the flow's own comment")
    assert "it''s the flow''s own comment" in statement


# --------------------------------------------------------------------------------------------
# (c) no braces anywhere -- the concatenation-vs-f-string trap
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "table,column,sql_type,comment",
    _all_additive_entries(),
    ids=[f"{t}.{c}" for t, c, _s, _cm in _all_additive_entries()],
)
def test_no_brace_characters_in_generated_statement(table, column, sql_type, comment):
    """``get_add_column_ddl`` concatenates rather than f-string-interpolates.

    A brace in one of these comments therefore stays a literal brace -- it is NOT doubled the way
    it must be in the CREATE DDL f-strings in the same module. Mixing the two conventions is how
    ``{{ }}`` ends up in a shipped COMMENT (or an unescaped ``{...}`` breaks every CREATE DDL), so
    the migration comments are simply required to be brace-free.
    """
    statement = get_add_column_ddl(CONTROL_SCHEMA, table, column, sql_type, comment)
    assert "{" not in statement and "}" not in statement, (
        f"{table}.{column}: ADDITIVE_CONTROL_TABLE_COLUMNS comments must contain no braces -- "
        "they are concatenated into SQL, not f-string-interpolated."
    )


# --------------------------------------------------------------------------------------------
# (d) ensure_control_table_columns: only the missing ones, and idempotence
# --------------------------------------------------------------------------------------------


def test_alter_issued_only_for_missing_columns():
    recon_columns = ADDITIVE_CONTROL_TABLE_COLUMNS["reconciliation_flow_spec"]
    present = [name for name, _t, _c in recon_columns[:1]]
    spark = _StubSpark(_all_tables_with({"reconciliation_flow_spec": ["reconciliation_id"] + present}))

    ensure_control_table_columns(spark, CONTROL_CATALOG)

    expected_missing = [name for name, _t, _c in recon_columns[1:]]
    expected_missing += [
        f"{table}.{name}"
        for table, columns in ADDITIVE_CONTROL_TABLE_COLUMNS.items()
        if table != "reconciliation_flow_spec"
        for name, _t, _c in columns
    ]
    assert len(spark.statements) == len(expected_missing)
    for name, _t, _c in recon_columns[1:]:
        assert any(f"ADD COLUMNS ({name} " in stmt for stmt in spark.statements)
    # The already-present column is never re-added.
    assert not any(f"ADD COLUMNS ({present[0]} " in stmt for stmt in spark.statements)


def test_nothing_issued_when_every_column_is_present():
    spark = _StubSpark(_fully_migrated_tables())
    ensure_control_table_columns(spark, CONTROL_CATALOG)
    assert spark.statements == []


def test_migration_is_idempotent_across_two_runs():
    """Second run over a now-migrated table issues nothing -- provisioning runs on every onboard."""
    spark = _StubSpark(_all_tables_with({}))
    ensure_control_table_columns(spark, CONTROL_CATALOG)
    assert len(spark.statements) == len(_all_additive_entries())

    migrated = _StubSpark(_fully_migrated_tables())
    ensure_control_table_columns(migrated, CONTROL_CATALOG)
    assert migrated.statements == []


def test_existing_column_match_is_case_insensitive():
    """Spark reports column names in whatever case the table was created with."""
    recon_columns = ADDITIVE_CONTROL_TABLE_COLUMNS["reconciliation_flow_spec"]
    upper = [name.upper() for name, _t, _c in recon_columns]
    tables = _fully_migrated_tables()
    tables[f"{CONTROL_SCHEMA}.reconciliation_flow_spec"] = ["RECONCILIATION_ID"] + upper
    spark = _StubSpark(tables)

    ensure_control_table_columns(spark, CONTROL_CATALOG)

    assert spark.statements == []


# --------------------------------------------------------------------------------------------
# (e) unreadable table is skipped, not fatal
# --------------------------------------------------------------------------------------------


def test_unreadable_table_is_skipped_with_a_warning(caplog):
    """A table absent right after the CREATE statements ran means a permission problem that its
    own error already reported more precisely -- migration must not turn that into a second,
    less-informative failure, and must not abandon the other tables."""
    tables = _fully_migrated_tables()
    del tables[f"{CONTROL_SCHEMA}.reconciliation_flow_spec"]
    # Leave dataflow_group_spec unmigrated so we can prove the loop continued past the skip.
    tables[f"{CONTROL_SCHEMA}.dataflow_group_spec"] = ["dataflow_group_id"]
    spark = _StubSpark(tables)

    with caplog.at_level("WARNING"):
        ensure_control_table_columns(spark, CONTROL_CATALOG)

    assert any(
        "Skipping column migration" in record.message and record.levelname == "WARNING"
        for record in caplog.records
    )
    expected = len(ADDITIVE_CONTROL_TABLE_COLUMNS["dataflow_group_spec"])
    assert len(spark.statements) == expected
    assert all("dataflow_group_spec" in stmt for stmt in spark.statements)


# --------------------------------------------------------------------------------------------
# (f) concurrent-add race swallowed; every other failure raises
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("message", [
    "[FIELDS_ALREADY_EXIST] Cannot add column, because `execution_mode` already exists",
    "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view because it already exists",
    "Column already exists in the target table",
])
def test_concurrent_add_race_is_swallowed(message):
    spark = _StubSpark(_all_tables_with({}), sql_error=lambda _stmt: Exception(message))
    ensure_control_table_columns(spark, CONTROL_CATALOG)  # must not raise
    assert len(spark.statements) == len(_all_additive_entries())


@pytest.mark.parametrize("message", [
    "PERMISSION_DENIED: User does not have MODIFY on table `reconciliation_flow_spec`",
    "[DELTA_UNSUPPORTED_ALTER] Unsupported ALTER TABLE operation",
    "Connection reset by peer",
])
def test_other_failures_raise_framework_config_error(message):
    spark = _StubSpark(_all_tables_with({}), sql_error=lambda _stmt: Exception(message))
    with pytest.raises(FrameworkConfigError) as excinfo:
        ensure_control_table_columns(spark, CONTROL_CATALOG)
    assert "Failed to add column" in str(excinfo.value)


# --------------------------------------------------------------------------------------------
# (g) CONSISTENCY GUARD: migrated column must also exist in the CREATE DDL
# --------------------------------------------------------------------------------------------


def _create_ddl_by_table():
    ddls = {}
    for description, ddl in get_all_control_table_ddls(CONTROL_SCHEMA, {}):
        ddls[description.rsplit(".", 1)[-1]] = ddl
    return ddls


@pytest.mark.parametrize(
    "table,column,sql_type,comment",
    _all_additive_entries(),
    ids=[f"{t}.{c}" for t, c, _s, _cm in _all_additive_entries()],
)
def test_every_migrated_column_is_also_in_the_create_ddl(table, column, sql_type, comment):
    """Otherwise a migrated workspace and a fresh install diverge: the column exists on one and
    not the other, and which behaviour you get depends on when the catalog was provisioned."""
    ddls = _create_ddl_by_table()
    assert table in ddls, (
        f"{table} is listed in ADDITIVE_CONTROL_TABLE_COLUMNS but has no CREATE DDL in "
        "get_all_control_table_ddls."
    )
    declaration = re.search(rf"(?m)^\s*{re.escape(column)}\s+(\S+)", ddls[table])
    assert declaration is not None, (
        f"{table}.{column} is migrated onto existing workspaces but is NOT declared in that "
        "table's CREATE TABLE DDL -- a fresh install would never get it."
    )
    assert declaration.group(1).upper() == sql_type.upper(), (
        f"{table}.{column}: migration declares {sql_type} but the CREATE DDL declares "
        f"{declaration.group(1)}."
    )


# --------------------------------------------------------------------------------------------
# (h) REVERSE GUARD: v1.5.0 columns must be on the migration list
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("column", [
    "two_tier_verification",
    "execution_mode",
    "publish_schema",
    "dq_config_json",
])
def test_v150_reconciliation_columns_are_listed_for_migration(column):
    """These four post-v1.0 columns must reach ALREADY-provisioned workspaces.

    If a future column is added to ``get_reconciliation_flow_spec_ddl`` and NOT here, it exists
    only on catalogs provisioned after the change -- the exact defect observed live on
    2026-08-31. Extend this list whenever a column is added to that CREATE DDL.
    """
    listed = {name for name, _t, _c in ADDITIVE_CONTROL_TABLE_COLUMNS.get("reconciliation_flow_spec", [])}
    assert column in listed, (
        f"reconciliation_flow_spec.{column} is in the CREATE DDL but not in "
        "ADDITIVE_CONTROL_TABLE_COLUMNS -- existing workspaces will never get it."
    )
