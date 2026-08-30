"""The snapshot strategies must feed apply_changes_from_snapshot a NAMED dataset, not a lambda.

Background -- two live Lakeflow failures on 2026-08-29 that together pin this design down.
``cdc/snapshot.py`` originally passed a Python lambda as ``apply_changes_from_snapshot(source=)``
and read its input with ``dlt.read(source_view)`` inside it. That fails in two different ways
depending on what the referenced dataset is:

* referencing a pipeline-local ``@dlt.view`` raises ``TABLE_OR_VIEW_NOT_FOUND`` against the
  fully-qualified name -- the lambda runs outside graph-element registration, so the bare name is
  qualified against the flow's target catalog/schema and resolved through the metastore;
* materializing that dataset so the metastore lookup succeeds then raises
  ``REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION``.

The second error is the decisive one: a snapshot lambda may not reference ANY dataset belonging
to the pipeline, materialized or not. So the read, the delete-value filter and the key-presence
guard all have to live inside a real dataset query definition, and the strategy hands
``apply_changes_from_snapshot`` that dataset's *name*.

``TC-CDC-006`` and ``TC-CDC-007`` are the live tests. These unit tests exist so the design cannot
silently regress to a lambda without a pipeline run to catch it.
"""

import ast
import inspect
import textwrap

from NextGen_Metadata_Framework.lakeflow_framework.cdc import dispatcher, snapshot


def _strategy_ast():
    """Parse register_full_snapshot_cdc structurally.

    Deliberately AST-based rather than text-based: this module's explanatory comments quote the
    very identifiers being asserted on ("dlt.read", "delete_values"), so substring checks match
    prose and prove nothing. The AST sees only code.
    """
    return ast.parse(textwrap.dedent(inspect.getsource(snapshot.register_full_snapshot_cdc))).body[0]


def _dlt_table_function(tree):
    """The inner function carrying a @dlt.table decorator -- the dataset query definition."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node is not tree:
            for dec in node.decorator_list:
                target = dec.func if isinstance(dec, ast.Call) else dec
                if isinstance(target, ast.Attribute) and target.attr == "table":
                    return node
    return None


def _call_named(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == name:
                return node
    return None


def test_snapshot_input_is_a_materialized_dataset():
    """It must be a @dlt.table: apply_changes_from_snapshot versions each snapshot it diffs,
    which needs a stable Delta relation -- a view is recomputed per access and has no version."""
    assert _dlt_table_function(_strategy_ast()) is not None, "no @dlt.table dataset query definition"


def test_apply_changes_from_snapshot_is_given_a_name_not_a_callable():
    call = _call_named(_strategy_ast(), "apply_changes_from_snapshot")
    assert call is not None
    source = next((kw.value for kw in call.keywords if kw.arg == "source"), None)
    assert isinstance(source, ast.Name), (
        "source= must be a plain name bound to the snapshot-input dataset; a lambda or function "
        "reference is what Lakeflow rejects"
    )


def test_every_dlt_read_sits_inside_the_dataset_query_definition():
    tree = _strategy_ast()
    inner = _dlt_table_function(tree)
    inner_reads = {id(n) for n in ast.walk(inner)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "read":
            assert id(node) in inner_reads, (
                "a dlt.read() sits outside the dataset query definition -- exactly the "
                "REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION failure this design removes"
            )


def test_delete_value_filter_and_key_guard_live_in_the_query_definition():
    """Both checks need source_view's schema, which only exists once Lakeflow executes the graph
    -- so both must sit inside the dataset query definition, not at registration time.

    The second assertion was the ``__framework_surrogate_key`` presence guard until v1.4.0. The
    surrogate key is gone; the guard it protected against is not. With no generated key to fall
    back on, a `primary_keys` entry that never reached the clean upstream (renamed by
    column_normalization, projected away by data_standardization_sql) is now the failure, and it
    is caught in the same place for the same reason."""
    inner = _dlt_table_function(_strategy_ast())
    inner_src = ast.unparse(inner)
    assert "delete_values" in inner_src, "delete-value filtering must happen inside the dataset"
    assert "missing_keys" in inner_src, "the primary-key presence guard must happen inside the dataset"


def test_dispatcher_routes_exactly_these_strategies_to_the_snapshot_registrar():
    """FULL_SNAPSHOT_CDC_NO_PK was withdrawn in v1.4.0 with the surrogate-key engine it was the
    only mandatory consumer of -- it must not reappear in the dispatch set."""
    assert dispatcher.SNAPSHOT_STRATEGIES == {"FULL_SNAPSHOT_CDC"}
