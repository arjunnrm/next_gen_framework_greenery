"""Structural guard: ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py`` must stay thin.

The whole v1.5.0 in-graph-reconciliation design rests on one premise: the pipeline notebook is a
*wiring* file, and every decision it appears to make actually lives in an importable module
(``engine/source_plane.py``, ``engine/flow_generators.py``,
``reconciliation/graph_registration.py``). That premise is not self-enforcing. A notebook is the
single least testable file in the repo -- it cannot be imported without a live ``spark``,
``dbutils`` and a running Lakeflow update -- so any logic that drifts back into it silently
becomes logic with no unit test, discoverable only by deploying and running a pipeline.

This module is the ratchet that stops that drift, by AST-parsing the notebook *from disk* (never
importing it) and asserting three properties:

1. **No ``dlt`` surface at all.** Not ``import dlt``, not a ``@dlt.table`` decorator, not a
   ``dlt.read(...)`` call. Every dataset in the graph is registered by a module function
   (``register_source_plane``, ``generate_*_flow``, ``register_reconciliation_flow``); the moment
   the notebook itself decorates a function with ``dlt``, that dataset's body is untestable.
2. **No long function bodies.** A helper in here is allowed to adapt a pipeline-configuration
   value (``_resolve_tristate_conf``); it is not allowed to grow into a generator.
3. **Only generator/logging loops at module level.** The module-level ``for`` loops are the three
   registration loops plus the ``describe_plan`` read-once logging loop -- each one a bare call,
   never a loop body that computes something.

Deliberately file-based, not import-based, and deliberately node-identity checks rather than
substring greps: this docstring quotes ``dlt.table`` and ``dlt.read``, so a text match would match
the prose and prove nothing -- the same reasoning as
``tests/unit/test_recon_registration_ast.py``.
"""

import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "03_engine" / "03_lakeflow_declarative_pipeline.py"

#: Ceiling on statements (recursively counted) in any ``def`` in the notebook. Generous enough
#: for a documented config adapter, far too small for a flow generator.
MAX_FUNCTION_STATEMENTS = 15

#: The three registration loops the notebook exists to run, plus the read-once logging loop.
#: A module-level ``for`` calling anything else is logic that belongs in a module.
ALLOWED_LOOP_CALLS = {
    "generate_ingestion_flow",
    "generate_transformation_flow",
    "generate_reconciliation_flow",
    "log_flow_event",
}

#: The three generator loops must each be present exactly once -- R1: ingestion, transformation
#: and reconciliation all registered into the one Lakeflow DAG, from this one notebook.
REQUIRED_GENERATOR_LOOP_CALLS = (
    "generate_ingestion_flow",
    "generate_transformation_flow",
    "generate_reconciliation_flow",
)


@pytest.fixture(scope="module")
def notebook_tree():
    assert NOTEBOOK_PATH.is_file(), f"Pipeline notebook not found at {NOTEBOOK_PATH}"
    return ast.parse(NOTEBOOK_PATH.read_text(encoding="utf-8"), filename=str(NOTEBOOK_PATH))


def _is_dlt_attribute(node):
    """``dlt.<anything>`` (including chained ``dlt.a.b``) as an attribute access."""
    while isinstance(node, ast.Attribute):
        node = node.value
    return isinstance(node, ast.Name) and node.id == "dlt"


def _called_names(node):
    """Every bare/attribute callee name invoked anywhere under ``node``."""
    names = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            func = sub.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def _count_statements(node):
    """Statements in ``node``'s body, recursively (``node`` itself excluded)."""
    return sum(1 for sub in ast.walk(node) if isinstance(sub, ast.stmt)) - 1


# ---------------------------------------------------------------------------
# 1. No dlt surface
# ---------------------------------------------------------------------------


def test_notebook_never_imports_dlt(notebook_tree):
    """``import dlt`` here would mean the notebook can define its own datasets."""
    for node in ast.walk(notebook_tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name.split(".")[0] != "dlt", (
                    f"{NOTEBOOK_PATH.name} imports 'dlt' at line {node.lineno}. Dataset "
                    "registration belongs in engine/source_plane.py, engine/flow_generators.py "
                    "or reconciliation/graph_registration.py, all of which are unit-testable; "
                    "this notebook is not."
                )
        elif isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[0] != "dlt", (
                f"{NOTEBOOK_PATH.name} imports from 'dlt' at line {node.lineno}."
            )


def test_notebook_makes_no_dlt_calls(notebook_tree):
    """No ``Call`` whose func is a ``dlt`` attribute -- the core assertion of this module."""
    offenders = [
        f"line {node.lineno}: dlt.{node.func.attr}(...)"
        for node in ast.walk(notebook_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and _is_dlt_attribute(node.func)
    ]
    assert not offenders, (
        f"{NOTEBOOK_PATH.name} calls the dlt API directly: {offenders}. Graph-definition logic "
        "must live in an importable module so it can be unit-tested without a workspace."
    )


def test_notebook_declares_no_dlt_datasets(notebook_tree):
    """No ``@dlt.table`` / ``@dlt.view`` / ``@dlt.append_flow`` decorated definition."""
    offenders = []
    for node in ast.walk(notebook_tree):
        for dec in getattr(node, "decorator_list", []):
            target = dec.func if isinstance(dec, ast.Call) else dec
            if _is_dlt_attribute(target):
                offenders.append(f"line {node.lineno}: {getattr(node, 'name', '<node>')}")
    assert not offenders, (
        f"{NOTEBOOK_PATH.name} defines dlt datasets inline: {offenders}. Every dataset in the "
        "DAG must be registered by a module function."
    )


# ---------------------------------------------------------------------------
# 2. No long function bodies
# ---------------------------------------------------------------------------


def test_notebook_functions_are_short(notebook_tree):
    oversized = {
        node.name: _count_statements(node)
        for node in ast.walk(notebook_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and _count_statements(node) > MAX_FUNCTION_STATEMENTS
    }
    assert not oversized, (
        f"{NOTEBOOK_PATH.name} defines function(s) longer than {MAX_FUNCTION_STATEMENTS} "
        f"statements: {oversized}. Move the body into the lakeflow_framework package (where it "
        "is importable and unit-testable) and call it from here."
    )


def test_notebook_defines_no_classes(notebook_tree):
    """A class here is a data model or a strategy -- both belong in the package."""
    classes = [n.name for n in notebook_tree.body if isinstance(n, ast.ClassDef)]
    assert not classes, (
        f"{NOTEBOOK_PATH.name} defines classes {classes}; move them into the package."
    )


# ---------------------------------------------------------------------------
# 3. Module-level loops are registration/logging loops only
# ---------------------------------------------------------------------------


def _module_level_for_loops(tree):
    return [node for node in tree.body if isinstance(node, (ast.For, ast.AsyncFor))]


def test_module_level_loops_only_call_generators_or_logging(notebook_tree):
    for loop in _module_level_for_loops(notebook_tree):
        called = _called_names(loop)
        assert called & ALLOWED_LOOP_CALLS, (
            f"{NOTEBOOK_PATH.name} has a module-level 'for' at line {loop.lineno} that calls "
            f"{sorted(called)} -- none of the permitted registration/logging entry points "
            f"{sorted(ALLOWED_LOOP_CALLS)}. Loops in this notebook must be bare fan-outs over "
            "control-table rows, not computation."
        )


def test_module_level_loop_bodies_are_single_statements(notebook_tree):
    """A registration loop is one call. Branching inside one is decision logic escaping a module."""
    for loop in _module_level_for_loops(notebook_tree):
        assert len(loop.body) == 1 and isinstance(loop.body[0], ast.Expr), (
            f"{NOTEBOOK_PATH.name}: the module-level 'for' at line {loop.lineno} has a body that "
            "is not a single call expression. Push the extra logic into the generator it calls."
        )
        assert not loop.orelse, f"{NOTEBOOK_PATH.name}: for/else at line {loop.lineno}."


def test_the_three_generator_loops_are_all_present(notebook_tree):
    """R1: one DAG per group carries ingestion + transformation + reconciliation."""
    loops = _module_level_for_loops(notebook_tree)
    for required in REQUIRED_GENERATOR_LOOP_CALLS:
        matching = [loop for loop in loops if required in _called_names(loop)]
        assert len(matching) == 1, (
            f"Expected exactly one module-level loop calling '{required}' in "
            f"{NOTEBOOK_PATH.name}, found {len(matching)}."
        )
