"""Structural (AST) enforcement of the Lakeflow rules that the in-graph reconciliation design
depends on -- locally, instead of one deploy + pipeline update per regression.

Every rule below corresponds to a Lakeflow failure mode that only surfaces at pipeline runtime,
which makes it exactly the kind of thing a unit test has to pin down:

* a write/action call inside a dataset query definition (``.saveAsTable``/``.save``/``.start``/
  ``.toTable``/``.createOrReplaceTempView``) -- Lakeflow owns the write for a ``@dlt.table``;
  performing one inside the body is a second, un-managed write;
* an eager action (``.count``/``.collect``/``.first``/``.take``/``.isEmpty``/``.toPandas``) on a
  plan built from a streaming read -- ``AnalysisException: Queries with streaming sources must be
  executed with writeStream.start()``;
* a dataset that reads the very name it defines (self-read cycle);
* a ``dlt.read`` where the registration chose a streaming table (or the reverse);
* the graph-external escape hatch: reading a pipeline-owned table's backing storage directly with
  ``spark.read.format("delta").load(<backing_table_path>)``, which bypasses the DAG entirely and
  therefore bypasses read-once (R2) and the update's own consistency.

**Rule 2 is deliberately scoped to streaming plans, not banned outright.** ``dq/quarantine.py``'s
``_quarantine_table`` ships an eager ``.agg(...).collect()[0]`` on its *batch* branch -- a
legitimate, working construct that a blanket ban on ``.collect()`` would condemn. The failure is
"eager action on a *streaming* plan", so that is what is asserted here; the batch precedent in
``dq/quarantine.py::_quarantine_table`` is why this test does not simply grep for ``.collect()``.

Modelled on ``tests/unit/test_snapshot_input_dataset.py``: ``ast.parse`` over
``inspect.getsource``, decorator-based discovery of the inner dataset query definitions, and
node-identity containment rather than substring matching (this module's own prose quotes the
identifiers under test, so text matching would match the docstrings and prove nothing).
"""

import ast
import inspect
import pathlib
import textwrap

from flowx.lakeflow_framework.engine import source_plane
from flowx.lakeflow_framework.reconciliation import graph_registration

WRITE_CALLS = {"saveAsTable", "save", "start", "toTable", "createOrReplaceTempView"}
EAGER_ACTIONS = {"count", "collect", "first", "take", "isEmpty", "toPandas"}
DLT_DATASET_DECORATORS = {"table", "view", "append_flow", "materialized_view"}

MODULES = {
    "reconciliation/graph_registration.py": graph_registration,
    "engine/source_plane.py": source_plane,
}


def _module_ast(module):
    return ast.parse(textwrap.dedent(inspect.getsource(module)))


def _dlt_decorator_call(node):
    """The ``dlt.<table|view|append_flow>(...)`` call decorating ``node``, if any."""
    for dec in getattr(node, "decorator_list", []):
        target = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(target, ast.Attribute) and target.attr in DLT_DATASET_DECORATORS:
            if isinstance(getattr(target, "value", None), ast.Name) and target.value.id == "dlt":
                return dec if isinstance(dec, ast.Call) else None
    return None


def _has_dlt_decorator(node):
    for dec in getattr(node, "decorator_list", []):
        target = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(target, ast.Attribute) and target.attr in DLT_DATASET_DECORATORS:
            if isinstance(getattr(target, "value", None), ast.Name) and target.value.id == "dlt":
                return True
    return False


def _dlt_bodies(tree):
    """Every dataset query definition in ``tree`` as ``(FunctionDef, registration Call or None)``.

    Two registration spellings are in use and both must be found: the ``@dlt.table(...)``
    decorator (``reconciliation/graph_registration.py``) and the applied form
    ``dlt.table(...)(fn)`` used by ``engine/source_plane.py::register_source_plane``, where the
    decoration happens inside a loop and so cannot be written as a decorator.
    """
    functions = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            if _has_dlt_decorator(node):
                found.append((node, _dlt_decorator_call(node)))
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Call):
            inner = node.func
            if (
                isinstance(inner.func, ast.Attribute)
                and inner.func.attr in DLT_DATASET_DECORATORS
                and isinstance(getattr(inner.func, "value", None), ast.Name)
                and inner.func.value.id == "dlt"
            ):
                for arg in node.args:
                    if isinstance(arg, ast.Name) and arg.id in functions:
                        found.append((functions[arg.id], inner))
    return found


def _calls_named(tree, names):
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in names:
            # F.count(...) / F.first(...) are pyspark.sql.functions Column expressions --
            # lazy by construction, not DataFrame actions. Only a receiver other than the
            # functions module can be an eager action or a write.
            receiver = node.func.value
            if isinstance(receiver, ast.Name) and receiver.id == "F":
                continue
            out.append(node)
    return out


def _attribute_chain_is(node, head, attr):
    """True for ``<head>.<attr>`` -- e.g. ``dlt.read_stream`` or ``spark.readStream``."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attr
        and isinstance(node.value, ast.Name)
        and node.value.id == head
    )


def _is_streaming_read(node):
    """``dlt.read_stream(...)`` or any ``<x>.readStream`` access/call."""
    if isinstance(node, ast.Call):
        func = node.func
        if _attribute_chain_is(func, "dlt", "read_stream"):
            return True
        return isinstance(func, ast.Attribute) and func.attr == "readStream"
    return isinstance(node, ast.Attribute) and node.attr == "readStream"


def _keyword(call, name):
    if call is None:
        return None
    return next((kw.value for kw in call.keywords if kw.arg == name), None)


def test_no_write_or_sink_call_inside_a_dataset_query_definition():
    """Lakeflow owns the write for every ``@dlt.table``/``@dlt.append_flow`` body. A
    ``.saveAsTable``/``.save``/``.start``/``.toTable``/``.createOrReplaceTempView`` inside one is a
    second, un-managed write that the update neither versions nor rolls back."""
    for label, module in MODULES.items():
        bodies = _dlt_bodies(_module_ast(module))
        assert bodies, f"{label}: no dlt dataset query definitions found -- discovery is broken"
        for fn, _dec in bodies:
            offenders = [c.func.attr for c in _calls_named(fn, WRITE_CALLS)]
            assert not offenders, (
                f"{label}::{fn.name} performs {sorted(set(offenders))} inside a dlt dataset query "
                f"definition -- Lakeflow owns that write"
            )


def test_no_eager_action_on_a_streaming_plan_inside_a_dataset_query_definition():
    """Scoped to streaming on purpose.

    ``dq/quarantine.py::_quarantine_table`` ships an eager ``.agg(...).collect()[0]`` on its batch
    branch and is correct; a blanket ban on ``.collect()`` would be wrong. What Lakeflow actually
    rejects is an eager action on a plan reachable from ``dlt.read_stream``/``spark.readStream``
    ("Queries with streaming sources must be executed with writeStream.start()"), so only bodies
    that perform a streaming read are checked."""
    for label, module in MODULES.items():
        for fn, _dec in _dlt_bodies(_module_ast(module)):
            if not any(_is_streaming_read(n) for n in ast.walk(fn)):
                continue
            offenders = [c.func.attr for c in _calls_named(fn, EAGER_ACTIONS)]
            assert not offenders, (
                f"{label}::{fn.name} reads a stream and then performs {sorted(set(offenders))} -- "
                f"an eager action on a streaming plan fails the update at runtime"
            )


def test_no_dataset_reads_the_name_it_defines():
    """A dataset whose body reads its own registered name is a self-edge -- Lakeflow rejects the
    graph, but only once the pipeline is actually built."""
    for label, module in MODULES.items():
        for fn, dec in _dlt_bodies(_module_ast(module)):
            own_name = _keyword(dec, "name")
            if own_name is None:
                continue
            own = ast.unparse(own_name)
            for call in _calls_named(fn, {"read", "read_stream"}):
                for arg in call.args:
                    assert ast.unparse(arg) != own, (
                        f"{label}::{fn.name} reads '{own}', the dataset it defines -- self-read cycle"
                    )


def test_stream_versus_batch_read_is_driven_by_the_registration_variable():
    """Wherever these modules choose between a streaming and a batch read with a conditional, the
    condition must be a plain local name that is also a parameter of the enclosing function -- the
    same value the registration API was chosen from, captured as a default. A conditional on
    anything else can disagree with the registered dataset mode (``dlt.read_stream`` against a
    materialized view), which only fails at graph-build time."""
    checked = 0
    for label, module in MODULES.items():
        tree = _module_ast(module)
        for node in ast.walk(tree):
            if not isinstance(node, ast.IfExp):
                continue
            branch_nodes = [n for b in (node.body, node.orelse) for n in ast.walk(b)]
            if not any(_is_streaming_read(n) for n in branch_nodes):
                continue
            checked += 1
            assert isinstance(node.test, ast.Name), (
                f"{label}: the stream/batch read choice `{ast.unparse(node)}` is not driven by a "
                f"plain variable"
            )
            enclosing = [
                f
                for f in ast.walk(tree)
                if isinstance(f, ast.FunctionDef) and any(n is node for n in ast.walk(f))
            ]
            assert enclosing, f"{label}: `{ast.unparse(node)}` sits outside any function"
            innermost = enclosing[-1]
            params = {a.arg for a in innermost.args.args} | {a.arg for a in innermost.args.kwonlyargs}
            assert node.test.id in params, (
                f"{label}::{innermost.name} switches between a streaming and a batch read on "
                f"'{node.test.id}', which is not one of its parameters -- the registration-time "
                f"decision and the read-time decision must be the same captured value"
            )
    assert checked >= 2, "expected the stream/batch read conditionals in bind() and _execute_reader()"


def test_no_module_reads_a_backing_table_path_directly():
    """``spark.read.format("delta").load(<backing_table_path>)`` reads a pipeline-owned table's
    storage from outside the DAG: no graph edge, no read-once reuse (R2), and no guarantee the
    files belong to the current update. It is the one escape hatch this design forbids outright,
    anywhere under ``src/``."""
    root = pathlib.Path(__file__).resolve().parents[2] / "src"
    scanned = 0
    for path in root.rglob("*.py"):
        scanned += 1
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "load":
                rendered = ast.unparse(node)
                assert "backing_table_path" not in rendered, (
                    f"{path}: `{rendered}` reads a backing_table_path directly -- the "
                    f"graph-external escape hatch this design forbids"
                )
    assert scanned > 0, "found no Python sources under src/ to scan"
