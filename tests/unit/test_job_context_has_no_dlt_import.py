"""Regression guard for v1.6.1 defect D3: a job-context module must never reach ``import dlt``.

``dq/quarantine.py`` does a module-level ``import dlt``. That is correct for a module only ever
loaded inside a Lakeflow Declarative Pipeline — and fatal for anything loaded from a plain **job**
notebook task, because importing ``dlt`` outside a pipeline calls into the runtime's notebook
entry point, which has no notebook id to hand back::

    Py4JJavaError: An error occurred while calling o36.get.
    : java.util.NoSuchElementException: None.get
        at scala.None$.get(Option.scala:627)

Confirmed live on 2026-09-01: ``observability/reconciliation_export.py`` imported one *pure*
exception-inspection helper (``_is_table_not_found``) from ``dq/quarantine.py``, and Sample 03's
``observability_export`` task died at **import time** — before a single line of framework code ran
— taking ``store_sample_config`` down with it. ``reconciliation/appender.py`` had the identical
latent import. The helper now lives in ``dq/table_errors.py``, which imports nothing at all.

WHY THIS IS A STATIC TEST AND NOT AN IMPORT TEST
------------------------------------------------
``import dlt`` *succeeds* in this repo's dev environment — the ``databricks-dlt`` stub package is
a dev dependency, so a test that merely imports the module proves nothing about the serverless
job runtime where the real ``dlt`` lives. The failure is environment-specific; the *import edge*
that causes it is not. So this walks the actual import graph with ``ast`` and asserts the edge is
absent, which fails locally the moment someone re-adds it.

The check is transitive on purpose. The original bug was two hops out
(``reconciliation_export`` -> ``quarantine`` -> ``dlt``), which no direct-import check would have
caught, and which reviewing the diff of ``reconciliation_export.py`` alone could never reveal.
"""

import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
FRAMEWORK_ROOT = REPO_ROOT / "src" / "NextGen_Metadata_Framework" / "lakeflow_framework"
PACKAGE = "NextGen_Metadata_Framework.lakeflow_framework"

#: Modules reachable from a plain job notebook task. Each entry names the notebook that loads it,
#: because that is the fact that makes the constraint true — if a module stops being job-loaded,
#: delete its row rather than silently relaxing the rule for everything.
JOB_CONTEXT_MODULES = {
    "observability/reconciliation_export.py": "notebooks/08_observability/08_dlt_observability_engine.py",
    "reconciliation/appender.py": "notebooks/05_reconciliation/05_reconciliation_engine.py",
    "dq/table_errors.py": "(shared helper — must stay importable from anywhere)",
}

#: The control. ``dq/quarantine.py`` is pipeline-side and SHOULD import ``dlt``; asserting that
#: keeps this suite honest — a refactor that removed the import everywhere would make every
#: assertion above vacuously true, and this row is what would notice.
PIPELINE_CONTEXT_MODULE = "dq/quarantine.py"


def _module_file(dotted: str):
    """Resolve a dotted framework module name to its file, or ``None`` if it is external."""
    relative = dotted[len(PACKAGE) + 1 :].replace(".", "/")
    for candidate in (FRAMEWORK_ROOT / f"{relative}.py", FRAMEWORK_ROOT / relative / "__init__.py"):
        if candidate.exists():
            return candidate
    return None


def _imported_names(path: pathlib.Path):
    """Every absolute module name this file imports, including inside functions.

    Walks the whole tree rather than just the top level: a lazy ``import dlt`` inside a function
    is a legitimate fix for this problem, but a lazy import of a module that itself imports
    ``dlt`` at module scope is not — and only a full walk distinguishes them.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.append(node.module)
    return names


def _dlt_import_chain(start_relative: str):
    """The first ``module -> … -> dlt`` chain reachable from ``start_relative``, or ``None``."""
    start = FRAMEWORK_ROOT / start_relative
    seen: set[str] = set()
    stack = [(start, [start_relative])]
    while stack:
        path, chain = stack.pop()
        if str(path) in seen:
            continue
        seen.add(str(path))
        for name in _imported_names(path):
            if name == "dlt" or name.startswith("dlt."):
                return chain + ["dlt"]
            if name.startswith(PACKAGE):
                nxt = _module_file(name)
                if nxt is not None:
                    stack.append((nxt, chain + [name[len(PACKAGE) + 1 :]]))
    return None


@pytest.mark.parametrize(("module", "loaded_by"), sorted(JOB_CONTEXT_MODULES.items()))
def test_job_context_module_never_transitively_imports_dlt(module, loaded_by):
    chain = _dlt_import_chain(module)
    assert chain is None, (
        f"{module} is loaded from {loaded_by}, a plain job notebook task, but reaches `import dlt` "
        f"via {' -> '.join(chain)}. Importing dlt outside a Lakeflow pipeline fails at import time "
        "with `NoSuchElementException: None.get`. Move the shared code into a dlt-free module "
        "(see dq/table_errors.py) instead of importing the pipeline-side one."
    )


def test_the_pipeline_side_module_still_does_import_dlt():
    """Control: keeps the assertions above from passing vacuously."""
    chain = _dlt_import_chain(PIPELINE_CONTEXT_MODULE)
    assert chain is not None, (
        f"{PIPELINE_CONTEXT_MODULE} no longer imports dlt anywhere. If that is deliberate, this "
        "suite's control has gone stale and the job-context assertions above are now vacuous."
    )


def test_the_relocated_helper_imports_nothing_at_all():
    """``dq/table_errors.py`` exists precisely to be safe to import from any context. The moment
    it grows an import it stops being that, however innocent the import looks."""
    imports = _imported_names(FRAMEWORK_ROOT / "dq" / "table_errors.py")
    assert imports == [], (
        f"dq/table_errors.py must stay dependency-free so it is importable from job context, "
        f"pipeline context and plain Python alike; found imports: {imports}"
    )


def test_quarantine_still_exposes_the_historical_helper_name():
    """Pipeline-side callers and older tests use ``dq.quarantine._is_table_not_found``. The D3 fix
    relocated the implementation; it must not have broken that spelling."""
    from NextGen_Metadata_Framework.lakeflow_framework.dq import quarantine, table_errors

    assert quarantine._is_table_not_found is table_errors.is_table_not_found
    assert quarantine._TABLE_NOT_FOUND_CONDITIONS is table_errors.TABLE_NOT_FOUND_CONDITIONS


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        ("TABLE_OR_VIEW_NOT_FOUND", True),
        ("DELTA_TABLE_NOT_FOUND", True),
        ("DELTA_PATH_DOES_NOT_EXIST", True),
        ("PATH_NOT_FOUND", True),
        ("PERMISSION_DENIED", False),
        ("ANALYSIS_EXCEPTION", False),
    ],
)
def test_relocated_helper_behaviour_is_unchanged(condition, expected):
    """The move must be behaviour-preserving: same conditions matched, same structured-accessor
    preference, same message-scanning fallback."""
    from NextGen_Metadata_Framework.lakeflow_framework.dq.table_errors import is_table_not_found

    class _Structured(Exception):
        def getCondition(self):  # noqa: N802 - mirrors PySpark's own accessor name
            return condition

    assert is_table_not_found(_Structured("irrelevant message")) is expected
    assert is_table_not_found(Exception(f"[{condition}] something went wrong")) is expected


def test_a_structured_accessor_that_raises_falls_back_to_the_message():
    """A shim whose ``getCondition`` blows up tells us nothing — the helper must keep going rather
    than propagate, or a cosmetic runtime change becomes a hard pipeline failure."""
    from NextGen_Metadata_Framework.lakeflow_framework.dq.table_errors import is_table_not_found

    class _Broken(Exception):
        def getCondition(self):  # noqa: N802
            raise RuntimeError("accessor unavailable on this runtime")

        def getErrorClass(self):  # noqa: N802
            raise RuntimeError("accessor unavailable on this runtime")

    assert is_table_not_found(_Broken("[TABLE_OR_VIEW_NOT_FOUND] gone")) is True
    assert is_table_not_found(_Broken("[PERMISSION_DENIED] nope")) is False
