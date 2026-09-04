"""Spark "that table/view does not exist" detection -- deliberately free of any ``dlt`` import.

This module exists because of *where* its one helper is called from, not because of what it
does. :func:`is_table_not_found` is pure exception inspection: no Spark session, no DLT, no
pipeline context, nothing but ``getattr`` and string comparison. It used to live in
``dq/quarantine.py``, which does a module-level ``import dlt`` -- and that turned a harmless
helper into a hard dependency on running inside a Lakeflow Declarative Pipeline.

**The failure that produced this module.** ``observability/reconciliation_export.py`` and
``reconciliation/appender.py`` both need the helper, and both are reached from plain **job**
notebook tasks (``notebooks/08_observability/08_dlt_observability_engine.py``,
``notebooks/05_reconciliation/05_reconciliation_engine.py``) -- not from a pipeline. Importing
``dlt`` outside a pipeline context calls into the runtime's notebook entry point, which has no
notebook id to return, and the import dies before a single line of framework code runs::

    Py4JJavaError: An error occurred while calling o36.get.
    : java.util.NoSuchElementException: None.get
        at scala.None$.get(Option.scala:627)
        ...
    File .../observability/reconciliation_export.py:69
        from ...dq.quarantine import _is_table_not_found
    File .../dq/quarantine.py:50
        import dlt

Confirmed live on 2026-09-01: Sample 03's ``observability_export`` task failed this way on
serverless environment version 4, taking ``store_sample_config`` down with it, while the
pipeline that produced the reconciliation datasets had succeeded moments earlier.

**The rule this encodes.** A module imported from job context must not transitively import
``dlt``. When a helper is needed on both sides of that line, it belongs here (or in another
DLT-free module), never in a pipeline-context module with a re-export. ``dq/quarantine.py``
keeps a backwards-compatible ``_is_table_not_found`` alias so existing pipeline-side callers and
tests are unaffected, but job-context callers import from *this* module directly -- importing the
alias would defeat the entire point.
"""

#: Spark error-condition names meaning "this table/view does not exist". Matched by name rather
#: than by message text so a Databricks-runtime wording change cannot silently turn a
#: never-materialized target into a hard failure (or vice versa). DELTA_TABLE_NOT_FOUND and
#: DELTA_PATH_DOES_NOT_EXIST cover the Delta-specific spellings; PATH_NOT_FOUND covers a target
#: whose storage location was removed out from under the metastore entry.
TABLE_NOT_FOUND_CONDITIONS = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "DELTA_TABLE_NOT_FOUND",
    "DELTA_PATH_DOES_NOT_EXIST",
    "PATH_NOT_FOUND",
)


def is_table_not_found(exc: Exception) -> bool:
    """True only when ``exc`` specifically means "that table/view does not exist".

    Prefers PySpark's structured ``getErrorClass()``/``getCondition()`` when the exception
    exposes one; falls back to scanning the rendered message for a condition name only when it
    does not. The fallback is a substring check against those same uppercase condition tokens,
    never against free-form prose, so it stays insensitive to message rewording.
    """
    for accessor in ("getCondition", "getErrorClass"):
        getter = getattr(exc, accessor, None)
        if callable(getter):
            try:
                condition = getter()
            except Exception:  # noqa: BLE001 - a shim that raises tells us nothing; try the next one
                condition = None
            if condition:
                return any(name in str(condition).upper() for name in TABLE_NOT_FOUND_CONDITIONS)
    rendered = str(exc).upper()
    return any(name in rendered for name in TABLE_NOT_FOUND_CONDITIONS)
