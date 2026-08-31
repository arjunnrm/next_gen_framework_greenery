"""Dataset reading for reconciliation (``source_config``/``target_configs[]``).

A reconciliation flow's ``source_config`` and each of its ``target_configs[]`` entries
independently describe *how* to read one side of a comparison. As of v1.3.0 that is always
``type: "table"`` -- a **Delta** table in Unity Catalog, and nothing else. ``read_mode`` still
picks ``spark.read``/``spark.readStream`` independently per side, so one reconciliation flow can
freely mix a streaming source against a batch target or vice versa
(``reconciliation/streaming.py`` is the ``read_mode: "streaming"`` orchestration counterpart to
this module's plain readers -- it calls this same function to obtain its streaming DataFrame).

**Why Delta tables only (v1.3.0, a deliberate narrowing).** The previous ``type: "file"`` /
``type: "sink"`` branch read a raw location with ``spark.read.format(...).load(path)``. That path
could never carry precomputed ``__framework_hash_key``/``__framework_hash_value`` (so it forced
an inline re-hash of a whole file tree on every run), had no transaction boundary -- a file
landing mid-run silently changed what "the source" meant between the fingerprint and the append
-- and gave the matcher no file skipping to make ``matcher.py``'s documented multiple re-reads of
each side affordable. Every one of those is a correctness or scale problem that disappears the
moment the location has been ingested into a Delta table first, which every deployment of this
framework is already set up to do. So the supported answer is now: land the files, then reconcile
against the landed table. The type check below is defense in depth for a hand-edited
control-table row -- ``onboarding/spec_validator.py`` rejects the same shapes at onboarding time.

**Delta-provider assertion.** ``type: "table"`` alone does not prove a table is Delta: Unity
Catalog also holds foreign/federated tables, Parquet and CSV external tables, and views. Those
bring back exactly the guarantees the narrowing above was about (a stable snapshot per read, file
skipping on ``filter_condition``, and the ability to hold framework hash columns), so the
resolved provider is asserted before the read. A provider that cannot be *determined* (a
temporary view, or a catalog that does not answer ``DESCRIBE TABLE EXTENDED``) is logged at
WARNING and allowed through -- refusing to read something merely because its metadata was
unreadable would fail runs that work today, which is a worse failure mode than the one this
assertion is guarding against.

``task_run_id`` filtering, ``filter_condition``, and ``data_standardization_sql`` are applied in
that order, immediately after the read and before anything else (matching/hashing) ever sees the
DataFrame -- this keeps ``matcher.py`` fully read-mode-agnostic: by the time it receives a
DataFrame, it's already narrowed and standardized, indistinguishable from any other dataset
shape.
"""

import logging
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.standardization_sql import apply_data_standardization_sql
from NextGen_Metadata_Framework.lakeflow_framework.transformation.parameters import substitute_dynamic_parameters

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.reconciliation.dataset_reader")

_TABLE_TYPE = "table"

#: The one table provider reconciliation supports -- compared case-insensitively, since
#: ``DESCRIBE TABLE EXTENDED`` reports it as ``delta`` on some runtimes and ``DELTA`` on others.
_DELTA_PROVIDER = "delta"

#: ``DESCRIBE TABLE EXTENDED``'s own row label for the provider. Used rather than
#: ``DESCRIBE DETAIL`` (which is itself a Delta-only command and therefore cannot report that a
#: table is *not* Delta) or ``spark.catalog.getTable`` (whose ``Table`` object carries no
#: provider field at all).
_PROVIDER_DESCRIBE_LABEL = "provider"


def resolve_table_provider(spark: SparkSession, table: str) -> Optional[str]:
    """Best-effort provider (``delta``, ``parquet``, ``csv``, ...) of a catalog table.

    Returns ``None`` when the provider genuinely cannot be determined -- an unreadable or
    provider-less entry (a temporary view, most commonly) -- rather than raising, so
    :func:`read_reconciliation_dataset` can degrade to a WARNING instead of failing a read that
    would otherwise have worked. See this module's docstring for why that degradation is the
    right trade-off here.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table:
        Fully-qualified table name.

    Returns
    -------
    Optional[str]
        The lowercased provider name, or ``None`` if it could not be determined.
    """
    try:
        for row in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect():
            if (row["col_name"] or "").strip().lower() == _PROVIDER_DESCRIBE_LABEL:
                data_type = row["data_type"]
                return data_type.strip().lower() if data_type else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not describe reconciliation dataset table '%s' to verify its provider: %s", table, exc)
        return None
    return None


def read_reconciliation_dataset(
    spark: SparkSession,
    config: Dict[str, Any],
    parameters: Optional[Dict[str, Any]] = None,
    task_run_id: Optional[str] = None,
    in_graph: bool = False,
) -> DataFrame:
    """Read one side of a reconciliation comparison (``source_config`` or one ``target_configs[]`` entry).

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        A ``source_config``- or ``target_configs[]``-shaped dict, as validated by
        ``onboarding/spec_validator.py::_validate_reconciliation_dataset_config`` --
        ``{type, table, read_mode, task_run_id_column, filter_condition,
        data_standardization_sql, hash_precomputed}``. ``type`` defaults to ``"table"`` (and, as
        of v1.3.0, may not be anything else -- see this module's docstring), ``read_mode`` to
        ``"batch"``, matching the validator's own defaults.
    parameters:
        Dynamic runtime parameters for ``${param}`` substitution in ``filter_condition`` (this
        flow's parent ``dataflow_group_spec.pipeline_parameters_json``, or ``{}`` when this
        reconciliation flow has no parent group / no parameters configured -- see
        ``05_reconciliation_engine.py``).
    task_run_id:
        **SIGNATURE CHANGE (v1.3.0, backward compatible):** a new *trailing* keyword argument
        defaulting to ``None``, so every pre-v1.3.0 call keeps its exact previous behaviour.
        When it is non-empty **and** this side declares a ``task_run_id_column``, the read is
        narrowed to that producing run's rows with
        ``df.filter(F.col(task_run_id_column) == task_run_id)``. Applied immediately after the
        read and **before** ``filter_condition``, so a ``filter_condition`` can narrow the
        already-narrowed set further (the reverse order would let a ``filter_condition``
        appear to widen it, which it cannot, and would read as if it could).

        With no ``task_run_id_column`` on this side, ``task_run_id`` stays what it has always
        been -- a correlation value written to the log tables and nothing more. (Before v1.4.0 the
        caller also had to pass ``None`` here for a ``recon_mode: "continuous"`` flow, which had no
        batch boundary for a producing run id to mean anything against. ``recon_mode`` is gone and
        every run is bounded, so that exception is gone with it.) The usual configured value is
        ``__framework_pipeline_run_id``, which ``dq/quarantine.py::add_quarantine_columns``
        stamps on every row of every framework-materialized table and ``_clean_upstream``
        preserves -- but this module never hardcodes that name, since the same mechanism must
        work against a non-framework table carrying its producer's own run-id column.
    in_graph:
        **NEW (pipeline mode), keyword-only by convention, defaulting to ``False`` so every
        pre-existing (job-mode) call is unaffected.** ``True`` when this read binds an
        ``in_graph_sibling`` source-plane binding -- a table this same dataflow group's Lakeflow
        Declarative Pipeline publishes earlier in the same DAG (an L3 RECON PREPARE node reading
        its own group's L1/L2 output) -- rather than an external table read fresh via
        ``spark.read``/``spark.readStream``. Two things change when it is ``True``:

        - :func:`resolve_table_provider`'s eager ``DESCRIBE TABLE EXTENDED`` probe is skipped
          entirely, rather than run and its ``FrameworkConfigError`` swallowed. On a
          pipeline-produced side the table may not physically exist yet on the pipeline's first
          update -- its producing node has not committed a snapshot -- so the ``DESCRIBE`` would
          raise, the probe's own ``except`` returns ``None``, and the Delta-only assertion below
          would degrade to a permanent, unactionable WARNING on *every* update for exactly the
          sides this change adds. A graph-internal side is a Lakeflow-managed Delta table by
          construction, so there is nothing to verify. The probe is kept verbatim for external
          sides, where an unreadable/non-Delta provider is still a real, actionable signal.
        - A configured ``task_run_id_column`` is rejected with ``FrameworkConfigError`` instead
          of silently accepted. In pipeline mode a graph-internal side is read once per update as
          the shared source-plane node, not re-read per ``task_run_id`` on demand, so narrowing
          it by a constant pipeline-supplied id would silently narrow every update's node to one
          producing run's rows rather than raising -- indistinguishable from a flow that simply
          forgot to drop a job-mode-only setting when it moved to
          ``execution_mode: "pipeline"``.

    Returns
    -------
    DataFrame
        A batch or streaming DataFrame (per ``read_mode``) with the ``task_run_id`` narrowing,
        ``filter_condition``, and ``data_standardization_sql`` already applied.

    Raises
    ------
    FrameworkConfigError
        If ``type`` is anything other than ``"table"`` (reconciliation is Delta-tables-only as of
        v1.3.0), ``table`` is missing, the resolved table is provably not Delta,
        ``hash_precomputed=True`` is combined with a non-``"table"`` ``type``, the configured
        ``task_run_id_column`` is not a column on this side, ``in_graph=True`` is combined with a
        configured ``task_run_id_column``, or the underlying read/filter/standardization step
        fails.
    """
    dataset_type = config.get("type", _TABLE_TYPE)
    read_mode = config.get("read_mode", "batch")
    is_streaming = read_mode == "streaming"
    hash_precomputed = bool(config.get("hash_precomputed", False))

    if in_graph and config.get("task_run_id_column"):
        raise FrameworkConfigError(
            "task_run_id_column is not supported when in_graph=True -- a graph-internal "
            "reconciliation side is read once per pipeline update as the shared source-plane "
            "node, not narrowed to one producing run's task_run_id. Remove task_run_id_column "
            "from this dataset config (it is a job-mode-only setting) or set execution_mode back "
            "to 'job' for this flow."
        )

    # Kept ahead of the type dispatch below even though the dispatch now rejects every
    # non-"table" type on its own: this message names the *specific* contradiction the operator
    # wrote down, which is more actionable than the generic scope error for the one combination
    # that was always impossible rather than merely unsupported.
    if hash_precomputed and dataset_type != _TABLE_TYPE:
        raise FrameworkConfigError(
            f"hash_precomputed=True is only valid for type == 'table' (got type={dataset_type!r}) -- only a "
            "framework-managed table can carry pre-built __framework_hash_key/__framework_hash_value columns"
        )

    try:
        if dataset_type != _TABLE_TYPE:
            raise FrameworkConfigError(
                f"Unsupported reconciliation dataset type {dataset_type!r} -- reconciliation is supported for Delta "
                "tables only ('table'). Read the file/sink output into a Delta table first, then reconcile against "
                "that table."
            )
        table = config["table"]
        if in_graph:
            # Graph-internal side: skip the eager DESCRIBE TABLE EXTENDED probe entirely. On a
            # pipeline-produced side the table may not physically exist yet on the pipeline's
            # first update (its producing node has not committed a snapshot), so the DESCRIBE
            # would raise, resolve_table_provider's own except returns None, and the Delta-only
            # assertion just below would degrade to a permanent, unactionable WARNING on every
            # update for exactly the sides this change adds. A graph-internal side is a
            # Lakeflow-managed Delta table by construction, so there is nothing to verify here.
            # The probe below is kept verbatim for external sides, where an unreadable/non-Delta
            # provider is still a real, actionable signal.
            pass
        else:
            provider = resolve_table_provider(spark, table)
            if provider is not None and provider != _DELTA_PROVIDER:
                raise FrameworkConfigError(
                    f"Reconciliation dataset table '{table}' is not a Delta table (provider={provider!r}) -- "
                    "reconciliation is supported for Delta tables only."
                )
            if provider is None:
                logger.warning(
                    "Could not determine the provider of reconciliation dataset table '%s' -- proceeding on the "
                    "assumption it is Delta (see dataset_reader.py's module docstring).",
                    table,
                )
        df = (spark.readStream if is_streaming else spark.read).table(table)
    except KeyError as exc:
        raise FrameworkConfigError(
            f"Reconciliation dataset config missing required key {exc} for type={dataset_type!r}"
        ) from exc
    except FrameworkConfigError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise FrameworkConfigError(f"Failed to read reconciliation dataset (type={dataset_type!r}): {exc}") from exc

    return apply_reconciliation_overlays(df, config, parameters, task_run_id)


def apply_reconciliation_overlays(
    df: DataFrame,
    dataset_config: Dict[str, Any],
    parameters: Optional[Dict[str, Any]] = None,
    task_run_id: Optional[str] = None,
) -> DataFrame:
    """Apply the post-read overlay chain shared by every reconciliation dataset read.

    Extracted from :func:`read_reconciliation_dataset` (v1.4.0) so the same LOAD-BEARING overlay
    can be applied to a DataFrame obtained a different way -- specifically, an L3 RECON PREPARE
    source-plane node (``_recon__<reconciliation_id>__src`` /
    ``_recon__<reconciliation_id>__<target_id>__tgt``) that binds its DataFrame via
    ``source_plane.bind(...)`` (a fresh external read, a materialized shared node, or a
    ``dlt.read``/``dlt.read_stream`` of an in-graph sibling) instead of calling
    ``spark.read``/``spark.readStream`` directly. ``read_reconciliation_dataset`` itself keeps
    calling this function too, so job-mode reconciliation is byte-for-byte unchanged.

    The order is LOAD-BEARING (see this module's docstring) and MUST NOT be reordered:

    1. ``task_run_id`` narrowing -- only when both ``task_run_id`` and this side's
       ``task_run_id_column`` are set. Narrows to one producing run's rows.
    2. ``filter_condition``, ``${param}``-substituted against ``parameters`` -- narrows further.
       Reversing steps 1 and 2 would let a ``filter_condition`` appear to widen the
       already-narrowed set, which it cannot.
    3. ``data_standardization_sql`` -- runs last, against the already-narrowed rows.

    Parameters
    ----------
    df:
        The freshly obtained (batch or streaming) DataFrame for this side, before any overlay.
    dataset_config:
        The same ``source_config``/``target_configs[]``-shaped dict :func:`read_reconciliation_dataset`
        accepts as ``config``.
    parameters:
        Dynamic runtime parameters for ``${param}`` substitution in ``filter_condition``.
    task_run_id:
        See :func:`read_reconciliation_dataset`'s ``task_run_id`` parameter.

    Returns
    -------
    DataFrame
        ``df`` with the ``task_run_id`` narrowing, ``filter_condition``, and
        ``data_standardization_sql`` overlay chain applied, in that order.

    Raises
    ------
    FrameworkConfigError
        If the ``task_run_id`` narrowing, ``filter_condition``, or ``data_standardization_sql``
        step fails.
    """
    task_run_id_column = dataset_config.get("task_run_id_column")
    if task_run_id and task_run_id_column:
        try:
            df = df.filter(F.col(task_run_id_column) == F.lit(task_run_id))
        except Exception as exc:  # noqa: BLE001
            raise FrameworkConfigError(
                f"Failed to narrow reconciliation dataset to task_run_id={task_run_id!r} on column "
                f"{task_run_id_column!r}: {exc}"
            ) from exc
        logger.info(
            "Reconciliation dataset narrowed to task_run_id='%s' via column '%s'.", task_run_id, task_run_id_column
        )

    filter_condition = dataset_config.get("filter_condition")
    if filter_condition:
        try:
            resolved_filter = substitute_dynamic_parameters(filter_condition, parameters or {})
            df = df.filter(resolved_filter)
        except Exception as exc:  # noqa: BLE001
            raise FrameworkConfigError(f"Failed to apply reconciliation filter_condition {filter_condition!r}: {exc}") from exc

    return apply_data_standardization_sql(df, dataset_config.get("data_standardization_sql"))
