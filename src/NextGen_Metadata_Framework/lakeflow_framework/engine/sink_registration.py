"""Genuine Lakeflow sink registration: ``target_type: "sink"`` and the export half of
``target_type: "external_sink"``.

**Why this module exists.** The project's core requirement (Phase 7) is explicit: "All
external outputs must use genuine Lakeflow/DLT sink functionality (``dlt.create_sink`` +
``@dlt.append_flow``), not ordinary DAG table writes; sink nodes must not appear as
persisted datasets; support direct streaming-transform-to-volume without materializing an
intermediate table." Before this module, every ``target_type`` -- ``sink`` included --
unconditionally registered a real, materialized ``@dlt.table`` via
``dq.quarantine.register_main_and_quarantine_tables`` (see
``engine/flow_registration.py::register_flow_output``'s Phase-7 dispatch), and
``external_sink`` egress ran as a *separate, post-deployment* plain-Spark write (the now
-removed ``control_plane/post_deployment.py::run_external_sink_exports``) racing a
``spark.read.table(...).write.format(...).save(...)`` against whatever the pipeline had most
recently materialized -- never actually inside the pipeline's own DAG. This module is the
fix: ``dlt.create_sink`` + ``@dlt.append_flow`` are genuine Lakeflow graph constructs,
defined at graph-definition time exactly like every ``@dlt.table``/``@dlt.view`` elsewhere
in this framework, and executed as part of the same pipeline update.

**Two call sites, two different upstream sources:**

* :func:`register_sink_target` (``target_type: "sink"``) -- the flow's staged ``@dlt.view``
  feeds the sink directly (``dlt.read_stream(staged_view_name)``). No main/clean table is
  *ever* registered for a sink target -- this is what satisfies "sink nodes must not appear
  as persisted datasets" and "support direct streaming-transform-to-volume without
  materializing an intermediate table."
* :func:`register_external_sink_export` (the export half of ``target_type: "external_sink"``)
  -- a REAL, governed, DQ-quarantined table is still materialized first (unchanged, via
  ``register_main_and_quarantine_tables`` + CDC dispatch, see ``flow_registration.py``), and
  this function ADDITIONALLY exports it via a second, separate ``@dlt.append_flow`` reading
  FROM that now-materialized table (``dlt.read_stream(qualified_main_table)``).

**The streaming-only constraint.** Per Lakeflow's own sink documentation ("Only streaming
queries are supported. Batch queries are not supported." --
https://learn.microsoft.com/en-us/azure/databricks/ldp/concepts/sinks#limitations),
``dlt.create_sink``/``@dlt.append_flow`` cannot be fed by a batch (non-streaming) query at
all. Both entry points below raise :class:`FrameworkConfigError` up front, naming the flow,
citing this exact Lakeflow constraint, and pointing at the fix, rather than letting Lakeflow
itself fail deep inside graph resolution with a much less actionable error.

**Quarantine routing without a persisted clean table.** A ``"sink"`` flow's staged view
still carries the DQ quarantine columns ``add_quarantine_columns`` (see
``engine/flow_registration.py::register_staged_view``) always attaches -- reading it
*unfiltered* into the sink's append flow would leak quarantined rows and internal
``_dq_*`` process columns straight out to an external system. ``register_sink_target``
therefore applies the same quarantine filter
(``.filter(~F.col("__framework_dq_quarantine_flag")).drop(*_QUARANTINE_PROCESS_COLUMNS)``) that
``dq.quarantine.register_main_and_quarantine_tables``'s internal ``_clean_upstream()``
applies before publishing a main table -- duplicated here (not imported/reused from
``dq/quarantine.py``, which is out of this phase's ownership) because that function has no
way to produce *just* the quarantine table without also unconditionally registering a
main/clean table, which a ``"sink"`` flow must never have. The quarantine table itself
(``<target_table>_quarantine``, when a DQ rule has ``action: "quarantine"``) is still
registered independently, exactly as for any other target type -- quarantine routing never
depended on the clean side being a persisted table.
"""

import logging
from typing import Any, Callable, Dict, List, Optional

import dlt
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.session import SparkSession

from NextGen_Metadata_Framework.lakeflow_framework.archive.pgp_zip_sink import PgpZipDataSource
from NextGen_Metadata_Framework.lakeflow_framework.crypto.secrets import resolve_secret_ref
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError
from NextGen_Metadata_Framework.lakeflow_framework.observability.structured_logger import logged_operation
from NextGen_Metadata_Framework.lakeflow_framework.storage.table_properties import build_table_properties, qualified_table_name

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.engine.sink_registration")

# Mirrors dq.quarantine._QUARANTINE_PROCESS_COLUMNS -- kept as a local literal (rather than
# importing quarantine.py's private constant) since dq/quarantine.py is intentionally out of
# this phase's file ownership; both modules must drop the same set of internal DQ-process
# columns before publishing anything external-facing.
_QUARANTINE_PROCESS_COLUMNS = ("__framework_dq_quarantine_flag", "__framework_dq_failed_rule_ids", "__framework_dq_failure_reasons")

_SINKS_DOC_URL = "https://learn.microsoft.com/en-us/azure/databricks/ldp/concepts/sinks#limitations"

_pgp_zip_datasource_registered = False


def _register_pgp_zip_datasource_once() -> None:
    """Register ``PgpZipDataSource`` with the active Spark session, at most once per process.

    ``spark.dataSource.register(...)`` is cheap and idempotent to call repeatedly, but a
    pipeline graph with several ``pgp_zip``-format sinks would otherwise call it once per
    flow for no benefit -- this guard just avoids the redundant calls/log noise.
    """
    global _pgp_zip_datasource_registered
    if _pgp_zip_datasource_registered:
        return
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise FrameworkConfigError(
            "No active SparkSession available to register the pgp_zip custom Data Source -- "
            "this must run inside a Lakeflow Declarative Pipeline's graph-definition context."
        )
    spark.dataSource.register(PgpZipDataSource)
    _pgp_zip_datasource_registered = True
    logger.info("Registered custom Lakeflow sink Data Source 'pgp_zip' (archive/pgp_zip_sink.py::PgpZipDataSource).")


def _resolve_secret_into_options(options: Dict[str, str], prefix: str, secret_ref: Dict[str, str]) -> None:
    """Resolve a ``{secret_catalog, secret_schema, secret_key}`` dict to its plaintext value
    RIGHT NOW (driver-side, at graph-definition time) and store it under
    ``<prefix>_secret_value``, mutating ``options`` in place.

    **Real bug found via live deployment.** The original design flattened the secret's
    *coordinates* (not its value) into options and called ``resolve_secret_ref`` lazily,
    inside ``archive/pgp_zip_sink.py``'s ``commit()`` -- which looked right (``commit()`` is
    documented as running "on the driver", and every other secret use in this framework
    resolves lazily, right where the value is needed). It fails anyway: ``commit()``/``abort()``
    for a Python Streaming Data Source run in a *separate, dedicated* "python streaming data
    source runtime" worker process (``pyspark/sql/worker/python_streaming_sink_runner.py``),
    not the main pipeline driver notebook process that owns a working ``dbutils`` gateway --
    confirmed live: every attempt raised ``Unable to resolve Unity Catalog secret ...:
    [Errno 13] Permission denied: '/databricks/spark/./bin/spark-submit'`` (``DBUtils(spark)``
    trying and failing to spawn a gateway subprocess in that restricted runtime). Databricks'
    own guidance for this exact class of problem (dbutils failing in a UDF/worker context) is
    the same fix applied here: resolve on the driver, in the *normal* graph-definition code
    path, and pass the already-resolved value through -- never call ``dbutils``/
    ``resolve_secret_ref`` again once execution has moved into ``write()``/``commit()``.

    The option key still contains ``secret`` (``<prefix>_secret_value``, not e.g.
    ``<prefix>_value``) so Spark's own credential-redaction machinery
    (``spark.redaction.regex``, matching the keyword ``secret`` -- see ``crypto/secrets.py``'s
    module docstring) still redacts it in query-plan/event-log diagnostics, same as the
    now-superseded coordinate-only fields did. This mirrors ``kafka_secret_options`` just
    below, which resolves the same way for the same structural reason (a flat ``str -> str``
    options dict cannot carry a nested secret-ref) -- see that block's comment for the
    accepted trade-off of a resolved value passing through pipeline graph-definition state.
    """
    options[f"{prefix}_secret_value"] = resolve_secret_ref(SparkSession.getActiveSession(), secret_ref)


def _build_sink_options(flow_label: str, sink_format: str, sink_config: Dict[str, Any]) -> Dict[str, str]:
    """Translate ``target_config.sink_config`` into the ``options`` dict ``dlt.create_sink``
    (or the ``pgp_zip`` custom Data Source it registers) expects.

    Raises
    ------
    FrameworkConfigError
        If ``sink_config`` is missing a field its ``format`` requires. ``onboarding/
        spec_validator.py::_validate_sink_config`` already rejects most of these at
        onboarding time -- these checks are a second, defense-in-depth line for a flow
        whose control-table row was written some other way (e.g. hand-edited, or onboarded
        by an older validator version).
    """
    if sink_format == "delta":
        # A Delta sink accepts EITHER a table_name (a Unity Catalog table reference --
        # options={"tableName": ...}) OR a path (an external/unmanaged location --
        # options={"path": ...}), per Lakeflow's own sink docs: "Delta table sinks ... Specify
        # either a file path or a fully qualified table name" (see _SINKS_DOC_URL). table_name
        # is the shape a UC-table sink needs -- for example the reconciliation L5
        # 'pipeline_audit_only' fallback declaring a plain dlt.create_sink over
        # append_target_table (a real three-part UC name, not a filesystem path). table_name
        # is preferred over path when both are present since it is the more specific,
        # governance-friendly reference.
        table_name = sink_config.get("table_name")
        if table_name:
            return {"tableName": table_name}
        path = sink_config.get("path")
        if path:
            return {"path": path}
        raise FrameworkConfigError(f"Flow '{flow_label}': sink_config must set either 'table_name' or 'path' for format 'delta'")

    if sink_format == "kafka":
        # Same options a Spark Structured Streaming Kafka writer supports (Lakeflow's own
        # docs: "these are the same options a Spark Structured Streaming Kafka sink
        # supports" -- https://learn.microsoft.com/en-us/azure/databricks/ldp/ldp-sinks).
        # kafka.bootstrap.servers/topic are the two options every example in Databricks'
        # own docs treats as mandatory; everything else (security/auth options, Azure Event
        # Hubs' databricks.serviceCredential, etc.) is a free-form passthrough this
        # framework does not attempt to enumerate.
        kafka_options = dict(sink_config.get("kafka_options") or {})
        if not kafka_options.get("kafka.bootstrap.servers") or not kafka_options.get("topic"):
            raise FrameworkConfigError(
                f"Flow '{flow_label}': sink_config.kafka_options must include at least "
                "'kafka.bootstrap.servers' and 'topic' for format 'kafka'"
            )
        # kafka_secret_options resolves eagerly (driver-side, at graph-definition time) and
        # embeds the plaintext directly into dlt.create_sink's options -- unlike every other
        # secret use in this framework, this value becomes part of the pipeline's own graph
        # definition/settings (potentially visible in pipeline configuration/event UI).
        # Prefer kafka_options["databricks.serviceCredential"] (a Unity Catalog service
        # credential *reference*, injected transparently by Databricks -- see
        # https://learn.microsoft.com/en-us/azure/databricks/ldp/ldp-sinks) wherever
        # possible; reserve kafka_secret_options for connector options (e.g.
        # "kafka.sasl.jaas.config") that have no injected-credential alternative and
        # genuinely require the literal value embedded in a config string. This framework
        # has no prior Kafka sink precedent to match (confirmed via repo-wide grep), so this
        # split is this module's own design choice, not a pre-existing convention.
        spark = SparkSession.getActiveSession()
        for option_key, secret_ref in (sink_config.get("kafka_secret_options") or {}).items():
            kafka_options[option_key] = resolve_secret_ref(spark, secret_ref)
        return kafka_options

    if sink_format == "pgp_zip":
        path = sink_config.get("path")
        archive_config = sink_config.get("post_export_archive") or {}
        output_zip_path = archive_config.get("output_zip_path")
        if not path or not output_zip_path:
            raise FrameworkConfigError(
                f"Flow '{flow_label}': format 'pgp_zip' requires both sink_config.path (a staging "
                "directory for per-microbatch raw row files) and "
                "sink_config.post_export_archive.output_zip_path (the destination directory for "
                "finished archive files) -- see archive/pgp_zip_sink.py"
            )
        options: Dict[str, str] = {"path": path, "output_zip_path": output_zip_path}

        # Optional -- controls the exported archive's own file name (not the whole path,
        # which output_zip_path already names). Defaults to the original hardcoded
        # "batch_{batch_id}" naming, now just expressed as this same template mechanism's
        # default rather than a separate code path. See archive/pgp_zip_sink.py's
        # _render_export_file_name for the supported placeholders.
        export_file_name_format = archive_config.get("export_file_name_format")
        if export_file_name_format:
            options["export_file_name_format"] = export_file_name_format

        zip_secret = archive_config.get("secret")
        if zip_secret:
            _resolve_secret_into_options(options, "zip", zip_secret)

        pgp_encryption = archive_config.get("pgp_encryption") or {}
        if pgp_encryption.get("enabled"):
            options["pgp_enabled"] = "true"
            _resolve_secret_into_options(options, "pgp_recipient", pgp_encryption["recipient_public_key_secret"])
            sign_secret = pgp_encryption.get("sign_with_private_key_secret")
            if sign_secret:
                _resolve_secret_into_options(options, "pgp_sign", sign_secret)
                # Optional -- a real signing private key is routinely passphrase-protected
                # (unlike this project's own throwaway test keypairs). Only meaningful
                # alongside sign_with_private_key_secret, per _validate_sink_config.
                sign_passphrase_secret = pgp_encryption.get("sign_passphrase_secret")
                if sign_passphrase_secret:
                    _resolve_secret_into_options(options, "pgp_sign_passphrase", sign_passphrase_secret)
        return options

    raise FrameworkConfigError(f"Flow '{flow_label}': unsupported sink_config.format '{sink_format}' (allowed: delta, kafka, pgp_zip)")


def _create_sink(flow_label: str, sink_name: str, sink_config: Dict[str, Any]) -> None:
    sink_format = sink_config.get("format")
    if not sink_format:
        raise FrameworkConfigError(f"Flow '{flow_label}': target_config.sink_config.format is required")
    options = _build_sink_options(flow_label, sink_format, sink_config)
    if sink_format == "pgp_zip":
        _register_pgp_zip_datasource_once()
    dlt.create_sink(name=sink_name, format=sink_format, options=options)


def register_foreach_batch_sink(sink_name: str, handler: Callable[[DataFrame, int], None]) -> None:
    """Register a genuine Lakeflow ``dlt.foreach_batch_sink`` -- the ForEachBatch sink type
    (see https://learn.microsoft.com/en-us/azure/databricks/ldp/for-each-batch) -- and apply
    ``handler`` to it, exactly as ``@dlt.foreach_batch_sink(name=...)`` would if used as a
    decorator. This is the ONE construct in Lakeflow where arbitrary Python control flow,
    ``.collect()``/``.count()`` and ``.saveAsTable()`` are legal at EXECUTION time on the
    current update's data -- see ``reconciliation/graph_registration.py``'s L5 healing handler,
    the intended (and, as of this change, only) caller.

    **Why this is guarded and ``dlt.create_sink`` (:func:`_create_sink` above, called
    unguarded at graph-definition time) is not:** ``dlt.create_sink`` is production-proven in
    this framework -- every ``"sink"``/``"external_sink"`` flow already deploys through it.
    ``dlt.foreach_batch_sink`` is Public Preview and, confirmed live in this project's own
    environment, is ABSENT from the installed ``databricks-dlt`` 0.3.0 stub
    (``hasattr(dlt, "foreach_batch_sink")`` is ``False``). Calling it unguarded would fail as a
    bare ``AttributeError`` deep inside pipeline graph resolution; guarding it here turns that
    into an actionable :class:`FrameworkConfigError` naming the operator-facing workaround.

    Parameters
    ----------
    sink_name
        The sink's registered name -- referenced as ``target=`` by the ``@dlt.append_flow``
        that feeds it (e.g. the reconciliation L5 healing flow).
    handler
        The per-micro-batch function, with the same ``(batch_df, batch_id)`` signature as
        Spark Structured Streaming's ``foreachBatch``. Side effects only -- its return value
        is ignored.

    Raises
    ------
    FrameworkConfigError
        If the active ``dlt`` module has no ``foreach_batch_sink`` attribute -- i.e. this
        workspace/runtime does not support it yet.
    """
    if not hasattr(dlt, "foreach_batch_sink"):
        raise FrameworkConfigError(
            f"Cannot register foreach_batch_sink '{sink_name}': the active Lakeflow runtime's "
            "'dlt' module has no 'foreach_batch_sink' attribute -- it is a Public Preview "
            "construct and is not available in every workspace/runtime (confirmed absent from "
            "the databricks-dlt 0.3.0 stub this project develops against). This construct is "
            "required for execution_mode 'pipeline' reconciliation healing. Fix: set the flow "
            "group's execution_mode to 'pipeline_audit_only' (ingestion/transformation run "
            "in-pipeline; reconciliation healing still runs as a separate job task, unchanged) "
            "or 'job' (the whole flow group runs as separate job tasks, unchanged) until "
            "foreach_batch_sink is available in this workspace/runtime."
        )
    dlt.foreach_batch_sink(name=sink_name)(handler)


def require_streaming_source(flow_label: str, is_streaming: bool, construct: str, detail: str) -> None:
    """Raise :class:`FrameworkConfigError` unless ``is_streaming`` is ``True``.

    Every genuine Lakeflow sink-adjacent construct in this framework -- ``dlt.create_sink`` +
    ``@dlt.append_flow`` here, and the reconciliation L5 healing ``@dlt.append_flow`` registered
    by ``reconciliation/graph_registration.py`` (its pulse-gated join into
    :func:`register_foreach_batch_sink`'s handler) -- accepts ONLY a streaming query: "Only
    streaming queries are supported. Batch queries are not supported." (see
    ``_SINKS_DOC_URL``). This is the ONE guard for that shared constraint; callers reuse it
    verbatim instead of each re-implementing an equivalent check with a slightly different
    message.

    Parameters
    ----------
    flow_label
        Identifies the flow in the raised message.
    is_streaming
        Whether the caller's upstream query is genuinely streaming.
    construct
        Names the thing that requires streaming, e.g. ``"target_type 'sink'"``.
    detail
        Caller-specific detail appended after the shared Lakeflow-constraint sentence -- what
        "not streaming" means for this caller, and how to fix it.

    Raises
    ------
    FrameworkConfigError
        If ``is_streaming`` is ``False``.
    """
    if not is_streaming:
        raise FrameworkConfigError(
            f"Flow '{flow_label}': {construct} requires a genuinely streaming source. "
            f"Lakeflow's dlt.create_sink()/@dlt.append_flow only supports streaming queries -- "
            f"batch queries are not supported (see {_SINKS_DOC_URL}) -- {detail}"
        )


def _register_sink_quarantine_table_if_configured(
    staged_view_name: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    dq_rules: List[Dict[str, Any]],
    is_streaming: bool,
    quarantine_table_override: Optional[str],
) -> None:
    """Register ``<target_table>_quarantine`` for a ``"sink"`` flow, without ever registering
    a persisted main/clean table alongside it (see this module's docstring).
    """
    if not any(rule.get("action") == "quarantine" for rule in dq_rules):
        return

    table_properties = build_table_properties(target_config)
    quarantine_table_bare = quarantine_table_override or f"{target_table}_quarantine"
    quarantine_table = qualified_table_name(target_catalog, target_schema, quarantine_table_bare)

    @dlt.table(
        name=quarantine_table,
        comment=f"Quarantined records failing DQ quarantine rules for sink '{target_table}'",
        table_properties=table_properties,
    )
    def _sink_quarantine_table():
        upstream = dlt.read_stream(staged_view_name) if is_streaming else dlt.read(staged_view_name)
        return upstream.filter(F.col("__framework_dq_quarantine_flag")).withColumn("__framework_quarantine_validated_at", F.current_timestamp())


def register_sink_target(
    flow_label: str,
    staged_view_name: str,
    target_table: str,
    target_catalog: str,
    target_schema: str,
    target_config: Dict[str, Any],
    dq_rules: List[Dict[str, Any]],
    is_streaming: bool,
    quarantine_table_override: Optional[str] = None,
) -> None:
    """Register ``target_type: "sink"``: staged view -> ``dlt.create_sink`` + ``@dlt.append_flow``.

    NO materialized main table is ever registered for this target type -- see this module's
    docstring. The quarantine table (if configured) is still registered independently.

    Raises
    ------
    FrameworkConfigError
        If the flow is not genuinely streaming (``dlt.create_sink``/``@dlt.append_flow`` is
        streaming-only), or ``target_config.sink_config`` is missing/malformed. Also emits a
        structured ``"sink_registration"`` JSON log event (Phase 10) -- ``SUCCESS`` with the
        sink's format/name, or ``FAILED`` with the real error (re-raised unchanged). Logged at
        registration time only (format, target_type) -- not per-record, since sink flows are
        always streaming (no legal eager count here either -- see ``dq/quarantine.py``'s
        analogous closure comment) and Lakeflow's own native ``flow_progress`` event already
        captures actual per-microbatch write counts for every ``@dlt.append_flow``.
    """
    with logged_operation(
        "sink_registration", flow_label, target_table=target_table, target_type="sink", sink_format=(target_config.get("sink_config") or {}).get("format")
    ):
        require_streaming_source(
            flow_label,
            is_streaming,
            "target_type 'sink'",
            "and this flow's staged view is NOT streaming (its ingestion source_type is batch-only, "
            "or -- for a transformation flow -- none of its source_inputs is marked "
            "'is_streaming: true'). Fix: either change target_type to 'batch_table' (and export it "
            "some other way, outside this framework's sink support), or make the underlying source "
            "streaming (source_type: autoloader/asn1/zerobus for an ingestion flow, or "
            "source_inputs[].is_streaming: true for a transformation flow).",
        )

        sink_config = target_config.get("sink_config") or {}
        if not sink_config:
            raise FrameworkConfigError(f"Flow '{flow_label}': target_type 'sink' requires target_config.sink_config")

        _register_sink_quarantine_table_if_configured(
            staged_view_name, target_table, target_catalog, target_schema, target_config, dq_rules, is_streaming, quarantine_table_override
        )

        sink_name = f"_{target_table}_sink"
        _create_sink(flow_label, sink_name, sink_config)

        @dlt.append_flow(name=f"{target_table}_sink_flow", target=sink_name, comment=f"Direct streaming sink export for {target_table} -- no materialized table")
        def _sink_flow():
            upstream = dlt.read_stream(staged_view_name)
            return upstream.filter(~F.col("__framework_dq_quarantine_flag")).drop(*_QUARANTINE_PROCESS_COLUMNS)

        logger.info(
            "Registered flow '%s' -> Lakeflow sink '%s' (format=%s, no materialized table)",
            flow_label,
            sink_name,
            sink_config.get("format"),
        )


def register_external_sink_export(
    flow_label: str,
    qualified_main_table: str,
    target_table: str,
    target_config: Dict[str, Any],
    main_table_is_streaming: bool,
) -> None:
    """Register the export half of ``target_type: "external_sink"``: a SECOND, separate
    ``@dlt.append_flow`` reading from the already-materialized main table
    (``dlt.read_stream(qualified_main_table)``) into a ``dlt.create_sink``.

    Called *after* ``qualified_main_table`` has already been registered (main table +
    quarantine + CDC dispatch, unchanged from every other target type) -- see
    ``engine/flow_registration.py::register_flow_output``.

    Raises
    ------
    FrameworkConfigError
        If the flow's main target table is not itself a genuine Lakeflow Streaming Table
        (``dlt.create_sink``/``@dlt.append_flow`` can only read a streaming source), or
        ``target_config.sink_config`` is missing/malformed. Also emits a structured
        ``"sink_registration"`` JSON log event (Phase 10) -- see :func:`register_sink_target`'s
        docstring for the same SUCCESS/FAILED contract and why no per-record counts are logged
        here either.
    """
    with logged_operation(
        "sink_registration",
        flow_label,
        target_table=target_table,
        target_type="external_sink",
        sink_format=(target_config.get("sink_config") or {}).get("format"),
    ):
        if not main_table_is_streaming:
            raise FrameworkConfigError(
                f"Flow '{flow_label}': target_type 'external_sink' requires its main target table "
                f"('{qualified_main_table}') to be a genuine Lakeflow Streaming Table so the export "
                f"append_flow can dlt.read_stream() it -- batch queries are not supported as a sink "
                f"source (see {_SINKS_DOC_URL}). This flow's main table is NOT a streaming table "
                "(for example: cdc_load_strategy 'SCD3' publishes its public target as a derived "
                "*batch* @dlt.table pivot over an internal streaming history table -- see "
                "cdc/scd.py::register_scd3 -- which cannot be read via dlt.read_stream; or "
                "cdc_load_strategy is APPEND/TRUNCATE_AND_LOAD with a non-streaming/batch source). "
                "Fix: use a cdc_load_strategy whose target is genuinely streaming (SCD1/SCD2/"
                "FULL_SNAPSHOT_CDC), or make the underlying source "
                "streaming, or export this table some other way outside this framework's sink "
                "support."
            )

        sink_config = target_config.get("sink_config") or {}
        if not sink_config:
            raise FrameworkConfigError(f"Flow '{flow_label}': target_type 'external_sink' requires target_config.sink_config")

        sink_name = f"_{target_table}_export_sink"
        _create_sink(flow_label, sink_name, sink_config)

        @dlt.append_flow(name=f"{target_table}_export_flow", target=sink_name, comment=f"external_sink export of '{qualified_main_table}'")
        def _export_flow():
            return dlt.read_stream(qualified_main_table)

        logger.info(
            "Registered external_sink export for flow '%s': '%s' -> Lakeflow sink '%s' (format=%s)",
            flow_label,
            qualified_main_table,
            sink_name,
            sink_config.get("format"),
        )
