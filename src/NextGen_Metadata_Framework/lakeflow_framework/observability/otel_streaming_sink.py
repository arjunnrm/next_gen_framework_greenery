"""A genuine Lakeflow custom sink (PySpark Data Source Sink API) that continuously exports a
streaming micro-batch of raw Lakeflow event-log rows to an OpenTelemetry OTLP/HTTP JSON logs
endpoint -- registered via ``spark.dataSource.register(OtelStreamingDataSource)`` and
referenced from ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py``
as ``dlt.create_sink(format="otel_streaming", ...)``.

**Why this exists.** The pre-existing ``observability`` package (``event_log_extractor.py`` +
``otel_payload_builder.py`` + ``destination_dispatcher.py``, see
``docs/25_dlt_observability_module.md``) is a one-shot *batch* engine: it runs as a downstream
Workflow task after one pipeline's update completes, reads that one pipeline's event log via a
bounded time-window query, and dispatches once. It is architecturally incapable of a
continuously-running, multi-pipeline, always-on export -- there is no streaming source in that
path at all. This module is the streaming counterpart: it is fed by a genuinely continuous
Lakeflow pipeline (``continuous: true``, see ``resources/observability_otel_streaming_pipeline.yml``)
that reads N event-log tables (each a real Unity Catalog Delta table a *source* pipeline
publishes its own event log to -- see
https://learn.microsoft.com/en-us/azure/databricks/ldp/observability -- "Publish pipeline
event logs to a Unity Catalog table") as streaming sources, unions them tagging every row with
a ``source_pipeline`` column, and exports that unified stream here, one OTLP POST per
micro-batch. The two paths deliberately share no code across the batch/streaming boundary
except the low-level OTel-shape helpers in ``otel_payload_builder.py`` (``_resource``,
``_log_record``, ``SCOPE_NAME``/``SCOPE_VERSION``) and ``destination_dispatcher.py``'s
``dispatch_to_otlp`` (retry/backoff/compression) -- reused here exactly as-is, never
reimplemented.

**Executor/driver split.** Same Python Data Source Sink contract as
``archive/pgp_zip_sink.py`` (see that module's docstring for the full mechanics, confirmed
live against this codebase): ``DataSource.streamWriter(schema, overwrite)`` builds one writer
instance per micro-batch, pickled and shipped to every executor for that micro-batch's
``write(iterator)`` calls (one per partition); ``commit(messages, batchId)``/
``abort(messages, batchId)`` run back on the driver afterward, but in a *separate*, restricted
"python streaming data source runtime" worker process (``pyspark/sql/worker/
python_streaming_sink_runner.py``) that cannot construct a working ``dbutils`` gateway.

**Why that restriction doesn't bite here.** ``pgp_zip_sink.py`` hit this the hard way because
its ``sink_config`` can carry Unity Catalog secret coordinates that only ``dbutils.secrets``
can resolve. This sink's only configuration is ``endpoint`` (a plain OTLP URL string) and
``service_name`` -- neither is a secret, per this project's own ``otel_endpoint``/
``otel_export_enabled`` configuration schema (see the engine notebook) -- so this module never
needs to call ``dbutils``/``resolve_secret_ref`` at all, in ``write()``, ``commit()``, or
anywhere else, and the restricted-runtime-can't-resolve-secrets failure mode simply doesn't
apply. (The one live assumption this design does still make about that restricted runtime:
that it permits ordinary outbound HTTPS via the ``requests`` library, since ``commit()``
below calls ``destination_dispatcher.dispatch_to_otlp``, which does exactly that. This was not
independently re-confirmed against a live restricted-runtime worker for this change -- the
restriction Databricks documents and this framework has hit live is specifically about
spawning a ``dbutils`` gateway subprocess, not about outbound network calls in general, so
this is treated as a safe, standard assumption rather than a re-verified fact.)

**In-memory rows, not staged files.** ``pgp_zip_sink.py`` stages each partition's rows into a
uniquely-named JSON-Lines file specifically because its ``commit()`` needs a *directory* of
files to hand to ``compress_and_encrypt_sink`` (a ZIP archiver). This sink has no archival
step -- ``commit()`` only ever needs the rows themselves, once, to build one OTLP payload and
POST it -- so ``write()`` returns each partition's rows directly as plain dicts inside its
``WriterCommitMessage`` (``row.asDict(recursive=True)``, same row-to-dict conversion
``pgp_zip_sink.py`` uses before ``json.dumps``, just not written to disk here). This keeps the
same partition-count/row-count discipline as ``pgp_zip_sink.py`` (a zero-row partition returns
a commit message with ``rows=None``, never an empty list ``commit()`` would have to
special-case) without introducing a staging directory this sink has no other use for.

**Options contract** (``DataSource`` options are always a plain ``str -> str`` mapping):
``endpoint`` (required; the OTLP/HTTP JSON logs collector URL,
e.g. ``https://otel-collector.example.com/v1/logs``) and ``service_name`` (optional; the
``service.name`` OTel resource attribute stamped on every ``ResourceLogs`` entry this sink
builds -- default ``"otel-streaming-sink"``).

**Unchanged by v1.3.0's continuous-mode work, deliberately.** The continuous pipeline now also
resolves *which* event-log tables to stream from ``observability_config`` rows whose
``mode == "continuous"`` (falling back to its own ``dataflow.otel_streaming.event_log_tables``
configuration), and can additionally register a **native JSON file sink** per continuous
``DATABRICKS_VOLUME`` destination. None of that reaches this module: table resolution happens
upstream of the stream this sink consumes, and the Volume sink is Lakeflow's own
``dlt.create_sink(format="json", options={"path": ...})`` -- a plain file write with no OTel
shape, no HTTP, no retry policy, and therefore nothing this OTLP sink could usefully provide.
Adding a "write to a Volume" branch here would mean reimplementing a native sink inside a custom
one. This sink stays OTLP-only, and its ``endpoint`` keeps coming from the pipeline's own
``configuration:`` block rather than from a control-table row -- see the notebook.

Its inline :class:`~config_loader.DestinationConfig` construction in ``commit()`` below is
likewise unaffected by the new ``mode`` field: ``mode`` was added last with a default, and this
one is a synthetic destination with no control-table row behind it, so the default is exactly
right for it.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import (
    FrameworkConfigError,
    ObservabilityDispatchError,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.config_loader import DestinationConfig
from NextGen_Metadata_Framework.lakeflow_framework.observability.destination_dispatcher import dispatch_to_otlp
from NextGen_Metadata_Framework.lakeflow_framework.observability.otel_payload_builder import (
    build_resource_logs_from_event_rows,
)

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.observability.otel_streaming_sink")


def _never_called_secret_resolver(scope: str, key: str) -> str:
    """``dispatch_to_otlp`` requires a ``secret_resolver`` callable, but this sink's
    ``DestinationConfig.auth_config`` is always ``{}`` (this sink has no auth option at all --
    ``otel_endpoint`` is a plain config string, never a secret reference) --
    ``destination_dispatcher.build_auth_headers({}, secret_resolver)`` short-circuits on
    ``auth_type in (None, "NONE")`` before ever invoking its ``secret_resolver`` argument. If
    this function ever actually runs, that short-circuit has changed upstream and needs
    re-checking -- it is deliberately not a silent no-op."""
    raise RuntimeError(
        f"otel_streaming sink: unexpected secret resolution requested (scope={scope!r}, key={key!r}) -- "
        "this sink's auth_config is always empty; see this function's docstring."
    )


@dataclass
class OtelStreamingCommitMessage(WriterCommitMessage):
    """Commit message for one partition's ``write()`` call.

    ``rows`` is ``None`` when a partition received zero rows this micro-batch (a normal,
    frequent occurrence for a low-throughput event-log source) -- mirrors
    ``archive/pgp_zip_sink.py``'s ``PgpZipCommitMessage(staged_file_path=None, row_count=0)``
    pattern, adapted to carry rows in-memory instead of a staged file path.
    """

    rows: Optional[List[Dict[str, Any]]]
    row_count: int


class _OtelStreamingWriter(DataSourceStreamWriter):
    """Per-micro-batch streaming writer: collect rows as plain dicts on ``write()`` (executor
    side), then build one OTLP payload for the whole micro-batch and POST it on ``commit()``
    (driver side, via ``destination_dispatcher.dispatch_to_otlp`` -- reused exactly as-is, not
    reimplemented). See this module's docstring for why neither ``write()`` nor ``commit()``
    ever needs to resolve a secret or call ``dbutils``.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        self._endpoint = (options.get("endpoint") or "").strip()
        if not self._endpoint:
            raise FrameworkConfigError(
                "otel_streaming sink requires an 'endpoint' option (the OTLP/HTTP JSON logs "
                "collector URL) -- see "
                "notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py's "
                "dlt.create_sink(format='otel_streaming', options={'endpoint': ...})."
            )
        self._service_name = options.get("service_name") or "otel-streaming-sink"

    # -- executor side -----------------------------------------------------------------

    def write(self, iterator: Iterator[Row]) -> WriterCommitMessage:
        rows = [row.asDict(recursive=True) for row in iterator]
        if not rows:
            return OtelStreamingCommitMessage(rows=None, row_count=0)
        return OtelStreamingCommitMessage(rows=rows, row_count=len(rows))

    # -- driver side -------------------------------------------------------------------

    def commit(self, messages: List[Optional["WriterCommitMessage"]], batchId: int) -> None:
        rows: List[Dict[str, Any]] = []
        for message in messages:
            if message is not None and message.rows:
                rows.extend(message.rows)
        total_rows = sum(message.row_count for message in messages if message is not None)

        if not rows:
            logger.info(
                "otel_streaming sink: microbatch %s had no rows across %d partition(s) -- nothing to export.",
                batchId,
                len(messages),
            )
            return

        resource_logs = build_resource_logs_from_event_rows(rows, service_name=self._service_name)

        # A minimal, inline DestinationConfig -- this sink has exactly one destination (its
        # own configured `endpoint`), never a control-table-resolved list of destinations
        # like the batch observability path (config_loader.py), so there is nothing to load
        # here. auth_config/retry_config are left at their dataclass defaults ({}), which
        # makes build_auth_headers({}, ...) return {} immediately (see
        # _never_called_secret_resolver above) and dispatch_to_otlp fall back to its own
        # DEFAULT_MAX_ATTEMPTS/DEFAULT_TIMEOUT_MS retry behavior.
        destination = DestinationConfig(
            config_id="otel_streaming_sink",
            dataflow_group_id="otel_streaming",
            destination_id="otel_streaming_sink",
            destination_type="OTLP_CONSUMER",
            destination_config={"endpoint": self._endpoint},
        )

        result = dispatch_to_otlp(resource_logs, destination, secret_resolver=_never_called_secret_resolver)

        if result.status != "SUCCESS":
            # ObservabilityDispatchError is this framework's existing exception for "OTLP
            # telemetry dispatch failed" (see exceptions.py) -- its docstring frames it around
            # destination_dispatcher.dispatch_all's "every destination failed" case, but this
            # sink only ever has exactly one destination, so "this destination failed" and
            # "every destination failed" are the same condition here; reusing it (rather than
            # ArchiveError, which is specifically about ZIP compression/extraction and doesn't
            # fit an HTTP export failure) keeps this framework's typed-exception convention
            # instead of adding a new exception class for what is still, semantically, an
            # observability dispatch failure.
            raise ObservabilityDispatchError(
                f"otel_streaming sink: microbatch {batchId} failed to dispatch {total_rows} row(s) "
                f"({len(resource_logs)} ResourceLogs entr(y/ies)) to '{self._endpoint}' after "
                f"{result.attempts} attempt(s): {result.error}"
            )

        logger.info(
            "otel_streaming sink: committed microbatch %s -- %d row(s) across %d ResourceLogs entr(y/ies) "
            "dispatched to '%s' in %.1f ms (attempts=%d, %d -> %d bytes)",
            batchId,
            total_rows,
            len(resource_logs),
            self._endpoint,
            result.duration_ms,
            result.attempts,
            result.uncompressed_bytes,
            result.compressed_bytes,
        )

    def abort(self, messages: List[Optional["WriterCommitMessage"]], batchId: int) -> None:
        discarded_partitions = sum(1 for message in messages if message is not None and message.rows)
        logger.warning(
            "otel_streaming sink: microbatch %s aborted -- discarded %d partition('s) buffered row-set(s) "
            "(nothing was staged to disk, so there is nothing else to clean up).",
            batchId,
            discarded_partitions,
        )


class OtelStreamingDataSource(DataSource):
    """Registered via ``spark.dataSource.register(OtelStreamingDataSource)`` before any
    ``dlt.create_sink(format="otel_streaming", ...)`` call references it (see
    ``notebooks/06_observability_streaming/06_event_log_otel_streaming_pipeline.py``).

    Write-only: only ``streamWriter`` is implemented -- same rationale as
    ``archive/pgp_zip_sink.py::PgpZipDataSource`` (Lakeflow sinks are streaming-only, fed
    exclusively via ``@dlt.append_flow``; ``reader``/``writer``/``streamReader``/``schema``
    would be dead code for a sink that is never read from).
    """

    @classmethod
    def name(cls) -> str:
        # Must exactly match the `format` string the engine notebook passes to
        # `dlt.create_sink(format=sink_format, ...)` -- Spark resolves a custom sink purely
        # by matching this registered name against that format string, so any mismatch here
        # fails at runtime with "data source not found", not at import/registration time.
        return "otel_streaming"

    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        # `overwrite` is not meaningful here, same as pgp_zip_sink.py: Lakeflow sinks are
        # always-append streaming writes via @dlt.append_flow -- there is no "overwrite the
        # sink" concept in Lakeflow's sink API.
        return _OtelStreamingWriter(self.options)
