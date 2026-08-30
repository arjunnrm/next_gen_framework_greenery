"""A genuine Lakeflow custom sink (PySpark Data Source Sink API) that writes each streaming
micro-batch to a ZIP archive, optionally PGP-encrypted -- registered via
``spark.dataSource.register(PgpZipDataSource)`` and referenced from
``engine/sink_registration.py`` as ``dlt.create_sink(format="pgp_zip", ...)``.

**Why this exists (Phase 7 requirement):** the framework's core requirement is that "all
external outputs must use genuine Lakeflow/DLT sink functionality (``dlt.create_sink`` +
``@dlt.append_flow``), not ordinary DAG table writes; sink nodes must not appear as
persisted datasets." Before this module, PGP+ZIP egress (``sink_config.post_export_archive``)
only ever ran as a *post-deployment* plain batch step (see the removed
``control_plane/post_deployment.py::run_external_sink_exports``) -- a real design defect the
project's own requirement calls out directly: it was never part of the pipeline's DAG at
all, just a separate job task racing a full `spark.read.table(...).write.save(...)` against
whatever the pipeline had most recently materialized. This module turns that into a genuine
streaming sink: every micro-batch's rows are written, archived, and (optionally)
PGP-encrypted *inside* the same Lakeflow graph execution that produced them, via the
`DataSourceStreamWriter` API added in Spark 4.0 / DBR 15.4+ (see
https://learn.microsoft.com/en-us/azure/databricks/pyspark/datasources -- "Example 4: Create
PySpark DataSource for streaming read and write" -- and
https://learn.microsoft.com/en-us/azure/databricks/ldp/ldp-sinks -- "Python custom data
sources").

**Executor/driver split.** Per the Python Data Source Sink contract (confirmed against
``pyspark.sql.datasource`` and the worker that drives it,
``pyspark/sql/worker/write_into_data_source.py``): ``DataSource.streamWriter(schema,
overwrite)`` is invoked once per micro-batch to build the writer instance, which is then
pickled and shipped to every executor for that micro-batch's ``write(iterator)`` calls
(one call per partition); ``commit(messages, batchId)``/``abort(messages, batchId)`` run back
on the driver afterward, on a *separately* re-constructed writer instance (a fresh
``streamWriter()`` call in a different worker process,
``pyspark/sql/worker/python_streaming_sink_runner.py`` -- it does not share Python object
state with the write-side instance). Two consequences drive this module's design:

1. A driver-constructed attribute (e.g. a live ``SparkSession``) that gets *pickled and sent
   to executors* would break -- ``SparkSession`` holds a JVM gateway/socket that is not
   picklable. So this module never stores a ``SparkSession`` on ``self`` at all.
2. Because commit-side and write-side writer instances are independently constructed (not
   the same Python object), there is no shared in-memory state to correlate "which files did
   THIS micro-batch's partitions write" -- the only channel guaranteed to survive the
   driver/executor round trip is the ``WriterCommitMessage`` itself. So ``write()`` stages
   each partition's rows into its own uniquely-named file (``uuid4``-suffixed, never a
   batch-id-derived name, since the two sides can't agree on one) and returns that exact
   path in its commit message; ``commit()`` reads the path list *only* from the messages it
   was actually handed, never by re-deriving or globbing a directory -- guaranteeing it only
   ever archives exactly this micro-batch's own output, never stale files left behind by a
   prior aborted batch or a concurrently-running different sink.

**Secrets are resolved BEFORE this module ever runs -- never in ``write()``/``commit()``.**
A first version resolved the AES ZIP passphrase and PGP keys lazily, inside ``commit()``
(which the ``DataSourceStreamWriter`` docs describe as running "on the driver", same as every
other secret resolution in this framework). That failed every time, live: ``commit()``/
``abort()`` actually execute in a *separate, dedicated* "python streaming data source
runtime" worker process (``python_streaming_sink_runner.py`` above), not the main pipeline
driver notebook process that owns a working ``dbutils`` gateway -- confirmed via the live
error, ``Unable to resolve Unity Catalog secret '...': [Errno 13] Permission denied:
'/databricks/spark/./bin/spark-submit'`` (``DBUtils(spark)`` failing to spawn its gateway
subprocess in that restricted runtime). This is the same class of restriction Databricks
documents for calling ``dbutils`` from inside a UDF, and the fix is the one Databricks itself
recommends: resolve secrets on the driver, in a context where ``dbutils`` actually works, and
pass the already-resolved values through as plain arguments. Here, that means
``engine/sink_registration.py::_resolve_secret_into_options`` resolves every configured
secret at graph-definition time (the normal pipeline driver process) and embeds the
plaintext values directly into ``dlt.create_sink``'s ``options`` -- this module never calls
``resolve_secret_ref``/``dbutils`` itself, in either ``write()`` or ``commit()``.

Reuses this framework's existing helpers exactly as instructed: ``archive/zip_utils.py``'s
``compress_and_encrypt_sink`` for the ZIP-with-optional-AES-password step (its ``passphrase``
parameter accepts an already-resolved value -- added specifically for this caller, see that
function's docstring) and ``crypto/pgp.py``'s ``pgp_encrypt`` for the optional additional
PGP-encryption (+ optional signing) layer on top of the finished ZIP, both fed pre-resolved
key material only.

**Options contract** (all values are plain strings -- ``DataSource`` options are always a
``str -> str`` mapping): built by ``engine/sink_registration.py::_build_sink_options`` from
``target_config.sink_config`` -- see that module for the exact mapping. Recognized keys:
``path`` (required; a staging directory for per-partition raw-row files -- *not* the final
archive location), ``output_zip_path`` (required; destination directory for one finished
archive file per micro-batch), ``zip_secret_value`` (optional; the already-resolved AES-256
password for the ZIP itself -- note this is the *value*, not secret coordinates, per the
"secrets resolved before this module runs" design above), ``pgp_enabled``
(``"true"``/``"false"``, default ``"false"``), ``pgp_recipient_secret_value`` (required when
``pgp_enabled``; the recipient's already-resolved ASCII-armored PGP public key),
``pgp_sign_secret_value`` (optional; the sender's already-resolved ASCII-armored PGP private
key -- signs before encrypting when present), ``pgp_sign_passphrase_secret_value`` (optional;
only meaningful alongside ``pgp_sign_secret_value`` -- a real signing private key is
routinely passphrase-protected, unlike this project's own throwaway test keypairs),
``export_file_name_format`` (optional; a ``str.format()``-style template for the exported
archive's own file name -- placeholders ``{batch_id}``/``{timestamp}`` (UTC, resolved at
commit time) -- defaults to ``"batch_{batch_id}"``; see
:meth:`_PgpZipStreamWriter._render_export_file_name`). Every
``*_secret_value`` key name still contains ``secret`` so Spark's own credential-redaction
machinery (``spark.redaction.regex``) continues to redact it in query-plan/event-log
diagnostics.
"""

import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from NextGen_Metadata_Framework.lakeflow_framework.archive.zip_utils import compress_and_encrypt_sink
from NextGen_Metadata_Framework.lakeflow_framework.crypto.pgp import pgp_encrypt
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import ArchiveError

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.archive.pgp_zip_sink")

# compress_and_encrypt_sink only picks up files with one of these suffixes (Spark/Delta
# marker files like _SUCCESS are deliberately excluded) -- ".json" matches the JSON-Lines
# staging format `write()` uses below.
_STAGED_FILE_SUFFIX = ".json"


@dataclass
class PgpZipCommitMessage(WriterCommitMessage):
    """Commit message for one partition's ``write()`` call.

    ``staged_file_path`` is ``None`` when a partition received zero rows (a normal,
    frequent occurrence for a low-throughput streaming source, or simply a partition with
    no data this micro-batch) -- ``write()`` removes the would-be-empty file itself rather
    than leaving a zero-row JSON-Lines file for ``commit()`` to special-case.
    """

    staged_file_path: Optional[str]
    row_count: int


class _PgpZipStreamWriter(DataSourceStreamWriter):
    """Per-micro-batch streaming writer: stage rows as JSON-Lines on ``write()`` (executor
    side), then zip (+ optionally PGP-encrypt) exactly this micro-batch's staged files on
    ``commit()`` (driver side). Every secret value used here was already resolved upstream,
    in ``engine/sink_registration.py`` -- see this module's docstring for why neither
    ``write()`` nor ``commit()`` may ever call ``resolve_secret_ref``/``dbutils`` directly.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        self._staging_dir = (options.get("path") or "").rstrip("/")
        self._output_dir = (options.get("output_zip_path") or "").rstrip("/")
        if not self._staging_dir or not self._output_dir:
            raise ArchiveError(
                "pgp_zip sink requires both 'path' (a staging directory for per-microbatch raw "
                "row files) and 'output_zip_path' (the destination directory for finished "
                "archive files) options -- see engine/sink_registration.py."
            )
        self._zip_secret_value = options.get("zip_secret_value")
        self._pgp_enabled = str(options.get("pgp_enabled", "false")).strip().lower() == "true"
        self._pgp_recipient_key_armored = options.get("pgp_recipient_secret_value") if self._pgp_enabled else None
        self._pgp_sign_key_armored = options.get("pgp_sign_secret_value") if self._pgp_enabled else None
        # Optional -- a real signing private key is routinely passphrase-protected. Only
        # meaningful (and only ever populated by engine/sink_registration.py) alongside
        # self._pgp_sign_key_armored.
        self._pgp_sign_passphrase = options.get("pgp_sign_passphrase_secret_value") if self._pgp_sign_key_armored else None
        # Optional -- controls the exported archive's own file name (never the full path,
        # output_zip_path already names the destination directory). Supported placeholders:
        # {batch_id} (the microbatch id) and {timestamp} (UTC, YYYYMMDDTHHMMSSZ, resolved at
        # commit time). Defaults to "batch_{batch_id}", preserving the original naming.
        self._export_file_name_format = options.get("export_file_name_format") or "batch_{batch_id}"
        if self._pgp_enabled and not self._pgp_recipient_key_armored:
            raise ArchiveError(
                "pgp_zip sink has pgp_enabled=true but is missing the pgp_recipient_secret_value option "
                "(the resolved recipient public key) -- see engine/sink_registration.py::_build_sink_options."
            )

    # -- executor side -----------------------------------------------------------------

    def write(self, iterator: Iterator[Row]) -> WriterCommitMessage:
        import json

        from pyspark import TaskContext

        os.makedirs(self._staging_dir, exist_ok=True)
        task_context = TaskContext.get()
        partition_id = task_context.partitionId() if task_context is not None else 0
        staged_file_path = os.path.join(self._staging_dir, f"part-{partition_id}-{uuid.uuid4().hex}{_STAGED_FILE_SUFFIX}")

        row_count = 0
        with open(staged_file_path, "w", encoding="utf-8") as staged_file:
            for row in iterator:
                # default=str: a row can carry dates/decimals/binary columns that
                # json.dumps can't natively serialize -- stringify rather than crash a
                # micro-batch over one awkward column type (this sink's job is to archive
                # the data for downstream consumption, not to be a strict-typed format).
                staged_file.write(json.dumps(row.asDict(recursive=True), default=str))
                staged_file.write("\n")
                row_count += 1

        if row_count == 0:
            os.remove(staged_file_path)
            return PgpZipCommitMessage(staged_file_path=None, row_count=0)
        return PgpZipCommitMessage(staged_file_path=staged_file_path, row_count=row_count)

    def _render_export_file_name(self, batch_id: int) -> str:
        """Render ``self._export_file_name_format`` for one micro-batch's exported archive
        (the bare file name -- e.g. ``"sales_export_20260828T120000Z"`` -- never a full path;
        the ``.zip``/``.zip.pgp`` suffix is always appended by the caller, since the archive
        format itself is never user-configurable here)."""
        from datetime import datetime, timezone

        try:
            return self._export_file_name_format.format(
                batch_id=batch_id, timestamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            )
        except (KeyError, IndexError) as exc:
            raise ArchiveError(
                f"Invalid export_file_name_format {self._export_file_name_format!r}: unknown placeholder {exc} "
                "(supported: {batch_id}, {timestamp})"
            ) from exc

    # -- driver side -------------------------------------------------------------------

    def commit(self, messages: List[Optional["WriterCommitMessage"]], batchId: int) -> None:
        staged_files = [m.staged_file_path for m in messages if m is not None and m.staged_file_path]
        total_rows = sum(m.row_count for m in messages if m is not None)
        if not staged_files:
            logger.info(
                "pgp_zip sink: microbatch %s had no rows across %d partition(s) -- nothing to archive.",
                batchId,
                len(messages),
            )
            return

        commit_staging_dir = os.path.join(self._staging_dir, f"_commit_{batchId}_{uuid.uuid4().hex[:8]}")
        os.makedirs(commit_staging_dir, exist_ok=True)
        try:
            # Move (not copy) each staged file into a batch-scoped directory before handing
            # it to compress_and_encrypt_sink, which walks a whole directory -- this keeps
            # the archive to *exactly* this batch's files even if another micro-batch's
            # write() calls are staging concurrently into the same shared self._staging_dir.
            for staged_file_path in staged_files:
                shutil.move(staged_file_path, os.path.join(commit_staging_dir, os.path.basename(staged_file_path)))

            export_file_name = self._render_export_file_name(batchId)
            plain_zip_path = os.path.join(self._output_dir, f"{export_file_name}.zip")
            compress_and_encrypt_sink(
                None,
                source_dir=commit_staging_dir,
                output_zip_path=plain_zip_path,
                passphrase=self._zip_secret_value,
            )

            if self._pgp_enabled:
                with open(plain_zip_path, "rb") as zip_file:
                    zip_bytes = zip_file.read()
                encrypted_bytes = pgp_encrypt(
                    zip_bytes,
                    self._pgp_recipient_key_armored,
                    sign_with_private_key_armored=self._pgp_sign_key_armored,
                    sign_passphrase=self._pgp_sign_passphrase,
                )
                encrypted_zip_path = f"{plain_zip_path}.pgp"
                with open(encrypted_zip_path, "wb") as encrypted_file:
                    encrypted_file.write(encrypted_bytes)
                # Never leave the un-encrypted intermediate zip behind once it's wrapped --
                # its whole purpose was to end up PGP-encrypted at rest.
                os.remove(plain_zip_path)
                logger.info(
                    "pgp_zip sink: committed microbatch %s -- %d row(s) archived and PGP-encrypted to '%s'",
                    batchId,
                    total_rows,
                    encrypted_zip_path,
                )
            else:
                logger.info(
                    "pgp_zip sink: committed microbatch %s -- %d row(s) archived to '%s'", batchId, total_rows, plain_zip_path
                )
        finally:
            shutil.rmtree(commit_staging_dir, ignore_errors=True)

    def abort(self, messages: List[Optional["WriterCommitMessage"]], batchId: int) -> None:
        discarded = 0
        for message in messages:
            if message is not None and message.staged_file_path:
                try:
                    os.remove(message.staged_file_path)
                    discarded += 1
                except OSError as exc:
                    logger.warning("pgp_zip sink: failed to clean up staged file '%s' after abort: %s", message.staged_file_path, exc)
        logger.warning("pgp_zip sink: microbatch %s aborted -- discarded %d staged partition file(s).", batchId, discarded)


class PgpZipDataSource(DataSource):
    """Registered via ``spark.dataSource.register(PgpZipDataSource)`` before any
    ``dlt.create_sink(format="pgp_zip", ...)`` call references it (see
    ``engine/sink_registration.py::_register_pgp_zip_datasource_once``).

    Write-only: only ``streamWriter`` is implemented. Per the PySpark DataSource contract,
    ``reader``/``writer`` (batch)/``streamReader``/``schema`` are only required for the
    read or batch-write paths this sink never uses -- Lakeflow sinks are streaming-only
    (``@dlt.append_flow`` is the only flow type that can target a sink; see
    https://learn.microsoft.com/en-us/azure/databricks/ldp/concepts/sinks#limitations), so
    implementing anything beyond ``streamWriter`` here would be dead code.
    """

    @classmethod
    def name(cls) -> str:
        # Must exactly match the `format` string engine/sink_registration.py::_create_sink
        # passes to `dlt.create_sink(format=sink_format, ...)` (== sink_config.format ==
        # "pgp_zip", per onboarding/spec_validator.py's ALLOWED_SINK_FORMATS) -- Spark
        # resolves a custom sink purely by matching this registered name against that format
        # string, so any mismatch here fails at runtime with "data source not found", not at
        # import/registration time.
        return "pgp_zip"

    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        # `overwrite` is not meaningful here: Lakeflow sinks are written to exclusively via
        # @dlt.append_flow, which is always an append-mode streaming write (there is no
        # "overwrite the sink" concept in Lakeflow's sink API at all) -- every micro-batch
        # simply produces one more archive file under output_zip_path.
        return _PgpZipStreamWriter(self.options)
