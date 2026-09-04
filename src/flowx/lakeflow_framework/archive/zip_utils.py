"""Landing-zone ZIP extraction and egress-sink ZIP compression, both optionally AES-256 encrypted.

The two low-level primitives every ZIP-handling code path in the framework is built on, via
``pyzipper`` (plain ``zipfile`` has no AES support): :func:`extract_encrypted_zip` unpacks an
incoming archive into a Unity Catalog Volume (called from ``ingestion/readers.py::
_apply_source_zip_handling`` for a single-ZIP ``source_zip_handling`` config, and from
``archive/zip_ingestion_pipeline.py`` for the multi-ZIP batch case); :func:`compress_and_encrypt_sink`
bundles a directory of already-written sink output files into one archive (called from
``archive/zip_ingestion_pipeline.py`` for the joined batch's output ZIP, and from
``archive/pgp_zip_sink.py``'s ``commit()`` for the genuine streaming ``pgp_zip`` sink format).
Neither function resolves a secret itself when a plaintext ``passphrase`` is supplied directly
instead -- required by ``pgp_zip_sink.py``, whose ``commit()`` runs in a separate worker process
that cannot construct a working ``dbutils`` gateway (see that module's docstring), so its caller
must resolve the secret earlier and pass the plaintext value through.

**Driver-memory isolation (both functions).** Neither function ever holds a whole archive's
bytes in the *calling* process's heap anymore:

* :func:`extract_encrypted_zip` no longer runs ``pyzipper.AESZipFile(...).extractall(...)``
  in whatever process calls it. When a live ``spark`` is supplied, the actual open/extract
  work is dispatched onto a Spark *executor* via a single-row ``DataFrame.mapInPandas`` job
  (see :func:`_extract_on_executor`) -- the calling process (typically the pipeline driver)
  only ever sees a small extracted-file-path list back, never archive bytes. ``spark=None``
  (the convenience path every offline/local-fixture unit test in
  ``tests/unit/test_zip_source_handling.py`` relies on) falls back to running the same core
  logic in-process, unchanged from the original behavior.
* :func:`compress_and_encrypt_sink` no longer builds the archive in an in-memory
  ``io.BytesIO()`` buffer sized to the whole finished archive. It now builds the ZIP into a
  real local-disk temporary file (:func:`_build_archive_to_local_temp`) -- still fully
  seekable, exactly like the old ``BytesIO`` buffer was, satisfying the same Volumes
  constraint below -- then streams it out to ``output_zip_path`` in bounded-size chunks
  (:func:`_stream_copy_file`), capping peak memory at one chunk's size regardless of the
  archive's total size. When a live ``spark`` is supplied, this whole build+copy step also
  runs on an executor (:func:`_compress_on_executor`); ``spark=None`` (the only mode
  ``archive/pgp_zip_sink.py``'s ``commit()`` can ever use -- see its docstring for why) still
  gets the disk-spill memory fix even though it can't get executor placement, since no
  ``SparkSession`` is architecturally available at that call site at all.

Both distributed paths use ``DataFrame.mapInPandas`` specifically, never the RDD API
(``sparkContext.parallelize``/``.mapPartitions``): the RDD API has no equivalent over Spark
Connect / Databricks Connect / serverless compute (this project's own ``tests/conftest.py``
``spark`` fixture is a ``DatabricksSession`` -- i.e. Spark Connect -- so an RDD-based design
would silently be untestable and, worse, broken on serverless compute), while
``DataFrame.mapInPandas`` is a DataFrame-level API that works identically everywhere this
framework runs -- the same reason ``asn1/decoder.py``'s partition decoder is built the same
way. For the same reason, a resolved passphrase is threaded through as a plain Python closure
variable captured by the ``mapInPandas`` partition function (cloudpickled into the task same
as any other closure), never a ``SparkContext.broadcast()`` value -- broadcast variables are
also an RDD-era API unavailable over Spark Connect.

**Landing-archive deletion policy.** :func:`extract_encrypted_zip` deliberately stays a
low-level primitive that takes a plain ``delete_source_after_extract: bool`` -- it deletes the
one archive it was handed, or it doesn't. The *policy* that decides which of those two a given
ingestion flow wants (``source_zip_handling.delete_source_after_extract``, which since v1.3.0
also accepts a nested ``{"action": "delete_after_x_days", "days": N}`` object) is normalized
one layer up by :func:`resolve_zip_delete_policy` into a :class:`ZipDeletePolicy`, and the
age-based half of that policy is executed by :func:`sweep_aged_archives`. Keeping the policy
out of the primitive is what lets ``archive/zip_ingestion_pipeline.py`` -- a batch job with its
own, unrelated ``cleanup_extracted_files`` lifecycle -- keep passing a bare
``delete_source_after_extract=False`` without ever knowing the ingestion-flow policy shape
exists.

:func:`compress_and_encrypt_sink` builds the archive in a local scratch file (never directly
against the destination path): the ZIP container format requires seeking back to patch local
file headers and write the central directory once every member is written, and Unity Catalog
Volumes' FUSE mount only supports sequential writes -- building directly against a Volume
path raises ``OSError: [Errno 5] Input/output error`` on close(). Building against a real
local file first (always seekable, whether that's the driver's or an executor's local disk),
then copying the finished bytes out in one sequential pass, sidesteps that limitation
entirely -- the same trick the original ``io.BytesIO()`` design used, just backed by disk
instead of RAM so archive size no longer bounds process memory.
"""

import fnmatch
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, StringType, StructField, StructType

from flowx.lakeflow_framework.crypto.secrets import resolve_secret_value
from flowx.lakeflow_framework.exceptions import ArchiveError, FrameworkConfigError

logger = logging.getLogger("common.archive.zip_utils")

try:
    import pyzipper

    _PYZIPPER_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only in environments missing the lib
    _PYZIPPER_AVAILABLE = False


def _resolve_optional_passphrase(
    spark: SparkSession, secret_catalog: Optional[str], secret_schema: Optional[str], secret_key: Optional[str]
) -> Optional[bytes]:
    if not (secret_catalog and secret_schema and secret_key):
        return None
    passphrase = resolve_secret_value(spark, secret_catalog, secret_schema, secret_key)
    return passphrase.encode("utf-8")


# ---------------------------------------------------------------------------------------
# Extraction: a pure, Spark/dbutils-independent core (safe on an executor or in-process),
# plus a thin executor-dispatch wrapper.
# ---------------------------------------------------------------------------------------


def _extract_archive_core(
    source_zip_path: str, target_volume_path: str, passphrase_bytes: Optional[bytes]
) -> List[str]:
    """Open and extract exactly one (optionally AES-password-protected) ZIP archive.

    Deliberately has no ``SparkSession``/``dbutils`` dependency -- every argument is either a
    plain path or already-resolved key material -- so it runs unmodified whether called
    in-process (the ``spark=None`` fallback) or from inside a Spark executor task (see
    :func:`_extract_on_executor`). Raises :class:`ArchiveError` on failure; a caller running
    this inside a distributed task must catch it itself and translate it into a per-row
    result rather than let it escape ``mapInPandas``, where PySpark would re-wrap it as a
    generic ``PythonException``/``SparkException`` and the caller would lose the ability to
    ``except ArchiveError`` around it.
    """
    if not _PYZIPPER_AVAILABLE:
        raise ArchiveError("pyzipper is required for encrypted ZIP extraction but is not installed.")

    try:
        os.makedirs(target_volume_path, exist_ok=True)
    except OSError as exc:
        raise ArchiveError(f"Failed to create target_volume_path '{target_volume_path}': {exc}") from exc

    extracted_paths: List[str] = []
    try:
        with pyzipper.AESZipFile(source_zip_path) as archive:
            if passphrase_bytes is not None:
                archive.setpassword(passphrase_bytes)
            member_names = archive.namelist()
            if not member_names:
                logger.warning("Archive '%s' contains no members.", source_zip_path)
            archive.extractall(path=target_volume_path)
            extracted_paths = [os.path.join(target_volume_path, name) for name in member_names]
    except RuntimeError as exc:
        # pyzipper raises RuntimeError for bad passwords on AES-protected members.
        raise ArchiveError(f"Failed to decrypt/extract '{source_zip_path}' - incorrect passphrase?: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise ArchiveError(f"Failed to extract archive '{source_zip_path}': {exc}") from exc

    return extracted_paths


_EXTRACT_RESULT_SCHEMA = StructType(
    [
        StructField("success", BooleanType(), False),
        StructField("extracted_paths", ArrayType(StringType()), False),
        StructField("error_message", StringType(), True),
    ]
)


def _extract_on_executor(
    spark: SparkSession,
    source_zip_path: str,
    target_volume_path: str,
    passphrase_bytes: Optional[bytes],
) -> List[str]:
    """Run :func:`_extract_archive_core` for exactly one archive on a Spark *executor*,
    never in the calling (driver) process, via a single-row ``mapInPandas`` job -- so the
    (potentially multi-GB) ``extractall()`` call, and every byte it touches, runs entirely
    off the driver's heap. See this module's docstring for why ``mapInPandas`` (never the
    RDD API) is used, and why the passphrase is threaded through as a plain closure variable
    rather than a broadcast variable.

    ``passphrase_bytes`` (when not ``None``) is cloudpickled into the task closure shipped to
    the executor that runs this partition -- the same mechanism (and the same exposure
    profile) this framework's own ``pgp_zip`` custom sink already relies on for
    ``zip_secret_value``/``pgp_recipient_secret_value`` (see ``archive/pgp_zip_sink.py`` and
    ``docs/16_encryption_and_secrets.md`` section 6): the value is never written to a query
    plan or DataFrame column (so Spark's ``spark.redaction.regex`` credential redaction,
    which matches literal ``secret``-shaped SQL text, never applies here), but it does exist
    as plain bytes in the executor process's memory for the life of the task, and in the
    pickled task closure while in flight -- an accepted, pre-existing trade-off in this
    codebase, not a new one introduced here.
    """

    def _run_partition(batches):
        import pandas as pd

        for batch in batches:
            rows = []
            for row in batch.itertuples(index=False):
                try:
                    paths = _extract_archive_core(row.source_zip_path, row.target_volume_path, passphrase_bytes)
                    rows.append({"success": True, "extracted_paths": paths, "error_message": None})
                except ArchiveError as exc:
                    rows.append({"success": False, "extracted_paths": [], "error_message": str(exc)})
            yield pd.DataFrame(rows, columns=["success", "extracted_paths", "error_message"])

    jobs_df = spark.createDataFrame(
        [(source_zip_path, target_volume_path)],
        schema=StructType(
            [
                StructField("source_zip_path", StringType(), False),
                StructField("target_volume_path", StringType(), False),
            ]
        ),
    ).coalesce(1)

    result = jobs_df.mapInPandas(_run_partition, schema=_EXTRACT_RESULT_SCHEMA).collect()[0]
    if not result["success"]:
        raise ArchiveError(result["error_message"])
    return list(result["extracted_paths"])


def extract_encrypted_zip(
    spark: SparkSession,
    source_zip_path: str,
    target_volume_path: str,
    secret_catalog: Optional[str] = None,
    secret_schema: Optional[str] = None,
    secret_key: Optional[str] = None,
    delete_source_after_extract: bool = True,
) -> List[str]:
    """Extract a (optionally AES-256 passphrase-protected) ZIP archive into a UC Volume.

    Parameters
    ----------
    spark:
        Active SparkSession, used to resolve the secret when the archive is encrypted, and
        (when not ``None``) to dispatch the actual extraction onto a Spark executor rather
        than running it in the calling process -- see this module's docstring. Pass ``None``
        to run entirely in-process (e.g. a local fixture in a unit test, or a context with no
        SparkSession available at all).
    source_zip_path:
        Path to the source ZIP file (typically a landing-zone Unity Catalog Volume path).
        For a PGP-then-ZIP-wrapped archive, this must already be the *decrypted* ZIP -- PGP
        decryption happens one layer up, in ``ingestion/readers.py::_apply_source_zip_handling``,
        before this function is ever called (see ``source_zip_handling.pre_extraction_decryption``).
    target_volume_path:
        Destination directory (Unity Catalog Volume path) to extract members into.
    secret_catalog, secret_schema, secret_key:
        Unity Catalog secret coordinates for the archive passphrase. Leave all ``None`` for
        a plain, unencrypted ZIP.
    delete_source_after_extract:
        When ``True`` (default), removes the source archive after a successful extraction,
        per the framework's landing-file lifecycle policy. Deliberately a plain bool and not a
        :class:`ZipDeletePolicy`: this stays the low-level "delete this one file or don't"
        primitive, and the richer ``source_zip_handling.delete_source_after_extract`` policy
        (including ``delete_after_x_days``) is resolved one layer up, in
        ``ingestion/readers.py::_apply_source_zip_handling``, via
        :func:`resolve_zip_delete_policy`. That split is what lets
        ``archive/zip_ingestion_pipeline.py`` -- whose extracted members have their own
        ``cleanup_extracted_files`` lifecycle -- keep passing a bare ``False`` unchanged.

    Returns
    -------
    list[str]
        Absolute paths of the extracted member files.

    Raises
    ------
    ArchiveError
        If pyzipper is unavailable, the archive is corrupt, the passphrase is incorrect, or
        extraction otherwise fails.
    """
    if not _PYZIPPER_AVAILABLE:
        raise ArchiveError("pyzipper is required for encrypted ZIP extraction but is not installed.")

    try:
        os.makedirs(target_volume_path, exist_ok=True)
    except OSError as exc:
        raise ArchiveError(f"Failed to create target_volume_path '{target_volume_path}': {exc}") from exc

    try:
        passphrase_bytes = _resolve_optional_passphrase(spark, secret_catalog, secret_schema, secret_key)
    except Exception as exc:  # noqa: BLE001 - includes SecretResolutionError
        raise ArchiveError(f"Failed to resolve archive passphrase for '{source_zip_path}': {exc}") from exc

    if spark is not None:
        extracted_paths = _extract_on_executor(spark, source_zip_path, target_volume_path, passphrase_bytes)
    else:
        extracted_paths = _extract_archive_core(source_zip_path, target_volume_path, passphrase_bytes)

    if delete_source_after_extract:
        try:
            os.remove(source_zip_path)
            logger.info("Removed source archive after successful extraction: %s", source_zip_path)
        except OSError as exc:
            logger.warning("Extraction succeeded but source archive cleanup failed for '%s': %s", source_zip_path, exc)

    logger.info("Extracted %d member(s) from '%s' into '%s'", len(extracted_paths), source_zip_path, target_volume_path)
    return extracted_paths


# ---------------------------------------------------------------------------------------
# Landing-archive deletion policy: normalize the spec value once, then execute the
# age-based half of it as an explicit sweep. See this module's docstring for why the policy
# lives here rather than inside extract_encrypted_zip.
# ---------------------------------------------------------------------------------------

ZIP_DELETE_NOW = "delete_now"
ZIP_DELETE_AFTER_X_DAYS = "delete_after_x_days"
ZIP_DELETE_NEVER = "never"

#: One day in seconds -- the unit ``delete_after_x_days`` counts in. Deliberately a fixed
#: 86400 rather than a calendar day: ``os.path.getmtime`` returns a POSIX timestamp, so an
#: elapsed-seconds threshold is the only comparison that needs no timezone/DST reasoning, and
#: a landing-zone retention window is an approximate operational policy, never an accounting
#: boundary.
_SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(frozen=True)
class ZipDeletePolicy:
    """Normalized internal representation of ``source_zip_handling.delete_source_after_extract``.

    ``action`` is one of :data:`ZIP_DELETE_NOW` / :data:`ZIP_DELETE_AFTER_X_DAYS` /
    :data:`ZIP_DELETE_NEVER` -- note that ``"never"`` is an INTERNAL-ONLY action with no spec
    spelling of its own; it is what the legacy boolean ``false`` normalizes to. Keeping it
    internal is deliberate: the spec surface stays exactly the two documented object actions
    plus the two legacy booleans, so no operator can write ``{"action": "never"}`` and expect
    the validator to accept it. ``days`` is always an int (``0`` for ``delete_now``/``never``,
    where it is meaningless).

    Frozen because a resolved policy is read once per matched archive inside
    ``ingestion/readers.py::_apply_source_zip_handling``'s extraction loop and then again by
    the post-loop sweep -- an accidentally-mutated policy midway through that loop would give
    two archives in the same pipeline update different lifecycles for no visible reason.
    """

    action: str
    days: int = 0

    @property
    def delete_current_archive(self) -> bool:
        """True only for ``delete_now`` -- the one action that removes THIS run's own archive.

        ``delete_after_x_days`` deliberately leaves the just-extracted archive in place: its
        whole point is that the archive stays available for ``days`` days, and it is a *later*
        pipeline update's sweep (:func:`sweep_aged_archives`) that eventually removes it.
        """
        return self.action == ZIP_DELETE_NOW


def resolve_zip_delete_policy(raw: Any) -> ZipDeletePolicy:
    """Normalize the raw spec value (``None`` / ``bool`` / ``dict``) into one :class:`ZipDeletePolicy`.

    The mapping, in the order the values are recognized:

    * ``None`` (omitted, or an explicit JSON ``null``) -> ``delete_now``. Every ``source_zip_handling``
      spec written before v1.3.0 omits this field and depends on that default, so "absent" must
      keep meaning exactly what the pre-v1.3.0 ``zip_handling.get(..., True)`` meant.
    * ``True`` -> ``delete_now``; ``False`` -> ``never``. The legacy boolean form stays fully
      supported -- it is what every spec in ``flowx_testing/`` uses.
    * ``{"action": "delete_now"}`` -> identical to ``True``.
    * ``{"action": "delete_after_x_days", "days": N}`` -> keep this run's archive, sweep archives
      already older than ``N`` days (see :func:`sweep_aged_archives`).

    Raises
    ------
    FrameworkConfigError
        On any other shape. ``onboarding/spec_validator.py`` already rejects these at onboarding
        time, so reaching this raise means the control-table row was hand-edited past the
        validator -- this is defense in depth, and failing the pipeline update loudly is far
        safer than guessing a deletion policy for an operator's landing zone.
    """
    if raw is None:
        return ZipDeletePolicy(action=ZIP_DELETE_NOW, days=0)
    # bool before dict/int: bool is a subclass of int, so the legacy boolean form must be
    # recognized before any numeric interpretation is attempted anywhere below.
    if isinstance(raw, bool):
        return ZipDeletePolicy(action=ZIP_DELETE_NOW if raw else ZIP_DELETE_NEVER, days=0)
    if isinstance(raw, dict):
        action = raw.get("action")
        if action == ZIP_DELETE_NOW:
            # `days` is meaningless here and the validator rejects it; silently ignoring a
            # stray one at runtime is correct -- it can never change what delete_now does.
            return ZipDeletePolicy(action=ZIP_DELETE_NOW, days=0)
        if action == ZIP_DELETE_AFTER_X_DAYS:
            days = raw.get("days")
            # `isinstance(days, bool)` is excluded explicitly: `True` would otherwise pass the
            # int check and silently become a one-day retention window.
            if isinstance(days, bool) or not isinstance(days, int) or days < 0:
                raise FrameworkConfigError(
                    f"source_zip_handling.delete_source_after_extract: unsupported value {raw!r} -- expected a "
                    f"boolean, {{'action': 'delete_now'}}, or {{'action': 'delete_after_x_days', 'days': <int>}}"
                )
            return ZipDeletePolicy(action=ZIP_DELETE_AFTER_X_DAYS, days=days)
    raise FrameworkConfigError(
        f"source_zip_handling.delete_source_after_extract: unsupported value {raw!r} -- expected a boolean, "
        f"{{'action': 'delete_now'}}, or {{'action': 'delete_after_x_days', 'days': <int>}}"
    )


def sweep_aged_archives(
    source_zip_dir: str,
    zip_file_pattern: str,
    days: int,
    exclude_paths: Optional[Iterable[str]] = None,
) -> List[str]:
    """Delete every already-aged archive in a landing directory. Returns the deleted paths.

    Executes the ``delete_after_x_days`` half of :class:`ZipDeletePolicy`. Every file in
    ``source_zip_dir`` whose name matches ``zip_file_pattern`` and whose ``os.path.getmtime``
    is older than ``days`` days is removed, except anything listed in ``exclude_paths``.

    **Why a lazy sweep rather than a scheduler.** Extraction runs inside a single Lakeflow
    pipeline update, which cannot sleep or schedule future work. ``delete_after_x_days`` is
    therefore enforced opportunistically: each update sweeps the landing directory for archives
    that have *already* aged past the threshold. With no subsequent pipeline update, nothing is
    ever deleted -- a documented, accepted property of this design, not a bug.

    **Why ``fnmatchcase`` and not ``fnmatch``.** Identical rationale to
    ``ingestion/readers.py::_apply_source_zip_handling``'s own matching, and it must stay
    identical to it: ``fnmatch.fnmatch`` case-normalizes per the *local* OS (case-insensitive on
    Windows, where this framework is commonly developed; case-sensitive on Linux, where
    Databricks compute actually runs). A sweep that matched more files than the extraction loop
    did -- which is exactly what a case-insensitive match on a case-sensitive filesystem
    produces -- would delete archives no flow has ever ingested.

    **Why ``days=0`` deletes everything eligible.** ``0`` means "no age threshold": the cutoff is
    "now", so every matched, non-excluded archive already on disk is older than it. That is the
    documented meaning of ``0`` at the spec level too, and it is the only reading that keeps
    ``days`` monotonic (a smaller number can never retain more).

    Parameters
    ----------
    source_zip_dir:
        The landing directory to sweep (a Unity Catalog Volume path in production).
    zip_file_pattern:
        The same glob the owning flow ingests with, so a sweep can only ever remove archives
        that flow itself is responsible for -- sibling flows sharing one incoming directory
        (the ``spec_16`` shape) must never have their archives swept by someone else's policy.
    days:
        Age threshold in days; ``0`` means no threshold.
    exclude_paths:
        Paths the caller has already handled in this same update and must not be swept.

    Returns
    -------
    list[str]
        The archives actually deleted, in sorted-name order.

    Notes
    -----
    Never raises. A per-file ``OSError`` (permission, a concurrent writer, a Volumes FUSE
    hiccup) is logged at WARNING and the sweep continues to the next candidate; an unlistable
    directory is logged and returns an empty list. Landing-zone cleanup is housekeeping that
    runs *after* a successful ingest -- failing the pipeline update over it would discard a
    perfectly good extraction for a reason the operator cannot act on from the pipeline.
    """
    excluded = {os.path.abspath(path) for path in (exclude_paths or ())}
    cutoff_timestamp = time.time() - days * _SECONDS_PER_DAY
    deleted_paths: List[str] = []

    try:
        directory_entries = sorted(os.listdir(source_zip_dir))
    except OSError as exc:
        logger.warning("delete_after_x_days sweep: failed to list '%s' -- skipping sweep: %s", source_zip_dir, exc)
        return deleted_paths

    for name in directory_entries:
        if not fnmatch.fnmatchcase(name, zip_file_pattern):
            continue
        candidate_path = os.path.join(source_zip_dir, name)
        if os.path.abspath(candidate_path) in excluded:
            continue
        try:
            if not os.path.isfile(candidate_path):
                # A directory whose name happens to match the glob is not an archive; never
                # recurse into or remove it -- the pattern selects files to ingest, nothing else.
                continue
            if os.path.getmtime(candidate_path) > cutoff_timestamp:
                continue
            os.remove(candidate_path)
        except OSError as exc:
            logger.warning("delete_after_x_days sweep: failed to remove '%s': %s", candidate_path, exc)
            continue
        deleted_paths.append(candidate_path)
        logger.info("delete_after_x_days sweep: removed archive older than %d day(s): %s", days, candidate_path)

    return deleted_paths


# ---------------------------------------------------------------------------------------
# Compression: build to local scratch disk (never a growing in-memory buffer), stream out
# in bounded chunks, optionally on an executor.
# ---------------------------------------------------------------------------------------


def _build_archive_to_local_temp(
    candidate_files: List[str], source_dir: str, passphrase_bytes: Optional[bytes]
) -> str:
    """Build the ZIP into a real local-disk temporary file -- always seekable, exactly like
    the ``io.BytesIO()`` buffer this replaces, but bounded by local disk rather than the
    calling process's heap. ``pyzipper``/``zipfile`` already streams each source file's bytes
    from disk in small chunks internally (this was never the source of the memory risk);
    what used to grow unboundedly was the *destination* -- an in-memory buffer sized to the
    whole finished archive. Returns the local temp file's path; the caller is responsible for
    streaming it out to its real destination and removing it afterward (success or failure).
    """
    fd, local_zip_path = tempfile.mkstemp(suffix=".zip")
    os.close(fd)
    try:
        with pyzipper.AESZipFile(
            local_zip_path,
            "w",
            compression=pyzipper.ZIP_DEFLATED,
            encryption=pyzipper.WZ_AES if passphrase_bytes else None,
        ) as archive:
            if passphrase_bytes is not None:
                archive.setpassword(passphrase_bytes)
                archive.setencryption(pyzipper.WZ_AES, nbits=256)
            for file_path in candidate_files:
                archive.write(file_path, arcname=os.path.relpath(file_path, source_dir))
    except Exception:
        # Don't leak a half-built temp file on the local disk if archive construction fails
        # partway through.
        try:
            os.remove(local_zip_path)
        except OSError:
            pass
        raise
    return local_zip_path


def _stream_copy_file(src_path: str, dest_path: str, chunk_size: int = 8 * 1024 * 1024) -> None:
    """Copy ``src_path`` to ``dest_path`` in bounded-size chunks -- never materializes the
    whole file in memory (unlike the old ``buffer.getvalue()`` single-shot write), while still
    performing exactly one, uninterrupted, sequential write pass against ``dest_path`` -- the
    only write pattern Unity Catalog Volumes' FUSE mount supports."""
    with open(src_path, "rb") as src, open(dest_path, "wb") as dest:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dest.write(chunk)


def _build_and_write_archive(
    candidate_files: List[str], source_dir: str, output_zip_path: str, passphrase_bytes: Optional[bytes]
) -> None:
    """Build to local scratch disk, stream out to ``output_zip_path``, then always clean up
    the local scratch file -- the in-process counterpart to :func:`_compress_on_executor`."""
    local_zip_path = _build_archive_to_local_temp(candidate_files, source_dir, passphrase_bytes)
    try:
        _stream_copy_file(local_zip_path, output_zip_path)
    finally:
        try:
            os.remove(local_zip_path)
        except OSError:
            pass


_COMPRESS_RESULT_SCHEMA = StructType(
    [
        StructField("success", BooleanType(), False),
        StructField("error_message", StringType(), True),
    ]
)


def _compress_on_executor(
    spark: SparkSession,
    candidate_files: List[str],
    source_dir: str,
    output_zip_path: str,
    passphrase_bytes: Optional[bytes],
) -> None:
    """Run :func:`_build_and_write_archive` on a Spark *executor* via a single-row
    ``mapInPandas`` job, instead of in the calling process -- see this module's docstring for
    why ``mapInPandas`` (never the RDD API) is used, and the same closure-based
    passphrase-handling note as :func:`_extract_on_executor`.

    Only reachable from ``archive/zip_ingestion_pipeline.py::ingest_zip_batch``, the one real
    caller of :func:`compress_and_encrypt_sink` that has a live ``SparkSession`` available.
    ``archive/pgp_zip_sink.py``'s ``commit()`` always calls ``compress_and_encrypt_sink`` with
    ``spark=None`` (see that module's docstring for why no SparkSession is ever available in
    that restricted worker process) and so always falls back to :func:`_build_and_write_archive`
    running in-process instead -- it still gets the disk-spill memory fix, just not executor
    placement, since there is no SparkSession there to dispatch a job through at all.
    """

    def _run_partition(batches):
        import pandas as pd

        for _ in batches:
            try:
                _build_and_write_archive(candidate_files, source_dir, output_zip_path, passphrase_bytes)
                yield pd.DataFrame([{"success": True, "error_message": None}])
            except Exception as exc:  # noqa: BLE001
                yield pd.DataFrame([{"success": False, "error_message": str(exc)}])

    marker_df = spark.createDataFrame(
        [(1,)], schema=StructType([StructField("_marker", IntegerType(), False)])
    ).coalesce(1)

    result = marker_df.mapInPandas(_run_partition, schema=_COMPRESS_RESULT_SCHEMA).collect()[0]
    if not result["success"]:
        raise ArchiveError(result["error_message"])


def compress_and_encrypt_sink(
    spark: Optional[SparkSession],
    source_dir: str,
    output_zip_path: str,
    secret_catalog: Optional[str] = None,
    secret_schema: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    include_glob_suffixes: Tuple[str, ...] = (".csv", ".json", ".parquet", ".avro", ".txt"),
) -> str:
    """Bundle partitioned egress-sink output files into a single (optionally encrypted) ZIP.

    Parameters
    ----------
    spark:
        Active SparkSession, used to resolve ``secret_catalog``/``secret_schema``/
        ``secret_key`` into a passphrase, and (when not ``None``) to dispatch the archive
        build onto a Spark executor rather than running it in the calling process -- see this
        module's docstring. May be ``None`` when ``passphrase`` is supplied directly instead
        (see below) -- it is never touched in that case, and the build simply runs
        in-process.
    source_dir:
        Directory containing the partitioned sink output (e.g. a Spark ``.write`` target).
    output_zip_path:
        Destination path for the resulting ZIP archive.
    secret_catalog, secret_schema, secret_key:
        Unity Catalog secret coordinates for an AES-256 archive password, resolved via
        ``dbutils.secrets.get()`` right here. Leave all ``None`` (along with ``passphrase``)
        to produce a plain ZIP. Ignored when ``passphrase`` is supplied.
    passphrase:
        An **already-resolved** plaintext AES-256 archive password, used as-is with no
        further secret resolution. Exists for callers that cannot call ``dbutils`` from where
        they run: ``archive/pgp_zip_sink.py``'s ``commit()`` executes in a dedicated "python
        streaming data source runtime" worker process that -- confirmed live -- cannot
        construct a working ``dbutils`` gateway (``resolve_secret_value`` fails there with
        ``[Errno 13] Permission denied: '/databricks/spark/./bin/spark-submit'``, the same
        class of restriction Databricks documents for calling ``dbutils`` from inside a UDF).
        Such callers must resolve the secret earlier, in a context where ``dbutils`` actually
        works (e.g. ``engine/sink_registration.py``, which runs in the normal pipeline
        graph-definition process), and pass the resolved value through here.
    include_glob_suffixes:
        File suffixes eligible for inclusion; Spark's ``_SUCCESS``/``_committed_*`` marker
        files and other non-data artifacts are always excluded.

    Returns
    -------
    str
        The path to the created ZIP archive.

    Raises
    ------
    ArchiveError
        If pyzipper is unavailable, ``source_dir`` doesn't exist, no eligible files are
        found, or compression fails.
    """
    if not _PYZIPPER_AVAILABLE:
        raise ArchiveError("pyzipper is required for archive compression but is not installed.")
    if not os.path.isdir(source_dir):
        raise ArchiveError(f"source_dir does not exist or is not a directory: {source_dir}")

    candidate_files = [
        os.path.join(root, fname)
        for root, _dirs, files in os.walk(source_dir)
        for fname in files
        if fname.lower().endswith(include_glob_suffixes)
    ]
    if not candidate_files:
        raise ArchiveError(f"No eligible data files found under '{source_dir}' to archive.")

    if passphrase is not None:
        passphrase_bytes = passphrase.encode("utf-8")
    else:
        try:
            passphrase_bytes = _resolve_optional_passphrase(spark, secret_catalog, secret_schema, secret_key)
        except Exception as exc:  # noqa: BLE001
            raise ArchiveError(f"Failed to resolve archive passphrase for '{output_zip_path}': {exc}") from exc

    try:
        os.makedirs(os.path.dirname(output_zip_path) or ".", exist_ok=True)
        if spark is not None:
            _compress_on_executor(spark, candidate_files, source_dir, output_zip_path, passphrase_bytes)
        else:
            _build_and_write_archive(candidate_files, source_dir, output_zip_path, passphrase_bytes)
    except Exception as exc:  # noqa: BLE001
        raise ArchiveError(f"Failed to compress sink output '{source_dir}' into '{output_zip_path}': {exc}") from exc

    logger.info("Archived %d file(s) from '%s' into '%s'", len(candidate_files), source_dir, output_zip_path)
    return output_zip_path
