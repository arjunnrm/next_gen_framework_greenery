"""Ingestion source readers: autoloader (cloudFiles), zerobus (Delta stream), asn1 (binary + decode).

All file discovery/reading here is fully distributed -- Auto Loader's ``cloudFiles`` source
lists and reads files across executors natively (never collects file content to the
driver), and ``asn1``'s decode step (``asn1/decoder.py``) runs via ``mapInPandas``, also
executor-side. See ``asn1/decoder.py``'s module docstring for the ASN.1-specific distributed
decode design.

Pre-ingest archive handling (``source_zip_handling``) also lives here: matched landing-zone
archives are optionally PGP-decrypted and unzipped into the Auto Loader path *before* the
reader is built, and their post-extraction lifecycle (delete now / age-based sweep / keep
forever) is driven by ``delete_source_after_extract``. See :func:`_apply_source_zip_handling`
for why an archive the policy *retains* is marked as already-extracted, and why the age-based
sweep excludes only the archives whose extraction failed in the current update.
"""

import fnmatch
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from flowx.lakeflow_framework.archive.zip_utils import (
    ZIP_DELETE_AFTER_X_DAYS,
    ZipDeletePolicy,
    extract_encrypted_zip,
    resolve_zip_delete_policy,
    sweep_aged_archives,
)
from flowx.lakeflow_framework.asn1.decoder import decode_asn1_binary_stream
from flowx.lakeflow_framework.crypto.pgp import pgp_decrypt
from flowx.lakeflow_framework.crypto.secrets import resolve_secret_ref
from flowx.lakeflow_framework.exceptions import ArchiveError, FrameworkConfigError
from flowx.lakeflow_framework.storage.table_properties import CLEAN_SOURCE_MODE_MAP

logger = logging.getLogger("common.ingestion.readers")

# Registry of supported pre-extraction decryption algorithms, dispatched by
# source_zip_handling.pre_extraction_decryption.type -- see onboarding/spec_validator.py's
# ALLOWED_PRE_EXTRACTION_DECRYPTION_TYPES (kept in sync: adding a new algorithm means adding
# a handler here AND to that allowed-values set, never restructuring the schema).


def _decrypt_pgp(spark: SparkSession, data: bytes, config: Dict[str, Any]) -> bytes:
    """``pre_extraction_decryption.type == "pgp"`` handler.

    ``private_key_secret`` is always required -- the recipient's ASCII-armored PGP private
    key. ``passphrase_secret`` is optional: a real, properly-secured PGP private key is
    routinely passphrase-protected (unlike this project's own throwaway test keypairs, which
    were deliberately generated without one), so this must be independently configurable, not
    assumed absent. Both resolve via ``resolve_secret_ref`` -- this function runs at Lakeflow
    graph-*execution* time inside the normal pipeline driver process (unlike
    ``archive/pgp_zip_sink.py``'s ``commit()``, which runs in a separate, restricted worker
    runtime where ``dbutils`` is confirmed live to fail -- see that module's docstring), so
    resolving secrets lazily, right here, is the correct and already-proven-working pattern
    (matching ``_apply_source_zip_handling``'s existing ZIP-password resolution, immediately
    below in this same module).
    """
    private_key_armored = resolve_secret_ref(spark, config["private_key_secret"])
    passphrase_secret = config.get("passphrase_secret")
    passphrase = resolve_secret_ref(spark, passphrase_secret) if passphrase_secret else None
    return pgp_decrypt(data, private_key_armored, passphrase=passphrase)


_PRE_EXTRACTION_DECRYPTION_HANDLERS = {"pgp": _decrypt_pgp}


#: Days a landing file is kept before ``clean_source`` archives/deletes it, when
#: ``landing_retention_policy.retention_days`` is omitted. A framework default is stated
#: explicitly rather than left to Auto Loader's own so that "what happens to my landing files"
#: is answerable from the spec alone -- an operator reading a spec with ``clean_source: delete``
#: and no ``retention_days`` must not have to know a runtime's internal default to know when
#: their files disappear.
DEFAULT_LANDING_RETENTION_DAYS = 7


def resolve_landing_retention_policy(source_config: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve one flow's ``landing_retention_policy`` block into a fully-defaulted dict.

    Returns ``{"clean_source": str, "archive_path": Optional[str], "retention_days": int}``,
    where ``clean_source`` is the **resolved** mode -- already degraded to ``"off"`` when an
    ``"archive"`` policy has no usable ``archive_path`` -- and ``retention_days`` is always an
    ``int`` (:data:`DEFAULT_LANDING_RETENTION_DAYS` when omitted).

    Deliberately pure: it takes a plain dict and touches neither Spark nor a reader, so the
    whole decision table (degradation, defaulting, mode validation) is unit-testable without a
    session. :func:`_apply_landing_retention_policy` is then a thin, obviously-correct
    translation of this result into ``cloudFiles`` options.

    Three decisions worth their justification:

    * **An ``"archive"`` policy with an empty/absent ``archive_path`` degrades to ``"off"``
      with a WARNING rather than raising.** This used to be a hard
      :class:`FrameworkConfigError`, which meant that blanking ``archive_path`` -- the one
      edit an operator makes to *temporarily stop archiving without deleting the whole block
      and losing its settings* -- failed the pipeline update outright. Degrading is the
      documented behaviour: no retention or cleanup action of any kind is taken, and the
      WARNING says so in those words so it cannot be mistaken for "archiving quietly kept
      working".
    * **``retention_days: 0`` is legal and means "no age threshold"** -- a file becomes
      eligible the moment Auto Loader has committed it. It must therefore be distinguished
      from "omitted" by an ``is None`` test, never by truthiness.
    * **``clean_source: "delete"`` ignores ``archive_path`` entirely.** Deleting has no
      destination; an ``archive_path`` left behind from a previous ``"archive"`` configuration
      is a harmless ignored sibling, not an error worth failing an update over.

    Raises
    ------
    FrameworkConfigError
        If ``clean_source`` is not one of ``CLEAN_SOURCE_MODE_MAP``'s keys -- an unknown mode
        is a typo whose intended meaning cannot be guessed, unlike the degradations above -- or
        if ``retention_days`` is present but cannot be coerced to an ``int``.
    """
    retention_policy = source_config.get("landing_retention_policy") or {}
    clean_source = retention_policy.get("clean_source", "off")
    if clean_source not in CLEAN_SOURCE_MODE_MAP:
        raise FrameworkConfigError(f"Unsupported clean_source value: {clean_source}")

    archive_path = retention_policy.get("archive_path")
    if clean_source == "archive" and not archive_path:
        logger.warning(
            "landing_retention_policy.clean_source='archive' but archive_path is empty/absent -- degrading to "
            "clean_source='off': NO retention or cleanup action will be taken for this source."
        )
        clean_source = "off"

    retention_days = retention_policy.get("retention_days")
    if retention_days is None:
        retention_days = DEFAULT_LANDING_RETENTION_DAYS
    try:
        retention_days = int(retention_days)
    except (TypeError, ValueError) as exc:
        # int() on a non-numeric value raises a bare ValueError/TypeError whose message names
        # neither the offending spec field nor the flow, so an operator sees an unattributed
        # pipeline crash. This function is also reachable from a hand-edited control-table row
        # that never passed through onboarding validation, so the coercion is defence in depth:
        # it must still fail the update, but as the FrameworkConfigError already documented
        # above -- the exception type every caller of this module already handles and reports.
        raise FrameworkConfigError(
            f"landing_retention_policy.retention_days must be an integer number of days, got {retention_days!r}"
        ) from exc

    return {"clean_source": clean_source, "archive_path": archive_path, "retention_days": retention_days}


def _apply_landing_retention_policy(reader, source_config: Dict[str, Any]):
    """Apply Auto Loader ``cleanSource`` options from a ``landing_retention_policy`` block.

    **Invariant: this is an Auto Loader (``cloudFiles``) concern only.** It is called from
    :func:`read_autoloader_source` and :func:`read_asn1_source` and from nowhere else. Neither
    :func:`_apply_source_zip_handling` nor ``archive/zip_ingestion_pipeline.py`` may ever read
    ``landing_retention_policy``: those run *before* Auto Loader ever sees a file and operate on
    raw pre-extraction archives, which have their own, separately-configured lifecycle
    (``source_zip_handling.delete_source_after_extract``). Wiring landing retention into the ZIP
    path would give one spec block two unrelated meanings and could delete an operator's only
    copy of an archive that has not yet been ingested. ``zerobus`` sources have no landing zone
    at all, so a ``landing_retention_policy`` on one is rejected at onboarding time.

    Emits **no** ``cloudFiles.cleanSource*`` option at all when the resolved mode is ``"off"``
    (explicitly configured, or degraded from ``"archive"`` by
    :func:`resolve_landing_retention_policy`) -- ``OFF`` is Auto Loader's own default, so not
    setting the option is byte-for-byte equivalent while avoiding sending a runtime an option it
    may not need to accept. The reader is returned untouched.
    """
    resolved = resolve_landing_retention_policy(source_config)
    clean_source = resolved["clean_source"]
    if clean_source == "off":
        return reader

    reader = reader.option("cloudFiles.cleanSource", CLEAN_SOURCE_MODE_MAP[clean_source])
    if clean_source == "archive":
        reader = reader.option("cloudFiles.cleanSource.moveDestination", resolved["archive_path"])
    # Unconditional now that retention_days is always resolved to an int -- `0` is a legal
    # value meaning "no age threshold", so it must reach Auto Loader as "0 days" rather than
    # being skipped by a truthiness test.
    reader = reader.option("cloudFiles.cleanSource.retentionDuration", f"{resolved['retention_days']} days")
    return reader


def _apply_common_autoloader_options(reader, source_config: Dict[str, Any]):
    """Options shared by both file-based Auto Loader readers (autoloader and asn1): glob file
    selection (``file_pattern``) and arbitrary Spark-reader passthrough (``reader_options``,
    e.g. delimiter/header/quote settings for a delimited format).

    ``file_pattern`` maps to Spark's generic file-source ``pathGlobFilter`` for **every**
    format, not to ``cloudFiles.fileNamePattern``. That distinction is the whole point of this
    note, because the obvious-looking ``cloudFiles.``-prefixed spelling does not exist:

    * Auto Loader validates every option key carrying the ``cloudFiles.`` prefix against a
      closed whitelist and rejects anything outside it with ``CF_UNKNOWN_OPTION_KEYS_ERROR``.
      That validation is **format-independent** -- it inspects the key, never
      ``cloudFiles.format`` -- so ``cloudFiles.fileNamePattern`` is rejected for csv/json/
      parquet/avro exactly as it is for ``binaryFile``.
    * ``pathGlobFilter`` (documented alongside its ``fileNamePattern`` synonym under Auto
      Loader's *generic* options, i.e. deliberately un-prefixed) is passed through to the
      underlying file source and is honoured by ``cloudFiles`` and ``binaryFile`` alike.

    Confirmed live on 2026-08-29 by ``TC-ING-004``, whose ASN.1 flow died at stream start with
    ``[CF_UNKNOWN_OPTION_KEYS_ERROR] Found unknown option keys: cloudFiles.filenamepattern``
    (Auto Loader lowercases the key in that message; it is the same option). ``TC-ING-004`` is
    the only spec in the corpus that sets ``file_pattern``, which is why the prefixed spelling
    survived undetected in the non-``binaryFile`` path for so long -- no other flow exercised
    it. Routing every format through ``pathGlobFilter`` removes that latent trap rather than
    fixing only the one format that happened to surface it.
    """
    file_pattern = source_config.get("file_pattern")
    if file_pattern:
        reader = reader.option("pathGlobFilter", file_pattern)
    for option_key, option_value in source_config.get("reader_options", {}).items():
        reader = reader.option(option_key, option_value)
    return reader


def read_autoloader_source(spark: SparkSession, source_config: Dict[str, Any]) -> DataFrame:
    """Build a streaming Auto Loader (``cloudFiles``) reader from ``source_config``.

    Expected keys: ``path`` (landing location), ``format`` (csv/parquet/json/avro/text),
    ``schema_location``, optional ``file_pattern`` (regex/glob file selection, e.g.
    ``"orc_*"``), optional ``reader_options`` passthrough dict, optional
    ``schema_evolution_mode`` (addNewColumns/addNewColumnsWithTypeWidening/rescue/failOnNewColumns/none), an
    optional ``landing_retention_policy`` block (``clean_source`` -- default ``"off"``;
    ``archive_path`` -- optional even for ``"archive"``, where an empty/absent one degrades the
    whole policy to ``"off"``; ``retention_days`` -- default
    :data:`DEFAULT_LANDING_RETENTION_DAYS`, and ``0`` legitimately means "no age threshold"; see
    :func:`resolve_landing_retention_policy`), and optional ``source_zip_handling`` (same shape as the ``asn1``
    reader's -- see :func:`_apply_source_zip_handling`) to extract every (optionally
    PGP-then-ZIP-wrapped) archive matched in a landing directory into ``path`` before Auto
    Loader reads it, all within this one pipeline update. JSON sources additionally support
    ``explode_columns`` (see
    ``ingestion/json_flattening.py``) and ``data_standardization_sql`` (see
    ``ingestion/standardization_sql.py``) -- both applied by the caller, not here, since
    they're post-read DataFrame transforms rather than reader options.

    Raises
    ------
    FrameworkConfigError
        If required keys are missing or the retention/evolution mode is unsupported.
    ArchiveError
        If ``source_zip_handling`` is enabled and extraction fails.
    """
    try:
        _apply_source_zip_handling(spark, source_config)

        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_config["format"])
            .option("cloudFiles.schemaLocation", source_config["schema_location"])
        )

        evolution_mode = source_config.get("schema_evolution_mode")
        if evolution_mode:
            if evolution_mode not in {"addNewColumns", "addNewColumnsWithTypeWidening", "rescue", "failOnNewColumns", "none"}:
                raise FrameworkConfigError(f"Unsupported schema_evolution_mode: {evolution_mode}")
            reader = reader.option("cloudFiles.schemaEvolutionMode", evolution_mode)

        reader = _apply_landing_retention_policy(reader, source_config)
        reader = _apply_common_autoloader_options(reader, source_config)

        return reader.load(source_config["path"])
    except KeyError as exc:
        raise FrameworkConfigError(f"autoloader source_config missing required key {exc}") from exc


def read_zerobus_source(spark: SparkSession, source_config: Dict[str, Any]) -> DataFrame:
    """Build a streaming reader over an existing Delta table landed via Zerobus.

    Expected keys: ``source_catalog``, ``source_schema``, ``source_table``, and optional
    ``starting_version`` / ``max_bytes_per_trigger`` streaming reader options.

    Raises
    ------
    FrameworkConfigError
        If required keys are missing.
    """
    try:
        qualified_table = (
            f"{source_config['source_catalog']}.{source_config['source_schema']}.{source_config['source_table']}"
        )
        reader = spark.readStream.format("delta")
        if source_config.get("starting_version") is not None:
            reader = reader.option("startingVersion", source_config["starting_version"])
        if source_config.get("max_bytes_per_trigger"):
            reader = reader.option("maxBytesPerTrigger", source_config["max_bytes_per_trigger"])
        return reader.table(qualified_table)
    except KeyError as exc:
        raise FrameworkConfigError(f"zerobus source_config missing required key {exc}") from exc


#: Filename suffix of the sidecar marker :func:`_apply_source_zip_handling` drops next to a
#: source archive it extracted successfully but deliberately did NOT delete -- i.e. whenever
#: ``delete_source_after_extract`` resolves to ``delete_after_x_days`` or to the legacy ``false``
#: ("never"). Those policies leave the archive sitting in the landing directory on purpose, so
#: without a marker the very same file is re-matched by every subsequent pipeline update and
#: therefore re-PGP-decrypted and re-extracted forever. On a large encrypted archive that is an
#: expensive, permanently recurring cost that buys nothing. The marker is a *hidden sibling file
#: in the landing directory* rather than state under ``target_volume_path`` because that
#: directory is precisely what Auto Loader ingests: any bookkeeping file written there would be
#: picked up as data.
_EXTRACTION_MARKER_SUFFIX = ".__framework_extracted__"


def _extraction_marker_name(zip_name: str) -> str:
    """Sidecar marker filename for one retained source archive: a leading dot, the archive name,
    then :data:`_EXTRACTION_MARKER_SUFFIX`.

    The leading dot plus the trailing suffix keep the marker clear of the ordinary
    ``zip_file_pattern`` shapes (``*.zip``, ``orders_*.zip``, ``orders_*``) -- the dot defeats
    prefix globs and the suffix defeats extension globs. That is a convenience rather than a
    guarantee for an arbitrary operator-authored pattern, which is why
    :func:`_extraction_marker_path` still tests the actual configured pattern before using it."""
    return f".{zip_name}{_EXTRACTION_MARKER_SUFFIX}"


def _extraction_marker_path(source_zip_dir: str, zip_name: str, zip_file_pattern: str) -> Optional[str]:
    """Marker path for ``zip_name``, or ``None`` when markers are unsafe for this flow's pattern.

    Returns ``None`` -- disabling the skip-re-extraction bookkeeping for this archive only, so
    the behaviour degrades to "re-extract on every update" (merely wasteful) and never to
    "delete or ingest something unexpected" -- if the marker's own name would match
    ``zip_file_pattern``. A marker matching the pattern would be treated as an archive by the
    extraction loop AND become a candidate for the ``delete_after_x_days`` sweep, so this is a
    correctness guard, not a cosmetic one.
    """
    marker_name = _extraction_marker_name(zip_name)
    if fnmatch.fnmatchcase(marker_name, zip_file_pattern):
        logger.warning(
            "source_zip_handling: zip_file_pattern '%s' also matches this framework's own extraction marker "
            "'%s' -- not recording '%s' as already-extracted, so it will be re-extracted (and re-decrypted) "
            "on every pipeline update. Narrow zip_file_pattern (e.g. '*.zip') to avoid this.",
            zip_file_pattern,
            marker_name,
            zip_name,
        )
        return None
    return os.path.join(source_zip_dir, marker_name)


def _archive_already_extracted(zip_path: str, marker_path: str) -> bool:
    """True when ``zip_path`` was already extracted successfully by an earlier pipeline update.

    The test is ``marker mtime >= archive mtime``, not merely "a marker exists": a landing zone
    routinely receives a *replacement* archive under an unchanged filename (the daily
    ``orders.zip`` drop), and treating that fresh content as already-done would silently discard
    a day's data. Any ``OSError`` reading either mtime answers ``False``: re-extracting
    needlessly costs CPU, skipping wrongly loses data, so the cheap failure is the one to take.
    """
    try:
        return os.path.getmtime(marker_path) >= os.path.getmtime(zip_path)
    except OSError:
        return False


def _write_extraction_marker(marker_path: str) -> None:
    """Write/refresh the sidecar marker after a retained archive extracted successfully.

    Failures are logged, never raised. The marker is a pure optimization: a landing directory
    that refuses the write (read-only mount, restricted Volume permissions) must still get its
    ingest, and the only consequence is that the archive is re-extracted on the next update --
    exactly the pre-marker behaviour, which was correct if wasteful.
    """
    try:
        with open(marker_path, "w", encoding="utf-8") as marker_file:
            marker_file.write("extracted by flowx source_zip_handling\n")
    except OSError as exc:
        logger.warning("source_zip_handling: failed to write extraction marker '%s': %s", marker_path, exc)


def _remove_stale_extraction_markers(source_zip_dir: str, directory_entries: List[str]) -> None:
    """Remove markers whose archive is no longer present in the landing directory.

    A marker necessarily outlives its archive: the ``delete_after_x_days`` sweep only removes
    files matching ``zip_file_pattern``, and a usable marker deliberately does not match it.
    Without this pass the landing directory would accumulate one hidden file per archive ever
    ingested, forever. Works off the caller's existing ``os.listdir`` snapshot so no second
    listing of a Volumes path is needed, and never raises -- an unremovable marker is clutter,
    not a reason to fail an ingest.
    """
    existing_names = set(directory_entries)
    marker_suffix_length = len(_EXTRACTION_MARKER_SUFFIX)
    for name in directory_entries:
        if not (name.startswith(".") and name.endswith(_EXTRACTION_MARKER_SUFFIX)):
            continue
        if name[1:-marker_suffix_length] in existing_names:
            continue
        try:
            os.remove(os.path.join(source_zip_dir, name))
        except OSError as exc:
            logger.warning("source_zip_handling: failed to remove stale extraction marker '%s': %s", name, exc)


def _apply_source_zip_handling(spark: SparkSession, source_config: Dict[str, Any]) -> None:
    """Extract every optionally-encrypted ZIP-wrapped archive matched in a landing directory,
    before Auto Loader (or the ASN.1 reader) ever reads the extracted files.

    A no-op when ``source_zip_handling.enabled`` is absent/false. ``source_zip_path`` is a
    *directory* (a landing-zone Unity Catalog Volume path), never a single file -- a real
    landing zone routinely accumulates more than one archive between pipeline updates (one
    per source extract/drop), so ``zip_file_pattern`` (a glob, matched the same way
    ``file_pattern``/``pathGlobFilter`` selects ingested files, e.g.
    ``"orders_*.zip"`` or ``"*.zip"``) picks which archive(s) in that directory this update
    processes. Every matching archive is extracted independently (see
    :func:`_extract_one_zip_file`), so one corrupt archive doesn't block the others.

    Idempotent across repeated pipeline updates, by two different mechanisms depending on the
    deletion policy. Under ``delete_now`` the archive is removed once extracted, so a later
    update finding the directory gone or no files left matching the pattern is expected
    steady-state, not an error -- extraction ran once, Auto Loader picks up the already-extracted
    files on every update after that. Under the policies that deliberately *retain* the archive
    (``delete_after_x_days``, and the legacy ``false``) that file is still there on the next
    update and matches the pattern again, so idempotency cannot come from its absence: a
    successful extraction instead drops a sidecar marker beside it
    (:data:`_EXTRACTION_MARKER_SUFFIX`) and later updates skip any archive whose marker is at
    least as new as the archive itself (:func:`_archive_already_extracted`). Without that, every
    single update would re-run the PGP decryption and re-write the same members into
    ``target_volume_path`` for as long as the archive is retained -- forever, under ``false``.
    Comparing mtimes rather than only testing for the marker's existence is what keeps a
    *replaced* archive (same filename, fresh content -- the routine daily-drop shape) from being
    mistaken for one already handled.

    ``delete_source_after_extract`` decides what happens to the *raw source archives* once
    they're extracted, and is normalized once per call by
    :func:`archive.zip_utils.resolve_zip_delete_policy` into a
    :class:`~archive.zip_utils.ZipDeletePolicy` that is then threaded through the whole
    extraction loop -- resolved once rather than re-read per archive so every archive matched in
    a single update provably gets the same lifecycle. It accepts the legacy plain boolean
    (``true``/omitted -> ``delete_now``, ``false`` -> keep forever) and, since v1.3.0, a nested
    ``{"action": "delete_now"}`` / ``{"action": "delete_after_x_days", "days": N}`` object.
    ``delete_after_x_days`` never deletes an archive as part of extracting it; instead, once the
    extraction loop has finished, this function runs
    :func:`archive.zip_utils.sweep_aged_archives` over the landing directory to remove archives
    that have already aged past the ``days`` threshold. The sweep runs after the loop, not inside
    it, and its own failures are logged rather than raised -- landing-zone housekeeping must
    never fail an otherwise-successful ingest.

    **Only the archives whose extraction FAILED in this update are excluded from that sweep.**
    An earlier revision excluded every archive this update matched, which made the policy
    provably inert: the sweep re-lists the same directory with the same pattern and the same
    ``fnmatchcase``, so "every archive this update matched" is by construction the entire
    candidate set, and -- because ``delete_after_x_days`` also keeps the archive on disk -- the
    same file was re-matched and re-excluded by every subsequent update, forever. The only
    justification for excluding anything was ever "a failed extraction must never cause its own
    source to be swept in the same run": that archive is the operator's remaining copy, needed to
    inspect and retry, and sweeping it would destroy it. That reasoning covers failures only. A
    successfully-extracted archive is already safely unpacked into ``target_volume_path``, so it
    is left to age normally in the landing directory and is swept by whichever later update runs
    past the threshold -- which is exactly the documented semantics of the policy ("an archive
    dropped today is deleted by whichever update runs at least N days later"). One corollary
    worth stating plainly: with ``days: 0`` ("no age threshold") a just-extracted archive is
    swept in the same update, and an archive that had already aged before a pattern change first
    matched it is extracted and then swept immediately -- both are the threshold behaving as
    documented, not a second, hidden deletion path.

    The sweep also runs on an update that matched **nothing** to extract, so the "already
    extracted, nothing to do" path can never skip housekeeping that still has aged archives to
    remove.

    **Invariant: ``landing_retention_policy`` is never read here.** That block configures Auto
    Loader's own ``cloudFiles.cleanSource`` over the *extracted* landing files and is applied
    exclusively by :func:`_apply_landing_retention_policy`. The raw pre-extraction ZIPs this
    function handles are governed only by ``delete_source_after_extract`` -- see
    :func:`_apply_landing_retention_policy`'s docstring for why conflating the two would be
    dangerous rather than merely redundant.

    ``pre_extraction_decryption`` is fully optional at every level -- absent, or ``{}``, means
    a plain, unencrypted, non-password-protected ZIP. Two independent, combinable concerns
    live under it: ``type`` (e.g. ``{"type": "pgp", "private_key_secret": {...}}``) decrypts
    each source file *before* it's treated as a ZIP -- the common real-world shape is
    "PGP-encrypt, then ZIP" (or vice versa in transit), so this framework decrypts the whole
    file to a temporary path first, then hands the now-plain ZIP to
    :func:`archive.zip_utils.extract_encrypted_zip`; dispatched via a type registry
    (``_PRE_EXTRACTION_DECRYPTION_HANDLERS``) so a future algorithm is a new registered
    handler, never a schema change. ``secret_passphrase`` is the AES-256 password on the ZIP
    archive itself, resolved independently of ``type`` -- present with no ``type`` means
    "just a password-protected ZIP, no outer decryption layer"; both present means "decrypt
    the envelope, then extract the password-protected ZIP it contained."

    Shared by both file-based readers (:func:`read_autoloader_source` and
    :func:`read_asn1_source`) -- not ``zerobus``, which streams an existing Delta table
    and has no landing-zone files to unpack. Runs inside the reader function, i.e. at the
    pipeline's actual *execution* time (not merely graph-definition time), so
    decrypt -> unzip -> Auto Loader ingest -> downstream transforms all happen within one
    Lakeflow Declarative Pipeline update -- no separate job task required.

    Raises
    ------
    FrameworkConfigError
        If ``source_zip_handling`` is enabled but missing ``source_zip_path`` /
        ``zip_file_pattern`` / ``target_volume_path``, ``pre_extraction_decryption.type``
        is unrecognized, or ``delete_source_after_extract`` has an unsupported shape.
    ArchiveError
        If a matched archive exists but fails to decrypt/extract (corrupt, wrong
        passphrase/key, etc).
    """
    zip_handling = source_config.get("source_zip_handling")
    if not zip_handling or not zip_handling.get("enabled"):
        return

    try:
        source_zip_dir = zip_handling["source_zip_path"]
        zip_file_pattern = zip_handling["zip_file_pattern"]
        target_volume_path = zip_handling["target_volume_path"]
    except KeyError as exc:
        raise FrameworkConfigError(f"source_config.source_zip_handling missing required key {exc}") from exc

    # Resolved before any filesystem work: a malformed deletion policy is a configuration
    # error that must surface on every update, not only on the updates that happen to find a
    # matching archive -- otherwise a typo sits latent until the day files actually land.
    delete_policy = resolve_zip_delete_policy(zip_handling.get("delete_source_after_extract"))

    if os.path.exists(source_zip_dir) and not os.path.isdir(source_zip_dir):
        # source_zip_path names a landing DIRECTORY in this design (v1 -- since replaced --
        # accepted a single ZIP file here). A path that exists but isn't a directory is a
        # real misconfiguration (e.g. an un-migrated v1-style spec, or a typo appending a
        # filename), not "not yet landed" -- those two cases must not be conflated into the
        # same silent no-op below, or the pipeline would run forever ingesting nothing with
        # no error pointing at the actual cause.
        raise FrameworkConfigError(
            f"source_config.source_zip_handling.source_zip_path '{source_zip_dir}' exists but is not a "
            f"directory -- this field names a landing directory (glob-matched via zip_file_pattern), not "
            f"a single ZIP file."
        )
    if not os.path.isdir(source_zip_dir):
        logger.info(
            "source_zip_handling: directory '%s' not present -- assuming its archive(s) were already "
            "extracted by a prior pipeline update (delete_source_after_extract) and skipping re-extraction.",
            source_zip_dir,
        )
        return

    try:
        directory_entries = os.listdir(source_zip_dir)
    except OSError as exc:
        raise ArchiveError(f"Failed to list source_zip_handling directory '{source_zip_dir}': {exc}") from exc

    # Markers whose archive has since gone (swept by delete_after_x_days, or removed by hand)
    # are dead weight -- cleaned from the snapshot just taken, before this update writes any new
    # ones, so the landing directory cannot accumulate one hidden file per archive ever ingested.
    _remove_stale_extraction_markers(source_zip_dir, directory_entries)

    # fnmatch.fnmatch case-normalizes per the *local* OS (case-insensitive on Windows, where
    # this framework is commonly developed; case-sensitive on Linux, where Databricks compute
    # actually runs) -- fnmatchcase is used deliberately instead so a pattern/filename casing
    # mismatch behaves identically everywhere, matching the case-sensitive Linux runtime this
    # code actually executes on rather than whatever OS last edited/tested it.
    matching_zip_names = sorted(name for name in directory_entries if fnmatch.fnmatchcase(name, zip_file_pattern))
    if not matching_zip_names:
        # Deliberately NOT an early return any more. The delete_after_x_days sweep below has to
        # run on this update too: "nothing new to extract" says nothing about whether archives
        # left behind by earlier updates have now aged past the threshold, and returning here
        # made the only code path that can delete them conditional on new work arriving.
        logger.info(
            "source_zip_handling: no files matching pattern '%s' in '%s' -- assuming already extracted by "
            "a prior pipeline update (delete_source_after_extract) and skipping re-extraction.",
            zip_file_pattern,
            source_zip_dir,
        )

    # Each matched file is attempted independently -- a failure on one must never prevent the
    # others from being attempted too (real scenario: 3 archives land together, 1 is corrupt;
    # the 2 good ones must still extract in this same update). Failures are collected rather
    # than raised immediately so the loop always runs to completion, then reported together --
    # this still fails the pipeline update (matching this function's documented ArchiveError
    # contract) but only after every match has genuinely been tried.
    failures: List[str] = []
    failed_zip_paths: List[str] = []
    # Only a policy that LEAVES the archive on disk can be asked to skip it next update. Under
    # delete_now the archive is gone the moment it extracts, so a marker would be orphaned
    # immediately and that path stays byte-for-byte what it always was.
    retain_archives = not delete_policy.delete_current_archive
    for zip_name in matching_zip_names:
        zip_path = os.path.join(source_zip_dir, zip_name)
        marker_path = _extraction_marker_path(source_zip_dir, zip_name, zip_file_pattern) if retain_archives else None
        if marker_path is not None and _archive_already_extracted(zip_path, marker_path):
            logger.info(
                "source_zip_handling: '%s' is retained by delete_source_after_extract and was already "
                "extracted by a prior pipeline update -- skipping re-extraction (and any PGP re-decryption). "
                "Remove '%s' to force a re-extract.",
                zip_name,
                os.path.basename(marker_path),
            )
            continue
        try:
            _extract_one_zip_file(spark, zip_path, target_volume_path, zip_handling, delete_policy)
        except ArchiveError as exc:
            logger.error("source_zip_handling: failed to extract '%s': %s", zip_name, exc)
            failures.append(f"{zip_name}: {exc}")
            # Tracked separately from the human-readable `failures` strings because this is the
            # exclusion set the sweep below needs: absolute paths, failures only.
            failed_zip_paths.append(zip_path)
            continue
        if marker_path is not None:
            _write_extraction_marker(marker_path)

    if delete_policy.action == ZIP_DELETE_AFTER_X_DAYS:
        # Runs after the whole extraction loop, and before the failure report below, so it
        # happens exactly once per update regardless of how many archives were matched, how many
        # were skipped as already-extracted, or how many failed -- including an update that
        # matched nothing at all, which still owes the directory a sweep.
        #
        # ONLY this update's failures are excluded, and the distinction is the whole point:
        # a failed archive is the operator's remaining copy, needed to inspect and retry, so
        # sweeping it in the same update that could not read it would destroy it. A SUCCESSFUL
        # one is already unpacked into target_volume_path and must be allowed to age normally --
        # excluding successes too (the previous behaviour) made the policy provably inert,
        # because the sweep re-lists this same directory with this same pattern, so the excluded
        # set was the entire candidate set and the retained archive was re-excluded on every
        # subsequent update forever. See this function's docstring for the full rationale.
        sweep_aged_archives(source_zip_dir, zip_file_pattern, delete_policy.days, exclude_paths=failed_zip_paths)

    if failures:
        raise ArchiveError(
            f"source_zip_handling: {len(failures)} of {len(matching_zip_names)} matched archive(s) in "
            f"'{source_zip_dir}' failed to extract -- {'; '.join(failures)}"
        )


def _extract_one_zip_file(
    spark: SparkSession,
    source_zip_path: str,
    target_volume_path: str,
    zip_handling: Dict[str, Any],
    delete_policy: ZipDeletePolicy,
) -> None:
    """Decrypt (if ``pre_extraction_decryption`` is configured) and extract exactly one archive
    matched by :func:`_apply_source_zip_handling`. Split out to a single-file helper so each
    file matched in the landing directory is handled independently -- one corrupt/undecryptable
    archive raises without touching the others matched in the same pipeline update.

    ``delete_policy`` is resolved once by :func:`_apply_source_zip_handling` and passed in,
    rather than each call re-reading ``zip_handling["delete_source_after_extract"]`` itself: the
    spec value now has three accepted shapes (bool, ``delete_now`` object, ``delete_after_x_days``
    object), and re-normalizing it per archive would be both wasted work and an opportunity for
    two archives in the same update to disagree about their own lifecycle. Only
    :attr:`~archive.zip_utils.ZipDeletePolicy.delete_current_archive` is consulted here -- the
    age-based sweep belongs to the update as a whole, not to any single archive, and so lives in
    the caller."""
    pre_extraction_decryption = zip_handling.get("pre_extraction_decryption") or {}
    decryption_type = pre_extraction_decryption.get("type")
    zip_path_to_extract = source_zip_path
    if decryption_type:
        # `type` absent (including `pre_extraction_decryption` entirely omitted, or `{}`)
        # means no outer decryption layer at all -- the ZIP itself may still be
        # password-protected via `secret_passphrase` below, handled independently.
        handler = _PRE_EXTRACTION_DECRYPTION_HANDLERS.get(decryption_type)
        if handler is None:
            raise FrameworkConfigError(
                f"source_config.source_zip_handling.pre_extraction_decryption.type "
                f"{decryption_type!r} is not supported (known types: {sorted(_PRE_EXTRACTION_DECRYPTION_HANDLERS)})"
            )
        try:
            with open(source_zip_path, "rb") as encrypted_file:
                encrypted_bytes = encrypted_file.read()
            decrypted_bytes = handler(spark, encrypted_bytes, pre_extraction_decryption)
            zip_path_to_extract = f"{source_zip_path}.decrypted"
            with open(zip_path_to_extract, "wb") as decrypted_file:
                decrypted_file.write(decrypted_bytes)
        except Exception as exc:  # noqa: BLE001
            raise ArchiveError(
                f"Failed to pre-extraction-decrypt '{source_zip_path}' (type={decryption_type!r}): {exc}"
            ) from exc

    # AES-256 password on the ZIP archive itself -- independent of, and combinable with, the
    # pre-extraction decryption layer above (e.g. PGP-decrypt the envelope, then extract the
    # password-protected ZIP it contained). Omitted (or `pre_extraction_decryption` entirely
    # absent) means a plain, unprotected ZIP -- extract_encrypted_zip handles that natively
    # when every secret_* argument is None.
    secret_ref = pre_extraction_decryption.get("secret_passphrase") or {}
    try:
        extract_encrypted_zip(
            spark=spark,
            source_zip_path=zip_path_to_extract,
            target_volume_path=target_volume_path,
            secret_catalog=secret_ref.get("secret_catalog"),
            secret_schema=secret_ref.get("secret_schema"),
            secret_key=secret_ref.get("secret_key"),
            delete_source_after_extract=delete_policy.delete_current_archive,
        )
    except ArchiveError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ArchiveError(f"Failed to extract source_zip_handling archive '{source_zip_path}': {exc}") from exc
    else:
        # Only reached when extract_encrypted_zip succeeded -- deliberately an `else`, not
        # part of the `finally` below. The original (pre-decryption) file is the one
        # remaining copy of this archive; deleting it after a FAILED extraction (the bug this
        # replaced: `finally` runs on every path, success or not) would destroy the only
        # copy right as the ArchiveError propagates, leaving no way to retry.
        # The original PGP-encrypted file is a distinct artifact from the decrypted ZIP
        # extract_encrypted_zip already cleaned up above -- remove it too, per the same
        # delete_source_after_extract policy, so a delete_now re-run doesn't re-decrypt every
        # time. Under delete_after_x_days (or the legacy `false`) it is deliberately left in
        # place: it is the operator's remaining copy of this archive, so it stays subject to
        # the same age-based sweep / keep-forever choice the plain-ZIP case gets, rather than
        # being silently deleted by the decryption layer alone -- and re-decryption is avoided
        # there by the caller's extraction marker, not by deleting the operator's only copy.
        # One flat condition rather than nested ifs (ruff SIM102); the ordering still reads as
        # "there IS a separate original" -> "policy says delete it" -> "it is still there".
        if (
            zip_path_to_extract != source_zip_path
            and delete_policy.delete_current_archive
            and os.path.exists(source_zip_path)
        ):
            os.remove(source_zip_path)
    finally:
        # Always safe to remove the decrypted temp intermediate regardless of success/failure
        # -- it's a throwaway artifact this function itself created, never the operator's only
        # copy of anything.
        if zip_path_to_extract != source_zip_path and os.path.exists(zip_path_to_extract):
            os.remove(zip_path_to_extract)


def read_asn1_source(spark: SparkSession, source_config: Dict[str, Any]) -> DataFrame:
    """Build a streaming binary reader for ASN.1-encoded CDR files, then decode via ``common.asn1``.

    Expected keys: ``path`` (binary landing location), ``schema_location``,
    ``asn1_schema_path`` (a real ``.asn``/ASN.1 module definition file -- not a hand-authored
    JSON field list; the Spark output schema is derived directly from this file via
    ``asn1/decoder.py::derive_asn1_field_defs`` introspection), ``asn1_codec`` (``"ber"`` or
    ``"der"``), ``asn1_pdu_name`` (the top-level ``SEQUENCE`` type in that module to decode
    each record as), optional ``file_pattern``/``reader_options`` (same as the autoloader
    reader), optional ``landing_retention_policy`` (same shape as the Auto Loader reader),
    and optional ``source_zip_handling`` (``enabled``, ``source_zip_path`` -- a landing
    *directory*, ``zip_file_pattern`` -- glob selecting which archive(s) in that directory to
    process, ``target_volume_path``, optional ``pre_extraction_decryption`` (itself fully
    optional -- absent or ``{}`` means a plain, unencrypted ZIP; ``type`` for an outer PGP
    decryption layer, ``secret_passphrase`` for an AES-password-protected ZIP, independently
    optional and combinable), optional ``delete_source_after_extract`` -- a plain boolean or a
    ``{"action": "delete_now"}`` / ``{"action": "delete_after_x_days", "days": N}`` object) -- when
    enabled, every matched archive is decrypted/extracted into ``path`` *before* Auto Loader
    ever reads it; see :func:`_apply_source_zip_handling`. File reading (via ``cloudFiles``)
    and ASN.1 decoding (via ``asn1/decoder.py``'s ``mapInPandas`` transform) are both fully
    distributed across executors -- no driver-side collection at any point.

    ``asn1_schema_path`` is a plain ``source_config`` field, unlike the old external-JSON-file
    design -- so any ``{{catalog}}``/``{{env}}`` placeholder it contains is already
    substituted by ``onboarding/spec_loader.py`` before this function ever runs, the same way
    ``path``/``schema_location`` are.

    Raises
    ------
    FrameworkConfigError
        If required keys are missing.
    ArchiveError
        If ``source_zip_handling`` is enabled and decryption/extraction fails.
    """
    try:
        _apply_source_zip_handling(spark, source_config)

        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.schemaLocation", source_config["schema_location"])
        )
        reader = _apply_landing_retention_policy(reader, source_config)
        reader = _apply_common_autoloader_options(reader, source_config)
        raw_df = reader.load(source_config["path"])
        return decode_asn1_binary_stream(
            raw_df,
            source_config["asn1_schema_path"],
            source_config["asn1_codec"],
            # .get(), not [...]: an absent/blank asn1_pdu_name is the documented request to
            # auto-detect the root PDU, not a missing required key. Subscripting here raised
            # KeyError -> "missing required key" and made detection unreachable at runtime even
            # once onboarding accepted the spec.
            source_config.get("asn1_pdu_name"),
            binary_column="content",
        )
    except KeyError as exc:
        raise FrameworkConfigError(f"asn1 source_config missing required key {exc}") from exc


_SOURCE_READERS = {
    "autoloader": read_autoloader_source,
    "zerobus": read_zerobus_source,
    "asn1": read_asn1_source,
}


#: The base-read subset of an ingestion ``source_config`` -- the keys that change *which bytes
#: are scanned* (format, schema location/evolution, file selection, reader passthrough, landing
#: lifecycle) or *which physical rows a table-based read returns* (the qualified
#: ``source_catalog``/``source_schema``/``source_table`` triple, plus the two zerobus streaming
#: reader options). This is the exact BASE-READ list from the source-plane read-once contract
#: (``docs/13``); everything NOT in it (``schema_config``, ``column_normalization``, DQ/quarantine
#: columns, encryption, etc.) is an *overlay*, applied per consumer downstream of the shared read
#: rather than folded into its identity. See ``ReadIdentity`` in ``engine/source_plane.py``.
_BASE_READ_KEYS = (
    "format",
    "schema_location",
    "schema_evolution_mode",
    "file_pattern",
    "reader_options",
    "landing_retention_policy",
    "source_zip_handling",
    "source_catalog",
    "source_schema",
    "source_table",
    "starting_version",
    "max_bytes_per_trigger",
)


def base_read_options(source_type: str, source_config: Dict[str, Any]) -> Dict[str, Any]:
    """Select the base-read subset of ``source_config`` that a source-plane ``ReadIdentity``'s
    ``options_fingerprint`` is computed over (``sha256`` of canonical JSON over exactly this
    dict, per the read-once contract) -- see :data:`_BASE_READ_KEYS`.

    Deliberately pure and dumb: no Spark, no I/O, no key mapping, no defaulting. In particular
    ``file_pattern`` is returned **as the spec spells it**, never translated to Auto Loader's
    ``pathGlobFilter`` option -- that un-prefixed mapping is
    :func:`_apply_common_autoloader_options`'s exclusive property (``cloudFiles.fileNamePattern``
    does not exist for any format; see that function's docstring), and duplicating it here would
    give this one mapping two owners.

    Only keys actually present in ``source_config`` are returned -- an omitted optional block
    (e.g. no ``landing_retention_policy`` at all) is left absent rather than defaulted, since
    resolving defaults/degradation (:func:`resolve_landing_retention_policy`) is itself a mapping
    step, not an identity-selection one. A caller that needs to compare two configs for the
    *semantic* side-effect collision the contract describes (">1 distinct
    landing_retention_policy / source_zip_handling on one path") is expected to resolve both
    sides first (e.g. via :func:`resolve_landing_retention_policy`) rather than rely on this
    function's raw, unresolved output for that comparison.

    Parameters
    ----------
    source_type:
        One of the registered ingestion source types (``autoloader`` / ``zerobus`` / ``asn1``).
    source_config:
        The flow's ingestion ``source_config``, exactly as it appears in the (already
        ``${param}``-substituted) spec -- before any reader-option mapping.

    Returns
    -------
    Dict[str, Any]
        The subset of :data:`_BASE_READ_KEYS` present in ``source_config``, values untouched.

    Raises
    ------
    FrameworkConfigError
        If ``source_type`` is not a registered ingestion source type.
    """
    if source_type not in _SOURCE_READERS:
        raise FrameworkConfigError(f"Unsupported ingestion source_type '{source_type}' for base_read_options")
    return {key: source_config[key] for key in _BASE_READ_KEYS if key in source_config}


def read_locator(source_config: Dict[str, Any], source_type: str) -> Tuple[str, str]:
    """Compute the ``(locator_kind, locator)`` half of a source-plane ``ReadIdentity`` for one
    ingestion ``source_config`` -- see the read-once contract's CANONICAL IDENTITY section
    (``docs/13``) and ``ReadIdentity`` in ``engine/source_plane.py``.

    ``zerobus`` reads an existing catalog table, so its locator is the ``source_catalog`` /
    ``source_schema`` / ``source_table`` triple joined into a fully-qualified name and
    ``casefold()``-ed -- casefolding is load-bearing (``flowx_testing/003`` writes a
    capitalized schema name in one spec; a case-sensitive miss here would silently fall through
    to a second, duplicate read of the same physical table). ``autoloader`` and ``asn1`` both
    read a landing directory via Auto Loader, so their locator is ``path`` with any trailing
    slash(es) stripped, so ``"/Volumes/x/y/z"`` and ``"/Volumes/x/y/z/"`` resolve to the same
    physical location. ``${param}``/``{{catalog}}`` substitution has already run on
    ``source_config`` by the time this function sees it (``onboarding/spec_loader.py``, exactly
    as for ``path``/``schema_location`` elsewhere in this module), so no substitution happens
    here.

    Deliberately pure: no Spark, no I/O. Takes the raw spec keys before any mapping, exactly
    like :func:`base_read_options` -- the un-prefixed ``pathGlobFilter`` mapping stays
    :func:`_apply_common_autoloader_options`'s exclusive property regardless of what a caller
    does with the locator this function returns.

    Parameters
    ----------
    source_config:
        The flow's ingestion ``source_config``.
    source_type:
        One of the registered ingestion source types (``autoloader`` / ``zerobus`` / ``asn1``).

    Returns
    -------
    Tuple[str, str]
        ``("zerobus", "cat.sch.tbl")`` (casefolded) for a zerobus source, or
        ``("path", "/Volumes/...")`` (trailing-slash-stripped) for an autoloader/asn1 source.

    Raises
    ------
    FrameworkConfigError
        If ``source_type`` is not a registered ingestion source type, or the required locator
        key(s) (``source_catalog``/``source_schema``/``source_table`` for zerobus, ``path``
        otherwise) are missing from ``source_config``.
    """
    if source_type not in _SOURCE_READERS:
        raise FrameworkConfigError(f"Unsupported ingestion source_type '{source_type}' for read_locator")

    if source_type == "zerobus":
        try:
            qualified_table = (
                f"{source_config['source_catalog']}.{source_config['source_schema']}.{source_config['source_table']}"
            )
        except KeyError as exc:
            raise FrameworkConfigError(f"zerobus source_config missing required key {exc} for read_locator") from exc
        return "zerobus", qualified_table.casefold()

    # autoloader / asn1 -- both land files at `path` and are read from there by Auto Loader.
    try:
        path = source_config["path"]
    except KeyError as exc:
        raise FrameworkConfigError(f"{source_type} source_config missing required key {exc} for read_locator") from exc
    return "path", path.rstrip("/")


def read_ingestion_source(spark: SparkSession, source_type: str, source_config: Dict[str, Any]) -> DataFrame:
    """Dispatch to the reader registered for ``source_type``.

    Raises
    ------
    FrameworkConfigError
        If ``source_type`` is not one of ``autoloader`` / ``zerobus`` / ``asn1``.
    """
    if source_type not in _SOURCE_READERS:
        raise FrameworkConfigError(f"Unsupported ingestion source_type '{source_type}'")
    return _SOURCE_READERS[source_type](spark, source_config)
