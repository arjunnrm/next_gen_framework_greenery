"""Multi-ZIP batch ingestion: validate -> extract -> load -> join/transform -> re-archive.

Reuses ``archive/zip_utils.py``'s existing extraction/compression primitives -- this
module only adds the batch-level orchestration (validating a set of input ZIPs together,
loading each extracted file into a staging view, running a configured join, and
re-packaging the result) that a single-archive utility function has no business knowing
about.

Deliberately outside the ``source_zip_handling.delete_source_after_extract`` policy layer
(``archive/zip_utils.py::ZipDeletePolicy`` / ``resolve_zip_delete_policy``, applied by
``ingestion/readers.py``): the input archives here are named explicitly by a
``zip_ingestion_configs/*.json`` config rather than glob-matched out of a landing zone, and
this job's own ``cleanup_extracted_files`` governs the only files it considers disposable --
the *extracted members*, which are working state. The input ZIPs themselves are the caller's
deliverables and are therefore always left in place (``delete_source_after_extract=False``
below), never subjected to an ingestion flow's age-based retention policy.
"""

import hashlib
import logging
import os
import zipfile
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from NextGen_Metadata_Framework.lakeflow_framework.archive.zip_utils import compress_and_encrypt_sink, extract_encrypted_zip
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import ArchiveError

logger = logging.getLogger("NextGen_Metadata_Framework.lakeflow_framework.archive.zip_ingestion_pipeline")


def _sha256_of_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def validate_zip_batch(zip_paths: List[str]) -> None:
    """Validate a batch of input ZIP archives before extracting any of them.

    Checks, for every path, in order: existence, non-zero size, and structural validity
    (a real ZIP, not corrupt/truncated) -- then checks the *batch* for duplicate archives
    (identical content, by SHA-256, landed under two different names). Every problem found
    is collected and reported together, the same exhaustive-validation convention used by
    ``onboarding/spec_validator.py``, so a batch with three bad files gets one report
    naming all three rather than a fix-one-rerun loop.

    Raises
    ------
    ArchiveError
        Naming every missing, empty, malformed, or duplicate archive found.
    """
    problems: List[str] = []
    content_hashes: Dict[str, str] = {}

    for zip_path in zip_paths:
        if not os.path.exists(zip_path):
            problems.append(f"missing: '{zip_path}' does not exist")
            continue
        if os.path.getsize(zip_path) == 0:
            problems.append(f"empty: '{zip_path}' is a zero-byte file")
            continue
        if not zipfile.is_zipfile(zip_path):
            problems.append(f"malformed: '{zip_path}' is not a valid ZIP archive")
            continue
        try:
            with zipfile.ZipFile(zip_path) as archive:
                bad_member = archive.testzip()
                if bad_member is not None:
                    problems.append(f"malformed: '{zip_path}' failed CRC check on member '{bad_member}'")
                    continue
                if not archive.namelist():
                    problems.append(f"empty: '{zip_path}' contains no members")
                    continue
        except zipfile.BadZipFile as exc:
            problems.append(f"malformed: '{zip_path}' could not be opened -- {exc}")
            continue

        digest = _sha256_of_file(zip_path)
        if digest in content_hashes:
            problems.append(f"duplicate: '{zip_path}' is byte-identical to '{content_hashes[digest]}'")
        else:
            content_hashes[digest] = zip_path

    if problems:
        raise ArchiveError(f"ZIP batch validation failed with {len(problems)} issue(s): {'; '.join(problems)}")


def ingest_zip_batch(
    spark: SparkSession,
    zip_paths: List[str],
    extract_dir: str,
    staging_view_configs: Dict[str, str],
    join_sql: str,
    output_table: str,
    output_csv_dir: str,
    output_zip_path: str,
    secret_catalog: Optional[str] = None,
    secret_schema: Optional[str] = None,
    secret_key: Optional[str] = None,
    cleanup_extracted_files: bool = True,
) -> Dict[str, Any]:
    """Validate, extract, load, join, write, and re-archive a batch of input ZIP files.

    Parameters
    ----------
    zip_paths:
        The input ZIP archives (validated as a batch first -- see :func:`validate_zip_batch`).
    extract_dir:
        Directory (typically a Unity Catalog Volume path) extracted members are written into.
    staging_view_configs:
        Maps an *extracted filename* (as it appears inside its ZIP, e.g. ``"orders_branch1.csv"``)
        to the temp view name it should be registered under for ``join_sql`` to reference
        (e.g. ``"orders_branch1"``). Every extracted file not present here is loaded and
        ignored (not registered), so an unexpectedly-present extra file in an archive
        doesn't silently participate in the join.
    join_sql:
        Spark SQL referencing the registered staging view names, producing the final result.
    output_table:
        Fully-qualified Delta table the join result is written to.
    output_csv_dir:
        Directory the join result is also written to as CSV, for re-archiving.
    output_zip_path:
        Destination path for the final (optionally encrypted) output ZIP.
    secret_catalog, secret_schema, secret_key:
        Optional Unity Catalog secret coordinates for an AES-256 password on the *output* ZIP.
    cleanup_extracted_files:
        When ``True`` (default), removes every extracted member file after the output ZIP
        has been produced -- the extraction directory is working state, not a deliverable.

    Returns
    -------
    dict
        ``{"extracted_file_count": int, "loaded_view_count": int, "output_row_count": int,
        "output_zip_path": str}``.

    Raises
    ------
    ArchiveError
        On batch validation failure, extraction failure, or archive-compression failure.
    FrameworkConfigError
        Propagated from ``spark.sql`` if ``join_sql`` is malformed or references an
        unregistered view.
    """
    validate_zip_batch(zip_paths)

    extracted_paths: List[str] = []
    try:
        for zip_path in zip_paths:
            extracted_paths.extend(extract_encrypted_zip(spark, zip_path, extract_dir, delete_source_after_extract=False))

        loaded_view_count = 0
        for extracted_path in extracted_paths:
            filename = os.path.basename(extracted_path)
            view_name = staging_view_configs.get(filename)
            if not view_name:
                logger.info("Extracted file '%s' has no configured staging view -- skipping load.", filename)
                continue
            # No `file:` scheme prefix: on Unity-Catalog-shared/serverless compute, a
            # `file:`-prefixed path is routed through a restricted local-filesystem reader
            # that only allow-lists `/Workspace` and raises
            # LocalFilesystemAccessDeniedException for a `/Volumes/...` path -- the plain
            # absolute path (no scheme) is what correctly routes through the Unity Catalog
            # Volume filesystem instead.
            spark.read.option("header", "true").option("inferSchema", "true").csv(extracted_path).createOrReplaceTempView(
                view_name
            )
            loaded_view_count += 1

        result_df = spark.sql(join_sql)
        output_row_count = result_df.count()

        # Unlike ingestion/transformation flows (whose schema is provisioned by the
        # DLT pipeline resource's own catalog/schema config), this is a plain notebook
        # write to an arbitrary output_table -- nothing else creates its schema first.
        output_catalog_schema = ".".join(output_table.split(".")[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_catalog_schema}")
        result_df.write.format("delta").mode("overwrite").saveAsTable(output_table)
        result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_csv_dir)

        compress_and_encrypt_sink(
            spark,
            source_dir=output_csv_dir,
            output_zip_path=output_zip_path,
            secret_catalog=secret_catalog,
            secret_schema=secret_schema,
            secret_key=secret_key,
        )

        return {
            "extracted_file_count": len(extracted_paths),
            "loaded_view_count": loaded_view_count,
            "output_row_count": output_row_count,
            "output_zip_path": output_zip_path,
        }
    finally:
        if cleanup_extracted_files:
            for extracted_path in extracted_paths:
                try:
                    os.remove(extracted_path)
                except OSError as exc:
                    logger.warning("Cleanup: failed to remove extracted temp file '%s': %s", extracted_path, exc)
