"""Technical audit metadata capture: Auto Loader ``_rescued_data`` + hidden storage metadata,
plus the framework-wide ``__framework_ingestion_timestamp_utc`` column.
"""

import logging
from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("common.ingestion.technical_metadata")

_METADATA_FIELD_EXTRACTIONS = {
    "__framework_source_file_name": "_metadata.file_name",
    "__framework_source_file_size": "_metadata.file_size",
    "__framework_source_file_modification_time": "_metadata.file_modification_time",
}

FRAMEWORK_INGESTION_TIMESTAMP_COLUMN = "__framework_ingestion_timestamp_utc"


def attach_technical_metadata(df: DataFrame, source_config: Dict[str, Any]) -> DataFrame:
    """Attach Auto Loader technical audit columns: rescued data + hidden storage metadata.

    Each ``_metadata`` struct-field extraction is wrapped independently so that connectors
    which don't populate optional fields (MIME type / ETag / custom headers) degrade
    gracefully to ``NULL`` rather than failing the whole read. ``_rescued_data`` is added
    automatically by Auto Loader for structured formats and is left untouched here.
    """
    if not source_config.get("capture_technical_metadata", True):
        return df

    result_df = df
    for output_col, source_expr in _METADATA_FIELD_EXTRACTIONS.items():
        try:
            result_df = result_df.withColumn(output_col, F.expr(source_expr))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Metadata field '%s' unavailable for this source, defaulting to NULL: %s", source_expr, exc)
            result_df = result_df.withColumn(output_col, F.lit(None).cast("string"))

    try:
        # Real bug fixed here: this used to read `_metadata.file_metadata`, a field that has
        # never existed on the `_metadata` struct (confirmed against Databricks' own
        # documentation: https://docs.databricks.com/aws/en/ingestion/file-metadata-column --
        # `_metadata` has exactly six fields: file_path/file_name/file_size/
        # file_modification_time/file_block_start/file_block_length, none named
        # `file_metadata`). Every extended/custom-header property instead lives on a
        # *separate* hidden pseudo-column, `_object_metadata` -- not nested under `_metadata`
        # at all -- with fields `mime_type`, `etag`, `user_metadata`, `system_metadata`,
        # `tags` (the last three are VARIANT; see
        # https://docs.databricks.com/aws/en/ingestion/object-metadata-column). Because the
        # wrong field name was silently swallowed by this same try/except, this column was
        # unconditionally NULL for every row, on every source, regardless of storage backend.
        #
        # Requires Databricks Runtime 18.2+; `_object_metadata` is always NULL for
        # Databricks-managed storage (e.g. a table's own managed location), and its `tags`
        # sub-field is only ever populated on S3 and non-HNS Azure Blob Storage -- both
        # documented, legitimate NULL cases this try/except still exists to tolerate.
        result_df = result_df.withColumn(
            "__framework_source_file_metadata_headers",
            F.expr(
                """
                map(
                    'mime_type', _object_metadata.mime_type,
                    'etag', _object_metadata.etag,
                    'user_metadata', CAST(_object_metadata.user_metadata AS STRING),
                    'system_metadata', CAST(_object_metadata.system_metadata AS STRING),
                    'tags', CAST(_object_metadata.tags AS STRING)
                )
                """
            ),
        )
    except Exception as exc:  # noqa: BLE001 - not all storage backends/runtimes populate extended headers
        logger.warning("Extended file metadata headers unavailable for this source: %s", exc)
        result_df = result_df.withColumn("__framework_source_file_metadata_headers", F.lit(None).cast("map<string,string>"))

    return result_df


def attach_framework_ingestion_timestamp(df: DataFrame, capture_technical_metadata: bool = True) -> DataFrame:
    """Attach ``__framework_ingestion_timestamp_utc`` -- a framework-wide, always-consistent
    processing timestamp, added to **every** ingestion and transformation target (gated by
    the same ``capture_technical_metadata`` toggle both flow types already expose).

    This column is also the default sequencer for SCD1/SCD2/SCD3 when a flow's
    ``target_config.sequence_by_column`` is omitted -- see ``cdc/scd.py`` -- since
    ``dlt.apply_changes`` always requires *some* ``sequence_by`` value.

    Idempotent-add: a transformation's ``source_inputs`` may already carry an upstream copy
    of this exact column through a join (e.g. two ingestion-sourced inputs joined together),
    so this is a no-op when the column is already present, rather than raising an ambiguous-
    column error or silently shadowing the upstream value with a new one.

    Parameters
    ----------
    df:
        Input DataFrame.
    capture_technical_metadata:
        When ``False``, this is a no-op (matches the same opt-out semantics as
        :func:`attach_technical_metadata`).
    """
    if not capture_technical_metadata:
        return df
    if FRAMEWORK_INGESTION_TIMESTAMP_COLUMN in df.columns:
        return df
    return df.withColumn(FRAMEWORK_INGESTION_TIMESTAMP_COLUMN, F.current_timestamp())
