"""Transformation-engine multi-input registration: one watermarked ``@dlt.view`` per input.

A transformation flow's ``transformation_sql`` can join/select across several named inputs
(``source_inputs[]``), each pointing at its own already-published table -- this module is what
turns that list into queryable named views before the SQL ever runs. Called from
``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``'s transformation-engine branch,
ahead of ``engine/flow_registration.py::register_staged_view`` for the flow's own output.

:func:`register_transformation_inputs` registers each input as a plain ``@dlt.view`` --
streaming or batch per ``is_streaming``, with an optional ``watermark`` applied and
``decrypted_columns`` (the *only* place a source column is ever decrypted; a target's own
``target_config.encrypted_columns`` only ever encrypts SQL *output* columns). A streaming
input's event-time column is cast to ``timestamp`` before ``withWatermark`` is applied, since a
source ingested from JSON/CSV routinely delivers that column as ``STRING`` and
``withWatermark`` requires an actual ``TimestampType``.

:func:`mark_streaming_references` is the necessary companion: because ``transformation_sql``
runs as one plain ``spark.sql(...)`` call, Spark SQL resolves a bare ``FROM``/``JOIN`` reference
to a streaming view as a *batch* reference regardless of how the view was actually registered
above -- confirmed via a live deployment failure (``AnalysisException: ... streaming view and
must be referenced using readStream``). This function rewrites the SQL text to prefix every
streaming input's ``FROM``/``JOIN`` reference with SQL's ``STREAM`` keyword automatically, so a
spec author writes ordinary SQL against ``input_name`` and never needs to know this Spark SQL
detail exists.
"""

import logging
import re
from typing import Any, Dict, List

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.crypto.column_crypto import apply_aes_column_decryption
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("common.transformation.inputs")


def register_transformation_inputs(spark: SparkSession, source_inputs: List[Dict[str, Any]]) -> None:
    """Register one ``@dlt.view`` per configured transformation input, applying watermarks.

    Each entry in ``source_inputs`` is shaped like::

        {
          "input_name": "raw_transactions",
          "table": "poc.bronze.raw_transactions",
          "is_streaming": true,
          "watermark": {"event_time_column": "txn_ts", "delay_threshold": "10 minutes"}
        }

    The resulting views are referenced by ``input_name`` directly from the transformation
    SQL's ``FROM``/``JOIN`` clauses.

    ``decrypted_columns`` (v2 schema) is the *only* valid place to decrypt a source column
    -- decryption happens here, per input, before ``transformation_sql`` ever runs; a
    target's own ``target_config.encrypted_columns`` only ever encrypts SQL *output*
    columns, never decrypts an input.

    A configured ``watermark.event_time_column`` is cast to ``timestamp`` before
    ``withWatermark`` is applied -- ``withWatermark`` requires an actual ``TimestampType``
    column, but a source ingested from JSON/CSV (Auto Loader has no native timestamp type
    to infer from free-text) routinely delivers its event-time field as ``STRING``
    (e.g. ISO-8601 text), which previously raised
    ``EVENT_TIME_IS_NOT_ON_TIMESTAMP_TYPE`` for any watermarked input over such a source.
    The cast is a no-op when the column is already a ``timestamp``.

    Raises
    ------
    FrameworkConfigError
        If an input config is missing ``input_name``/``table``, or a configured
        ``watermark`` is missing ``event_time_column``/``delay_threshold``.
    """
    for input_config in source_inputs:
        try:
            input_name = input_config["input_name"]
            qualified_table = input_config["table"]
        except KeyError as exc:
            raise FrameworkConfigError(f"Transformation input config missing required key {exc}: {input_config}") from exc

        is_streaming = input_config.get("is_streaming", False)
        watermark = input_config.get("watermark")
        if watermark:
            try:
                event_time_column = watermark["event_time_column"]
                delay_threshold = watermark["delay_threshold"]
            except KeyError as exc:
                raise FrameworkConfigError(
                    f"Transformation input '{input_name}': watermark config missing required key {exc}: {watermark}"
                ) from exc
        else:
            event_time_column = delay_threshold = None

        decrypted_columns = input_config.get("decrypted_columns", [])

        def _make_input_view(
            qualified_table=qualified_table,
            is_streaming=is_streaming,
            event_time_column=event_time_column,
            delay_threshold=delay_threshold,
            decrypted_columns=decrypted_columns,
        ):
            if is_streaming:
                view_df = spark.readStream.table(qualified_table)
                if event_time_column:
                    view_df = view_df.withColumn(event_time_column, F.col(event_time_column).cast("timestamp"))
                    view_df = view_df.withWatermark(event_time_column, delay_threshold)
            else:
                view_df = spark.read.table(qualified_table)
            if decrypted_columns:
                view_df = apply_aes_column_decryption(view_df, decrypted_columns)
            return view_df

        dlt.view(name=input_name, comment=f"Transformation input view for {qualified_table}")(_make_input_view)
        logger.info("Registered transformation input view '%s' -> %s", input_name, qualified_table)


def mark_streaming_references(sql_text: str, source_inputs: List[Dict[str, Any]]) -> str:
    """Prefix every ``FROM``/``JOIN`` reference to a streaming input with SQL's ``STREAM`` keyword.

    ``register_transformation_inputs`` builds a streaming input's view via
    ``spark.readStream.table(...)``, which Lakeflow tracks internally as a genuinely
    streaming dataset. But ``transformation_sql`` runs as a single ``spark.sql(...)``
    call, and plain Spark SQL resolves a bare ``FROM some_view`` as a *batch* reference
    regardless of what kind of dataset ``some_view`` actually is -- confirmed via a live
    deployment failure the first time any transformation flow's SQL referenced a
    streaming input: ``AnalysisException: View '...' is a streaming view and must be
    referenced using readStream``. Spark SQL's ``FROM STREAM <name>`` /
    ``JOIN STREAM <name>`` syntax (see
    https://docs.databricks.com/en/query/streaming.html) is the documented way to mark a
    specific table reference as streaming within ad-hoc SQL text; this rewrites
    ``transformation_sql`` to use it automatically for every input whose ``is_streaming``
    is ``True``, so a spec author writes ordinary SQL against `input_name` and never
    needs to know this Spark SQL detail exists at all.

    Only a bare ``FROM``/``JOIN`` keyword immediately followed by the input's exact name
    (word boundaries on both sides, case-insensitive keyword) is rewritten -- an
    ``input_name`` that happens to be a substring of another identifier, or that appears
    elsewhere in the query (a column reference, a string literal), is never touched.

    Parameters
    ----------
    sql_text:
        Already parameter-substituted transformation SQL (call *after*
        :func:`common.transformation.parameters.substitute_dynamic_parameters`).
    source_inputs:
        Same list passed to :func:`register_transformation_inputs`.
    """
    for input_config in source_inputs:
        if not input_config.get("is_streaming"):
            continue
        input_name = input_config["input_name"]
        pattern = re.compile(rf"\b(FROM|JOIN)\s+{re.escape(input_name)}\b", re.IGNORECASE)
        sql_text = pattern.sub(lambda m: f"{m.group(1)} STREAM {input_name}", sql_text)
    return sql_text
