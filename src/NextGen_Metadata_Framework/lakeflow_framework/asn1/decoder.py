"""ASN.1 BER/DER binary decoding for telecom CDR-style sources.

**Distributed by construction, end to end.** File discovery/reading happens via Auto
Loader's ``cloudFiles`` source (``ingestion/readers.py::read_asn1_source``), which lists and
reads files across executors natively -- never collects file content to the driver. The
decode step below runs via :meth:`~pyspark.sql.DataFrame.mapInPandas`, which Spark also
executes entirely on executors, one partition at a time, with no driver-side collection at
any point.

**Real perf bug fixed here**: the original implementation decoded via a plain row UDF
(``F.udf``), which called ``asn1tools.compile_files(...)`` *inside* the per-row decode
function -- Spark has no way to know that call is expensive and reusable, so it recompiled
the full ASN.1 module from scratch for every single row. ``mapInPandas`` instead hands the
decode function one *partition* at a time (as an iterator of pandas DataFrame micro-batches)
-- compiling the schema once, before iterating the batches, and reusing that one compiled
object for every row across the whole partition, cuts compilation from O(rows) to O(partitions).

**Schema source: a real ``.asn`` module file, not a hand-authored JSON field list.** The
output field list used to be duplicated by hand into an external
``asn1_schema_json_path``'s ``fields: [{name, spark_type}]`` array -- a second, drift-prone
description of exactly what the ``.asn`` module already says authoritatively. This module
now derives the Spark output schema directly from the real ASN.1 module file via
``asn1tools.parse_files`` introspection (:func:`derive_asn1_field_defs`) -- one source of
truth, the ``.asn`` file itself, exactly as a data engineer would already have it from
whoever owns the wire format.
"""

import logging
from typing import Any, Callable, Dict, FrozenSet, Iterator, List

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import Asn1DecodeError

logger = logging.getLogger("common.asn1.decoder")

try:
    import asn1tools

    _ASN1TOOLS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _ASN1TOOLS_AVAILABLE = False

# ASN.1 primitive/string type -> Spark type. INTEGER maps to LongType (int64) -- ample for
# any real telecom/CDR field; an ASN.1 INTEGER constrained beyond 64 bits is not supported.
# Every ASN.1 "restricted character string" type (UTF8String, IA5String, NumericString,
# PrintableString, VisibleString, GeneralString, BMPString, UniversalString, GraphicString,
# TeletexString/T61String) maps to StringType -- Spark has no narrower equivalent, and this
# framework doesn't attempt to re-validate the SIZE/character-set constraints those types
# encode (asn1tools' own codec already enforces them during decode). GeneralizedTime/UTCTime
# -- the standard ASN.1 timestamp types (e.g. a CDR's recordingTimeStamp) -- map to
# TimestampType: confirmed live, asn1tools decodes both to a plain Python datetime.datetime.
# OBJECT IDENTIFIER (protocol/algorithm identifiers) decodes to its dotted-notation string
# (e.g. "1.2.840.113549") verbatim, so it maps to StringType with no transformation needed.
# NULL always decodes to None regardless of declared type, so any nullable type works;
# StringType is used for consistency with the other always-nullable mappings above.
_ASN1_PRIMITIVE_TYPE_MAP: Dict[str, DataType] = {
    "BOOLEAN": BooleanType(),
    "INTEGER": LongType(),
    "REAL": DoubleType(),
    "OCTET STRING": BinaryType(),
    "UTF8String": StringType(),
    "IA5String": StringType(),
    "NumericString": StringType(),
    "PrintableString": StringType(),
    "VisibleString": StringType(),
    "GeneralString": StringType(),
    "BMPString": StringType(),
    "UniversalString": StringType(),
    "GraphicString": StringType(),
    "TeletexString": StringType(),
    "T61String": StringType(),
    "GeneralizedTime": TimestampType(),
    "UTCTime": TimestampType(),
    "OBJECT IDENTIFIER": StringType(),
    "NULL": StringType(),
}

# BIT STRING decodes (confirmed live) to a bare (bytes_or_bytearray, bit_length) tuple, not a
# named-field structure Arrow can map directly -- _normalize_decoded_value below reshapes it
# into {"bytes": ..., "bit_length": ...} at decode time to match this declared output type.
BIT_STRING_SPARK_TYPE = StructType(
    [StructField("bytes", BinaryType(), True), StructField("bit_length", LongType(), True)]
)


def _asn1_node_to_spark_type(node: Dict[str, Any], module_types: Dict[str, Any], seen: FrozenSet[str]) -> DataType:
    """Resolve one ``asn1tools.parse_files`` node (a top-level type def, a ``SEQUENCE``
    member, or a ``SEQUENCE OF`` element) to a Spark ``DataType``, recursively.

    Every node ``asn1tools.parse_files`` returns -- whether it's a module-level named type,
    a member of a ``SEQUENCE``, or the ``element`` of a ``SEQUENCE OF`` -- shares the same
    ``{"type": ..., ...}`` shape, so one recursive function handles all three contexts:

    * ``SEQUENCE``/``SET`` -> ``StructType`` of its ``members``, each resolved recursively (an
      inline nested record, e.g. ``UserProfile.address: Address``, decodes to a Spark
      ``struct`` column, matched by ``ingestion/json_flattening.py``'s existing
      ``explode_columns`` mechanism for any downstream flattening a spec wants). ``SET`` is
      handled identically to ``SEQUENCE`` -- both decode to a plain Python ``dict`` keyed by
      member name; ``SET``'s "unordered" semantics only affect wire encoding, not the shape
      asn1tools hands back.
    * ``SEQUENCE OF``/``SET OF`` -> ``ArrayType`` of its ``element``, resolved recursively
      (confirmed live: ``asn1tools`` decodes both to a plain Python ``list``).
    * ``ENUMERATED`` -> ``StringType`` (confirmed live: ``asn1tools`` decodes an enumerated
      value to its symbolic member name, e.g. ``"administrator"``, not its integer tag).
    * ``BIT STRING`` -> :data:`BIT_STRING_SPARK_TYPE` (a 2-field struct) -- confirmed live,
      asn1tools decodes this to a bare ``(bytes_or_bytearray, bit_length)`` tuple; see
      :func:`_normalize_decoded_value` for the corresponding value reshape at decode time.
    * A primitive/string/timestamp type name (``INTEGER``, ``BOOLEAN``, ``UTF8String``,
      ``GeneralizedTime``, ``OBJECT IDENTIFIER``, ...) -> :data:`_ASN1_PRIMITIVE_TYPE_MAP`.
    * Anything else is a reference to another named type defined in the same module (e.g.
      a member's ``"type"`` is literally ``"Address"``) -- resolved by recursing into
      ``module_types[name]``. ``seen`` guards against a self-referential/recursive type
      definition, which this framework does not support (an ASN.1 type recursively
      containing itself has no finite Spark ``StructType``).

    Raises
    ------
    Asn1DecodeError
        If a member's ``"type"`` doesn't resolve to a known primitive/string type, a
        ``SEQUENCE``/``SET``/``SEQUENCE OF``/``SET OF``/``ENUMERATED``/``BIT STRING``
        construct, or a type actually defined in this module (including ``CHOICE``, which
        this framework does not yet support).
    """
    asn1_type = node.get("type")

    if asn1_type in ("SEQUENCE", "SET"):
        fields = [
            StructField(member["name"], _asn1_node_to_spark_type(member, module_types, seen), True)
            for member in node.get("members", [])
        ]
        return StructType(fields)

    if asn1_type in ("SEQUENCE OF", "SET OF"):
        element = node.get("element")
        if element is None:
            raise Asn1DecodeError(f"ASN.1 {asn1_type!r} node is missing its 'element' definition: {node!r}")
        return ArrayType(_asn1_node_to_spark_type(element, module_types, seen), True)

    if asn1_type == "ENUMERATED":
        return StringType()

    if asn1_type == "BIT STRING":
        return BIT_STRING_SPARK_TYPE

    if asn1_type in _ASN1_PRIMITIVE_TYPE_MAP:
        return _ASN1_PRIMITIVE_TYPE_MAP[asn1_type]

    if asn1_type == "CHOICE":
        raise Asn1DecodeError(
            "ASN.1 CHOICE types are not yet supported by schema auto-derivation "
            f"(node: {node.get('name', asn1_type)!r})."
        )

    if not asn1_type or asn1_type not in module_types:
        raise Asn1DecodeError(
            f"Unsupported or unresolvable ASN.1 type {asn1_type!r} (node: {node.get('name', '?')!r}); "
            f"known types in this module: {sorted(module_types)}"
        )
    if asn1_type in seen:
        raise Asn1DecodeError(
            f"Unsupported recursive/self-referential ASN.1 type reference: "
            f"{' -> '.join(sorted(seen))} -> {asn1_type}"
        )
    return _asn1_node_to_spark_type(module_types[asn1_type], module_types, seen | {asn1_type})


def derive_asn1_field_defs(schema_path: str, pdu_name: str) -> List[Dict[str, Any]]:
    """Derive the Spark output field list for ``pdu_name`` directly from a real ASN.1
    module file, via ``asn1tools.parse_files`` introspection -- no hand-authored field list.

    ``pdu_name`` must name a top-level ``SEQUENCE`` type in the module (the ASN.1 "record"
    this source decodes one instance of per binary payload, e.g. ``CallDetailRecord`` or
    ``UserProfile``). Each of that ``SEQUENCE``'s own direct members becomes one output
    column -- nested ``SEQUENCE``/``SEQUENCE OF`` members become ``struct``/``array``
    columns rather than being flattened further here (a spec can flatten them downstream via
    ``explode_columns``, same as a JSON source).

    Parameters
    ----------
    schema_path:
        Path to the real ``.asn``/ASN.1 module definition file (a single self-contained
        module -- this framework does not support ``IMPORTS`` spanning multiple files).
    pdu_name:
        Name of the top-level ``SEQUENCE`` type in that module to decode each record as.

    Returns
    -------
    list of dict
        ``[{"name": <member name>, "spark_type": <resolved pyspark.sql.types.DataType>}, ...]``,
        in the member order the ``.asn`` file itself declares them.

    Raises
    ------
    Asn1DecodeError
        If asn1tools is unavailable, the file can't be parsed, the module doesn't define
        exactly one ASN.1 module, ``pdu_name`` isn't a defined type, ``pdu_name`` isn't a
        ``SEQUENCE``, or any member's type can't be resolved (see
        :func:`_asn1_node_to_spark_type`).
    """
    if not _ASN1TOOLS_AVAILABLE:
        raise Asn1DecodeError("asn1tools is required for ASN.1 schema derivation but is not installed.")

    try:
        parsed = asn1tools.parse_files([schema_path])
    except OSError as exc:
        raise Asn1DecodeError(f"Failed to read ASN.1 module file '{schema_path}': {exc}") from exc
    except Exception as exc:  # noqa: BLE001 - asn1tools raises its own parser-specific error types
        raise Asn1DecodeError(f"Failed to parse ASN.1 module file '{schema_path}': {exc}") from exc

    if len(parsed) != 1:
        raise Asn1DecodeError(
            f"ASN.1 module file '{schema_path}' must define exactly one module; found {sorted(parsed)}."
        )
    module = next(iter(parsed.values()))
    module_types = module.get("types", {})

    if pdu_name not in module_types:
        raise Asn1DecodeError(
            f"PDU '{pdu_name}' is not defined in ASN.1 module file '{schema_path}' "
            f"(defined types: {sorted(module_types)})."
        )
    pdu_def = module_types[pdu_name]
    if pdu_def.get("type") != "SEQUENCE":
        raise Asn1DecodeError(
            f"PDU '{pdu_name}' must be a top-level ASN.1 SEQUENCE (a record type); got '{pdu_def.get('type')}'."
        )

    return [
        {"name": member["name"], "spark_type": _asn1_node_to_spark_type(member, module_types, frozenset({pdu_name}))}
        for member in pdu_def.get("members", [])
    ]


def _normalize_decoded_value(value: Any) -> Any:
    """Reshape one decoded ASN.1 value into a form Arrow/pandas can map onto the Spark output
    type :func:`_asn1_node_to_spark_type` declared for it.

    Every decoded value except ``BIT STRING`` already matches its declared Spark type
    verbatim (confirmed live: ``int``/``str``/``bool``/``bytes``/``datetime.datetime``/
    ``None`` pass straight through; ``dict`` for ``SEQUENCE``/``SET``, ``list`` for
    ``SEQUENCE OF``/``SET OF``). ``BIT STRING`` is the one exception, decoding to a bare
    ``(bytes_or_bytearray, bit_length)`` tuple that Arrow cannot map onto a *named*-field
    ``StructType`` on its own -- reshaped here into
    ``{"bytes": <bytes>, "bit_length": <int>}`` to match :data:`BIT_STRING_SPARK_TYPE`.
    Recurses into ``dict``/``list`` values so a ``BIT STRING`` nested inside a ``SEQUENCE``/
    ``SET``/``SEQUENCE OF``/``SET OF`` at any depth is also reshaped correctly.
    """
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], (bytes, bytearray)) and isinstance(value[1], int):
        return {"bytes": bytes(value[0]), "bit_length": value[1]}
    if isinstance(value, dict):
        return {key: _normalize_decoded_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_decoded_value(item) for item in value]
    return value


def make_partition_decoder(
    module_files: List[str],
    codec: str,
    pdu_name: str,
    field_defs: List[Dict[str, str]],
    binary_column: str,
    passthrough_columns: List[str],
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """Build the ``mapInPandas`` partition function.

    Spark invokes the returned generator function exactly once per partition, handing it an
    iterator over that partition's pandas micro-batches -- ``asn1tools.compile_files(...)``
    is called exactly once, *before* the ``for batch in batches`` loop, and the resulting
    compiled schema is reused for every row in every batch of the partition. This is the
    fix for the original per-row-UDF implementation, which recompiled the ASN.1 module on
    every single row.

    Returned as a plain, independently callable/testable function -- a unit test can invoke
    it directly with a small list of pandas DataFrames (no live Spark cluster needed) and
    monkeypatch ``asn1tools.compile_files`` with a call-counting stub to prove the
    once-per-partition behavior directly.

    Parameters
    ----------
    module_files, codec, pdu_name:
        The compiled-schema identity -- ``module_files`` is ``[schema_path]`` (a single-file
        list; see :func:`decode_asn1_binary_stream`), ``codec`` is ``"ber"``/``"der"``.
    field_defs:
        The PDU's own field list, as returned by :func:`derive_asn1_field_defs` (only
        ``field_def["name"]`` is read here -- ``field_def["spark_type"]`` is used by
        :func:`decode_asn1_binary_stream` to build the ``mapInPandas`` output schema, not by
        this function).
    binary_column:
        Name of the raw-bytes column to decode and drop from the output.
    passthrough_columns:
        Every other input column name, carried through to the output unchanged (e.g. Auto
        Loader's ``_metadata``/``path``/``modificationTime`` technical columns).
    """
    field_names = [field_def["name"] for field_def in field_defs]
    output_columns = passthrough_columns + field_names + ["_asn1_decode_error"]

    def _decode_partition(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        compiled = asn1tools.compile_files(module_files, codec)
        for batch in batches:
            records: List[Dict[str, Any]] = []
            for row in batch.itertuples(index=False):
                row_dict = dict(zip(batch.columns, row))
                raw_bytes = row_dict.get(binary_column)
                result: Dict[str, Any] = {col: row_dict.get(col) for col in passthrough_columns}
                for name in field_names:
                    result[name] = None
                if raw_bytes is None:
                    result["_asn1_decode_error"] = "null_payload"
                else:
                    try:
                        decoded = compiled.decode(pdu_name, raw_bytes)
                        for name in field_names:
                            result[name] = _normalize_decoded_value(decoded.get(name))
                        result["_asn1_decode_error"] = None
                    except Exception as decode_exc:  # noqa: BLE001 - isolate bad records, don't fail the batch
                        result["_asn1_decode_error"] = f"{type(decode_exc).__name__}: {decode_exc}"
                records.append(result)
            yield pd.DataFrame.from_records(records, columns=output_columns)

    return _decode_partition


def _materialize_hidden_metadata_column(df: DataFrame) -> DataFrame:
    """Force Auto Loader's hidden ``_metadata`` pseudo-column to become a real, physical column.

    ``mapInPandas`` reconstructs the DataFrame from scratch across the Python/Arrow boundary
    -- any hidden metadata column that was never made real is silently dropped, which would
    make ``ingestion/technical_metadata.py::attach_technical_metadata`` (called downstream
    on this function's output, extracting ``_metadata.file_name`` etc.) degrade every
    ASN.1-sourced row's ``__framework_source_file_name``/``__framework_source_file_size``/etc. to NULL. Best-effort:
    a source that genuinely has no ``_metadata`` column (e.g. a non-Auto-Loader DataFrame in
    a test) is left unchanged rather than raising.

    This project runs on Spark Connect (Databricks Connect / serverless), where ``DataFrame``
    construction is lazy by design: ``withColumn("_metadata", F.col("_metadata"))`` only
    builds an unresolved-column plan node client-side and never round-trips to the server, so
    it **cannot** raise for a genuinely-absent ``_metadata`` column -- confirmed live, this
    made the surrounding ``try/except`` dead code, and the real failure (an ``AnalysisException``
    Spark only raises once the plan is actually analyzed) instead erupted, uncaught, from the
    unrelated ``df.schema.fields`` access several lines later in
    :func:`decode_asn1_binary_stream`. Accessing ``.schema`` *inside* this function's own
    ``try`` forces that same analysis eagerly, right here, where the fallback can actually
    catch it.
    """
    if "_metadata" in df.columns:
        return df
    try:
        from pyspark.sql import functions as F

        candidate = df.withColumn("_metadata", F.col("_metadata"))
        _ = candidate.schema  # noqa: B018 - force eager analysis; see docstring above
        return candidate
    except Exception:  # noqa: BLE001
        return df


def decode_asn1_binary_stream(
    df: DataFrame, schema_path: str, codec: str, pdu_name: str, binary_column: str = "content"
) -> DataFrame:
    """Decode a column of raw ASN.1 BER/DER-encoded binary payloads into structured columns.

    Runs entirely via :meth:`~pyspark.sql.DataFrame.mapInPandas` (see
    :func:`make_partition_decoder`) -- fully distributed across executors, with the ASN.1
    schema compiled once per partition rather than once per row. The output schema itself is
    derived directly from ``schema_path`` (see :func:`derive_asn1_field_defs`) -- no
    hand-authored field list.

    Parameters
    ----------
    df:
        Input DataFrame containing a binary column of raw ASN.1-encoded records.
    schema_path:
        Path to the real ``.asn``/ASN.1 module definition file. Since this is a plain
        ``source_config`` field (unlike the old external-JSON-file design), any
        ``{{catalog}}``/``{{env}}`` placeholder it contains is already substituted by
        ``onboarding/spec_loader.py`` before the pipeline ever runs -- no separate
        catalog-inference step is needed here.
    codec:
        ASN.1 encoding rule, ``"ber"`` or ``"der"``.
    pdu_name:
        Name of the top-level ``SEQUENCE`` type in ``schema_path`` to decode each record as.
    binary_column:
        Name of the column in ``df`` holding the raw encoded bytes.

    Returns
    -------
    DataFrame
        ``df`` with the binary column replaced by one decoded column per member of
        ``pdu_name`` (nested ``SEQUENCE``/``SEQUENCE OF`` members become ``struct``/``array``
        columns), plus a ``_asn1_decode_error`` column (null on success) capturing per-row
        decode failures without aborting the whole micro-batch.

    Raises
    ------
    Asn1DecodeError
        If asn1tools is unavailable, ``binary_column`` doesn't exist, or the module file
        can't be parsed/doesn't resolve to a valid schema (see
        :func:`derive_asn1_field_defs`). Per-row decode failures are captured inline, not
        raised.
    """
    if not _ASN1TOOLS_AVAILABLE:
        raise Asn1DecodeError("asn1tools is required for ASN.1 decoding but is not installed.")
    if binary_column not in df.columns:
        raise Asn1DecodeError(f"binary_column '{binary_column}' not found in input DataFrame columns: {df.columns}")

    df = _materialize_hidden_metadata_column(df)
    field_defs = derive_asn1_field_defs(schema_path, pdu_name)

    passthrough_fields = [f for f in df.schema.fields if f.name != binary_column]
    decoded_fields = [StructField(field_def["name"], field_def["spark_type"], True) for field_def in field_defs]
    output_schema = StructType(passthrough_fields + decoded_fields + [StructField("_asn1_decode_error", StringType(), True)])

    partition_decoder = make_partition_decoder(
        [schema_path], codec, pdu_name, field_defs, binary_column, [f.name for f in passthrough_fields]
    )
    return df.mapInPandas(partition_decoder, schema=output_schema)
