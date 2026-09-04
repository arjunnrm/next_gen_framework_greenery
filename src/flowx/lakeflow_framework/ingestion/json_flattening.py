"""Runtime application of ``explode_columns`` -- JSON struct/array flattening for ingestion flows.

Empty/absent ``explode_columns`` is schema-preserving by default: the DataFrame is returned
unchanged. Recursively flattening every struct column and exploding every array column found
anywhere in the schema (a full "flatten this whole JSON document" pass) is opt-in only, via
``auto_flatten_all=True`` -- since ``explode_columns`` is an optional ``source_config`` field,
a spec that simply omits it must not undergo any implicit flattening (that used to cause
silent cartesian row explosion and schema distortion on ordinary, un-configured sources).

A populated ``explode_columns`` list scopes struct-flatten/array-explode treatment to exactly
the named top-level columns, regardless of ``auto_flatten_all``; each must resolve, on the
actual DataFrame, to a ``StructType`` or ``ArrayType`` (checked here at runtime -- the
onboarding validator only checks the field is a list of strings, since it has no access to the
source's actual runtime schema at onboarding time).

**Absent vs present-but-empty.** At the *spec* level those two are no longer the same thing: a
``source_config`` carrying a literal ``"explode_columns": []`` means "auto-flatten everything",
while one that omits the key entirely -- or spells it as an explicit JSON ``null``, which is how
a scaffolded template says "not configured" -- keeps the schema-preserving pass-through
described above. That distinction is *impossible* to make inside :func:`apply_explode_columns`:
by the time a caller has done ``source_config.get("explode_columns")``, absent and
present-but-empty have already collapsed to the same ``None``/``[]``. It is therefore resolved
one layer up, against the raw dict, by :func:`resolve_auto_flatten_all`.
:func:`apply_explode_columns` keeps its exact previous signature *and* semantics -- only the
``auto_flatten_all`` value the call site computes for it changes -- which is what preserves the
cartesian-explosion protection for every existing spec.

**JSON held in a STRING column.** Parquet, CSV, Delta and Zerobus sources frequently carry a
JSON document inside an ordinary string column rather than as a native nested type.
:func:`parse_json_string_columns` parses those named columns into structs with ``from_json``
*immediately before* the flatten/explode pass, so everything else in this module then applies to
them unchanged. That is what gives non-JSON source formats parity with a native JSON source,
rather than a second, parallel flattening implementation that would inevitably drift from this
one. Struct/array columns that Parquet already carries natively need nothing new -- they flow
straight into :func:`apply_explode_columns` exactly as a JSON source's do.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DataType, StringType, StructType

from flowx.lakeflow_framework.exceptions import FrameworkConfigError

logger = logging.getLogger("common.ingestion.json_flattening")

# Defensive bound on recursive flatten passes for the "flatten everything" mode -- not
# expected to matter for any real-world JSON document nesting depth.
_MAX_FLATTEN_PASSES = 10


def _flatten_struct_column(df: DataFrame, column_name: str) -> DataFrame:
    """Replace a struct column with one flattened column per its sub-field, named
    ``<column>_<subfield>`` so a nested ``address.city`` becomes the top-level ``address_city``."""
    struct_field = next(f for f in df.schema.fields if f.name == column_name)
    sub_fields = struct_field.dataType.fields
    passthrough = [c for c in df.columns if c != column_name]
    flattened = [F.col(f"`{column_name}`.`{sub.name}`").alias(f"{column_name}_{sub.name}") for sub in sub_fields]
    return df.select(*passthrough, *flattened)


def _explode_array_column(df: DataFrame, column_name: str) -> DataFrame:
    """Explode an array column into one row per element, keeping the same column name.

    Uses ``explode_outer`` (not ``explode``) so a row with an empty/null array is kept (with
    a null element) rather than silently dropped -- exploding is a shape transform for JSON
    ingestion, not a row filter.
    """
    return df.withColumn(column_name, F.explode_outer(F.col(column_name)))


def _column_types(df: DataFrame) -> Dict[str, DataType]:
    return {f.name: f.dataType for f in df.schema.fields}


def _flatten_all(df: DataFrame) -> DataFrame:
    """Repeatedly flatten every struct column and explode every array column until none
    remain (or the pass budget is exhausted)."""
    result_df = df
    for _ in range(_MAX_FLATTEN_PASSES):
        nested_columns = [(name, data_type) for name, data_type in _column_types(result_df).items() if isinstance(data_type, (StructType, ArrayType))]
        if not nested_columns:
            break
        for column_name, data_type in nested_columns:
            current_type = _column_types(result_df).get(column_name)
            if current_type is None:
                continue  # consumed by an earlier operation processed within this same pass
            if isinstance(current_type, ArrayType):
                result_df = _explode_array_column(result_df, column_name)
            elif isinstance(current_type, StructType):
                result_df = _flatten_struct_column(result_df, column_name)
    return result_df


def apply_explode_columns(
    df: DataFrame, explode_columns: Optional[List[str]], auto_flatten_all: bool = False
) -> DataFrame:
    """Apply ``explode_columns`` semantics to ``df``.

    * Empty/``None``: schema-preserving pass-through -- ``df`` is returned unchanged --
      *unless* ``auto_flatten_all`` is truthy, in which case every struct column is flattened
      and every array column is exploded, anywhere in the schema, recursively -- see
      :func:`_flatten_all`. ``auto_flatten_all`` is ignored once ``explode_columns`` is
      populated (see below); it only governs what happens when the list is empty/absent.
    * Populated: scope the same struct-flatten/array-explode treatment to exactly the named
      top-level columns. An array of structs is exploded, then its resulting struct element
      is immediately flattened too, so one entry (e.g. ``"line_items"``) fully de-nests an
      ``array<struct<...>>`` column in one step.

    Raises
    ------
    FrameworkConfigError
        If a named column doesn't exist on ``df``, or resolves to neither a struct nor an
        array type.
    """
    if not explode_columns:
        if auto_flatten_all:
            return _flatten_all(df)
        return df

    result_df = df
    for column_name in explode_columns:
        column_types = _column_types(result_df)
        if column_name not in column_types:
            raise FrameworkConfigError(
                f"explode_columns names '{column_name}', which is not a column on this DataFrame "
                f"(columns: {result_df.columns})"
            )
        data_type = column_types[column_name]
        if isinstance(data_type, ArrayType):
            result_df = _explode_array_column(result_df, column_name)
            exploded_type = _column_types(result_df).get(column_name)
            if isinstance(exploded_type, StructType):
                result_df = _flatten_struct_column(result_df, column_name)
        elif isinstance(data_type, StructType):
            result_df = _flatten_struct_column(result_df, column_name)
        else:
            raise FrameworkConfigError(
                f"explode_columns names '{column_name}', which is type {data_type.simpleString()} -- "
                "only struct or array columns can be exploded/flattened"
            )
    return result_df


def resolve_auto_flatten_all(source_config: Dict[str, Any]) -> bool:
    """Resolve ``auto_flatten_all`` for one ingestion flow from its raw ``source_config`` dict.

    Returns ``True`` when either ``auto_flatten_all`` is explicitly ``true``, **or**
    ``explode_columns`` is *present* on the dict and is an empty list. An **absent**
    ``explode_columns`` -- and an explicit JSON ``null``, which is how a scaffolded template
    spells "not configured" -- returns ``False``, preserving the schema-preserving pass-through
    that exists specifically to stop silent cartesian row explosion on un-configured sources.

    This takes the **raw** ``source_config`` dict rather than a value already pulled out of it
    because the absent-vs-present-but-empty distinction only survives while the ``in`` test is
    still possible: ``.get()`` collapses both cases to the same ``None``/``[]``. That is exactly
    why the decision lives here instead of inside :func:`apply_explode_columns`, whose signature
    and semantics are deliberately left untouched so every existing caller and test keeps its
    previous meaning.
    """
    if source_config.get("auto_flatten_all") is True:
        return True
    explode_columns = source_config.get("explode_columns")
    return "explode_columns" in source_config and explode_columns is not None and len(explode_columns) == 0


def _resolve_json_string_column_entry(entry: Any) -> Tuple[str, Optional[str]]:
    """Normalize one ``json_string_columns`` entry to a ``(column, schema_ddl)`` pair.

    Both accepted spec shapes -- the plain-string shorthand ``"payload"`` and the object
    ``{"column": "payload", "schema_ddl": "struct<...>"}`` -- collapse to the same pair here, so
    the parsing code below only ever reasons about one shape.

    Anything else raises rather than being skipped: the onboarding validator already rejects
    these shapes, so reaching this branch means a hand-edited control-table row, and silently
    ignoring an entry the operator believes is being parsed would leave a raw JSON string sitting
    in the target table with no diagnostic anywhere. An empty ``schema_ddl`` is treated as absent
    (it carries no schema either way) and therefore takes the streaming-inference path.
    """
    if isinstance(entry, str):
        return entry, None
    if isinstance(entry, dict):
        column = entry.get("column")
        if not isinstance(column, str) or not column:
            raise FrameworkConfigError(
                f"json_string_columns entry {entry!r} has no usable 'column' key -- expected a column-name "
                "string or an object with a non-empty 'column' key"
            )
        schema_ddl = entry.get("schema_ddl")
        if schema_ddl is not None and not isinstance(schema_ddl, str):
            raise FrameworkConfigError(
                f"json_string_columns entry for '{column}' has a non-string schema_ddl {schema_ddl!r} -- "
                "expected Spark DDL, e.g. 'struct<order_id:string,total:double>'"
            )
        return column, schema_ddl or None
    raise FrameworkConfigError(
        f"json_string_columns entry {entry!r} is neither a column-name string nor an object with a 'column' key"
    )


def _parse_one_json_string_column(df: DataFrame, column: str, schema_ddl: Optional[str]) -> DataFrame:
    """Replace one STRING column holding a JSON document with the struct ``from_json`` parses out of it.

    The column is replaced in place (same name, new type) rather than added alongside, so the
    downstream flatten/explode pass sees exactly the shape a native JSON source would have
    produced -- and so an ``explode_columns`` entry naming it needs no special spelling.

    ``schema_ddl`` is the recommended form: ``from_json`` with an explicit schema is fully
    deterministic and behaves identically on batch and streaming plans. Without it the framework
    falls back to Databricks' *inferring* ``from_json``, which persists the inferred schema under
    a ``schemaLocationKey`` in the flow's checkpoint and therefore only exists for streaming
    plans. There is deliberately no batch fallback via ``schema_of_json(lit(<sampled value>))``:
    sampling a value requires an eager action, which is illegal on a streaming plan and a full
    extra scan on a batch one, and the ingestion staged view is built lazily inside a
    ``@dlt.view`` closure where neither is acceptable.

    Raises
    ------
    FrameworkConfigError
        If ``column`` is not on ``df``, is not a string column, or has no ``schema_ddl`` while
        ``df`` is not streaming.
    """
    column_types = _column_types(df)
    if column not in column_types:
        raise FrameworkConfigError(
            f"json_string_columns names '{column}', which is not a column on this DataFrame "
            f"(columns: {df.columns})"
        )
    data_type = column_types[column]
    if not isinstance(data_type, StringType):
        raise FrameworkConfigError(
            f"json_string_columns names '{column}', which is type {data_type.simpleString()} -- "
            "only string columns holding a JSON document can be parsed with from_json"
        )

    # Backtick-quoted so a raw source column name containing a '.' resolves as a literal
    # identifier rather than being parsed as nested-field access -- the same reason
    # _flatten_struct_column quotes its own references.
    if schema_ddl:
        return df.withColumn(column, F.from_json(F.col(f"`{column}`"), schema_ddl))

    if not df.isStreaming:
        raise FrameworkConfigError(
            f"json_string_columns entry '{column}' has no schema_ddl and this flow's DataFrame is not "
            "streaming -- schema inference via from_json's schemaLocationKey requires a streaming source "
            "with a checkpoint. Supply an explicit schema_ddl (e.g. 'struct<a:string,b:int>') for this column."
        )
    logger.info(
        "json_string_columns: parsing '%s' with inferring from_json (no schema_ddl supplied); the inferred "
        "schema is persisted in this flow's checkpoint under schemaLocationKey '%s'.",
        column,
        column,
    )
    return df.withColumn(
        column,
        F.expr(f"from_json(`{column}`, map('schemaLocationKey', '{column}', 'schemaEvolutionMode', 'addNewColumns'))"),
    )


def parse_json_string_columns(df: DataFrame, json_string_columns: Optional[List[Any]]) -> DataFrame:
    """Parse each named STRING column holding a JSON document into a struct, via ``from_json``.

    A no-op for ``None``/empty, which is the overwhelmingly common case -- this runs on every
    ingestion flow, and a flow that names no JSON string columns must be byte-for-byte unaffected.

    Each item is either a plain column-name string (``"payload"``) or an object
    (``{"column": "payload", "schema_ddl": "struct<order_id:string,total:double>"}``); see
    :func:`_resolve_json_string_column_entry`. Supplying ``schema_ddl`` is strongly recommended --
    see :func:`_parse_one_json_string_column` for why the schema-less form is streaming-only.

    Columns are processed in the order given, against the running DataFrame, so naming the same
    column twice fails on the second pass with the "which is type struct<...>" error rather than
    silently re-parsing an already-parsed struct; the onboarding validator rejects duplicates up
    front, and this is simply the runtime restatement of that rule.

    This deliberately does **not** flatten anything itself: it only changes a column's *type*
    from string to struct, leaving :func:`apply_explode_columns` -- which runs immediately after
    it -- as the single implementation of struct-flatten/array-explode for every source format.

    Raises
    ------
    FrameworkConfigError
        On a malformed entry, a column that isn't on ``df``, a non-string column, or a
        schema-less entry on a non-streaming DataFrame.
    """
    if not json_string_columns:
        return df

    result_df = df
    for entry in json_string_columns:
        column, schema_ddl = _resolve_json_string_column_entry(entry)
        result_df = _parse_one_json_string_column(result_df, column, schema_ddl)

    logger.info("json_string_columns: parsed %d string column(s) into structs.", len(json_string_columns))
    return result_df
