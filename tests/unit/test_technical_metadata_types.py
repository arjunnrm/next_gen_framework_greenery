"""``__framework_source_*`` technical-metadata columns must have DETERMINISTIC types.

Regression test for a real Phase C failure. ``attach_technical_metadata`` extracts each column
from the ``_metadata`` pseudo-column inside a ``try``, falling back to a NULL literal when the
expression does not resolve. The fallback used to cast to ``string`` unconditionally, while the
success path yielded whatever ``_metadata`` natively produces -- ``BIGINT`` for ``file_size``,
``TIMESTAMP`` for ``file_modification_time``.

``_metadata`` is a FILE-SOURCE pseudo-column, so against a non-file source (a ``zerobus`` read of
an existing Delta table) it may or may not resolve depending on how the plan is analysed. The
same flow could therefore emit ``LongType`` on one run and ``StringType`` on the next, and Delta
refused to merge the result::

    [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields '__framework_source_file_size' ...
    [DELTA_MERGE_INCOMPATIBLE_DATATYPE] Failed to merge incompatible data types
                                         StringType and LongType

That is a graph-ANALYSIS failure: it killed the entire pipeline update -- every table in the
group, not only the one named in the message -- and it could not be cleared by dropping the
target tables, because the conflicting schema is held in the pipeline's own state.

The fix pins the type on BOTH branches. These tests assert the declared types and that the two
branches agree, so the two can never drift apart again.
"""

import pytest

from flowx.lakeflow_framework.ingestion.technical_metadata import _METADATA_FIELD_EXTRACTIONS


EXPECTED_TYPES = {
    "__framework_source_file_name": "string",
    "__framework_source_file_size": "bigint",
    "__framework_source_file_modification_time": "timestamp",
}


def test_every_metadata_field_declares_an_explicit_type():
    """Each entry is ``(expression, spark_type)`` -- a bare string would reintroduce the bug."""
    for output_col, entry in _METADATA_FIELD_EXTRACTIONS.items():
        assert isinstance(entry, tuple), f"{output_col} must declare (expression, spark_type), got {entry!r}"
        assert len(entry) == 2, f"{output_col} must declare exactly (expression, spark_type), got {entry!r}"
        expression, spark_type = entry
        assert isinstance(expression, str) and expression
        assert isinstance(spark_type, str) and spark_type


@pytest.mark.parametrize("output_col,expected_type", sorted(EXPECTED_TYPES.items()))
def test_declared_type_matches_the_metadata_fields_native_type(output_col, expected_type):
    """The declared type must be what ``_metadata`` natively produces.

    Declaring ``string`` for ``file_size`` would "work" (both branches would agree) but would
    silently store a number as text and diverge from every table already materialized with the
    native type. The point is agreement with Databricks' ``_metadata`` struct, not merely
    internal self-consistency.
    """
    _, spark_type = _METADATA_FIELD_EXTRACTIONS[output_col]
    assert spark_type == expected_type


def test_the_field_set_is_exactly_the_three_supported_columns():
    """Guards against a fourth field being added without a type decision being made."""
    assert set(_METADATA_FIELD_EXTRACTIONS) == set(EXPECTED_TYPES)


def test_no_field_falls_back_to_string_unless_it_is_natively_a_string():
    """The precise shape of the original defect, pinned.

    ``file_size`` and ``file_modification_time`` falling back to ``string`` is exactly what made
    the emitted schema non-deterministic.
    """
    non_string_fields = {
        output_col: spark_type
        for output_col, (_, spark_type) in _METADATA_FIELD_EXTRACTIONS.items()
        if EXPECTED_TYPES[output_col] != "string"
    }
    assert non_string_fields, "expected at least one non-string metadata field to guard"
    for output_col, spark_type in non_string_fields.items():
        assert spark_type != "string", (
            f"{output_col} must not fall back to 'string' -- that is the non-deterministic-type "
            "regression this test exists to prevent"
        )
