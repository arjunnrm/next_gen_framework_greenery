"""End-to-end unit tests for asn1/decoder.py::decode_asn1_binary_stream -- real Spark
(Databricks Connect, via the `spark` fixture), real asn1tools compile/encode/decode, real
mapInPandas execution. No mocking of the codec or Spark.

Regression coverage for a real bug found by adversarial code review, before this file
existed: decode_asn1_binary_stream and read_asn1_source had ZERO test coverage anywhere in
this repo (make_partition_decoder was tested directly with a fake compiled codec;
derive_asn1_field_defs was tested directly with pure Python/asn1tools; neither exercised
mapInPandas or a real DataFrame). That gap is exactly how a real bug went undetected:
_materialize_hidden_metadata_column's try/except around `df.withColumn("_metadata", ...)`
looked like it would catch an unresolved-column failure for a DataFrame with no genuine
Auto-Loader `_metadata` pseudo-column (the docstring's own claimed "non-Auto-Loader
DataFrame in a test" case) -- but under Spark Connect, DataFrame construction is lazy:
`withColumn` never raises for an unresolved column reference on its own, only accessing
`.schema` (or another analysis-triggering call) does. The `except` was dead code, and the
real `AnalysisException` erupted, uncaught, from an unrelated `.schema` access two lines
later in `decode_asn1_binary_stream` -- confirmed live and fixed by forcing `.schema`
resolution *inside* `_materialize_hidden_metadata_column`'s own try. The tests below call
`decode_asn1_binary_stream` directly against both a DataFrame *without* a `_metadata` column
(the exact case that crashed) and one *with* it (the real Auto-Loader shape), proving both
paths now work.
"""

import tempfile
import os

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder import (
    CHOICE_DISCRIMINATOR_FIELD,
    decode_asn1_binary_stream,
)

pytest.importorskip("asn1tools")
import asn1tools  # noqa: E402

_FLAT_MODULE = """
FlatModule DEFINITIONS ::= BEGIN

FlatRec ::= SEQUENCE {
    imsi                    IA5String,
    callDurationSeconds     INTEGER
}

END
"""

_NESTED_MODULE = """
NestedModule DEFINITIONS ::= BEGIN

Address ::= SEQUENCE {
    city UTF8String,
    zip  UTF8String
}

NestedRec ::= SEQUENCE {
    id       INTEGER,
    address  Address,
    tags     SEQUENCE OF UTF8String
}

END
"""


def _write_module(tmp_path, text):
    path = tmp_path / "module.asn"
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_decode_without_a_metadata_column_does_not_raise(spark, tmp_path):
    """The exact regression case: a DataFrame with no genuine Auto Loader `_metadata` hidden
    column (e.g. any non-Auto-Loader DataFrame, as decode_asn1_binary_stream's own docstring
    claims is handled gracefully) must decode successfully, not crash with a raw
    AnalysisException from _materialize_hidden_metadata_column's dead-code try/except."""
    module_path = _write_module(tmp_path, _FLAT_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    encoded = compiled.encode("FlatRec", {"imsi": "999888777", "callDurationSeconds": 42})

    df = spark.createDataFrame([("file1.ber", encoded)], ["path", "content"])

    result = decode_asn1_binary_stream(df, module_path, "ber", "FlatRec", binary_column="content")
    row = result.collect()[0]

    assert row["imsi"] == "999888777"
    assert row["callDurationSeconds"] == 42
    assert row["_asn1_decode_error"] is None
    assert "content" not in result.columns


def test_decode_with_a_real_metadata_column_present_is_passed_through(spark, tmp_path):
    """The real Auto Loader shape: `_metadata` already a real struct column (simulating what
    _materialize_hidden_metadata_column's fast path -- "_metadata" in df.columns -- handles)."""
    module_path = _write_module(tmp_path, _FLAT_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    encoded = compiled.encode("FlatRec", {"imsi": "111222333", "callDurationSeconds": 7})

    df = spark.createDataFrame(
        [("file1.ber", encoded, "file1.ber")], ["path", "content", "_metadata"]
    )

    result = decode_asn1_binary_stream(df, module_path, "ber", "FlatRec", binary_column="content")
    row = result.collect()[0]

    assert row["imsi"] == "111222333"
    assert row["_metadata"] == "file1.ber"


def test_decode_nested_sequence_and_sequence_of_end_to_end(spark, tmp_path):
    """A nested SEQUENCE (-> struct) and SEQUENCE OF (-> array) round-trip correctly through
    the full mapInPandas decode path, not just through derive_asn1_field_defs' schema
    derivation in isolation."""
    module_path = _write_module(tmp_path, _NESTED_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    encoded = compiled.encode(
        "NestedRec", {"id": 5, "address": {"city": "Springfield", "zip": "12345"}, "tags": ["a", "b"]}
    )

    df = spark.createDataFrame([("file1.ber", encoded)], ["path", "content"])

    result = decode_asn1_binary_stream(df, module_path, "ber", "NestedRec", binary_column="content")
    row = result.collect()[0]

    assert row["id"] == 5
    assert row["address"]["city"] == "Springfield"
    assert row["address"]["zip"] == "12345"
    assert list(row["tags"]) == ["a", "b"]


def test_decode_failure_is_isolated_to_the_offending_row_via_end_to_end_mapinpandas(spark, tmp_path):
    module_path = _write_module(tmp_path, _FLAT_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    good_encoded = compiled.encode("FlatRec", {"imsi": "555", "callDurationSeconds": 1})

    df = spark.createDataFrame(
        [("good.ber", good_encoded), ("bad.ber", b"not-valid-ber-at-all")], ["path", "content"]
    )

    result = decode_asn1_binary_stream(df, module_path, "ber", "FlatRec", binary_column="content")
    rows = {row["path"]: row for row in result.collect()}

    assert rows["good.ber"]["_asn1_decode_error"] is None
    assert rows["good.ber"]["imsi"] == "555"
    assert rows["bad.ber"]["_asn1_decode_error"] is not None


_CHOICE_MODULE = """
ChoiceModule DEFINITIONS ::= BEGIN

Notification ::= SEQUENCE {
    sender  UTF8String,
    seq     INTEGER
}

TransferBatch ::= SEQUENCE {
    batchId UTF8String
}

-- Arms are context-tagged, exactly as the real GGSN/PSGW CallEventRecord is. This is not
-- cosmetic: two UNTAGGED SEQUENCE arms are genuinely indistinguishable on the BER wire (both
-- carry the universal SEQUENCE tag), so asn1tools cannot round-trip such a CHOICE at all --
-- verified live, a bare compiled.decode() fails on it identically, with no framework involved.
DataInterChange ::= CHOICE {
    transferBatch [1] TransferBatch,
    notification  [2] Notification,
    ...
}

Wrapper ::= SEQUENCE {
    id      INTEGER,
    payload DataInterChange
}

END
"""

_CHOICE_COLLISION_MODULE = """
CollisionModule DEFINITIONS ::= BEGIN

Payload ::= CHOICE {
    raw  OCTET STRING,
    text UTF8String
}

Rec ::= SEQUENCE {
    payload Payload,
    flags   BIT STRING
}

END
"""


def test_root_choice_pdu_decodes_end_to_end_with_unselected_arms_null(spark, tmp_path):
    """The feature's headline case, proven across the real mapInPandas/Arrow boundary rather
    than in schema-derivation isolation: a CHOICE root PDU (the shape of TAP's
    DataInterChange, 3GPP's CallEventRecord and EMSC's CallDataRecord) yields one column per
    arm, the selected arm populated, every other arm NULL, plus a discriminator naming it.

    A mostly-NULL struct is exactly the kind of shape that derives fine and then fails at the
    Arrow boundary, which is the gap this whole test file exists to close."""
    module_path = _write_module(tmp_path, _CHOICE_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    encoded = compiled.encode("DataInterChange", ("notification", {"sender": "BT", "seq": 9}))

    df = spark.createDataFrame([("file1.ber", encoded)], ["path", "content"])

    result = decode_asn1_binary_stream(df, module_path, "ber", "DataInterChange", binary_column="content")
    row = result.collect()[0]

    assert row[CHOICE_DISCRIMINATOR_FIELD] == "notification"
    assert row["notification"]["sender"] == "BT"
    assert row["notification"]["seq"] == 9
    assert row["transferBatch"] is None, "an unselected CHOICE arm must decode to NULL"
    assert row["_asn1_decode_error"] is None


def test_root_choice_other_arm_selected_decodes_to_the_other_column(spark, tmp_path):
    """Same PDU, the other arm selected -- proves the arm columns are genuinely driven by the
    decoded discriminator and not just whichever arm happens to be listed first."""
    module_path = _write_module(tmp_path, _CHOICE_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    encoded = compiled.encode("DataInterChange", ("transferBatch", {"batchId": "B-1"}))

    df = spark.createDataFrame([("file1.ber", encoded)], ["path", "content"])

    result = decode_asn1_binary_stream(df, module_path, "ber", "DataInterChange", binary_column="content")
    row = result.collect()[0]

    assert row[CHOICE_DISCRIMINATOR_FIELD] == "transferBatch"
    assert row["transferBatch"]["batchId"] == "B-1"
    assert row["notification"] is None


def test_nested_choice_inside_a_sequence_pdu_decodes_end_to_end(spark, tmp_path):
    """A CHOICE reached through a SEQUENCE member (TAP's TransferBatch -> CallEventDetail
    shape) becomes a nested struct column with its own discriminator."""
    module_path = _write_module(tmp_path, _CHOICE_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    encoded = compiled.encode("Wrapper", {"id": 3, "payload": ("notification", {"sender": "EE", "seq": 1})})

    df = spark.createDataFrame([("file1.ber", encoded)], ["path", "content"])

    result = decode_asn1_binary_stream(df, module_path, "ber", "Wrapper", binary_column="content")
    row = result.collect()[0]

    assert row["id"] == 3
    assert row["payload"][CHOICE_DISCRIMINATOR_FIELD] == "notification"
    assert row["payload"]["notification"]["sender"] == "EE"
    assert row["payload"]["transferBatch"] is None


def test_choice_of_octet_string_and_a_real_bit_string_are_not_confused(spark, tmp_path):
    """The collision case end to end. A CHOICE decodes to ``(name, value)`` and a BIT STRING to
    ``(bytes, bit_length)`` -- both bare 2-tuples. Here one record carries both, so a
    shape-guessing normalizer would necessarily get one of them wrong; only dispatching on the
    declared ASN.1 type produces both correctly."""
    module_path = _write_module(tmp_path, _CHOICE_COLLISION_MODULE)
    compiled = asn1tools.compile_files([module_path], "ber")
    encoded = compiled.encode("Rec", {"payload": ("raw", b"\x01\x02"), "flags": (b"\xf0", 4)})

    df = spark.createDataFrame([("file1.ber", encoded)], ["path", "content"])

    result = decode_asn1_binary_stream(df, module_path, "ber", "Rec", binary_column="content")
    row = result.collect()[0]

    assert row["payload"][CHOICE_DISCRIMINATOR_FIELD] == "raw"
    assert bytes(row["payload"]["raw"]) == b"\x01\x02", "OCTET STRING arm must stay raw binary"
    assert row["payload"]["text"] is None
    assert bytes(row["flags"]["bytes"]) == b"\xf0"
    assert row["flags"]["bit_length"] == 4
    assert row["_asn1_decode_error"] is None
