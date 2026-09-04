"""Unit tests for asn1/decoder.py::make_partition_decoder -- pure Python + pandas, no Spark
or a live ASN.1 codec needed.

``make_partition_decoder`` returns a plain generator function (the exact callable
``mapInPandas`` would invoke once per partition on a live cluster) -- calling it directly
here, outside of Spark, is what lets this prove the "compiled once per partition, not once
per row" behavior deterministically: `asn1tools.compile_files` is monkeypatched with a
call-counting stub, and the test asserts it was invoked exactly once even though the
generator is fed multiple pandas micro-batches (simulating a partition with several
Auto-Loader-sized batches), and even though those batches together contain many rows.

Regression coverage for the real perf bug being fixed: the original implementation used a
plain row UDF that called ``asn1tools.compile_files`` inside the per-row decode function,
recompiling the ASN.1 module from scratch on every single row.
"""

from unittest.mock import patch

import pandas as pd

from flowx.lakeflow_framework.asn1.decoder import (
    CHOICE_DISCRIMINATOR_FIELD,
    make_partition_decoder,
)


class _FakeCompiled:
    def decode(self, pdu_name, raw_bytes):
        if raw_bytes == b"BAD":
            raise ValueError("corrupt payload")
        return {"imsi": raw_bytes.decode("utf-8"), "callDurationSeconds": len(raw_bytes)}


def _batches(*row_lists):
    for rows in row_lists:
        yield pd.DataFrame(rows, columns=["path", "content"])


# ``asn1_node`` is what makes normalization schema-aware: it is the asn1tools parse node the
# column was derived from, so _normalize_decoded_value dispatches on the DECLARED ASN.1 type
# rather than guessing from the decoded value's Python shape (a BIT STRING and a CHOICE both
# decode to a bare 2-tuple and are otherwise indistinguishable).
FIELD_DEFS = [
    {"name": "imsi", "spark_type": "string", "asn1_node": {"type": "IA5String", "name": "imsi"}},
    {
        "name": "callDurationSeconds",
        "spark_type": "long",
        "asn1_node": {"type": "INTEGER", "name": "callDurationSeconds"},
    },
]


def test_compile_files_called_exactly_once_per_partition_across_multiple_batches():
    call_count = {"n": 0}

    def _counting_compile_files(module_files, codec):
        call_count["n"] += 1
        return _FakeCompiled()

    decoder = make_partition_decoder(
        module_files=["/Volumes/poc/schemas/telecom_cdr.asn"],
        codec="ber",
        pdu_name="CallDetailRecord",
        field_defs=FIELD_DEFS,
        binary_column="content",
        passthrough_columns=["path"],
    )

    batches = _batches(
        [{"path": "f1.bin", "content": b"111"}, {"path": "f2.bin", "content": b"222"}],
        [{"path": "f3.bin", "content": b"333"}],
        [{"path": "f4.bin", "content": b"444"}, {"path": "f5.bin", "content": b"555"}, {"path": "f6.bin", "content": b"666"}],
    )

    with patch("flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files", side_effect=_counting_compile_files):
        result_batches = list(decoder(batches))

    assert call_count["n"] == 1, "the ASN.1 schema must be compiled exactly once for the whole partition, not once per row/batch"
    total_rows = sum(len(b) for b in result_batches)
    assert total_rows == 6


def test_successfully_decoded_rows_populate_configured_fields():
    decoder = make_partition_decoder(
        module_files=["/x.asn"], codec="ber", pdu_name="CallDetailRecord", field_defs=FIELD_DEFS,
        binary_column="content", passthrough_columns=["path"],
    )
    with patch("flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
        result = list(decoder(_batches([{"path": "f1.bin", "content": b"ABC"}])))[0]

    row = result.iloc[0]
    assert row["imsi"] == "ABC"
    assert row["callDurationSeconds"] == 3
    assert row["_asn1_decode_error"] is None
    assert row["path"] == "f1.bin"


def test_decode_failure_is_isolated_to_the_offending_row_not_the_whole_batch():
    decoder = make_partition_decoder(
        module_files=["/x.asn"], codec="ber", pdu_name="CallDetailRecord", field_defs=FIELD_DEFS,
        binary_column="content", passthrough_columns=["path"],
    )
    with patch("flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
        result = list(decoder(_batches([{"path": "good.bin", "content": b"OK"}, {"path": "bad.bin", "content": b"BAD"}])))[0]

    good_row = result[result["path"] == "good.bin"].iloc[0]
    bad_row = result[result["path"] == "bad.bin"].iloc[0]
    assert good_row["_asn1_decode_error"] is None
    assert good_row["imsi"] == "OK"
    assert bad_row["_asn1_decode_error"] is not None
    assert "corrupt payload" in bad_row["_asn1_decode_error"]
    assert bad_row["imsi"] is None


def test_null_binary_payload_flagged_without_crashing():
    decoder = make_partition_decoder(
        module_files=["/x.asn"], codec="ber", pdu_name="CallDetailRecord", field_defs=FIELD_DEFS,
        binary_column="content", passthrough_columns=["path"],
    )
    with patch("flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
        result = list(decoder(_batches([{"path": "null.bin", "content": None}])))[0]

    row = result.iloc[0]
    assert row["_asn1_decode_error"] == "null_payload"
    assert row["imsi"] is None


def test_empty_partition_yields_no_rows_without_error():
    decoder = make_partition_decoder(
        module_files=["/x.asn"], codec="ber", pdu_name="CallDetailRecord", field_defs=FIELD_DEFS,
        binary_column="content", passthrough_columns=["path"],
    )
    with patch("flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
        result_batches = list(decoder(iter([])))

    assert result_batches == []


class _FakeCompiledWithBitString:
    """Simulates asn1tools' real decode shape for BIT STRING (a bare
    (bytes_or_bytearray, bit_length) tuple, confirmed live -- see decoder.py's
    _normalize_decoded_value), including nested inside a dict and a list, to prove the
    normalization recurses correctly."""

    def decode(self, pdu_name, raw_bytes):
        return {
            "flags": (bytearray(b"\xf0"), 4),
            "nested": {"bs": (bytearray(b"\xff"), 8), "x": 7},
            "listOfBits": [(bytearray(b"\xff"), 8), (bytearray(b"\x0f"), 4)],
        }


# The ASN.1 shapes _FakeCompiledWithBitString's decoded values correspond to. Normalization
# walks these in lockstep with the value, so a BIT STRING nested inside a SEQUENCE or a
# SEQUENCE OF at any depth is still reshaped.
_NESTED_BIT_STRING_NODE = {
    "type": "SEQUENCE",
    "name": "nested",
    "members": [
        {"type": "BIT STRING", "name": "bs"},
        {"type": "INTEGER", "name": "x"},
    ],
}
_LIST_OF_BITS_NODE = {
    "type": "SEQUENCE OF",
    "name": "listOfBits",
    "element": {"type": "BIT STRING", "name": "item"},
}


def test_bit_string_tuple_is_normalized_to_a_bytes_and_bit_length_dict():
    decoder = make_partition_decoder(
        module_files=["/x.asn"],
        codec="ber",
        pdu_name="Rec",
        field_defs=[{"name": "flags", "spark_type": "unused", "asn1_node": {"type": "BIT STRING", "name": "flags"}}],
        binary_column="content",
        passthrough_columns=["path"],
    )
    with patch(
        "flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files",
        return_value=_FakeCompiledWithBitString(),
    ):
        result = list(decoder(_batches([{"path": "f1.bin", "content": b"ABC"}])))[0]

    row = result.iloc[0]
    assert row["flags"] == {"bytes": b"\xf0", "bit_length": 4}


def test_bit_string_nested_inside_a_struct_and_a_list_is_also_normalized():
    decoder = make_partition_decoder(
        module_files=["/x.asn"],
        codec="ber",
        pdu_name="Rec",
        field_defs=[
            {"name": "nested", "spark_type": "unused", "asn1_node": _NESTED_BIT_STRING_NODE},
            {"name": "listOfBits", "spark_type": "unused", "asn1_node": _LIST_OF_BITS_NODE},
        ],
        binary_column="content",
        passthrough_columns=["path"],
    )
    with patch(
        "flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files",
        return_value=_FakeCompiledWithBitString(),
    ):
        result = list(decoder(_batches([{"path": "f1.bin", "content": b"ABC"}])))[0]

    row = result.iloc[0]
    assert row["nested"] == {"bs": {"bytes": b"\xff", "bit_length": 8}, "x": 7}
    assert row["listOfBits"] == [{"bytes": b"\xff", "bit_length": 8}, {"bytes": b"\x0f", "bit_length": 4}]


def test_output_columns_are_passthrough_plus_decoded_fields_plus_error_no_binary_column():
    decoder = make_partition_decoder(
        module_files=["/x.asn"], codec="ber", pdu_name="CallDetailRecord", field_defs=FIELD_DEFS,
        binary_column="content", passthrough_columns=["path", "modificationTime"],
    )
    with patch("flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
        batch = pd.DataFrame(
            [{"path": "f1.bin", "modificationTime": "2026-01-01", "content": b"ABC"}],
            columns=["path", "modificationTime", "content"],
        )
        result = list(decoder(iter([batch])))[0]

    assert list(result.columns) == ["path", "modificationTime", "imsi", "callDurationSeconds", "_asn1_decode_error"]
    assert "content" not in result.columns


class _FakeCompiledWithChoice:
    """Simulates asn1tools' real decode shape for CHOICE: a bare ``(member_name, value)``
    tuple (confirmed live).

    ``adversarial`` is the case that proves the old shape-guessing normalization is genuinely
    gone: a CHOICE arm whose *value* is itself a ``(bytes, int)`` 2-tuple. Under the previous
    implementation -- "any (bytes|bytearray, int) 2-tuple is a BIT STRING" -- that value would
    be silently rewritten into ``{"bytes": ..., "bit_length": ...}`` even though the schema
    says OCTET STRING. Only a schema-driven walk gets it right."""

    def decode(self, pdu_name, raw_bytes):
        return {
            "payload": ("text", "hello"),
            "adversarial": ("raw", (b"\xff", 8)),
        }


_CHOICE_NODE = {
    "type": "CHOICE",
    "name": "payload",
    "members": [
        {"type": "UTF8String", "name": "text"},
        {"type": "INTEGER", "name": "number"},
        None,  # ASN.1 extension marker "..." -- asn1tools emits a bare None here
    ],
}

_ADVERSARIAL_CHOICE_NODE = {
    "type": "CHOICE",
    "name": "adversarial",
    "members": [
        {"type": "OCTET STRING", "name": "raw"},
        {"type": "INTEGER", "name": "number"},
    ],
}


def test_choice_tuple_is_normalized_to_discriminator_plus_nullable_arms():
    """A decoded CHOICE becomes {discriminator: selected arm name, selected arm: value, every
    other arm: None} -- the shape derive_asn1_field_defs declared for it."""
    decoder = make_partition_decoder(
        module_files=["/x.asn"],
        codec="ber",
        pdu_name="Rec",
        field_defs=[{"name": "payload", "spark_type": "unused", "asn1_node": _CHOICE_NODE}],
        binary_column="content",
        passthrough_columns=["path"],
    )
    with patch(
        "flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files",
        return_value=_FakeCompiledWithChoice(),
    ):
        result = list(decoder(_batches([{"path": "f1.bin", "content": b"ABC"}])))[0]

    assert result.iloc[0]["payload"] == {
        CHOICE_DISCRIMINATOR_FIELD: "text",
        "text": "hello",
        "number": None,
    }


def test_choice_arm_holding_a_bytes_int_tuple_is_not_mistaken_for_a_bit_string():
    """The collision case, and the regression guard that proves normalization is schema-aware
    rather than shape-guessing. The arm's declared type is OCTET STRING, so its ``(b'\xff', 8)``
    value must pass through verbatim -- NOT be rewritten as a BIT STRING struct."""
    decoder = make_partition_decoder(
        module_files=["/x.asn"],
        codec="ber",
        pdu_name="Rec",
        field_defs=[
            {"name": "adversarial", "spark_type": "unused", "asn1_node": _ADVERSARIAL_CHOICE_NODE}
        ],
        binary_column="content",
        passthrough_columns=["path"],
    )
    with patch(
        "flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files",
        return_value=_FakeCompiledWithChoice(),
    ):
        result = list(decoder(_batches([{"path": "f1.bin", "content": b"ABC"}])))[0]

    value = result.iloc[0]["adversarial"]
    assert value[CHOICE_DISCRIMINATOR_FIELD] == "raw"
    assert value["raw"] == (b"\xff", 8), "an OCTET STRING arm must not be reshaped as a BIT STRING"
    assert value["number"] is None


class _FakeCompiledRootChoice:
    """A ROOT CHOICE decodes to a bare ``(arm_name, value)`` tuple, not a dict -- so the
    decoder cannot ``.get(column_name)`` its way to the columns and must spread the tuple."""

    def decode(self, pdu_name, raw_bytes):
        return ("notification", {"sender": "BT", "seq": 4})


def test_root_choice_pdu_spreads_the_selected_arm_across_arm_columns():
    """Mirrors TAP's DataInterChange / 3GPP's CallEventRecord: the selected arm column is
    populated, every other arm column is NULL, and the discriminator names which arm it was."""
    field_defs = [
        {"name": CHOICE_DISCRIMINATOR_FIELD, "spark_type": "string", "asn1_node": None},
        {"name": "transferBatch", "spark_type": "unused", "asn1_node": {"type": "TransferBatch"}},
        {
            "name": "notification",
            "spark_type": "unused",
            "asn1_node": {
                "type": "SEQUENCE",
                "members": [{"type": "UTF8String", "name": "sender"}, {"type": "INTEGER", "name": "seq"}],
            },
        },
    ]
    decoder = make_partition_decoder(
        module_files=["/x.asn"],
        codec="ber",
        pdu_name="DataInterChange",
        field_defs=field_defs,
        binary_column="content",
        passthrough_columns=["path"],
        root_is_choice=True,
    )
    with patch(
        "flowx.lakeflow_framework.asn1.decoder.asn1tools.compile_files",
        return_value=_FakeCompiledRootChoice(),
    ):
        result = list(decoder(_batches([{"path": "f1.bin", "content": b"ABC"}])))[0]

    row = result.iloc[0]
    assert row[CHOICE_DISCRIMINATOR_FIELD] == "notification"
    assert row["notification"] == {"sender": "BT", "seq": 4}
    assert row["transferBatch"] is None, "an unselected CHOICE arm must be NULL"
    assert row["_asn1_decode_error"] is None
