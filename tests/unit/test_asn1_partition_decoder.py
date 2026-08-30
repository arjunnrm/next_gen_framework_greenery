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

from NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder import make_partition_decoder


class _FakeCompiled:
    def decode(self, pdu_name, raw_bytes):
        if raw_bytes == b"BAD":
            raise ValueError("corrupt payload")
        return {"imsi": raw_bytes.decode("utf-8"), "callDurationSeconds": len(raw_bytes)}


def _batches(*row_lists):
    for rows in row_lists:
        yield pd.DataFrame(rows, columns=["path", "content"])


FIELD_DEFS = [{"name": "imsi", "spark_type": "string"}, {"name": "callDurationSeconds", "spark_type": "long"}]


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

    with patch("NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files", side_effect=_counting_compile_files):
        result_batches = list(decoder(batches))

    assert call_count["n"] == 1, "the ASN.1 schema must be compiled exactly once for the whole partition, not once per row/batch"
    total_rows = sum(len(b) for b in result_batches)
    assert total_rows == 6


def test_successfully_decoded_rows_populate_configured_fields():
    decoder = make_partition_decoder(
        module_files=["/x.asn"], codec="ber", pdu_name="CallDetailRecord", field_defs=FIELD_DEFS,
        binary_column="content", passthrough_columns=["path"],
    )
    with patch("NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
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
    with patch("NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
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
    with patch("NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
        result = list(decoder(_batches([{"path": "null.bin", "content": None}])))[0]

    row = result.iloc[0]
    assert row["_asn1_decode_error"] == "null_payload"
    assert row["imsi"] is None


def test_empty_partition_yields_no_rows_without_error():
    decoder = make_partition_decoder(
        module_files=["/x.asn"], codec="ber", pdu_name="CallDetailRecord", field_defs=FIELD_DEFS,
        binary_column="content", passthrough_columns=["path"],
    )
    with patch("NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
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


def test_bit_string_tuple_is_normalized_to_a_bytes_and_bit_length_dict():
    decoder = make_partition_decoder(
        module_files=["/x.asn"],
        codec="ber",
        pdu_name="Rec",
        field_defs=[{"name": "flags", "spark_type": "unused"}],
        binary_column="content",
        passthrough_columns=["path"],
    )
    with patch(
        "NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files",
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
        field_defs=[{"name": "nested", "spark_type": "unused"}, {"name": "listOfBits", "spark_type": "unused"}],
        binary_column="content",
        passthrough_columns=["path"],
    )
    with patch(
        "NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files",
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
    with patch("NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder.asn1tools.compile_files", return_value=_FakeCompiled()):
        batch = pd.DataFrame(
            [{"path": "f1.bin", "modificationTime": "2026-01-01", "content": b"ABC"}],
            columns=["path", "modificationTime", "content"],
        )
        result = list(decoder(iter([batch])))[0]

    assert list(result.columns) == ["path", "modificationTime", "imsi", "callDurationSeconds", "_asn1_decode_error"]
    assert "content" not in result.columns
