"""Unit tests for asn1/decoder.py::derive_asn1_field_defs -- pure Python + asn1tools, no
Spark/live Databricks needed. Real ``.asn`` module text written to real temp files (not
mocked) since ``asn1tools.parse_files`` needs to genuinely parse ASN.1 grammar.

Regression coverage for the v2 redesign: the Spark output schema used to be a second,
hand-authored ``fields: [{name, spark_type}]`` JSON list, duplicating -- and routinely
drifting from -- what the ``.asn`` module file already says authoritatively (see git history
of ``sample_data/asn1_schema/telecom_cdr.json``, now deleted). This module derives that
field list directly from the real ``.asn`` file via ``asn1tools.parse_files`` introspection,
so there is exactly one source of truth. Every type-mapping case here was cross-checked
against a live ``asn1tools`` encode/decode round-trip before being encoded as a test (see
``derive_asn1_field_defs``'s docstring for the confirmed decode return types this relies on:
``ENUMERATED`` -> the symbolic member name as ``str``, ``SEQUENCE`` -> ``dict``,
``SEQUENCE OF`` -> ``list``).
"""

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.asn1.decoder import BIT_STRING_SPARK_TYPE, derive_asn1_field_defs
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import Asn1DecodeError
from pyspark.sql.types import ArrayType, BinaryType, BooleanType, LongType, StringType, StructType, TimestampType


def _write_module(tmp_path, text, filename="module.asn"):
    path = tmp_path / filename
    path.write_text(text, encoding="utf-8")
    return str(path)


_TELECOM_CDR_MODULE = """
TelecomCDR DEFINITIONS ::= BEGIN

CallDetailRecord ::= SEQUENCE {
    imsi                    IA5String,
    msisdn                  IA5String,
    regionCode              IA5String,
    callDurationSeconds     INTEGER,
    cellId                  IA5String
}

END
"""

_USER_DIRECTORY_MODULE = """
UserDirectoryModule DEFINITIONS AUTOMATIC TAGS ::= BEGIN

UserRole ::= ENUMERATED {
    guest(0),
    standardUser(1),
    administrator(2)
}

Address ::= SEQUENCE {
    street          UTF8String (SIZE (1..100)),
    city            UTF8String (SIZE (1..50)),
    postalCode      NumericString (SIZE (5..10)),
    countryCode     PrintableString (SIZE (2))
}

UserProfile ::= SEQUENCE {
    id              INTEGER (1..MAX),
    username        UTF8String (SIZE (3..30)),
    email           IA5String OPTIONAL,
    role            UserRole DEFAULT standardUser,
    isActive        BOOLEAN,
    address         Address OPTIONAL,
    tags            SEQUENCE OF UTF8String,
    metadata        OCTET STRING OPTIONAL
}

END
"""


def test_flat_record_fields_map_to_expected_spark_types(tmp_path):
    path = _write_module(tmp_path, _TELECOM_CDR_MODULE)
    field_defs = derive_asn1_field_defs(path, "CallDetailRecord")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}

    assert by_name["imsi"] == StringType()
    assert by_name["msisdn"] == StringType()
    assert by_name["callDurationSeconds"] == LongType()
    assert [f["name"] for f in field_defs] == ["imsi", "msisdn", "regionCode", "callDurationSeconds", "cellId"]


def test_enumerated_field_maps_to_string(tmp_path):
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    field_defs = derive_asn1_field_defs(path, "UserProfile")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["role"] == StringType()


def test_nested_sequence_maps_to_struct_with_its_own_members(tmp_path):
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    field_defs = derive_asn1_field_defs(path, "UserProfile")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}

    address_type = by_name["address"]
    assert isinstance(address_type, StructType)
    assert [f.name for f in address_type.fields] == ["street", "city", "postalCode", "countryCode"]
    assert all(f.dataType == StringType() for f in address_type.fields)


def test_sequence_of_maps_to_array(tmp_path):
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    field_defs = derive_asn1_field_defs(path, "UserProfile")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["tags"] == ArrayType(StringType(), True)


def test_octet_string_maps_to_binary(tmp_path):
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    field_defs = derive_asn1_field_defs(path, "UserProfile")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["metadata"] == BinaryType()


def test_integer_and_boolean_map_correctly(tmp_path):
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    field_defs = derive_asn1_field_defs(path, "UserProfile")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["id"] == LongType()
    assert by_name["isActive"] == BooleanType()


def test_optional_and_default_fields_are_still_mapped_by_type_only(tmp_path):
    """OPTIONAL/DEFAULT affect whether a value is present at decode time, not its Spark
    type -- every output column is nullable regardless (see decode_asn1_binary_stream's
    StructField(..., True))."""
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    field_defs = derive_asn1_field_defs(path, "UserProfile")
    names = [f["name"] for f in field_defs]
    assert "email" in names
    assert "role" in names


_EXTENDED_TYPES_MODULE = """
ExtendedModule DEFINITIONS ::= BEGIN

Rec ::= SEQUENCE {
    ts       GeneralizedTime,
    ut       UTCTime,
    flags    BIT STRING,
    oid      OBJECT IDENTIFIER,
    n        NULL,
    setField SET { a INTEGER, b UTF8String },
    setOf    SET OF INTEGER
}

END
"""


def test_generalized_time_and_utc_time_map_to_timestamp(tmp_path):
    path = _write_module(tmp_path, _EXTENDED_TYPES_MODULE)
    field_defs = derive_asn1_field_defs(path, "Rec")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["ts"] == TimestampType()
    assert by_name["ut"] == TimestampType()


def test_bit_string_maps_to_a_bytes_and_bit_length_struct(tmp_path):
    path = _write_module(tmp_path, _EXTENDED_TYPES_MODULE)
    field_defs = derive_asn1_field_defs(path, "Rec")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["flags"] == BIT_STRING_SPARK_TYPE


def test_object_identifier_and_null_map_to_string(tmp_path):
    path = _write_module(tmp_path, _EXTENDED_TYPES_MODULE)
    field_defs = derive_asn1_field_defs(path, "Rec")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["oid"] == StringType()
    assert by_name["n"] == StringType()


def test_set_maps_to_struct_like_sequence(tmp_path):
    path = _write_module(tmp_path, _EXTENDED_TYPES_MODULE)
    field_defs = derive_asn1_field_defs(path, "Rec")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    set_type = by_name["setField"]
    assert isinstance(set_type, StructType)
    assert [f.name for f in set_type.fields] == ["a", "b"]


def test_set_of_maps_to_array_like_sequence_of(tmp_path):
    path = _write_module(tmp_path, _EXTENDED_TYPES_MODULE)
    field_defs = derive_asn1_field_defs(path, "Rec")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["setOf"] == ArrayType(LongType(), True)


def test_unknown_pdu_name_raises_asn1_decode_error(tmp_path):
    path = _write_module(tmp_path, _TELECOM_CDR_MODULE)
    with pytest.raises(Asn1DecodeError, match="not defined"):
        derive_asn1_field_defs(path, "NoSuchRecord")


def test_pdu_that_is_not_a_sequence_raises_asn1_decode_error(tmp_path):
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    with pytest.raises(Asn1DecodeError, match="must be a top-level ASN.1 SEQUENCE"):
        derive_asn1_field_defs(path, "UserRole")


def test_missing_file_raises_asn1_decode_error(tmp_path):
    with pytest.raises(Asn1DecodeError):
        derive_asn1_field_defs(str(tmp_path / "nonexistent.asn"), "CallDetailRecord")


def test_malformed_asn1_syntax_raises_asn1_decode_error(tmp_path):
    path = _write_module(tmp_path, "NotValidAsn1 ::: this is not { real grammar")
    with pytest.raises(Asn1DecodeError):
        derive_asn1_field_defs(path, "CallDetailRecord")


def test_choice_type_is_explicitly_rejected_not_silently_mismapped(tmp_path):
    module = """
    ChoiceModule DEFINITIONS ::= BEGIN
    Payload ::= CHOICE {
        text UTF8String,
        number INTEGER
    }
    Wrapper ::= SEQUENCE {
        payload Payload
    }
    END
    """
    path = _write_module(tmp_path, module)
    with pytest.raises(Asn1DecodeError, match="CHOICE"):
        derive_asn1_field_defs(path, "Wrapper")


def test_reference_to_an_undefined_type_raises_asn1_decode_error(tmp_path):
    module = """
    BrokenModule DEFINITIONS ::= BEGIN
    Wrapper ::= SEQUENCE {
        thing UndefinedType
    }
    END
    """
    path = _write_module(tmp_path, module)
    with pytest.raises(Asn1DecodeError):
        derive_asn1_field_defs(path, "Wrapper")
