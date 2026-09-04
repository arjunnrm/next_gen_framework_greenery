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

import pathlib

import pytest

from flowx.lakeflow_framework.asn1.decoder import (
    BIT_STRING_SPARK_TYPE,
    CHOICE_DISCRIMINATOR_FIELD,
    derive_asn1_field_defs,
)
from flowx.lakeflow_framework.exceptions import Asn1DecodeError
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


def test_pdu_that_is_neither_sequence_nor_choice_raises_asn1_decode_error(tmp_path):
    """SEQUENCE and CHOICE are both legal root PDUs; an ENUMERATED is neither and must still
    be rejected rather than producing a nonsense single-column projection."""
    path = _write_module(tmp_path, _USER_DIRECTORY_MODULE)
    with pytest.raises(Asn1DecodeError, match="must be a top-level ASN.1 SEQUENCE or CHOICE"):
        derive_asn1_field_defs(path, "UserRole")


def test_missing_file_raises_asn1_decode_error(tmp_path):
    with pytest.raises(Asn1DecodeError):
        derive_asn1_field_defs(str(tmp_path / "nonexistent.asn"), "CallDetailRecord")


def test_malformed_asn1_syntax_raises_asn1_decode_error(tmp_path):
    path = _write_module(tmp_path, "NotValidAsn1 ::: this is not { real grammar")
    with pytest.raises(Asn1DecodeError):
        derive_asn1_field_defs(path, "CallDetailRecord")


_CHOICE_MODULE = """
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


def test_choice_member_maps_to_a_struct_of_all_arms_all_nullable(tmp_path):
    """Formerly ``test_choice_type_is_explicitly_rejected_not_silently_mismapped``: CHOICE used
    to raise Asn1DecodeError. Spark has no union type, so a CHOICE now maps to a struct of ALL
    its arms with every field nullable -- exactly one arm is populated per decoded value and
    the rest are NULL -- plus a discriminator naming the selected arm."""
    path = _write_module(tmp_path, _CHOICE_MODULE)
    field_defs = derive_asn1_field_defs(path, "Wrapper")
    by_name = {f["name"]: f["spark_type"] for f in field_defs}

    payload_type = by_name["payload"]
    assert isinstance(payload_type, StructType)
    assert [f.name for f in payload_type.fields] == [CHOICE_DISCRIMINATOR_FIELD, "text", "number"]
    assert all(f.nullable for f in payload_type.fields), "every CHOICE arm must be nullable"
    by_field = {f.name: f.dataType for f in payload_type.fields}
    assert by_field[CHOICE_DISCRIMINATOR_FIELD] == StringType()
    assert by_field["text"] == StringType()
    assert by_field["number"] == LongType()


def test_choice_root_pdu_projects_one_column_per_arm_plus_discriminator(tmp_path):
    """The whole point of the feature: a real telecom module's natural root PDU is a CHOICE
    (TAP's DataInterChange, 3GPP's CallEventRecord). Its arms become the output columns."""
    path = _write_module(tmp_path, _CHOICE_MODULE)
    field_defs = derive_asn1_field_defs(path, "Payload")

    assert [f["name"] for f in field_defs] == [CHOICE_DISCRIMINATOR_FIELD, "text", "number"]
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name[CHOICE_DISCRIMINATOR_FIELD] == StringType()
    assert by_name["text"] == StringType()
    assert by_name["number"] == LongType()


def test_sequence_root_pdu_emits_no_discriminator_column(tmp_path):
    """Guard against the CHOICE work leaking a synthetic column into the SEQUENCE path, whose
    behaviour must be byte-for-byte unchanged."""
    path = _write_module(tmp_path, _TELECOM_CDR_MODULE)
    names = [f["name"] for f in derive_asn1_field_defs(path, "CallDetailRecord")]
    assert CHOICE_DISCRIMINATOR_FIELD not in names
    assert names == ["imsi", "msisdn", "regionCode", "callDurationSeconds", "cellId"]


def test_choice_extension_marker_member_is_skipped_not_crashed_on(tmp_path):
    """asn1tools represents an ASN.1 extension marker (``...``) as a bare ``None`` entry in
    ``members``. Iterating it unguarded raised a raw TypeError, not the Asn1DecodeError this
    module's contract promises. Real case: TAP.311 has 8 CHOICE types with one."""
    module = """
    ExtChoiceModule DEFINITIONS ::= BEGIN
    Payload ::= CHOICE { a INTEGER, b UTF8String, ... }
    END
    """
    path = _write_module(tmp_path, module)
    field_defs = derive_asn1_field_defs(path, "Payload")
    assert [f["name"] for f in field_defs] == [CHOICE_DISCRIMINATOR_FIELD, "a", "b"]


def test_sequence_extension_marker_member_is_skipped_not_crashed_on(tmp_path):
    """The same bug in the *pre-existing* SEQUENCE path, independent of CHOICE. TAP.311 has 79
    SEQUENCE/SET types carrying an extension marker, which is why
    ``derive_asn1_field_defs(TAP.311, 'Notification')`` -- an already-legal top-level SEQUENCE
    PDU -- crashed with ``TypeError: 'NoneType' object is not subscriptable`` before this fix."""
    module = """
    ExtSeqModule DEFINITIONS ::= BEGIN
    Rec ::= SEQUENCE { a INTEGER, b UTF8String, ... }
    Inner ::= SET { x INTEGER, ... }
    Outer ::= SEQUENCE { rec Rec, inner Inner, ... }
    END
    """
    path = _write_module(tmp_path, module)
    assert [f["name"] for f in derive_asn1_field_defs(path, "Rec")] == ["a", "b"]

    outer = {f["name"]: f["spark_type"] for f in derive_asn1_field_defs(path, "Outer")}
    assert [f.name for f in outer["rec"].fields] == ["a", "b"]
    assert [f.name for f in outer["inner"].fields] == ["x"]


def test_nested_choice_inside_a_sequence_resolves_at_depth(tmp_path):
    """Mirrors TAP's TransferBatch -> CallEventDetail: a named CHOICE reached through a
    SEQUENCE member, one or more levels down, not at the root."""
    module = """
    DeepModule DEFINITIONS ::= BEGIN
    Detail ::= CHOICE { call INTEGER, sms UTF8String }
    Batch ::= SEQUENCE { detail Detail }
    Interchange ::= SEQUENCE { batch Batch }
    END
    """
    path = _write_module(tmp_path, module)
    by_name = {f["name"]: f["spark_type"] for f in derive_asn1_field_defs(path, "Interchange")}
    detail = by_name["batch"].fields[0].dataType
    assert isinstance(detail, StructType)
    assert [f.name for f in detail.fields] == [CHOICE_DISCRIMINATOR_FIELD, "call", "sms"]


def test_tagged_choice_arms_resolve_by_member_name_ignoring_the_tag(tmp_path):
    """GGSN/PSGW tag every CallEventRecord arm (``[20]``...), EMSC mixes tagged and untagged.
    Tags affect wire encoding only -- asn1tools decodes to the arm NAME regardless -- so the
    derived struct must key on ``name`` and ignore ``tag`` entirely."""
    module = """
    TaggedModule DEFINITIONS ::= BEGIN
    Rec ::= CHOICE {
        untagged   INTEGER,
        implicitly [1] IMPLICIT UTF8String,
        explicitly [20] IA5String
    }
    END
    """
    path = _write_module(tmp_path, module)
    field_defs = derive_asn1_field_defs(path, "Rec")
    assert [f["name"] for f in field_defs] == [
        CHOICE_DISCRIMINATOR_FIELD,
        "untagged",
        "implicitly",
        "explicitly",
    ]
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name["untagged"] == LongType()
    assert by_name["implicitly"] == StringType()
    assert by_name["explicitly"] == StringType()


def test_choice_arm_that_is_an_octet_string_maps_to_binary_not_a_bit_string_struct(tmp_path):
    """The collision case. A CHOICE selecting an OCTET STRING decodes to ``('raw', b'..')`` and
    a BIT STRING decodes to ``(b'..', 4)`` -- both bare 2-tuples. The schema must distinguish
    them, which is only possible by node type, never by value shape."""
    module = """
    CollisionModule DEFINITIONS ::= BEGIN
    Payload ::= CHOICE { raw OCTET STRING, bits BIT STRING }
    END
    """
    path = _write_module(tmp_path, module)
    by_name = {f["name"]: f["spark_type"] for f in derive_asn1_field_defs(path, "Payload")}
    assert by_name["raw"] == BinaryType()
    assert by_name["bits"] == BIT_STRING_SPARK_TYPE


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


# ---------------------------------------------------------------------------------------
# Real telecom modules. These are the fixtures the feature exists for: every one of them has
# a CHOICE as its natural root PDU, and TAP.311 is additionally the only one carrying ASN.1
# extension markers (79 SEQUENCE/SET + 8 CHOICE types), which crashed the SEQUENCE path with a
# raw TypeError long before CHOICE support was on the table.
# ---------------------------------------------------------------------------------------

_BT_TESTING_DIR = pathlib.Path(__file__).resolve().parents[2] / "flowx_testing" / "BT_Testing"


def _bt_schema(filename):
    path = _BT_TESTING_DIR / filename
    if not path.exists():  # pragma: no cover - fixture absent in a trimmed checkout
        pytest.skip(f"BT_Testing fixture not present: {path}")
    return str(path)


@pytest.mark.parametrize(
    "filename, pdu_name, expected_arms",
    [
        ("TAP.311.asn1", "DataInterChange", ["transferBatch", "notification"]),
        ("TAP.310.asn1", "DataInterChange", ["transferBatch", "notification"]),
        ("PSGW.asn1", "CallEventRecord", ["sgsnPDPRecord", "ggsnPDPRecord", "sgsnMMRecord"]),
        ("GGSN.asn1", "CallEventRecord", ["sgsnPDPRecord", "ggsnPDPRecord", "sgsnMMRecord"]),
        ("EMSC.asn1", "CallDataRecord", ["uMTSGSMPLMNCallDataRecord", "compositeCallDataRecord"]),
    ],
)
def test_real_telecom_module_root_choice_pdu_derives(filename, pdu_name, expected_arms):
    """Every one of the five real BT_Testing modules has a CHOICE root PDU and was therefore
    completely un-onboardable at its true top-level type before this change."""
    field_defs = derive_asn1_field_defs(_bt_schema(filename), pdu_name)
    names = [f["name"] for f in field_defs]

    assert names[0] == CHOICE_DISCRIMINATOR_FIELD
    for arm in expected_arms:
        assert arm in names
    by_name = {f["name"]: f["spark_type"] for f in field_defs}
    assert by_name[CHOICE_DISCRIMINATOR_FIELD] == StringType()
    for arm in expected_arms:
        # A CHOICE arm resolves to whatever its own ASN.1 type is -- usually a SEQUENCE
        # (struct), but EMSC's compositeCallDataRecord is a SEQUENCE OF (array). Either way it
        # must be a real composite type, never left unresolved.
        assert isinstance(by_name[arm], (StructType, ArrayType)), f"{arm} did not resolve"


def test_tap311_notification_sequence_pdu_no_longer_raises_typeerror():
    """The pre-existing bug, independent of CHOICE: ``Notification`` is already a legal
    top-level SEQUENCE, but TAP.311's extension markers made ``derive_asn1_field_defs`` raise
    ``TypeError: 'NoneType' object is not subscriptable`` -- not the Asn1DecodeError this
    module's contract promises. Every other BT_Testing module is marker-free, which is exactly
    why this went unnoticed."""
    field_defs = derive_asn1_field_defs(_bt_schema("TAP.311.asn1"), "Notification")
    names = [f["name"] for f in field_defs]
    assert "sender" in names
    assert "fileSequenceNumber" in names
    assert None not in names


def test_deeply_nested_real_schema_resolves_without_hitting_the_recursion_limit():
    """TAP.311 is the deepest of the five (nesting depth 27 from DataInterChange). Well inside
    CPython's default 1000-frame limit, but asserted so a future refactor that multiplies
    frames per level is caught here rather than at pipeline runtime."""
    field_defs = derive_asn1_field_defs(_bt_schema("TAP.311.asn1"), "DataInterChange")
    transfer_batch = {f["name"]: f["spark_type"] for f in field_defs}["transferBatch"]
    assert isinstance(transfer_batch, StructType)
    assert len(transfer_batch.fields) > 0
