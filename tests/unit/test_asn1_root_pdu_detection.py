"""Unit tests for ASN.1 root-PDU auto-detection -- ``asn1/decoder.py::detect_root_pdu_name``
and ``resolve_pdu_name``. Pure Python + asn1tools, no Spark or live Databricks needed.

``asn1_pdu_name`` used to be a required ``source_config`` field: a spec author onboarding a
real telecom module had to already know that TAP's root PDU is ``DataInterChange`` and 3GPP's
is ``CallEventRecord``, and a wrong guess did not fail loudly -- naming a *component* type
that happens to parse produces a full table of garbage columns with every row reporting
success. It is now optional, and inferred from the module's own type-dependency structure.

**The detection rule under test**: the root PDU is the one top-level ``SEQUENCE``/``CHOICE``
that no *other* type in the module references. An ASN.1 module is a directed graph of type
definitions and the PDU sent on the wire is its entry point, so nothing depends on it, while
every component type exists because something references it.

The central claim -- that this rule resolves real production telecom modules, not just
hand-written fixtures -- is asserted directly against the five genuine ``.asn`` files in
``flowx_testing/BT_Testing/`` (EMSC 195 types, GGSN 136, PSGW 138, TAP.310 375, TAP.311
307), each with its independently-known true root. A rule that worked on a toy module and
failed on a 375-type real one would be worthless, so the real modules are the primary
evidence here and the synthetic modules cover only the error branches they cannot exhibit.

Also covered: the ``(None, None)`` data-integrity hazard. ``asn1tools`` does not raise when a
CHOICE matches none of its arms -- it returns a bare ``(None, None)``, which the decoder used
to spread into an all-NULL row with a NULL ``_asn1_decode_error``, i.e. a row reported as a
*successful* decode of an empty record. ``flowx_testing/BT_Testing/tap311_sample.ber`` is a
real malformed payload that triggers exactly this (its outer tag is the high-tag-number form
``7f01``, where TAP.311's ``TransferBatch`` is ``[APPLICATION 1]`` = short-form ``0x61``).
"""

import pathlib

import pandas as pd
import pytest

from flowx.lakeflow_framework.asn1.decoder import (
    CHOICE_DISCRIMINATOR_FIELD,
    derive_asn1_field_defs,
    detect_root_pdu_name,
    make_partition_decoder,
    resolve_pdu_name,
)
from flowx.lakeflow_framework.exceptions import Asn1DecodeError

pytest.importorskip("asn1tools")
import asn1tools  # noqa: E402

_BT_TESTING = pathlib.Path(__file__).resolve().parents[2] / "flowx_testing" / "BT_Testing"

# The five real production telecom modules and their independently-known true root PDUs. Every
# one of these roots is a CHOICE -- which is exactly why a root CHOICE had to be supported
# before detection was worth adding.
_REAL_MODULES = [
    ("EMSC.asn1", "CallDataRecord"),
    ("GGSN.asn1", "CallEventRecord"),
    ("PSGW.asn1", "CallEventRecord"),
    ("TAP.310.asn1", "DataInterChange"),
    ("TAP.311.asn1", "DataInterChange"),
]


def _real_module(filename):
    path = _BT_TESTING / filename
    if not path.exists():
        pytest.skip(f"real ASN.1 module fixture not present: {path}")
    return str(path)


def _write_module(tmp_path, text, filename="module.asn"):
    path = tmp_path / filename
    path.write_text(text, encoding="utf-8")
    return str(path)


# A module with exactly one unreferenced SEQUENCE -- the unambiguous case, plus a component
# type (``Address``) that IS referenced and so must never be offered as the root.
_SINGLE_ROOT_MODULE = """
SingleRootModule DEFINITIONS ::= BEGIN

Address ::= SEQUENCE {
    street      UTF8String,
    city        UTF8String
}

CallDetailRecord ::= SEQUENCE {
    imsi        IA5String,
    billingTo   Address
}

END
"""

# Two SEQUENCEs, neither referenced by anything -- structurally indistinguishable, so
# detection must refuse rather than pick one.
_AMBIGUOUS_MODULE = """
AmbiguousModule DEFINITIONS ::= BEGIN

FirstRecord ::= SEQUENCE {
    alpha       IA5String
}

SecondRecord ::= SEQUENCE {
    beta        INTEGER
}

END
"""

# No SEQUENCE/CHOICE at all -- nothing that could serve as a record type.
_NO_STRUCTURAL_TYPE_MODULE = """
NoStructuralModule DEFINITIONS ::= BEGIN

Meter ::= INTEGER

Label ::= IA5String

END
"""

# Every SEQUENCE is referenced by another type: a mutually-referencing pair leaves no entry
# point at all, which is a distinct failure from "too many entry points".
_NO_UNREFERENCED_MODULE = """
NoRootModule DEFINITIONS ::= BEGIN

Outer ::= SEQUENCE {
    id          INTEGER,
    child       Inner
}

Inner ::= SEQUENCE {
    id          INTEGER,
    parentRef   Outer
}

END
"""


class TestDetectionAgainstRealModules:
    """The load-bearing evidence: the rule resolves all five genuine telecom modules."""

    @pytest.mark.parametrize("filename,expected_root", _REAL_MODULES)
    def test_detects_true_root_pdu(self, filename, expected_root):
        assert detect_root_pdu_name(_real_module(filename)) == expected_root

    @pytest.mark.parametrize("filename,expected_root", _REAL_MODULES)
    def test_detected_root_is_a_choice_and_derives_a_usable_schema(self, filename, expected_root):
        """Detection is only useful if what it returns actually drives schema derivation.

        Asserting the name alone would pass even if the detected type were unusable as a PDU,
        so this derives the real field list with no ``pdu_name`` supplied and checks it matches
        the one derived from the known-correct root explicitly.
        """
        path = _real_module(filename)
        auto = derive_asn1_field_defs(path)
        explicit = derive_asn1_field_defs(path, expected_root)
        assert [f["name"] for f in auto] == [f["name"] for f in explicit]
        # Every one of these five real roots is a CHOICE, so the discriminator leads the list.
        assert auto[0]["name"] == CHOICE_DISCRIMINATOR_FIELD
        assert len(auto) > 1

    @pytest.mark.parametrize("filename,expected_root", _REAL_MODULES)
    def test_detection_is_not_merely_picking_the_only_structural_type(self, filename, expected_root):
        """Guards against a rule that "works" only because the module has one SEQUENCE/CHOICE.

        These modules define 130-375 types; if the detected root were being selected from a
        field of one candidate the test above would prove nothing about the rule itself. The
        floor is deliberately low (EMSC declares only 7 top-level SEQUENCE/CHOICE types -- most
        of its 195 types are primitives and component SEQUENCEs nested inside others) because
        the claim being defended is "the rule discriminated among several candidates", not any
        particular count.
        """
        parsed = asn1tools.parse_files([_real_module(filename)])
        types = next(iter(parsed.values()))["types"]
        structural = [n for n, d in types.items() if d.get("type") in ("SEQUENCE", "CHOICE")]
        assert len(structural) > 1, f"{filename} should have several structural types, got {len(structural)}"
        assert expected_root in structural


class TestExplicitOverride:
    """A supplied ``asn1_pdu_name`` wins unconditionally -- detection is never consulted."""

    def test_explicit_name_overrides_what_detection_would_pick(self, tmp_path):
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        # Detection picks CallDetailRecord; the author names the referenced component instead.
        assert detect_root_pdu_name(path) == "CallDetailRecord"
        assert resolve_pdu_name(path, "Address") == "Address"

        field_names = [f["name"] for f in derive_asn1_field_defs(path, "Address")]
        assert field_names == ["street", "city"]

    def test_explicit_name_on_a_real_module_overrides_detection(self):
        """The override must hold on a real module too, not just a 2-type fixture."""
        path = _real_module("TAP.311.asn1")
        assert detect_root_pdu_name(path) == "DataInterChange"
        assert resolve_pdu_name(path, "TransferBatch") == "TransferBatch"
        assert derive_asn1_field_defs(path, "TransferBatch") != derive_asn1_field_defs(path)

    def test_explicit_name_is_never_second_guessed_on_an_ambiguous_module(self, tmp_path):
        """An ambiguous module is only an error when nothing was specified.

        This is the case that matters most for the override contract: detection *would* raise
        here, so an implementation that ran detection first and validated the user's choice
        against it would break a spec that is perfectly correct.
        """
        path = _write_module(tmp_path, _AMBIGUOUS_MODULE)
        with pytest.raises(Asn1DecodeError):
            detect_root_pdu_name(path)
        assert resolve_pdu_name(path, "SecondRecord") == "SecondRecord"
        assert [f["name"] for f in derive_asn1_field_defs(path, "SecondRecord")] == ["beta"]

    def test_explicit_name_is_whitespace_trimmed(self, tmp_path):
        """A padded but non-blank name is a real name, not a request to auto-detect."""
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        assert resolve_pdu_name(path, "  Address  ") == "Address"


class TestBlankTriggersDetection:
    """Presence of a blank value means "not specified", exactly like an absent key."""

    @pytest.mark.parametrize("blank", [None, "", "   ", "\t", "\n", "  \t\n "])
    def test_blank_values_all_trigger_detection(self, tmp_path, blank):
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        assert resolve_pdu_name(path, blank) == "CallDetailRecord"

    def test_omitted_argument_triggers_detection(self, tmp_path):
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        assert [f["name"] for f in derive_asn1_field_defs(path)] == ["imsi", "billingTo"]

    @pytest.mark.parametrize("blank", [None, "", "   "])
    def test_blank_values_reach_derive_asn1_field_defs(self, tmp_path, blank):
        """The blank must survive the whole call path, not just resolve_pdu_name in isolation."""
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        assert [f["name"] for f in derive_asn1_field_defs(path, blank)] == ["imsi", "billingTo"]

    def test_non_string_pdu_name_is_rejected_not_silently_detected(self, tmp_path):
        """A truthy non-string (e.g. a YAML mis-type) is an authoring error, not "unspecified".

        Falling back to detection here would silently ignore a value the author clearly meant
        to be meaningful.
        """
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        with pytest.raises(Asn1DecodeError, match="must be a string"):
            resolve_pdu_name(path, 42)


class TestUndetectableModulesRaise:
    """Never guess between plausible roots -- a wrong root fails silently, not loudly."""

    def test_ambiguous_module_raises_listing_candidates(self, tmp_path):
        path = _write_module(tmp_path, _AMBIGUOUS_MODULE)
        with pytest.raises(Asn1DecodeError) as excinfo:
            detect_root_pdu_name(path)
        message = str(excinfo.value)
        assert "ambiguous" in message.lower()
        # Both candidates named, so the author can copy the right one into the spec.
        assert "FirstRecord" in message
        assert "SecondRecord" in message
        assert "asn1_pdu_name" in message

    def test_module_with_no_sequence_or_choice_raises(self, tmp_path):
        path = _write_module(tmp_path, _NO_STRUCTURAL_TYPE_MODULE)
        with pytest.raises(Asn1DecodeError) as excinfo:
            detect_root_pdu_name(path)
        message = str(excinfo.value)
        assert "no " in message.lower()
        assert "asn1_pdu_name" in message

    def test_module_with_no_unreferenced_type_raises(self, tmp_path):
        """Distinct failure from ambiguity: every candidate is referenced, so there is no entry
        point at all. The message must say that rather than reporting a bogus candidate list."""
        path = _write_module(tmp_path, _NO_UNREFERENCED_MODULE)
        with pytest.raises(Asn1DecodeError) as excinfo:
            detect_root_pdu_name(path)
        message = str(excinfo.value)
        assert "referenced by another type" in message
        assert "asn1_pdu_name" in message

    def test_ambiguity_surfaces_through_derive_asn1_field_defs(self, tmp_path):
        """The error must not be swallowed on the real call path used by the reader."""
        path = _write_module(tmp_path, _AMBIGUOUS_MODULE)
        with pytest.raises(Asn1DecodeError, match="ambiguous"):
            derive_asn1_field_defs(path)


class TestDetectionLogging:
    """An operator must be able to see which PDU was inferred, and why, in the driver log."""

    def test_detection_logs_the_chosen_pdu_and_reason(self, tmp_path, caplog):
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        with caplog.at_level("INFO", logger="common.asn1.decoder"):
            detect_root_pdu_name(path)
        messages = [record.getMessage() for record in caplog.records]
        assert any("CallDetailRecord" in message for message in messages)
        assert any("no other type" in message for message in messages)

    def test_explicit_override_logs_no_detection_claim(self, tmp_path, caplog):
        """An override must not emit an inference line -- it would misreport what happened."""
        path = _write_module(tmp_path, _SINGLE_ROOT_MODULE)
        with caplog.at_level("INFO", logger="common.asn1.decoder"):
            resolve_pdu_name(path, "Address")
        assert not [r for r in caplog.records if "auto-detected" in r.getMessage()]


class TestUnmatchedChoiceIsNotSilentlyEmpty:
    """``asn1tools`` returns ``(None, None)`` rather than raising when a CHOICE matches no arm.

    Left unguarded this writes a row with every arm NULL, a NULL discriminator, and a NULL
    ``_asn1_decode_error`` -- indistinguishable from a successful decode of an empty record,
    and reported as SUCCESS. Silent empty rows are worse than a failed batch: nothing signals
    that the data is missing.
    """

    def _decode_one(self, schema_path, pdu_name, payload):
        field_defs = derive_asn1_field_defs(schema_path, pdu_name)
        parsed = asn1tools.parse_files([schema_path])
        module_types = next(iter(parsed.values()))["types"]
        decoder = make_partition_decoder(
            [schema_path],
            "ber",
            pdu_name,
            field_defs,
            binary_column="content",
            passthrough_columns=[],
            module_types=module_types,
            root_is_choice=module_types[pdu_name].get("type") == "CHOICE",
        )
        batch = pd.DataFrame({"content": [payload]})
        return pd.concat(list(decoder(iter([batch]))), ignore_index=True)

    def test_tap311_sample_ber_reports_an_error_not_an_empty_row(self):
        """The real malformed payload: 16 bytes whose outer tag ``7f01`` matches no arm."""
        schema_path = _real_module("TAP.311.asn1")
        sample = _BT_TESTING / "tap311_sample.ber"
        if not sample.exists():
            pytest.skip(f"fixture not present: {sample}")
        payload = sample.read_bytes()

        # Precondition: asn1tools really does return (None, None) here rather than raising --
        # if this ever changes upstream, this test's premise needs revisiting rather than the
        # guard being quietly assumed still necessary.
        compiled = asn1tools.compile_files([schema_path], "ber")
        assert compiled.decode("DataInterChange", payload) == (None, None)

        result = self._decode_one(schema_path, "DataInterChange", payload)
        assert len(result) == 1
        error = result["_asn1_decode_error"].iloc[0]
        assert error is not None, "a (None, None) decode must not be reported as success"
        assert "matched none of its arms" in error
        assert result[CHOICE_DISCRIMINATOR_FIELD].iloc[0] is None

    def test_valid_synthetic_payload_still_decodes_cleanly(self):
        """The guard must not turn a genuinely-good record into an error."""
        schema_path = _real_module("TAP.311.asn1")
        sample = _BT_TESTING / "synthetic" / "tap311_synthetic.ber"
        if not sample.exists():
            pytest.skip(f"fixture not present: {sample}")

        result = self._decode_one(schema_path, "DataInterChange", sample.read_bytes())
        assert len(result) == 1
        assert result["_asn1_decode_error"].iloc[0] is None
        assert result[CHOICE_DISCRIMINATOR_FIELD].iloc[0] in ("transferBatch", "notification")

    @pytest.mark.parametrize("filename,expected_root", _REAL_MODULES)
    def test_synthetic_payloads_decode_under_auto_detected_root(self, filename, expected_root):
        """End-to-end: auto-detect the root, then decode a real payload with it successfully.

        This is what ties detection to correctness -- a detected root that produced the right
        column names but could not decode the module's own payloads would still be wrong.
        """
        schema_path = _real_module(filename)
        prefix = filename.split(".asn1")[0].replace(".", "").lower()
        sample = _BT_TESTING / "synthetic" / f"{prefix}_synthetic.ber"
        if not sample.exists():
            pytest.skip(f"fixture not present: {sample}")

        detected = detect_root_pdu_name(schema_path)
        assert detected == expected_root

        result = self._decode_one(schema_path, detected, sample.read_bytes())
        assert len(result) == 1
        assert result["_asn1_decode_error"].iloc[0] is None, result["_asn1_decode_error"].iloc[0]
        assert result[CHOICE_DISCRIMINATOR_FIELD].iloc[0] is not None
