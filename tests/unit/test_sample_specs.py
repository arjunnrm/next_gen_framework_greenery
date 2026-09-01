"""Offline validation of the sample reference suite's onboarding specs
(``resources/sample_jobs/onboarding/*.json`` -- the 6 ``resources/sample_jobs/*`` sample jobs).

Every sample spec must (a) parse as JSON, (b) pass the REAL onboarding validator
(``onboarding/spec_validator.py::validate_spec``) with zero errors after the same
``{{catalog}}`` substitution the onboarding driver performs, and (c) honor the suite's own
isolation contract: at least two meaningful ``dq_config`` rules on every flow, every
``target_schema``/``publish_schema`` equal to ``metaflow_sample``, and every ``/Volumes/`` path
scoped under ``/Volumes/{{catalog}}/metaflow_sample/``.

Pure Python, no Spark. ``validate_spec`` only reaches for a session on a non-empty
``transformation_sql``/``transform_sql``; with ``spark=None`` that surfaces as an
"unexpected error ... NoneType object has no attribute 'sql'" entry which is a property of the
test harness, not of the spec, and is filtered out by ``_real_errors`` below -- the same idiom
as ``tests/unit/test_recon_backward_compatibility.py``.

``sink_config.staged_file_format`` (Sample 04) is NEW in v1.6.0 and lands in the
validator/schema through a PARALLEL framework change. Today's validator ignores unknown
``sink_config`` keys, so the spec validates cleanly already; if an interim validator revision
were to reject the key as unknown before the parallel change lands, ``_transition_errors``
tolerates exactly that one finding (scoped to sample_04 only) rather than failing this suite
on a known in-flight attribute. Everything else must pass now.
"""

import json
import pathlib
import re

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.onboarding.spec_validator import validate_spec

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SAMPLES_DIR = _REPO_ROOT / "resources" / "sample_jobs" / "onboarding"

# The {{catalog}} placeholder every spec carries; the onboarding driver substitutes the deploy
# target's catalog before validation, so a validator replay must too.
_CATALOG = "metaflow"
_SAMPLE_SCHEMA = "metaflow_sample"

# Signature of the no-Spark-session artefact described in the module docstring. Not a finding.
_NO_SESSION_MARKERS = ("NoneType", "object has no attribute")

_EXPECTED_SPEC_NAMES = [
    "sample_01_multi_scd.json",
    "sample_02_zip_ingestion.json",
    "sample_03_multi_table_recon.json",
    "sample_04_export_encrypt_zip.json",
    "sample_05_encrypted_ingestion.json",
    "sample_06_asn1_tap3_ingestion.json",
]

_VOLUME_PATH = re.compile(r"/Volumes/[^\s\"']*")
_REQUIRED_VOLUME_PREFIX = "/Volumes/{{catalog}}/metaflow_sample/"


def _spec_paths():
    return sorted(_SAMPLES_DIR.glob("*.json"))


def _load_spec(path):
    return json.loads(path.read_text(encoding="utf-8").replace("{{catalog}}", _CATALOG))


def _real_errors(errors):
    """Drop the ``spark=None`` harness artefact, keep everything genuinely about the spec."""
    return [
        e
        for e in errors
        if not (all(marker in e for marker in _NO_SESSION_MARKERS) and "unexpected error" in e.lower())
    ]


def _transition_errors(spec_path, errors):
    """Split off findings tolerated ONLY during the parallel v1.6.0 rollout: an unknown-key
    rejection of ``sink_config.staged_file_format`` on sample_04. Scoped by filename so this
    tolerance can never mask a real error in any other spec (or any other error in sample_04)."""
    if spec_path.name != "sample_04_export_encrypt_zip.json":
        return [], errors
    tolerated = [e for e in errors if "staged_file_format" in e]
    return tolerated, [e for e in errors if "staged_file_format" not in e]


def _all_flows(spec):
    """Every flow object in the spec, labeled -- ingestion, transformation, and reconciliation."""
    labeled = []
    for array_name in ("ingestion_flows", "transformation_flows", "reconciliation_flows"):
        for flow in spec.get(array_name) or []:
            identifier = flow.get("dataflow_id") or flow.get("flow_step_id") or flow.get("reconciliation_id")
            labeled.append((f"{array_name}[{identifier}]", flow))
    return labeled


def test_all_sample_specs_exist():
    """A silent zero-spec discovery would turn every parametrized test below into a vacuous pass."""
    found = sorted(path.name for path in _spec_paths())
    assert found == _EXPECTED_SPEC_NAMES, (
        f"resources/sample_jobs/onboarding/ should hold exactly the "
        f"{len(_EXPECTED_SPEC_NAMES)} sample suite specs; found {found}."
    )


@pytest.mark.parametrize("spec_path", _spec_paths(), ids=lambda p: p.name)
def test_sample_spec_validates_with_zero_errors(spec_path):
    """THE gate: 02_onboarding_engine.py raises on any non-empty errors list, so a single entry
    here is a sample job that cannot onboard at all."""
    errors = _real_errors(validate_spec(None, _load_spec(spec_path))[-1])
    tolerated, errors = _transition_errors(spec_path, errors)
    if tolerated:
        pytest.xfail(
            "sink_config.staged_file_format rejected by an interim validator revision -- the "
            f"attribute lands with the parallel v1.6.0 framework change: {tolerated}"
        )
    assert errors == [], f"{spec_path.name} fails onboarding validation:\n  " + "\n  ".join(errors)


@pytest.mark.parametrize("spec_path", _spec_paths(), ids=lambda p: p.name)
def test_every_flow_declares_at_least_two_dq_rules(spec_path):
    """The suite's own bar: >= 2 meaningful dq_config rules on EVERY flow (data-quality
    expectations + the metrics they emit are a stated requirement of every sample job)."""
    spec = _load_spec(spec_path)
    flows = _all_flows(spec)
    assert flows, f"{spec_path.name} declares no flows at all"
    for label, flow in flows:
        rules = (flow.get("dq_config") or {}).get("rules")
        assert isinstance(rules, list) and len(rules) >= 2, (
            f"{spec_path.name}: {label} must declare a dq_config with at least 2 rules, got {rules!r}"
        )
        for rule in rules:
            assert rule.get("rule_id") and rule.get("expression") and rule.get("action"), (
                f"{spec_path.name}: {label} carries an incomplete dq rule: {rule!r}"
            )


@pytest.mark.parametrize("spec_path", _spec_paths(), ids=lambda p: p.name)
def test_every_target_schema_is_the_isolated_sample_schema(spec_path):
    """Isolation contract: every produced dataset -- including a pipeline-mode reconciliation
    flow's published recon__* datasets -- lives in the single metaflow_sample schema."""
    spec = _load_spec(spec_path)
    for array_name in ("ingestion_flows", "transformation_flows"):
        for flow in spec.get(array_name) or []:
            identifier = flow.get("dataflow_id") or flow.get("flow_step_id")
            assert flow.get("target_schema") == _SAMPLE_SCHEMA, (
                f"{spec_path.name}: {array_name}[{identifier}].target_schema must be "
                f"'{_SAMPLE_SCHEMA}', got {flow.get('target_schema')!r}"
            )
    for flow in spec.get("reconciliation_flows") or []:
        publish_schema = flow.get("publish_schema")
        if publish_schema is not None:
            assert publish_schema == _SAMPLE_SCHEMA, (
                f"{spec_path.name}: reconciliation_flows[{flow.get('reconciliation_id')}].publish_schema "
                f"must be '{_SAMPLE_SCHEMA}', got {publish_schema!r}"
            )


@pytest.mark.parametrize("spec_path", _spec_paths(), ids=lambda p: p.name)
def test_every_volume_path_is_scoped_to_the_sample_schema(spec_path):
    """Isolation contract: every /Volumes/ path (landing, _schemas, extracted, exports,
    observability) stays under /Volumes/{{catalog}}/metaflow_sample/. Checked against the RAW
    spec text so the {{catalog}} templating itself is asserted too."""
    raw_text = spec_path.read_text(encoding="utf-8")
    volume_paths = _VOLUME_PATH.findall(raw_text)
    assert volume_paths, f"{spec_path.name} declares no /Volumes/ paths at all -- unexpected for this suite"
    offenders = [p for p in volume_paths if not p.startswith(_REQUIRED_VOLUME_PREFIX)]
    assert offenders == [], (
        f"{spec_path.name}: every /Volumes/ path must start with '{_REQUIRED_VOLUME_PREFIX}', "
        f"found {offenders}"
    )


@pytest.mark.parametrize("spec_path", _spec_paths(), ids=lambda p: p.name)
def test_every_ingestion_flow_captures_technical_metadata(spec_path):
    """The suite's data-metrics requirement: capture_technical_metadata on every ingestion
    flow's source_config (transformation flows host it on target_config where needed)."""
    spec = _load_spec(spec_path)
    for flow in spec.get("ingestion_flows") or []:
        assert (flow.get("source_config") or {}).get("capture_technical_metadata") is True, (
            f"{spec_path.name}: ingestion_flows[{flow.get('dataflow_id')}].source_config."
            "capture_technical_metadata must be true"
        )
