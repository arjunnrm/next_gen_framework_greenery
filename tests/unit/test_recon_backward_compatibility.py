"""v1.5.0 in-pipeline reconciliation must be a PURELY ADDITIVE change.

The user's acceptance criteria for the v1.5.0 reconciliation work were stated as "no impact on
existing functionality" and "no change in json spec". Both are contracts about what happens to
things that were onboarded BEFORE the change, and neither is checked anywhere else in the suite:
every other v1.5.0 test (``test_removed_attributes_v150``, ``test_source_plane_plan``,
``test_recon_registration_ast``, ``test_read_once_wiring``) exercises the NEW behaviour under the
NEW attributes. This module exercises the old world under the new code.

Four separate surfaces have to hold still, one test class each:

``TestExistingSpecsStillValidate`` (a)
    A pre-v1.5.0 spec declares no ``execution_mode`` anywhere. Replaying the real shipped specs
    in ``flowx_testing/`` that carry ``reconciliation_flows`` without ``execution_mode``
    through ``validate_spec`` must still produce zero errors -- if a v1.5.0 rule fires on a spec
    that has not opted into pipeline mode, that spec can no longer be onboarded at all.

``TestJobModeIsTheDefaultInTheLoader`` (b)
    ``repository.load_active_group_metadata`` must treat a reconciliation row that has no
    ``execution_mode`` ATTRIBUTE AT ALL as job mode and leave it out of ``reconciliation_rows``.
    This is the already-provisioned-table case: ``01_setup`` only ever runs
    ``CREATE TABLE IF NOT EXISTS``, so an existing ``reconciliation_flow_spec`` really can come
    back from ``.collect()`` with no such field, and ``getattr(r, "execution_mode", None)`` is
    the only reason that does not raise.

``TestUpsertWritesNullNotADefault`` (c)
    ``upsert_reconciliation_flow_spec`` must write SQL NULL -- not a materialised ``"job"`` /
    ``true`` -- for every v1.5.0 attribute the operator omitted, so re-onboarding an existing
    flow does not stamp today's default into a row that was deliberately leaving it open.
    Static ``ast`` inspection of the ``Row(...)`` literal, following
    ``test_upsert_schema_agreement.py``'s idiom, so nothing here imports pyspark or delta.

``TestJsonSpecSurfaceIsUnchanged`` (d)
    The literal reading of "no change in json spec": every attribute v1.5.0 added to a
    reconciliation flow must be OPTIONAL in ``onboarding_spec.schema.json``. A document written
    against the v1.4.0 schema must still validate against the v1.5.0 one.

Pure Python, no Spark. ``validate_spec`` only reaches for a session on a non-empty
``transformation_sql`` / ``transform_sql``; with ``spark=None`` that surfaces as an
"unexpected error ... NoneType object has no attribute 'sql'" entry which is a property of the
test harness, not of the spec, and is filtered out by ``_real_errors`` below rather than being
allowed to mask (or manufacture) a finding.
"""

import ast
import json
import pathlib

import pytest

from flowx.lakeflow_framework.control_plane import repository
from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SPEC_DIR = _REPO_ROOT / "flowx_testing"
_SCHEMA_PATH = _REPO_ROOT / "onboarding_templates" / "onboarding_spec.schema.json"
_UPSERT_PATH = (
    _REPO_ROOT
    / "src"
    / "flowx"
    / "lakeflow_framework"
    / "onboarding"
    / "metadata_upsert.py"
)

# Every attribute v1.5.0 introduced on a reconciliation flow. Kept in one place because (c) and
# (d) both need it and they must not be able to drift apart.
V150_RECONCILIATION_FLOW_ATTRIBUTES = ("execution_mode", "publish_schema", "dq_config", "dataflow_group_id")

# The {{catalog}} placeholder every flowx_testing spec carries; the onboarding driver
# substitutes the deploy target's catalog before validation, so a validator replay must too.
_CATALOG = "flowx"

# Signature of the no-Spark-session artefact described in the module docstring. Not a finding.
_NO_SESSION_MARKERS = ("NoneType", "object has no attribute")


def _real_errors(errors):
    """Drop the ``spark=None`` harness artefact, keep everything that is genuinely about the spec."""
    return [
        e
        for e in errors
        if not (all(marker in e for marker in _NO_SESSION_MARKERS) and "unexpected error" in e.lower())
    ]


def _load_spec(path):
    return json.loads(path.read_text(encoding="utf-8").replace("{{catalog}}", _CATALOG))


def _legacy_recon_spec_paths():
    """Shipped specs that declare reconciliation_flows and predate execution_mode.

    Discovered by inspection rather than by hard-coded filename, so a spec added later without
    ``execution_mode`` is picked up automatically and one retro-fitted WITH ``execution_mode``
    drops out (it is no longer a pre-v1.5.0 document).
    """
    paths = []
    for path in sorted(_SPEC_DIR.glob("*.json")):
        try:
            spec = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        flows = spec.get("reconciliation_flows") or []
        if flows and all(isinstance(f, dict) and "execution_mode" not in f for f in flows):
            paths.append(path)
    return paths


_LEGACY_SPEC_PATHS = _legacy_recon_spec_paths()


# ---------------------------------------------------------------------------------------------
# (a) An existing spec that never heard of execution_mode still validates
# ---------------------------------------------------------------------------------------------


class TestExistingSpecsStillValidate:
    def test_legacy_recon_specs_were_actually_found(self):
        """A silent zero-spec discovery would turn every test below into a vacuous pass."""
        assert len(_LEGACY_SPEC_PATHS) >= 3, (
            "expected at least three shipped flowx_testing specs with reconciliation_flows and no "
            f"execution_mode; found {[p.name for p in _LEGACY_SPEC_PATHS]}. Either the corpus moved or "
            "every legacy spec has been retro-fitted with execution_mode -- which would itself be the "
            "'no change in json spec' promise being broken in the test fixtures."
        )

    @pytest.mark.parametrize("spec_path", _LEGACY_SPEC_PATHS, ids=lambda p: p.name)
    def test_legacy_spec_validates_with_zero_errors(self, spec_path):
        """THE backward-compatibility assertion: a spec written before v1.5.0 must still onboard.

        ``02_onboarding_engine.py`` raises on a non-empty ``errors`` list, so a single new entry
        here is not a warning -- it is a spec that can no longer be onboarded at all, with no
        edit by its author and no attribute they ever opted into.
        """
        errors = _real_errors(validate_spec(None, _load_spec(spec_path))[-1])
        assert errors == [], (
            f"{spec_path.name} declares no execution_mode -- it is a pre-v1.5.0 document, entirely "
            "job-mode -- yet v1.5.0 validation rejects it with:\n  "
            + "\n  ".join(errors)
            + "\nEvery rule that fires only because of the in-pipeline reconciliation feature must be "
            "gated on the owning flow's execution_mode being 'pipeline'/'pipeline_audit_only'."
        )

    @pytest.mark.parametrize("spec_path", _LEGACY_SPEC_PATHS, ids=lambda p: p.name)
    def test_legacy_spec_reconciliation_flows_survive_validation_unmodified(self, spec_path):
        """``validate_spec`` returns the flow arrays the upsert then persists. A legacy recon flow
        must come back byte-identical -- in particular the validator must not helpfully inject a
        materialised ``execution_mode: "job"``, which would write the default into the control
        table and freeze it there."""
        spec = _load_spec(spec_path)
        expected = json.loads(json.dumps(spec["reconciliation_flows"]))
        returned = validate_spec(None, spec)[2]
        assert returned == expected, (
            f"{spec_path.name}: validate_spec mutated the reconciliation flows it returned. A legacy "
            "flow must reach metadata_upsert exactly as the operator wrote it."
        )
        for flow in returned:
            for attribute in V150_RECONCILIATION_FLOW_ATTRIBUTES:
                if attribute == "dataflow_group_id":
                    continue  # legitimately absent on a legacy flow; never injected either
                assert attribute not in flow, (
                    f"{spec_path.name}: validate_spec injected {attribute!r} into a reconciliation flow "
                    "that did not declare it."
                )


# ---------------------------------------------------------------------------------------------
# (b) The loader treats absence as job mode, and job mode as "not mine"
# ---------------------------------------------------------------------------------------------


class _StubRow:
    """A duck-typed control-table row over a fixed field set.

    A genuinely absent column must raise ``AttributeError`` -- exactly what a real
    ``pyspark.sql.Row`` collected from a table lacking that column does -- so this cannot be a
    ``SimpleNamespace``, where every unset name would resolve to a present ``None`` and the
    already-provisioned-table case would go untested. Same rationale as
    ``test_source_plane_plan._StubRow``.
    """

    def __init__(self, **fields):
        self._fields = dict(fields)

    def __getattr__(self, name):
        try:
            return self.__dict__["_fields"][name]
        except KeyError:
            raise AttributeError(name) from None


class _StubDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *_args, **_kwargs):
        """``load_active_group_metadata`` filters on is_active/dataflow_group_id in Spark; the
        rows below are already the post-filter set, so this is a pass-through. The
        ``execution_mode`` split it does NOT do in Spark is the point of these tests."""
        return self

    def collect(self):
        return list(self._rows)


class _StubSpark:
    def __init__(self, tables):
        self._tables = tables
        self.requested = []

    def table(self, name):
        self.requested.append(name)
        return _StubDataFrame(self._tables.get(name.rsplit(".", 1)[-1], []))


def _recon_row(reconciliation_id, **fields):
    return _StubRow(reconciliation_id=reconciliation_id, **fields)


def _load(recon_rows, ingestion_rows=None):
    spark = _StubSpark(
        {
            "dataflow_group_spec": [_StubRow(dataflow_group_id="dfg_legacy", is_active=True)],
            "ingestion_flow_spec": ingestion_rows if ingestion_rows is not None else [_StubRow(dataflow_id="ing_1")],
            "transformation_flow_spec": [],
            "reconciliation_flow_spec": recon_rows,
        }
    )
    return repository.load_active_group_metadata(spark, "ctl", "dfg_legacy")


class TestJobModeIsTheDefaultInTheLoader:
    def test_row_with_no_execution_mode_attribute_is_excluded(self):
        """The already-provisioned-table case: ``reconciliation_flow_spec`` was created before
        v1.5.0 and ``01_setup`` only ever issues ``CREATE TABLE IF NOT EXISTS``, so the column may
        genuinely not exist. Such a row must read as job mode and stay out of the pipeline DAG --
        and must NOT raise, which a bare ``row.execution_mode`` would."""
        metadata = _load([_recon_row("recon_pre_v150")])
        assert metadata.reconciliation_rows == [], (
            "a reconciliation row with no execution_mode attribute at all was pulled into the pipeline "
            "DAG. An already-onboarded job-mode flow would suddenly start registering L3/L4/L5 datasets "
            "inside the Lakeflow update it was never part of."
        )

    def test_absent_column_does_not_raise(self):
        """Guards the specific regression a ``F.col('execution_mode')`` Spark filter, or a bare
        attribute read, would reintroduce: the whole pipeline update failing at metadata load."""
        _load([_recon_row("recon_pre_v150")])  # must not raise AnalysisException/AttributeError

    @pytest.mark.parametrize("execution_mode", [None, "", "job"])
    def test_job_mode_values_are_excluded(self, execution_mode):
        """``NULL`` (the DDL default, what an ALTER-added column backfills to), the empty string,
        and an explicit ``"job"`` all mean the same thing: the standalone job engine owns this
        flow, and the pipeline loader must not see it."""
        metadata = _load([_recon_row("recon_job", execution_mode=execution_mode)])
        assert metadata.reconciliation_rows == [], f"execution_mode={execution_mode!r} must resolve to job mode"

    @pytest.mark.parametrize("execution_mode", ["pipeline", "pipeline_audit_only"])
    def test_pipeline_modes_are_included(self, execution_mode):
        """The converse -- without this the exclusion tests above would pass on a loader that
        returns nothing at all."""
        metadata = _load([_recon_row("recon_pipeline", execution_mode=execution_mode)])
        assert [r.reconciliation_id for r in metadata.reconciliation_rows] == ["recon_pipeline"]

    def test_mixed_table_returns_only_pipeline_rows_in_order(self):
        """A control table part-migrated to v1.5.0: some rows never touched, some job, some
        pipeline. Only the pipeline ones cross into the DAG, and their relative order (which
        determines dlt registration order) is preserved."""
        metadata = _load(
            [
                _recon_row("legacy_no_column"),
                _recon_row("explicit_pipeline", execution_mode="pipeline"),
                _recon_row("null_mode", execution_mode=None),
                _recon_row("audit_only", execution_mode="pipeline_audit_only"),
                _recon_row("explicit_job", execution_mode="job"),
            ]
        )
        assert [r.reconciliation_id for r in metadata.reconciliation_rows] == ["explicit_pipeline", "audit_only"]

    def test_group_metadata_still_carries_the_other_three_fields(self):
        """``GroupMetadata`` grew a fourth field in v1.5.0. Positional unpacking of the first
        three -- how every pre-v1.5.0 caller reads it -- must still mean the same things."""
        metadata = _load([_recon_row("legacy_no_column")], ingestion_rows=[_StubRow(dataflow_id="ing_1")])
        assert metadata._fields == (
            "group_row",
            "ingestion_rows",
            "transformation_rows",
            "reconciliation_rows",
        ), "the new field must be APPENDED, never inserted ahead of an existing one"
        group_row, ingestion_rows, transformation_rows, reconciliation_rows = metadata
        assert group_row.dataflow_group_id == "dfg_legacy"
        assert [r.dataflow_id for r in ingestion_rows] == ["ing_1"]
        assert transformation_rows == []
        assert reconciliation_rows == []

    def test_group_of_only_job_mode_recon_flows_is_not_a_pipeline_group(self):
        """A pre-v1.5.0 group whose ONLY flows are job-mode reconciliations has nothing to build
        in a Lakeflow update, and must fail loudly rather than register an empty graph. This is
        the pre-existing 'nothing to run' contract, unchanged."""
        from flowx.lakeflow_framework.exceptions import FrameworkConfigError

        with pytest.raises(FrameworkConfigError):
            _load([_recon_row("recon_job", execution_mode="job")], ingestion_rows=[])


# ---------------------------------------------------------------------------------------------
# (c) Omitted attributes are persisted as NULL, not as today's default
# ---------------------------------------------------------------------------------------------


def _upsert_tree():
    assert _UPSERT_PATH.is_file(), f"metadata_upsert.py not found at {_UPSERT_PATH}"
    return ast.parse(_UPSERT_PATH.read_text(encoding="utf-8"))


_UPSERT_TREE = _upsert_tree()


def _row_keyword_values(function_name):
    """``{keyword name: ast node}`` for every ``Row(...)`` keyword inside ``function_name``."""
    for node in ast.walk(_UPSERT_TREE):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name != function_name:
            continue
        values = {}
        for call in ast.walk(node):
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "Row":
                for keyword in call.keywords:
                    if keyword.arg is not None:
                        values[keyword.arg] = keyword.value
        assert values, f"{function_name} contains no Row(...) keyword arguments"
        return values
    raise AssertionError(f"{function_name} is not defined in metadata_upsert.py")


_RECON_ROW_VALUES = _row_keyword_values("upsert_reconciliation_flow_spec")


def _is_bare_flow_get(node, key):
    """True for exactly ``flow.get("<key>")`` -- one argument, so the miss yields ``None``."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "flow"
        and len(node.args) == 1
        and not node.keywords
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == key
    )


class TestUpsertWritesNullNotADefault:
    @pytest.mark.parametrize(
        "column,spec_key",
        [
            ("execution_mode", "execution_mode"),
            ("publish_schema", "publish_schema"),
            ("two_tier_verification", "two_tier_verification"),
        ],
    )
    def test_omitted_attribute_is_written_as_null(self, column, spec_key):
        """``flow.get(key)`` with no default -- so an operator who never wrote the key gets SQL
        NULL and keeps whatever the DDL comment says the default is. ``flow.get(key, "job")`` or
        ``flow.get(key, True)`` would stamp TODAY's default into the row, making a later change of
        default invisible to every flow already onboarded, and rewriting rows that were
        deliberately leaving the choice open."""
        assert column in _RECON_ROW_VALUES, (
            f"upsert_reconciliation_flow_spec's Row(...) does not set {column} at all -- the operator's "
            "value would be dropped silently (the two_tier_verification defect)."
        )
        node = _RECON_ROW_VALUES[column]
        assert _is_bare_flow_get(node, spec_key), (
            f"{column} must be persisted as a bare flow.get({spec_key!r}) so an omitted key becomes SQL "
            f"NULL; found {ast.dump(node)}. A second argument to .get() materialises today's default "
            "into the control table."
        )

    def test_omitted_dq_config_is_written_as_null_not_an_empty_json_object(self):
        """``dq_config`` is the one that is json-encoded, so the NULL has to be explicit: the
        sibling ``error_handling_json``/``logging_config_json`` columns use
        ``json.dumps(flow.get(key, {}))``, which would write the string ``"{}"`` -- a present,
        empty dq_config -- rather than NULL. On a reconciliation flow that distinction is
        load-bearing: an empty rules list is a dq_config the engine will attach expectations
        for, NULL is a flow that never opted in."""
        node = _RECON_ROW_VALUES["dq_config_json"]
        assert isinstance(node, ast.IfExp), (
            "dq_config_json must be a conditional expression falling back to None; found "
            f"{ast.dump(node)}"
        )
        assert isinstance(node.orelse, ast.Constant) and node.orelse.value is None, (
            "dq_config_json's fallback for an omitted dq_config must be the literal None (SQL NULL), "
            f"not {ast.dump(node.orelse)}"
        )

    def test_the_pre_v150_columns_are_still_written_the_same_way(self):
        """The other half of "no impact": v1.5.0 must not have quietly changed how an EXISTING
        column is persisted while adding the new ones next to it."""
        for column in (
            "reconciliation_id",
            "dataflow_group_id",
            "source_config_json",
            "target_configs_json",
            "match_keys_json",
            "compare_columns_json",
            "transform_sql",
            "error_handling_json",
            "logging_config_json",
            "is_active",
        ):
            assert column in _RECON_ROW_VALUES, (
                f"{column} disappeared from upsert_reconciliation_flow_spec's Row(...) literal -- a "
                "pre-v1.5.0 attribute would stop being persisted."
            )
        assert _is_bare_flow_get(_RECON_ROW_VALUES["transform_sql"], "transform_sql")


# ---------------------------------------------------------------------------------------------
# (d) "no change in json spec": every new attribute is optional
# ---------------------------------------------------------------------------------------------


def _load_schema():
    return json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))


_SCHEMA = _load_schema()
_RECON_FLOW_SCHEMA = _SCHEMA["$defs"]["reconciliationFlow"]


def _every_required_array(node, path="#"):
    """Yield ``(json_pointer, required_list)`` for every ``required`` array anywhere in a schema."""
    if isinstance(node, dict):
        required = node.get("required")
        if isinstance(required, list):
            yield path, required
        for key, value in node.items():
            yield from _every_required_array(value, f"{path}/{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _every_required_array(value, f"{path}/{index}")


class TestJsonSpecSurfaceIsUnchanged:
    @pytest.mark.parametrize("attribute", V150_RECONCILIATION_FLOW_ATTRIBUTES)
    def test_new_reconciliation_attribute_is_not_required_anywhere_in_the_flow_schema(self, attribute):
        """The literal test of "no change in json spec": a document written against the v1.4.0
        schema names none of these, so any of them appearing in a ``required`` array inside the
        reconciliation-flow subtree would make every existing spec schema-invalid overnight."""
        offenders = [
            pointer for pointer, required in _every_required_array(_RECON_FLOW_SCHEMA) if attribute in required
        ]
        assert offenders == [], (
            f"{attribute!r} is listed as required at {offenders} inside $defs/reconciliationFlow. Every "
            "v1.5.0 reconciliation attribute is opt-in; requiring one invalidates every pre-v1.5.0 spec."
        )

    def test_reconciliation_flow_required_set_is_exactly_the_pre_v150_one(self):
        """Pinned as a whole set, not just checked for the four new names -- the promise is that
        the required surface did not move at all, in either direction. Loosening it (dropping a
        name) is also a change, and would let a spec onboard that the validator then rejects."""
        assert sorted(_RECON_FLOW_SCHEMA.get("required", [])) == sorted(
            ["reconciliation_id", "source_config", "target_configs", "match_keys"]
        )

    @pytest.mark.parametrize("attribute", V150_RECONCILIATION_FLOW_ATTRIBUTES)
    def test_new_reconciliation_attribute_is_accepted_by_the_flow_schema(self, attribute):
        """The other direction: optional must not mean unexpressible. A v1.5.0 spec has to be
        schema-valid too, either because the property is declared or because the object is open."""
        declared = attribute in (_RECON_FLOW_SCHEMA.get("properties") or {})
        open_object = _RECON_FLOW_SCHEMA.get("additionalProperties", True) is not False
        assert declared or open_object, (
            f"{attribute!r} is neither declared in $defs/reconciliationFlow.properties nor permitted by "
            "additionalProperties -- a v1.5.0 spec using it would be schema-invalid."
        )

    def test_top_level_required_did_not_grow(self):
        """``dataflow_group_id`` was already the single top-level requirement; v1.5.0 must not
        have added a sibling (e.g. a mandatory source-plane block)."""
        assert _SCHEMA.get("required") == ["dataflow_group_id"]

    def test_reconciliation_flows_itself_is_still_optional(self):
        """A spec with no reconciliation at all -- the majority of the shipped corpus -- must not
        have acquired a requirement to declare one.

        ``reconciliation_flows`` legitimately appears in a ``required`` array in exactly one
        place: the top-level ``anyOf``, whose three branches say "declare at least one of
        ingestion/transformation/reconciliation flows". That is a disjunction, so satisfying any
        ONE branch is enough and an ingestion-only spec is unaffected. So the assertion is on the
        shape of that disjunction, not on the mere absence of the name."""
        branch_requirements = [tuple(branch.get("required", [])) for branch in _SCHEMA.get("anyOf", [])]
        assert sorted(branch_requirements) == sorted(
            [("ingestion_flows",), ("transformation_flows",), ("reconciliation_flows",)]
        ), (
            "the top-level anyOf must stay three single-key branches -- collapsing two of them into one "
            f"branch turns the disjunction into a conjunction; found {branch_requirements}"
        )
        outside_the_disjunction = [
            pointer
            for pointer, required in _every_required_array(_SCHEMA)
            if "reconciliation_flows" in required and not pointer.startswith("#/anyOf/")
        ]
        assert outside_the_disjunction == [], (
            f"reconciliation_flows became required at {outside_the_disjunction}; every ingestion-only "
            "spec would break."
        )
