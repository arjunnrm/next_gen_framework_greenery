"""``apply_all_governance_tags`` belongs to the GOVERNANCE module.

Two things are asserted here, and they fail for different reasons:

1. **Module ownership.** The function was moved out of
   ``control_plane/post_deployment.py`` -- where it sat beside unrelated CDC change-count
   capture -- into ``governance/tags.py``, next to the single-table primitive it delegates to.
   ``post_deployment`` re-exports it so the original import path keeps working, and the test
   pins BOTH paths to the same object so a future edit cannot quietly re-fork them.

2. **Orchestration behaviour.** The group-level function is the piece that decides *which*
   flows get tagged. It is exercised with fakes (no Spark session, no control tables) so the
   selection logic -- skip a flow with no ``governance_tags_json``, skip one whose JSON is
   empty, tag everything else, across BOTH ingestion and transformation rows -- is covered by a
   fast unit test rather than only by the Spark-dependent integration test.

Context for why this is worth a test at all: during the UC3 build a pipeline ran green end to
end while applying **zero** tags, because nothing invoked this function. The failure was
completely silent -- no error, no warning, just an untagged table. Tag application is opt-in
orchestration, so the code path that chooses what to tag deserves explicit coverage.
"""

import sys
import types

import pytest

from flowx.lakeflow_framework.control_plane import post_deployment
from flowx.lakeflow_framework.governance import tags as governance_tags


class _FakeRow:
    """A stand-in for a control-table flow row (only the attributes the function reads)."""

    def __init__(self, table, governance_tags_json, catalog="cat", schema="sch"):
        self.target_catalog = catalog
        self.target_schema = schema
        self.target_table = table
        self.governance_tags_json = governance_tags_json


class _FakeMetadata:
    def __init__(self, ingestion_rows=(), transformation_rows=()):
        self.ingestion_rows = list(ingestion_rows)
        self.transformation_rows = list(transformation_rows)


def test_apply_all_governance_tags_lives_in_the_governance_module():
    """It is DEFINED in governance.tags, not merely importable from there."""
    assert governance_tags.apply_all_governance_tags.__module__ == "flowx.lakeflow_framework.governance.tags"


def test_post_deployment_reexports_the_same_object():
    """The legacy import path still works and is not a divergent copy.

    ``notebooks/04_governance`` and any operator script pinned to the old path must keep
    working; re-exporting the *same function object* (rather than redefining it) is what
    guarantees the two paths can never drift apart.
    """
    assert post_deployment.apply_all_governance_tags is governance_tags.apply_all_governance_tags


def test_post_deployment_retains_its_own_unrelated_concern():
    """The move took ONLY the governance function -- CDC capture stayed put."""
    assert callable(post_deployment.capture_all_scd_change_counts)
    assert post_deployment.capture_all_scd_change_counts.__module__ == (
        "flowx.lakeflow_framework.control_plane.post_deployment"
    )


def _run_with_fakes(monkeypatch, metadata):
    """Call the group-level entrypoint with the control-table load and the tag DDL stubbed."""
    applied = []

    monkeypatch.setattr(
        governance_tags,
        "apply_governance_tags",
        lambda spark, catalog, schema, table, tags: applied.append((table, tags)),
    )

    # The repository import is function-local (it breaks a governance <-> control_plane cycle),
    # so it must be patched on the module the function imports FROM, not on governance.tags.
    fake_repository = types.ModuleType("flowx.lakeflow_framework.control_plane.repository")
    fake_repository.load_active_group_metadata = lambda spark, catalog, group_id: metadata
    monkeypatch.setitem(sys.modules, "flowx.lakeflow_framework.control_plane.repository", fake_repository)

    governance_tags.apply_all_governance_tags(spark=None, control_catalog="ctl", group_id="dfg_x")
    return applied


def test_tags_are_applied_across_ingestion_and_transformation_rows(monkeypatch):
    metadata = _FakeMetadata(
        ingestion_rows=[_FakeRow("ingest_tbl", '{"table_tags": {"domain": "crm"}}')],
        transformation_rows=[_FakeRow("transform_tbl", '{"table_tags": {"domain": "gold"}}')],
    )
    applied = _run_with_fakes(monkeypatch, metadata)
    assert [t for t, _ in applied] == ["ingest_tbl", "transform_tbl"]


@pytest.mark.parametrize(
    "governance_tags_json",
    [
        pytest.param(None, id="absent"),
        pytest.param("", id="empty_string"),
        pytest.param("{}", id="empty_json_object"),
    ],
)
def test_flows_without_governance_tags_are_skipped(monkeypatch, governance_tags_json):
    """Tagging is opt-in per flow: no block (or an empty one) means no DDL at all.

    ``{}`` matters as much as ``None`` -- it parses successfully but carries nothing to apply,
    and issuing an empty ``SET TAGS`` would be a syntax error rather than a no-op.
    """
    metadata = _FakeMetadata(ingestion_rows=[_FakeRow("untagged_tbl", governance_tags_json)])
    assert _run_with_fakes(monkeypatch, metadata) == []


def test_only_the_flows_carrying_tags_are_touched(monkeypatch):
    """A mixed group tags the flows that declare tags and leaves the others entirely alone."""
    metadata = _FakeMetadata(
        ingestion_rows=[
            _FakeRow("tagged_a", '{"table_tags": {"pii": "true"}}'),
            _FakeRow("untagged", None),
            _FakeRow("tagged_b", '{"column_tags": [{"column": "email", "tags": {"pii": "true"}}]}'),
        ]
    )
    applied = _run_with_fakes(monkeypatch, metadata)
    assert [t for t, _ in applied] == ["tagged_a", "tagged_b"]
