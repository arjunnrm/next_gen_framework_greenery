"""Unit tests for control_plane/repository.py::load_active_group_metadata.

Hand-written duck-typed stubs (``_StubRow``/``_StubDataFrame``/``_StubSpark``), in the style of
``databricks-app/tests/test_framework_spec_agreement.py``'s ``_StubRow``/``_StubDataFrame``/
``_StubSpark`` -- NOT ``unittest.mock.MagicMock`` -- because the real ``spark`` fixture in
``tests/conftest.py`` opens a live Databricks Connect session, which this pure-Python control-flow
test has no need to pay for. ``load_active_group_metadata`` builds its row filter with
``pyspark.sql.functions as F`` (``F.col("dataflow_group_id") == group_id) & (F.col("is_active"))``)
and passes it to ``DataFrame.filter(...)`` -- constructing that ``Column`` needs no live session
(pyspark builds it as an unresolved expression), but *evaluating* it does. So ``_StubDataFrame``
never inspects the predicate it is handed: each stub table is pre-scoped to exactly the rows the
real filter would have narrowed it to, and ``.filter()`` is a pure pass-through. This mirrors how
little the app's own stub simulates -- it returns canned data regardless of the query -- and keeps
these tests about ``load_active_group_metadata``'s own control flow (the group lookup, the three
flow-spec reads, the pipeline-mode reconciliation filter, the emptiness guard) rather than
re-implementing Spark's filter semantics.

Covers:
  * the ``GroupMetadata`` NamedTuple's shape and field names;
  * a group with pipeline-mode reconciliation rows returns them in ``reconciliation_rows``;
  * a group whose reconciliation rows are all ``execution_mode`` NULL/``"job"`` comes back with an
    EMPTY ``reconciliation_rows`` list (they belong to the standalone job engine, not this loader);
  * a reconciliation-only group (no ingestion/transformation rows, but a pipeline-mode
    reconciliation row) is permitted -- ``load_active_group_metadata`` does not raise;
  * a group with nothing active in pipeline-relevant terms still raises ``FrameworkConfigError``,
    with the NEW three-way message naming ingestion, transformation AND pipeline-mode
    reconciliation flows;
  * rows lacking an ``execution_mode`` attribute entirely (simulating a pre-migration control
    table row -- ``01_setup`` only ever runs ``CREATE TABLE IF NOT EXISTS``, so an
    already-provisioned ``reconciliation_flow_spec`` need not carry a column added later) are
    treated as ``"job"``, exercising the ``getattr(r, "execution_mode", None)`` default path
    rather than a present-but-``None`` value.
"""

import pytest

from flowx.lakeflow_framework.control_plane.repository import (
    GroupMetadata,
    load_active_group_metadata,
)
from flowx.lakeflow_framework.exceptions import FrameworkConfigError

CONTROL_CATALOG = "flowx"
GROUP_ID = "dfg_test_group"
CONTROL_SCHEMA = f"{CONTROL_CATALOG}.config"


class _StubRow:
    """A duck-typed control-table row: attribute access over a fixed field set.

    Unlike a ``dict``, a genuinely absent field raises ``AttributeError`` on access -- exactly
    the behaviour of a real ``pyspark.sql.Row`` built from a table that never had that column --
    so ``getattr(row, "execution_mode", None)`` exercises its default branch rather than finding
    a present ``None``.
    """

    def __init__(self, **fields):
        self._fields = dict(fields)

    def __getattr__(self, name):
        try:
            return self._fields[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def asDict(self):
        return dict(self._fields)


class _StubDataFrame:
    """Pre-scoped row set; ``.filter()`` ignores its predicate and returns itself."""

    def __init__(self, rows):
        self._rows = list(rows)

    def filter(self, _predicate):
        return self

    def collect(self):
        return list(self._rows)


class _StubSpark:
    """``.table(name) -> _StubDataFrame`` over a fixed {qualified_name: rows} map."""

    def __init__(self, tables):
        self._tables = tables

    def table(self, name):
        try:
            return self._tables[name]
        except KeyError as exc:
            raise AssertionError(f"unexpected table requested: {name!r}") from exc


def _group_row(**overrides):
    fields = {"dataflow_group_id": GROUP_ID, "is_active": True, "pipeline_parameters_json": None}
    fields.update(overrides)
    return _StubRow(**fields)


def _spark_for(group_rows=(), ingestion_rows=(), transformation_rows=(), reconciliation_rows=()):
    return _StubSpark(
        {
            f"{CONTROL_SCHEMA}.dataflow_group_spec": _StubDataFrame(group_rows),
            f"{CONTROL_SCHEMA}.ingestion_flow_spec": _StubDataFrame(ingestion_rows),
            f"{CONTROL_SCHEMA}.transformation_flow_spec": _StubDataFrame(transformation_rows),
            f"{CONTROL_SCHEMA}.reconciliation_flow_spec": _StubDataFrame(reconciliation_rows),
        }
    )


def test_group_metadata_is_a_namedtuple_with_the_expected_fields():
    assert GroupMetadata._fields == ("group_row", "ingestion_rows", "transformation_rows", "reconciliation_rows")


def test_group_metadata_fields_are_accessible_positionally_and_by_name():
    meta = GroupMetadata(group_row="g", ingestion_rows=["i"], transformation_rows=["t"], reconciliation_rows=["r"])
    assert meta.group_row == "g"
    assert meta.ingestion_rows == ["i"]
    assert meta.transformation_rows == ["t"]
    assert meta.reconciliation_rows == ["r"]
    assert tuple(meta) == ("g", ["i"], ["t"], ["r"])


def test_group_with_pipeline_mode_recon_rows_returns_them():
    recon_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_1", execution_mode="pipeline")
    spark = _spark_for(group_rows=[_group_row()], ingestion_rows=[_StubRow(dataflow_id="df_1")], reconciliation_rows=[recon_row])

    meta = load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)

    assert isinstance(meta, GroupMetadata)
    assert meta.reconciliation_rows == [recon_row]


def test_recon_rows_all_job_mode_yield_empty_reconciliation_rows():
    job_rows = [
        _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_null", execution_mode=None),
        _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_job", execution_mode="job"),
    ]
    # An active ingestion row keeps this a normal (non-reconciliation-only) group so only the
    # recon-filtering behaviour is under test here.
    spark = _spark_for(group_rows=[_group_row()], ingestion_rows=[_StubRow(dataflow_id="df_1")], reconciliation_rows=job_rows)

    meta = load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)

    assert meta.reconciliation_rows == []


def test_reconciliation_only_group_is_permitted():
    recon_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_1", execution_mode="pipeline")
    spark = _spark_for(group_rows=[_group_row()], ingestion_rows=[], transformation_rows=[], reconciliation_rows=[recon_row])

    meta = load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)

    assert meta.ingestion_rows == []
    assert meta.transformation_rows == []
    assert meta.reconciliation_rows == [recon_row]


def test_nothing_active_raises_with_the_new_three_way_message():
    spark = _spark_for(group_rows=[_group_row()], ingestion_rows=[], transformation_rows=[], reconciliation_rows=[])

    with pytest.raises(FrameworkConfigError, match="no active ingestion, transformation or pipeline-mode reconciliation flows"):
        load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)


def test_job_mode_only_recon_rows_do_not_rescue_an_otherwise_empty_group():
    """All-job-mode recon rows filter down to empty, so a group with only those and no
    ingestion/transformation rows must still raise -- it has nothing for THIS (pipeline) loader
    to run, even though the standalone job engine still has work."""
    job_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_job", execution_mode="job")
    spark = _spark_for(group_rows=[_group_row()], ingestion_rows=[], transformation_rows=[], reconciliation_rows=[job_row])

    with pytest.raises(FrameworkConfigError, match="no active ingestion, transformation or pipeline-mode reconciliation flows"):
        load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)


def test_missing_group_row_raises():
    spark = _spark_for(group_rows=[])

    with pytest.raises(FrameworkConfigError):
        load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)


def test_recon_rows_missing_execution_mode_attribute_entirely_are_treated_as_job():
    """Simulates a pre-migration control table: the row has no ``execution_mode`` attribute at
    all (not even ``None``), so ``getattr(r, "execution_mode", None)`` must take its default
    rather than raising, and the row must be treated exactly like an explicit ``"job"`` row."""
    pre_migration_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_legacy")
    assert not hasattr(pre_migration_row, "execution_mode")

    spark = _spark_for(
        group_rows=[_group_row()], ingestion_rows=[_StubRow(dataflow_id="df_1")], reconciliation_rows=[pre_migration_row]
    )

    meta = load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)

    assert meta.reconciliation_rows == []


def test_recon_rows_missing_execution_mode_entirely_do_not_rescue_an_otherwise_empty_group():
    pre_migration_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_legacy")
    spark = _spark_for(group_rows=[_group_row()], ingestion_rows=[], transformation_rows=[], reconciliation_rows=[pre_migration_row])

    with pytest.raises(FrameworkConfigError, match="no active ingestion, transformation or pipeline-mode reconciliation flows"):
        load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)


def test_mixed_execution_mode_recon_rows_keep_only_pipeline_mode_ones():
    pipeline_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_pipeline", execution_mode="pipeline")
    job_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_job", execution_mode="job")
    null_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_null", execution_mode=None)
    legacy_row = _StubRow(dataflow_group_id=GROUP_ID, is_active=True, reconciliation_id="rc_legacy")
    spark = _spark_for(
        group_rows=[_group_row()],
        ingestion_rows=[_StubRow(dataflow_id="df_1")],
        reconciliation_rows=[pipeline_row, job_row, null_row, legacy_row],
    )

    meta = load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)

    assert meta.reconciliation_rows == [pipeline_row]
