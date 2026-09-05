"""``plan_source_plane`` must resolve ``${param}`` placeholders in ``source_config_json``.

Regression test for a real Phase C failure. ``plan_source_plane`` accepted a
``pipeline_parameters`` argument and **never used it** -- its own docstring claimed substitution
had already happened in ``onboarding/spec_loader.py`` before the control-table write. That was
wrong on both counts: ``spec_loader.py`` performs no ``${param}`` substitution at all, and the
control tables demonstrably stored the raw placeholder::

    SELECT get_json_object(source_config_json, '$.path') ...
    -> ${landing_root}/physical_device/

The source plane is what builds the base ingestion node that issues the read, so the literal text
reached Auto Loader and the update died at execution time with::

    IllegalArgumentException: Path must be absolute: ${landing_root}/subscriber

Substitution is applied fresh on every pipeline update (``engine/flow_generators.py`` already did
this for the flow layer) precisely so an operator can retarget a group's paths without
re-onboarding -- which is exactly why the source plane has to apply it too, with the same
function, so the two layers can never disagree about what a path means.

These tests are pure: ``plan_source_plane`` takes duck-typed rows and needs no Spark session.
"""

import json

import pytest

from flowx.lakeflow_framework.engine.source_plane import plan_source_plane
from flowx.lakeflow_framework.exceptions import FrameworkConfigError


LANDING_ROOT = "/Volumes/flowx/staging/uc_3/batch"


class _Row:
    """Duck-typed stand-in for a control-table ingestion row."""

    def __init__(self, dataflow_id, path, cdc_load_strategy="APPEND"):
        self.dataflow_id = dataflow_id
        self.source_type = "autoloader"
        self.cdc_load_strategy = cdc_load_strategy
        self.source_config_json = json.dumps({"path": path, "format": "csv"})
        self.target_catalog = "flowx"
        self.target_schema = "staging"
        self.target_table = f"{dataflow_id}_tgt"


def _reader_paths(plan):
    """Every ``path`` the plan would hand to a reader."""
    return [spec["source_config"].get("path") for spec in plan.node_reader_specs.values()]


def test_placeholder_is_substituted_in_the_reader_spec():
    plan = plan_source_plane(
        [_Row("df_a", "${landing_root}/physical_device/")],
        [],
        [],
        {"landing_root": LANDING_ROOT},
    )
    assert _reader_paths(plan) == [f"{LANDING_ROOT}/physical_device/"]


def test_no_unsubstituted_placeholder_survives_into_any_reader_spec():
    """The precise shape of the defect: a literal ``${...}`` reaching the reader."""
    rows = [
        _Row("df_a", "${landing_root}/physical_device/"),
        _Row("df_b", "${landing_root}/customer/"),
        _Row("df_c", "${landing_root}/subscriber/"),
    ]
    plan = plan_source_plane(rows, [], [], {"landing_root": LANDING_ROOT})
    paths = _reader_paths(plan)
    assert len(paths) == 3
    for path in paths:
        assert "${" not in path, f"unsubstituted placeholder reached the reader: {path!r}"
        assert path.startswith("/Volumes/"), f"path is not absolute: {path!r}"


def test_distinct_placeholder_paths_stay_distinct_identities():
    """Substitution must not collapse three sources into one shared read identity.

    Before substitution the three paths differ only after the placeholder; a naive
    implementation that fingerprinted the raw string would still separate them, but one that
    substituted incorrectly (e.g. dropping the suffix) could merge them -- which would silently
    make three tables read one directory.
    """
    rows = [
        _Row("df_a", "${landing_root}/physical_device/"),
        _Row("df_b", "${landing_root}/customer/"),
        _Row("df_c", "${landing_root}/subscriber/"),
    ]
    plan = plan_source_plane(rows, [], [], {"landing_root": LANDING_ROOT})
    assert len(set(_reader_paths(plan))) == 3
    assert len(plan.node_reader_specs) == 3


def test_paths_without_placeholders_are_untouched():
    literal = f"{LANDING_ROOT}/physical_device/"
    plan = plan_source_plane([_Row("df_a", literal)], [], [], {"landing_root": "/should/not/be/used"})
    assert _reader_paths(plan) == [literal]


@pytest.mark.parametrize("parameters", [pytest.param(None, id="none"), pytest.param({}, id="empty")])
def test_unresolvable_placeholder_fails_loudly_at_PLANNING_time(parameters):
    """A placeholder with no matching parameter raises here, not at read time.

    This is strictly better than the behaviour the bug produced. Before the fix the raw text
    slipped through planning and surfaced much later as an opaque
    ``IllegalArgumentException: Path must be absolute: ${landing_root}/subscriber`` from deep
    inside Auto Loader. Now ``substitute_path_parameters`` refuses up front and names the missing
    parameter, so a misspelled or undeclared ``pipeline_parameters`` key is caught before any
    dataset is defined.
    """
    with pytest.raises(FrameworkConfigError) as excinfo:
        plan_source_plane([_Row("df_a", "${landing_root}/x/")], [], [], parameters)
    assert "landing_root" in str(excinfo.value)
