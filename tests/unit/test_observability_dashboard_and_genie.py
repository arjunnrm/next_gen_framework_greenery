"""Structural guards for the two serialized artefacts the observability surfaces ship as JSON:
``databricks-bi/flowx_observability_dashboard.lvdash.json`` and
``databricks-genie/flowx_observability.geniespace.json``.

WHY THESE NEED A TEST AT ALL. Both are large machine-generated JSON files that the platform
validates only at deploy time, and both have failure modes that no other check in this repo can
see:

* A Lakeview dashboard with a wrong widget ``version`` renders a broken widget rather than
  erroring -- version is per widget type and is the single most common cause of a silently blank
  dashboard.
* ``queryLines`` elements are concatenated **verbatim**, with no separator. An element that does
  not end in a newline welds itself to the next line, which can turn a trailing ``--`` comment
  into a swallowed clause.
* A dashboard query carrying a hardcoded catalog silently defeats ``dataset_catalog`` /
  ``dataset_schema`` and pins the dashboard to one workspace -- the same trap documented at
  length in ``resources/flowx_bi/flowx_control_dashboard.yml``.
* The Genie API **rejects** an unsorted ``data_sources.tables`` or an unsorted repeated block
  outright ("Invalid export proto: ... must be sorted by ..."). That is a deploy-time failure
  which is trivially preventable here, and very easy to reintroduce by hand-editing the file.

Deliberately file-based: nothing here imports the framework or talks to a workspace, so it runs
in the repo's offline environment.
"""

import json
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DASHBOARD = REPO_ROOT / "databricks-bi" / "flowx_observability_dashboard.lvdash.json"
GENIE_SPACE = REPO_ROOT / "databricks-genie" / "flowx_observability.geniespace.json"

#: The version each widget type must declare. Wrong version = silently broken widget.
#: Source: .claude/skills/databricks-aibi-dashboards/references/*.md.
WIDGET_VERSIONS = {
    "counter": 2,
    "table": 2,
    "bar": 3,
    "line": 3,
    "area": 3,
    "pie": 3,
    "heatmap": 3,
    "combo": 1,
    "sankey": 1,
    "forecast-line": 1,
    "filter-single-select": 2,
    "filter-multi-select": 2,
    "filter-date-range-picker": 2,
}

#: Datasets that read ``system.billing`` directly, by design. ``AI_FORECAST`` needs an HOURLY
#: series and ``v_dataflow_cost`` aggregates to the day: on a young workspace a daily series has
#: too few points to fit a model, and AI_FORECAST on three daily points extrapolated to ten
#: billion DBUs when this was first tried. They are still catalog-portable -- ``system.*`` is
#: a fixed three-part name on every workspace -- and they carry no ``dataflow_group_id`` because
#: billing usage rows attribute to a pipeline or job id, not to a group.
FORECAST_DATASETS = {"forecast_spend", "forecast_runs", "forecast_summary"}

#: The observability views every dataset query is allowed to read, plus the two control tables
#: reachable as a two-part name. Keep in step with observability_views.py.
KNOWN_VIEWS = {
    "v_dataflow_group_catalog", "v_flow_inventory", "v_pipeline_registry",
    "v_pipeline_updates", "v_job_runs", "v_dataflow_cost", "v_flow_metrics",
    "v_dq_results", "v_reconciliation_health", "v_dataflow_lineage",
    "v_group_health_summary",
}


@pytest.fixture(scope="module")
def dashboard():
    assert DASHBOARD.exists(), f"{DASHBOARD} is missing"
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def genie():
    assert GENIE_SPACE.exists(), f"{GENIE_SPACE} is missing"
    return json.loads(GENIE_SPACE.read_text(encoding="utf-8"))


def widgets(dash):
    for page in dash["pages"]:
        for entry in page["layout"]:
            yield page, entry["widget"], entry["position"]


# ============================================================== dashboard: structure


def test_dashboard_has_datasets_pages_and_a_theme(dashboard):
    assert dashboard["datasets"], "no datasets"
    assert dashboard["pages"], "no pages"
    # Without a theme the dashboard inherits the workspace default and looks generic; this one
    # is a customer-facing surface.
    assert "theme" in dashboard["uiSettings"]


def test_every_page_declares_type_and_layout_version(dashboard):
    # A page missing layoutVersion falls back to legacy positioning and the grid collapses.
    for page in dashboard["pages"]:
        assert page.get("pageType") in ("PAGE_TYPE_CANVAS", "PAGE_TYPE_GLOBAL_FILTERS"), page
        assert page.get("layoutVersion") == "GRID_V1", page.get("displayName")


def test_exactly_one_global_filter_page(dashboard):
    globals_ = [p for p in dashboard["pages"]
                if p["pageType"] == "PAGE_TYPE_GLOBAL_FILTERS"]
    assert len(globals_) == 1


def test_every_row_fills_the_twelve_column_grid(dashboard):
    """A row that does not sum to 12 leaves a visible gap or overflows the canvas."""
    for page in dashboard["pages"]:
        rows = {}
        for entry in page["layout"]:
            pos = entry["position"]
            rows.setdefault(pos["y"], []).append(pos["width"])
        for y, widths in sorted(rows.items()):
            assert sum(widths) == 12, (
                f"page {page.get('displayName')!r} row y={y} sums to {sum(widths)}, not 12")


def test_widget_names_are_identifier_safe(dashboard):
    for _, widget, _ in widgets(dashboard):
        name = widget["name"]
        assert name.replace("-", "").replace("_", "").isalnum(), name
        assert len(name) <= 60, name


def test_counters_and_charts_are_tall_enough_to_read(dashboard):
    """A counter at height 2 clips its own value; charts need room for a legend."""
    for page, widget, pos in widgets(dashboard):
        spec = widget.get("spec") or {}
        kind = spec.get("widgetType")
        if kind == "counter":
            assert pos["height"] >= 3, f"{widget['name']} counter height {pos['height']}"
        elif kind in ("bar", "line", "area", "pie", "heatmap", "combo", "sankey"):
            assert pos["height"] >= 5, f"{widget['name']} chart height {pos['height']}"


# ============================================================ dashboard: widget specs


def test_every_widget_declares_the_correct_version_for_its_type(dashboard):
    """Wrong version is the number-one cause of a silently blank Lakeview widget."""
    for _, widget, _ in widgets(dashboard):
        spec = widget.get("spec")
        if not spec:
            continue  # a text widget uses multilineTextboxSpec and has no spec block
        kind = spec["widgetType"]
        assert kind in WIDGET_VERSIONS, f"unknown widget type {kind!r} ({widget['name']})"
        assert spec["version"] == WIDGET_VERSIONS[kind], (
            f"{widget['name']} is {kind} v{spec['version']}, "
            f"expected v{WIDGET_VERSIONS[kind]}")


def test_text_widgets_use_multiline_textbox_and_no_spec(dashboard):
    for _, widget, _ in widgets(dashboard):
        if "multilineTextboxSpec" in widget:
            assert "spec" not in widget, widget["name"]
            assert widget["multilineTextboxSpec"]["lines"], widget["name"]


def test_every_encoded_field_is_declared_in_its_query(dashboard):
    """A ``fieldName`` with no matching ``query.fields[].name`` renders
    "no selected fields to visualize" -- and nothing else tells you why."""
    for _, widget, _ in widgets(dashboard):
        spec = widget.get("spec")
        if not spec:
            continue
        declared = {
            f["name"]
            for q in widget.get("queries", [])
            for f in q["query"].get("fields", [])
        }
        referenced = set()

        def collect(node):
            if isinstance(node, dict):
                if "fieldName" in node and isinstance(node["fieldName"], str):
                    referenced.add(node["fieldName"])
                for value in node.values():
                    collect(value)
            elif isinstance(node, list):
                for item in node:
                    collect(item)

        collect(spec.get("encodings", {}))
        missing = referenced - declared
        assert not missing, f"{widget['name']} encodes undeclared field(s): {sorted(missing)}"


def test_every_widget_query_points_at_a_declared_dataset(dashboard):
    names = {d["name"] for d in dashboard["datasets"]}
    for _, widget, _ in widgets(dashboard):
        for q in widget.get("queries", []):
            ds = q["query"]["datasetName"]
            assert ds in names, f"{widget['name']} references unknown dataset {ds!r}"


def test_every_dataset_is_actually_used_by_a_widget(dashboard):
    used = {q["query"]["datasetName"]
            for _, widget, _ in widgets(dashboard)
            for q in widget.get("queries", [])}
    unused = {d["name"] for d in dashboard["datasets"]} - used
    assert not unused, f"datasets defined but never rendered: {sorted(unused)}"


def test_widgets_show_their_titles(dashboard):
    # An untitled chart on a seven-page dashboard is unreadable out of context.
    for _, widget, _ in widgets(dashboard):
        spec = widget.get("spec")
        if not spec:
            continue
        frame = spec.get("frame", {})
        assert frame.get("showTitle") is True, widget["name"]
        assert frame.get("title"), widget["name"]


# =========================================================== dashboard: dataset SQL


def test_query_lines_all_end_with_a_newline(dashboard):
    """``queryLines`` are joined verbatim: a line without a trailing newline welds itself to
    the next one, which can silently swallow a clause."""
    for ds in dashboard["datasets"]:
        for i, line in enumerate(ds["queryLines"]):
            assert line.endswith("\n"), f"{ds['name']} line {i} has no trailing newline"


def test_no_dataset_query_hardcodes_a_catalog_or_the_control_schema(dashboard):
    """A three-part name defeats ``dataset_catalog``/``dataset_schema`` and pins the dashboard
    to one workspace. ``system.*`` is the deliberate exception -- but the joins live in the
    views, so no dashboard query should need it either."""
    for ds in dashboard["datasets"]:
        sql = "".join(ds["queryLines"])
        assert "flowx." not in sql, f"{ds['name']} hardcodes the flowx catalog"
        if ds["name"] in FORECAST_DATASETS:
            # Allowed to read system.billing (see FORECAST_DATASETS), but nothing else, and
            # still never a hardcoded user catalog.
            for token in ("system.lakeflow", "system.access", "system.compute"):
                assert token not in sql, f"{ds['name']} reads {token}; use the views"
            continue
        assert "system." not in sql, (
            f"{ds['name']} reads a system table directly; join through the observability views")


def test_dataset_queries_only_read_known_observability_objects(dashboard):
    """Guards against a dataset quietly re-pointed at a table the views do not expose."""
    for ds in dashboard["datasets"]:
        sql = "".join(ds["queryLines"])
        for token in sql.replace("\n", " ").split():
            stripped = token.strip("(),")
            if stripped.startswith("v_"):
                assert stripped in KNOWN_VIEWS, f"{ds['name']} reads unknown view {stripped!r}"


def test_every_dataset_carries_dataflow_group_id_for_the_global_filter(dashboard):
    """The global filter only reaches datasets that expose the field it filters on."""
    for ds in dashboard["datasets"]:
        if ds["name"] in FORECAST_DATASETS:
            continue  # billing usage attributes to a pipeline/job id, not to a group
        sql = "".join(ds["queryLines"])
        assert "dataflow_group_id" in sql, ds["name"]


def test_percent_formatted_fields_are_fractions_not_hundreds(dashboard):
    """``number-percent`` multiplies by 100, so a 0-100 value renders as e.g. "9700%".

    Every percent-formatted encoding must therefore point at a ``_frac`` column, which the
    dataset SQL derives by dividing the view's ``_pct`` column by 100.
    """
    offenders = []
    for _, widget, _ in widgets(dashboard):
        spec = widget.get("spec")
        if not spec:
            continue

        def walk(node):
            if isinstance(node, dict):
                fmt = node.get("format")
                if isinstance(fmt, dict) and fmt.get("type") == "number-percent":
                    field = str(node.get("fieldName", ""))
                    if field.endswith("_pct") or "_pct)" in field:
                        offenders.append(f"{widget['name']}:{field}")
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(spec.get("encodings", {}))
    assert not offenders, f"number-percent applied to 0-100 values: {offenders}"


# ================================================================= genie space


def test_genie_space_declares_the_expected_schema_version(genie):
    assert genie["version"] == 2


def test_genie_tables_are_sorted_by_identifier(genie):
    """The API rejects an unsorted list: "data_sources.tables must be sorted by identifier"."""
    ids = [t["identifier"] for t in genie["data_sources"]["tables"]]
    assert ids == sorted(ids), "data_sources.tables is not sorted"


@pytest.mark.parametrize("path", [
    ("instructions", "text_instructions"),
    ("instructions", "example_question_sqls"),
    ("config", "sample_questions"),
])
def test_genie_repeated_blocks_are_sorted_by_id(genie, path):
    """Every repeated block must be id-sorted or the deploy fails with "must be sorted by id"."""
    node = genie
    for key in path:
        node = node[key]
    ids = [item["id"] for item in node]
    assert ids == sorted(ids), f"{'.'.join(path)} is not sorted by id"


def test_genie_benchmark_questions_are_sorted_by_id(genie):
    ids = [q["id"] for q in genie["benchmarks"]["questions"]]
    assert ids == sorted(ids)


#: The only raw system tables the Genie space may see. Forecasting needs an HOURLY spend
#: series and ``v_dataflow_cost`` is daily, so these two are a deliberate, narrow exception.
GENIE_ALLOWED_SYSTEM_TABLES = {"system.billing.usage", "system.billing.list_prices"}


def test_genie_points_only_at_curated_views_and_two_billing_tables(genie):
    """Pointing the space at ``system.lakeflow`` directly would let it answer platform-wide
    questions unrelated to FlowX and lose the dataflow_group_id framing. The billing pair is
    exempt because AI_FORECAST needs an hourly series the views do not expose."""
    for table in genie["data_sources"]["tables"]:
        ident = table["identifier"]
        if ident.startswith("system."):
            assert ident in GENIE_ALLOWED_SYSTEM_TABLES, (
                f"{ident} exposes a system table beyond the forecasting exception")
            continue
        assert ".observability." in ident or ".config." in ident, ident


def test_genie_instructions_carry_the_forecasting_caveats(genie):
    """A forecast presented without its caveats is the most confidently wrong answer available.

    The hourly-grain rule in particular is not a style preference: AI_FORECAST fitted to three
    daily points on a young workspace projected ten billion DBUs.
    """
    text = "".join(
        line for i in genie["instructions"]["text_instructions"] for line in i["content"])
    for phrase in ["AI_FORECAST", "HOURLY", "global_floor", "confidence band",
                   "projections, not commitments"]:
        assert phrase in text, f"forecasting instructions never mention {phrase!r}"


def test_genie_has_a_forecast_example_using_ai_forecast(genie):
    sqls = ["".join(e["sql"]) for e in genie["instructions"]["example_question_sqls"]]
    forecasts = [q for q in sqls if "AI_FORECAST(" in q]
    assert forecasts, "no AI_FORECAST example for the space to learn from"
    for sql in forecasts:
        # Hourly grain, current bucket excluded, and the floor that keeps the lower band >= 0.
        assert "date_trunc('HOUR'" in sql
        assert "global_floor" in sql


def test_genie_covers_every_observability_view(genie):
    idents = {t["identifier"].rsplit(".", 1)[-1] for t in genie["data_sources"]["tables"]}
    missing = KNOWN_VIEWS - idents
    assert not missing, f"Genie space cannot see: {sorted(missing)}"


def test_genie_strings_are_line_arrays_with_trailing_newlines(genie):
    """Every human-readable string in the format is an array of lines, not one string.

    Only the final element omits the trailing newline; getting this wrong produces a space
    whose instructions render as one unreadable run-on line.
    """
    blocks = [i["content"] for i in genie["instructions"]["text_instructions"]]
    blocks += [e["sql"] for e in genie["instructions"]["example_question_sqls"]]
    blocks += [a["content"] for q in genie["benchmarks"]["questions"] for a in q["answer"]]
    for block in blocks:
        assert isinstance(block, list) and block
        for line in block[:-1]:
            assert line.endswith("\n")
        assert not block[-1].endswith("\n")


def test_genie_instructions_encode_the_non_obvious_caveats(genie):
    """These are the traps that make an answer confidently wrong rather than merely wrong."""
    text = "".join(
        line for i in genie["instructions"]["text_instructions"] for line in i["content"])
    for phrase in [
        "dataflow_group_id",          # always report per group
        "is_full_refresh",            # exclude full refreshes from duration comparisons
        "is_retry",                   # retries are not throughput
        "estimated",                  # cost is list price
        "has_run_history",            # no recon rows is not health
        "name_match",                 # job attribution quality
        "feature_summary",            # quote it verbatim
    ]:
        assert phrase in text, f"instructions never mention {phrase!r}"


def test_genie_has_example_sql_and_sample_questions(genie):
    assert len(genie["instructions"]["example_question_sqls"]) >= 8
    assert len(genie["config"]["sample_questions"]) >= 8
    assert genie["benchmarks"]["questions"], "no benchmark questions to measure the space with"


def test_genie_example_sql_reads_only_sanctioned_objects(genie):
    for example in genie["instructions"]["example_question_sqls"]:
        sql = "".join(example["sql"])
        if "AI_FORECAST(" in sql:
            # Forecast examples build their hourly series from system.billing, and must not
            # reach any other system schema.
            assert "system.billing." in sql, example["question"]
            for banned in ("system.lakeflow", "system.access", "system.compute"):
                assert banned not in sql, example["question"]
            continue
        assert ".observability." in sql or ".config." in sql, example["question"]


def test_genie_sample_questions_include_the_documentation_ask(genie):
    """"Document this dataflow group" is the flagship ask; it must be discoverable in the UI."""
    questions = " ".join(
        line.lower()
        for q in genie["config"]["sample_questions"] for line in q["question"])
    assert "document" in questions
    assert "feature" in questions
