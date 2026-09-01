"""Post-deployment verification for Metaflow test scenario 001 ("ZIP file onboarding") --
``metaflow_testing/001_zip_file_onboarding.json``, ``dataflow_group_id
dfg_metaflow_001_zip_join_export``.

This scenario is the first end-to-end proof (against the new, dedicated ``metaflow`` catalog)
that a single Lakeflow Declarative Pipeline update can chain all three stages the framework
supports -- ingestion, transformation, and sink export -- with ZIP handling on *both* ends:

* Four ingestion flows (``df_ea_departments_ingest``, ``df_ea_employees_ingest``,
  ``df_ea_projects_ingest``, ``df_ea_assignments_ingest``), each a plain ``streaming_table``
  Auto Loader read, but each source file arrives zipped: ``source_config.source_zip_handling``
  unpacks one archive (e.g. ``ea_departments.zip``) from
  ``/Volumes/metaflow/EA_usecase/landing_zip/incoming/`` into its own
  ``target_volume_path`` under ``.../landing_zip/extracted/<table>/`` before Auto Loader ever
  sees a CSV. If ``source_zip_handling`` silently no-op'd (wrong pattern, extraction skipped),
  the corresponding ``bronze_ea`` table below would simply never populate.
* One transformation flow (``ts_ea_joined_export``) doing a real 4-way ``JOIN`` across all
  four Bronze tables (assignments as the streaming driver, employees/departments/projects as
  static lookups) with ``target_type: "external_sink"`` -- meaning it must do *both* halves at
  once: materialize a genuine queryable table (``silver_ea.ea_joined_assignments``) **and**
  export it via ``sink_config.format: "pgp_zip"``. That format name is deliberately misleading
  here: ``post_export_archive.pgp_encryption`` is omitted from this spec on purpose, so
  ``archive/pgp_zip_sink.py`` writes a plain, unencrypted ``.zip`` -- not a ``.zip.pgp`` -- to
  ``/Volumes/metaflow/egress_ea/export_zips/output/``. (Contrast with
  ``tests/integration/test_lakeflow_sink_dag.py``'s ``heavy_usage_export`` scenario, which
  *does* configure ``pgp_encryption`` and asserts a ``.zip.pgp`` suffix -- this scenario is the
  negative case: same sink format, encryption knob deliberately left off.)

The join itself is an ``INNER JOIN``, not an outer one, which this file's row-level assertions
exist specifically to prove: the seed data has 5 employees but only 4 assignment rows -- E005
"Olivia Brown" (Operations dept) has no row in ``assignments_raw`` at all. An inner join
correctly drops her from the output entirely; an accidentally-outer join would instead produce
a 6th row with null ``project_id``/``hours_allocated``. Asserting the output is exactly 5 rows
*and* that E005/Olivia Brown appears nowhere in it is what actually distinguishes those two
cases -- a bare row-count check alone would not (an outer join bug that also, coincidentally,
dropped a different legitimate row would still total 5).

Run ``databricks bundle run metaflow_test_001_job --target dev`` (see
``resources/feature_tests/metaflow_test_001_job.yml``, which chases through
``setup_control_tables -> seed_metaflow_testing_data -> onboard_001 -> run_pipeline_update``)
before running these -- these tests assert against tables/Volume paths *already materialized*
by that real run; Lakeflow Declarative Pipelines cannot run locally (see the note in
``tests/conftest.py`` and the analogous docstring in ``tests/integration/test_pipeline1_scd.py``).
"""

CATALOG = "metaflow"
BRONZE_SCHEMA = f"{CATALOG}.bronze_ea"
SILVER_SCHEMA = f"{CATALOG}.silver_ea"

DEPARTMENTS_RAW = f"{BRONZE_SCHEMA}.departments_raw"
EMPLOYEES_RAW = f"{BRONZE_SCHEMA}.employees_raw"
PROJECTS_RAW = f"{BRONZE_SCHEMA}.projects_raw"
ASSIGNMENTS_RAW = f"{BRONZE_SCHEMA}.assignments_raw"
JOINED_TABLE = f"{SILVER_SCHEMA}.ea_joined_assignments"

EXPORT_ZIP_DIR = f"/Volumes/{CATALOG}/egress_ea/export_zips/output/"

# Every ingestion flow in this spec sets capture_technical_metadata: true, so every bronze_ea
# table should carry the same standard framework columns alongside its own business columns.
TECHNICAL_METADATA_COLUMNS = {
    "_rescued_data",
    "__framework_source_file_name",
    "__framework_source_file_size",
    "__framework_source_file_modification_time",
    "__framework_source_file_metadata_headers",
    "__framework_ingestion_timestamp_utc",
    "__framework_pipeline_run_id",
    "__framework_record_id",
}

# The 5 (employee_id, project_id) pairs an INNER JOIN of assignments_raw x employees_raw x
# departments_raw x projects_raw must produce -- see module docstring for why E005/Olivia
# Brown (no assignments_raw row) is deliberately absent from this set.
EXPECTED_JOIN_PAIRS = {
    ("E001", "P001"),
    ("E002", "P001"),
    ("E002", "P002"),
    ("E003", "P003"),
    ("E004", "P004"),
}

JOINED_TABLE_COLUMNS = {
    "employee_id",
    "project_id",
    "hours_allocated",
    "employee_name",
    "role",
    "dept_id",
    "dept_name",
    "region",
    "project_name",
    "budget",
    "__framework_ingestion_timestamp_utc",
    "__framework_pipeline_run_id",
    "__framework_record_id",
}


def _assert_bronze_shape(spark, qualified_table: str, business_columns: set, expected_row_count: int):
    """Shared shape check for the four ZIP-sourced bronze_ea tables: exact row count (proves
    source_zip_handling extracted the whole archive, not a partial/duplicated read) plus both
    the table's own business columns and the standard technical-metadata columns every
    capture_technical_metadata: true ingestion flow should attach."""
    df = spark.table(qualified_table)
    assert df.count() == expected_row_count, (
        f"expected {qualified_table} to have {expected_row_count} row(s) after ZIP extraction + Auto Loader ingest, "
        f"got {df.count()}"
    )
    columns = set(df.columns)
    assert business_columns <= columns, f"{qualified_table} missing expected business column(s): {business_columns - columns}"
    assert TECHNICAL_METADATA_COLUMNS <= columns, (
        f"{qualified_table} missing expected technical metadata column(s): {TECHNICAL_METADATA_COLUMNS - columns}"
    )


# ---------------------------------------------------------------------------
# Bronze ingestion (4 ZIP-sourced Auto Loader tables)
# ---------------------------------------------------------------------------


def test_all_four_bronze_ea_tables_exist(table_exists):
    """Confirms all four source_zip_handling ingestion flows registered real tables -- if
    onboarding or the pipeline update partially failed, one or more of these could be missing
    while the others succeeded."""
    for table in (DEPARTMENTS_RAW, EMPLOYEES_RAW, PROJECTS_RAW, ASSIGNMENTS_RAW):
        assert table_exists(table), f"expected {table} to exist after a real pipeline run"


def test_departments_raw_shape(spark):
    _assert_bronze_shape(spark, DEPARTMENTS_RAW, {"dept_id", "dept_name", "region"}, expected_row_count=4)


def test_employees_raw_shape(spark):
    _assert_bronze_shape(
        spark, EMPLOYEES_RAW, {"employee_id", "dept_id", "employee_name", "role"}, expected_row_count=5
    )


def test_projects_raw_shape(spark):
    _assert_bronze_shape(
        spark, PROJECTS_RAW, {"project_id", "dept_id", "project_name", "budget"}, expected_row_count=4
    )


def test_assignments_raw_shape(spark):
    _assert_bronze_shape(
        spark, ASSIGNMENTS_RAW, {"employee_id", "project_id", "hours_allocated"}, expected_row_count=5
    )


# ---------------------------------------------------------------------------
# Silver transformation (4-way INNER JOIN, target_type: "external_sink")
# ---------------------------------------------------------------------------


def test_joined_table_exists(table_exists):
    assert table_exists(JOINED_TABLE), (
        f"expected {JOINED_TABLE} to exist -- target_type: 'external_sink' must still materialize "
        "a real main table in addition to the ZIP export"
    )


def test_joined_table_has_expected_columns(spark):
    df = spark.table(JOINED_TABLE)
    columns = set(df.columns)
    assert JOINED_TABLE_COLUMNS <= columns, f"{JOINED_TABLE} missing expected column(s): {JOINED_TABLE_COLUMNS - columns}"


def test_joined_table_row_count_is_five(spark):
    """The core inner-join proof, part 1: exactly 5 rows, not 6 -- 5 employees exist but only
    4 assignments do, so E005 "Olivia Brown" (no assignments_raw row) must be excluded rather
    than appearing with null project_id/hours_allocated the way a LEFT/OUTER join would
    produce."""
    df = spark.table(JOINED_TABLE)
    assert df.count() == 5, f"expected exactly 5 rows from the INNER JOIN, got {df.count()}"


def test_joined_table_contains_exactly_the_expected_employee_project_pairs(spark):
    """The core inner-join proof, part 2: not just *a* count of 5, but the *specific* 5
    (employee_id, project_id) pairs the assignments_raw fixture actually contains -- rules out
    a coincidental row count from, say, a broken join key silently producing a different set
    of 5 rows via a fan-out/fan-in that happens to total 5."""
    rows = spark.table(JOINED_TABLE).select("employee_id", "project_id").collect()
    pairs = {(row["employee_id"], row["project_id"]) for row in rows}
    assert pairs == EXPECTED_JOIN_PAIRS, f"expected {EXPECTED_JOIN_PAIRS}, got {pairs}"


def test_joined_table_excludes_e005_and_olivia_brown_entirely(spark):
    """Direct negative assertion complementing the two tests above: E005/Olivia Brown must not
    appear under any identifier, in case a bug substituted a different employee_id for her row
    rather than dropping it outright."""
    df = spark.table(JOINED_TABLE)
    assert df.filter("employee_id = 'E005'").count() == 0, "E005 must not appear in an INNER JOIN output -- she has no assignment"
    assert (
        df.filter("employee_name = 'Olivia Brown'").count() == 0
    ), "Olivia Brown must not appear in an INNER JOIN output -- she has no assignment"


def test_joined_table_row_content_matches_source_tables(spark):
    """Spot-checks that the join actually pulled the right attributes from the right side
    tables (not just the right row count/keys) -- E001/P001 should carry Ava Thompson's
    employee attributes, her department's (Engineering/US), and that project's own attributes
    (Platform Migration / 250000), proving all three JOIN conditions
    (employee_id, dept_id, project_id) resolved correctly together."""
    row = spark.table(JOINED_TABLE).filter("employee_id = 'E001' AND project_id = 'P001'").collect()[0]
    assert row["employee_name"] == "Ava Thompson"
    assert row["role"] == "Engineer"
    assert row["dept_id"] == "D001"
    assert row["dept_name"] == "Engineering"
    assert row["region"] == "US"
    assert row["project_name"] == "Platform Migration"
    assert row["budget"] == 250000
    assert row["hours_allocated"] == 120


# ---------------------------------------------------------------------------
# Sink export (target_type: "external_sink", sink_config.format: "pgp_zip", encryption omitted)
# ---------------------------------------------------------------------------


def test_export_zip_archive_exists_in_output_volume(volume_exists, volume_file_names):
    """Confirms the second half of target_type: "external_sink" actually ran: a real ZIP
    archive was written to sink_config.post_export_archive.output_zip_path. Matches by
    prefix/suffix (``ea_joined_export_`` / ``.zip``) rather than the exact
    ``ea_joined_export_0.zip`` filename observed during manual verification, since the
    ``{batch_id}`` suffix in export_file_name_format would legitimately differ on a re-run of
    this pipeline (e.g. a later batch than 0)."""
    assert volume_exists(EXPORT_ZIP_DIR), f"expected {EXPORT_ZIP_DIR} to exist after a real pipeline run"
    names = volume_file_names(EXPORT_ZIP_DIR)
    matches = [n for n in names if n.startswith("ea_joined_export_") and n.endswith(".zip")]
    assert matches, f"expected an 'ea_joined_export_*.zip' archive under {EXPORT_ZIP_DIR}, found: {names}"


def test_export_zip_archive_is_not_pgp_encrypted(volume_exists, volume_file_names):
    """Locks in the deliberate quirk this scenario exercises: sink_config.format is
    "pgp_zip" but post_export_archive.pgp_encryption is omitted from
    metaflow_testing/001_zip_file_onboarding.json on purpose, so archive/pgp_zip_sink.py
    should write a plain '.zip' here -- never a '.zip.pgp' -- despite the format name.
    Contrast with tests/integration/test_lakeflow_sink_dag.py's heavy_usage_export scenario,
    which *does* configure encryption and asserts the opposite (.zip.pgp present)."""
    if not volume_exists(EXPORT_ZIP_DIR):
        return
    names = volume_file_names(EXPORT_ZIP_DIR)
    pgp_wrapped = [n for n in names if n.endswith(".zip.pgp")]
    assert not pgp_wrapped, f"expected no PGP-encrypted archives (encryption was omitted from the spec), found: {pgp_wrapped}"
