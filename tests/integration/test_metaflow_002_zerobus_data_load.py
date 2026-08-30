"""Post-deployment verification for Metaflow test scenario 002 ("Zerobus data load") --
``metaflow_testing/002_zerobus_data_load.json``, ``dataflow_group_id
dfg_metaflow_002_zerobus_bronze``.

This scenario's one ingestion flow (``df_excalibur_zerobus_ingest``) streams
``metaflow.Excalibur_usecase.zerobus_source_bus`` -- a plain Delta table standing in for a
Zerobus-style landed bus, seeded by
``notebooks/00_seed_sample_data/02_seed_metaflow_testing_data.py`` from
``sample_data/metaflow_testing/excalibur_usecase/zerobus_source_bus_batch1.csv`` -- into
``metaflow.bronze_excalibur.zerobus_bronze`` via ``source_type: "zerobus"``
(``ingestion/readers.py::read_zerobus_source``, a bare ``spark.readStream.format("delta")
.table(...)``, no file-based reader involved at all) with ``cdc_load_strategy: APPEND``.

**Which technical-metadata columns actually land on a zerobus-sourced table is the
non-obvious part this file locks in.** ``ingestion/technical_metadata.py::
attach_technical_metadata`` is called unconditionally for *every* ingestion flow regardless
of ``source_type`` (see ``notebooks/03_engine/03_lakeflow_declarative_pipeline.py``'s
``generate_ingestion_flow``) -- it is not autoloader-specific by construction. Its
``__framework_source_file_name``/``__framework_source_file_size``/``__framework_source_file_modification_time`` extractions
read Databricks' hidden ``_metadata`` struct, which (confirmed live, against this exact
table) resolves for a plain ``.table()`` read of a Delta table just as it does for a
file-based Auto Loader read -- because a Delta table's rows are still physically backed by
Parquet files, ``_metadata.file_name``/``file_size``/``file_modification_time`` resolve to
*that underlying file's* identity. So ``zerobus_bronze`` carries these four columns too,
populated with real (non-null) values pointing at ``zerobus_source_bus``'s own backing
Parquet file -- a naive assumption that "those are Auto-Loader-only" would get this wrong.
What genuinely does *not* carry over is ``_rescued_data``: that column is added by Auto
Loader itself as part of a ``cloudFiles`` read (``attach_technical_metadata``'s own docstring:
"added automatically by Auto Loader ... left untouched here"), and a Zerobus/plain-Delta
stream is not a ``cloudFiles`` read, so nothing ever introduces it here. Confirmed by directly
inspecting ``zerobus_bronze``'s live schema (``DataFrame.columns``/``printSchema()``) rather
than assumed from reading the framework source alone.

**Why there is no ``updated_at`` column here despite the source table having one.**
``zerobus_source_bus`` was originally seeded from ``zerobus_source_bus_batch1.csv``, which has
*only* ``customer_id``/``customer_name``/``status`` -- no ``updated_at``. This ingestion flow's
one and only stream materialization run happened against that original 3-column schema
(scenario 002's pipeline runs before scenario 003's reconciliation task in
``resources/metaflow_test_002_003_job.yml``'s task graph). ``updated_at`` only comes into
existence on ``zerobus_source_bus`` later, as a side effect of scenario 003's reconciliation
appending C006/C007 back into it with ``mergeSchema=true``
(``reconciliation/appender.py::append_missing_records``) -- see
``test_metaflow_003_autoload_recon_append.py`` for that half of the story. So
``zerobus_bronze`` -- materialized once, before that schema-evolving append ever happened --
correctly has no ``updated_at`` column at all.

Run ``databricks bundle run metaflow_test_002_003_job --target dev`` (see
``resources/metaflow_test_002_003_job.yml``) before running these -- these tests assert
against a table *already materialized* by that real run; Lakeflow Declarative Pipelines cannot
run locally (see the note in ``tests/conftest.py`` and the analogous docstring in
``tests/integration/test_pipeline1_scd.py``).

Row-count assertions here use a floor (``>=``), not an exact count: scenario 003's
reconciliation flow feeds C006/C007 back into ``zerobus_source_bus`` itself (not just its
bronze copy), so a *subsequent* run of this same pipeline's Zerobus stream will legitimately
pick those up too, growing ``zerobus_bronze`` from 5 rows to 7. A hardcoded ``== 5`` would
start failing the very next time this job chain runs end to end again -- exactly the kind of
brittle-to-reruns assertion ``tests/conftest.py`` and ``test_reconciliation.py`` warn against.
"""

CATALOG = "metaflow"
ZEROBUS_BRONZE = f"{CATALOG}.bronze_excalibur.zerobus_bronze"

BUSINESS_COLUMNS = {"customer_id", "customer_name", "status"}

# See module docstring: attach_technical_metadata() runs for every source_type, including
# zerobus -- these four are populated from Delta's own underlying-file _metadata, not just an
# Auto-Loader-only artifact. __framework_ingestion_timestamp_utc/__framework_pipeline_run_id/__framework_record_id
# are the framework-wide columns every ingestion flow gets regardless of source_type.
TECHNICAL_METADATA_COLUMNS = {
    "__framework_source_file_name",
    "__framework_source_file_size",
    "__framework_source_file_modification_time",
    "__framework_source_file_metadata_headers",
    "__framework_ingestion_timestamp_utc",
    "__framework_pipeline_run_id",
    "__framework_record_id",
}

# Confirmed live NOT present on zerobus_bronze -- see module docstring for why each is absent.
ABSENT_COLUMNS = {
    "_rescued_data",  # Auto-Loader-only, never introduced by a plain Delta stream read
    "updated_at",  # didn't exist on zerobus_source_bus yet when this table was first materialized
}

# The 5 seed rows every run of this scenario must have (from zerobus_source_bus_batch1.csv) --
# a floor, not the whole set, since a later run of scenario 002 can add C006/C007 on top (see
# module docstring).
SEED_ROWS = {
    "C001": ("Ava Thompson", "ACTIVE"),
    "C002": ("Noah Martinez", "ACTIVE"),
    "C003": ("Liam Chen", "ACTIVE"),
    "C004": ("Emma Wilson", "INACTIVE"),
    "C005": ("Olivia Brown", "ACTIVE"),
}


def test_zerobus_bronze_exists(table_exists):
    """Confirms the one ``source_type: "zerobus"`` ingestion flow actually registered and
    materialized a real streaming table -- not just that onboarding parsed the spec."""
    assert table_exists(ZEROBUS_BRONZE), f"expected {ZEROBUS_BRONZE} to exist after a real pipeline run"


def test_zerobus_bronze_has_expected_and_only_expected_technical_columns(spark):
    """Locks in the exact technical-metadata shape this source_type actually produces --
    both what's present (all four ``_metadata``-derived columns, framework-wide columns) and
    what's deliberately absent (``_rescued_data``, ``updated_at``). See module docstring for
    why a naive "zerobus == no file metadata" assumption would get the first half wrong."""
    columns = set(spark.table(ZEROBUS_BRONZE).columns)
    assert BUSINESS_COLUMNS <= columns, f"{ZEROBUS_BRONZE} missing expected business column(s): {BUSINESS_COLUMNS - columns}"
    assert TECHNICAL_METADATA_COLUMNS <= columns, (
        f"{ZEROBUS_BRONZE} missing expected technical metadata column(s): {TECHNICAL_METADATA_COLUMNS - columns}"
    )
    present_but_should_be_absent = ABSENT_COLUMNS & columns
    assert not present_but_should_be_absent, (
        f"{ZEROBUS_BRONZE} unexpectedly has column(s) {present_but_should_be_absent} -- "
        "see module docstring for why these should never appear on this table"
    )


def test_zerobus_bronze_row_count_and_seed_rows(spark):
    """Floor check (>= 5), not an exact count -- see module docstring: a subsequent run of
    this pipeline's Zerobus stream can legitimately add C006/C007 on top once scenario 003's
    reconciliation has fed them back into zerobus_source_bus."""
    df = spark.table(ZEROBUS_BRONZE)
    count = df.count()
    assert count >= 5, f"expected at least the 5 original seed rows in {ZEROBUS_BRONZE}, got {count}"

    rows = {row["customer_id"]: (row["customer_name"], row["status"]) for row in df.collect()}
    for customer_id, expected in SEED_ROWS.items():
        assert customer_id in rows, f"expected seed row {customer_id} to be present in {ZEROBUS_BRONZE}"
        assert rows[customer_id] == expected, (
            f"expected {customer_id} to be {expected} in {ZEROBUS_BRONZE}, got {rows[customer_id]}"
        )


def test_zerobus_bronze_framework_timestamp_and_run_id_are_populated(spark):
    """Column *presence* alone (asserted above) doesn't prove capture_technical_metadata
    actually ran for every row rather than defaulting everything to NULL -- spot-check that
    the two columns every ingested row must genuinely carry a real value for
    (``__framework_ingestion_timestamp_utc``, stamped by
    ``attach_framework_ingestion_timestamp``; ``__framework_pipeline_run_id``, stamped by
    ``dq/quarantine.py`` for traceability) are in fact non-null."""
    df = spark.table(ZEROBUS_BRONZE)
    null_timestamps = df.filter("__framework_ingestion_timestamp_utc IS NULL").count()
    null_run_ids = df.filter("__framework_pipeline_run_id IS NULL").count()
    assert null_timestamps == 0, f"expected every row to carry a non-null __framework_ingestion_timestamp_utc, found {null_timestamps} without one"
    assert null_run_ids == 0, f"expected every row to carry a non-null __framework_pipeline_run_id, found {null_run_ids} without one"


def test_zerobus_bronze_record_id_is_null(spark):
    """Contrasts with the timestamp/run-id check above: ``__framework_record_id`` is populated from
    ``dq_config.record_id_column`` (see ``dq/quarantine.py``), and this flow's
    ``dq_config`` (``{"rules": []}``) never sets ``record_id_column`` -- so every row's
    ``__framework_record_id`` must be NULL, not merely absent as a column."""
    df = spark.table(ZEROBUS_BRONZE)
    non_null_record_ids = df.filter("__framework_record_id IS NOT NULL").count()
    assert non_null_record_ids == 0, (
        f"expected every row's __framework_record_id to be NULL (no record_id_column configured for this flow), "
        f"found {non_null_record_ids} non-null"
    )
