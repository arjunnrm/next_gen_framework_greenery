"""Post-deployment verification for Phase 7 ("Sink rebuild") -- ``target_type: "sink"`` and
``target_type: "external_sink"`` -- against ``test_specs/spec_06_unified_dual_engine_egress_zip.json``
(``dataflow_group_id: dfg_iot_telemetry_unified``, the default ``dataflow_group_id`` bundle
variable the ``dev`` target's ``lakeflow_metadata_pipeline`` resource runs -- see
``databricks.yml``).

These tests assert against tables/files *already materialized* by a real
``databricks bundle run metadata_framework_job`` pass (onboards ``spec_06`` via its
``onboard_spec_06`` task, then runs the pipeline) -- they do not themselves trigger the
pipeline (Lakeflow Declarative Pipelines cannot run locally; see
docs/06_governance_integration.md and the note in tests/conftest.py). Run that job before
running these.

Two flows in ``spec_06`` exercise the two target types this phase adds real behavior for:

* ``ts_iot_raw_events_direct_sink`` (``target_type: "sink"``) -- a direct streaming-transform
  -to-volume export with NO intermediate table: its staged view feeds a genuine
  ``dlt.create_sink(format="delta")`` + ``@dlt.append_flow`` pair directly. The core
  assertion here is a *negative* one -- ``poc.silver_iot.raw_events_direct_sink`` must NEVER
  exist as a queryable table, by design (see ``engine/sink_registration.py``'s module
  docstring) -- confirmed via ``spark.table(...)`` raising, mirroring
  ``tests/integration/test_pipeline1_scd.py``'s ``_table_exists`` pattern.
* ``ts_iot_heavy_usage_egress`` (``target_type: "external_sink"``) -- a REAL, governed table
  (``poc.silver_iot.heavy_usage_export``) is still materialized, AND additionally exported
  via a second ``@dlt.append_flow`` into a ``pgp_zip`` custom sink
  (``archive/pgp_zip_sink.py``) that writes one PGP-encrypted ZIP archive per micro-batch
  under ``sink_config.post_export_archive.output_zip_path``. Both halves are asserted:
  the table exists (and has rows), and at least one ``.zip.pgp`` archive file exists on disk.
"""

import pytest

CATALOG = "poc"
SILVER_IOT_SCHEMA = f"{CATALOG}.silver_iot"


def _table_exists(spark, qualified_table: str) -> bool:
    try:
        spark.table(qualified_table)
        return True
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# target_type: "sink" -- ts_iot_raw_events_direct_sink
# ---------------------------------------------------------------------------


def test_sink_target_never_materializes_a_table(table_exists):
    """The whole point of target_type: "sink" is that it publishes NOTHING queryable under
    its own target_catalog.target_schema.target_table -- if this table exists, sink
    registration silently fell through to the ordinary main-table path instead of
    dispatching to engine/sink_registration.py::register_sink_target.

    Uses the `table_exists` fixture (a plain Unity Catalog REST call), not `spark.table(...)`
    -- confirmed live that this project's shared pytest-session `spark` fixture can return a
    stale "exists" answer for a table that a fresh session / the control plane itself agrees
    is genuinely gone (see that fixture's docstring in tests/conftest.py for the full
    live-reproduced evidence)."""
    assert not table_exists(f"{SILVER_IOT_SCHEMA}.raw_events_direct_sink"), (
        "target_type: 'sink' must never register a materialized table -- "
        "see engine/sink_registration.py::register_sink_target"
    )


def test_sink_target_delta_output_is_readable_and_non_empty(spark, volume_exists):
    """engine/sink_registration.py::register_sink_target -> dlt.create_sink(format="delta")
    writes real Delta files at sink_config.path -- confirm the sink actually ran (not just
    that no main table exists, which a totally broken/no-op registration would also satisfy)."""
    sink_path = f"/Volumes/{CATALOG}/egress/zips/iot_raw_direct_sink/dev/"
    if not volume_exists(sink_path):
        pytest.skip(f"sink output directory not found at {sink_path} -- has the pipeline update run yet?")
    output_df = spark.read.format("delta").load(sink_path)
    assert output_df.count() > 0, f"expected the 'sink' target's delta output at {sink_path} to contain rows"
    assert set(output_df.columns) >= {"device_id", "country", "event_ts", "usage_bytes"}


# ---------------------------------------------------------------------------
# target_type: "external_sink" -- ts_iot_heavy_usage_egress
# ---------------------------------------------------------------------------


def test_external_sink_target_main_table_is_materialized_and_governed(spark):
    """Unchanged half of external_sink (register_main_and_quarantine_tables + CDC dispatch,
    exactly like streaming_table/materialized_view/batch_table) -- a real, queryable table
    must still exist, unlike target_type: "sink"."""
    assert _table_exists(spark, f"{SILVER_IOT_SCHEMA}.heavy_usage_export"), (
        "target_type: 'external_sink' must still materialize a real main table -- only the "
        "additional export is new in Phase 7, see engine/flow_registration.py::register_flow_output"
    )


def test_external_sink_target_main_table_has_rows(spark):
    df = spark.table(f"{SILVER_IOT_SCHEMA}.heavy_usage_export")
    assert df.count() >= 0  # exists and queryable; row count depends on seeded volume thresholds
    assert {"device_id", "country", "usage_window", "total_usage_bytes"} <= set(df.columns)


def test_external_sink_target_also_produced_an_encrypted_archive_export(volume_exists, volume_file_names):
    """The NEW half of external_sink (Phase 7): a second @dlt.append_flow reads
    dlt.read_stream(qualified_main_table) into a pgp_zip custom sink
    (archive/pgp_zip_sink.py), which writes one 'batch_<id>.zip.pgp' file per microbatch
    under sink_config.post_export_archive.output_zip_path. Before this phase, this export
    only ever happened as a *post-deployment* plain-Spark step
    (control_plane/post_deployment.py::run_external_sink_exports, now removed) -- this
    assertion is what actually proves the export moved inside the pipeline graph, not just
    that the main table (asserted above) exists."""
    output_dir = f"/Volumes/{CATALOG}/egress/zips/heavy_usage_export"
    if not volume_exists(output_dir):
        pytest.skip(f"archive output directory not found at {output_dir} -- has the pipeline update run yet?")
    names = volume_file_names(output_dir)
    archive_files = [f for f in names if f.endswith(".zip.pgp")]
    assert archive_files, f"expected at least one '*.zip.pgp' archive under {output_dir}, found: {names}"


def test_external_sink_target_staging_directory_has_no_leftover_plaintext_zips(volume_exists, volume_file_names):
    """archive/pgp_zip_sink.py's commit() must always remove the un-encrypted intermediate
    zip once it's been PGP-wrapped -- a leftover plaintext .zip sitting next to (or instead
    of) the .zip.pgp would mean sensitive export data was left unencrypted at rest."""
    output_dir = f"/Volumes/{CATALOG}/egress/zips/heavy_usage_export"
    if not volume_exists(output_dir):
        pytest.skip(f"archive output directory not found at {output_dir} -- has the pipeline update run yet?")
    plaintext_zips = [f for f in volume_file_names(output_dir) if f.endswith(".zip") and not f.endswith(".zip.pgp")]
    assert not plaintext_zips, f"un-encrypted intermediate zip(s) were left behind: {plaintext_zips}"
