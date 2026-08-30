"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import os, sys
from contextlib import contextmanager


try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    import pytest
except ImportError:
    raise ImportError(
        "Test dependencies not found.\n\nRun tests using 'uv run pytest'. See http://docs.astral.sh/uv to learn more about uv."
    )


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def volume_exists():
    """Provide a callable ``(path) -> bool`` for checking Unity Catalog Volume paths remotely.

    ``/Volumes/...`` paths are FUSE-mounted only on a Databricks cluster/driver -- never on
    this local dev machine, so a plain ``os.path.isdir("/Volumes/...")`` (an earlier pattern
    a few integration tests used) is always False here regardless of what actually exists
    remotely, silently turning every such check into a false pytest.skip rather than a real
    assertion. ``pyspark.dbutils.DBUtils(spark).fs.ls(...)``, the other tempting option, was
    tried and confirmed live to fail with a bare "Bad Request" from this local/Databricks
    Connect context. ``WorkspaceClient().files.list_directory_contents(path)`` (confirmed
    live) is the one that actually works: a plain REST call, independent of the Spark Connect
    session/dbutils shim entirely.

    Minimal example:
        def test_dir_exists(volume_exists):
            assert volume_exists("/Volumes/poc/egress/zips")
    """
    client = WorkspaceClient()

    def _exists(path: str) -> bool:
        try:
            client.files.list_directory_contents(path)
            return True
        except Exception:  # noqa: BLE001 - NotFound (and any other resolution failure) both mean "doesn't exist"
            return False

    return _exists


@pytest.fixture()
def volume_file_names():
    """Provide a callable ``(path) -> list[str]`` listing basenames directly under a Volume
    directory (non-recursive) -- see ``volume_exists`` for why this goes through the Files
    API rather than a local filesystem call.

    Minimal example:
        def test_archive_was_written(volume_file_names):
            names = volume_file_names("/Volumes/poc/egress/zips/heavy_usage_export")
            assert any(n.endswith(".zip.pgp") for n in names)
    """
    client = WorkspaceClient()

    def _list(path: str) -> list:
        return [entry.path.rstrip("/").rsplit("/", 1)[-1] for entry in client.files.list_directory_contents(path)]

    return _list


@pytest.fixture()
def table_exists():
    """Provide a callable ``(qualified_name) -> bool`` checking Unity Catalog table existence
    via a plain REST call (``WorkspaceClient().tables.get``), bypassing Spark/Spark-Connect's
    own catalog resolution entirely.

    ``spark.table(...)``/``spark.catalog.tableExists(...)`` (the obvious choice) were confirmed
    live to give a **stale, session-cached** answer for a table asserted *not* to exist: this
    project's ``spark`` fixture reuses one ``DatabricksSession`` for the whole pytest run
    (``pytest_configure`` warms it once, at collection time -- see below), and once that one
    session's Catalyst analyzer has resolved a table name, it appears to keep answering
    affirmatively even after the underlying object is genuinely gone (confirmed by cross-checking
    the exact same table three ways at once: ``spark.table(...)`` said it existed,
    ``SHOW TABLES`` said it existed, but ``databricks tables get`` -- the same REST call this
    fixture wraps -- correctly said it did not; a fresh, separately-constructed
    ``DatabricksSession`` in a brand-new process also correctly said it did not exist). This
    mirrors ``volume_exists``/``volume_file_names`` above, which hit the analogous problem for
    Volume paths and solved it the same way: skip the Spark Connect session's own cache, ask the
    control plane directly.

    Minimal example:
        def test_sink_never_materializes_a_table(table_exists):
            assert not table_exists("poc.silver_iot.raw_events_direct_sink")
    """
    client = WorkspaceClient()

    def _exists(qualified_name: str) -> bool:
        try:
            client.tables.get(qualified_name)
            return True
        except Exception:  # noqa: BLE001 - NotFound (and any other resolution failure) both mean "doesn't exist"
            return False

    return _exists


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    with _allow_stderr_output(config):
        try:
            _enable_fallback_compute()

            # Initialize Spark session eagerly, so it is available even when
            # SparkSession.builder.getOrCreate() is used. For DB Connect 15+,
            # we validate version compatibility with the remote cluster.
            if hasattr(DatabricksSession.builder, "validateSession"):
                DatabricksSession.builder.validateSession().getOrCreate()
            else:
                DatabricksSession.builder.getOrCreate()
        except Exception as e:
            # Databricks Connect requires active workspace credentials.
            # For offline/local unit tests that do not need Spark, proceed gracefully.
            pass
