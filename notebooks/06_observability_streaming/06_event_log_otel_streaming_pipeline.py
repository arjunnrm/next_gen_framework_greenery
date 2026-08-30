# Databricks notebook source
# MAGIC %md
# MAGIC # Event Log OTel Streaming Pipeline
# MAGIC
# MAGIC A genuinely continuous Lakeflow Declarative Pipeline (``continuous: true`` -- see
# MAGIC `resources/observability_otel_streaming_pipeline.yml`): reads N event-log tables (each a
# MAGIC real Unity Catalog Delta table one *source* pipeline publishes its own event log to --
# MAGIC see https://learn.microsoft.com/en-us/azure/databricks/ldp/observability) as streaming
# MAGIC sources, unions them tagging every row with a `source_pipeline` column, and continuously
# MAGIC exports the unified stream to an OpenTelemetry OTLP/HTTP endpoint via a genuine
# MAGIC `dlt.create_sink`/`@dlt.append_flow` pair -- gated by an `otel_export_enabled` flag --
# MAGIC and, since v1.3.0, additionally to a Databricks Volume path per continuous
# MAGIC `DATABRICKS_VOLUME` destination declared in `observability_config`.
# MAGIC
# MAGIC ## Where this pipeline's configuration comes from (v1.3.0)
# MAGIC
# MAGIC Originally this pipeline was deliberately **not** control-table-driven: it is a "meta"
# MAGIC pipeline observing *other* pipelines' event logs, and it was configured entirely via its
# MAGIC own deploy-time `configuration:` block (`dataflow.otel_streaming.*` keys), read with the
# MAGIC same `spark.conf.get(...)` mechanism `03_lakeflow_declarative_pipeline.py` uses for
# MAGIC `dataflow.group.id`/`dataflow.control.catalog`.
# MAGIC
# MAGIC v1.3.0 adds an **optional, higher-precedence** control-table source for the table list, so
# MAGIC the same onboarding spec that declares a pipeline's flows can also declare where its
# MAGIC telemetry goes. When `dataflow.group.id` **and** `dataflow.control.catalog` are set on
# MAGIC this pipeline, it reads `<catalog>.config.observability_config`, keeps the rows whose
# MAGIC `mode` is `continuous`, and streams the union of their
# MAGIC `destination_config.event_log_tables`. When that yields nothing -- no such rows, no
# MAGIC configured group, or the control table simply isn't readable from this pipeline's
# MAGIC identity -- it **falls back** to the original `dataflow.otel_streaming.event_log_tables`
# MAGIC JSON array, unchanged. An existing deployment that sets neither new key therefore behaves
# MAGIC exactly as before; the fallback is permanent and fully supported, not a deprecation path.
# MAGIC
# MAGIC The control table is only ever *read*, and only for configuration. Nothing here writes to
# MAGIC the control plane, and a missing/unreadable `observability_config` degrades to the
# MAGIC fallback rather than failing an always-on pipeline.
# MAGIC
# MAGIC Also unrelated to, and does not modify, the existing batch `observability` engine
# MAGIC (`notebooks/08_observability/08_dlt_observability_engine.py` +
# MAGIC `event_log_extractor.py`/`otel_payload_builder.py::build_resource_logs`) -- that engine
# MAGIC is a one-shot post-update export for exactly one pipeline's own event log; this one is a
# MAGIC standing, always-on export unioning arbitrarily many *other* pipelines' event logs. The
# MAGIC only code shared between the two paths is the low-level OTel-shape helpers inside
# MAGIC `observability/otel_payload_builder.py` and `observability/destination_dispatcher.py`'s
# MAGIC `dispatch_to_otlp` -- see `observability/otel_streaming_sink.py`'s module docstring.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap
# MAGIC
# MAGIC Lakeflow Declarative Pipeline source notebooks do not support `%run`. In production,
# MAGIC attach `NextGen_Metadata_Framework`'s wheel to this pipeline via
# MAGIC `resources/observability_otel_streaming_pipeline.yml`'s `environment.dependencies` --
# MAGIC once installed that way, a plain `import` resolves it like any other site-packages
# MAGIC library. The fallback below only kicks in for local, wheel-less notebook development.

# COMMAND ----------

import json
import logging
import os
import re
import sys
from typing import List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("event_log_otel_streaming_pipeline")

try:
    import NextGen_Metadata_Framework.lakeflow_framework  # noqa: F401
except ImportError:
    try:
        this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        dev_src_root = os.path.abspath(os.path.join(this_dir, "..", "..", "src"))
        if dev_src_root not in sys.path:
            sys.path.insert(0, dev_src_root)
        import NextGen_Metadata_Framework.lakeflow_framework  # noqa: F401
        logger.warning("Loaded 'NextGen_Metadata_Framework' from local 'src/' (dev fallback) -- not from an installed wheel.")
    except ImportError as exc:
        raise ImportError(
            "Could not import 'NextGen_Metadata_Framework'. In production this must be attached as a "
            "wheel library (see resources/observability_otel_streaming_pipeline.yml); for local "
            f"development, run from within the repo so '../../src' resolves. Original error: {exc}"
        ) from exc

import dlt  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import (  # noqa: E402
    FrameworkConfigError,
    ObservabilityConfigError,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.config_loader import (  # noqa: E402
    DestinationConfig,
    filter_destinations_by_mode,
    load_destination_configs,
    resolve_event_log_tables,
)
from NextGen_Metadata_Framework.lakeflow_framework.observability.otel_streaming_sink import (  # noqa: E402
    OtelStreamingDataSource,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Resolution
# MAGIC
# MAGIC Two sources for the table list, tried in this order:
# MAGIC
# MAGIC 1. **`observability_config` rows with `mode == "continuous"`** for this pipeline's
# MAGIC    configured `dataflow.group.id`, read from `<dataflow.control.catalog>.config`. Their
# MAGIC    `destination_config.event_log_tables` arrays are unioned (de-duplicated,
# MAGIC    order-preserving) by `config_loader.resolve_event_log_tables`. Requires **both** new
# MAGIC    `configuration:` keys to be set; when either is absent this source is skipped without
# MAGIC    an error, because a pre-v1.3.0 deployment legitimately has neither.
# MAGIC 2. **`dataflow.otel_streaming.event_log_tables`** -- the original fallback, a JSON-encoded
# MAGIC    array (a Spark pipeline `configuration:` value is always a string, so the list of N
# MAGIC    source event-log tables is encoded/decoded as JSON, the same convention as every other
# MAGIC    JSON-shaped config column in this framework, e.g. `target_config_json`).
# MAGIC
# MAGIC A malformed value in source 2 still raises -- silently falling through a typo'd JSON array
# MAGIC to "nothing configured" would hide the operator's actual mistake. Only *absence* falls
# MAGIC through.

# COMMAND ----------

GROUP_ID = spark.conf.get("dataflow.group.id", None)
CONTROL_CATALOG = spark.conf.get("dataflow.control.catalog", None)
EVENT_LOG_TABLES_JSON = spark.conf.get("dataflow.otel_streaming.event_log_tables", None)
OTEL_ENDPOINT = spark.conf.get("dataflow.otel_streaming.otel_endpoint", None)
OTEL_EXPORT_ENABLED_RAW = spark.conf.get("dataflow.otel_streaming.otel_export_enabled", "false")


def _load_continuous_destinations() -> List[DestinationConfig]:
    """Continuous-mode destinations for this pipeline's `dataflow.group.id`, or `[]`.

    Never raises. This is an always-on pipeline: a control table that is missing, not yet
    provisioned, or unreadable by this pipeline's run-as identity must degrade to the
    `dataflow.otel_streaming.*` fallback, not take down a standing telemetry export. A genuine
    misconfiguration still surfaces -- as the "nothing to stream" error below, which names both
    sources -- rather than as an opaque table-not-found deep inside graph resolution.
    """
    if not GROUP_ID or not CONTROL_CATALOG:
        logger.info(
            "Pipeline configuration 'dataflow.group.id' (%r) and/or 'dataflow.control.catalog' (%r) is not set -- "
            "skipping the observability_config lookup and using the 'dataflow.otel_streaming.event_log_tables' "
            "configuration instead. This is the expected shape for a deployment made before v1.3.0.",
            GROUP_ID,
            CONTROL_CATALOG,
        )
        return []
    try:
        return filter_destinations_by_mode(load_destination_configs(spark, CONTROL_CATALOG, GROUP_ID), "continuous")
    except ObservabilityConfigError as exc:
        logger.warning(
            "Could not read continuous destinations from '%s.config.observability_config' for "
            "dataflow_group_id='%s' (%s) -- falling back to the 'dataflow.otel_streaming.event_log_tables' "
            "pipeline configuration.",
            CONTROL_CATALOG,
            GROUP_ID,
            exc,
        )
        return []


def _event_log_tables_from_pipeline_configuration() -> List[str]:
    """Decode the `dataflow.otel_streaming.event_log_tables` fallback, or `[]` when unset."""
    if not EVENT_LOG_TABLES_JSON:
        return []
    try:
        decoded = json.loads(EVENT_LOG_TABLES_JSON)
    except json.JSONDecodeError as exc:
        raise FrameworkConfigError(
            f"'dataflow.otel_streaming.event_log_tables' is not valid JSON: {exc}"
        ) from exc
    if not isinstance(decoded, list) or not decoded:
        raise FrameworkConfigError(
            "'dataflow.otel_streaming.event_log_tables' must decode to a non-empty JSON array of table names, "
            f"got {decoded!r}."
        )
    return decoded


CONTINUOUS_DESTINATIONS = _load_continuous_destinations()
# Resolved into its own name (rather than inlined into the `or`) so the log line below can state
# which of the two sources actually won without re-running the resolution and re-emitting its
# own log line.
CONTROL_TABLE_EVENT_LOG_TABLES = resolve_event_log_tables(CONTINUOUS_DESTINATIONS)
EVENT_LOG_TABLES = CONTROL_TABLE_EVENT_LOG_TABLES or _event_log_tables_from_pipeline_configuration()

if not EVENT_LOG_TABLES:
    raise FrameworkConfigError(
        f"No continuous observability destinations found in observability_config for dataflow_group_id='{GROUP_ID}' "
        "and no 'dataflow.otel_streaming.event_log_tables' pipeline configuration was set -- there is nothing to "
        "stream. Onboard a destination with mode: 'continuous' and destination_config.event_log_tables, or set the "
        "pipeline configuration key."
    )

OTEL_EXPORT_ENABLED = str(OTEL_EXPORT_ENABLED_RAW).strip().lower() == "true"

logger.info(
    "Configuration resolved: %d event_log_tables=%s (from %s), %d continuous destination(s)=%s, "
    "otel_export_enabled=%s, otel_endpoint=%s",
    len(EVENT_LOG_TABLES),
    EVENT_LOG_TABLES,
    "observability_config" if CONTROL_TABLE_EVENT_LOG_TABLES else "dataflow.otel_streaming.event_log_tables",
    len(CONTINUOUS_DESTINATIONS),
    [(d.destination_id, d.destination_type) for d in CONTINUOUS_DESTINATIONS],
    OTEL_EXPORT_ENABLED,
    OTEL_ENDPOINT,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Table Streaming Views
# MAGIC
# MAGIC One `@dlt.view` per configured event-log table: a plain streaming read tagged with
# MAGIC `source_pipeline` (the table's own fully-qualified name, since this framework configures
# MAGIC exactly one event-log table per source pipeline -- see
# MAGIC `resources/observability_otel_streaming_pipeline.yml`'s header comment). Registered via a
# MAGIC helper function called once per table (not a `for` loop building closures directly) so
# MAGIC each view's `table_name` is bound to that call's own local variable -- a bare loop-body
# MAGIC closure here would otherwise suffer Python's classic late-binding-in-a-loop bug, where
# MAGIC every registered view ends up reading whichever table happened to be *last* in the list.
# MAGIC This mirrors `notebooks/03_engine/03_lakeflow_declarative_pipeline.py`'s own
# MAGIC `generate_ingestion_flow(flow_row)` / `generate_transformation_flow(flow_row)` pattern,
# MAGIC for the same reason.

# COMMAND ----------

_NON_IDENTIFIER_CHARS = re.compile(r"[^0-9a-zA-Z_]")


def _sanitize_identifier(value: str) -> str:
    """Turn an arbitrary configured name into a valid Lakeflow dataset/sink identifier fragment.

    Used for both a fully-qualified table name (dots, and potentially other punctuation), e.g.
    'observability.event_logs.pipeline_1' -> 'observability_event_logs_pipeline_1', and for a
    control-table `destination_id` (which an onboarding spec author is free to write with
    hyphens, e.g. 'dest-continuous-volume') when naming that destination's Volume sink below.
    Both need the same treatment for the same reason: Lakeflow identifiers are Spark identifiers,
    and an unsanitized dot or hyphen in one either fails graph resolution or silently reads as a
    qualified name.
    """
    return _NON_IDENTIFIER_CHARS.sub("_", value)


def _register_event_log_view(table_name: str) -> str:
    view_name = f"_{_sanitize_identifier(table_name)}_event_log_view"

    @dlt.view(
        name=view_name,
        comment=f"Streaming read of event log table '{table_name}', tagged with source_pipeline='{table_name}'",
    )
    def _event_log_view():
        return spark.readStream.table(table_name).withColumn("source_pipeline", F.lit(table_name))

    return view_name


VIEW_NAMES = [_register_event_log_view(table_name) for table_name in EVENT_LOG_TABLES]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unified Event Log Table
# MAGIC
# MAGIC Unions every per-table view via `dlt.read_stream(view_name).unionByName(...)` chained in
# MAGIC a loop -- plain `DataFrame.unionByName` (never positional `.union()`, which is
# MAGIC type/order-sensitive and would silently misalign columns if any source event-log table's
# MAGIC column order ever drifted from another's) and never the nonexistent `union_all`. Every
# MAGIC source event-log table is expected to share the same schema (they are all the same
# MAGIC native Lakeflow event-log format -- see
# MAGIC https://docs.databricks.com/aws/en/ldp/monitor-event-logs), but a real-world drift (e.g.
# MAGIC one source pipeline still on an older event-log schema version) is caught here and
# MAGIC re-raised as a clear `FrameworkConfigError` naming every configured view, rather than
# MAGIC letting a cryptic Spark `AnalysisException` about mismatched columns surface deep inside
# MAGIC graph resolution.

# COMMAND ----------


@dlt.table(
    name="unified_event_log",
    comment="Unified streaming union of all configured event log tables, each row tagged with source_pipeline",
)
def unified_event_log():
    try:
        unified_df = None
        for view_name in VIEW_NAMES:
            next_df = dlt.read_stream(view_name)
            unified_df = next_df if unified_df is None else unified_df.unionByName(next_df)
        return unified_df
    except AnalysisException as exc:
        raise FrameworkConfigError(
            f"Failed to union event log views {VIEW_NAMES} into 'unified_event_log' -- this usually means "
            "a schema mismatch across the configured event_log_tables (every source pipeline's published "
            f"event log table is expected to share the same schema). Underlying error: {exc}"
        ) from exc


# COMMAND ----------

# MAGIC %md
# MAGIC ## OTel Streaming Export (gated by `otel_export_enabled`)
# MAGIC
# MAGIC When enabled: registers the custom `otel_streaming` sink Data Source once, creates the
# MAGIC sink, and registers a genuine `@dlt.append_flow` reading `unified_event_log` into it --
# MAGIC a real Lakeflow graph node. When disabled: the sink/flow are never created at all (not
# MAGIC created-then-made-a-no-op) -- matching this framework's existing convention for
# MAGIC optional flow registration (see `engine/flow_registration.py::register_flow_output`'s
# MAGIC `target_type` dispatch, which only ever calls `sink_registration.register_sink_target`/
# MAGIC `register_external_sink_export` for the target types that need a sink at all).

# COMMAND ----------

_otel_streaming_datasource_registered = False


def _register_otel_streaming_datasource_once() -> None:
    """Register `OtelStreamingDataSource` with the active Spark session, at most once per
    process -- mirrors `engine/sink_registration.py::_register_pgp_zip_datasource_once`'s
    idempotent-registration guard (this notebook only calls this once regardless, but the
    guard avoids a redundant call+log if this cell were ever re-executed in the same
    interactive session)."""
    global _otel_streaming_datasource_registered
    if _otel_streaming_datasource_registered:
        return
    spark.dataSource.register(OtelStreamingDataSource)
    _otel_streaming_datasource_registered = True
    logger.info(
        "Registered custom Lakeflow sink Data Source 'otel_streaming' "
        "(observability/otel_streaming_sink.py::OtelStreamingDataSource)."
    )


if OTEL_EXPORT_ENABLED:
    if not OTEL_ENDPOINT:
        raise FrameworkConfigError(
            "'dataflow.otel_streaming.otel_export_enabled' is true but "
            "'dataflow.otel_streaming.otel_endpoint' was not set -- an OTLP/HTTP JSON logs collector URL "
            "is required whenever export is enabled."
        )

    _register_otel_streaming_datasource_once()
    dlt.create_sink(name="otel_sink", format="otel_streaming", options={"endpoint": OTEL_ENDPOINT})

    @dlt.append_flow(name="unified_event_log_otel_flow", target="otel_sink")
    def unified_event_log_otel_flow():
        return dlt.read_stream("unified_event_log")

    logger.info(
        "OTel streaming export ENABLED -- 'unified_event_log' -> sink 'otel_sink' (format=otel_streaming) -> '%s'",
        OTEL_ENDPOINT,
    )
else:
    logger.info(
        "OTel streaming export DISABLED ('dataflow.otel_streaming.otel_export_enabled' is not 'true') -- "
        "no sink/flow registered this update; 'unified_event_log' still materializes as a streaming table."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Volume Streaming Export (one sink per continuous `DATABRICKS_VOLUME` destination)
# MAGIC
# MAGIC The second continuous destination type. Each `observability_config` row with
# MAGIC `mode: "continuous"` and `type: "DATABRICKS_VOLUME"` gets its own
# MAGIC `dlt.create_sink(format="json", options={"path": <volume_path>})` fed by its own
# MAGIC `@dlt.append_flow` reading the *same* `unified_event_log` streaming table the OTel sink
# MAGIC reads. Reading once and fanning out is why `resolve_event_log_tables` de-duplicates the
# MAGIC table union: N destinations never mean N reads of the same source table.
# MAGIC
# MAGIC **Why Lakeflow's own native `json` sink rather than `observability/otel_streaming_sink.py`
# MAGIC or `destination_dispatcher.dispatch_to_volume`.** Writing files to a Volume from a
# MAGIC streaming source is exactly what a native file sink already does -- with Lakeflow managing
# MAGIC the checkpoint, exactly-once file commits, and restart semantics. `dispatch_to_volume` is
# MAGIC the *batch* engine's counterpart: it opens a file with `open()` and writes one
# MAGIC deterministically-named `<dataflow_group_id>_<task_run_id>` file per task run, which is
# MAGIC coherent only because a triggered task run is a single bounded event. A continuous stream
# MAGIC has no task_run_id and no single terminal moment to name a file after, so reusing it here
# MAGIC would mean reinventing checkpointing inside a custom sink to get worse guarantees.
# MAGIC
# MAGIC **Consequence, deliberate and documented:** rows land as raw event-log JSON (the
# MAGIC `unified_event_log` schema, `source_pipeline` column included), **not** as OTel
# MAGIC `ResourceLogs`. The OTel shaping in `otel_payload_builder.py` exists to satisfy an OTLP
# MAGIC collector's wire contract; a Volume archive is read back with Spark, where the native
# MAGIC event-log columns are far more useful than an OTLP envelope wrapped around them.
# MAGIC
# MAGIC A destination missing `volume_path` is skipped with a WARNING rather than failing the
# MAGIC pipeline -- one misconfigured destination must never stop an always-on export serving the
# MAGIC others, the same isolation rule `destination_dispatcher.dispatch_all` follows.
# MAGIC
# MAGIC Registration goes through a helper called once per destination (never a bare loop body)
# MAGIC for the same late-binding-closure reason documented on the per-table views above.

# COMMAND ----------

VOLUME_DESTINATIONS = [d for d in CONTINUOUS_DESTINATIONS if d.destination_type == "DATABRICKS_VOLUME"]


def _register_volume_sink(destination: DestinationConfig) -> str:
    """Register one `json`-format Lakeflow sink + `@dlt.append_flow` for one continuous
    `DATABRICKS_VOLUME` destination. Returns the sink name, or `""` when the destination was
    skipped as unusable."""
    volume_path = (destination.destination_config or {}).get("volume_path")
    if not volume_path:
        logger.warning(
            "Continuous DATABRICKS_VOLUME destination '%s' (config_id='%s') has no "
            "destination_config.volume_path -- skipping it; no sink registered for this destination.",
            destination.destination_id,
            destination.config_id,
        )
        return ""

    sink_name = f"_{_sanitize_identifier(destination.destination_id)}_volume_sink"
    dlt.create_sink(name=sink_name, format="json", options={"path": volume_path})

    @dlt.append_flow(name=f"{sink_name}_flow", target=sink_name)
    def _volume_sink_flow():
        return dlt.read_stream("unified_event_log")

    logger.info(
        "Volume streaming export registered -- 'unified_event_log' -> sink '%s' (format=json) -> '%s' "
        "(destination_id='%s')",
        sink_name,
        volume_path,
        destination.destination_id,
    )
    return sink_name


VOLUME_SINK_NAMES = [name for name in (_register_volume_sink(d) for d in VOLUME_DESTINATIONS) if name]

if not VOLUME_DESTINATIONS:
    logger.info(
        "No continuous DATABRICKS_VOLUME destination configured -- no Volume sink registered this update "
        "(this is the normal shape for a pipeline exporting only to OTLP)."
    )
else:
    logger.info(
        "Registered %d of %d continuous DATABRICKS_VOLUME destination(s) as Volume sinks: %s",
        len(VOLUME_SINK_NAMES),
        len(VOLUME_DESTINATIONS),
        VOLUME_SINK_NAMES,
    )
