# Databricks notebook source
# MAGIC %md
# MAGIC # Core Lakeflow Declarative Pipeline Engine
# MAGIC
# MAGIC Thin orchestration notebook: resolves an active `dataflow_group_id` from the control
# MAGIC tables and dynamically registers the corresponding Lakeflow Declarative Pipeline graph.
# MAGIC All business logic lives in the loosely-coupled `NextGen_Metadata_Framework.lakeflow_framework`
# MAGIC package (see `src/NextGen_Metadata_Framework/lakeflow_framework/`) -- this notebook
# MAGIC only wires metadata rows to `@dlt.table`/`@dlt.view` registrations, since that wiring
# MAGIC must execute at notebook top level for Lakeflow's graph-definition phase to see it.
# MAGIC
# MAGIC * **Ingestion Engine** -- `.ingestion` (readers, technical metadata), `.dq`
# MAGIC   (expectations, quarantine), `.crypto` (encryption), `.cdc` (CDC/materialization strategies).
# MAGIC * **Transformation Engine** -- `.transformation` (watermarked inputs, dynamic
# MAGIC   parameters), plus the same `.dq` / `.crypto` / `.cdc` modules.
# MAGIC
# MAGIC A single `dataflow_group_id` yields a **unified**, **ingestion-only**, or
# MAGIC **transformation-only** DAG purely based on which control tables have active rows for
# MAGIC that group -- no separate pipeline code path is required.
# MAGIC
# MAGIC ## Spark Session Configuration
# MAGIC Group-scoped Spark settings are resolved and applied *once*, between control-metadata
# MAGIC resolution and the first flow registration, by `.engine.spark_config` -- framework
# MAGIC built-in defaults < the onboarded `spark_config` block < this pipeline resource's own
# MAGIC `configuration: dataflow.spark.conf`. Applying them before any `@dlt.table`/`@dlt.view`
# MAGIC is defined is what puts every flow closure under them; nothing in that path can fail an
# MAGIC update (an unsettable or static key is logged and skipped).
# MAGIC
# MAGIC ## Governance Tags & Sink Egress
# MAGIC Governance tags (`.governance.tags`) are Unity Catalog DDL against a materialized
# MAGIC table, so they must run *after* the pipeline update -- exposed as
# MAGIC `apply_all_governance_tags`, invoked from a downstream Databricks Workflow task (see
# MAGIC `notebooks/04_governance/04_apply_governance_and_egress.py`), not from inside the
# MAGIC pipeline graph-definition code path below.
# MAGIC
# MAGIC `external_sink`/`sink` egress, by contrast, is **not** a post-deployment step at all
# MAGIC (Phase 7): every sink export is a genuine `dlt.create_sink`/`@dlt.append_flow` pair,
# MAGIC registered right here at graph-definition time by `generate_ingestion_flow`/
# MAGIC `generate_transformation_flow` -> `register_flow_output` -> `.engine.sink_registration`,
# MAGIC and executed as part of this same pipeline update -- see that module's docstring for
# MAGIC why (a prior post-deployment egress step, `control_plane/post_deployment.py::
# MAGIC run_external_sink_exports`, has been removed entirely).
# MAGIC
# MAGIC ## Structured Logging (Phase 10)
# MAGIC Both `register_staged_view` (source read / transformation SQL execution) and
# MAGIC `register_flow_output` (flow registration outcome) emit structured JSON log events via
# MAGIC `.observability.structured_logger` -- see that module's docstring for what "available via
# MAGIC the Lakeflow event table" honestly means in practice (a driver-stdout JSON log line, not
# MAGIC a custom row in Lakeflow's own fixed-schema event log), and `dq/quarantine.py` for where
# MAGIC quarantine-row counts are captured (inside the quarantine table's own `@dlt.table`
# MAGIC closure, at Lakeflow execution time -- not here at graph-definition time).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap
# MAGIC
# MAGIC Lakeflow Declarative Pipeline source notebooks do not support `%run`. In production,
# MAGIC attach `NextGen_Metadata_Framework`'s wheel to this pipeline via
# MAGIC `resources/lakeflow_metadata_pipeline.yml`'s `environment.dependencies` -- once
# MAGIC installed that way, a plain `import` resolves it like any other site-packages library.
# MAGIC The fallback below only kicks in for local, wheel-less notebook development.

# COMMAND ----------

import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lakeflow_declarative_pipeline")

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
            "wheel library (see resources/lakeflow_metadata_pipeline.yml); for local development, run "
            f"from within the repo so '../../src' resolves. Original error: {exc}"
        ) from exc

from NextGen_Metadata_Framework.lakeflow_framework.control_plane.repository import (
    load_active_group_metadata,  # noqa: E402
)
from NextGen_Metadata_Framework.lakeflow_framework.engine.flow_registration import (  # noqa: E402
    register_flow_output,
    register_staged_view,
)
from NextGen_Metadata_Framework.lakeflow_framework.engine.run_context import resolve_pipeline_run_id  # noqa: E402
from NextGen_Metadata_Framework.lakeflow_framework.engine.spark_config import (  # noqa: E402
    apply_spark_conf,
    read_pipeline_spark_config,
    resolve_spark_conf,
)
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError  # noqa: E402
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.column_normalization import (  # noqa: E402
    normalize_column_names,
)
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.dedup import apply_stream_dedup  # noqa: E402
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.json_flattening import (  # noqa: E402
    apply_explode_columns,
    parse_json_string_columns,
    resolve_auto_flatten_all,
)
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.readers import read_ingestion_source  # noqa: E402
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.schema_config import (  # noqa: E402
    apply_schema_config,
    load_schema_config,
)
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.standardization_sql import (  # noqa: E402
    apply_data_standardization_sql,
)
from NextGen_Metadata_Framework.lakeflow_framework.ingestion.technical_metadata import (
    attach_technical_metadata,  # noqa: E402
)
from NextGen_Metadata_Framework.lakeflow_framework.transformation.inputs import (  # noqa: E402
    mark_streaming_references,
    register_transformation_inputs,
)
from NextGen_Metadata_Framework.lakeflow_framework.transformation.parameters import (
    substitute_dynamic_parameters,  # noqa: E402
    substitute_path_parameters,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control Metadata Resolution

# COMMAND ----------

GROUP_ID = spark.conf.get("dataflow.group.id")
CONTROL_CATALOG = spark.conf.get("dataflow.control.catalog")

if not GROUP_ID:
    raise ValueError("Required pipeline configuration 'dataflow.group.id' was not set.")
if not CONTROL_CATALOG:
    raise ValueError("Required pipeline configuration 'dataflow.control.catalog' was not set.")

GROUP_ROW, INGESTION_ROWS, TRANSFORMATION_ROWS = load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)
PIPELINE_PARAMETERS = json.loads(GROUP_ROW.pipeline_parameters_json) if GROUP_ROW.pipeline_parameters_json else {}
PIPELINE_RUN_ID = resolve_pipeline_run_id(spark, GROUP_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hierarchical Spark Configuration
# MAGIC
# MAGIC Resolve and apply this group's Spark session configuration **before any flow is
# MAGIC registered**, so every `@dlt.table`/`@dlt.view` closure defined below executes under it.
# MAGIC Three layers, lowest to highest: `engine/spark_config.py`'s `FRAMEWORK_SPARK_DEFAULTS`
# MAGIC < the onboarded, group-scoped `spark_config` (persisted to
# MAGIC `dataflow_group_spec.spark_config_json`) < this pipeline resource's own
# MAGIC `configuration: dataflow.spark.conf` JSON-object-as-string entry -- see that module's
# MAGIC docstring for why the deployment-time bundle value must be the one that wins.
# MAGIC
# MAGIC This is deliberately **not** `pipeline_parameters`: that field is `${param}` *string
# MAGIC substitution* into SQL and paths (`transformation/parameters.py`), a completely
# MAGIC different mechanism that must not be overloaded with Spark tuning keys.

# COMMAND ----------

# getattr, not GROUP_ROW.spark_config_json: this column may not exist yet on a
# dataflow_group_spec table provisioned before this field was added (01_setup only ever runs
# CREATE TABLE IF NOT EXISTS, never a migration) -- absent means "no group-level Spark config",
# exactly what an empty {} would. Same defensive pattern 05_reconciliation_engine.py already
# uses for logging_config_json.
_SPARK_CONFIG_JSON = getattr(GROUP_ROW, "spark_config_json", None)
SPEC_SPARK_CONFIG = json.loads(_SPARK_CONFIG_JSON) if _SPARK_CONFIG_JSON else {}
RESOLVED_SPARK_CONF = resolve_spark_conf(
    spec_spark_config=SPEC_SPARK_CONFIG,
    pipeline_spark_config=read_pipeline_spark_config(spark),
)
# apply_spark_conf returns only the keys Spark actually accepted (a static SQL configuration is
# logged and skipped, never raised -- tuning metadata must not fail an update), and the summary
# is logged here rather than inside the applier because this is the one scope where the
# dataflow_group_id being configured is in hand.
APPLIED_SPARK_CONF = apply_spark_conf(spark, RESOLVED_SPARK_CONF)
logger.info(
    "Applied %d Spark configuration key(s) for dataflow_group_id='%s': %s",
    len(APPLIED_SPARK_CONF),
    GROUP_ID,
    ", ".join(f"{_key}={_value!r}" for _key, _value in sorted(APPLIED_SPARK_CONF.items())) or "<none>",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Engine: Flow Registration
# MAGIC
# MAGIC For each active ingestion flow: build a staged view (read + technical metadata + AES
# MAGIC encryption + quarantine columns), then materialize the main target table (and
# MAGIC quarantine sibling, if configured) and apply the configured CDC/materialization
# MAGIC strategy.

# COMMAND ----------


def generate_ingestion_flow(flow_row) -> None:
    try:
        # ${param} placeholders in path-bearing fields (source_config/target_config) are
        # resolved fresh from PIPELINE_PARAMETERS on every pipeline update -- see
        # transformation/parameters.py::substitute_path_parameters's docstring. dq_config is
        # deliberately excluded (its expr fields are SQL predicates, not paths).
        source_config = (
            json.loads(substitute_path_parameters(flow_row.source_config_json, PIPELINE_PARAMETERS))
            if flow_row.source_config_json
            else {}
        )
        target_config = (
            json.loads(substitute_path_parameters(flow_row.target_config_json, PIPELINE_PARAMETERS))
            if flow_row.target_config_json
            else {}
        )
        dq_config = json.loads(flow_row.dq_config_json) if flow_row.dq_config_json else {}
    except json.JSONDecodeError as exc:
        raise FrameworkConfigError(f"Ingestion flow '{flow_row.dataflow_id}': malformed JSON configuration: {exc}") from exc

    dq_rules = dq_config.get("rules", [])
    is_streaming = flow_row.target_type == "streaming_table"
    staged_view_name = f"_{flow_row.target_table}_staged"

    def _build_ingestion_dataframe():
        staged_df = read_ingestion_source(spark, flow_row.source_type, source_config)
        # schema_config (explicit type/rename/comment) runs first -- its source_name keys
        # reference the source's true raw column names, before anything else here touches
        # them. normalize_column_names runs next, over whatever names remain (including any
        # column schema_config didn't cover) -- see ingestion/column_normalization.py's module
        # docstring for why this exact order matters.
        schema_config_path = source_config.get("schema_config_path")
        if schema_config_path:
            schema_config = load_schema_config(dbutils, schema_config_path)
            staged_df = apply_schema_config(staged_df, schema_config)
        staged_df = normalize_column_names(staged_df, source_config)
        staged_df = attach_technical_metadata(staged_df, source_config)
        # parse_json_string_columns turns STRING columns holding a JSON document into real
        # structs, which is what lets a Parquet/CSV/Delta source with an embedded JSON payload
        # get the same struct-flatten/array-explode treatment as a native JSON source. It must
        # run *after* normalize_column_names (its configured column names are the normalized
        # ones) and *immediately before* apply_explode_columns (the structs it produces are
        # exactly what explode consumes -- run it afterwards and they would never be flattened).
        staged_df = parse_json_string_columns(staged_df, source_config.get("json_string_columns"))
        # resolve_auto_flatten_all, not source_config.get("auto_flatten_all", False): a
        # PRESENT-but-empty "explode_columns": [] means "auto-flatten everything", while an
        # ABSENT (or null) explode_columns stays a schema-preserving pass-through. That
        # distinction is load-bearing -- it is what prevents silent cartesian row explosion on
        # un-configured sources (see ingestion/json_flattening.py's module docstring) -- and it
        # can only be made against the raw dict here, because a `.get()` inside the function
        # collapses "absent" and "present-but-empty" to the same value.
        staged_df = apply_explode_columns(
            staged_df, source_config.get("explode_columns"), resolve_auto_flatten_all(source_config)
        )
        # Full-row dedup (source_config.remove_dups, default False -- a no-op otherwise) sits
        # after explode and before standardization on purpose: a source row delivered twice
        # becomes 2xM rows once an array is explode_outer'ed, so only a post-explode dedup
        # collapses it correctly; and standardization must run over the surviving rows only,
        # since a standardization expression built on current_timestamp() (or any other
        # non-deterministic function) would otherwise make every duplicate look distinct and
        # defeat the dedup entirely. See ingestion/dedup.py for the unbounded-state warning that
        # applies to a watermark-less streaming source.
        staged_df = apply_stream_dedup(staged_df, source_config)
        staged_df = apply_data_standardization_sql(staged_df, source_config.get("data_standardization_sql"))
        return staged_df

    register_staged_view(
        staged_view_name,
        f"Staged intermediate view for ingestion flow {flow_row.dataflow_id}",
        dq_rules,
        _build_ingestion_dataframe,
        target_config,
        pipeline_run_id=PIPELINE_RUN_ID,
        record_id_column=dq_config.get("record_id_column"),
        capture_technical_metadata=source_config.get("capture_technical_metadata", True),
        flow_id=flow_row.dataflow_id,
        read_operation_name="ingestion_read",
    )

    register_flow_output(
        flow_row.dataflow_id,
        staged_view_name,
        flow_row.target_table,
        flow_row.target_catalog,
        flow_row.target_schema,
        flow_row.cdc_load_strategy,
        target_config,
        dq_rules,
        flow_row.source_description,
        is_streaming,
        flow_row.target_type,
        quarantine_table_override=dq_config.get("quarantine_table"),
    )


for _ingestion_row in INGESTION_ROWS:
    generate_ingestion_flow(_ingestion_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Engine: Flow Registration
# MAGIC
# MAGIC For each active transformation flow: register per-input watermarked views, execute
# MAGIC the (dynamic-parameter-substituted) native SQL transformation as a staged view, apply
# MAGIC decrypt/re-encrypt and DQ + quarantine, then apply the configured CDC/materialization
# MAGIC strategy.

# COMMAND ----------


def generate_transformation_flow(flow_row) -> None:
    try:
        source_inputs = json.loads(flow_row.source_inputs_json) if flow_row.source_inputs_json else []
        target_config = (
            json.loads(substitute_path_parameters(flow_row.target_config_json, PIPELINE_PARAMETERS))
            if flow_row.target_config_json
            else {}
        )
        dq_config = json.loads(flow_row.dq_config_json) if flow_row.dq_config_json else {}
    except json.JSONDecodeError as exc:
        raise FrameworkConfigError(
            f"Transformation flow '{flow_row.flow_step_id}': malformed JSON configuration: {exc}"
        ) from exc

    dq_rules = dq_config.get("rules", [])
    register_transformation_inputs(spark, source_inputs)
    resolved_sql = substitute_dynamic_parameters(flow_row.transformation_sql, PIPELINE_PARAMETERS)
    resolved_sql = mark_streaming_references(resolved_sql, source_inputs)

    # A transformation's staged view is a genuinely streaming computation whenever *any*
    # of its source_inputs is streaming (Spark propagates streaming through the whole
    # query plan once one input is), independent of this flow's own target_type -- a
    # windowed streaming aggregation feeding an `external_sink`/`batch_table`/
    # `materialized_view` target is still a streaming view under the hood. Missing this
    # raised `AnalysisException: View '...' is a streaming view and must be referenced
    # using readStream` the moment `register_main_and_quarantine_tables` read such a
    # staged view via a plain (non-streaming) `dlt.read(...)`.
    is_streaming = flow_row.target_type == "streaming_table" or any(
        input_config.get("is_streaming") for input_config in source_inputs
    )
    staged_view_name = f"_{flow_row.target_table}_staged"

    register_staged_view(
        staged_view_name,
        f"Staged transformation output for {flow_row.flow_step_id}",
        dq_rules,
        lambda: spark.sql(resolved_sql),
        target_config,
        pipeline_run_id=PIPELINE_RUN_ID,
        record_id_column=dq_config.get("record_id_column"),
        capture_technical_metadata=target_config.get("capture_technical_metadata", True),
        flow_id=flow_row.flow_step_id,
        read_operation_name="transformation_execute",
    )

    register_flow_output(
        flow_row.flow_step_id,
        staged_view_name,
        flow_row.target_table,
        flow_row.target_catalog,
        flow_row.target_schema,
        flow_row.cdc_load_strategy,
        target_config,
        dq_rules,
        f"Transformation target for flow step {flow_row.flow_step_id}",
        is_streaming,
        flow_row.target_type,
        quarantine_table_override=dq_config.get("quarantine_table"),
    )


for _transformation_row in TRANSFORMATION_ROWS:
    generate_transformation_flow(_transformation_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Deployment Governance
# MAGIC
# MAGIC Governance tag application is implemented in
# MAGIC `NextGen_Metadata_Framework.lakeflow_framework.control_plane.post_deployment`
# MAGIC (`apply_all_governance_tags`) rather than here, so a dedicated job task can call it
# MAGIC *after* this pipeline's update completes without re-triggering the
# MAGIC `@dlt.table`/`@dlt.view` graph-definition code above. See
# MAGIC `notebooks/04_governance/04_apply_governance_and_egress.py` and
# MAGIC `resources/metadata_framework_job.yml`.
# MAGIC
# MAGIC `external_sink`/`sink` egress has no post-deployment counterpart any more (Phase 7)
# MAGIC -- see the "Governance Tags & Sink Egress" section above.
