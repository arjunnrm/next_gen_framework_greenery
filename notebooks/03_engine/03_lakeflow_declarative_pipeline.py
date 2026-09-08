# Databricks notebook source
# MAGIC %md
# MAGIC # Core Lakeflow Declarative Pipeline Engine
# MAGIC
# MAGIC Thin registration notebook: resolves an active `dataflow_group_id` from the control
# MAGIC tables and dynamically registers the corresponding Lakeflow Declarative Pipeline graph.
# MAGIC All business logic lives in the loosely-coupled `flowx.lakeflow_framework`
# MAGIC package (see `src/flowx/lakeflow_framework/`) -- this notebook
# MAGIC only wires metadata rows to `@dlt.table`/`@dlt.view` registrations, since that wiring
# MAGIC must execute at notebook top level for Lakeflow's graph-definition phase to see it.
# MAGIC
# MAGIC As of v1.5.0 that claim is literally true: the two ~90-line generator bodies that used
# MAGIC to live here have moved verbatim into `.engine.flow_generators`, where they are
# MAGIC importable and unit-testable without a workspace (`tests/unit/test_flow_generators.py`).
# MAGIC What remains below is three phases and three bare `for` loops.
# MAGIC
# MAGIC ## The three flow types
# MAGIC
# MAGIC * **Ingestion Engine** -- `.ingestion` (readers, technical metadata), `.dq`
# MAGIC   (expectations, quarantine), `.crypto` (encryption), `.cdc` (CDC/materialization strategies).
# MAGIC * **Transformation Engine** -- `.transformation` (watermarked inputs, dynamic
# MAGIC   parameters), plus the same `.dq` / `.crypto` / `.cdc` modules.
# MAGIC * **Reconciliation Engine (v1.5.0)** -- `.reconciliation.graph_registration`, the DAG's
# MAGIC   third first-class flow type. Only `reconciliation_flow_spec` rows whose
# MAGIC   `execution_mode` is `"pipeline"`/`"pipeline_audit_only"` are registered here;
# MAGIC   `"job"`-mode rows are filtered out by `load_active_group_metadata` and belong to the
# MAGIC   standalone `notebooks/05_reconciliation/05_reconciliation_engine.py` job task, whose
# MAGIC   behaviour is entirely unchanged.
# MAGIC
# MAGIC A single `dataflow_group_id` yields a **unified**, **ingestion-only**,
# MAGIC **transformation-only** or any combination DAG purely based on which control tables have
# MAGIC active rows for that group -- no separate pipeline code path is required.
# MAGIC
# MAGIC ## The L0 source plane (read-once)
# MAGIC
# MAGIC Every physical read in this pipeline -- ingestion source, transformation input, and both
# MAGIC reconciliation sides -- is routed through `.engine.source_plane`, so one physical
# MAGIC table/path is read **exactly once per update** and reused across all its consumers. The
# MAGIC API is deliberately three-phase and the phases must run in this order:
# MAGIC
# MAGIC 1. `plan_source_plane(...)` -- pure. No `dlt` import, no Spark action. Computes one read
# MAGIC    identity per distinct physical locator and one `Binding` per consumer id.
# MAGIC    `assert_acyclic(...)` then Kahn-sorts the whole edge set and raises
# MAGIC    `FrameworkGraphCycleError` naming the ring, before a single dataset is defined.
# MAGIC 2. `register_source_plane(spark, PLAN)` -- registers one `@dlt.table` per shared node.
# MAGIC    This must precede phase 3: Lakeflow resolves `dlt.read`/`dlt.read_stream` by dataset
# MAGIC    name at graph-build time, so a node a generator binds to must already be defined.
# MAGIC 3. The three registration loops, each generator resolving its own reads via
# MAGIC    `bind(plan, consumer_id, want_stream)`.
# MAGIC
# MAGIC `describe_plan(PLAN)` is emitted as one `source_plane_node` structured log event per
# MAGIC binding and per shared node, so "was my table actually read once" is answerable from the
# MAGIC driver log stream without reverse-engineering the event log.
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
# MAGIC
# MAGIC ## DEPLOYING A CHANGE TO THIS FILE
# MAGIC `flowx_testing/TESTING_PLAN.md` section 0, rule 5: a stale DABs sync snapshot makes
# MAGIC `bundle deploy` report **"Files: 0 uploaded"** while the workspace keeps running the
# MAGIC *old* notebook -- and this notebook is the file that has actually been bitten by it.
# MAGIC After editing this file, confirm the deploy reported a non-zero upload count (or force a
# MAGIC re-sync) before concluding that a behaviour change "did not work"; every pipeline in
# MAGIC `resources/*.yml` lists this exact path in its `libraries:` block, so a silent no-op
# MAGIC deploy here silently no-ops the whole framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Bootstrap
# MAGIC
# MAGIC Lakeflow Declarative Pipeline source notebooks do not support `%run`. In production,
# MAGIC attach `flowx`'s wheel to this pipeline via
# MAGIC `resources/lakeflow_metadata_pipeline.yml`'s `environment.dependencies` -- once
# MAGIC installed that way, a plain `import` resolves it like any other site-packages library.
# MAGIC The fallback below only kicks in for local, wheel-less notebook development.

# COMMAND ----------

import json
import logging
import os
import sys
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lakeflow_declarative_pipeline")

try:
    import flowx.lakeflow_framework  # noqa: F401
except ImportError:
    try:
        this_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        dev_src_root = os.path.abspath(os.path.join(this_dir, "..", "..", "src"))
        if dev_src_root not in sys.path:
            sys.path.insert(0, dev_src_root)
        import flowx.lakeflow_framework  # noqa: F401
        logger.warning("Loaded 'flowx' from local 'src/' (dev fallback) -- not from an installed wheel.")
    except ImportError as exc:
        raise ImportError(
            "Could not import 'flowx'. In production this must be attached as a "
            "wheel library (see resources/lakeflow_metadata_pipeline.yml); for local development, run "
            f"from within the repo so '../../src' resolves. Original error: {exc}"
        ) from exc

from flowx.lakeflow_framework.control_plane.repository import (
    load_active_group_metadata,  # noqa: E402
)
from flowx.lakeflow_framework.engine.flow_generators import (  # noqa: E402
    generate_ingestion_flow,
    generate_reconciliation_flow,
    generate_transformation_flow,
    resolve_pipeline_schema,
)
from flowx.lakeflow_framework.engine.run_context import resolve_pipeline_run_id  # noqa: E402
from flowx.lakeflow_framework.engine.source_plane import (  # noqa: E402
    assert_acyclic,
    describe_plan,
    plan_source_plane,
    register_source_plane,
)
from flowx.lakeflow_framework.engine.spark_config import (  # noqa: E402
    apply_spark_conf,
    read_pipeline_spark_config,
    resolve_spark_conf,
)
from flowx.lakeflow_framework.exceptions import FrameworkConfigError  # noqa: E402
from flowx.lakeflow_framework.observability.structured_logger import (  # noqa: E402
    log_flow_event,
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

MD = load_active_group_metadata(spark, CONTROL_CATALOG, GROUP_ID)
GROUP_ROW = MD.group_row
PIPELINE_PARAMETERS = json.loads(GROUP_ROW.pipeline_parameters_json) if GROUP_ROW.pipeline_parameters_json else {}
PIPELINE_RUN_ID = resolve_pipeline_run_id(spark, GROUP_ID)

# The three shared reconciliation control tables (reconciliation_run_log /
# reconciliation_result / reconciliation_mismatch_log) live beside the four spec tables, exactly
# as 05_reconciliation_engine.py resolves them (`CONTROL_SCHEMA = f"{CATALOG}.config"`).
CONTROL_SCHEMA = f"{CONTROL_CATALOG}.config"

# getattr, not GROUP_ROW.source_plane_config_json: this column is new in v1.5.0 and may not
# exist yet on a dataflow_group_spec table provisioned before it was added (01_setup only ever
# runs CREATE TABLE IF NOT EXISTS, never a migration) -- absent means "all source-plane
# defaults", exactly what an empty {} would. Same defensive pattern spark_config_json uses
# below, and that 05_reconciliation_engine.py already uses for logging_config_json.
_SOURCE_PLANE_CONFIG = json.loads(getattr(GROUP_ROW, "source_plane_config_json", None) or "{}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hosting-Pipeline Identity and Reconciliation Runtime Overrides
# MAGIC
# MAGIC Two things the control tables deliberately do **not** record, because they belong to the
# MAGIC *pipeline resource* rather than to the onboarded group:
# MAGIC
# MAGIC * **Where "the hosting pipeline's own catalog/schema" is.** `resources/*.yml` sets
# MAGIC   `catalog:`/`schema:` on the pipeline, and Lakeflow makes those the update's current
# MAGIC   catalog/database. That is the documented default for a null `source_plane.catalog` /
# MAGIC   `source_plane.schema`. Since v1.7.07 it is NOT a default for a null
# MAGIC   `reconciliation_flow_spec.publish_schema` -- a null publish_schema publishes nothing;
# MAGIC   `PIPELINE_SCHEMA` is still resolved and passed for call-site compatibility only.
# MAGIC * **The per-run reconciliation log-silencing override.** `05_reconciliation_engine.py`
# MAGIC   exposes `recon_run_log_capture` / `recon_mismatch_log` as tri-state *job widgets*; a
# MAGIC   pipeline update has no widgets and `pipelines start-update` accepts only
# MAGIC   `--full-refresh`. The replacement is the two pipeline-configuration keys
# MAGIC   `dataflow.recon.run_log_capture` / `dataflow.recon.mismatch_log`, read here exactly as
# MAGIC   `dataflow.group.id` is. **The tri-state is preserved**: absent *or* `''` defers to the
# MAGIC   flow's own `logging_config`; `'true'`/`'false'` force writes on/off. The loss is real
# MAGIC   and documented in `docs/13`: silencing now takes a pipeline settings edit that takes
# MAGIC   effect on the *next* update, not the current one.

# COMMAND ----------


def _resolve_tristate_conf(key: str) -> Optional[bool]:
    """Read a pipeline configuration key as an ``''``/``'true'``/``'false'`` tri-state.

    Tri-state rather than a plain boolean for the same reason the job widget is: an operator
    must be able to *silence* log writes without re-onboarding the flow, and must equally be
    able to leave the decision to the flow's own onboarded metadata -- a two-state boolean
    cannot express "I am not expressing an opinion", and would silently override
    ``logging_config`` on every single update. Absent and ``''`` are the same answer: defer.
    """
    raw = (spark.conf.get(key, "") or "").strip().lower()
    if raw not in ("", "true", "false"):
        raise FrameworkConfigError(f"Pipeline configuration '{key}' must be '', 'true', or 'false', got {raw!r}")
    return None if raw == "" else raw == "true"


RECON_LOG_CAPTURE_OVERRIDES = {
    "recon_run_log_capture": _resolve_tristate_conf("dataflow.recon.run_log_capture"),
    "recon_mismatch_log": _resolve_tristate_conf("dataflow.recon.mismatch_log"),
}

# The hosting pipeline's own catalog/schema. Inside a running update these are the session's
# current catalog/database; outside one (a local import of this file for linting/AST tests)
# they are unavailable, so fall back to the group's onboarded catalog_name and leave the schema
# unset. Since v1.6.0 these are consumed only by reconciliation dataset naming (published
# metrics/mismatch nodes) -- the source plane no longer receives them (an unpublished node
# needs no home), and generate_reconciliation_flow is never called at all for a group with no
# pipeline-mode reconciliation rows.
try:
    _CURRENT_CATALOG = spark.catalog.currentCatalog()
except Exception as _catalog_exc:  # noqa: BLE001 -- must never fail graph definition
    logger.warning("Could not resolve the pipeline's current catalog (%s); falling back to catalog_name.", _catalog_exc)
    _CURRENT_CATALOG = None
PIPELINE_CATALOG = _CURRENT_CATALOG or getattr(GROUP_ROW, "catalog_name", None) or CONTROL_CATALOG
# Mirrors PIPELINE_CATALOG's fallback chain, which this line previously lacked entirely -- it read
# `= _CURRENT_SCHEMA`, and during graph definition that is NOT the pipeline's declared target
# schema, so reconciliation dataset naming failed. The resolution order lives in
# `.engine.flow_generators.resolve_pipeline_schema` rather than here, so it is unit-testable and so
# this notebook keeps only bare registration fan-outs (tests/unit/test_pipeline_notebook_is_thin.py).
PIPELINE_SCHEMA = resolve_pipeline_schema(spark, GROUP_ROW)
if not PIPELINE_SCHEMA:
    # v1.7.07: no longer fatal for reconciliation -- a flow without publish_schema publishes
    # nothing, so PIPELINE_SCHEMA is informational here.
    logger.info(
        "Could not resolve this pipeline's target schema from pipelines.schema/pipelines.target, "
        "the session's current database, or the group row. Reconciliation naming no longer depends "
        "on it (a flow without publish_schema publishes nothing)."
    )

# The onboarding spec (and therefore source_plane_config_json) names these keys `catalog`/
# `schema`; plan_source_plane's own parameters are `node_catalog`/`node_schema`, which is what
# makes them unambiguous at its call sites. The rename happens here, once, rather than being
# forced into either of those two contracts. Since v1.6.0 the spec's raw values are passed
# WITHOUT falling back to the pipeline's own catalog/schema: "null" now means "do not publish"
# -- the shared node becomes a pipeline-scoped temporary table (Intermediate Object Rule) --
# whereas the old fallback made every L0 node a published table in the pipeline's target schema.
_SOURCE_PLANE_KWARGS = {
    # v1.7.3 Single-Read mandate: "always" is the default. NOTE this dict is built from the
    # PERSISTED dataflow_group_spec.source_plane_config_json row, which onboarding validation
    # never re-inspects -- so a group onboarded before the mandate can still carry
    # {"materialize": "never"} here. That value is deliberately passed through rather than
    # silently coerced: plan_source_plane raises on it, which surfaces the stale row as a loud
    # failure instead of a pipeline that quietly keeps doing per-consumer inline reads.
    "materialize": _SOURCE_PLANE_CONFIG.get("materialize", "always"),
    "node_catalog": _SOURCE_PLANE_CONFIG.get("catalog"),
    "node_schema": _SOURCE_PLANE_CONFIG.get("schema"),
}

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
# MAGIC ## PHASE 1 -- Plan the L0 Source Plane (pure; no `dlt`, no Spark action)
# MAGIC
# MAGIC One read identity per distinct physical locator across **all three** flow arrays, one
# MAGIC `Binding` per consumer id. `assert_acyclic` then Kahn-sorts the full edge set and raises
# MAGIC `FrameworkGraphCycleError` naming the ring -- multi-hop rings included -- before any
# MAGIC dataset is defined, so a cycle costs a plan-time error message instead of an opaque
# MAGIC Lakeflow graph failure. Guards `G-STREAM` (a streaming read of a MERGE-written in-graph
# MAGIC target) and `G-SIDE` (two competing `landing_retention_policy` / `source_zip_handling`
# MAGIC lifecycle regimes on one path) also fire here.

# COMMAND ----------

PLAN = plan_source_plane(
    MD.ingestion_rows,
    MD.transformation_rows,
    MD.reconciliation_rows,
    PIPELINE_PARAMETERS,
    **_SOURCE_PLANE_KWARGS,
)
assert_acyclic(PLAN)

# One structured event per binding and per shared node. This is what makes "was this table
# actually read once?" answerable from the driver log stream: a `kind: "shared_node"` row with
# fanout N means one physical read serving N consumers, whereas N separate `kind: "inline"`
# binding rows on the same locator would mean N physical reads.
for _plane_node in describe_plan(PLAN):
    log_flow_event(
        "source_plane_node",
        _plane_node.get("consumer_id") or _plane_node.get("dataset_name") or _plane_node.get("locator") or GROUP_ID,
        "SUCCESS",
        dataflow_group_id=GROUP_ID,
        **_plane_node,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 2 -- Register the L0 Source Plane
# MAGIC
# MAGIC One `@dlt.table` per shared node: a **streaming table** when any consumer streams (it is
# MAGIC then legally readable by `dlt.read_stream` *and* `dlt.read` consumers in the same
# MAGIC update), a materialized view when every consumer is batch. Never a `@dlt.view` -- a view
# MAGIC is inlined into each consumer, so each consumer would open its own `DeltaSource` on the
# MAGIC same table and "declared once" would not be "read once".
# MAGIC
# MAGIC This must run **before** phase 3: Lakeflow resolves `dlt.read`/`dlt.read_stream` by
# MAGIC dataset name at graph-build time, so a node a generator binds to must already be defined.

# COMMAND ----------

register_source_plane(spark, PLAN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHASE 3 -- Flow Registration (ingestion, transformation, reconciliation)
# MAGIC
# MAGIC Three bare loops over the three control-table row sets. Each generator lives in
# MAGIC `.engine.flow_generators` and resolves its own physical reads through `PLAN` via
# MAGIC `source_plane.bind(...)`; nothing in this notebook reads a source directly any more.
# MAGIC An empty row set is simply a loop that does not execute, which is how one
# MAGIC `dataflow_group_id` yields a unified / ingestion-only / transformation-only /
# MAGIC reconciliation-only DAG with no separate code path.

# COMMAND ----------

for _ingestion_row in MD.ingestion_rows:
    generate_ingestion_flow(
        spark,
        dbutils,
        _ingestion_row,
        plan=PLAN,
        pipeline_parameters=PIPELINE_PARAMETERS,
        pipeline_run_id=PIPELINE_RUN_ID,
    )

for _transformation_row in MD.transformation_rows:
    generate_transformation_flow(
        spark,
        dbutils,
        _transformation_row,
        plan=PLAN,
        pipeline_parameters=PIPELINE_PARAMETERS,
        pipeline_run_id=PIPELINE_RUN_ID,
    )

for _reconciliation_row in MD.reconciliation_rows:
    generate_reconciliation_flow(
        spark,
        _reconciliation_row,
        plan=PLAN,
        publish_catalog=PIPELINE_CATALOG,
        publish_schema=PIPELINE_SCHEMA,
        control_schema=CONTROL_SCHEMA,
        pipeline_update_id=PIPELINE_RUN_ID,
        log_capture_overrides=RECON_LOG_CAPTURE_OVERRIDES,
        pipeline_parameters=PIPELINE_PARAMETERS,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Deployment Governance
# MAGIC
# MAGIC Governance tag application is implemented in
# MAGIC `flowx.lakeflow_framework.control_plane.post_deployment`
# MAGIC (`apply_all_governance_tags`) rather than here, so a dedicated job task can call it
# MAGIC *after* this pipeline's update completes without re-triggering the
# MAGIC `@dlt.table`/`@dlt.view` graph-definition code above. See
# MAGIC `notebooks/04_governance/04_apply_governance_and_egress.py` and
# MAGIC `resources/metadata_framework_job.yml`.
# MAGIC
# MAGIC `external_sink`/`sink` egress has no post-deployment counterpart any more (Phase 7)
# MAGIC -- see the "Governance Tags & Sink Egress" section above.
# MAGIC
# MAGIC Reconciliation control rows are written by the L5 `foreach_batch_sink` handler *inside*
# MAGIC this update whenever its pulse carries rows. The BACKSTOP for an empty pulse and for a
# MAGIC `"pipeline_audit_only"` flow is `.observability.reconciliation_export`, called from the
# MAGIC observability **job** task (`notebooks/08_observability/08_dlt_observability_engine.py`)
# MAGIC -- deliberately not from here: observability stays a normal Lakeflow job task and no
# MAGIC observability notebook ever enters a pipeline's `libraries:` block.
