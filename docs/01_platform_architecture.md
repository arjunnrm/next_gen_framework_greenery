# 🏗️ Metaflow — Platform Architecture & Core Concepts

> **Audience**: Solution architects, lead data engineers, and framework contributors who need to understand how Metaflow compiles metadata into Databricks Lakeflow Declarative Pipelines.

---

## 1. Executive Summary & Problem Statement

**Metaflow** is an enterprise-grade, metadata-driven data framework built natively on [Databricks Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/) (formerly Delta Live Tables / DLT).

### The Challenge of Traditional Data Engineering
In traditional Lakehouse implementations:
- **Code Duplication**: Every new source requires writing bespoke Python/SQL notebooks with redundant `@dlt.table` decorators and boilerplate.
- **Inconsistent CDC & DQ**: Different developers implement Slowly Changing Dimensions (SCD), error handling, and quarantine routing in subtly conflicting ways.
- **High Maintenance Overhead**: Upgrading security standards, schema evolution policies, or telemetry logging requires modifying dozens of individual pipeline notebooks.

### The Metaflow Solution
Metaflow decouples **Pipeline Definition** (declarative JSON/YAML onboarding specifications) from **Pipeline Execution** (a single, generic Lakeflow compilation engine). Adding a new ingestion flow, multi-table join, SCD2 dimension, or PGP-encrypted sink requires only configuration—**zero new Python code**.

---

## 2. Two-Phase Execution Model

Metaflow operates strictly on a two-phase execution lifecycle that prevents runtime data contamination and guarantees graph determinism:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   PHASE 1: GRAPH DEFINITION (Compile Time)                  │
│                                                                             │
│ • Entrypoint: notebooks/03_engine/03_lakeflow_declarative_pipeline.py       │
│ • Reads active metadata from control tables for dataflow.group.id           │
│ • Eagerly resolves secrets (UC secret paths) and compiles SQL templates     │
│ • Dynamically registers @dlt.table, @dlt.view, and dlt.create_sink          │
│ • Crucial Rule: NO STREAMING DATA IS READ. Only the DAG is constructed.    │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PHASE 2: GRAPH EXECUTION (Run Time)                      │
│                                                                             │
│ • Engine: Databricks Lakeflow Spark Runtime (Serverless / Classic)          │
│ • DLT executes the registered DAG in topological dependency order           │
│ • Lazy closures execute: Auto Loader streams, ASN.1 mapInPandas decode,     │
│   DQ expectation evaluation, AES column encryption, and dlt.apply_changes   │
│ • Target Delta tables & Lakeflow sinks are materialized transactionally     │
└─────────────────────────────────────────────────────────────────────────────┘
```

> [!NOTE]
> **Graph Definition Safety**: Code executing inside Phase 1 cannot inspect data row counts or access streaming buffers. All DataFrame transformations (e.g. `build_dataframe()`, quarantine filtering) are defined as lazy closures that execute only during Phase 2.

---

## 3. Medallion Architecture Implementation

Metaflow structures data processing across the standard Medallion layers while enforcing enterprise governance:

```
                  ┌──────────────────────────────┐
                  │      SOURCE DATA ASSETS      │
                  │ Object Storage / Kafka / Bus │
                  └──────────────┬───────────────┘
                                 │
                                 ▼ Ingestion Flow (Auto Loader / Zerobus / ASN.1)
                  ┌──────────────────────────────┐
                  │         BRONZE LAYER         │
                  │   Raw Streaming / Append     │
                  │   Technical Metadata Added   │
                  │   (Optional PII Encryption)  │
                  └──────────────┬───────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
  ┌──────────────────────────────┐ ┌──────────────────────────────┐
  │     DQ QUARANTINE TABLE      │ │         SILVER LAYER         │
  │   Failed Records Routed Here │ │   Cleaned & Validated Data   │
  │   Full Error Diagnostics     │ │   SCD1 / SCD2 Dimensions     │
  └──────────────────────────────┘ └──────────────┬───────────────┘
                                                  │
                                                  ▼ Transformation Flow (SQL Joins / Aggs)
                                   ┌──────────────────────────────┐
                                   │          GOLD LAYER          │
                                   │   Business Aggregates / MVs  │
                                   │   Reconciliation Verified    │
                                   └──────────────┬───────────────┘
                                                  │
                                                  ▼ Sinks & Observability
                                   ┌──────────────────────────────┐
                                   │      EGRESS & TELEMETRY      │
                                   │   Delta / Kafka / OTLP / PGP │
                                   └──────────────────────────────┘
```

---

## 4. Control Plane & Metadata Schema (ERD)

All pipeline configurations are stored in 8 Delta control tables located in the `{catalog}.config` schema. When an onboarding spec is submitted, `notebooks/02_onboarding/02_onboarding_engine.py` validates the schema and executes an idempotent `MERGE INTO` upsert:

```
┌────────────────────────────────┐
│      dataflow_group_spec       │
├────────────────────────────────┤
│ PK: dataflow_group_id (STRING) │
│     environment (STRING)       │
│     catalog_name (STRING)      │
│     has_ingestion_flows (BOOL) │
│ has_transformation_flows(BOOL) │
│     pipeline_parameters_json   │
│     is_active (BOOL)           │
│     created_at / updated_at    │
└───────────────┬────────────────┘
                │ 1:N
        ┌───────┴───────────────────────────────┬───────────────────────────────┐
        ▼                                       ▼                               ▼
┌──────────────────────────────┐ ┌──────────────────────────────┐ ┌──────────────────────────────┐
│     ingestion_flow_spec      │ │   transformation_flow_spec   │ │   reconciliation_flow_spec   │
├──────────────────────────────┤ ├──────────────────────────────┤ ├──────────────────────────────┤
│ PK: dataflow_id (STRING)     │ │ PK: flow_step_id (STRING)    │ │ PK: reconciliation_id (STR) │
│ FK: dataflow_group_id        │ │ FK: dataflow_group_id        │ │ FK: dataflow_group_id        │
│     source_type (STRING)     │ │     dataflow_id (STRING)     │ │     source_config_json       │
│     target_catalog (STRING)  │ │     source_inputs_json       │ │     target_configs_json      │
│     target_schema (STRING)   │ │     transformation_sql       │ │     match_keys_json (STR)    │
│     target_table (STRING)    │ │     target_catalog (STRING)  │ │     compare_columns_json     │
│     target_type (STRING)     │ │     target_schema (STRING)   │ │     error_handling_json      │
│     cdc_load_strategy (STR)  │ │     target_table (STRING)    │ │     match_keys_json (STR)    │
│     source_config_json (STR) │ │     target_type (STRING)     │ │     is_active (BOOL)         │
│     target_config_json (STR) │ │     cdc_load_strategy (STR)  │ └──────────────┬───────────────┘
│     dq_config_json (STR)     │ │     target_config_json (STR) │                │ 1:N
│     governance_tags_json     │ │     dq_config_json (STR)     │        ┌───────┴───────────────┐
│     is_active (BOOL)         │ │     governance_tags_json     │        ▼                       ▼
└──────────────────────────────┘ └──────────────────────────────┘ ┌──────────────┐ ┌─────────────┐
                                                                  │ recon_run_log│ │mismatch_log │
                                                                  └──────────────┘ └─────────────┘
```

`dataflow_group_spec` gains one column in v1.3.0, appended after `pipeline_parameters_json`:
`spark_config_json (STRING, nullable)` — a JSON object of group-scoped Spark configuration. See
§5 below for the precedence chain it participates in.

---

## 5. Hierarchical Spark Configuration

Before v1.3.0 the framework set **no** Spark configuration of its own: the pipeline notebook read
exactly two `configuration:` keys (`dataflow.group.id`, `dataflow.control.catalog`) and every
tuning knob — shuffle partitions, Delta optimize-write, anything — was whatever the Lakeflow
runtime happened to default to. There was no metadata-driven way to say "this group shuffles
wide, give it different partitioning" short of hand-editing notebook code, and no way at all to
express a framework-wide starting point. v1.3.0 introduces a three-layer, strictly-overriding
precedence chain, resolved by the new `engine/spark_config.py`.

### The three layers, lowest to highest

1. **Framework built-in defaults** — `FRAMEWORK_SPARK_DEFAULTS` in `engine/spark_config.py`. As of
   v1.3.0 this dict carries exactly one entry, `{"spark.sql.shuffle.partitions": "200"}` — Spark's
   own default restated explicitly. Restating it costs nothing behaviorally, but it makes the
   value show up in the framework's applied-configuration log line, turning "why is this pipeline
   shuffling into 200 files" into a one-line log lookup instead of a runtime archaeology exercise.
   This layer is a starting point, never an opinion that should outrank a human's.
2. **The onboarding spec's group-scoped `spark_config` object** — a new top-level property, a
   sibling of `pipeline_parameters` (not nested inside any flow's `source_config`/`target_config`),
   persisted to `dataflow_group_spec.spark_config_json` and applied to the pipeline session
   **before any flow is registered** — every `@dlt.table`/`@dlt.view` closure the notebook
   subsequently defines executes under it. This layer beats the framework defaults because a spec
   author knows their workload better than the framework does.
3. **The pipeline resource's own `configuration:` block**, specifically the key
   `dataflow.spark.conf`, whose value is a **JSON object encoded as a string** — the same
   convention `dataflow.otel_streaming.event_log_tables` already uses in
   `resources/observability_otel_streaming_pipeline.yml` (a bundle `configuration:` block can only
   hold flat string values, so a nested mapping has to travel as JSON text and get decoded at
   read time).

**Why the bundle YAML wins.** The pipeline resource is the deployment-time, per-environment
artifact an operator edits for dev/staging/prod. It must be able to override metadata that was
onboarded *once* and is shared across every environment, without forcing a re-onboarding cycle —
so it sits at the top, not the bottom, of the chain.

**Why an explicit, framework-owned key rather than "whatever the session already has."** An
implicit "only set it if nobody else did" test is not implementable:
`spark.conf.get("spark.sql.shuffle.partitions")` returns Spark's own default (`200`) whether or
not a human ever set it, so "already set" and "never set" are indistinguishable at read time.
Reading one dedicated key (`dataflow.spark.conf`) that only this framework writes is what makes
the precedence decidable at all.

### Canonical acceptance case

```
FRAMEWORK_SPARK_DEFAULTS["spark.sql.shuffle.partitions"] = "200"
```
```yaml
# resources/*_pipeline.yml
configuration:
  dataflow.group.id: dfg_orders
  dataflow.control.catalog: metaflow
  dataflow.spark.conf: '{"spark.sql.shuffle.partitions": "auto"}'
```
resolves to `spark.sql.shuffle.partitions = "auto"` — the pipeline resource's value strictly
overrides the framework default for that one key, and `apply_spark_conf` calls
`spark.conf.set("spark.sql.shuffle.partitions", "auto")` on the session before flow registration.
A key the bundle does not mention (e.g. one only the onboarded `spark_config` sets) is untouched
by this override — resolution is a per-key **merge** across the three layers, never a wholesale
replace.

### Onboarding spec surface

```json
{
  "dataflow_group_id": "dfg_orders",
  "pipeline_parameters": {},
  "spark_config": {
    "spark.sql.shuffle.partitions": "auto",
    "spark.databricks.delta.optimizeWrite.enabled": true
  },
  "ingestion_flows": []
}
```

`onboarding/spec_validator.py::_validate_spark_config` enforces the surface at onboarding time:
every key must be a string starting with `"spark."`, and every value must be a `str`/`int`/
`float`/`bool` — a dict value, a list value, or a key without the `spark.` prefix is a validation
error before the spec is ever persisted.

### Value coercion and failure tolerance

Every resolved value is rendered with `str(value)` before `spark.conf.set` — **except** Python
booleans, which render as the lowercase `"true"`/`"false"` Spark itself expects (`str(True)`
produces `"True"`, which several Spark boolean parsers reject). JSON numbers therefore become
`"200"`, not `200`.

Nothing in `engine/spark_config.py` may ever fail a pipeline update. A malformed
`dataflow.spark.conf` JSON string is logged and ignored (falls back to the two lower layers); a
key `spark.conf.set` refuses — most commonly a **static** SQL configuration
(`AnalysisException: Cannot modify the value of a static config`), or a key that simply does not
exist on the running DBR — is caught per-key, logged at `WARNING`, and skipped, never raised:

```
Could not apply Spark configuration '%s' = %r: %s -- this is typically a static SQL configuration
that cannot be changed after the session starts. Continuing without it.
```

This is tuning metadata; losing a shuffle-partition hint is a performance regression, while
raising would take down an otherwise-correct ingestion graph over a mis-typed key.

### Distinct from `pipeline_parameters`

`spark_config` is **not** the same mechanism as the pre-existing top-level `pipeline_parameters`.
`pipeline_parameters` does `${param}` string substitution into SQL and file paths
(`transformation/parameters.py`); `spark_config` calls `spark.conf.set(...)` on the live Spark
session. Putting a Spark tuning key under `pipeline_parameters`, or a substitution placeholder
under `spark_config`, is a no-op at best and a validation error at worst — keep the two separate
even though both are top-level, group-scoped objects.

> [!NOTE]
> **Current persistence gap (code vs. contract).** `engine/spark_config.py`, the notebook wiring
> in `03_lakeflow_declarative_pipeline.py`, the `spark_config_json` column in
> `control_plane/ddl_definitions.py`, and the `spark_config` validation in `spec_validator.py` are
> all implemented and consistent with the design above. However, as of this release
> `onboarding/metadata_upsert.py`'s `_DATAFLOW_GROUP_SPEC_SCHEMA`, its `group_row` builder, and its
> `MERGE` update map do **not** include `spark_config_json` — a validated `spark_config` block is
> never written to the control table by the onboarding engine. Until that gap is closed, layer 2
> of the chain resolves to `{}` for every group onboarded through the standard flow (the pipeline
> notebook's `getattr(GROUP_ROW, "spark_config_json", None)` degrades gracefully to "no group-level
> config" rather than failing), and only layers 1 and 3 are effective in practice. The frozen
> v1.3.0 contract calls for `metadata_upsert.py` to be updated; the code does not yet reflect that.

---

## 6. Architectural Invariants & Design Principles

1. **Pure Delta Foundation**: All storage is Delta Lake. Setting `storage_format: "iceberg"` enables [UniForm (Universal Format)](https://docs.databricks.com/delta/uniform.html) Iceberg read metadata generation over Delta files.
2. **Unity Catalog Three-Level Namespace**: All table and secret references adhere to `catalog.schema.table` and `secret:<scope>:<key>` or Unity Catalog secret paths.
3. **Decoupled Governance DDL**: Applying `ALTER TABLE ... SET TAGS` is handled as a post-deployment task (`04_apply_governance_and_egress.py`) because Databricks prohibits catalog DDL within active DLT streaming micro-batches.
4. **Native In-Graph Sinks**: Egress sinks (`target_type: "sink"` / `"external_sink"`) are registered as native Lakeflow sink flows (`dlt.create_sink` + `@dlt.append_flow`), ensuring exactly-once processing without external batch scripts.
