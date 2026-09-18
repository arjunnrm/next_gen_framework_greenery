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
   `resources/observability/observability_otel_streaming_pipeline.yml` (a bundle `configuration:` block can only
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
# resources/<group>/*_pipeline.yml
configuration:
  dataflow.group.id: dfg_orders
  dataflow.control.catalog: flowx
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
> **Persistence (verified 2026-09-15).** `onboarding/metadata_upsert.py` declares `spark_config_json` in
> `_DATAFLOW_GROUP_SPEC_SCHEMA`, writes it in the group row (`json.dumps(spec.get("spark_config", {}))`) and
> carries it in the `MERGE` update map, so a validated `spark_config` block reaches
> `dataflow_group_spec.spark_config_json` and layer 2 of the chain is live for every group onboarded through
> the standard flow. An earlier release documented this as a gap; it is closed.

---

## 6. Architectural Invariants & Design Principles

1. **Pure Delta Foundation**: All storage is Delta Lake. Setting `storage_format: "iceberg"` enables [UniForm (Universal Format)](https://docs.databricks.com/delta/uniform.html) Iceberg read metadata generation over Delta files.
2. **Unity Catalog Three-Level Namespace**: All table and secret references adhere to `catalog.schema.table` and `secret:<scope>:<key>` or Unity Catalog secret paths.
3. **Decoupled Governance DDL**: Applying `ALTER TABLE ... SET TAGS` is handled as a post-deployment task (`04_apply_governance_and_egress.py`) because Databricks prohibits catalog DDL within active DLT streaming micro-batches.
4. **Native In-Graph Sinks**: Egress sinks (`target_type: "sink"` / `"external_sink"`) are registered as native Lakeflow sink flows (`dlt.create_sink` + `@dlt.append_flow`), ensuring exactly-once processing without external batch scripts.
5. **One Group, One DAG (v1.5.0)**: ingestion, transformation **and** reconciliation are all registered into the *same* Lakeflow pipeline update for a dataflow group. Reconciliation is a third first-class flow type, opted into per flow with `execution_mode` (default `"job"`, so nothing already deployed changes). Observability deliberately stays a normal Lakeflow **job task** and is unchanged. See [§7](#7-the-read-once-source-plane) and [`07_reconciliation_engine.md` §11](07_reconciliation_engine.md#11-execution-modes-job-pipeline-pipeline_audit_only).
6. **The Single-Read DAG (v1.5.0)**: every source — ingestion source, transformation input, both reconciliation sides — is routed through the source plane. An *external* read (`spark.read`/`spark.readStream` against a storage path, Delta location or external catalog) occurs exactly once per pipeline per required source table, per execution mode, and every downstream consumer reaches it via `dlt.read()`/`dlt.read_stream()` instead of re-reading the origin. See [§7](#7-the-read-once-source-plane) and `AGENTS.md`'s four-rule statement of the mandate.

---

## <a id="7-the-read-once-source-plane"></a>7. The Single-Read DAG Source Plane

> This section is the mechanism behind the **Single-Read DAG mandate** stated as four rules in
> `AGENTS.md`. Rules 1 (one external read per source table, per execution mode), 2 (downstream
> lineage through `dlt.read()`) and 4 (`materialize` defaults to `"always"`; `"never"` is
> hard-rejected) are all enforced here. Rule 3 — "no intermediate tables" means no throwaway
> staging tables, and explicitly does *not* apply to the base ingestion nodes below — is covered
> by the Intermediate Object Rule later in this section.

**New in v1.5.0** (`engine/source_plane.py`, `engine/identifiers.py`). Before v1.5.0, each
consumer opened its own read of a source. Two quarantine rules over one Auto Loader path meant
two `cloudFiles` streams over that path; nine transformation inputs over six distinct tables
meant nine reads. The source plane makes one physical read per distinct physical locator per
update, and hands every consumer a binding to it.

### Three pure phases

| Phase | Function | Property |
|---|---|---|
| 1 | `plan_source_plane(...)` | Pure Python. No `dlt`, no Spark action, no workspace — unit-testable on a laptop. Computes one `ReadIdentity` per distinct locator, counts fan-out, and runs `assert_acyclic()`. |
| 2 | `register_source_plane(spark, plan)` | Registers the shared nodes the plan decided on — and nothing else. |
| 3 | `bind(plan, consumer_id, want_stream)` | Called from each consumer's closure. Returns the DataFrame that consumer should build on. |

`bind()` is keyed on a **consumer id**, never on a re-derived identity: only the plan pass ever
computes an identity, so plan and bind cannot disagree about what is shared with what.

### The three binding kinds

| Kind | When | What the consumer gets |
|---|---|---|
| `in_graph_sibling` | the locator is a table **this same group publishes** | `dlt.read(...)` / `dlt.read_stream(...)` — a real graph edge, same-update fresh by topological order, and no second physical read because the producer already materialized it. No plane node: one would be a *second* read of something the graph already produces. |
| `shared_node` | **every** external locator, at any fan-out (v1.7.3 Single-Read mandate) | one materialized `_src__<locator>__<8hex>__{stream,batch}` node. A **streaming table** if *any* consumer streams, a materialized view otherwise — never the reverse, because an MV emits update/delete commits and cannot be a streaming source. **v1.6.0:** the node is a pipeline-scoped `@dlt.table(temporary=True)` under its bare name — still materialized once per update (read-once holds; a view is still never used for a shared node), but never published to Unity Catalog — unless the spec sets **both** `source_plane.catalog` and `source_plane.schema`, which is the explicit opt-in for a published, durable, queryable node. |
| `inline` | **never chosen since v1.7.3** | Formerly: external, fan-out 1 (or `materialize: "never"`) — the read evaluated inside the single consumer, preserving predicate pushdown of that consumer's filter into the original source. The `Binding.kind` value and its branch remain in the code as dead-code defence, but the planner no longer reaches them for any external identity. |

### What is in the identity, and what is not

`ReadIdentity(locator_kind, locator, options_fingerprint)` is internal to `source_plane.py`.

* **Locator** — a `casefold()`ed `catalog.schema.table`, or a `${param}`-substituted,
  trailing-slash-stripped path. Casefolding is load-bearing: a case-sensitive miss would silently
  fall through to a duplicate read.
* **In the fingerprint (base-read options — they change *which bytes are scanned*)**: `format`,
  `schema_location`, `schema_evolution_mode`, `file_pattern`, `reader_options`,
  `landing_retention_policy`, `source_zip_handling`, source catalog/schema/table,
  `starting_version`, `max_bytes_per_trigger`.
* **Not in the fingerprint (overlays — applied per consumer, downstream of the shared read)**:
  `schema_config`, `column_normalization`, technical metadata, `json_string_columns` /
  `explode_columns` / `auto_flatten_all`, `remove_dups`, `data_standardization_sql`,
  `decrypted_columns`, watermarks, `filter_condition`, hashing for matching, DQ/quarantine
  columns, encryption. **Two consumers that shape the data differently still share one read.**
* **Execution mode *is* part of the node identity — the mandate's one deliberate exception.** A
  locator consumed both as a stream and as a batch legitimately yields two base nodes,
  `…__stream` and `…__batch`, because streaming and batch run on different primitives
  (checkpointed continuous state vs. a point-in-time snapshot) and forcing one binding onto the
  other introduces checkpoint locking and full-refresh side effects. `bind()` raises rather than
  silently reading a materialized view as a stream. Within a single mode, one materialized
  streaming table serves `dlt.read_stream` *and* `dlt.read` consumers in the same update; a
  `@dlt.view` can serve neither pair (see [`13_known_limitations_and_gotchas.md` L7](13_known_limitations_and_gotchas.md#l7)).

The node name always carries an 8-hex digest of the locator — never only on truncation — because
sanitizing maps every non-identifier character to `_`, and `flowx.bronze.a_b` and
`flowx.bronze_a.b` would otherwise collide into one name and fail the whole update with
*"Cannot redefine dataset"*.

### Guards, at plan time, before a single `dlt` call

* **Cycles** — `assert_acyclic()` runs a Kahn topological sort over the full edge set and raises
  `FrameworkGraphCycleError` naming the exact ring. Strictly stronger than a list of pairwise
  rules: it catches multi-hop rings nobody enumerated.
* **Streaming a non-append-only locator** — `want_stream` against a MERGE-written or overwritten
  target raises `FrameworkConfigError` naming the locator, the producing flow and
  `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. `skipChangeCommits` is refused.
* **Competing file lifecycles** — more than one distinct `landing_retention_policy` or
  `source_zip_handling` on one path is rejected at onboarding *and* at plan time. Two lifecycle
  regimes on one directory is a latent data-loss bug; sharing the read is also the fix, because
  the side effects then run exactly once.

`describe_plan(plan)` emits one structured `source_plane_node` event per node, so *"was my table
actually read once?"* is answerable from the log stream rather than by inference.

> **Materialization is a real cost, deliberately accepted.** A shared node is a full physical
> copy (pipeline-managed storage for a temporary node, UC storage for a published one), an extra
> DAG step, and — the part usually missed — it destroys predicate
> pushdown of a consumer's filter into the *original* source.
>
> Until v1.7.3 the framework tried to avoid paying that cost where it seemed unnecessary:
> `materialize` defaulted to `"auto"` (a node only at fan-out ≥ 2) and `"never"` existed as an
> escape hatch for a huge, heavily-filtered table. **v1.7.3's Single-Read architectural mandate
> reverses that trade.** `materialize` now defaults to `"always"`: every external source identity
> is materialized into its own base node, so N source tables produce N base ingestion nodes and
> the "was this read once?" question has one answer everywhere instead of depending on fan-out.
> `"never"` is prohibited and rejected at onboarding time (and again at plan time, for
> control-table rows written before the mandate); `"auto"` is still accepted but resolves to
> `"always"`. The pushdown cost is real and is now simply paid — a predictable graph is judged
> worth more than a per-consumer optimization that made the topology depend on fan-out.

### Where a shared node lives — `source_plane.catalog` / `source_plane.schema` (semantics changed in v1.6.0)

Since v1.6.0 an L0 node is plumbing, not a deliverable — the framework-wide **Intermediate Object
Rule**: an intermediate dataset is a `@dlt.view` when it has a single reader, and a pipeline-scoped
`@dlt.table(temporary=True)` under its bare name when materialization is required (multi-reader
read-once, or an API that demands a table); only final sinks are durable published tables.
Accordingly:

* **`source_plane.catalog` / `source_plane.schema` both null (the default)** — the shared node is
  registered as `@dlt.table(temporary=True)` under its bare `_src__…` name. Read-once still holds
  (it is materialized, never a view), but the node never appears in Unity Catalog.
  **This is a semantic change:** pre-v1.6.0, null fell back to the hosting pipeline's own
  catalog/schema, so every shared node was a published table.
* **Both set** — the node is published as `catalog.schema._src__…`, the explicit opt-in for a
  durable, externally queryable node (`PlaneNode.published = True`). Setting only one of the two
  behaves as unpublished.

Upgrading an already-deployed pipeline whose plan produced shared nodes renames/unpublishes those
datasets on the first post-v1.6.0 update — see
[`13_known_limitations_and_gotchas.md` O7](13_known_limitations_and_gotchas.md#o7).

---

## 8. The Engine Notebook — One Registrar, Three Loops

**New in v1.5.0.** Reconciliation used to be something that happened *after* a pipeline: a
`05_reconciliation_engine.py` notebook task, wired into the job downstream of the pipeline task,
reading the already-materialized target with its own Spark session. That is still the default
(`execution_mode: "job"`, which is also what a `NULL` column reads back as, so nothing already
deployed changes). What v1.5.0 adds is the ability to register a reconciliation flow as a **third
first-class flow type inside the same Lakeflow update** as the ingestion and transformation flows
of its dataflow group — the same graph, the same topological ordering, the same event log.

### 8.1 The three hard requirements

| | Requirement | Where it is enforced |
|---|---|---|
| **R1** | Ingestion, transformation **and** reconciliation all run inside **one** Lakeflow DAG per dataflow group. | `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` — three registration loops in one notebook; `reconciliation/graph_registration.py` for the recon nodes. |
| **R2** | Every external source table is read **exactly once** per update, per execution mode, and every downstream consumer reaches it through the DAG (`dlt.read`/`dlt.read_stream`) rather than re-reading the origin. | `engine/source_plane.py` — the L0 plane of [§7](#7-the-read-once-source-plane). Both reconciliation sides go through it like any other read. See `AGENTS.md`'s four-rule Single-Read DAG mandate. |
| **R3** | Observability stays a normal Lakeflow **job task** and is *never* folded into the pipeline. | `notebooks/08_observability/08_dlt_observability_engine.py` (triggered) and the separate `06_event_log_otel_streaming_pipeline.py` (continuous) — neither is reachable from the engine notebook. See [§8.4](#84-r3-observability-is-still-a-job-task). |

### 8.2 The actual shape of the notebook

After v1.5.0 the engine notebook is a **thin registrar**. The two ~90-line flow-generator bodies
that used to live inline moved verbatim into `engine/flow_generators.py`, where they are importable
and unit-testable without a workspace. What is left, stripped of logging and config resolution, is
literally this:

```python
# Phase 1 — plan (pure Python: no dlt, no Spark action, no workspace call)
PLAN = plan_source_plane(
    MD.ingestion_rows,
    MD.transformation_rows,
    MD.reconciliation_rows,
    ...,
)

# Phase 2 — register the shared L0 nodes the plan decided on, and nothing else.
# MUST precede phase 3: Lakeflow resolves dlt.read/dlt.read_stream by dataset name at
# graph-build time, so a node a generator binds to has to be defined already.
register_source_plane(spark, PLAN)

# Phase 3 — three bare loops over the three control-table row sets.
for _ingestion_row in MD.ingestion_rows:
    generate_ingestion_flow(spark, dbutils, _ingestion_row, plan=PLAN, ...)

for _transformation_row in MD.transformation_rows:
    generate_transformation_flow(spark, dbutils, _transformation_row, plan=PLAN, ...)

for _reconciliation_row in MD.reconciliation_rows:
    generate_reconciliation_flow(
        spark, _reconciliation_row, plan=PLAN,
        publish_catalog=PIPELINE_CATALOG, publish_schema=PIPELINE_SCHEMA,
        control_schema=CONTROL_SCHEMA, pipeline_update_id=PIPELINE_RUN_ID, ...,
    )
```

Three properties follow directly from that shape and are worth stating explicitly:

* **An empty row set is a loop that does not execute.** One `dataflow_group_id` yields a unified,
  ingestion-only, transformation-only, reconciliation-only — or any combination — DAG with no
  separate code path and no branching.
* **`MD.reconciliation_rows` is already filtered.** `load_active_group_metadata` returns only rows
  whose `execution_mode` is `"pipeline"` or `"pipeline_audit_only"`; `"job"`-mode rows never reach
  the notebook and remain the property of the standalone job task, whose behaviour is unchanged.
* **No generator reads a source directly.** Each resolves its physical reads through
  `bind(PLAN, consumer_id, want_stream)` — that is what makes R2 structural rather than a
  convention someone has to remember.

### 8.3 Where L0 sits relative to the three flow types

The source plane is not "an ingestion feature." It is a layer *underneath* all three loops, which is
precisely why reconciliation could join the DAG without doubling the scan cost:

```
                         ┌───────────────────────────────────────────────┐
                         │  L0 · SOURCE PLANE   register_source_plane()  │
                         │  one physical read per distinct locator       │
                         │  _src__<locator>__<8hex>__{stream,batch}      │
                         │  (or: inline @ fan-out 1 / in_graph_sibling)  │
                         └───┬───────────────┬───────────────────┬───────┘
                             │ bind()        │ bind()            │ bind()
              ┌──────────────▼──┐   ┌────────▼─────────┐   ┌─────▼───────────────┐
              │ LOOP 1          │   │ LOOP 2           │   │ LOOP 3  (v1.5.0)    │
              │ ingestion_rows  │   │ transformation_  │   │ reconciliation_rows │
              │                 │   │ rows             │   │ execution_mode in   │
              │ L1  _<tbl>_     │   │ L2  <input_name> │   │ {pipeline,          │
              │     staged      │   │     alias views  │   │  pipeline_audit_    │
              │     → CDC/DQ    │   │     → CDC/DQ     │   │  only}              │
              │     → target    │   │     → target     │   │ L3 prepare src/tgt  │
              │                 │   │                  │   │ L4 classified /     │
              │                 │   │                  │   │    metrics /        │
              │                 │   │                  │   │    mismatch         │
              │                 │   │                  │   │ L5 pulse → heal     │
              └─────────────────┘   └──────────────────┘   └─────────────────────┘
                          ONE Lakeflow update, one topological sort
```

A reconciliation side that happens to be a table **this same group publishes** does not become an
L0 node at all — it binds as an `in_graph_sibling` (`dlt.read` / `dlt.read_stream`), which is a real
graph edge and therefore same-update fresh by topological order. That is the mechanism behind R1:
the comparison genuinely sees what this update just wrote, without a second physical read and
without a second job task.

Two consequences of living in the graph, both real constraints rather than footnotes:

* A `read_mode: "streaming"` reconciliation side is **rejected at onboarding** in both pipeline
  modes — the in-graph comparison is a whole-snapshot batch classification, and a stream-static
  join cannot express `MISSING_IN_SOURCE`.
* The L0 streaming guard applies to the reconciliation source like any other: an in-graph producer
  that is not append-only — `TRUNCATE_AND_LOAD`, `SCD1`/`SCD2`/`SCD3`, `FULL_SNAPSHOT_CDC`, or any
  `materialized_view` — cannot be streamed from, so the L5 heal lane is unavailable and the flow
  must use `execution_mode: "pipeline_audit_only"`. See
  [`12_module_permutation_matrix.md` §4.1](12_module_permutation_matrix.md#41-execution_mode-what-is-legal-v150).

Layer-by-layer node names, the full DAG picture, and the heal-lane ordering edge:
[`07_reconciliation_engine.md` §11.1–§11.2](07_reconciliation_engine.md#111-the-l0l5-node-map).

### 8.4 R3 — observability is still a job task

Reconciliation moved into the pipeline. **Observability deliberately did not, and no part of
v1.5.0 changes it.** The reason is lifecycle, not effort:

* The triggered observability engine (`08_dlt_observability_engine.py`) reads the **event log of a
  pipeline update that has already finished**. A node inside that same update could not read its
  own update's completed event log — the rows it wants do not exist until after the update it would
  be part of has ended.
* The continuous engine (`06_event_log_otel_streaming_pipeline.py`) is a separate always-on
  `continuous: true` pipeline by design. Folding a never-terminating export flow into a bounded
  data pipeline would mean the data pipeline never reports completion.

So a dataflow group's job graph after v1.5.0 is: *(optional) onboarding task → pipeline task
(ingestion + transformation + reconciliation) → observability task*. Reconciliation left the
right-hand side of that chain; observability stayed.

### 8.5 Resolving the publish schema (why `PIPELINE_SCHEMA` has a fallback chain)

`generate_reconciliation_flow` needs a catalog **and** a schema to name its L3–L5 nodes before any
of them is defined. During graph definition `spark.catalog.currentDatabase()` is *not* the
pipeline's target schema, so reading it alone yields `None` even for a pipeline that plainly
declares `schema: bronze_excalibur`, and the first `_node_name()` call fails with
`ValueError: Unsafe or malformed target_schema: None`. The notebook therefore resolves the schema
through the same style of chain the catalog already had:

```
spark.conf "pipelines.schema"  →  spark.conf "pipelines.target"
  →  spark.catalog.currentDatabase()  →  GROUP_ROW.target_schema   (warn if all miss)
```

This matters for any reconciliation flow that sets no explicit `publish_schema`; a flow that does
set one overrides the chain for its own nodes.

### 8.6 Verification status

Verified live on **2026-08-31**, target `dev_flowx`: job `flowx_test_recon_dag_job`
(`854232399214818`) ran `setup_control_tables → seed → onboard → run pipeline` to SUCCESS, and
pipeline `be78d88d-6064-414d-a10c-2aacd900fa86`
(`flowx_test_003_autoload_recon_pipeline`) registered — in **one update**, per its own event
log — the ingestion streaming table, both L3 prepare nodes, all three L4 comparison datasets, the
L5 pulse streaming table, the heal `@dlt.append_flow`, and the `foreach_batch` heal sink. R1 and R2
are therefore observed, not inferred.

One design assumption changed as a result: `dlt.foreach_batch_sink` **is** available on DBR
serverless. The design previously treated that as unproven and positioned `pipeline_audit_only` as
the fallback if it were missing; the heal lane is now confirmed real. The `hasattr` guard around
`register_foreach_batch_sink` stays — the local `databricks-dlt` 0.3.0 stub still lacks the symbol,
so the guard is what keeps unit tests runnable off-cluster — but it is no longer expected to trip
on DBR.

> [!WARNING]
> **The v1.5.0 control-table columns do not arrive via `bundle deploy`.** Every statement in
> `get_all_control_table_ddls` is `CREATE TABLE IF NOT EXISTS`, a no-op against a table that already
> exists, so a column added to a `CREATE` statement reaches **new installations only**. v1.5.0 adds
> a strictly additive migration (`ADDITIVE_CONTROL_TABLE_COLUMNS` + `get_add_column_ddl()` in
> `control_plane/ddl_definitions.py`, applied by `ensure_control_table_columns()` in
> `control_plane/schema_provisioner.py`) that adds `execution_mode`, `publish_schema` and
> `dq_config_json` to an existing `reconciliation_flow_spec`. It is applied **only by running the
> `setup_control_tables` task**, never by deploying the bundle. On a workspace provisioned before
> v1.5.0 and not migrated, pipeline-mode onboarding fails with `UNRESOLVED_COLUMN` — observed live.
> Recipe: [`09_developer_guide_and_recipes.md` §5](09_developer_guide_and_recipes.md#5-recipes-in-pipeline-reconciliation-and-workspace-bring-up).

