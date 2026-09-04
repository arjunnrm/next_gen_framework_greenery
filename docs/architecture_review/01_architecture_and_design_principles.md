# FlowX Architecture Review — Pillar 1: Architecture & Design Principles

**Evaluation Area:** Modular Architecture, Medallion Alignment, Dynamic DAG Generation, Control Plane & Governance Integration  
**Score:** 8.5 / 10  
**Status:** Highly Robust with Minor Orchestration Couplings

---

## 1. Architectural Overview & Design Evaluation

FlowX establishes an extensible, metadata-driven architecture for orchestrating streaming and batch data pipelines on Databricks. It decouples pipeline definition from procedural code by maintaining declarative flow specifications in control tables, dynamically compiling these specifications into a **Lakeflow Declarative Pipelines** DAG at runtime.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                FLOWX CONTROL PLANE TO EXECUTION DAG                           │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
   [Onboarding Spec (YAML/JSON)]
               │
               ▼
   [onboarding/spec_validator.py] ──> [onboarding/metadata_upsert.py]
                                                   │
                                                   ▼
                           [(Control Plane Delta Tables: <catalog>.config)]
                           ├── dataflow_group_spec
                           ├── ingestion_flow_spec
                           ├── transformation_flow_spec
                           ├── reconciliation_flow_spec
                           └── observability_config
                                                   │
               ┌───────────────────────────────────┴───────────────────────────────────┐
               ▼                                                                       ▼
   [03_lakeflow_declarative_pipeline.py]                                   [05_reconciliation_engine.py]
   (Core DLT Declarative Engine)                                           (Standalone Job Task)
   ├── Ingestion Flows                                                     ├── Hash-First Matcher
   │   ├── cloudFiles / Zerobus / ASN.1                                    ├── Idempotent Appender
   │   ├── Staged View (@dlt.view)                                         ├── Mismatch Logger
   │   ├── Dual-Target Quarantine (@dlt.table)                             └── Run Log Ledger
   │   └── CDC Dispatch (dlt.apply_changes)                                           │
   ├── Transformation Flows                                                            ▼
   │   ├── Watermarked Multi-Inputs (@dlt.view)                            [08_dlt_observability_engine.py]
   │   ├── SQL Transformation                                              (Downstream Telemetry Exporter)
   │   └── CDC Dispatch (dlt.apply_changes)                               ├── Event Log Extractor
   └── Lakeflow Sinks                                                      ├── OTel ResourceLogs Formatter
       └── dlt.create_sink + @dlt.append_flow                              └── OTLP HTTP / Volume Dispatch
```

---

## 2. Deep-Dive Component Analysis

### 2.1 Medallion Architecture Alignment
- **Bronze Layer (Raw Ingestion):** Ingests streaming files via Auto Loader (`cloudFiles`), Delta change streams (`zerobus`), or binary files (`asn1`, `pgp_zip`). Ingestion flows attach standardized technical metadata (`__framework_source_file_name`, `__framework_source_file_size`, `__framework_source_file_modification_time`, `__framework_ingestion_timestamp_utc`) and support schema normalization and explicit type casting via `schema_config`.
- **Silver Layer (Cleaned, Conformed, SCD Dimensions & Facts):** Handles multi-input transformations with stream-stream watermarking, dynamic parameter substitution (`${param}`), data quality quarantine routing, and CDC materialization (`SCD1`, `SCD2`, `SCD3`, `FULL_SNAPSHOT_CDC`).
- **Gold Layer (Aggregations, Data Products & Egress Sinks):** Supports materialized views, batch tables, and native egress exports (`target_type: "sink"` / `"external_sink"`) delivering conformed data directly to Kafka, external Delta locations, or PGP-encrypted ZIP archives.

### 2.2 Dynamic DAG Generation (`engine/flow_registration.py` & `notebooks/03_engine/`)
- **Evaluation:** The dynamic DAG generation mechanism is exemplary. Instead of statically declaring `@dlt.table` decorators in Python scripts, `03_lakeflow_declarative_pipeline.py` reads active flows for the provided `dataflow.group.id`, iterates through each specification, and dynamically binds closure functions to `@dlt.table`, `@dlt.view`, and `dlt.create_sink`.
- **Closure Isolation:** Helper factories (such as `_make_input_view`, `_make_staged_view`, and `_make_quarantine_table`) correctly use default keyword argument binding (e.g., `def _closure(staged_name=staged_name): ...`) to prevent Python loop variable scoping collisions.
- **Qualified Naming:** Uses `qualified_table_name(catalog, schema, table)` to pass three-level identifiers (`catalog.schema.table`) to `@dlt.table(name=...)`, allowing a single Lakeflow pipeline update to materialize datasets across multiple schemas and catalogs seamlessly.

### 2.3 Sink & Egress Modernization (`engine/sink_registration.py`)
- **Evaluation:** The architecture has successfully transitioned from legacy post-deployment egress notebooks to native in-graph Lakeflow sinks using `dlt.create_sink()` and `@dlt.append_flow()`.
- **Supported Sinks:** Native support for `delta` (external Delta tables/locations), `kafka` (Kafka topic streaming writer with topic, bootstrap servers, and SASL/JAAS options), and `pgp_zip` (custom PySpark streaming data source writing compressed and encrypted JSONL partitions).

### 2.4 Control Plane & Metadata Schema (`control_plane/`)
- **DDL Definitions:** Pure SQL DDL builders create 8 dedicated control tables in `<catalog>.config` with strongly-typed schemas and explicit primary keys (`dataflow_group_id`, `dataflow_id`, `flow_step_id`, `reconciliation_id`, `config_id`).
- **Transactional Upserts:** `onboarding/metadata_upsert.py` utilizes Delta Lake `MERGE INTO` statements with explicit `StructType` schemas to ensure idempotent onboarding and eliminate type-inference errors on empty/null optional fields.

---

## 3. Identified Architectural Anti-Patterns & Deficiencies

### Issue 1.1: Standalone Reconciliation Engine Decoupled from Lakeflow DAG
- **Severity:** Medium
- **Location:** `notebooks/05_reconciliation/05_reconciliation_engine.py` & `src/.../reconciliation/matcher.py` (Lines 40–53).
- **Description:** Reconciliation flows run as standalone Databricks Job tasks separate from the Lakeflow pipeline update. If an ingestion flow and its reconciliation flow run in sequence, the reconciliation job re-reads the source and target Delta tables from storage rather than evaluating the comparison in-memory as a node within the Lakeflow DAG.
- **Architectural Risk:** Redundant I/O and compute billing when reconciliation could be registered as a native DLT flow when source and target reside in the same pipeline group.

### Issue 1.2: Hardcoded Control Schema Resolution (`<catalog>.config`)
- **Severity:** Low
- **Location:** `control_plane/schema_provisioner.py` and across all engine notebooks.
- **Description:** The control plane strictly hardcodes the control schema name as `config` (`f"{catalog}.config"`).
- **Architectural Risk:** Prevents enterprises with strict schema naming conventions (e.g., `<catalog>.metadata`, `<catalog>.control_plane`, or centralized `<env>_flowx_control.main`) from deploying the framework without modifying source code.

---

## 4. Architectural Recommendations & Target State

### Recommendation 1.1: Unify In-Pipeline Reconciliation Nodes with Fallback to Standalone Tasks
Allow reconciliation specifications that share a `dataflow_group_id` with active ingestion/transformation flows to be registered directly as `@dlt.table` / `@dlt.view` DAG nodes inside `03_lakeflow_declarative_pipeline.py`. Maintain `05_reconciliation_engine.py` for cross-pipeline or external dataset comparisons.

### Recommendation 1.2: Configurable Control Schema Parameter
Allow the control schema name to be configurable via Spark configuration (`dataflow.control.schema`, defaulting to `config`), enabling flexible enterprise catalog layout strategies.
