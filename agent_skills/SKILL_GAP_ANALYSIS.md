# 🕵️ Metaflow Agent Registry — Skill Gap Analysis & Audit Checklist

> **Purpose**: Systematic audit comparing the Metaflow framework's consolidated capabilities against existing Agent Skills and Tool definitions, itemizing missing skills, behavioral modifications, and net-new capabilities.

---

## 1. Executive Summary & Audit Matrix

| Functional Area | Consolidated Documentation Reference | Prior Agent Skill Coverage | Current Gap Status | Action Taken |
|---|---|---|---|---|
| **Pipeline Governance & Naming** | `docs/04_data_quality_and_governance.md` | None (Informal mentions in SKILL.md) | **MISSING SKILL** | Implemented dedicated `agent_skills/governance/SKILL.md`. |
| **Resource Tagging & Metadata** | `docs/04_data_quality_and_governance.md` | Fragmented in SKILL.md | **BEHAVIORAL GAP** | Formalized mandatory tagging rules in Governance Skill. |
| **Spec Preflight & JSON Linting** | `docs/09_developer_guide_and_recipes.md` | Pure Python in `uc_spec_preflight.py` | **TOOL SPEC GAP** | Created standardized OpenAI tool schema `validate_json`. |
| **Lifecycle Entity Onboarding** | `docs/01_platform_architecture.md` | Notebook execution only | **TOOL SPEC GAP** | Created standardized tool schema `onboard_entity`. |
| **Catalog Metadata Discovery** | `docs/00_master_reference_index.md` | Manual SQL queries | **TOOL SPEC GAP** | Created standardized tool schema `get_catalog_schema_parameters`. |
| **DLT Observability & Telemetry** | `docs/08_observability_and_telemetry.md` | `dlt_observability_tools.json` | **ALIGNED** | Integrated into Master Tool Catalog (`tool_specifications.json`). |
| **In-Graph Lakeflow Sinks** | `docs/06_egress_and_lakeflow_sinks.md` | Post-deployment batch paradigm | **BEHAVIOR MODIFICATION** | Updated agent prompt to reflect in-graph `dlt.create_sink`. |
| **Column Normalization & Schemas**| `docs/02_ingestion_and_sources.md` | Partially documented | **NEW CAPABILITY** | Added `column_normalization` (`{enabled, case}`) & `schema_config_path` rules. The legacy `normalize_column_names` boolean was removed in v1.4.0. |

---

## 2. Itemized Skill Checklist

### 2.1 Missing Skills (Net-New Implementations)
- [x] **`metaflow-governance`**: Dedicated skill enforcing naming conventions for pipelines (`dfg_*`), flows (`df_*`, `tf_*`, `rf_*`), tasks (`run_pipeline_update`, `apply_governance_tags`, `observability_export`), mandatory table/column tags, and metric signatures.
- [x] **`metaflow-onboarding-lifecycle`**: Standardized tool calling loop for spec validation (`validate_json`), idempotent control table merge (`onboard_entity`), and schema introspection (`get_catalog_schema_parameters`).

### 2.2 Modified Behaviors & Breaking Changes
- [x] **Lakeflow Sink Registration**: Sinks are now registered *inside* the DLT DAG (`dlt.create_sink` + `@dlt.append_flow`) during Phase 1 graph definition rather than executed via external post-deployment batch jobs.
- [x] **Unity Catalog 3-Level Secrets**: Secrets must strictly follow `secret_catalog` / `secret_schema` / `secret_key` or `secret:<scope>:<key>` format. Hardcoded secrets or plain workspace paths are strictly rejected.
- [x] **SCD3 Transformation-Only Constraint**: `cdc_load_strategy: "SCD3"` is strictly prohibited in `ingestion_flows` and is only allowed in `transformation_flows`.
- [x] **Auto Flatten vs. Explicit Explode**: `auto_flatten_all: true` applies only when `explode_columns` is empty. If `explode_columns` is populated, only explicitly named columns are processed.

### 2.3 Net-New Agent Capabilities Added to Registry
- [x] `validate_json`: Immediate AST linting, structural validation, type checking, and SQL expression analysis for candidate onboarding specs.
- [x] `onboard_entity`: Automated idempotent `create` and `update` lifecycle actions for pipelines, jobs, and control table records.
- [x] `get_catalog_schema_parameters`: Real-time retrieval of table definitions, column types, volume paths, and active tags from Unity Catalog.
