# 💬 Metaflow — Multi-Role FAQs

> **Audience**: Developers, Data Architects, and Project Managers evaluating, building, and operating Metaflow pipelines.

---

## 🧑‍💻 Section 1: Developer FAQ

### Q1.1: How do I debug a pipeline error when `@dlt.table` closures fail?
**Answer**:
Remember the **Two-Phase Execution Model** ([01_platform_architecture.md](01_platform_architecture.md)):
1. **Phase 1 (Graph Definition)**: Checks control-table metadata and registers the `@dlt.table` and `@dlt.view` DAG. If a failure occurs here, check `dataflow.group.id` parameters, missing control table rows, or syntax errors in `pipeline_parameters`.
2. **Phase 2 (Graph Execution)**: Occurs when Spark processes the stream. Errors inside closures (e.g. `AnalysisException` from invalid column names in `transformation_sql` or type mismatch in `data_standardization_sql`) only surface during Phase 2. To debug:
   - Check the **Delta Live Tables UI Events tab**.
   - Look for the specific failing flow step named `dlt_view_<flow_step_id>` or `dlt_table_<flow_step_id>`.
   - Run the query in a standard Databricks SQL worksheet against sample data to verify column types.

### Q1.2: How can I test onboarding specs locally with `pytest` before deploying?
**Answer**:
The entire `onboarding/spec_validator.py` suite runs in pure Python without needing a live Spark session:
```powershell
uv run pytest tests/unit/test_spec_validator.py -v
```
To validate a custom spec file programmatically:
```python
from flowx.lakeflow_framework.onboarding.spec_loader import load_and_template_spec
from flowx.lakeflow_framework.onboarding.spec_validator import validate_spec

spec = load_and_template_spec("path/to/my_spec.json", catalog="poc", env="dev")
errors = validate_spec(spec)
assert len(errors) == 0, f"Validation failed: {errors}"
```

### Q1.3: How does `schema_config_path` handle directory resolution and schema evolution?
**Answer**:
When `source_config.schema_config_path` points to a directory (e.g. `/Volumes/poc/schemas/orders/`), Metaflow sorts all `.json` and `.yaml` files in that directory by modification timestamp and automatically loads the **latest file**. If new columns appear in the data that are not defined in `schema_config_path`, Auto Loader's `schema_evolution_mode` (e.g. `addNewColumns` or `rescue`) governs how unmapped columns are handled.

### Q1.4: How do I perform a dry-run validation without writing to control tables?
**Answer**:
Use the `preflight_check_onboarding_spec` tool or run the onboarding engine notebook with widget parameter `action_type = "VALIDATE_ONLY"`. The engine executes all structural linting, allowed-value checks, and Unity Catalog existence checks, outputting a complete diagnostic report without issuing `MERGE INTO` DDL.

---

## 🏛️ Section 2: Data Architect FAQ

### Q2.1: How does Metaflow achieve true decoupling between metadata and the Spark runtime?
**Answer**:
Metaflow implements a declarative compiler pattern:
1. Business definitions (sources, joins, CDC rules, encryption, tags) reside purely in JSON/YAML specifications and are stored in standard Delta control tables.
2. The runtime engine (`03_lakeflow_declarative_pipeline.py`) contains zero customer-specific business logic. It reads active rows for a `dataflow_group_id` and dynamically synthesizes the Databricks Lakeflow DAG (`@dlt.table`, `@dlt.view`, `dlt.create_sink`, `dlt.apply_changes`).
3. This architecture guarantees that engine performance enhancements, security patches, or runtime upgrades apply instantaneously to all onboarded pipelines without refactoring individual business flows.

### Q2.2: How does the framework enforce security boundaries and secret isolation?
**Answer**:
- **Zero In-Flight Plaintext Secrets**: Cryptographic keys and connection credentials are never stored in control tables, environment variables, or log files.
- **Unity Catalog 3-Level Namespace**: Secrets are referenced via Unity Catalog secret identifiers (`secret_catalog` / `secret_schema` / `secret_key`) or workspace scopes (`secret:<scope>:<key>`).
- **ABAC Integration**: Target tables are automatically integrated with Unity Catalog Row Filters and Column Masks, ensuring fine-grained access control at the storage layer.

### Q2.3: What are the performance implications of SCD2 tracking and liquid clustering?
**Answer**:
- **Liquid Clustering**: Metaflow supports native Delta Liquid Clustering via `target_config.liquid_clustering: ["col1", "col2"]`. This eliminates the pitfalls of static table partitioning (such as small-file skew and partition over-segmentation) and dynamically optimizes layout for CDC merge keys.
- **CDF Change Capture**: For incremental downstream processing, Metaflow leverages Delta Change Data Feed (CDF) to capture row change metrics without performing expensive full-table diffs.

### Q2.4: How do in-graph Lakeflow sinks (`dlt.create_sink`) outperform traditional batch egress?
**Answer**:
Traditional batch egress approaches require separate post-deployment cron jobs that read target tables from scratch and write to external destinations, introducing latency and double-read compute costs. Metaflow registers sinks directly into the Lakeflow DAG using `dlt.create_sink` + `@dlt.append_flow`. Data flows directly from the processing stage into external storage or Kafka within the same micro-batch checkpoint, guaranteeing exactly-once semantics and minimal end-to-end latency.

---

## 📋 Section 3: Project Manager & Delivery FAQ

### Q3.1: What are the prerequisites before onboarding the first production pipeline?
**Answer**:
1. **Databricks Workspace**: Access to a Databricks workspace with Unity Catalog enabled.
2. **Unity Catalog Assets**: Target Catalog (e.g. `enterprise_prod`) with `CREATE SCHEMA` and `CREATE VOLUME` privileges.
3. **Secret Scopes**: Required secret scopes provisioned via Databricks CLI for any encrypted columns or external sink destinations.
4. **Tooling**: Databricks CLI v0.200+ and `uv` installed on the deployment CI/CD runner.

### Q3.2: What operational risks exist during schema evolution and how are they mitigated?
**Answer**:
- **Risk of Breaking Downstream Schemas**: Unannounced column additions or type changes from upstream source systems can break downstream reporting.
- **Mitigation**:
  - **Auto Loader Rescue**: Setting `schema_evolution_mode: "rescue"` routes unexpected fields to `_rescued_data` without failing ingestion.
  - **Quarantine Tables**: Setting `action: "quarantine"` on DQ rules routes invalid records to `{table}_quarantine` with full diagnostic metadata, keeping the main Silver table clean while preventing pipeline stoppage.
  - **Reconciliation Engine**: Automated daily reconciliation audits verify consistency between Bronze sources and Gold targets, automatically flagging or self-healing missing data.

### Q3.3: How does Metaflow accelerate delivery timelines and onboarding velocity?
**Answer**:
- **From Days to Minutes**: Traditional pipeline development requires 3–5 days per data source (writing notebooks, configuring streaming checkpoints, coding SCD logic, implementing DQ). With Metaflow, a developer completes an onboarding JSON/YAML template in **under 30 minutes**.
- **Standardized CI/CD**: Reusable Databricks Asset Bundles (DABs) automate packaging, wheel builds, deployment, and testing, drastically reducing deployment risk and operational toil.

### Q3.4: How do governance guardrails prevent non-compliant data promotion?
**Answer**:
- **Mandatory Resource Tagging**: Onboarding specs enforce mandatory table tags (`cost_center`, `data_owner`, `classification`, `sla`) that are automatically applied to Unity Catalog assets.
- **Preflight AST Linting**: The `validate_json` and preflight tools reject non-compliant specs prior to control table upsert, preventing unvalidated configurations from reaching production.
