# 🛠️ Metaflow — Developer Step-by-Step Guide & Recipes

> **Audience**: Data engineers building, validating, and deploying data pipelines using the Metaflow framework.

---

## 1. Prerequisites & Environment Setup

### Required Tooling
- **Databricks CLI**: v0.200+ ([Install Guide](https://docs.databricks.com/dev-tools/cli/install.html))
- **uv**: Modern fast Python package manager ([Install Guide](https://docs.astral.sh/uv/getting-started/installation/))
- **Python 3.12**: Core runtime environment

### Authentication
```bash
# Verify authentication against target Databricks workspace
databricks auth login
databricks current-user me
```

---

## 2. The 11-Step Pipeline Onboarding Walkthrough

Follow these sequential steps to onboard a new end-to-end pipeline:

### Step 1: Create Onboarding Spec File
Create a new file in `onboarding_templates/my_pipeline.json` (or `.yaml`). Declare `dataflow_group_id`:
```json
{
  "dataflow_group_id": "dfg_ecommerce_orders_pipeline",
  "pipeline_parameters": {
    "target_region": "NORTH_AMERICA"
  },
  "ingestion_flows": [],
  "transformation_flows": [],
  "reconciliation_flows": []
}
```

### Step 2: Define Bronze Ingestion Flow
Add an ingestion flow to read raw landing files:
```json
{
  "dataflow_id": "df_raw_orders",
  "source_type": "autoloader",
  "target_catalog": "{{catalog}}",
  "target_schema": "bronze_ecommerce",
  "target_table": "raw_orders",
  "target_type": "streaming_table",
  "source_config": {
    "path": "/Volumes/{{catalog}}/landing/orders",
    "format": "csv",
    "reader_options": { "header": "true" },
    "capture_technical_metadata": true
  },
  "target_config": {
    "cdc_load_strategy": "APPEND"
  }
}
```

### Step 3: Define Silver Transformation Flow & CDC Strategy
Add a transformation flow to clean data and apply SCD1 merge:
```json
{
  "dataflow_id": "tf_orders_cleaned",
  "flow_step_id": "step_orders_scd1",
  "source_inputs": [
    { "table": "{{catalog}}.bronze_ecommerce.raw_orders", "alias": "ord" }
  ],
  "transformation_sql": "SELECT ord.order_id, ord.customer_id, cast(ord.amount as double) as amount, ord.order_status, ord.order_date FROM ord",
  "target_catalog": "{{catalog}}",
  "target_schema": "silver_ecommerce",
  "target_table": "orders_dim",
  "target_type": "streaming_table",
  "target_config": {
    "cdc_load_strategy": "SCD1",
    "primary_keys": ["order_id"],
    "sequence_by_column": "order_date",
    "compute_hash_key": true
  }
}
```

### Step 4: Add Data Quality Expectations & Quarantine
Add `dq_config` with quarantine routing to isolate corrupted rows:
```json
"dq_config": {
  "quarantine_table": "orders_dim_quarantine",
  "record_id_column": "order_id",
  "rules": [
    { "name": "valid_order_id", "sql_condition": "order_id IS NOT NULL", "action": "fail" },
    { "name": "positive_amount", "sql_condition": "amount > 0", "action": "quarantine" }
  ]
}
```

### Step 5: Add Governance Tags
Assign metadata attribution to target tables:
```json
"governance_tags": {
  "table_tags": {
    "cost_center": "CC-ECOMMERCE",
    "classification": "confidential",
    "sla": "silver_hourly"
  }
}
```

### Step 6: Validate Your Spec
Run the preflight validator tool or CLI command:
```powershell
uv run pytest tests/unit/test_spec_validator.py -k "my_pipeline"
```

### Step 7: Build the Wheel Package
```powershell
python scripts/bump_and_build.py
```

### Step 8: Deploy via Databricks Asset Bundles (DABs)
```bash
databricks bundle deploy -t dev
```

### Step 9: Onboard Spec to Control Tables
Trigger the reusable onboarding job:
```bash
databricks bundle run onboarding_job \
  --params spec_file_path=/Workspace/.../my_pipeline.json,catalog=poc,env=dev,action_type=CREATE
```

### Step 10: Run the Lakeflow Pipeline
Trigger the Lakeflow Declarative Pipeline for your `dataflow_group_id`:
```bash
databricks pipelines start --pipeline-id <pipeline_id>
```

### Step 11: Verify Target Tables & Metrics
Query target tables in SQL Editor:
```sql
SELECT * FROM poc.silver_ecommerce.orders_dim LIMIT 10;
SELECT * FROM poc.silver_ecommerce.orders_dim_quarantine LIMIT 10;
```

---

## 3. Production Copy-Paste Recipes

### Recipe A: Auto Loader JSON with Schema Evolution & Rescued Data
```json
{
  "dataflow_id": "df_iot_telemetry",
  "source_type": "autoloader",
  "target_catalog": "{{catalog}}",
  "target_schema": "bronze_iot",
  "target_table": "sensor_telemetry",
  "target_type": "streaming_table",
  "source_config": {
    "path": "/Volumes/{{catalog}}/iot/landing",
    "format": "json",
    "schema_evolution_mode": "rescue",
    "capture_technical_metadata": true
  },
  "target_config": { "cdc_load_strategy": "APPEND" }
}
```

### Recipe B: SCD2 Dimension with AES Encryption
```json
{
  "dataflow_id": "tf_customer_scd2",
  "flow_step_id": "step_cust_scd2",
  "source_inputs": [{ "table": "{{catalog}}.bronze_crm.raw_customers", "alias": "c" }],
  "transformation_sql": "SELECT c.customer_id, c.email, c.ssn, c.updated_at FROM c",
  "target_catalog": "{{catalog}}",
  "target_schema": "silver_crm",
  "target_table": "customer_scd2_dim",
  "target_type": "streaming_table",
  "target_config": {
    "cdc_load_strategy": "SCD2",
    "primary_keys": ["customer_id"],
    "sequence_by_column": "updated_at",
    "encrypted_columns": [
      {
        "column_name": "ssn",
        "algorithm": "AES",
        "mode": "GCM",
        "secret": {
          "secret_catalog": "poc",
          "secret_schema": "security",
          "secret_key": "pii_aes_gcm_key"
        }
      }
    ]
  }
}
```

### Recipe C: Native Delta Lake Egress Sink
```json
{
  "dataflow_id": "tf_export_partner",
  "flow_step_id": "step_export_partner",
  "source_inputs": [{ "table": "{{catalog}}.silver_crm.customer_scd2_dim", "alias": "src" }],
  "transformation_sql": "SELECT src.customer_id, src.email FROM src WHERE src.__end_at IS NULL",
  "target_type": "sink",
  "target_config": {
    "sink_config": {
      "format": "delta",
      "path": "/Volumes/partner_catalog/drops/customer_export",
      "mode": "append"
    }
  }
}
```

---

## 4. Local Testing & Verification Patterns

Run fast unit tests without requiring a remote cluster:
```powershell
# Run all unit tests
uv run pytest tests/unit/ -v

# Test specific onboarding spec validation
uv run pytest tests/unit/test_spec_validator.py -v
```

---

## 5. Recipes: In-Pipeline Reconciliation and Workspace Bring-Up

Two end-to-end procedures added in v1.5.0. Both are written to be followed verbatim, top to bottom;
the ordering constraints in them are load-bearing, not stylistic.

### Recipe D: Turn on in-pipeline reconciliation for an existing flow

**What you are changing.** A reconciliation flow that today runs as a standalone
`05_reconciliation_engine.py` job task after the pipeline finishes will instead be registered as a
third first-class flow type *inside* its dataflow group's Lakeflow update, alongside the ingestion
and transformation flows. Nothing about `execution_mode: "job"` — the default, and what a `NULL`
column reads back as — changes; this is opt-in per flow.

#### D.0 — Preconditions, checked before you edit anything

The two pipeline modes reject several per-flow settings at onboarding. Confirm the flow satisfies
all of these, or the onboarding job will fail with a validation error:

| Precondition | Why |
|---|---|
| The flow has a `dataflow_group_id` | A group-less flow has no pipeline to live in. |
| Neither side sets `read_mode: "streaming"` | The in-graph comparison is a whole-snapshot batch classification; a stream-static join cannot express `MISSING_IN_SOURCE`. |
| No `task_run_id_column` on either side | A Lakeflow update exposes no stable per-update run id, so the narrowing would be a silent no-op. |
| Any `dq_config` rule uses `action: "fail"` or `"warn"`, never `"quarantine"` | There is nothing to quarantine on a one-row `__metrics` table. |

Then decide **which** pipeline mode you need — this is the step people get wrong:

* Use **`"pipeline"`** (comparison *and* the L5 self-healing lane in-graph) only if the
  reconciliation **source** is append-only. If it is a table this same group publishes, that means
  its producing flow's `cdc_load_strategy` is `APPEND`.
* Use **`"pipeline_audit_only"`** (comparison and expectations in-graph, healing left to job mode)
  if the source is produced by `TRUNCATE_AND_LOAD`, `SCD1`/`SCD2`/`SCD3`, `FULL_SNAPSHOT_CDC`, or
  is a `materialized_view`. The heal lane must stream from the source, and Delta refuses to stream
  from a table that is rewritten wholesale each update. This is checked at plan time — you get a
  `FrameworkConfigError` naming the locator, the producing flow and the correct
  `execution_mode` — but only *after* onboarding succeeds, so choose correctly up front. Full
  matrix: [`12_module_permutation_matrix.md` §4.2](12_module_permutation_matrix.md#42-execution_mode--the-producing-strategy-of-the-recon-source-v150).

#### D.1 — Run `setup_control_tables` FIRST if the workspace predates v1.5.0

> [!WARNING]
> **`databricks bundle deploy` does not apply this migration.** Only *running* the
> `setup_control_tables` task does.

v1.5.0 adds three columns to `reconciliation_flow_spec`: `execution_mode`, `publish_schema` and
`dq_config_json`. Every statement in `control_plane/ddl_definitions.py::get_all_control_table_ddls`
is `CREATE TABLE IF NOT EXISTS`, which is a **no-op against a table that already exists** — so a
column added to a `CREATE` statement reaches new installations only. A workspace provisioned before
v1.5.0 therefore has none of the three columns, and onboarding a pipeline-mode flow into it fails
with `UNRESOLVED_COLUMN` (observed live on `metaflow.config.reconciliation_flow_spec`).

The additive migration that fixes this is `ADDITIVE_CONTROL_TABLE_COLUMNS` + `get_add_column_ddl()`
in `control_plane/ddl_definitions.py`, applied by `ensure_control_table_columns()` in
`control_plane/schema_provisioner.py`, which `ensure_control_schema_exists` now calls at the end.
Its only entrypoint is `notebooks/01_setup/01_setup_control_tables.py`.

Check whether you need it:

```sql
DESCRIBE TABLE metaflow.config.reconciliation_flow_spec;
-- look for: execution_mode, publish_schema, dq_config_json
```

If any of the three is absent, run the setup task once before onboarding — for example as the first
task of the job that will do the onboarding (this is exactly why
`resources/metaflow_test_recon_dag_job.yml` begins with a `setup_control_tables` task rather than
treating it as boilerplate).

The migration is **strictly additive**: it adds columns, never drops or retypes one. It is
deliberately *not* written as `ADD COLUMNS IF NOT EXISTS` — Databricks SQL rejects that with
`PARSE_SYNTAX_ERROR` (verified). Idempotence is caller-side: columns already present are skipped,
and a narrow duplicate-column race is swallowed.

#### D.2 — Set `execution_mode` in the spec

```json
{
  "reconciliation_id": "recon_orders_bronze_vs_silver",
  "dataflow_group_id": "dfg_ecommerce_orders_pipeline",
  "execution_mode": "pipeline",
  "publish_schema": "recon_ecommerce",
  "source_config": { "type": "table", "table": "{{catalog}}.bronze_ecommerce.raw_orders", "read_mode": "batch" },
  "target_configs": [
    { "target_id": "silver_orders", "type": "table", "table": "{{catalog}}.silver_ecommerce.orders_dim", "read_mode": "batch" }
  ],
  "match_keys": ["order_id"],
  "compare_columns": ["amount", "order_status"],
  "dq_config": {
    "rules": [
      { "name": "no_missing_in_target", "sql_condition": "missing_in_target_count = 0", "action": "warn" }
    ]
  }
}
```

`publish_schema` is optional. Omit it and the L3–L5 nodes are published into the pipeline's own
schema, resolved by the notebook's fallback chain
(`pipelines.schema` → `pipelines.target` → `currentDatabase()` → `GROUP_ROW.target_schema`).
`publish_schema` and `dq_config` are **rejected** on `execution_mode: "job"` — a job task publishes
no dataset to name or to attach expectations to.

#### D.3 — Onboard via the GENERIC `onboarding_job`

> **Rule: a new job must NEVER inline a `02_onboarding_engine.py` notebook_task.** It delegates to
> the single parameterised entrypoint `resources/onboarding_job.yml`. The ~20 pre-existing
> `metaflow_test_*_job.yml` files that each pin their own inline copy are **legacy and deliberately
> left as-is** (per the standing "keep legacy jobs as-is, add new orchestration alongside"
> decision recorded in `onboarding_job.yml`'s own header) — duplicating that pattern is how the
> onboarding contract drifts between callers. This convention is already applied in
> `resources/metaflow_test_recon_dag_job.yml`, `metaflow_test_dag_001_unified_job.yml` and
> `metaflow_test_104_geneva_tariffs_recon_job.yml`.

In a job resource, delegate with a `run_job_task`:

```yaml
        - task_key: onboard_x
          run_job_task:
            job_id: ${resources.jobs.onboarding_job.id}
            job_parameters:
              spec_file_path: "${workspace.file_path}/metaflow_testing/<spec>.json"
              catalog: metaflow
              env: dev
              action_type: CREATE
```

Or, ad hoc from the CLI:

```bash
databricks bundle run onboarding_job -t dev \
  --params spec_file_path=/Workspace/.../my_pipeline.json,catalog=metaflow,env=dev,action_type=CREATE
```

Re-onboarding the same `reconciliation_id` **MERGEs in place**; onboarding a spec that declares the
same group with *different* flow ids adds duplicate flows instead of replacing them. If you are
mirroring flows that already exist in the control tables, mirror their real ids.

#### D.4 — Run the pipeline (and delete the now-duplicate recon job task)

```bash
databricks pipelines start --pipeline-id <pipeline_id>
```

Before you do, **check the job graph for a standalone reconciliation task**. Under
`execution_mode: "pipeline"` the L3/L4/L5 datasets are registered by the pipeline update itself; a
surviving `05_reconciliation_engine.py` task would run the comparison a **second** time and
double-append into the heal target. This was a real defect found in
`resources/metaflow_test_002_003_job.yml`, whose `run_003_reconciliation` task still existed after
scenario 003 was flipped to pipeline mode.

Observability is the opposite case: it **stays** a separate job task (R3) and must not be folded
into the pipeline. See
[`01_platform_architecture.md` §8.4](01_platform_architecture.md#84-r3-observability-is-still-a-job-task).

#### D.5 — Verify

From the pipeline's event log, the update should contain the L3 prepare nodes, the three L4
datasets, and — in `"pipeline"` mode only — the L5 pulse and heal flow:

```
_recon__<rid>__src                    STREAMING_TABLE / MATERIALIZED_VIEW   (L3)
_recon__<rid>__<tid>__tgt             MATERIALIZED_VIEW                     (L3)
recon__<rid>__<tid>__classified       MATERIALIZED_VIEW                     (L4, published)
recon__<rid>__<tid>__metrics          MATERIALIZED_VIEW                     (L4, published, 1 row)
recon__<rid>__<tid>__mismatch         MATERIALIZED_VIEW                     (L4, published)
_recon__<rid>__pulse                  STREAMING_TABLE                       (L5, "pipeline" only)
recon__<rid>__heal_flow               APPEND flow                           (L5, "pipeline" only)
```

This exact shape was observed live on 2026-08-31 for pipeline
`metaflow_test_003_autoload_recon_pipeline` — see
[`01_platform_architecture.md` §8.6](01_platform_architecture.md#86-verification-status).

---

### Recipe E: Bring up a NEW workspace

On a brand-new workspace the D.1 migration is a **no-op** — and that is the correct outcome, not a
skipped step. `CREATE TABLE IF NOT EXISTS` creates `reconciliation_flow_spec` with
`execution_mode`, `publish_schema` and `dq_config_json` already present, because those columns are
in the `CREATE` DDL too. The additive migration only matters for workspaces provisioned *before*
v1.5.0. You still run `setup_control_tables` first — it is what creates the control tables at all.

**Order, and it does not commute:**

1. **`setup_control_tables`** — run `notebooks/01_setup/01_setup_control_tables.py` (or the
   `setup_control_tables` task of a job that declares it) with `catalog=<your control catalog>`.
   This creates the `{catalog}.config` schema and all control tables, then runs
   `ensure_control_table_columns` as a no-op. Nothing downstream works before this: onboarding
   writes into these tables and the pipeline reads from them.
2. **Onboard** — via the generic `onboarding_job` for a single spec, exactly as in
   [D.3](#d3--onboard-via-the-generic-onboarding_job). For a whole **directory** of specs, use its
   sibling `resources/framework_config_onboarding_job.yml`, which takes a `spec_dir` rather than a
   `spec_file_path`. Do not inline `02_onboarding_engine.py` in a new job in either case.
3. **Run the pipeline** for the `dataflow_group_id` you just onboarded.

```bash
# 1. control tables (once per workspace/catalog)
databricks bundle run <job containing setup_control_tables> -t <target>

# 2. onboard one spec ...
databricks bundle run onboarding_job -t <target> \
  --params spec_file_path=/Workspace/.../my_pipeline.json,catalog=<catalog>,env=<env>,action_type=CREATE

# 2'. ... or a whole directory
databricks bundle run framework_config_onboarding_job -t <target>

# 3. run the pipeline
databricks pipelines start --pipeline-id <pipeline_id>
```

> [!WARNING]
> **Never `bundle deploy` while a pipeline or test wave is running.** `bundle deploy` prunes
> superseded artifacts from `<artifact_path>/.internal/` — on a UC Volume exactly as in the
> workspace — so a deploy issued mid-update kills the running update with
> `ENVIRONMENT_PIP_INSTALL_ERROR`. Unique per-deploy wheel filenames prevent overwrite-in-place,
> not removal. Deploy first, then run.

Two known bring-up hazards worth checking on a fresh workspace, because both have bitten this repo
and neither is caused by v1.5.0:

* **Table-level `SELECT` grants on a reconciliation target.** Schema-level access is not enough if
  a specific table carries its own grants. The identity the pipeline runs as needs
  `SELECT` on **both** reconciliation sides. This is the open blocker on the Geneva scenario, which
  is consequently verified **offline only** (validator plus `plan_source_plane`, pinned by
  `tests/unit/test_geneva_e41a47ba_topology.py`) and has **never been proven by a live run**.
* **A pipeline that is not in the bundle target you deploy.** A pipeline that reads its wheel from
  another principal's artifact path is orphaned by every deploy of *your* target — the deploy prunes
  the wheel it was pinned to and updates only the pipelines that target owns — and it then fails
  with `ENVIRONMENT_PIP_INSTALL_ERROR` that no amount of re-deploying your target repairs. Bring
  such a pipeline under the bundle, or give it its own `artifact_path`.
