# Implementing a Use Case on Metaflow — End-to-End Guide

**Audience:** a data engineer onboarding a new source system onto Databricks with Metaflow.
**Scope:** the whole path from "here is a source system and a governance sheet" to
"three governed Bronze tables with CDC, reconciliation and observability running on a schedule".

This guide is written from a **real, completed implementation** — the BT/EE Excalibur CRM
migration (UC3): three tables, 254 governed columns, SCD1 + SCD2, batch reconciliation. Every
command, failure and remedy below was actually executed. Where something went wrong, the guide
says so, because the failures are the parts worth knowing in advance.

Companion documents:

- [`UC3/BUILD_CONTRACT.md`](UC3/BUILD_CONTRACT.md) — the concrete UC3 contract and its Phase C log
- [`UC3/FRAMEWORK_CAPABILITY_MAP.md`](UC3/FRAMEWORK_CAPABILITY_MAP.md) — config-vs-code per capability
- [`14_onboarding_restrictions_and_validation_rules.md`](14_onboarding_restrictions_and_validation_rules.md) — what the validator enforces
- [`13_known_limitations_and_gotchas.md`](13_known_limitations_and_gotchas.md)

---

## 1. What Metaflow actually buys you

Metaflow is a **metadata-driven ingestion framework**: you declare *what* you want in an onboarding
spec (JSON/YAML), and the framework generates and runs the Lakeflow Declarative Pipeline that
does it. The unit of work is a **dataflow group** — one `dataflow_group_id` owning a set of
ingestion, transformation and reconciliation flows, driven by one pipeline.

### 1.1 Advantages, stated concretely

| Advantage | What it means in practice |
|---|---|
| **Configuration, not code** | UC3 delivered 3 tables / 254 governed columns / 2 SCD strategies / 3 reconciliations with **zero** hand-written CDC, hashing, tagging or merge logic. The only bespoke code was a test-data generator and a source *simulator* — neither of which is production ingestion. |
| **One canonical hash** | `__framework_hash_key` / `__framework_hash_value`, SHA-256, one implementation (`cdc/hashing.py`). Reproducible by hand in SQL. The legacy system this replaced had a documented bug where the hash included the PK and timestamps despite claiming otherwise; here that is *structurally impossible* — `resolve_comparison_columns` always excludes `primary_keys`. |
| **Native CDC, no MERGE by hand** | `cdc_load_strategy: SCD1 \| SCD2 \| SCD3 \| APPEND \| TRUNCATE_AND_LOAD \| FULL_SNAPSHOT_CDC`, mapped onto Lakeflow's own `AUTO CDC`. Switching a table from SCD1 to SCD2 is a one-word spec change. |
| **Governance as data** | Column and table tags travel in the spec, land in the control tables, and are applied post-deployment by `governance/tags.py`. Tag *values* come from the source governance sheet — never hand-typed. |
| **Reconciliation is a first-class engine** | Four counts (matched / missing-in-target / missing-in-source / value-drift), a heal/append lane, and a run log — declared, not coded. |
| **Unknown attributes are rejected** | Since v1.7.1 an unrecognised spec key is a hard error, not a silent no-op. A misspelled `dq_config` used to mean data quality silently never ran. |
| **Single-Read DAG** | One external read per source per execution mode, enforced by the source plane. Five source tables means five base reads, no accidental re-scans at fanout. |
| **Auditable by construction** | Every flow, its config JSON, its governance tags and its reconciliation results live in queryable control tables under `<catalog>.config`. |

### 1.2 What Metaflow does *not* do

Be clear about the boundary before you start:

- **It does not create the catalog.** That is a one-time workspace prerequisite (see §3).
- **It does not create schemas or volumes** — it creates *tables*. Provision schemas/volumes first.
- **It does not administer masking or row-filter policies.** It applies *tags*; a workspace admin
  binds enforcement to those tags. Metaflow never issues `SET MASK`.
- **It does not fake a source.** If you need a simulated feed for testing, that is your own job
  (UC3 wrote one — legitimately, since "pretend to be a message bus" is not an ingestion feature).

---

## 2. The shape of a use case

```
source system  ──►  landing (Volume or Delta)  ──►  Bronze (governed, CDC-applied)
                          ▲                              │
                     Job 1 (optional simulator)          ├─► reconciliation + heal lane
                                                          └─► observability export
```

UC3 realised this as three jobs. Most use cases need only the last two.

| Job | Purpose | Framework or bespoke? |
|---|---|---|
| `003_lfj_..._streaming_simulator` | drains test CSVs into staging on a timer, standing in for the real feed | **bespoke** — no framework equivalent, and correctly so |
| `004_lfj_..._streaming_cdc` | staging → Bronze with per-table SCD semantics | **framework** — spec + generic onboarding job |
| `005_lfj_..._batch_recon` | land batch files, reconcile against Bronze, heal | **framework** — spec + generic onboarding job |

### 2.1 Naming convention

```
<seq>_lfj_<uc>_<system>_<purpose>     Lakeflow Job
<seq>_ldp_<uc>_<system>_<purpose>     Lakeflow Declarative Pipeline
dfg_<uc>_<system>_<purpose>           dataflow_group_id
df_<uc>_<table>_<purpose>             dataflow_id
rf_<uc>_<table>_<comparison>          reconciliation_id
```

---

## 3. Step-by-step

### Step 0 — Prerequisites (once per workspace)

```bash
databricks auth describe -p <profile>          # confirm you are pointed where you think
databricks catalogs list -p <profile>          # the catalog MUST already exist
databricks schemas list <catalog> -p <profile> # check schema headroom BEFORE you start
```

> **Real failure.** UC3's first Phase C attempt targeted a workspace whose catalog did not exist
> at all (the rename migration was still pending there). `bundle validate` passes regardless — the
> catalog is not a bundle-managed resource — so this only surfaces at *runtime*. Check first.

> **Real failure.** A metastore has a schema/Volume ceiling. One workspace in this estate sat at
> 51 schemas with a Volume quota failure at 52/50. Count headroom before adding objects.

Provision what the framework will not:

```bash
databricks schemas create <staging_schema> <catalog> -p <profile>
databricks schemas create <bronze_schema>  <catalog> -p <profile>
databricks volumes create <catalog> <staging_schema> <volume> MANAGED -p <profile>
```

**Prefer reusing existing schemas.** UC3 initially created `uc3_staging` / `uc3_bronze`, then moved
to the estate's existing `staging` / `bronze` with a `uc_3` volume. Fewer objects, less quota
pressure — but **check for table-name collisions first**:

```sql
SHOW TABLES IN <catalog>.<bronze_schema>;
```

### Step 1 — Derive the column inventory from the governance sheet

Do **not** hand-type column lists. Parse the source-of-truth sheet at build time. UC3's sheets
carried, per column: name, Oracle type, PK flag, nullability, description, and eight governance
flags (PII / Info Type / Data Class / PDBT / Sensitive / Drop / Null / three visibility tiers).

Watch for real-world sheet defects — UC3's had all of these:

| Defect | Handling |
|---|---|
| A header misspelled (`Sesnitive Columns`) | read it verbatim; never "correct" it |
| One sheet spelling a flag differently (`csql.securedro` vs `csql.secured_ro`) | lookup-with-fallback, **never a fixed column index** |
| A blank data type | fall back to the other type column; do not guess |
| Mixed type case (`char(2)` vs `CHAR(2)`) | case-insensitive matching on **types**, not just headers |
| A BOM on the file | `encoding="utf-8-sig"` |
| Trailing legend rows | skip any row with an empty key column |

**Two classification flags decide table shape:**

- `Drop = Y` → the column **must not exist** in the target at all. Record it in a `_`-prefixed
  changelog key in the spec so its absence is documented.
- `Null = Y` → the column exists but is **always** NULL. Configure with
  `source_config.data_standardization_sql`, one column expression per entry:
  `"CAST(NULL AS STRING) AS acc_password"`.

If a column carries **both**, **Drop wins** — a dropped column cannot also be nulled, and it must
not appear in `data_standardization_sql`.

**Type mapping (Oracle → Spark):** `VARCHAR2(n)`/`CHAR(n)` → `STRING`; `NUMBER(p,0)` → `BIGINT`
(`INT` when p ≤ 9); `NUMBER(p,s>0)` → `DECIMAL(p,s)`; `DATE`/`TIMESTAMP` → `TIMESTAMP` (Oracle
`DATE` carries a time component); `BOOLEAN` → `BOOLEAN`.

### Step 2 — Write the onboarding spec

Start from a **golden example**, not from memory:
`.claude/skills/flowx-onboarding/references/golden_specs.json` holds five machine-validated specs.

```json
{
  "dataflow_group_id": "dfg_uc3_excalibur_streaming_cdc",
  "ingestion_flows": [
    {
      "dataflow_id": "df_uc3_customer_stream_cdc",
      "source_type": "zerobus",
      "source_config": {
        "source_catalog": "flowx",
        "source_schema": "staging",
        "source_table": "customer_stream",
        "column_normalization": { "enabled": true, "case": "lower" },
        "data_standardization_sql": [
          "CAST(NULL AS STRING) AS acc_password"
        ]
      },
      "target_catalog": "flowx",
      "target_schema": "bronze",
      "target_table": "customer",
      "target_type": "streaming_table",
      "target_config": {
        "cdc_load_strategy": "SCD2",
        "primary_keys": ["customer_id"],
        "sequence_by_column": "sys_update_date",
        "cdc_operation_column": "src_deleted_flg",
        "cdc_operation_mapping": { "delete_values": ["1"] },
        "generate_hash_columns": true,
        "columns_to_exclude": ["sys_creation_date", "sys_update_date"],
        "liquid_clustering_columns": ["customer_id"]
      },
      "governance_tags": {
        "table_tags": { "source_system": "excalibur", "domain": "crm" },
        "column_tags": [
          { "column": "customer_id", "tags": { "tokenise_pii": "Y", "pdbt": "Customer ID" } }
        ]
      }
    }
  ]
}
```

**Attribute traps** — these are hard errors, not warnings:

| Do not write | Write |
|---|---|
| `scd_type: 2` | `target_config.cdc_load_strategy: "SCD2"` |
| `cdc_config: {...}` | CDC fields live directly in `target_config` |
| `sequence_by` | `target_config.sequence_by_column` |
| `primary_key` (scalar) | `target_config.primary_keys` (list) |
| `tags` | `governance_tags.table_tags` |
| `cluster_by` | `target_config.liquid_clustering_columns` |
| `partition_by` | `target_config.partition_columns` |
| `target_schema: "cat.schema"` | separate `target_catalog` + `target_schema` |
| `columns[]` | **no such key** — schema is inferred or pinned via `schema_config_path` |

**Constraints worth knowing before you design:**

- `liquid_clustering_columns` is capped at **3**. A 4-column PK cannot cluster on the full key —
  cluster on `__framework_hash_key` instead (the SHA-256 of the ordered PK, which the framework
  materializes anyway).
- `SCD3` is transformation-only.
- `FULL_SNAPSHOT_CDC` requires real `primary_keys`.

### Step 3 — Validate offline. Every time.

**Never hand over or deploy a spec you have not validated.** No cluster needed, sub-second:

```bash
python -c "
import sys,io; sys.path.insert(0,'src')
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json
r = validate_json(io.open('onboarding/<uc>/<spec>.json', encoding='utf-8').read())
print(r['summary'], '| valid =', r['valid'])
[print(' ERROR:', e) for e in r['errors']]
[print(' warn :', w) for w in r['warnings']]
"
```

Loop until `valid` is `True`. Error messages name the correct attribute — fix from the message
rather than guessing again. Re-validate after **every** edit, including one-line ones.

### Step 4 — Wire the bundle resources

Two files per group: a pipeline and a job.

**Pipeline** — points the framework engine at your group:

```yaml
resources:
  pipelines:
    uc3_streaming_cdc_pipeline:
      name: 004_ldp_uc3_excalibur_streaming_cdc
      catalog: ${var.catalog}
      schema: bronze
      serverless: true
      libraries:
        - notebook:
            path: ../../notebooks/03_engine/03_lakeflow_declarative_pipeline.py
      environment:
        dependencies: [ "../../dist/*.whl" ]
      configuration:
        dataflow.group.id: dfg_uc3_excalibur_streaming_cdc
        dataflow.control.catalog: ${var.catalog}
```

**Job** — four tasks, in this order:

```yaml
tasks:
  - task_key: setup_control_tables        # applies the additive control-table migration
  - task_key: onboard_uc3                 # ← DELEGATED, see below
    depends_on: [{ task_key: setup_control_tables }]
    run_job_task:
      job_id: ${resources.jobs.onboarding_job.id}
      job_parameters:
        spec_file_path: "${workspace.file_path}/onboarding/uc3/<spec>.json"
        catalog: ${var.catalog}
        env: ${bundle.target}
        action_type: CREATE
  - task_key: run_pipeline_update
    depends_on: [{ task_key: onboard_uc3 }]
    pipeline_task: { pipeline_id: ${resources.pipelines.uc3_streaming_cdc_pipeline.id} }
  - task_key: apply_governance_uc3        # ← REQUIRED for tags, see below
    depends_on: [{ task_key: run_pipeline_update }]
    notebook_task:
      notebook_path: ../../notebooks/04_governance/04_apply_governance_and_egress.py
      base_parameters:
        catalog: ${var.catalog}
        dataflow_group_id: dfg_uc3_excalibur_streaming_cdc
        apply_abac: "true"
        capture_cdc_change_counts: "false"
  - task_key: observability_export
    depends_on: [{ task_key: apply_governance_uc3 }]
```

> **Onboarding is DELEGATED, never inlined.** Call the existing generic `onboarding_job` via
> `run_job_task`. Do **not** create an onboarding job or point a `notebook_task` at
> `02_onboarding_engine.py` — older `flowx_test_*_job.yml` files that do are legacy drift.

> **Tagging needs its own task.** Tags are applied *post-deployment*
> (`ALTER TABLE ... SET TAGS` against an already-materialized table), so the pipeline cannot do
> it. **A pipeline that runs green with no governance task applies zero tags and reports no
> error.** UC3 hit exactly this. Verify the *effect*, never infer it from a green run.

> **Check the bundle `include:` glob.** It is an explicit per-directory list, not recursive. A new
> `resources/<uc>/` directory that is not listed deploys **nothing**, silently.

### Step 5 — Deploy, then run in dependency order

```bash
databricks bundle validate -t <target> -p <profile>
databricks bundle deploy   -t <target> -p <profile>
databricks bundle run <job> -t <target> -p <profile>
```

> **Never deploy while a pipeline or test wave is running.** `bundle deploy` prunes superseded
> artifacts, killing in-flight updates with `ENVIRONMENT_PIP_INSTALL_ERROR`.

Run order is the runtime dependency order: simulator → CDC → reconciliation. Do not parallelize
(the shared `setup_control_tables` risks a Unity Catalog create race under concurrency).

### Step 6 — Verify the effect, not the exit code

This is the step most often skipped, and the one that catches the most.

```sql
-- SCD1: one row per PK, deletes applied
SELECT count(*) rows, count(DISTINCT <pk>) pks FROM <cat>.<bronze>.<table>;

-- SCD2: history columns present
DESCRIBE TABLE <cat>.<bronze>.customer;          -- expect __START_AT

-- Null(DF)=Y really NULL
SELECT sum(CASE WHEN acc_password IS NULL THEN 1 ELSE 0 END), count(*) FROM ...;

-- Drop(DF)=Y really absent
SELECT count(*) FROM information_schema.columns
 WHERE table_name='subscriber' AND column_name IN ('ctn_password','sub_password');  -- expect 0

-- the hash is reproducible by hand
SELECT count(*) FROM <t>
 WHERE __framework_hash_key = sha2(concat_ws('||',
        coalesce(trim(lower(cast(<pk> AS STRING))),'__NULL__')), 256);

-- reconciliation metrics
SELECT * FROM <cat>.config.reconciliation_run_log WHERE reconciliation_id LIKE 'rf_<uc>%';

-- governance tags: ALWAYS QUALIFY THE CATALOG (see the warning below)
SELECT table_name, count(DISTINCT column_name) tagged_cols
  FROM <cat>.information_schema.column_tags
 WHERE schema_name = '<bronze_schema>' GROUP BY table_name;
SELECT * FROM <cat>.information_schema.table_tags WHERE schema_name = '<bronze_schema>';
```

> **The single most misleading query in this whole guide.** `information_schema` is
> **catalog-scoped**. An *unqualified* `SELECT ... FROM information_schema.column_tags` resolves
> against `current_catalog()` — and a SQL warehouse with no default catalog puts every session in
> `workspace`. Your tags are in `<cat>`, so the query returns **0 rows, truthfully, for the wrong
> catalog**. This cost UC3 a completely wrong conclusion ("tagging is unsupported on this
> workspace") while 445 tags were sitting in `flowx.information_schema.column_tags` the whole
> time. Always write `<catalog>.information_schema.…`, or use `system.information_schema.…` for a
> metastore-wide answer. There is no `SHOW TAGS` statement in Databricks SQL — its syntax error
> tells you nothing about whether tagging works.
>
> A control experiment does not help if it repeats the flawed step: the "prove it fails on a plain
> Delta table too" test created the probe table in the *default* catalog and verified it with the
> *same* unqualified read.

UC3's verified results: `physical_device` 89 rows (100 − 11 deletes, SCD1), `customer` 100 rows
with `__START_AT` (SCD2), `subscriber` 94 rows (100 − 6 deletes, SCD1); hash matched on 100/100
rows recomputed independently.

---

## 4. Reconciliation — the settings that decide whether it means anything

Batch reconciliation compares a freshly-landed batch table against the CDC target. **Three
settings determine whether the numbers are meaningful**, and two of them defeated UC3 on the
first attempts:

```json
{
  "reconciliation_id": "rf_uc3_customer_batch_vs_bronze",
  "execution_mode": "pipeline",
  "match_keys": ["customer_id"],
  "compare_columns": ["...every business column you consider part of 'the same row'..."],
  "source_config": { "type": "table", "table": "flowx.staging.customer_batch",
                     "read_mode": "batch", "hash_precomputed": false },
  "target_configs": [{
      "target_id": "bronze_customer", "type": "table", "table": "flowx.bronze.customer",
      "read_mode": "batch", "hash_precomputed": false,
      "append_target_table": "flowx.staging.customer_stream"
  }],
  "logging_config": { "run_log_capture": true, "mismatch_log_capture": false }
}
```

| Setting | Get it wrong and… |
|---|---|
| **`compare_columns` omitted** | comparison degrades to **key-presence only**: every co-present key counts as matched and `value_drift_count` is **structurally 0**. UC3 saw exactly this. |
| **`hash_precomputed: true` on the target** | the stored hash was built by the *CDC* engine over *its* comparison set; reconciliation hashes the source over *your* `compare_columns`. Different sets never produce equal hashes, so **everything reports as drifted** and `matched_count` collapses to 0. Set it `false` unless the two sets provably agree. |
| **`run_log_capture: false`** | the `__metrics` dataset is never registered — no metrics at all. Both log flags default to `false` since v1.7.3, so state them explicitly. |
| **`mismatch_log_capture: true`** | you also get a row-level mismatch table. Leave it `false` for metrics-only. |
| **`append_target_table` → Bronze directly** | bypasses CDC. Point it at the **staging** table so healed rows re-enter through the SCD engine and history cannot regress. |

**Metric name mapping** — the framework's names are authoritative:

| You may call it | Framework column | Note |
|---|---|---|
| `matched_count` | `matched_count` | |
| `mismatched_count` | `value_drift_count` | |
| `batch_only_count` | `missing_in_target_count` | ⚠ **includes `VALUE_DRIFT`** — subtract `value_drift_count` for a true batch-only count |
| `bronze_only_count` | `missing_in_source_count` | informational |

**Per-`batch_date` granularity:** `_counts_query` is an ungrouped `.agg()` producing exactly one
row per (flow, target, run) — there is no group-by key. To get per-date metrics, land **one date
per pipeline run**; the run boundary supplies the grain. With `execution_mode: "pipeline"` and a
heal target the source side is a *streaming* read (incremental), while the target side is always a
full batch snapshot — so `missing_in_source_count` stays cumulative-flavoured and should be read
as informational only.

---

## 5. Failure playbook

Every entry below is a failure UC3 actually hit.

| Symptom | Cause | Fix |
|---|---|---|
| `Catalog 'x' does not exist` | catalog is a workspace prerequisite | create it in the UI, or target a workspace that has it |
| `no such directory` on `fs cp` to a Volume | `databricks fs cp` does not create intermediate dirs | `databricks fs mkdir` the tree first |
| `NotebookImportException: ... appears to be a notebook` | a shared `.py` under `notebooks/` deploys as a *notebook*; Databricks refuses `import` | move it to `src/` (deploys as a plain file) and strip the `# Databricks notebook source` header |
| `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE` | `.cache()`/`.persist()` on serverless | remove them; also avoid `sparkContext`, `.rdd`, `repartition` |
| `DELTA_UNIVERSAL_FORMAT_VIOLATION` | UniForm needs **three** properties, not one | the framework now emits all three; see §6 |
| `CANNOT_UPDATE_TABLE_SCHEMA` / `StringType` vs `LongType` | a technical-metadata column's type was non-deterministic | fixed in `technical_metadata.py`; clear with a **full refresh**, not a table drop |
| `Path must be absolute: ${param}` | `${param}` not substituted in the source plane | fixed; `${param}` resolves **fresh at pipeline-update time** |
| Green run, **zero tags** | no governance task in the job | add the `04_apply_governance_and_egress.py` task |
| Governance task green, still **zero tags** in `information_schema` | you queried the **wrong catalog** — `information_schema` is catalog-scoped and the warehouse's default is not yours | qualify it: `<catalog>.information_schema.column_tags`, or `system.information_schema.column_tags` |
| `value_drift_count` always 0 | `compare_columns` omitted | declare it |
| `matched_count` always 0 | `hash_precomputed: true` with a divergent comparison set | set `false` |
| Deploy aborts on an Apps resource | known CLI update-mask bug | harmless — jobs/pipelines deploy first; verify with `bundle summary` |

> **A schema-merge conflict cannot be fixed incrementally, and dropping the table does not clear
> it** — the conflicting schema lives in the *pipeline's* state. Use a full refresh:
> `databricks pipelines start-update <id> --full-refresh`.

---

## 6. Iceberg / UniForm — read-only external access

Set `target_config.table_properties.enable_iceberg_read_uniformity: true`. The framework then
emits a **different property set depending on `target_type`**, because Databricks has two
IcebergCompat generations and only one works on pipeline-managed datasets:

| `target_type` | Generation | Properties emitted |
|---|---|---|
| `batch_table` | **IcebergCompatV2** | `delta.columnMapping.mode=name`, `delta.enableIcebergCompatV2=true`, `delta.universalFormat.enabledFormats=iceberg` |
| `streaming_table`, `materialized_view` | **IcebergCompatV3** | the above `columnMapping`/`enabledFormats`, plus `delta.enableIcebergCompatV3=true`, `delta.enableRowTracking=true`, `pipelines.externalMetadata.enabled=true` |

> *"Iceberg reads can't be enabled on materialized views or streaming tables using
> IcebergCompatV2. However, for pipeline-managed materialized views and streaming tables, you can
> enable external Iceberg access using IcebergCompatV3 instead."* — Databricks docs

**Requirements for the V3 path:** DBR **17.3+**, and the dataset must be pipeline-managed.

### The CDF trade-off — know this before you enable it

**IcebergCompatV3 and Change Data Feed are mutually exclusive.** The framework enables CDF for
every CDC-dispatched strategy (SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC), so requesting Iceberg read
access on such a flow forces a choice. The framework resolves it as:

- the **explicit spec request wins** — `delta.enableChangeDataFeed` is suppressed;
- the suppression is **logged at WARNING** naming exactly what is lost. It is never silent;
- **what you lose:** `capture_all_scd_change_counts` can no longer compute insert/update/delete
  counts for that target (it reads the CDF). Reconciliation, SCD correctness and the framework
  hashes are unaffected.

If those change counts matter more than external Iceberg access, do not set the flag.

### Why the property list is so specific

Every property was discovered by a **live failure**, one per pipeline update, because Delta
validates them in sequence and reports only the *next* missing one:

```
enabledFormats alone        → DELTA_UNIVERSAL_FORMAT_VIOLATION ("requires IcebergCompat")
+ enableIcebergCompatV2     → WRONG_REQUIRED_TABLE_PROPERTY   ("columnMapping.mode must be name")
+ columnMapping.mode=name   → DELETION_VECTORS_SHOULD_BE_DISABLED   ← the V2 wall on a streaming table
```

That third error is not a fourth hurdle to clear — it is the point at which V2 simply cannot work
on a pipeline-managed dataset, and the reason the V3 path exists.

## 7. Definition of Done

A change is complete when every artefact that *describes* it agrees with it. See `AGENTS.md` for
the nine steps. The two most often skipped:

- **Prove a regression test can fail.** Break the fix deliberately, watch the test go red, restore
  it. UC3 found a feature that had been broken in production while its unit tests passed green —
  they asserted only one of the three required properties.
- **Verify the effect in the platform**, not the exit code. A green job proves the tasks ran; it
  does not prove tags landed, deletes applied, or drift was detected.
