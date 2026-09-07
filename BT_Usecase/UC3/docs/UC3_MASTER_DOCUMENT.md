<style>
:root { --aptos: Aptos, "Aptos Display", "Segoe UI Variable", "Segoe UI", system-ui, -apple-system, sans-serif; }
body, .md-typeset, .md-typeset table, .md-typeset h1, .md-typeset h2,
.md-typeset h3, .md-typeset h4, .md-typeset p, .md-typeset li { font-family: var(--aptos) !important; }
.md-typeset code, .md-typeset pre { font-family: "Cascadia Code", Consolas, "Courier New", monospace !important; }
.screenshot { border: 1px dashed #9aa0a6; background: #f6f7f9; padding: 14px 16px;
  margin: 12px 0; border-radius: 6px; color: #4a4f55; font-family: var(--aptos); }
.screenshot b { color: #1a1a1a; }
</style>

# UC3 Excalibur to Databricks : Master Document

**Use case:** UC3 — BT Group Excalibur (Oracle CRM) migration to Databricks Lakehouse
**Catalog:** `flowx`  **Target / profile:** `metaflow_v7`  **Framework:** FlowX metadata-driven ingestion
**Status:** All three jobs verified green end-to-end. Every number in this document was read back from the live workspace.

---

## Index

| # | Section | What you will learn |
|---|---|---|
| 1 | [Business Context](#1-business-context) | Why UC3 exists, who cares |
| 2 | [As-Is Process](#2-as-is-process-today-in-excalibur) | How Excalibur works today, and its pain points |
| 3 | [To-Be on Databricks](#3-to-be-the-databricks-implementation) | The target architecture in one picture |
| 4 | [The Data We Use](#4-the-data-we-use) | Three tables, their PKs, where definitions come from |
| 5 | [Generated Test Data](#5-generated-test-data-what-exactly-is-produced) | What the generator makes and why |
| 6 | [File Locations](#6-file-locations-the-exact-paths) | Exact paths, in repo and on the volume |
| 7 | [Onboarding JSON - Attribute by Attribute](#7-onboarding-json-attribute-by-attribute) | Every attribute, and *why* it is set that way |
| 8 | [The Three Jobs](#8-the-three-jobs) | Task-by-task walkthrough |
| 9 | [DLT Pipeline DAGs](#9-dlt-pipeline-dags-every-node-explained) | Every node: is it a table or a view? |
| 10 | [Table Details and Metadata](#10-table-details-and-metadata) | Columns, hash spec, properties |
| 11 | [PII Handling](#11-how-pii-is-handled) | Drop, Null, Tag - no masking |
| 12 | [How NULLs Are Handled](#12-how-nulls-are-handled) | Three distinct meanings of NULL |
| 13 | [Access Restriction](#13-how-access-restriction-is-handled) | Tags to ABAC policies |
| 14 | [CDC Explained With Data](#14-cdc-explained-with-real-data) | Real rows, before and after |
| 15 | [Timestamp Traceability](#15-timestamp-traceability-simulated-to-bronze) | Simulated time vs landed time |
| 16 | [Testing and Validation SQL](#16-testing-and-validation-sql-for-business-users) | Copy-paste queries |
| 17 | [Event Log Proof of Counts](#17-event-log-proof-of-counts) | Prove counts from the pipeline own log |
| 18 | [How Databricks Simplifies This](#18-how-databricks-simplifies-the-process) | Before/after comparison |
| 19 | [Reference Links](#19-databricks-reference-links) | Official documentation |
| A | [Framework Defects Found](#appendix-a-framework-defects-found-by-this-build) | Five real bugs this build surfaced |
| B | [Operational Runbook](#appendix-b-operational-runbook) | How to run it, and what to do when it breaks |

---

## 1. Business Context

**What is Excalibur?**

- Excalibur is BT Group's **Oracle-based CRM system**.
- It holds customer, subscriber and device records for the mobile business.
- It is the **system of record** for "who is the customer, what number do they have, what handset is on it".

**Why migrate to Databricks?**

| Business driver | What it means in practice |
|---|---|
| **Cost** | Oracle licensing plus hardware is expensive to scale. |
| **Analytics ceiling** | Analysts cannot run heavy queries on a live OLTP CRM without hurting call-centre response times. |
| **Data silos** | Excalibur data cannot easily be joined with billing, network or usage data. |
| **Regulatory** | GDPR requires provable control over PII. Oracle grants are coarse-grained. |
| **AI/ML readiness** | Models need historical data. Excalibur only keeps the *current* state. |

**What UC3 delivers:**

- A **governed Bronze layer** in Databricks holding all three Excalibur tables.
- **Near-real-time** updates (streaming CDC), not overnight-only.
- **Full history** on the customer table (SCD2) — Excalibur cannot do this.
- **Automated PII controls** — tags applied from a governance sheet, no manual work.
- **Automated reconciliation** — proves the streaming data is correct, and self-heals when it is not.

---

## 2. As-Is Process (Today, in Excalibur)

**How it works today:**

```
Oracle Excalibur (OLTP)
        |
        |  nightly full extract  (batch, once per day)
        v
   Flat files on a landing server
        |
        |  ETL tool (row-by-row processing)
        v
   Downstream reporting DB
```

**Point-by-point pain:**

1. **Latency is one full day.** A SIM swap at 9 AM is invisible to reporting until the next morning.
2. **Full extract every night.** The whole table is pulled even if only 200 rows changed. Wasteful and slow.
3. **No history.** The extract overwrites yesterday. Nobody can answer *"what was this customer address in March?"*
4. **Deletes are invisible.** When a row is deleted in Oracle it simply stops appearing. No record that it ever existed.
5. **PII handled manually.** A spreadsheet lists sensitive columns. An engineer applies the rules by hand. Errors are common and silent.
6. **No reconciliation.** If the ETL drops rows, nobody finds out until a business user notices a wrong number.
7. **Schema changes break things.** A new column in Oracle silently fails the load.

**The core problem:** every one of these is a *manual* control. Manual controls fail quietly.

---

## 3. To-Be — The Databricks Implementation

**The whole picture:**

```
  Excalibur (Oracle)
        |
        |  [ simulated in this build by Job 1 ]
        v
  +----------------------------------------------------+
  |  STREAMING PATH                 BATCH PATH         |
  |  (near real-time)               (daily files)      |
  |                                                    |
  |  flowx.staging.<t>_stream       /Volumes/.../batch  |
  |         |                               |          |
  |    [ Job 2 : 004 ]               [ Job 3 : 005 ]   |
  |    streaming CDC                 autoloader        |
  |         |                               |          |
  |         v                               v          |
  |   flowx.bronze.<t>           flowx.staging.<t>_batch|
  |    (SCD1 / SCD2)                        |          |
  |         ^                               |          |
  |         |         reconciliation        |          |
  |         +-------------------------------+          |
  |            heal: append missing rows               |
  +----------------------------------------------------+
```

**Reading it in words:**

1. **Job 1** pretends to be Excalibur — it drips rows into staging tables on a timer.
2. **Job 2** reads those tables as a **stream** and applies CDC into governed Bronze tables.
3. **Job 3** reads the **daily batch files**, then **compares** batch against Bronze.
4. Where the batch has rows the stream missed, Job 3 **feeds them back** into the stream lane, and Job 2 CDC engine applies them properly.

**The key idea:** the batch path is not a second copy of the data. It is an **audit and repair mechanism** for the streaming path.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 1 ]</b><br/>
Databricks Workflows list showing the three UC3 jobs:<br/>
<code>003_lfj_uc3_excalibur_streaming_simulator</code>, <code>004_lfj_uc3_excalibur_streaming_cdc</code>, <code>005_lfj_uc3_excalibur_batch_recon</code> — all with green SUCCESS status.</div>

---

## 4. The Data We Use

### 4.1 The three tables

| Table | Business meaning | Primary Key | SCD strategy |
|---|---|---|---|
| `PHYSICAL_DEVICE` | The handset / SIM on an account | `customer_id`, `subscriber_no`, `equipment_no`, `phy_seq_no` (4 cols) | **SCD1** |
| `CUSTOMER` | The account holder | `customer_id` (1 col) | **SCD2** |
| `SUBSCRIBER` | The mobile line / MSISDN | `subscriber_no`, `customer_id` (2 cols) | **SCD1** |

**Why CUSTOMER is SCD2 and the other two are SCD1:**

- **SCD2 = keep history.** Customer attributes (address, name, contact preference) have legal and analytical value over time. GDPR requests and disputes need "what did we hold, and when".
- **SCD1 = keep latest only.** A device current state is what matters operationally. Nobody asks "what was the IMEI three months ago" — the device either is or is not on the line.

### 4.2 Column counts (verified against the governance sheets)

| Table | Business columns in sheet | Dropped | Nulled | Column tags applied |
|---|---|---|---|---|
| `PHYSICAL_DEVICE` | 31 | 4 legacy `gcp_*` | 2 | **8** |
| `CUSTOMER` | 90 | 4 legacy `gcp_*` | 3 | **33** |
| `SUBSCRIBER` | 133 | `ctn_password`, `sub_password` + 4 `gcp_*` | 0 | **18** |

### 4.3 Where the column definitions come from

The column lists are **not hand-typed anywhere**. They are read at runtime from three governance sheets:

- [docs/UC3/PHYSICAL_DEVICE_DDL.csv](PHYSICAL_DEVICE_DDL.csv)
- [docs/UC3/CUSTOMER_DDL.csv](CUSTOMER_DDL.csv)
- [docs/UC3/SUBSCRIBER_DDL.csv](SUBSCRIBER_DDL.csv)

**Sheet columns that drive behaviour:**

| Sheet column | Effect |
|---|---|
| `Reservoir Column Name` | The source column name. Blank means the row is skipped. |
| `Feed column name` | Blank means the column is not ingestible, skipped. |
| `Primary Key` | Marks PK membership. |
| `Drop(Specific to Data Fabric)` = `Y` | Column is **absent entirely** from the target. |
| `Null(Specific to Data Fabric)` = `Y` | Column **exists but is always NULL**. |
| `Sesnitive Columns` *(misspelt in source)* | PII sensitivity flag. |
| `csql.ro`, `csql.secured_ro`, `bq.deid_ro`, `Tokenise` | Access-tier flags, become governance tags. |

> **Note on two real quirks, deliberately preserved:** the header `Sesnitive Columns` is misspelt in the source sheets, and `PHYSICAL_DEVICE` spells the tier flag `csql.securedro` while the other two use `csql.secured_ro`. The code reads them **as-is with a fallback**. We never "corrected" the source data — correcting it would mean the code no longer matches the file BT actually sends.

---

## 5. Generated Test Data — What Exactly Is Produced

Real Excalibur data cannot be used for development. `scripts/generate_uc3_test_data.py` produces a realistic stand-in.

### 5.1 What it generates

| Output | Rows | Purpose |
|---|---|---|
| `streaming/<table>/<table>_stream.csv` | **100 rows** | Fed by Job 1 into the streaming lane |
| `batch/<table>/batch_date=YYYY-MM-DD/<table>_batch.csv` | **30 rows x 4 dates = 120** | Fed by Job 3 into the batch lane |

**The four batch dates:** `2026-08-01`, `2026-08-02`, `2026-08-03`, `2026-08-04`.

### 5.2 How the batch set is deliberately made to *disagree* with the stream

This is the most important design point in the test data. The batch set is **engineered to contain discrepancies**, because a reconciliation that always reports "everything matches" proves nothing.

| Constant | Value | What it produces |
|---|---|---|
| `BATCH_OVERLAP_FRACTION` | **0.60** | 60% of batch rows reuse a streaming PK, so these *can* match |
| `BATCH_MUTATION_FRACTION` | **0.55** | 55% of those have changed values, giving **`value_drift_count`** |
| *(remaining 40%)* | PKs >= 1000 | Genuinely new rows, giving **`missing_in_target_count`** |
| `DELETE_FRACTION` | **0.06** | About 6% of rows carry `src_deleted_flg = '1'`, testing delete handling |
| `SEED` | `20260803` | **Fixed seed** — regenerating gives byte-identical files |

**In plain terms:** we deliberately made the batch file disagree with the stream, so we can prove the reconciliation *notices*.

### 5.3 Verified generated volumes

| Table | Stream rows | Distinct PKs | Rows with `src_deleted_flg='1'` |
|---|---|---|---|
| `physical_device_stream` | 100 | 100 | **11** |
| `customer_stream` | 100 | 100 | **6** |
| `subscriber_stream` | 100 | 100 | **6** |

---

## 6. File Locations — The Exact Paths

### 6.1 In the repository (source of truth)

| What | Path |
|---|---|
| Governance sheets | `docs/UC3/*_DDL.csv` |
| Test data generator | `scripts/generate_uc3_test_data.py` |
| Streaming CDC spec | `onboarding/uc3/uc3_excalibur_streaming_cdc.json` |
| Batch and recon spec | `onboarding/uc3/uc3_excalibur_batch_recon.json` |
| Job / pipeline YAMLs | `resources/uc3/*.yml` |
| Simulator notebooks | `notebooks/uc3/simulator/*.py` |
| Shared DDL parser | `src/uc3_simulator/uc3_ddl_schema.py` |

> **Why the DDL parser lives in `src/` and not `notebooks/`:** anything Databricks must `import` as a Python module cannot live under `notebooks/`. DABs deploys files there as *notebooks*, and Databricks refuses `import` on a notebook (`NotebookImportException`). Files under `src/` deploy as plain files and import normally.

### 6.2 On the Unity Catalog volume (runtime data)

**Root:** `/Volumes/flowx/staging/uc_3/`

| Path | Contents |
|---|---|
| `/Volumes/flowx/staging/uc_3/streaming/<table>/` | The 100-row streaming CSV that Job 1 drains |
| `/Volumes/flowx/staging/uc_3/batch/<table>/batch_date=YYYY-MM-DD/` | The 4 x 30-row batch CSVs Job 3 reads |
| `/Volumes/flowx/staging/uc_3/_schemas/<table>_batch/` | Auto Loader schema-inference checkpoints |
| `/Volumes/flowx/observability/app_logs/streaming_cdc/` | Exported observability JSON |

> **Gotcha worth knowing:** `databricks fs cp` does **not** create intermediate directories on a UC Volume. It fails with `no such directory`. Create the directory tree first.

### 6.3 Tables created

| Layer | Tables |
|---|---|
| **Staging (stream)** | `flowx.staging.{physical_device,customer,subscriber}_stream` |
| **Staging (batch)** | `flowx.staging.{physical_device,customer,subscriber}_batch` |
| **Bronze (governed)** | `flowx.bronze.{physical_device,customer,subscriber}` |
| **Control tables** | `reconciliation_run_log`, `reconciliation_result`, `ingestion_flow_spec`, and others |

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 2 ]</b><br/>
Catalog Explorer showing <code>flowx</code> with the <code>staging</code> and <code>bronze</code> schemas and the UC3 tables listed.</div>

---
## 7. Onboarding JSON — Attribute by Attribute

This is the heart of the build. **No CDC code, no hashing code, no tagging code and no MERGE statement was written by hand.** All behaviour below comes from JSON attributes.

> **Reading the JSON.** Each spec starts with `$schema` (editor validation against `onboarding_templates/onboarding_spec.schema.json`) and one `_about` header — use case, description, framework version, date, developer. `_about` is the **only** key the framework does not read: JSON has no comment syntax, so the framework reserves the `_` prefix for author notes and ignores them (`spec_validator.py`). Every other key is a real framework attribute, present in the standard template `onboarding_templates/pipeline_onboarding_template.json` in the same order — including `data_standardization_sql`, which is how `Null(DF)=Y` is enforced (section 11.3). All tags live under `governance_tags` and nowhere else.

### 7.1 Job 2 spec — `uc3_excalibur_streaming_cdc.json`

**Group level:**

```json
{
  "dataflow_group_id": "dfg_uc3_excalibur_streaming_cdc",
  "ingestion_flows": [ "... 3 flows ..." ],
  "observability": [ "..." ]
}
```

| Attribute | Value | Why configured this way |
|---|---|---|
| `dataflow_group_id` | `dfg_uc3_excalibur_streaming_cdc` | The group is the unit of deployment. One group equals one pipeline. The pipeline YAML references only this id. |

**Source configuration (per flow):**

| Attribute | Value | Why |
|---|---|---|
| `source_type` | `zerobus` | Reads an **existing Delta table** as a stream. The simulator writes Delta, so this is the correct reader — not `autoloader`, which reads files. |
| `source_catalog` | `{{catalog}}` | **Never hardcode the catalog.** Substituted at onboarding time so the same spec works in dev, test and prod. |
| `source_schema` / `source_table` | `staging` / `<table>_stream` | The simulator output tables. |
| `capture_technical_metadata` | `true` | Adds `__framework_ingestion_timestamp_utc` and source-file lineage columns. This is what makes section 15 traceability possible. |
| `column_normalization` | `{enabled: true, case: "lower"}` | Oracle sheets are UPPERCASE; Databricks convention is lowercase. Normalising once at the boundary means no downstream query ever has to guess the case. |
| `data_standardization_sql` | `["CAST(NULL AS STRING) AS esn_pin", "..."]` | **This is how `Null(DF)=Y` is enforced.** The column still exists (schema stability) but is forced to NULL at ingestion. |

**Target configuration (per flow):**

| Attribute | `physical_device` | `customer` | `subscriber` | Why |
|---|---|---|---|---|
| `target_type` | `streaming_table` | same | same | Continuously updated, checkpointed. |
| `cdc_load_strategy` | `SCD1` | **`SCD2`** | `SCD1` | The business rule from 4.1. **One word switches the whole history model.** |
| `primary_keys` | 4 cols | 1 col | 2 cols | Identity for CDC matching. Order is load-bearing — it is the basis of `__framework_hash_key`. |
| `sequence_by_column` | `sys_update_date` | same | same | **Which change wins.** Out-of-order arrivals are ordered by this, not by arrival time. Critical for correctness. |
| `cdc_operation_column` | `src_deleted_flg` | same | same | The column carrying the delete signal. |
| `cdc_operation_mapping` | `{"delete_values": ["1"]}` | same | same | Value `'1'` means "delete this row". |
| `generate_hash_columns` | `true` | same | same | Framework materialises `__framework_hash_key` and `__framework_hash_value`. **One boolean replaces a hand-written SHA-256 loop.** |
| `columns_to_exclude` | `["sys_creation_date","sys_update_date"]` | same | same | Excluded from the **value hash** — audit timestamps change on every touch and would make every row look drifted. |
| `liquid_clustering_columns` | `["__framework_hash_key"]` | `["customer_id"]` | `["subscriber_no","customer_id"]` | Physical layout for fast lookups. |

> **Why `physical_device` clusters on `__framework_hash_key` and not its PK:** Liquid clustering supports a **maximum of 3 columns**. `physical_device` has a **4-column** PK. Rather than truncate the PK — which would cluster on a partial key and skew the layout — the framework clusters on the single hash column that already encodes all four. **The PK itself is never truncated.**

**Governance (per flow):**

```json
"governance_tags": {
  "table_tags": { "source_system": "excalibur", "use_case": "uc3", "domain": "crm" },
  "column_tags": [ { "column": "customer_id", "tags": { "...8 tags..." } } ]
}
```

Full detail in section 11.

### 7.2 Job 3 spec — `uc3_excalibur_batch_recon.json`

**Pipeline parameters:**

```json
"pipeline_parameters": { "landing_root": "/Volumes/flowx/staging/uc_3/batch" }
```

| Attribute | Why |
|---|---|
| `pipeline_parameters` | Declares `${landing_root}`, referenced by all three source paths. **Resolved fresh on every pipeline update**, so an operator can retarget paths without re-onboarding. |

**Batch ingestion flows:**

| Attribute | Value | Why |
|---|---|---|
| `source_type` | `autoloader` | Reads **files** incrementally with a checkpoint. Only new files are picked up on each run. |
| `path` | `${landing_root}/<table>/` | Points at the **top-level table folder**, not a pinned `batch_date`. Auto Loader discovers new date folders automatically. |
| `schema_location` | `/Volumes/.../_schemas/<table>_batch/` | Where Auto Loader remembers the inferred schema. |
| `reader_options.cloudFiles.inferColumnTypes` | `true` | Infers real types from CSV rather than making everything a string. |
| `cdc_load_strategy` | **`APPEND`** | **Deliberately not a CDC strategy.** Staging keeps all four dated sets side by side as an audit record. |
| `partition_columns` | `["batch_date"]` | Enables efficient per-day filtering. |

> **Why no `generate_hash_columns` on the batch side:** the hashes for comparison are computed by the **reconciliation engine itself** (`hash_precomputed: false`). Adding them here would compute a hash over a *different* column set and cause exactly the mismatch described below.

**Reconciliation flows:**

| Attribute | Value | Why |
|---|---|---|
| `execution_mode` | `pipeline` | Runs **inside the DAG**, not as a separate job task. Lakeflow tracks lineage natively. |
| `match_keys` | The table PK | How a source row is paired with a target row. |
| `source_config.table` | `flowx.staging.<table>_batch` | The batch side. |
| `target_configs[].table` | `flowx.bronze.<table>` | The Bronze side produced by Job 2. |
| `compare_columns` | 25 / 87 / 127 columns | **Which columns must agree** for a row to count as matched. |
| `hash_precomputed` | **`false`** on both sides | See the critical note below. |
| `comparison_direction` | `both` | Reports rows missing in target *and* rows missing in source. |
| `append_target_table` | `flowx.staging.<table>_stream` | **The self-healing lane.** |
| `two_tier_verification` | `true` | Fast hash comparison first, then column-level detail only for rows that differ. |
| `logging_config.run_log_capture` | `true` | Writes metrics to `reconciliation_run_log`. **This is what section 16 queries read.** |
| `logging_config.mismatch_log_capture` | `false` | Per-record detail suppressed — would be very large and is not needed for the metrics. |

> ### Two configuration traps, both hit for real in this build
>
> **1. `compare_columns` omitted means `value_drift_count` is structurally 0.** Without it, matching is **key-presence only**: a row whose values changed still counts as matched. The metric is not broken — it is answering "does this key exist?", not "do the values agree?". Always declare `compare_columns` when you want drift detection.
>
> **2. `hash_precomputed: true` on the target makes `matched_count` collapse to 0.** Bronze `__framework_hash_value` was built by the **CDC engine** over its comparison column set. The recon builds its source hash over **`compare_columns`**. Two different column sets give two different hashes, so every row reports as drifted. Setting `hash_precomputed: false` on **both** sides makes the engine compute both hashes over the same set. **After changing this, a `--full-refresh` is required** — otherwise the pipeline serves cached datasets and the metrics do not move.

### 7.3 `${param}` vs `{{catalog}}` — two different lifecycles

This distinction matters and is easy to get wrong.

| Placeholder | Resolved when | By what | Stored in control table as |
|---|---|---|---|
| `{{catalog}}`, `{{env}}` | **Onboarding time** (once) | `onboarding/spec_loader.py` | the **resolved value** |
| `${param}` | **Every pipeline update** | `engine/source_plane.py` and `engine/flow_generators.py` | the **raw placeholder** |

**Why the difference is deliberate:** the catalog is fixed for a deployment, so resolving it once is right. A path may need retargeting without re-onboarding, so it stays a placeholder in the control table and resolves fresh each run.

---

## 8. The Three Jobs

### 8.1 Job 1 — `003_lfj_uc3_excalibur_streaming_simulator`

**This is the one job in UC3 that is legitimately hand-written code.** It fakes a source system, and there is no framework feature for "pretend to be a message bus".

| Task | Notebook | What it does |
|---|---|---|
| `delta_table_setup` | `01_delta_table_setup.py` | `CREATE TABLE IF NOT EXISTS` for the three `<table>_stream` tables plus a cursor table. Column lists derived from the DDL sheets at runtime. |
| `stream_producer` | `02_stream_producer.py` | Every 20 seconds, appends the next 15 rows of each CSV into its staging table. |

**Job parameters:**

| Parameter | Default | Purpose |
|---|---|---|
| `catalog` | `${var.catalog}` (equals `flowx`) | Never hardcoded. |
| `schema` | `staging` | Target schema. |
| `sleep_seconds` | `20` | Gap between ticks. |
| `chunk_size` | `15` | Rows per table per tick. |
| `max_ticks` | `200` | Hard safety ceiling — the loop cannot run forever. |

**Why chunked, not one big load:** loading 100 rows at once would give the downstream stream **one** micro-batch. Streaming CDC would be untested. **7 ticks** gives 7 genuine micro-batches, which is what proves the CDC engine handles incremental arrival.

**Verified run:**

```
delta_table_setup: physical_device_stream 32 cols | customer_stream 91 | subscriber_stream 132
                   drops enforced (subscriber: ctn_password, sub_password absent)
stream_producer  : 7 ticks, 220.7s, chunk_size=15 -> 100 rows/table, all three drained
```

**Idempotent:** every DDL is `IF NOT EXISTS`; the producer resumes from a Delta cursor. Re-running after a full drain is a **no-op**.

### 8.2 Job 2 — `004_lfj_uc3_excalibur_streaming_cdc`

| # | Task | What it does | Why it must be here |
|---|---|---|---|
| 1 | `setup_control_tables` | Applies control-table migrations | `bundle deploy` does **not** do this. |
| 2 | `onboard_uc3` | **`run_job_task`** to the generic `onboarding_job` | Reads the JSON, validates it, writes control rows. **Delegated, never inlined.** |
| 3 | `run_pipeline_update` | Triggers pipeline `004_ldp_...` | Where the actual data work happens. |
| 4 | `apply_governance_uc3` | Applies tags | **Post-deployment, not in-pipeline** — see below. |
| 5 | `observability_export` | Exports run telemetry | Feeds `/Volumes/flowx/observability/app_logs/`. |

> **Why tagging needs its own task.** `ALTER TABLE ... SET TAGS` runs against an **already-materialised** table. It cannot run inside the pipeline update, because during the update the table does not yet exist in its final form. This build proved it the hard way: Job 2 succeeded end-to-end with **zero tags applied**, because no task ever invoked the tagging step. The dependency `apply_governance` after `run_pipeline_update` is mandatory.

> **Why onboarding is a `run_job_task`.** There is exactly **one** onboarding entrypoint in the bundle. Inlining `02_onboarding_engine.py` into a new job creates a second copy that drifts. The job depends only on the spec **path**.

### 8.3 Job 3 — `005_lfj_uc3_excalibur_batch_recon`

Same five-task shape, driving pipeline **`006_ldp_uc3_excalibur_batch_recon`**.

> **The 005 / 006 numbering mismatch is deliberate.** Job 2 job and pipeline are both `004`. Job 3 are `005` and `006`. This is intentional and preserved from the original specification. Do not "fix" it.

**Runtime dependency:** Job 3 **must** run after Job 2. It compares against `flowx.bronze.<table>`, which does not exist until Job 2 publishes it.

**Never run the three jobs in parallel.** They share `setup_control_tables`, and concurrent Unity Catalog `CREATE` calls hit a known race.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 3 ]</b><br/>
Job run detail for <code>004_lfj_uc3_excalibur_streaming_cdc</code> showing all five tasks green.</div>

---

## 9. DLT Pipeline DAGs — Every Node Explained

**The one rule that shapes both DAGs:** *one external read per source table, per execution mode.* Reading the same source twice means paying twice and risking two different answers.

### 9.1 Job 2 DAG — streaming CDC

```
  flowx.staging.physical_device_stream --+
                                         |
                                         +--> [ _src_... ] --> AUTO CDC --> flowx.bronze.physical_device
  flowx.staging.customer_stream ---------+    (temporary)                   flowx.bronze.customer
                                         |                                  flowx.bronze.subscriber
  flowx.staging.subscriber_stream -------+
```

| Node | Type | Stored? | Why it exists |
|---|---|---|---|
| `_src_<fingerprint>__stream` | `@dlt.table(temporary=True)` | **Pipeline-scoped only.** Materialised, but **not published** to Unity Catalog. | The single read boundary. Materialised rather than a view because a view is **inlined into each consumer** — "declared once" is not "read once". Only materialisation guarantees one read. |
| `flowx.bronze.<table>` | Streaming table | **Yes — this is the real data.** | The CDC target. Written by AUTO CDC (`apply_changes`). |
| `_<table>_scd2_history` | Hidden backing table | Yes (internal) | **`customer` only.** Lakeflow internal SCD2 history, managed via `stored_as_scd_type="2"`. |

**How to read this:** if a name starts with `_`, it is **internal plumbing** — it holds no business data you should query. Query `flowx.bronze.<table>`.

### 9.2 Job 3 DAG — batch and reconciliation (five layers)

```
 /Volumes/.../batch/<table>/  --> [ autoloader ] --> flowx.staging.<table>_batch   (L1: real table)
                                                              |
                                                              v
                                    _recon__<id>__src         (L3: temporary)
                                    _recon__<id>__<tgt>__tgt  (L3: temporary)  <-- flowx.bronze.<table>
                                                              |
                                                              v
                                    _recon__<id>__<tgt>__classified  (L4: temporary)
                                                              |
                              +-------------------------------+-----------------+
                              v                               v                 v
                     recon__<id>__<tgt>__metrics    _recon__..__missing    (mismatch: OFF)
                          (L4: PUBLISHED)              (L4: temporary)
                                                              |
                                                              v
                                    _recon__<id>__heal_sink  (L5) --> appends into
                                                                      flowx.staging.<table>_stream
```

**Every node, and whether it stores data:**

| Layer | Node | Stored in Unity Catalog? | Purpose |
|---|---|---|---|
| L1 | `flowx.staging.<table>_batch` | **YES** | The landed batch data. Query this. |
| L3 | `_recon__<id>__src` | **No** (temporary) | One shared hash-prepared read of the source. Paid **once** regardless of target count. |
| L3 | `_recon__<id>__<tgt>__tgt` | **No** (temporary) | The Bronze side, read as batch. |
| L4 | `_recon__<id>__<tgt>__classified` | **No** (temporary) | The full-outer-join classification. Read up to 3 times downstream, so materialised to compute the join once. |
| L4 | `recon__<id>__<tgt>__metrics` | **YES — published** | **One row** of counts. This is the audit record. |
| L4 | `..__mismatch` | **Not registered** | Per-record detail. `mismatch_log_capture: false`, so this node does not exist in our DAG. |
| L4 | `_recon__<id>__<tgt>__missing` | **No** (temporary) | The rows to heal. |
| L5 | `_recon__<id>__pulse` | **No** (temporary) | A one-column streaming projection whose only job is to give the sink something to trigger on. |
| L5 | `_recon__<id>__heal_sink` | **Never a dataset** | The handler that appends healed rows back into the stream lane. |

> **The single most useful thing to understand here:** of the roughly 9 nodes per reconciliation flow, only **two** hold queryable business data — the `_batch` table and the `__metrics` table. Everything prefixed `_` is intermediate. This is the **Intermediate Object Rule**: intermediates are materialised for correctness and performance but never published, so the catalog stays clean.

> **Why the healing path needs a "pulse" at all.** `foreach_batch_sink` is streaming-only and needs a stream to trigger on. The pulse is a minimal one-column stream that exists purely to fire the sink once per update, joined against a one-row aggregate of `__classified` **as an ordering edge** — this is what forces Lakeflow to schedule healing *after* classification completes.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 4 ]</b><br/>
Lakeflow pipeline graph view for <code>006_ldp_uc3_excalibur_batch_recon</code>, showing the L1 to L3 to L4 to L5 node chain.</div>

---

## 10. Table Details and Metadata

### 10.1 Bronze table anatomy

Each Bronze table contains these groups of columns:

| Group | Columns | Origin |
|---|---|---|
| **Business** | `customer_id`, `subscriber_no`, and the rest | From Excalibur, minus `Drop(DF)=Y` |
| **Framework audit** | `__framework_ingestion_timestamp_utc`, `__framework_source_file_name`, `__framework_source_file_size`, `__framework_source_file_modification_time` | Added by `capture_technical_metadata: true` |
| **Framework hash** | `__framework_hash_key`, `__framework_hash_value` | Added by `generate_hash_columns: true` |
| **SCD2 tracking** | `__START_AT`, `__END_AT` | **`customer` only.** Managed by Lakeflow. |

### 10.2 The hash specification

| Property | Value |
|---|---|
| Algorithm | **SHA-256**, hex encoding |
| Expression | `sha2(concat_ws('||', coalesce(trim(lower(cast(c AS STRING))), '__NULL__'), ...), 256)` |
| `__framework_hash_key` | Over `primary_keys`, **in declared order** |
| `__framework_hash_value` | Over all business columns, **alphabetically sorted**, PKs excluded structurally, minus `columns_to_exclude` |

**Why each normalisation step exists:**

- `cast(... AS STRING)` — a number and its string form must hash identically across systems.
- `lower()` and `trim()` — `"BT Group "` and `"bt group"` are the same value.
- `coalesce(..., '__NULL__')` — without a sentinel, `('a', NULL)` and `(NULL, 'a')` would produce the same concatenation. The sentinel keeps them distinct.
- `'||'` separator — prevents `('ab','c')` colliding with `('a','bc')`.

**Verified:** the hash was recomputed by hand in SQL and matched **100 of 100 rows**.

### 10.3 Table properties

| Property | Value | Effect |
|---|---|---|
| `delta.columnMapping.mode` | `name` | Allows column rename and drop without rewrite. |
| `delta.universalFormat.enabledFormats` | `iceberg` | UniForm — readable by Iceberg clients. |
| `delta.enableIcebergCompatV3` | `true` | **Streaming tables and MVs.** V2 does not work on pipeline-managed tables. |
| `delta.enableIcebergCompatV2` | `true` | **Batch tables only.** |
| `delta.enableRowTracking` | `true` | Required by V3. |

> **A real constraint worth knowing:** `delta.enableChangeDataFeed` is **suppressed under IcebergCompatV3** — they are mutually incompatible. The framework logs a WARNING naming what is lost rather than failing silently.

---
## 11. How PII Is Handled

### 11.1 Three mechanisms, in order of strength

| # | Mechanism | Driven by | Result |
|---|---|---|---|
| 1 | **Drop** | `Drop(DF)=Y` | Column **does not exist**. Strongest possible control. |
| 2 | **Null** | `Null(DF)=Y` | Column exists, **always NULL**. Schema stable, no data. |
| 3 | **Tag** | governance flags | Column exists with data, **labelled** for access policy. |

> **There are no masking functions anywhere in this build.** No `CREATE FUNCTION ... MASK`, no `ALTER TABLE ... SET MASK`. This was an explicit requirement. Governance is expressed as **tags**, which ABAC policies then consume.

### 11.2 Drop — the column never arrives

| Table | Dropped columns | Why |
|---|---|---|
| `SUBSCRIBER` | `ctn_password`, `sub_password` | `Drop(DF)=Y` — credentials, never needed downstream |
| All three | `gcp_insert_date`, `gcp_insert_user`, `gcp_update_date`, `gcp_update_user` | Legacy GCP audit columns, superseded by framework technical metadata |

> **Drop wins over Null.** `ctn_password` is flagged **both** `Drop=Y` and `Null=Y`. A column absent from the target cannot also be a nulled column, so Drop takes precedence.

**Verified:** `information_schema.columns` returns **0 rows** for `subscriber.ctn_password` and `subscriber.sub_password`.

### 11.3 Null — column present, always empty

| Table | Nulled columns |
|---|---|
| `PHYSICAL_DEVICE` | `esn_pin`, `blacklist_password` |
| `CUSTOMER` | `gur_cr_card_no`, `acc_password`, `imei_black_list_pass` |

**Configured as:**

```json
"data_standardization_sql": [
  "CAST(NULL AS STRING) AS acc_password",
  "CAST(NULL AS STRING) AS imei_black_list_pass",
  "CAST(NULL AS STRING) AS gur_cr_card_no"
]
```

**Verified:** `customer` has **100 of 100 rows NULL** for all three.

**Why keep the column at all?** Downstream code and BI tools break when a column vanishes. Keeping it NULL means the contract is stable while the data is gone.

### 11.4 Tag — labelled for policy

**Table tags** (all three tables):

| Tag | Value |
|---|---|
| `source_system` | `excalibur` |
| `use_case` | `uc3` |
| `domain` | `crm` |

**Column tags** — example, `customer_id`:

| Tag | Value | Meaning |
|---|---|---|
| `tokenise_pii` | `Y` | Must be tokenised in non-prod |
| `info_type` | `Customer ID` | Human-readable type |
| `data_class` | `CUSTOMER_ID` | Machine classification |
| `pdbt` | `Customer ID - Individual Externally Identifiable` | Personal-data classification |
| `sensitive` | `N` | Not in the "sensitive" tier |
| `csql_ro` | `Y` | Restricted in CloudSQL read-only |
| `csql_secured_ro` | `Y` | Restricted in the secured tier |
| `bq_deid_ro` | `Y-Hash` | **De-identified by hashing** in BigQuery read-only |

**Which columns get tagged.** A column is skipped **only when all five defaults hold**: `csql.ro=Y` **and** `csql.secured_ro=Y` **and** `bq.deid_ro=Y` (not `Y-Hash`) **and** `Sensitive=N` **and** `Tokenise=N`. If **any one** is non-default, the column is tagged. This matters — gating on `csql.ro` alone would have missed exactly the `Y-Hash` PII columns that matter most (`customer_id`, MSISDN, email).

**Verified applied tags — spec versus reality, exact match:**

| Table | Declared | Applied | Tag pairs | Table tags |
|---|---|---|---|---|
| `customer` | 33 | **33** | 231 | 3 |
| `physical_device` | 8 | **8** | 51 | 3 |
| `subscriber` | 18 | **18** | 133 | 3 |

> ### How to query tags correctly — the trap that cost this build a day
>
> **`information_schema` is CATALOG-SCOPED, not metastore-wide.** An unqualified query resolves against `current_catalog()`. The Serverless Starter Warehouse has **no default catalog**, so sessions land in `current_catalog() = workspace`, which genuinely holds no UC3 objects.
>
> | Query | Rows |
> |---|---|
> | `SELECT count(*) FROM information_schema.column_tags` *(unqualified)* | **0** — wrong catalog |
> | `SELECT count(*) FROM flowx.information_schema.column_tags` | **445** — correct |
> | `SELECT count(*) FROM system.information_schema.column_tags` | **445** — metastore-wide |
>
> **Always qualify with the catalog**, or use `system.information_schema` for a metastore-wide answer. Tagging works correctly on Lakeflow streaming tables — an earlier conclusion that it did not was a wrong-catalog reading error, not a platform limitation.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 5 ]</b><br/>
Catalog Explorer, <code>flowx.bronze.customer</code>, Columns tab, showing tag chips on <code>customer_id</code>.</div>

---

## 12. How NULLs Are Handled

NULL means three different things in this build. Confusing them leads to wrong conclusions.

| # | Kind of NULL | Cause | How to tell |
|---|---|---|---|
| 1 | **Governance NULL** | `Null(DF)=Y` forced it | Column carries tag `data_fabric_action = NULL_AT_SOURCE`. **100% of rows NULL.** |
| 2 | **Genuine source NULL** | Excalibur had no value | No governance tag. Some rows NULL, some not. |
| 3 | **Hash sentinel** | Inside hash computation only | Never stored. `coalesce(..., '__NULL__')` replaces NULL **only** while hashing. |

**Why the hash sentinel matters:** without it, `(customer_id='A', name=NULL)` and `(customer_id=NULL, name='A')` would concatenate identically and hash the same. The sentinel keeps distinct rows distinct.

**Verified:** all three `Null(DF)=Y` columns carry `data_fabric_action = NULL_AT_SOURCE`.

---

## 13. How Access Restriction Is Handled

### 13.1 The model — tag first, policy second

```
  Governance sheet (CSV)
        |
        |  onboarding
        v
  governance_tags in JSON spec
        |
        |  apply_governance task  (ALTER TABLE/COLUMN SET TAGS)
        v
  Unity Catalog tags on table and columns
        |
        |  ABAC policy  (references tags, not column names)
        v
  Row/column access enforced at query time, per user
```

**Why tags and not direct grants:**

| Direct grants | Tag-based ABAC |
|---|---|
| Must be re-applied per new table | Policy written once, applies to **every** tagged column |
| Break when a column is renamed | Tag travels with the column |
| No link to the source classification | Tag value **is** the sheet classification |
| Manual, error-prone | Applied automatically from the spec |

### 13.2 Which tags drive access

| Tag | Access meaning |
|---|---|
| `csql_ro` = `Y` | Restricted in the read-only tier |
| `csql_secured_ro` = `Y` | Restricted in the secured read-only tier |
| `bq_deid_ro` = `Y-Hash` | Must be de-identified by hashing |
| `tokenise_pii` = `Y` | Must be tokenised outside production |
| `sensitive` = `Y` | Highest sensitivity tier |

### 13.3 Layers of control

| Layer | Granularity | Mechanism |
|---|---|---|
| Catalog / schema | Coarse | `GRANT USE CATALOG`, `GRANT USE SCHEMA` |
| Table | Medium | `GRANT SELECT ON TABLE` |
| Column | Fine | **ABAC policy over tags** |
| Row | Fine | Row filter functions |
| Absolute | Total | **`Drop(DF)=Y`** — the column does not exist, so no policy can leak it |

> **The strongest control is the one that removes the data.** Tags and policies are enforcement; `Drop` is elimination. For credentials like `ctn_password`, elimination is the right answer.

---

## 14. CDC Explained With Real Data

### 14.1 What CDC means here

CDC means **Change Data Capture**. Instead of reloading the whole table, we apply only what changed: **inserts**, **updates**, **deletes**.

### 14.2 SCD1 — latest value wins (`physical_device`, `subscriber`)

**Scenario: a customer changes their handset.**

*Arrives in the stream:*

| customer_id | subscriber_no | equipment_no | phy_seq_no | model | sys_update_date | src_deleted_flg |
|---|---|---|---|---|---|---|
| C001 | 100001 | E500 | 1 | iPhone 13 | 2026-08-01 09:00:00 | 0 |
| C001 | 100001 | E500 | 1 | **iPhone 15** | **2026-08-03 14:22:00** | 0 |

*Result in `flowx.bronze.physical_device`:* **one row only.**

| customer_id | subscriber_no | equipment_no | phy_seq_no | model | sys_update_date |
|---|---|---|---|---|---|
| C001 | 100001 | E500 | 1 | **iPhone 15** | 2026-08-03 14:22:00 |

**Why:** `cdc_load_strategy: SCD1` keeps only the latest. `sequence_by_column: sys_update_date` decides which is latest — **not arrival order**. If the 09:00 row arrived *after* the 14:22 row, the 14:22 row still wins.

### 14.3 SCD1 delete

**Scenario: device removed from the account.**

| customer_id | ... | sys_update_date | **src_deleted_flg** |
|---|---|---|---|
| C007 | ... | 2026-08-04 11:00:00 | **1** |

*Result:* the row is **removed** from Bronze.

**Configured by:**

```json
"cdc_operation_column": "src_deleted_flg",
"cdc_operation_mapping": { "delete_values": ["1"] }
```

**Verified end to end:**

| Table | Source rows | Deletes | Bronze rows | Check |
|---|---|---|---|---|
| `physical_device` | 100 | 11 | **89** | 100 minus 11 equals 89 |
| `subscriber` | 100 | 6 | **94** | 100 minus 6 equals 94 |
| `customer` (SCD2) | 100 | 6 | **100** | history retained |

### 14.4 SCD2 — keep the history (`customer`)

**Scenario: a customer moves house.**

*Arrives:*

| customer_id | address | sys_update_date |
|---|---|---|
| C001 | 10 Old Street | 2026-08-01 09:00:00 |
| C001 | **25 New Road** | 2026-08-03 14:22:00 |

*Result in `flowx.bronze.customer`:* **two rows**, one closed and one current.

| customer_id | address | `__START_AT` | `__END_AT` | Meaning |
|---|---|---|---|---|
| C001 | 10 Old Street | 2026-08-01 09:00:00 | 2026-08-03 14:22:00 | Historic |
| C001 | 25 New Road | 2026-08-03 14:22:00 | **NULL** | **Current** |

> **`__END_AT IS NULL` means "this is the current version".** That is the single most useful filter on an SCD2 table.

**Note on SCD2 deletes:** a delete **closes** the row (sets `__END_AT`) rather than removing it. History is preserved — which is exactly why `customer` still shows 100 rows despite 6 deletes.

### 14.5 The reconciliation CDC case — the scenario that matters most

**The situation:** a record exists in the source and was loaded to Bronze via streaming. Later, the **batch file carries different values** for that same key — values the stream never captured, because of a missed message, a late correction, or a network drop.

**Step 1 — the streaming row already in Bronze:**

| customer_id | contact_telno | sys_update_date | source |
|---|---|---|---|
| C042 | 07700 900111 | 2026-08-01 10:00:00 | stream |

**Step 2 — the batch file for `2026-08-03` carries different values:**

| customer_id | contact_telno | sys_update_date | batch_date |
|---|---|---|---|
| C042 | **07700 900999** | **2026-08-03 16:45:00** | 2026-08-03 |

**Step 3 — reconciliation compares them.**

- `match_keys: ["customer_id"]` — same key, so the rows **pair up**.
- `compare_columns` (87 columns) — `contact_telno` differs.
- Both hashes computed over the same 87 columns, so **hashes differ**.
- Classification: **`VALUE_DRIFT`**, so `value_drift_count` increments.

**Step 4 — self-healing.** `append_target_table: flowx.staging.customer_stream` appends the batch row **back into the streaming lane**.

**Step 5 — Job 2 CDC engine applies it.** On the next update, that row is treated as a normal streaming change: sequenced by `sys_update_date`, and since `2026-08-03 16:45` is later than `2026-08-01 10:00`, it wins. On SCD2 it creates a **new version**; on SCD1 it **overwrites**.

> **Why heal through the stream lane rather than writing to Bronze directly?** Writing directly would bypass the CDC engine — no sequencing, no SCD2 versioning, no delete handling. Routing the repair through the same lane means **there is exactly one implementation of "how a change is applied"**. The healed row is indistinguishable from one that arrived on time.

**Verified reconciliation metrics:**

| Flow | src | tgt | matched | missing_in_target | missing_in_source | drift |
|---|---|---|---|---|---|---|
| `rf_uc3_customer_batch_vs_bronze` | 120 | 100 | 26 | 70 | 52 | 22 |
| `rf_uc3_physical_device_batch_vs_bronze` | 120 | 89 | 1 | 103 | 40 | 48 |
| `rf_uc3_subscriber_batch_vs_bronze` | 120 | 94 | 0 | 101 | 44 | 50 |

**Arithmetic check (customer):** `matched 26 + drift 22 = 48`, which is the number of **distinct overlapping keys**, verified independently. And `96 distinct batch keys - 48 = 48` batch-only keys. The identity holds exactly.

> ### Two results that look wrong but are correct
>
> **1. Metrics are counted PER KEY, not per row.** The batch sets hold 120 rows but only 96 / 101 / 104 **distinct PKs** — the same key recurs across `batch_date` values by design. Every row sharing a `__framework_hash_key` collapses to one outcome, where `MATCHED` beats `VALUE_DRIFT` beats `MISSING_*`. A by-hand *row-pair* count gave 32 for customer versus the framework 26 *keys* — **both correct, counting different things.** Never compare a row-level count against these key-level metrics.
>
> **2. Low `matched_count` is a test-data property, not a framework fault.** The generator mutation model redraws **every** non-key column on a mutated row. A pair therefore matches only if **all** compare columns coincide, and the odds collapse as column count grows:
>
> | Table | Compare columns | Matched |
> |---|---|---|
> | `physical_device` | 25 | 1 |
> | `customer` | 87 | 26 |
> | `subscriber` | 127 | 0 |
>
> Confirmed by column-level diff on subscriber: **no column differs across all 69 pairs, and not one of the 127 columns is identical across all of them.** The reconciliation is reporting the data accurately. To make this visibly non-zero in a demo, change the **generator** to mutate a small subset of columns — not the framework.

---

## 15. Timestamp Traceability — Simulated to Bronze

Four distinct timestamps exist. Knowing which is which is essential for any latency question.

| # | Timestamp | Set by | Meaning |
|---|---|---|---|
| 1 | `sys_creation_date` | Generator | When the record was *notionally* created in Excalibur. Range: **2024-01-01 to 2026-05-31**. |
| 2 | `sys_update_date` | Generator | When the record was *notionally* last changed. Up to **2026-08-04**. **This is the CDC sequencer.** |
| 3 | *(Job 1 append time)* | Simulator tick | Wall-clock time the row entered `staging.<table>_stream`. |
| 4 | `__framework_ingestion_timestamp_utc` | **Framework** | **Wall-clock UTC time the row was processed into Bronze.** |

### 15.1 The end-to-end trace

```
 sys_update_date            Job 1 tick             __framework_ingestion_timestamp_utc
 (simulated business time)  (row hits staging)     (row lands in Bronze)
 2026-08-03 14:22:00   -->  actual wall clock  --> actual wall clock
       |                          |                          |
       +---- business time -------+------- processing time --+
```

**The critical distinction:**

- **`sys_update_date` is business time.** It can be older than the ingestion time by months. It decides **which version wins**.
- **`__framework_ingestion_timestamp_utc` is processing time.** It only ever moves forward. It tells you **when we learned about it**.

> **Why CDC must sequence on business time, not processing time.** If a network hiccup delays a message, it arrives *later* but describes an *earlier* change. Sequencing on arrival would let a stale value overwrite a fresh one. `sequence_by_column: sys_update_date` makes the pipeline immune to out-of-order arrival.

### 15.2 Measuring end-to-end latency

Simulator ticks were **20 seconds** apart, and a full drain took **220.7 seconds** for 7 ticks. Section 16 gives the SQL to measure the staging-to-Bronze leg directly.

---
## 16. Testing and Validation SQL (For Business Users)

**Every query below is copy-paste ready.** Set the catalog first — this is not optional.

```sql
-- ALWAYS run this first. Without it, information_schema queries read the WRONG catalog
-- and return 0 rows while telling you nothing is wrong.
USE CATALOG flowx;
```

### T1 — Row counts across all three layers

```sql
SELECT 'staging_stream' AS layer, 'customer' AS tbl, count(*) AS row_count FROM flowx.staging.customer_stream
UNION ALL SELECT 'staging_batch', 'customer', count(*) FROM flowx.staging.customer_batch
UNION ALL SELECT 'bronze',        'customer', count(*) FROM flowx.bronze.customer
UNION ALL SELECT 'staging_stream','physical_device', count(*) FROM flowx.staging.physical_device_stream
UNION ALL SELECT 'staging_batch', 'physical_device', count(*) FROM flowx.staging.physical_device_batch
UNION ALL SELECT 'bronze',        'physical_device', count(*) FROM flowx.bronze.physical_device
UNION ALL SELECT 'staging_stream','subscriber', count(*) FROM flowx.staging.subscriber_stream
UNION ALL SELECT 'staging_batch', 'subscriber', count(*) FROM flowx.staging.subscriber_batch
UNION ALL SELECT 'bronze',        'subscriber', count(*) FROM flowx.bronze.subscriber
ORDER BY tbl, layer;
```

**Expected:** stream 100, batch 120, bronze 89 / 100 / 94.

### T2 — Prove SCD1 deletes were applied

```sql
SELECT
  (SELECT count(*) FROM flowx.staging.physical_device_stream)                             AS source_rows,
  (SELECT count(*) FROM flowx.staging.physical_device_stream WHERE src_deleted_flg = '1') AS deletes,
  (SELECT count(*) FROM flowx.bronze.physical_device)                                     AS bronze_rows,
  (SELECT count(*) FROM flowx.staging.physical_device_stream)
    - (SELECT count(*) FROM flowx.staging.physical_device_stream WHERE src_deleted_flg = '1')
                                                                                          AS expected_bronze;
```

**Expected:** `bronze_rows` equals `expected_bronze` equals 89.

### T3 — Prove SCD1 keeps exactly one row per key

```sql
SELECT count(*) AS total_rows,
       count(DISTINCT __framework_hash_key) AS distinct_keys,
       CASE WHEN count(*) = count(DISTINCT __framework_hash_key)
            THEN 'PASS - one row per key' ELSE 'FAIL - duplicates present' END AS verdict
FROM flowx.bronze.physical_device;
```

### T4 — See SCD2 history on a customer

```sql
-- Customers with more than one version
SELECT customer_id, count(*) AS versions
FROM flowx.bronze.customer
GROUP BY customer_id
HAVING count(*) > 1
ORDER BY versions DESC
LIMIT 10;
```

```sql
-- Full history for one customer. __END_AT IS NULL means the current version.
SELECT customer_id, __START_AT, __END_AT,
       CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORIC' END AS version_status,
       sys_update_date
FROM flowx.bronze.customer
WHERE customer_id = (
        SELECT customer_id FROM flowx.bronze.customer
        GROUP BY customer_id HAVING count(*) > 1 LIMIT 1)
ORDER BY __START_AT;
```

### T5 — Current-state-only view of an SCD2 table

```sql
-- This is how a business user should normally query customer.
SELECT * FROM flowx.bronze.customer WHERE __END_AT IS NULL;
```

### T6 — Prove `Null(DF)=Y` columns are always NULL

```sql
SELECT count(*) AS total_rows,
       count(acc_password)         AS non_null_acc_password,
       count(imei_black_list_pass) AS non_null_imei_pass,
       count(gur_cr_card_no)       AS non_null_card_no,
       CASE WHEN count(acc_password) + count(imei_black_list_pass) + count(gur_cr_card_no) = 0
            THEN 'PASS - all governance-nulled' ELSE 'FAIL - data leaked' END AS verdict
FROM flowx.bronze.customer;
```

### T7 — Prove `Drop(DF)=Y` columns do not exist

```sql
SELECT count(*) AS should_be_zero,
       CASE WHEN count(*) = 0 THEN 'PASS - dropped columns absent'
            ELSE 'FAIL - dropped column present' END AS verdict
FROM flowx.information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name = 'subscriber'
  AND column_name IN ('ctn_password', 'sub_password');
```

### T8 — See all governance tags

```sql
-- NOTE the flowx. prefix. Without it this returns 0 rows and tells you nothing.
SELECT table_name, column_name, tag_name, tag_value
FROM flowx.information_schema.column_tags
WHERE schema_name = 'bronze'
  AND table_name IN ('customer','physical_device','subscriber')
ORDER BY table_name, column_name, tag_name;
```

```sql
-- Summary: tagged columns per table. Expect 33 / 8 / 18.
SELECT table_name,
       count(DISTINCT column_name) AS tagged_columns,
       count(*)                    AS tag_pairs
FROM flowx.information_schema.column_tags
WHERE schema_name = 'bronze'
  AND table_name IN ('customer','physical_device','subscriber')
GROUP BY table_name
ORDER BY table_name;
```

### T9 — Find all PII columns needing de-identification

```sql
SELECT table_name, column_name, tag_value AS deid_rule
FROM flowx.information_schema.column_tags
WHERE schema_name = 'bronze'
  AND tag_name  = 'bq_deid_ro'
  AND tag_value = 'Y-Hash'
ORDER BY table_name, column_name;
```

### T10 — Reconciliation metrics (the audit answer)

```sql
-- Replace <control_schema> with your deployment control schema.
SELECT reconciliation_id, target_id, status,
       source_record_count, target_record_count,
       matched_count, missing_in_target_count, missing_in_source_count,
       value_drift_count, appended_count, run_at
FROM flowx.<control_schema>.reconciliation_run_log
WHERE reconciliation_id LIKE 'rf_uc3%'
QUALIFY row_number() OVER (PARTITION BY reconciliation_id, target_id ORDER BY run_at DESC) = 1
ORDER BY reconciliation_id;
```

### T11 — Verify the reconciliation arithmetic

```sql
WITH batch_keys AS (
  SELECT DISTINCT customer_id FROM flowx.staging.customer_batch
),
bronze_keys AS (
  SELECT DISTINCT customer_id FROM flowx.bronze.customer
)
SELECT
  (SELECT count(*) FROM batch_keys)  AS distinct_batch_keys,
  (SELECT count(*) FROM bronze_keys) AS distinct_bronze_keys,
  (SELECT count(*) FROM batch_keys b JOIN bronze_keys z USING (customer_id)) AS overlapping_keys,
  'overlapping_keys should equal matched_count + value_drift_count' AS note;
```

**Expected:** `overlapping_keys` is 48, and from T10 `matched 26 + drift 22 = 48`.

### T12 — Batch rows per `batch_date`

```sql
SELECT batch_date, count(*) AS row_count
FROM flowx.staging.customer_batch
GROUP BY batch_date
ORDER BY batch_date;
```

**Expected:** 4 dates, 30 rows each, 120 total.

### T13 — Verify the hash by hand

```sql
-- Recompute __framework_hash_key from the PK and compare with what the framework stored.
SELECT count(*) AS total_rows,
       sum(CASE WHEN __framework_hash_key =
             sha2(concat_ws('||',
               coalesce(trim(lower(cast(customer_id AS STRING))), '__NULL__')), 256)
           THEN 1 ELSE 0 END) AS matching_hashes,
       CASE WHEN count(*) = sum(CASE WHEN __framework_hash_key =
             sha2(concat_ws('||',
               coalesce(trim(lower(cast(customer_id AS STRING))), '__NULL__')), 256)
           THEN 1 ELSE 0 END)
       THEN 'PASS - hash reproducible' ELSE 'FAIL' END AS verdict
FROM flowx.bronze.customer;
```

**Expected:** `PASS`. Verified at 100 of 100 rows.

### T14 — Staging to Bronze latency

```sql
SELECT
  min(__framework_ingestion_timestamp_utc) AS first_row_landed,
  max(__framework_ingestion_timestamp_utc) AS last_row_landed,
  timestampdiff(SECOND, min(__framework_ingestion_timestamp_utc),
                        max(__framework_ingestion_timestamp_utc)) AS ingestion_span_seconds,
  count(*) AS row_count
FROM flowx.bronze.customer;
```

### T15 — Business time versus processing time

```sql
SELECT customer_id,
       sys_update_date                     AS business_time,
       __framework_ingestion_timestamp_utc AS processing_time,
       timestampdiff(DAY, sys_update_date,
                     __framework_ingestion_timestamp_utc) AS lag_days
FROM flowx.bronze.customer
WHERE __END_AT IS NULL
ORDER BY lag_days DESC
LIMIT 20;
```

**Reading it:** a large `lag_days` is **normal** — the generated business dates are historic. It confirms the two clocks are genuinely independent.

### T16 — Full data-quality scorecard (run this one for a demo)

```sql
USE CATALOG flowx;

SELECT 'Bronze row count matches source minus deletes' AS check_name,
       CASE WHEN (SELECT count(*) FROM flowx.bronze.physical_device) =
                 (SELECT count(*) FROM flowx.staging.physical_device_stream)
               - (SELECT count(*) FROM flowx.staging.physical_device_stream WHERE src_deleted_flg='1')
            THEN 'PASS' ELSE 'FAIL' END AS result
UNION ALL
SELECT 'SCD1: one row per key (physical_device)',
       CASE WHEN (SELECT count(*) FROM flowx.bronze.physical_device) =
                 (SELECT count(DISTINCT __framework_hash_key) FROM flowx.bronze.physical_device)
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'SCD2: customer carries history columns',
       CASE WHEN (SELECT count(*) FROM flowx.information_schema.columns
                  WHERE table_schema='bronze' AND table_name='customer'
                    AND column_name IN ('__START_AT','__END_AT')) = 2
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Governance-nulled columns are 100 percent NULL',
       CASE WHEN (SELECT count(acc_password)+count(imei_black_list_pass)+count(gur_cr_card_no)
                  FROM flowx.bronze.customer) = 0
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Dropped columns absent from schema',
       CASE WHEN (SELECT count(*) FROM flowx.information_schema.columns
                  WHERE table_schema='bronze' AND table_name='subscriber'
                    AND column_name IN ('ctn_password','sub_password')) = 0
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Governance tags applied (expect 59 tagged columns)',
       CASE WHEN (SELECT count(DISTINCT concat(table_name,'.',column_name))
                  FROM flowx.information_schema.column_tags
                  WHERE schema_name='bronze'
                    AND table_name IN ('customer','physical_device','subscriber')) = 59
            THEN 'PASS' ELSE 'FAIL' END;
```

**Expected:** six `PASS` rows. The 59 is 33 plus 8 plus 18.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 6 ]</b><br/>
SQL Editor showing the T16 scorecard returning six PASS rows.</div>

---

## 17. Event Log Proof of Counts

The pipeline own **event log** is the independent authority on how many rows were written. It is not something we compute — it is what Lakeflow recorded.

**Access via the `event_log()` table-valued function.** Find the pipeline id first: Workflows, then Pipelines, then `004_ldp_uc3_excalibur_streaming_cdc`. The id is in the URL and on the pipeline detail page.

### E1 — Rows written per dataset, per update

```sql
SELECT
  origin.update_id,
  origin.dataset_name,
  origin.flow_name,
  get_json_object(details, '$.flow_progress.metrics.num_output_rows') AS rows_written,
  timestamp
FROM event_log('<PIPELINE_ID>')
WHERE event_type = 'flow_progress'
  AND get_json_object(details, '$.flow_progress.metrics.num_output_rows') IS NOT NULL
ORDER BY timestamp DESC;
```

### E2 — Total rows written per table (proves the Bronze counts)

```sql
SELECT
  origin.dataset_name,
  sum(cast(get_json_object(details, '$.flow_progress.metrics.num_output_rows') AS BIGINT))
      AS total_rows_written
FROM event_log('<PIPELINE_ID>')
WHERE event_type = 'flow_progress'
  AND get_json_object(details, '$.flow_progress.metrics.num_output_rows') IS NOT NULL
GROUP BY origin.dataset_name
ORDER BY origin.dataset_name;
```

**Cross-check:** compare against T1. The event log written count and the table row count must reconcile — for SCD1, written rows include updates that did not increase the count.

### E3 — Confirm the update completed successfully

```sql
SELECT timestamp, event_type, message,
       get_json_object(details, '$.update_progress.state') AS update_state
FROM event_log('<PIPELINE_ID>')
WHERE event_type = 'update_progress'
ORDER BY timestamp DESC
LIMIT 20;
```

**Look for:** `state = COMPLETED`.

### E4 — Any errors or warnings

```sql
SELECT timestamp, level, event_type, message, error
FROM event_log('<PIPELINE_ID>')
WHERE level IN ('ERROR','WARN')
ORDER BY timestamp DESC
LIMIT 50;
```

### E5 — Prove the streaming path ran as multiple micro-batches

```sql
-- Job 1 produced 7 ticks. A genuine stream shows MULTIPLE flow_progress events per dataset,
-- not one. This is what distinguishes streaming CDC from a disguised bulk load.
SELECT origin.dataset_name,
       count(*)       AS micro_batches,
       min(timestamp) AS first_batch,
       max(timestamp) AS last_batch
FROM event_log('<PIPELINE_ID>')
WHERE event_type = 'flow_progress'
  AND get_json_object(details, '$.flow_progress.metrics.num_output_rows') IS NOT NULL
GROUP BY origin.dataset_name
ORDER BY origin.dataset_name;
```

**Expected:** `micro_batches` greater than 1 per dataset.

### E6 — Data-quality expectation results

```sql
SELECT timestamp, origin.dataset_name,
       get_json_object(details, '$.flow_progress.data_quality.expectations') AS expectations
FROM event_log('<PIPELINE_ID>')
WHERE event_type = 'flow_progress'
  AND get_json_object(details, '$.flow_progress.data_quality.expectations') IS NOT NULL
ORDER BY timestamp DESC;
```

> **A practical note on querying the event log.** The `origin` field is a roughly 30-field nested struct, and `SELECT *` can fail with an Arrow schema mismatch (`Schema at index 1 was different`). **Always project the specific columns you need**, as every query above does.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 7 ]</b><br/>
Event log query results showing per-dataset <code>num_output_rows</code> reconciling with the Bronze table counts.</div>

---

## 18. How Databricks Simplifies the Process

### 18.1 The comparison

| Task | Traditional way | Databricks plus FlowX | Saving |
|---|---|---|---|
| **CDC merge** | Hand-written MERGE per table, per strategy | `"cdc_load_strategy": "SCD1"` | About 200 lines becomes **1 line** |
| **SCD2 history** | Hand-managed valid_from/valid_to, close-and-insert logic | `"cdc_load_strategy": "SCD2"` | About 300 lines becomes **1 line** |
| **Deletes** | Custom soft/hard delete branch | `cdc_operation_column` plus `delete_values` | About 50 lines becomes **2 lines** |
| **Hashing** | Loop over columns, normalise, concat, SHA-256 | `"generate_hash_columns": true` | About 80 lines becomes **1 line** |
| **Null enforcement** | Per-column transform, easy to miss one | `data_standardization_sql` | Explicit and auditable |
| **Tagging** | `ALTER TABLE ... SET TAGS` loop | `governance_tags` block | About 100 lines becomes **config** |
| **Reconciliation** | Bespoke compare job, then a repair script | `reconciliation_flows` | About 500 lines becomes **config** |
| **Self-healing** | Manual re-run after investigation | `append_target_table` | Manual becomes **automatic** |
| **Schema evolution** | Job fails, engineer patches | Auto Loader schema inference | Outage becomes **handled** |
| **Orchestration** | Cron plus custom dependency scripts | DABs plus Lakeflow `depends_on` | Fragile becomes **declarative** |
| **Lineage** | Documented manually, goes stale | Automatic from the DAG | Stale becomes **live** |
| **Observability** | Custom logging | Event log plus observability export | Bespoke becomes **built in** |

### 18.2 The structural advantages

1. **One implementation of every rule.** There is exactly one CDC engine, one hash function, one tagging path. A bug is fixed once. This build proved the point in the opposite direction too — five genuine framework defects were found by UC3 and fixed **for every use case at once**.

2. **The spec is the documentation.** The JSON is not a description of what the pipeline does — it *is* what the pipeline does. It cannot go stale.

3. **Governance is derived, not transcribed.** The tags come from the same CSV the governance team maintains. Nobody retypes them.

4. **Unknown attributes are rejected, not ignored.** A misspelt `data_quality` instead of `dq_config` is a **hard error**. Previously it silently onboarded and data quality never ran, with nothing anywhere to say so.

5. **Reconciliation is a first-class citizen.** Most platforms treat "is the data right?" as an afterthought. Here it is a declared flow with its own DAG nodes and audit table.

6. **Serverless.** No cluster sizing, no idle cost.

### 18.3 Honest limitations

Worth stating plainly:

- **IcebergCompatV3 and Change Data Feed are mutually exclusive.** Enabling Iceberg read on a streaming table suppresses CDF.
- **A `--full-refresh` is needed after certain config changes.** Changing `hash_precomputed` or a technical-metadata column type will not take effect otherwise — the pipeline schema state persists across table drops.
- **Liquid clustering is capped at 3 columns.** A 4-column PK must cluster on the hash key instead.
- **Never deploy while a pipeline is running.** `bundle deploy` prunes superseded artifacts and kills the in-flight update with `ENVIRONMENT_PIP_INSTALL_ERROR`.

---

## 19. Databricks Reference Links

### Core platform

- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
- [Streaming tables](https://docs.databricks.com/aws/en/dlt/streaming-tables)
- [Materialized views](https://docs.databricks.com/aws/en/dlt/materialized-views)
- [Pipeline settings and configuration](https://docs.databricks.com/aws/en/dlt/settings)

### CDC and SCD

- [AUTO CDC / APPLY CHANGES INTO](https://docs.databricks.com/aws/en/dlt/cdc)
- [Change Data Feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed)

### Ingestion

- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Auto Loader schema inference and evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
- [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)

### Unity Catalog and governance

- [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
- [Tags on tables and columns](https://docs.databricks.com/aws/en/database-objects/tags)
- [ABAC, attribute-based access control](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)
- [Column masks and row filters](https://docs.databricks.com/aws/en/tables/row-and-column-filters)
- [information_schema](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema)
- [Volumes](https://docs.databricks.com/aws/en/volumes/)

### Delta and Iceberg

- [Delta Lake](https://docs.databricks.com/aws/en/delta/)
- [Liquid clustering](https://docs.databricks.com/aws/en/delta/clustering)
- [Delta UniForm (Iceberg reads)](https://docs.databricks.com/aws/en/delta/uniform)
- [Delta table properties](https://docs.databricks.com/aws/en/delta/table-properties)

### Observability

- [Pipeline event log](https://docs.databricks.com/aws/en/dlt/observability)
- [System tables](https://docs.databricks.com/aws/en/admin/system-tables/)

### Orchestration and deployment

- [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Serverless compute](https://docs.databricks.com/aws/en/compute/serverless/)

### SQL functions used here

- [sha2](https://docs.databricks.com/aws/en/sql/language-manual/functions/sha2)
- [concat_ws](https://docs.databricks.com/aws/en/sql/language-manual/functions/concat_ws)
- [get_json_object](https://docs.databricks.com/aws/en/sql/language-manual/functions/get_json_object)
- [QUALIFY](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-qualify)

---

## Appendix A — Framework Defects Found by This Build

UC3 surfaced **five genuine framework defects**, all fixed. They are recorded here because each was found only by running real data end to end.

| # | Defect | Impact | Fix |
|---|---|---|---|
| R5 | Iceberg property trio invalid on streaming tables | Pipeline failed on all three tables | V2/V3 split by `target_type` |
| R8 | `__framework_source_file_size` had a **non-deterministic type** | Graph-analysis failure killing the **whole pipeline** | Typed fallback pinning both branches |
| R9 | Tagging orchestration misfiled outside the governance module | Confusing ownership | Moved into `governance/tags.py` |
| R10 | `${param}` ignored in the source plane | `IllegalArgumentException: Path must be absolute: ${landing_root}/subscriber` | Substitution applied in `source_plane.py` |
| — | Iceberg V2 versus V3 target-type split | V2 cannot work on pipeline-managed tables | `target_type` threaded through |

> **R8 is an estate-wide breaking change.** Every existing table already carrying `__framework_source_file_size` or `__framework_source_file_modification_time` was materialised with the old `string` typing. Any pipeline in any workspace with those columns will hit the same merge conflict on its next update and needs a **full refresh**. **Dropping the table is not enough**, because the conflicting schema lives in the pipeline own state.

---

## Appendix B — Operational Runbook

### Running from scratch

```bash
# 1. Generate test data
python scripts/generate_uc3_test_data.py --upload --catalog flowx --staging-schema staging

# 2. Deploy. NEVER do this while a pipeline is running.
databricks bundle deploy -t metaflow_v7 -p metaflow_v7

# 3. Run IN ORDER. Never in parallel.
databricks bundle run uc3_streaming_simulator_job -t metaflow_v7 -p metaflow_v7
databricks bundle run uc3_streaming_cdc_job       -t metaflow_v7 -p metaflow_v7
databricks bundle run uc3_batch_recon_job         -t metaflow_v7 -p metaflow_v7
```

### Validating a spec before deploying

```bash
python -c "
import json,sys; sys.path.insert(0,'src')
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json
r = validate_json(open('onboarding/uc3/uc3_excalibur_streaming_cdc.json', encoding='utf-8').read())
print(r['summary'])
[print(' ERROR:', e) for e in r['errors']]
"
```

**Never deploy a spec that has not returned `valid = True`.**

### Failure playbook

Every entry below is a failure this build actually hit.

| Symptom | Cause | Fix |
|---|---|---|
| `Path must be absolute: ${landing_root}/...` | `${param}` not substituted | Confirm `pipeline_parameters` declares it |
| `DELTA_FAILED_TO_MERGE_FIELDS` | Schema conflict held in **pipeline state** | `--full-refresh`. Dropping the table will **not** work. |
| `value_drift_count` always 0 | `compare_columns` missing | Declare it |
| `matched_count` is 0 | `hash_precomputed: true` mismatch | Set `false` on both sides, then full refresh |
| Tag query returns 0 rows | Wrong catalog | Qualify it: `flowx.information_schema....` |
| `ENVIRONMENT_PIP_INSTALL_ERROR` | Deployed mid-update | Never deploy while a pipeline runs |
| `NotebookImportException` | Python module placed under `notebooks/` | Move it to `src/` |
| `PERSIST TABLE is not supported` | `.cache()` on serverless | Remove it |
| `no such directory` on volume copy | Intermediate dirs missing | Create the directory tree first |

---

*Every figure in this document was read back from the live `metaflow_v7` workspace after a successful run. Where a result looks surprising, the reason is stated rather than smoothed over.*
