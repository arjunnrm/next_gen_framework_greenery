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
| 3a | [The real streaming source](#3a-the-real-streaming-source-debezium-cdc-v004-supersedes-job-1-for-the-stream-lane) | The Debezium CDC feed, and the traps in parsing it |
| 3b | [The real batch source](#3b-the-real-batch-source-lakeflow-connect-and-why-its-flows-are-transformation-flows-v007) | Lakeflow Connect, and why Job 3's flows are *transformation* flows, not ingestion flows |
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
        +---------------------+---------------------+
        |                                           |
   Debezium CDC feed                   Lakeflow Connect Oracle
        |                              query-based connector
        v                                           v
  +-------------------------------------------------------------------+
  |  STREAMING PATH                      BATCH PATH                   |
  |  (near real-time)                    (scheduled snapshot)         |
  |                                                                   |
  |  <cat>.staging.oracle_excalibur_cdc   <cat>.oracle_excalibur_batch.<t>  |
  |         |                                      |                  |
  |    [ Job 2 : 004 ]                    [ Job 3 : 005 / 006 ]       |
  |    streaming CDC                      transformation flow         |
  |         |                             (batch table read)          |
  |         v                                      v                  |
  |   <cat>.bronze.<t>                    <cat>.staging.<t>_batch     |
  |    (SCD1 / SCD2)                       (materialized view,        |
  |         ^                               TRUNCATE_AND_LOAD)        |
  |         |                                      |                  |
  |         |           reconciliation             |                  |
  |         |   (pipeline_audit_only: compare in   |                  |
  |         |    the pipeline, heal in job tasks)  |                  |
  |         |                                      |                  |
  |         |    heal: Debezium envelope rows      |                  |
  |         +--<-- <cat>.staging.oracle_excalibur_cdc --<-------------+
  +-------------------------------------------------------------------+
```

**Reading it in words:**

1. **Job 1** pretended to be Excalibur, dripping rows into staging tables on a timer.
   *(Superseded for the streaming lane in v0.0.4 and for the batch lane in v0.0.7. Both lanes now
   read real connector output: the stream reads the Debezium CDC feed in
   `<catalog>.staging.oracle_excalibur_cdc` (§3a), and the batch lane reads the three tables the
   Lakeflow Connect Oracle query-based connector writes into `<catalog>.oracle_excalibur_batch`.
   Job 1 and its generated CSVs are now historical.)*
2. **Job 2** reads the CDC landing table as a **stream** and applies CDC into governed Bronze tables.
3. **Job 3** reads the three connector snapshot tables as **batch table inputs**, recomputes
   `<catalog>.staging.<table>_batch` as a full snapshot, then **compares** batch against Bronze.
4. Where the batch disagrees with Bronze, Job 3 **feeds the miss set back** into
   `<catalog>.staging.oracle_excalibur_cdc` as a complete Debezium envelope row, and Job 2's CDC
   engine applies it as an ordinary change event.

**The key idea:** the batch path is not a second copy of the data. It is an **audit and repair mechanism** for the streaming path.

---

## 3a. The real streaming source — Debezium CDC (v0.0.4, supersedes Job 1 for the stream lane)

Everything above describes the **simulated** stream: Job 1 dripping rows into three
pre-split, pre-flattened staging tables (`staging.physical_device_stream`,
`customer_stream`, `subscriber_stream`), which Job 2 then read one-for-one. The batch lane
and the reconciliation description are unchanged and still accurate.

The streaming lane now reads the **real** Excalibur feed. Debezium Server captures the Oracle
redo log and writes change events, through the Zerobus gRPC sink, into a **single multiplexed
landing table**:

```
  Excalibur (Oracle)  --redo log-->  Debezium Server  --gRPC-->  Zerobus sink
                                                                      |
                                                                      v
                              <catalog>.staging.oracle_excalibur_cdc      (ONE table)
                              all three Excalibur tables interleaved,
                              Debezium envelope as JSON in `value`
                                                                      |
                            +-----------------+--------------------+--+
                            |                 |                    |
                     filter CUSTOMER   filter PHYSICAL_DEVICE  filter SUBSCRIBER
                            |                 |                    |
                            v                 v                    v
                  bronze.customer   bronze.physical_device   bronze.subscriber
                      (SCD2)              (SCD1)                 (SCD1)
```

### 3a.1 What the landing table looks like

Nine columns, of which four matter to the pipeline:

| Column | Type | What it carries |
|---|---|---|
| `destination` | STRING | The Debezium topic, e.g. `oracdc-excalibur.EXCALIBUR.CUSTOMER`. **This is the column each flow filters on** — it is what identifies which Oracle table a row belongs to. |
| `value` | STRING | The full Debezium envelope as a **JSON string**: `{schema, payload{before, after, source, op, ts_ms}}`. |
| `key` | STRING | Debezium key JSON, carrying the primary key. Not read by the pipeline (the keys come out of the payload). |
| `source_position` | STRING | The Oracle SCN. The landing table's own comment says *"deduplicate on this"*. |
| `operation` | STRING | `read` / `create` / `update` / `delete` / `change` — the sink's own label, mirroring `payload.op` (`r`/`c`/`u`/`d`). |

A fourth topic, `oracdc-excalibur` with no table suffix, carries Debezium
**heartbeat/schema-change** events. It has no `payload.op` and no row image, and is excluded
by both DQ rules on every flow.

### 3a.2 One read, three targets — and why that is not a rule violation

All three ingestion flows declare the **same** `source_catalog` / `source_schema` /
`source_table`. That is deliberate and is what satisfies the Single-Read DAG mandate rather
than breaking it: `engine/source_plane.py` fingerprints only the **base-read** options, so the
three flows collapse to **one** `ReadIdentity` and therefore **one** physical
`spark.readStream` on the landing table, which all three consume through `bind()`. The
per-flow topic filter, envelope parse and projection are per-consumer *overlays* applied on
top of that shared DataFrame — they do not re-read the source.

The graph is:

```
  1 source-plane base node  (the single streaming read of oracle_excalibur_cdc)
        |
        +--> staged view: filter CUSTOMER topic         --> apply_changes SCD2 --> bronze.customer
        +--> staged view: filter PHYSICAL_DEVICE topic  --> apply_changes SCD1 --> bronze.physical_device
        +--> staged view: filter SUBSCRIBER topic       --> apply_changes SCD1 --> bronze.subscriber
```

`tests/unit/test_uc3_specs.py::test_single_read_identity_across_all_three_flows` asserts this
using the framework's own identity functions, so it cannot drift from the engine.

### 3a.3 How each flow splits its own rows out

`data_standardization_sql` is **projection-only** — one `withColumn` per entry — so it cannot
filter rows. The split is therefore a `dq_config` rule with `action: "drop"`, which the engine
turns into `dlt.expect_all_or_drop` on the staged view, *ahead* of `apply_changes`:

| Rule | Expression | Why |
|---|---|---|
| `only_<table>_topic` | `destination = 'oracdc-excalibur.EXCALIBUR.<TABLE>'` | Admits only this flow's Oracle table. Without it every Bronze table would receive all three tables' change events. |
| `cdc_op_recognised` | `__cdc_op IN ('r','c','u','d')` | Drops the heartbeat topic and any envelope with no operation. |

### 3a.4 Three traps in parsing the envelope — each one silent

Every one of these produced a spec that passed **both** validation gates and would still have
written a wrong Bronze table. All three were caught by querying the live table, and all three
are now pinned by unit tests.

| # | Trap | Symptom | Fix |
|---|---|---|---|
| 1 | **`from_json` field matching is case-sensitive.** Debezium emits Oracle's **UPPERCASE** column names. | A lowercase `schema_ddl` parsed **every** payload column to NULL — all 31 / 90 / 131 of them, primary keys included. Row counts and op codes looked perfect. | Declare the row-image fields **uppercase** in `schema_ddl` and reference them uppercase; lowercase only in the `AS` alias, which is what Bronze stores. |
| 2 | **Connect `Timestamp` is epoch MILLIseconds** in an int64. | Declaring the field as `TIMESTAMP` makes Spark read the number as **MICRO**seconds — a silent 1000x error that turned `2026-09-11` into year **+58664**. | Declare those fields `BIGINT` and convert with `timestamp_millis(...)` in the projection. |
| 3 | **A delete carries its row image in `before`**, not `after`. | Projecting only `payload.after.<col>` yields an all-NULL row for every `op='d'`, primary keys included, so `apply_as_deletes` could never match a target key. | Every payload column is `CASE WHEN payload.op = 'd' THEN payload.before.<col> ELSE payload.after.<col> END`. |

### 3a.5 Sequencing, dedup and the initial snapshot

| Concern | Decision |
|---|---|
| **Sequencer** | `sequence_by_column: __cdc_scn`, derived from `payload.source.scn` (the Oracle SCN). **Not** `sys_update_date`: two changes committed inside the same second share that timestamp, so `apply_changes` would order them arbitrarily. |
| **At-least-once delivery** | Handled by the sequencer, not by a dedup pass. `apply_changes` discards a replayed row whose sequence is equal or lower, which makes redelivery idempotent by construction. `source_config.remove_dups` is deliberately **not** set: it is *full-row* dedup with **unbounded** streaming state, and the sink's per-row `idempotency_key` would make every replay look unique to it anyway. |
| **Initial snapshot** | Debezium's snapshot rows arrive as `op='r'` and are **kept** as the Bronze initial load. `starting_version: 0` on the source ensures the stream starts from the landing table's first version rather than from "now". A snapshot row carries no `scn`, so `__cdc_scn` is `COALESCE(scn, '0')` — it sorts below every real change, which is exactly right for an initial load. |
| **Deletes** | `payload.op = 'd'` sets `src_deleted_flg = '1'`, which `target_config.cdc_operation_column` reads and maps to `apply_as_deletes`. On SCD1 the row is removed; on SCD2 the open version is closed (`__END_AT` set), preserving history. |

### 3a.6 Latency — the pipeline is continuous

`004_ldp_uc3_excalibur_streaming_cdc` is now **`continuous: true`**. UC3's requirement is
seconds-level latency from an Oracle commit to Bronze, and a triggered pipeline's latency is
its trigger interval (minutes at best) however often it is scheduled. Two consequences, both
deliberate:

1. **A continuous update never completes**, so the pipeline is **not** wrapped by a job that
   waits on it. Start it with
   `databricks bundle run uc3_streaming_cdc_pipeline -t <target>`; Databricks owns the
   lifecycle thereafter. This is the same pattern as
   `resources/observability/observability_otel_streaming_pipeline.yml`.
2. **Observability moved to `mode: "continuous"`.** The triggered engine hard-requires a
   `pipeline_task_run_id` resolved from an upstream pipeline task
   (`observability/runtime_params.py::REQUIRED_TRIGGERED_PARAMETERS`), and a standing pipeline
   has no such task. The continuous engine is told which event-log tables to stream instead,
   which is why the pipeline now publishes its event log to
   `<catalog>.observability.uc3_excalibur_streaming_cdc_event_log` and the spec names that
   table in `destination_config.event_log_tables`.

Serverless compute stays up for as long as a continuous pipeline runs, so this bills
continuously rather than per update. That is the cost of seconds-level latency.

> [!WARNING]
> **Never `bundle deploy` while the pipeline is running.** A deploy prunes superseded artifacts
> from `<artifact_path>/.internal/` and kills the live update with
> `ENVIRONMENT_PIP_INSTALL_ERROR`. With a *continuous* pipeline that window is always open,
> unlike a triggered one — stop the pipeline first (`flowx_testing/TESTING_PLAN.md` §0).

### 3a.7 Expected row counts on the current landing-table contents

Measured against the live table (196 rows, 4 topics) by replaying the exact spec projection,
so these are the verification baseline for the first run:

| Bronze table | Strategy | Change events | Distinct keys | Deletes | Expected rows |
|---|---|---|---|---|---|
| `bronze.physical_device` | SCD1 | 88 | 80 | 3 | **77** (live keys) |
| `bronze.customer` | SCD2 | 40 | 30 | 0 | **30** current versions (plus closed history) |
| `bronze.subscriber` | SCD1 | 63 | 55 | 0 | **55** |

A green `bundle run` does **not** prove the pipeline worked — check these counts, and check
the pipeline's own update state, not just the job's.

### 3a.8 `columns_to_exclude` does two things (framework prose corrected in 0.0.5)

`target_config.columns_to_exclude` is
`["sys_creation_date", "sys_update_date", "__cdc_op", "__cdc_source_table"]` on all three flows.
That attribute does **two** things, and both are intended:

1. it narrows the **comparison** basis (change detection and `__framework_hash_value`), and
2. `cdc/scd.py` passes the same list to `dlt.apply_changes`'s `except_column_list`, which
   **drops those columns from the target table's schema**.

This was verified empirically, not from the docs: before this rebuild,
`bt_digital_poc.bronze.physical_device` genuinely lacked `sys_creation_date`/`sys_update_date`.

The framework's *prose* used to deny the second half — `cdc/comparison_columns.py`,
`spec_validator.py`, `docs/00_master_reference_index.md` and `agent_skills/SKILL.md` all claimed
"comparison-only … never drops it from the target table". **That prose was wrong and is corrected
in 0.0.5**; the code was left alone, because `except_column_list` is the framework's *only*
mechanism for "don't store this column at all" on a CDC target — `data_standardization_sql` is
add/replace-only (every expression must end in `AS <name>`), and `schema_config` /
`column_normalization` only rename, cast or comment. Narrowing it would have deleted a capability
with no replacement and silently re-added columns to every already-materialized SCD target.
`tests/unit/test_comparison_columns.py` now pins both halves, including a guard that fails if the
"comparison-only" claim reappears.

**So these four columns are deliberately absent from Bronze.** `sys_creation_date` /
`sys_update_date` are Oracle audit timestamps that change on every touch, so comparing them would
make every row look drifted. Keeping them stored *and* out of comparison would mean populating
`columns_to_check` — but for SCD2 that is also passed as `track_history_column_list`, so an
explicit list would change which column changes open a new history version. That is a real
semantic change to history tracking, so comparison is left implicit. `__cdc_op` /
`__cdc_source_table` are framework control columns with no business meaning in Bronze.
`__cdc_scn` is deliberately **not** excluded — `apply_changes` must read its own `sequence_by`
column, and the SCN is useful lineage.

---

## 3b. The real batch source: Lakeflow Connect, and why its flows are *transformation* flows (v0.0.7)

**This is the single most useful thing to understand about Job 3 as it now stands.** The batch lane
no longer reads CSVs from a Volume. Its source is the **Lakeflow Connect Oracle query-based
connector**, which writes three tables into `{{catalog}}.oracle_excalibur_batch`:
`customer`, `physical_device`, `subscriber`. There is no Volume, no CSV, no `batch_date` partition
and no `pipeline_parameters.landing_root` anywhere in the spec.

**Those three tables cannot be read by an ingestion flow of any kind, and this is not a preference.**
Three independent facts stack up:

| # | Fact | Evidence |
|---|---|---|
| 1 | The connector writes those tables by **MERGE**, not append. Its default is `scd_type: SCD_TYPE_1`, so it collapses to exactly one row per primary key. | A `MERGE` operation at version 2 in each table's Delta history, and the `__ingestion_connector_primary_key` / `__ingestion_connector_cursor_columns` table properties the connector sets. |
| 2 | **Delta refuses to stream a MERGE-written table**, raising `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. Delta's own escape hatch, `skipChangeCommits`, is **refused framework-wide** because it silently drops changed rows instead of failing. | `docs/07_reconciliation_engine.md` §11.7, `docs/13_known_limitations_and_gotchas.md` R7. |
| 3 | **Every FlowX ingestion flow is planned as a streaming read**, unconditionally. `engine/source_plane.py` requests every ingestion source with `want_stream=True` ("there is no batch ingestion reader to fall back to"), and `_execute_reader` hard-rejects a batch bind of an ingestion identity. Changing `target_type` does not help: the constraint sits on the *read*, not on the write. | `engine/source_plane.py`. |

Put together: an ingestion flow would try to stream a table Delta will not stream. **A transformation
flow does not.** `source_inputs[].is_streaming: false` is honoured verbatim and resolves to a real
`spark.read.table(...)` batch read, which is exactly what a point-in-time snapshot comparison wants.

**So each of the three batch flows is a `transformation_flows[]` entry:**

- one `source_inputs[]` entry, `is_streaming: false`, naming `{{catalog}}.oracle_excalibur_batch.<table>`;
- a `transformation_sql` that casts **every** business column to the type the streaming lane writes
  to Bronze (Oracle `NUMBER` to `DOUBLE`, `DATE` to `TIMESTAMP`, `CHAR`/`VARCHAR2` to `STRING`) and
  forces the `Null(DF)=Y` columns to `NULL` with `CAST(NULL AS STRING)`;
- `target_type: "materialized_view"` with `cdc_load_strategy: "TRUNCATE_AND_LOAD"`, so
  `{{catalog}}.staging.<table>_batch` is a **full snapshot recomputed on every update**.

**Why the casting matters more than it looks.** The reconciliation compares hashes. A hash is over
values *and* their types, so if the batch lane left `CUSTOMER_ID` as the connector's `DECIMAL` while
the streaming lane wrote `DOUBLE` into Bronze, every single row would report as drifted while the
data was in fact identical. The casts exist to make the two lanes' hashes agree, and the forced
NULLs exist so a batch row can never re-introduce a credential column the streaming lane nulled out.

**And why the reconciliation flows are `pipeline_audit_only`.** Their source,
`{{catalog}}.staging.<table>_batch`, is now this same group's own `TRUNCATE_AND_LOAD` materialized
view, fully replaced on every update. `execution_mode: "pipeline"` *streams* its source to drive the
L5 heal pulse, so it is illegal here for the same reason as fact 2 above. `pipeline_audit_only`
binds the source as a batch read and registers no heal lane at all, which is why the three
`heal_<table>` job tasks in §8.3 exist.

---


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

### 4.2 Column counts (verified against the governance sheets — **[Customer-Provided]**)

| Table | Business columns in sheet | Dropped | Nulled | Column tags applied |
|---|---|---|---|---|
| `PHYSICAL_DEVICE` | 31 | 4 legacy `gcp_*` | 2 | **8** |
| `CUSTOMER` | 90 | 4 legacy `gcp_*` | 3 | **33** |
| `SUBSCRIBER` | 133 | `ctn_password`, `sub_password` + 4 `gcp_*` | 0 | **18** |

### 4.3 Where the column definitions come from

The column lists are **not hand-typed anywhere**. They are read at runtime from three governance sheets. All three are **[Customer-Provided]** — they are the Excalibur governance sheets BT supplies, and they are the only UC3 data asset not generated by this repo. `scripts/generate_uc3_test_data.py` **reads** them to learn column names, types and flags; nothing in the build ever writes them.

- [PHYSICAL_DEVICE_DDL.csv](PHYSICAL_DEVICE_DDL.csv) — **[Customer-Provided]** (`BT_Usecase/UC3/data/PHYSICAL_DEVICE_DDL.csv`)
- [CUSTOMER_DDL.csv](CUSTOMER_DDL.csv) — **[Customer-Provided]** (`BT_Usecase/UC3/data/CUSTOMER_DDL.csv`)
- [SUBSCRIBER_DDL.csv](SUBSCRIBER_DDL.csv) — **[Customer-Provided]** (`BT_Usecase/UC3/data/SUBSCRIBER_DDL.csv`)

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

Real Excalibur data cannot be used for development. `scripts/generate_uc3_test_data.py` produces a realistic stand-in. **Everything in this section is [Simulated]** — every row is synthesised by that one script. It is written to `build/uc3_test_data/` (`--out-dir build/uc3_test_data`, the default), which is **generated output, not source**.

> ### This whole section is now HISTORICAL. Neither lane reads it.
>
> The streaming lane stopped reading the simulator in **v0.0.4** (§3a) and the batch lane stopped
> in **v0.0.7** (§3b). Job 3 now reads the three Lakeflow Connect Oracle tables in
> `{{catalog}}.oracle_excalibur_batch`. There is no Volume, no CSV and no `batch_date` anywhere in
> its spec. The section is kept intact rather than deleted, because the verified figures below are
> the evidence behind the reconciliation results quoted in §14.5, which were measured in the CSV
> era and cannot be reproduced now. Read everything under §5 as "what the CSV-era build did".

**The two provenance classes, side by side:**

| Asset | Provenance | Who produces it |
|---|---|---|
| `BT_Usecase/UC3/data/{CUSTOMER,SUBSCRIBER,PHYSICAL_DEVICE}_DDL.csv` | **[Customer-Provided]** | BT's Excalibur governance sheets. Read by the generator, never written by it. |
| Everything under `build/uc3_test_data/` | **[Simulated]** | `scripts/generate_uc3_test_data.py` |

### 5.1 What it generates — all **[Simulated]**

Paths below are relative to the `--out-dir` (default `build/uc3_test_data/`).

| Output | Rows | Provenance | Purpose |
|---|---|---|---|
| `streaming/<table>/<table>_stream.csv` | **100 rows** | **[Simulated]** | *Historical.* Fed by Job 1 into the streaming lane until v0.0.4 |
| `batch/<table>/batch_date=YYYY-MM-DD/<table>_batch.csv` | **30 rows x 4 dates = 120** | **[Simulated]** | *Historical.* Fed by Job 3 into the batch lane until v0.0.7 |

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

### 5.3 Verified generated volumes — **[Simulated]**

| Table | Stream rows | Distinct PKs | Rows with `src_deleted_flg='1'` |
|---|---|---|---|
| `physical_device_stream` | 100 | 100 | **11** |
| `customer_stream` | 100 | 100 | **6** |
| `subscriber_stream` | 100 | 100 | **6** |

---

## 6. File Locations — The Exact Paths

### 6.1 In the repository (source of truth)

| What | Path | Provenance |
|---|---|---|
| Governance sheets | `BT_Usecase/UC3/data/*_DDL.csv` | **[Customer-Provided]** |
| Test data generator | `scripts/generate_uc3_test_data.py` *(historical: no lane reads its output any more, see §5)* | n/a |
| Generated test data | `build/uc3_test_data/` *(generated output, not source; historical)* | **[Simulated]** |
| Streaming CDC spec | `BT_Usecase/UC3/onboarding/uc3_excalibur_streaming_cdc.json` | — |
| Batch and recon spec | `BT_Usecase/UC3/onboarding/uc3_excalibur_batch_recon.json` | — |
| This document and its companions | `BT_Usecase/UC3/docs/{UC3_MASTER_DOCUMENT,BUILD_CONTRACT,FRAMEWORK_CAPABILITY_MAP}.md` | — |
| Job / pipeline YAMLs | `resources/uc3/*.yml` | — |
| Simulator notebooks | `notebooks/uc3/simulator/*.py` | — |
| Shared DDL parser | `src/uc3_simulator/uc3_ddl_schema.py` | — |

> **Why the sibling links in section 4.3 look relative and the table above does not.** `scripts/build_docs_reference.py::stage_usecase_docs` copies `BT_Usecase/UC3/docs/*.md` **and** the `BT_Usecase/UC3/data/*.csv` sheets into `docs/UC3/` at mkdocs build time, so a link written as `CUSTOMER_DDL.csv` resolves in the built site. `docs/UC3/` is **derived output** — it is gitignored, and `BT_Usecase/UC3/` is the single source of truth.

> **Why the DDL parser lives in `src/` and not `notebooks/`:** anything Databricks must `import` as a Python module cannot live under `notebooks/`. DABs deploys files there as *notebooks*, and Databricks refuses `import` on a notebook (`NotebookImportException`). Files under `src/` deploy as plain files and import normally.

### 6.2 On the Unity Catalog volume (runtime data)

**Root:** `/Volumes/br_digital_poc/staging/uc_3/`

| Path | Contents |
|---|---|
| `/Volumes/br_digital_poc/observability/app_logs/streaming_cdc/` | Exported observability JSON, streaming lane |
| `/Volumes/br_digital_poc/observability/app_logs/batch_recon/` | Exported observability JSONL + gzip, batch lane |
| `/Volumes/br_digital_poc/staging/uc_3/streaming/<table>/` | *Historical.* The 100-row streaming CSV Job 1 drained, until v0.0.4 |
| `/Volumes/br_digital_poc/staging/uc_3/batch/<table>/batch_date=YYYY-MM-DD/` | *Historical.* The 4 x 30-row batch CSVs Job 3 read, until v0.0.7 |
| `/Volumes/br_digital_poc/staging/uc_3/_schemas/<table>_batch/` | *Historical.* Auto Loader schema-inference checkpoints. Job 3 no longer uses Auto Loader, so nothing reads or writes these |

> **Neither lane reads this volume any more.** The streaming lane reads
> `{{catalog}}.staging.oracle_excalibur_cdc` (§3a) and the batch lane reads
> `{{catalog}}.oracle_excalibur_batch.<table>` (§3b). Only the two observability paths are live.
> The `uc_3` volume and its leftover CSVs are inert, not load-bearing.

> **Gotcha worth knowing:** `databricks fs cp` does **not** create intermediate directories on a UC Volume. It fails with `no such directory`. Create the directory tree first.

### 6.3 Tables created

| Layer | Tables |
|---|---|
| **Landing (stream)** | `br_digital_poc.staging.oracle_excalibur_cdc`, the multiplexed Debezium CDC feed Job 2 streams, and the table Job 3's heal tasks append envelope rows into |
| **Landing (batch)** | `br_digital_poc.oracle_excalibur_batch.{physical_device,customer,subscriber}`, written by the Lakeflow Connect Oracle query-based connector, **not** by this framework |
| **Staging (batch)** | `br_digital_poc.staging.{physical_device,customer,subscriber}_batch`, now **materialized views**, `TRUNCATE_AND_LOAD`, recomputed in full on every Job 3 update |
| **Bronze (governed)** | `br_digital_poc.bronze.{physical_device,customer,subscriber}` |
| **Control tables** | `reconciliation_run_log`, `reconciliation_result`, `ingestion_flow_spec`, and others |
| *Historical* | `br_digital_poc.staging.{physical_device,customer,subscriber}_stream`, the Job 1 simulator's tables. Also the heal append target until v0.0.7; healing now goes to `staging.oracle_excalibur_cdc` instead |

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 2 ]</b><br/>
Catalog Explorer showing <code>flowx</code> with the <code>staging</code> and <code>bronze</code> schemas and the UC3 tables listed.</div>

---
## 7. Onboarding JSON — Attribute by Attribute

This is the heart of the build. **No CDC code, no hashing code, no tagging code and no MERGE statement was written by hand.** All behaviour below comes from JSON attributes.

**The two spec files this section documents.** Every attribute below was read from these two files; nothing here is invented.

| Job | Spec file | `dataflow_group_id` |
|---|---|---|
| Job 2 — streaming CDC | `BT_Usecase/UC3/onboarding/uc3_excalibur_streaming_cdc.json` | `dfg_uc3_excalibur_streaming_cdc` |
| Job 3 — batch and reconciliation | `BT_Usecase/UC3/onboarding/uc3_excalibur_batch_recon.json` | `dfg_uc3_excalibur_batch_recon` |

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

| Attribute | Type | Value | Why configured this way |
|---|---|---|---|
| `dataflow_group_id` | string | `dfg_uc3_excalibur_streaming_cdc` | The group is the unit of deployment. One group equals one pipeline. The pipeline YAML references only this id. |
| `ingestion_flows` | array of objects | 3 flows | One flow per Excalibur table. Each becomes one CDC target in the DAG. |
| `observability` | array of objects | 1 destination | Post-update telemetry export. Detailed in 7.4. |

**Flow identity (per flow, at the top level of each `ingestion_flows[]` entry):**

| Attribute | Type | Value | What it does at runtime |
|---|---|---|---|
| `dataflow_id` | string | `df_uc3_<table>_stream_cdc` | Unique flow id. Becomes the `ingestion_flow_spec` control-table key and names the generated DAG nodes. |
| `source_system` | string | `excalibur` | Lineage label written to the control table. Does not affect reads. |
| `source_database` | string | `staging` | Lineage label for the originating database. Does not affect reads — the actual read is driven by `source_config`. |
| `source_table_name` | string | `PHYSICAL_DEVICE` / `CUSTOMER` / `SUBSCRIBER` | Lineage label recording the original Excalibur table name, in its source uppercase form. |
| `source_description` | string | free text | Human-readable note persisted to the control table. |
| `target_catalog` | string | `{{catalog}}` | Catalog for the Bronze target. Resolved once at onboarding time. |
| `target_schema` | string | `bronze` | Schema for the Bronze target. |
| `target_table` | string | `physical_device` / `customer` / `subscriber` | The published Bronze table name. |

**Source configuration (`source_config`, per flow):**

| Attribute | Type | Value | Why |
|---|---|---|---|
| `source_type` *(flow-level, not inside `source_config`)* | string | `zerobus` | Reads an **existing Delta table** as a stream. The Zerobus sink writes Delta, so this is the correct reader — not `autoloader`, which reads files. |
| `source_catalog` | string | `{{catalog}}` | **Never hardcode the catalog.** Substituted at onboarding time so the same spec works in dev, test and prod. |
| `source_schema` / `source_table` | string | `staging` / `oracle_excalibur_cdc` | **v0.0.4: the one multiplexed Debezium landing table, identical on all three flows** — which is what collapses them to a single physical read (§3a.2). Was `<table>_stream`, the three simulator output tables. |
| `starting_version` | integer | `0` | Stream the landing table from its **first** version, so Debezium's initial snapshot (`op='r'`) is ingested as the Bronze initial load rather than skipped (§3a.5). |
| `json_string_columns` | array of objects | `[{column: "value", schema_ddl: "struct<payload:struct<...>>"}]` | Parses the Debezium envelope out of the `value` **JSON string** column with an **explicit** schema — deterministic on batch and streaming alike, unlike the inferring `from_json`. Field names are **UPPERCASE** because `from_json` matching is case-sensitive (§3a.4 trap 1). |
| `capture_technical_metadata` | boolean | `true` | Adds `__framework_ingestion_timestamp_utc` and source-file lineage columns. This is what makes section 15 traceability possible. |
| `column_normalization` | object `{enabled, case}` | `{enabled: true, case: "lower"}` | Oracle sheets are UPPERCASE; Databricks convention is lowercase. Normalising once at the boundary means no downstream query ever has to guess the case. |
| `data_standardization_sql` | array of strings | 35 / 94 / 135 expressions | **v0.0.4: now also does the envelope projection.** It promotes the CDC control columns (`__cdc_op`, `__cdc_scn`, `__cdc_source_table`), then projects every payload column as `CASE WHEN payload.op='d' THEN payload.before.<COL> ELSE payload.after.<COL> END` (a delete carries its image in `before` — §3a.4 trap 3), converting Connect Timestamp fields with `timestamp_millis()` (§3a.4 trap 2), and derives `src_deleted_flg`. It still enforces `Null(DF)=Y` with `CAST(NULL AS STRING) AS <col>`. **Projection-only — it cannot filter rows**, which is why the topic split is a `dq_config` drop rule (§3a.3). |

> **`data_standardization_sql` is not present on all three flows.** `physical_device` declares 2 expressions (`esn_pin`, `blacklist_password`) and `customer` declares 3 (`gur_cr_card_no`, `acc_password`, `imei_black_list_pass`). **`subscriber` omits the key entirely** — its two sensitive columns are `Drop(DF)=Y`, and a dropped column cannot also be a nulled one, so there is nothing left to null. This matches 11.2 and 11.3 exactly.

**Target configuration (`target_config`, per flow — except `target_type`, which is flow-level):**

| Attribute | Type | `physical_device` | `customer` | `subscriber` | What it does at runtime |
|---|---|---|---|---|---|
| `target_type` *(flow-level)* | string | `streaming_table` | same | same | Continuously updated, checkpointed. |
| `cdc_load_strategy` | string | `SCD1` | **`SCD2`** | `SCD1` | The business rule from 4.1. Selects the `dlt.apply_changes` mode — `SCD2` sets `stored_as_scd_type="2"`. **One word switches the whole history model.** |
| `primary_keys` | array of strings | 4 cols | 1 col | 2 cols | Identity for CDC matching. Order is load-bearing — it is the basis of `__framework_hash_key`. |
| `sequence_by_column` | string | `__cdc_scn` | same | same | **Which change wins.** Out-of-order arrivals are ordered by this, not by arrival time. **v0.0.4: the Oracle SCN**, derived from `payload.source.scn` — not `sys_update_date`, which is identical for changes committed inside the same second and so cannot order them. Sequencing by SCN also makes `apply_changes` idempotent against Debezium's at-least-once redelivery (§3a.5). |
| `cdc_operation_column` | string | `src_deleted_flg` | same | same | The column carrying the delete signal. |
| `cdc_operation_mapping` | object `{delete_values: [string]}` | `{"delete_values": ["1"]}` | same | same | Evaluated as `col(cdc_operation_column).isin(delete_values)`. Value `'1'` means "delete this row". |
| `generate_hash_columns` | boolean | `true` | same | same | Framework materialises `__framework_hash_key` and `__framework_hash_value`. **One boolean replaces a hand-written SHA-256 loop.** |

**Data quality (`dq_config`, per flow — new in v0.0.4):**

The landing table is multiplexed, so each flow must admit only its own Oracle table's rows.
`data_standardization_sql` is projection-only and cannot filter, so the split is a DQ rule with
`action: "drop"`, which the engine turns into `dlt.expect_all_or_drop` on the staged view —
*before* `apply_changes` sees the rows.

| `rule_id` | Expression | Action | Why |
|---|---|---|---|
| `only_<table>_topic` | `destination = 'oracdc-excalibur.EXCALIBUR.<TABLE>'` | `drop` | Admits only this flow's Oracle table. Without it every Bronze table would receive all three tables' change events. |
| `cdc_op_recognised` | `__cdc_op IN ('r','c','u','d')` | `drop` | Drops the `oracdc-excalibur` heartbeat/schema-change topic, which carries no `payload.op` and no row image. |

> Dropped rows are still counted in the pipeline's data-quality metrics, so the fan-out is
> observable: each flow's drop count is the number of rows belonging to the *other* two tables.
| `columns_to_exclude` | array of strings | `["sys_creation_date","sys_update_date","__cdc_op","__cdc_source_table"]` | same | same | Excluded from the **value hash** and from change detection — audit timestamps change on every touch and would make every row look drifted. **This attribute also DROPS these columns from the target schema** (`cdc/scd.py` passes it to `apply_changes`'s `except_column_list`) — it is the framework's only "don't store this column at all" mechanism. So all four are deliberately absent from Bronze. The framework prose that used to claim comparison-only was wrong and is corrected in 0.0.5. See §3a.8. |
| `liquid_clustering_columns` | array of strings | `["__framework_hash_key"]` | `["customer_id"]` | `["subscriber_no","customer_id"]` | Physical layout for fast lookups. |

> **Why `physical_device` clusters on `__framework_hash_key` and not its PK:** Liquid clustering supports a **maximum of 3 columns**. `physical_device` has a **4-column** PK. Rather than truncate the PK — which would cluster on a partial key and skew the layout — the framework clusters on the single hash column that already encodes all four. **The PK itself is never truncated.**

**Governance (per flow):**

```json
"governance_tags": {
  "table_tags": { "source_system": "excalibur", "use_case": "uc3", "domain": "crm" },
  "column_tags": [ { "column": "customer_id", "tags": { "...8 tags..." } } ]
}
```

| Attribute | Type | What it does at runtime |
|---|---|---|
| `governance_tags.table_tags` | object of string to string | Applied by the `apply_governance` task as `ALTER TABLE ... SET TAGS`. Identical on all three flows: `source_system`, `use_case`, `domain`. |
| `governance_tags.column_tags` | array of `{column, tags}` | Applied as `ALTER TABLE ... ALTER COLUMN ... SET TAGS`. **8 entries** on `physical_device`, **33** on `customer`, **18** on `subscriber` — matching the applied counts verified in 11.4. |

Full detail in section 11.

### 7.2 Job 3 spec — `uc3_excalibur_batch_recon.json`

> **Rewritten for v0.0.7.** The batch lane no longer reads files. There is **no**
> `pipeline_parameters` block, **no** `${landing_root}`, **no** `source_type: "autoloader"`, no
> `path` / `format` / `schema_location` / `reader_options`, and no `partition_columns`. Those
> attributes were correct for the CSV-era build and are listed in the *historical* table at the end
> of this section so a reader of an older deployment can still map it. §3b explains why the flows
> changed shape; this section lists what they now declare.

**Batch snapshot flows, declared as `transformation_flows[]`, 3 flows, one per table:**

| Attribute | Type | Value | What it does at runtime |
|---|---|---|---|
| `dataflow_id` / `flow_step_id` | string | `df_uc3_<table>_batch_load` | Unchanged names, deliberately: the same flow identity, a different flow *type*. |
| `source_inputs[].input_name` | string | `<table>_src` | The alias the `transformation_sql` selects `FROM`. |
| `source_inputs[].table` | string | `{{catalog}}.oracle_excalibur_batch.<table>` | The Lakeflow Connect Oracle query-based connector's output table. |
| `source_inputs[].is_streaming` | boolean | **`false`** | **The load-bearing attribute.** A transformation flow honours it verbatim and resolves to a real `spark.read.table(...)`. This is the only way to read a MERGE-written table in this framework. See §3b. |
| `transformation_sql` | string | one `SELECT` per flow, 31 / 90 / 131 columns | Casts **every** business column to the Bronze-side type (Oracle `NUMBER` to `DOUBLE`, `DATE` to `TIMESTAMP`, `CHAR`/`VARCHAR2` to `STRING`) so the two lanes' hashes agree, and forces the `Null(DF)=Y` columns to `NULL` via `CAST(NULL AS STRING)` so a batch row cannot re-introduce a credential the streaming lane discarded. |
| `target_catalog` / `target_schema` / `target_table` | string | `{{catalog}}` / `staging` / `<table>_batch` | Unchanged destination. |
| `target_type` | string | **`materialized_view`** | Was a streaming table. An MV is what `TRUNCATE_AND_LOAD` needs, and it is what makes each update a clean point-in-time snapshot. |
| `target_config.cdc_load_strategy` | string | **`TRUNCATE_AND_LOAD`** | Full recompute on every update. Was `APPEND`, which kept four dated sets side by side; there are no dated sets any more, and a reconciliation wants *one* current snapshot, not an accumulating pile. |
| `target_config.liquid_clustering_columns` | array of strings | the table PK, 3 / 1 / 2 columns | Capped at 3 by the framework. |
| `governance_tags.table_tags` | object of string to string | `source_system`, `use_case`, `domain`, **`layer: staging`**, **`load_pattern: batch`** | Unchanged. Two tags more than the Bronze tables carry, marking these as the staging/batch lane. |
| `governance_tags.column_tags[]` | array | 2 on `physical_device`, 3 on `customer`, **absent on `subscriber`** | The credential columns the `transformation_sql` nulls out, tagged `data_fabric_action: NULL_AT_SOURCE`. |

> **Why no `generate_hash_columns` on the batch side:** the hashes for comparison are computed by the **reconciliation engine itself** (`hash_precomputed: false`). Adding them here would compute a hash over a *different* column set and cause exactly the mismatch described below.

> **Why the forced NULLs moved from `data_standardization_sql` into `transformation_sql`.** They are
> the same nulls, expressed in the only place a transformation flow has to express them.
> `data_standardization_sql` is a `source_config` attribute of an **ingestion** flow; a
> transformation flow's shaping *is* its `transformation_sql`, so the `CAST(NULL AS STRING) AS
> <col>` projections carry it. The `Null(DF)=Y` contract in §4.3 is unchanged.

**Reconciliation flows (3 flows, one per table):**

| Attribute | Type | Value | What it does at runtime |
|---|---|---|---|
| `reconciliation_id` | string | `rf_uc3_<table>_batch_vs_bronze` | Unique flow id. Names the generated DAG nodes and is the key written to `reconciliation_run_log`. |
| `dataflow_group_id` | string | `dfg_uc3_excalibur_batch_recon` | Binds the flow to the same group as the batch ingestion flows, so both land in one pipeline. |
| `execution_mode` | string | **`pipeline_audit_only`** | The comparison (L3 + L4: prepared sides, `__classified`, the published `__metrics`) runs **inside the DAG**. The corrective append does **not**, because audit-only registers no heal lane, which is why §8.3 has three `heal_<table>` job tasks. Was `pipeline`; that mode streams its source for the L5 pulse and is now illegal here, because the source is this group's own `TRUNCATE_AND_LOAD` materialized view (§3b). |
| `publish_schema` | string | `reconciliation` | Where the published `recon__<id>__<tgt>__metrics` table is created. |
| `match_keys` | array of strings | The table PK — 4 / 1 / 2 columns | How a source row is paired with a target row. |
| `source_config.type` | string | `table` | The source is a UC table, not a path. |
| `source_config.table` | string | `br_digital_poc.staging.<table>_batch` | The batch side. |
| `source_config.read_mode` | string | `batch` | Point-in-time snapshot read, not a stream. Reconciliation compares two settled states. |
| `source_config.hash_precomputed` | boolean | **`false`** | See the critical note below. |
| `target_configs[].target_id` | string | `bronze_<table>` | Names this target within the flow; appears in the metrics table and the run log. |
| `target_configs[].type` / `.read_mode` | string | `table` / `batch` | As above, for the Bronze side. |
| `target_configs[].table` | string | `br_digital_poc.bronze.<table>` | The Bronze side produced by Job 2. |
| `target_configs[].hash_precomputed` | boolean | **`false`** | See the critical note below. |
| `target_configs[].comparison_direction` | string | `both` | Reports rows missing in target *and* rows missing in source. |
| `target_configs[].append_target_table` | string | **`{{catalog}}.staging.oracle_excalibur_cdc`** | **The self-healing lane.** Missing/drifted rows are appended into the **multiplexed Debezium CDC landing table Job 2 already streams**, so a healed row re-enters through the same CDC engine as any real change. Was `staging.<table>_stream`, the simulator tables, which nothing reads any more. |
| `target_configs[].filter_condition` | string | **`__END_AT IS NULL`**, **`customer` only** | Restricts the Bronze side to **current** versions. `bronze.customer` is **SCD2** and holds every historical version, so without this filter a stale closed version could mask real drift: the matcher collapses duplicate keys `MATCHED > VALUE_DRIFT > MISSING`, so one matching historical row makes the key count as matched even when the current row disagrees. `physical_device` and `subscriber` are SCD1 and hold one row per key, so they need no filter. |
| `transform_sql` | string | one per flow, 12 / 22 / 32 KB | **Reshapes the miss set into a complete Debezium envelope row** before the append. Detailed immediately below. |
| `compare_columns` | array of strings | **25 / 87 / 127** columns | **Which columns must agree** for a row to count as matched. Omitting it degrades matching to key-presence only. |
| `two_tier_verification` | boolean | `true` | Fast hash comparison first, then column-level detail only for rows that differ. |
| `error_handling.on_failure` | string | `warn` | A reconciliation failure logs a warning rather than failing the pipeline update — the batch lane is an audit mechanism and must not take the pipeline down. |
| `logging_config.run_log_capture` | boolean | `true` | Writes metrics to `reconciliation_run_log`. **This is what section 16 queries read.** |
| `logging_config.mismatch_log_capture` | boolean | `false` | Per-record detail suppressed — would be very large and is not needed for the metrics. Also why the `..__mismatch` node is absent from the DAG in 9.2. |

> ### Two configuration traps, both hit for real in this build
>
> **1. `compare_columns` omitted means `value_drift_count` is structurally 0.** Without it, matching is **key-presence only**: a row whose values changed still counts as matched. The metric is not broken — it is answering "does this key exist?", not "do the values agree?". Always declare `compare_columns` when you want drift detection.
>
> **2. `hash_precomputed: true` on the target makes `matched_count` collapse to 0.** Bronze `__framework_hash_value` was built by the **CDC engine** over its comparison column set. The recon builds its source hash over **`compare_columns`**. Two different column sets give two different hashes, so every row reports as drifted. Setting `hash_precomputed: false` on **both** sides makes the engine compute both hashes over the same set. **After changing this, a `--full-refresh` is required** — otherwise the pipeline serves cached datasets and the metrics do not move.

**What `transform_sql` produces (new in v0.0.7).** `reconciliation/appender.py::apply_transform_sql`
runs this SQL over the miss set, exposed as `_reconciliation_unmatched_records`, **before**
`append_missing_records` writes it. So the appended shape is this SQL's `SELECT` list, not the
source table's columns. Each flow's SQL emits the **nine columns of the Debezium landing table**
(`destination`, `target_table`, `key`, `value`, `operation`, `source_position`,
`idempotency_key`, `partition`, `headers`), populated so the result is indistinguishable from a
real connector message:

| Piece of the envelope | How it is built |
|---|---|
| `key` / `value` | `concat('{"schema":', <verbatim Kafka-Connect schema block>, ',"payload":', to_json(...), '}')`. The schema block is copied verbatim from the real feed, not re-derived. |
| Field names inside the payload | **UPPERCASE Oracle names** (`CUSTOMER_ID`, not `customer_id`), because `from_json` is case-sensitive and the streaming lane parses against the uppercase schema. |
| Connect `Timestamp` fields | `unix_millis(...)`, giving epoch milliseconds as `int64`, which is what Connect's `org.apache.kafka.connect.data.Timestamp` logical type means. A string timestamp parses to NULL silently. |
| `before` | `null`. A heal is a corrected *current* image; there is no prior image to report. |
| `op` | `'r'` (read/snapshot), and `operation` is `'read'`. |
| `source.scn` | `MAX(existing scn) + 1`, read from the landing table itself, so a healed row sequences **after** everything already in the feed and cannot be beaten by a stale CDC event. |
| `headers` | `__flowx.producer`, `__flowx.reconciliation_id`, `__flowx.target_id`, `__flowx.heal_scn`, `__flowx.healed_at_utc`. Enough to trace any Bronze row back to the heal that produced it. |
| `idempotency_key` | `sha256` over `destination`, `key` and `value`, so a re-run of the same heal produces the same key. |

> **The onboarding preflight will tell you it cannot check this shape.** Since **v1.7.11** the
> `append_schema` existence check reports the advisory status `SHAPE_DEFINED_BY_TRANSFORM_SQL`
> instead of a false `SCHEMA_MISMATCH` when a flow declares `transform_sql`. It is informational,
> not an error. **You must verify the `SELECT` list yourself**: the append runs with
> `mergeSchema`, so a misspelt alias silently **adds a column** to the landing table rather than
> failing.

**Historical: the CSV-era attributes, for reading an older deployment.** None of these appear in
the spec any more. Presence of an unknown key is a hard onboarding rejection since v1.7.1, so do
not copy them forward.

| Attribute | Old value | Replaced by |
|---|---|---|
| `pipeline_parameters.landing_root` | `/Volumes/br_digital_poc/staging/uc_3/batch` | *(nothing, no paths remain)* |
| `source_type` *(flow-level)* | `autoloader` | a `transformation_flows[]` entry with `source_inputs[].is_streaming: false` |
| `source_config.path` | `${landing_root}/<table>/` | `source_inputs[].table` |
| `source_config.format` | `csv` | *(nothing, the source is a table)* |
| `source_config.schema_location` | `/Volumes/.../_schemas/<table>_batch/` | *(nothing, no Auto Loader and no schema checkpoint)* |
| `source_config.reader_options` | `{header, delimiter, cloudFiles.inferColumnTypes}` | *(nothing)* |
| `source_config.column_normalization` | `{enabled: true, case: "lower"}` | the lower-case aliases in `transformation_sql` |
| `source_config.data_standardization_sql` | forced `NULL`s per `Null(DF)=Y` | the `CAST(NULL AS ...)` projections in `transformation_sql` |
| `target_config.cdc_load_strategy` | `APPEND` | `TRUNCATE_AND_LOAD` |
| `target_config.partition_columns` | `["batch_date"]` | *(nothing, there is no `batch_date`)* |
| `target_configs[].append_target_table` | `staging.<table>_stream` | `staging.oracle_excalibur_cdc` plus `transform_sql` |
| `execution_mode` | `pipeline` | `pipeline_audit_only` plus three `heal_<table>` job tasks |

### 7.3 `${param}` vs `{{catalog}}` — two different lifecycles

This distinction matters and is easy to get wrong.

| Placeholder | Resolved when | By what | Stored in control table as |
|---|---|---|---|
| `{{catalog}}`, `{{env}}` | **Onboarding time** (once) | `onboarding/spec_loader.py` | the **resolved value** |
| `${param}` | **Every pipeline update** | `engine/source_plane.py` and `engine/flow_generators.py` | the **raw placeholder** |

**Why the difference is deliberate:** the catalog is fixed for a deployment, so resolving it once is right. A path may need retargeting without re-onboarding, so it stays a placeholder in the control table and resolves fresh each run.

> **Neither UC3 spec uses `${param}` any more (v0.0.7).** It went away with `landing_root`, the last
> path either lane had. The distinction is kept here because it is a framework-wide rule and the
> failure it prevents (`Path must be absolute: ${landing_root}/...`, Appendix A R10) is one this
> build actually hit.

### 7.4 `observability` — the telemetry export block

Both specs declare exactly one observability destination. This is what the `observability_export` task in section 8.2 drains.

| Attribute | Type | Job 2 value | Job 3 value | What it does at runtime |
|---|---|---|---|---|
| `id` | string | `uc3_streaming_cdc_volume_export` | `uc3_batch_recon_volume_export` | Unique destination id in the observability control table. |
| `enabled` | boolean | `true` | `true` | Active flag. A disabled destination is stored but never dispatched to. |
| `type` | string | `DATABRICKS_VOLUME` | same | Writes event-log telemetry to a UC Volume. The alternative is `OTLP_CONSUMER`, which posts to an OTLP endpoint. |
| `mode` | string | `triggered` | same | Bounded post-update export for one pipeline, run by the `observability_export` task. The alternative, `continuous`, is an always-on streaming export and is **not** used here. |
| `destination_config.volume_path` | string | `/Volumes/{{catalog}}/observability/app_logs/streaming_cdc` | `/Volumes/{{catalog}}/observability/app_logs/batch_recon` | Export target. Both specs now use the `{{catalog}}` placeholder. Job 3 used to hardcode a catalog name, which is corrected. See 7.3 for why the placeholder form is preferred. |
| `destination_config.file_format` | string | `JSON` | `JSONL` | Output encoding. Allowed values are `JSONL` (default) and `JSON`. |
| `destination_config.compression` | **string** | *(omitted — defaults to `none`)* | `GZIP` | Compression applied to the exported file. |

> **`compression` is a STRING, not a boolean.** The key is `compression` and its allowed values are `"GZIP"`, `"gzip"`, `"none"` and `""`, defaulting to `"none"` when omitted. There is **no** boolean `compressed` attribute anywhere in the framework — writing one is an unknown attribute and is **hard-rejected** at onboarding.

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

Since 2026-09-08 the run job carries only what every run needs; provisioning and tagging live
in two one-time jobs (same split as `resources/sample_jobs/flowx_sample_seed_job.yml`):

| Job | Tasks | When to run |
|---|---|---|
| `003b_lfj_uc3_excalibur_seed` (`uc3_seed_job.yml`) | `setup_control_tables` → `onboard_streaming_cdc` → `onboard_batch_recon` | Once per workspace, and after any change to either spec. `setup_control_tables` is optional for the pipelines (onboarding already provisions and migrates the control tables); it adds the preflight UC function and observability views. |
| `004_lfj_uc3_excalibur_streaming_cdc` | `run_pipeline_update` → `observability_export` | Every run. |
| `009_lfj_uc3_excalibur_governance` (`uc3_governance_job.yml`) | `tag_streaming_cdc` → `tag_batch_recon` | Once the tables exist, and after any `governance_tags` change (re-seed first: tags are read from control rows). |

| # | Task (run job) | What it does | Why it must be here |
|---|---|---|---|
| 1 | `run_pipeline_update` | Triggers pipeline `004_ldp_...` | Where the actual data work happens. The pipeline needs only two `spark.conf` keys and active control rows. |
| 2 | `observability_export` | Exports run telemetry | Feeds `/Volumes/<catalog>/observability/app_logs/`. |

> **Why tagging needs its own job.** `ALTER TABLE ... SET TAGS` runs against an **already-materialised** table. It cannot run inside the pipeline update, because during the update the table does not yet exist in its final form. This build proved it the hard way: Job 2 succeeded end-to-end with **zero tags applied**, because no task ever invoked the tagging step. Removing the task from the run job (2026-09-08) must not recreate that gap, which is why `009_lfj_uc3_excalibur_governance` exists and is part of the run order.

> **Why onboarding is a `run_job_task`.** There is exactly **one** onboarding entrypoint in the bundle. Inlining `02_onboarding_engine.py` into a new job creates a second copy that drifts. The job depends only on the spec **path**.

### 8.3 Job 3 — `005_lfj_uc3_excalibur_batch_recon`

**Five tasks**, driving pipeline **`006_ldp_uc3_excalibur_batch_recon`**; its spec is onboarded by the seed job's second task and tagged by the governance job's second task.

| Order | Task | What it does |
|---|---|---|
| 1 | `run_pipeline_update` | Recomputes the three `staging.<table>_batch` materialized views from the Lakeflow Connect tables, then runs the three reconciliation comparisons (L3 + L4) inside the same update and publishes `recon__<id>__<tgt>__metrics`. |
| 2 | `heal_physical_device` | `notebooks/05_reconciliation/05_reconciliation_engine.py`, `reconciliation_id: rf_uc3_physical_device_batch_vs_bronze` |
| 2 | `heal_customer` | same notebook, `rf_uc3_customer_batch_vs_bronze` |
| 2 | `heal_subscriber` | same notebook, `rf_uc3_subscriber_batch_vs_bronze` |
| 3 | `observability_export` | Drains the pipeline's event log to the observability volume. Depends on all three heal tasks. |

> **Why the three `heal_*` tasks exist, and why removing them breaks the use case silently.** The
> three reconciliation flows are `execution_mode: "pipeline_audit_only"` (§7.2, §3b), and audit-only
> registers the comparison but **no heal lane at all**. Without these tasks the batch lane would
> compare and report forever while never healing anything: every run green, every metric
> populated, and not one correction applied. One task per `reconciliation_id`; the notebook takes
> exactly one.

The three heal tasks run **in parallel with each other** (different `reconciliation_id`, different
source, different target, no shared state) but all **after** the pipeline update, because each
re-reads the prepared source and target that update just published. Each passes
`task_run_id: {{job.run_id}}`, which is what correlates every `reconciliation_run_log` and
`reconciliation_result` row back to this job run.

> **The 005 / 006 numbering mismatch is deliberate.** Job 2 job and pipeline are both `004`. Job 3 are `005` and `006`. This is intentional and preserved from the original specification. Do not "fix" it.

**Runtime dependency:** Job 3 **must** run after Job 2. It compares against `br_digital_poc.bronze.<table>`, which does not exist until Job 2 publishes it.

**Never run the three jobs in parallel.** Job 3 reads Job 2's bronze tables, and (in the seed job) concurrent onboarding into one catalog hits a known Unity Catalog `CREATE` race — which is why its two onboarding tasks are serial.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 3 ]</b><br/>
Job run detail for <code>004_lfj_uc3_excalibur_streaming_cdc</code> showing all five tasks green.</div>

---

## 9. DLT Pipeline DAGs — Every Node Explained

**The one rule that shapes both DAGs:** *one external read per source table, per execution mode.* Reading the same source twice means paying twice and risking two different answers.

### 9.1 Job 2 DAG — streaming CDC

```
  br_digital_poc.staging.physical_device_stream --+
                                         |
                                         +--> [ _src_... ] --> AUTO CDC --> br_digital_poc.bronze.physical_device
  br_digital_poc.staging.customer_stream ---------+    (temporary)                   br_digital_poc.bronze.customer
                                         |                                  br_digital_poc.bronze.subscriber
  br_digital_poc.staging.subscriber_stream -------+
```

| Node | Type | Stored? | Why it exists |
|---|---|---|---|
| `_src_<fingerprint>__stream` | `@dlt.table(temporary=True)` | **Pipeline-scoped only.** Materialised, but **not published** to Unity Catalog. | The single read boundary. Materialised rather than a view because a view is **inlined into each consumer** — "declared once" is not "read once". Only materialisation guarantees one read. |
| `br_digital_poc.bronze.<table>` | Streaming table | **Yes — this is the real data.** | The CDC target. Written by AUTO CDC (`apply_changes`). |
| `_<table>_scd2_history` | Hidden backing table | Yes (internal) | **`customer` only.** Lakeflow internal SCD2 history, managed via `stored_as_scd_type="2"`. |

**How to read this:** if a name starts with `_`, it is **internal plumbing** — it holds no business data you should query. Query `br_digital_poc.bronze.<table>`.

### 9.2 Job 3 DAG, batch and reconciliation (four layers in the graph, healing outside it)

```
 <cat>.oracle_excalibur_batch.<table>  --> [ transformation flow,     --> <cat>.staging.<table>_batch
   (Lakeflow Connect, MERGE-written)         is_streaming: false,          (L1: MATERIALIZED VIEW,
                                             transformation_sql ]           TRUNCATE_AND_LOAD,
                                                              |             recomputed every update)
                                                              v
                                    _recon__<id>__src         (L3: batch read)
                                    _recon__<id>__<tgt>__tgt  (L3: batch read)  <-- <cat>.bronze.<table>
                                                              |                     (customer: __END_AT IS NULL)
                                                              v
                                    _recon__<id>__<tgt>__classified  (L4: temporary)
                                                              |
                              +-------------------------------+-----------------+
                              v                               v                 v
                     recon__<id>__<tgt>__metrics    _recon__..__missing    (mismatch: OFF)
                          (L4: PUBLISHED)              (L4: temporary)

  ------------------------------- end of the pipeline graph -------------------------------

  heal_<table> job task --> 05_reconciliation_engine.py --> transform_sql --> appends
                                                             (Debezium envelope rows)
                                                             into <cat>.staging.oracle_excalibur_cdc
                                                             --> Job 2 applies them as ordinary CDC
```

**Every node, and whether it stores data:**

| Layer | Node | Stored in Unity Catalog? | Purpose |
|---|---|---|---|
| L1 | `br_digital_poc.staging.<table>_batch` | **YES, a materialized view** | The current batch snapshot, recomputed in full on every update. Query this. |
| L3 | `_recon__<id>__src` | **No** (temporary) | One shared hash-prepared **batch** read of the source. Under `pipeline_audit_only` this is bound with `want_stream=False`, which is the whole point (§3b). |
| L3 | `_recon__<id>__<tgt>__tgt` | **No** (temporary) | The Bronze side, read as batch. On `customer` the `filter_condition: "__END_AT IS NULL"` is applied here, so only current SCD2 versions are compared. |
| L4 | `_recon__<id>__<tgt>__classified` | **No** (temporary) | The full-outer-join classification. Read up to 3 times downstream, so materialised to compute the join once. |
| L4 | `recon__<id>__<tgt>__metrics` | **YES — published** | **One row** of counts. This is the audit record. |
| L4 | `..__mismatch` | **Not registered** | Per-record detail. `mismatch_log_capture: false`, so this node does not exist in our DAG. |
| L4 | `_recon__<id>__<tgt>__missing` | **No** (temporary) | The rows to heal. |

> **There is no L5 in this graph any more.** Under `pipeline_audit_only` the framework returns
> before registering the L5 pulse, heal flow and `foreach_batch_sink` handler, so
> `_recon__<id>__pulse` and `_recon__<id>__heal_sink` **do not exist** in `006_ldp_uc3_excalibur_batch_recon`.
> If you go looking for them in the Lakeflow graph you will not find them, and that is correct.
> Healing is the three `heal_<table>` job tasks in §8.3, running the standalone
> `05_reconciliation_engine.py` after the update completes. See §3b for why the pipeline lane is
> not available here, and `docs/07_reconciliation_engine.md` §11.7 for the framework rule.

> **The single most useful thing to understand here:** of the nodes per reconciliation flow, only **two** hold queryable business data: the `_batch` materialized view and the `__metrics` table. Everything prefixed `_` is intermediate. This is the **Intermediate Object Rule**: intermediates are materialised for correctness and performance but never published, so the catalog stays clean.

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
> | `SELECT count(*) FROM br_digital_poc.information_schema.column_tags` | **445** — correct |
> | `SELECT count(*) FROM system.information_schema.column_tags` | **445** — metastore-wide |
>
> **Always qualify with the catalog**, or use `system.information_schema` for a metastore-wide answer. Tagging works correctly on Lakeflow streaming tables — an earlier conclusion that it did not was a wrong-catalog reading error, not a platform limitation.

<div class="screenshot"><b>[ SCREENSHOT PLACEHOLDER 5 ]</b><br/>
Catalog Explorer, <code>br_digital_poc.bronze.customer</code>, Columns tab, showing tag chips on <code>customer_id</code>.</div>

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

*Result in `br_digital_poc.bronze.physical_device`:* **one row only.**

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

*Result in `br_digital_poc.bronze.customer`:* **two rows**, one closed and one current.

| customer_id | address | `__START_AT` | `__END_AT` | Meaning |
|---|---|---|---|---|
| C001 | 10 Old Street | 2026-08-01 09:00:00 | 2026-08-03 14:22:00 | Historic |
| C001 | 25 New Road | 2026-08-03 14:22:00 | **NULL** | **Current** |

> **`__END_AT IS NULL` means "this is the current version".** That is the single most useful filter on an SCD2 table.

**Note on SCD2 deletes:** a delete **closes** the row (sets `__END_AT`) rather than removing it. History is preserved — which is exactly why `customer` still shows 100 rows despite 6 deletes.

### 14.5 The reconciliation CDC case — the scenario that matters most

**The situation:** a record exists in the source and was loaded to Bronze via streaming. Later, the **batch snapshot carries different values** for that same key, values the stream never captured, because of a missed message, a late correction, or a network drop.

**Step 1 — the streaming row already in Bronze:**

| customer_id | contact_telno | sys_update_date | source |
|---|---|---|---|
| C042 | 07700 900111 | 2026-08-01 10:00:00 | stream |

**Step 2. The batch snapshot carries different values for the same key:**

| customer_id | contact_telno | sys_update_date |
|---|---|---|
| C042 | **07700 900999** | **2026-08-03 16:45:00** |

**Step 3. Reconciliation compares them, inside the pipeline update.**

- `match_keys: ["customer_id"]` — same key, so the rows **pair up**.
- The Bronze side is filtered to `__END_AT IS NULL`, so only the **current** SCD2 version of C042 takes part. Without that filter a stale closed version could match and mask the drift.
- `compare_columns` (87 columns) — `contact_telno` differs.
- Both hashes computed over the same 87 columns, so **hashes differ**.
- Classification: **`VALUE_DRIFT`**, so `value_drift_count` increments, and `recon__…__metrics` is published by the same update.

**Step 4. Self-healing, in the `heal_customer` job task.** The comparison ran inside the pipeline,
but the correction does not: `execution_mode: "pipeline_audit_only"` registers no heal lane, so the
`heal_customer` task runs `05_reconciliation_engine.py` after the update finishes (§8.3). It reads
the miss set, runs the flow's `transform_sql` over it, and appends the result into
`append_target_table: {{catalog}}.staging.oracle_excalibur_cdc`, **the same multiplexed Debezium
landing table Job 2 is already streaming**.

**What is appended is a complete Debezium envelope row**, not a copy of the staging row: the nine
landing columns, the verbatim Kafka-Connect `schema` block, UPPERCASE Oracle field names,
`unix_millis()` on the Connect `Timestamp` fields, `before: null`, `op: 'r'`, and
`source.scn = MAX(existing scn) + 1` so the heal sequences after everything already in the feed.
See §7.2 for the full breakdown.

**Step 5. Job 2's CDC engine applies it.** Job 2 is `continuous: true`, so it picks the row up as
an ordinary change event: parsed by the same `from_json`, sequenced by the same column, applied by
the same `apply_changes`. On SCD2 it creates a **new version**; on SCD1 it **overwrites**.

> **Why heal through the CDC landing table rather than writing to Bronze directly?** Writing
> directly would bypass the CDC engine: no sequencing, no SCD2 versioning, no delete handling.
> Routing the repair through the same bus means **there is exactly one implementation of "how a
> change is applied"**. The healed row is indistinguishable from one that arrived on time, which is
> exactly the property that makes the audit trustworthy.
>
> *(Until v0.0.7 the heal target was `staging.customer_stream`, the Job 1 simulator's table. The
> principle was the same; the target was a simulator table that nothing reads any more.)*

**Reconciliation metrics from the CSV-era build. HISTORICAL, not current:**

> These figures were measured when Job 3 read the four dated CSV sets. **They cannot be reproduced
> against the Lakeflow Connect source and should not be quoted as current results.** They are kept
> because the arithmetic checks below are the evidence for two counting behaviours that are still
> true, and deleting the numbers would delete the proof. The v0.0.7 end-to-end run had not been
> verified at the time of writing, so no current figures are stated here.

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
SELECT 'staging_stream' AS layer, 'customer' AS tbl, count(*) AS row_count FROM br_digital_poc.staging.customer_stream
UNION ALL SELECT 'staging_batch', 'customer', count(*) FROM br_digital_poc.staging.customer_batch
UNION ALL SELECT 'bronze',        'customer', count(*) FROM br_digital_poc.bronze.customer
UNION ALL SELECT 'staging_stream','physical_device', count(*) FROM br_digital_poc.staging.physical_device_stream
UNION ALL SELECT 'staging_batch', 'physical_device', count(*) FROM br_digital_poc.staging.physical_device_batch
UNION ALL SELECT 'bronze',        'physical_device', count(*) FROM br_digital_poc.bronze.physical_device
UNION ALL SELECT 'staging_stream','subscriber', count(*) FROM br_digital_poc.staging.subscriber_stream
UNION ALL SELECT 'staging_batch', 'subscriber', count(*) FROM br_digital_poc.staging.subscriber_batch
UNION ALL SELECT 'bronze',        'subscriber', count(*) FROM br_digital_poc.bronze.subscriber
ORDER BY tbl, layer;
```

**Expected:** stream 100, batch 120, bronze 89 / 100 / 94.

### T2 — Prove SCD1 deletes were applied

```sql
SELECT
  (SELECT count(*) FROM br_digital_poc.staging.physical_device_stream)                             AS source_rows,
  (SELECT count(*) FROM br_digital_poc.staging.physical_device_stream WHERE src_deleted_flg = '1') AS deletes,
  (SELECT count(*) FROM br_digital_poc.bronze.physical_device)                                     AS bronze_rows,
  (SELECT count(*) FROM br_digital_poc.staging.physical_device_stream)
    - (SELECT count(*) FROM br_digital_poc.staging.physical_device_stream WHERE src_deleted_flg = '1')
                                                                                          AS expected_bronze;
```

**Expected:** `bronze_rows` equals `expected_bronze` equals 89.

### T3 — Prove SCD1 keeps exactly one row per key

```sql
SELECT count(*) AS total_rows,
       count(DISTINCT __framework_hash_key) AS distinct_keys,
       CASE WHEN count(*) = count(DISTINCT __framework_hash_key)
            THEN 'PASS - one row per key' ELSE 'FAIL - duplicates present' END AS verdict
FROM br_digital_poc.bronze.physical_device;
```

### T4 — See SCD2 history on a customer

```sql
-- Customers with more than one version
SELECT customer_id, count(*) AS versions
FROM br_digital_poc.bronze.customer
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
FROM br_digital_poc.bronze.customer
WHERE customer_id = (
        SELECT customer_id FROM br_digital_poc.bronze.customer
        GROUP BY customer_id HAVING count(*) > 1 LIMIT 1)
ORDER BY __START_AT;
```

### T5 — Current-state-only view of an SCD2 table

```sql
-- This is how a business user should normally query customer.
SELECT * FROM br_digital_poc.bronze.customer WHERE __END_AT IS NULL;
```

### T6 — Prove `Null(DF)=Y` columns are always NULL

```sql
SELECT count(*) AS total_rows,
       count(acc_password)         AS non_null_acc_password,
       count(imei_black_list_pass) AS non_null_imei_pass,
       count(gur_cr_card_no)       AS non_null_card_no,
       CASE WHEN count(acc_password) + count(imei_black_list_pass) + count(gur_cr_card_no) = 0
            THEN 'PASS - all governance-nulled' ELSE 'FAIL - data leaked' END AS verdict
FROM br_digital_poc.bronze.customer;
```

### T7 — Prove `Drop(DF)=Y` columns do not exist

```sql
SELECT count(*) AS should_be_zero,
       CASE WHEN count(*) = 0 THEN 'PASS - dropped columns absent'
            ELSE 'FAIL - dropped column present' END AS verdict
FROM br_digital_poc.information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name = 'subscriber'
  AND column_name IN ('ctn_password', 'sub_password');
```

### T8 — See all governance tags

```sql
-- NOTE the flowx. prefix. Without it this returns 0 rows and tells you nothing.
SELECT table_name, column_name, tag_name, tag_value
FROM br_digital_poc.information_schema.column_tags
WHERE schema_name = 'bronze'
  AND table_name IN ('customer','physical_device','subscriber')
ORDER BY table_name, column_name, tag_name;
```

```sql
-- Summary: tagged columns per table. Expect 33 / 8 / 18.
SELECT table_name,
       count(DISTINCT column_name) AS tagged_columns,
       count(*)                    AS tag_pairs
FROM br_digital_poc.information_schema.column_tags
WHERE schema_name = 'bronze'
  AND table_name IN ('customer','physical_device','subscriber')
GROUP BY table_name
ORDER BY table_name;
```

### T9 — Find all PII columns needing de-identification

```sql
SELECT table_name, column_name, tag_value AS deid_rule
FROM br_digital_poc.information_schema.column_tags
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
  SELECT DISTINCT customer_id FROM br_digital_poc.staging.customer_batch
),
bronze_keys AS (
  SELECT DISTINCT customer_id FROM br_digital_poc.bronze.customer
)
SELECT
  (SELECT count(*) FROM batch_keys)  AS distinct_batch_keys,
  (SELECT count(*) FROM bronze_keys) AS distinct_bronze_keys,
  (SELECT count(*) FROM batch_keys b JOIN bronze_keys z USING (customer_id)) AS overlapping_keys,
  'overlapping_keys should equal matched_count + value_drift_count' AS note;
```

**Expected:** the identity `overlapping_keys = matched_count + value_drift_count` from T10 holds.
The identity is what this query proves; the absolute numbers depend on what the Oracle source
currently holds. *(In the CSV-era build it read 48, with `matched 26 + drift 22 = 48`.)*

**Note for `customer`:** T10's counts come from a Bronze side filtered to `__END_AT IS NULL`, while
`bronze_keys` above is unfiltered. `customer` is SCD2, so add `WHERE __END_AT IS NULL` to
`bronze_keys` to compare like with like.

### T12. Batch snapshot row count

```sql
SELECT count(*) AS row_count, count(DISTINCT customer_id) AS distinct_keys
FROM br_digital_poc.staging.customer_batch;
```

**Expected:** one row per primary key. `staging.customer_batch` is a `TRUNCATE_AND_LOAD`
materialized view over a Lakeflow Connect `SCD_TYPE_1` table, so it is a **current snapshot**, not
an accumulation. `row_count` and `distinct_keys` must be equal.

> **`batch_date` no longer exists.** Until v0.0.7 this query grouped by `batch_date` and expected
> 4 dates x 30 rows = 120, because the lane appended four dated CSV sets. There are no dated sets
> and no partition column now (§3b).

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
FROM br_digital_poc.bronze.customer;
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
FROM br_digital_poc.bronze.customer;
```

### T15 — Business time versus processing time

```sql
SELECT customer_id,
       sys_update_date                     AS business_time,
       __framework_ingestion_timestamp_utc AS processing_time,
       timestampdiff(DAY, sys_update_date,
                     __framework_ingestion_timestamp_utc) AS lag_days
FROM br_digital_poc.bronze.customer
WHERE __END_AT IS NULL
ORDER BY lag_days DESC
LIMIT 20;
```

**Reading it:** a large `lag_days` is **normal** — the generated business dates are historic. It confirms the two clocks are genuinely independent.

### T16 — Full data-quality scorecard (run this one for a demo)

```sql
USE CATALOG flowx;

SELECT 'Bronze row count matches source minus deletes' AS check_name,
       CASE WHEN (SELECT count(*) FROM br_digital_poc.bronze.physical_device) =
                 (SELECT count(*) FROM br_digital_poc.staging.physical_device_stream)
               - (SELECT count(*) FROM br_digital_poc.staging.physical_device_stream WHERE src_deleted_flg='1')
            THEN 'PASS' ELSE 'FAIL' END AS result
UNION ALL
SELECT 'SCD1: one row per key (physical_device)',
       CASE WHEN (SELECT count(*) FROM br_digital_poc.bronze.physical_device) =
                 (SELECT count(DISTINCT __framework_hash_key) FROM br_digital_poc.bronze.physical_device)
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'SCD2: customer carries history columns',
       CASE WHEN (SELECT count(*) FROM br_digital_poc.information_schema.columns
                  WHERE table_schema='bronze' AND table_name='customer'
                    AND column_name IN ('__START_AT','__END_AT')) = 2
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Governance-nulled columns are 100 percent NULL',
       CASE WHEN (SELECT count(acc_password)+count(imei_black_list_pass)+count(gur_cr_card_no)
                  FROM br_digital_poc.bronze.customer) = 0
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Dropped columns absent from schema',
       CASE WHEN (SELECT count(*) FROM br_digital_poc.information_schema.columns
                  WHERE table_schema='bronze' AND table_name='subscriber'
                    AND column_name IN ('ctn_password','sub_password')) = 0
            THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Governance tags applied (expect 59 tagged columns)',
       CASE WHEN (SELECT count(DISTINCT concat(table_name,'.',column_name))
                  FROM br_digital_poc.information_schema.column_tags
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
# 1. HISTORICAL -- the test-data generator fed the CSV-era build. Neither lane reads its
#    output now (see section 5). Skip it unless you are reproducing the old topology.
# python scripts/generate_uc3_test_data.py --upload --catalog flowx --staging-schema staging

# 2. Deploy. NEVER do this while a pipeline is running.
databricks bundle deploy -t metaflow_v7 -p metaflow_v7

# 3. Seed the control tables once per workspace (onboards both UC3 specs).
databricks bundle run uc3_seed_job -t hoonartek -p Hoonartek

# 4. Start the STREAMING lane. v0.0.4: this is the PIPELINE, not a job -- it is
#    `continuous: true`, so it never completes and Databricks owns its lifecycle
#    thereafter. Do NOT wrap it in a job that waits on it. (UC3_MASTER_DOCUMENT.md 3a.6)
databricks bundle run uc3_streaming_cdc_pipeline -t hoonartek -p Hoonartek

# 5. The BATCH lane still runs as an ordinary triggered job.
databricks bundle run uc3_batch_recon_job -t hoonartek -p Hoonartek

# 6. Tag the tables once they exist -- and again after any governance_tags change,
#    because tags are applied from the control-table rows, not from the spec file.
databricks bundle run uc3_governance_job -t hoonartek -p Hoonartek
```

> **Neither lane needs `uc3_streaming_simulator_job` any more.** The stream reads the real Debezium
> CDC feed in `<catalog>.staging.oracle_excalibur_cdc` (v0.0.4) and the batch lane reads the
> Lakeflow Connect Oracle tables in `<catalog>.oracle_excalibur_batch` (v0.0.7). The simulator, its
> CSVs and the `uc_3` volume are historical.
>
> **Stop the continuous pipeline before any redeploy.** `bundle deploy` prunes superseded
> artifacts from `<artifact_path>/.internal/` and kills a live update with
> `ENVIRONMENT_PIP_INSTALL_ERROR`; with a continuous pipeline that window is always open.

### One-time migration to the v0.0.7 batch lane

**Only needed on a workspace that ran the CSV-era Job 3.** A fresh workspace needs neither step.
Both are once-only, and skipping either fails in a way that is easy to misread.

**1. Drop the three old `_batch` streaming tables first.**

```sql
DROP TABLE IF EXISTS <catalog>.staging.physical_device_batch;
DROP TABLE IF EXISTS <catalog>.staging.customer_batch;
DROP TABLE IF EXISTS <catalog>.staging.subscriber_batch;
```

They were `STREAMING_TABLE`s and are now materialized views. **Lakeflow cannot convert a streaming
table into a materialized view in place**: the update fails rather than rewriting the object. Drop
them and let the pipeline recreate them.

**2. Re-onboard the spec with `prune_missing_flows=true`.**

Changing a flow's *type* does not retire the old row. The three `df_uc3_<table>_batch_load` flows
moved from `ingestion_flows` to `transformation_flows`, and their old `ingestion_flow_spec` rows
stay `is_active = true` unless pruned, so the pipeline would build **both** shapes of the flow and
the ingestion one would fail exactly as §3b predicts. Run the onboarding job for
`uc3_excalibur_batch_recon.json` with `prune_missing_flows=true`.

**Then the batch lane runs as it always has:**

```bash
databricks bundle run uc3_batch_recon_job -t hoonartek -p Hoonartek
```

### Validating a spec before deploying

```bash
python -c "
import json,sys; sys.path.insert(0,'src')
from flowx.lakeflow_framework.onboarding.agent_tools import validate_json
r = validate_json(open('BT_Usecase/UC3/onboarding/uc3_excalibur_streaming_cdc.json', encoding='utf-8').read())
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
| Tag query returns 0 rows | Wrong catalog | Qualify it: `br_digital_poc.information_schema....` |
| `ENVIRONMENT_PIP_INSTALL_ERROR` | Deployed mid-update | Never deploy while a pipeline runs |
| `NotebookImportException` | Python module placed under `notebooks/` | Move it to `src/` |
| `PERSIST TABLE is not supported` | `.cache()` on serverless | Remove it |
| `no such directory` on volume copy | Intermediate dirs missing | Create the directory tree first |
| `DELTA_SOURCE_TABLE_IGNORE_CHANGES` on a batch flow | Something is trying to **stream** the MERGE-written Lakeflow Connect table, or the `_batch` MV | The batch flows must be `transformation_flows` with `is_streaming: false`, and the recon flows `pipeline_audit_only`. See §3b |
| Batch lane reports drift every run but nothing is ever corrected | `pipeline_audit_only` registers no heal lane and the `heal_<table>` job tasks are missing | Add one `05_reconciliation_engine.py` task per `reconciliation_id` (§8.3) |
| `_batch` table fails to become a materialized view | It still exists as the old `STREAMING_TABLE`; Lakeflow will not convert in place | Drop the three `staging.<table>_batch` tables once, then re-run (Appendix B) |
| The old ingestion flow still builds after the spec changed | Flow-type change leaves `ingestion_flow_spec` rows `is_active = true` | Re-onboard with `prune_missing_flows=true` (Appendix B) |

---

*Every figure in this document was read back from the live `metaflow_v7` workspace after a successful run. Where a result looks surprising, the reason is stated rather than smoothed over.*
