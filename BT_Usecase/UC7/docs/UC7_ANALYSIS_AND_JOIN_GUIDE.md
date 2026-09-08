<style>
:root { --aptos: Aptos, "Aptos Display", "Segoe UI Variable", "Segoe UI", system-ui, -apple-system, sans-serif; }
body, .md-typeset, .md-typeset table, .md-typeset h1, .md-typeset h2,
.md-typeset h3, .md-typeset h4, .md-typeset p, .md-typeset li { font-family: var(--aptos) !important; }
.md-typeset code, .md-typeset pre { font-family: "Cascadia Code", Consolas, "Courier New", monospace !important; }
.screenshot { border: 1px dashed #9aa0a6; background: #f6f7f9; padding: 14px 16px;
  margin: 12px 0; border-radius: 6px; color: #4a4f55; font-family: var(--aptos); }
.screenshot b { color: #1a1a1a; }
</style>

# UC7 CDR ASN.1 : Data Analysis, Join and Testing Guide

**Use case:** UC7 — Call Data Record (CDR) ASN.1 decoding for four mobile network elements
**Catalog:** `bt_digital_poc`  **Schema:** `bronze`  **Target:** `hoonartek`  **Profile:** `Hoonartek`  **Framework:** FlowX metadata-driven ingestion
**Companion query pack:** [UC7_ANALYSIS_QUERIES.sql](UC7_ANALYSIS_QUERIES.sql) — every query in this document, ready to copy and run
**Status:** All four decoders verified green. Every figure in this document was read back from the live workspace on 2026-09-08.

---

## Index

| # | Section | What you will learn |
|---|---|---|
| 1 | [What This Document Adds](#1-what-this-document-adds) | How this differs from the existing UC7 master document |
| 2 | [The Use Case in Plain Terms](#2-the-use-case-in-plain-terms) | What a CDR is, and why four separate decoders exist |
| 3 | [The Four Tables and Their Purpose](#3-the-four-tables-and-their-purpose) | Every table, what it holds, who uses it |
| 4 | [Step-by-Step Pipeline Walkthrough](#4-step-by-step-pipeline-walkthrough) | Nine steps, file on disk to queryable table |
| 5 | [Every Node in the Pipeline Explained](#5-every-node-in-the-pipeline-explained) | Node by node, purpose and use |
| 6 | [The CHOICE Structure](#6-the-choice-structure-the-single-most-important-concept) | The one concept you must grasp before writing SQL |
| 7 | [Source Data Analysis](#7-source-data-analysis) | Volumes, keys, cardinality, quality |
| 8 | [Join Analysis](#8-join-analysis-the-honest-answer) | Which joins work, which return nothing, and why |
| 9 | [Target and Exported Data Analysis](#9-target-and-exported-data-analysis) | What lands, what a consumer would export |
| 10 | [Testing SQL for Any User](#10-testing-sql-for-any-user) | Copy-paste checks with expected answers |
| 11 | [Data Quality Rules](#11-data-quality-rules-in-force) | The eleven rules, and what each catches |
| 12 | [Known Gaps and Honest Limitations](#12-known-gaps-and-honest-limitations) | What UC7 does not do today |
| 13 | [Comparison With UC3 and UC6](#13-comparison-with-uc3-and-uc6) | Why the three use cases look so different |

---

## 1. What This Document Adds

A UC7 master document already exists at [UC7_MASTER_DOCUMENT.md](UC7_MASTER_DOCUMENT.md). It covers business context, architecture, ASN.1 decode internals and governance.

**This document deliberately does not repeat that material.** It adds the four things the master document does not contain:

| Addition | Why it was needed |
|---|---|
| **Source data analysis** | Actual row counts, key cardinality and null rates, measured on live tables |
| **Join analysis** | The master document mentions joins exactly once. This document tests them |
| **Exported data analysis** | What a downstream consumer would actually select, and how it reconciles |
| **A runnable SQL pack** | A single file anybody can open and execute, with expected answers stated |

**Every number here was measured, not estimated.** Where a result is uncomfortable, such as the zero-overlap join finding in Section 8, it is stated plainly rather than smoothed over.

---

## 2. The Use Case in Plain Terms

### 2.1 What a CDR is

- A **Call Data Record** is the receipt a mobile network writes every time a subscriber does something billable.
- Every call, text message and data session produces one.
- They are the raw material for billing, revenue assurance, fraud detection and network planning.

### 2.2 Why the files are difficult

| Property | Consequence |
|---|---|
| Encoded in **ASN.1 BER**, a binary standard | Cannot be opened in a text editor. Needs a schema-driven decoder |
| Each network element uses a **different ASN.1 schema** | Four schemas, four decoders. They are not interchangeable |
| Each record is a **CHOICE**, one of many shapes | You cannot assume a fixed column layout. See Section 6 |
| Fields are **TBCD-encoded** binary | Phone numbers are not readable digits until decoded |

### 2.3 What UC7 does today

- **In scope:** land the binary files, decode them against the correct ASN.1 schema, publish faithful bronze tables, quarantine anything that fails a quality rule.
- **Out of scope today:** silver conformance, gold aggregation, cross-element correlation, TBCD decoding to readable numbers.

**This is a deliberate scope decision, not an omission.** Bronze must mirror the source exactly. The moment you reshape data in bronze, you lose the ability to prove what the network actually sent.

---

## 3. The Four Tables and Their Purpose

### 3.1 The four network elements

| Network element | Bronze table | What it records | Business consumer |
|---|---|---|---|
| **EMSC** | `emsc_cdr_raw` | Voice calls and SMS through the mobile switching centre | Voice billing, call-quality analysis |
| **PSGW** | `psgw_cdr_raw` | Data sessions at the packet gateway (SGW and PGW) | Data billing, APN usage reporting |
| **SGSN** | `sgsn_cdr_raw` | Data sessions at the serving GPRS support node | Data billing, mobility and roaming analysis |
| **TAP 3.10** | `tap310_raw` | Inter-operator roaming settlement batches | Roaming settlement, partner reconciliation |

### 3.2 What is actually in each table today

| Table | Rows | Files | Quarantined | Decode errors | Columns (business + framework) |
|---|---|---|---|---|---|
| `sgsn_cdr_raw` | **175,048** | 1 | 0 | 0 | 27 (20 + 7) |
| `emsc_cdr_raw` | **353** | 3 | 0 | 0 | 16 (9 + 7) |
| `psgw_cdr_raw` | **93** | 3 | 0 | 0 | 27 (20 + 7) |
| `tap310_raw` | **4** | 4 | 0 | 0 | 16 (9 + 7) |
| **Total** | **175,498** | **11** | **0** | **0** | |

**Three observations that matter:**

- **SGSN dominates**, at 99.7 percent of all rows. Any performance tuning starts and ends there.
- **Zero quarantined and zero decode errors.** Every single record decoded cleanly against its schema.
- **TAP has four files but only four rows.** Each TAP file is one settlement batch containing many call events nested inside it. One file equals one row is correct behaviour for TAP.

### 3.3 The quarantine sibling tables

- Every business table has a `_quarantine` twin, for example `sgsn_cdr_raw_quarantine`.
- **Purpose:** hold records that failed a data-quality rule, instead of dropping them.
- **All four are currently empty**, which is the desired state.
- **Why they matter:** a record that fails a rule is diverted, never lost. You can always ask why it failed and reprocess it.

---

## 4. Step-by-Step Pipeline Walkthrough

This is the complete journey, from a binary file arriving on a volume to a row you can query.

| Step | What happens | Where it happens | How you verify it |
|---|---|---|---|
| **1** | Binary CDR files land on the Unity Catalog volume | `/Volumes/bt_digital_poc/staging/uc_7/raw/<ELEMENT>/` | List the volume folder |
| **2** | Auto Loader detects new files | Databricks Auto Loader | Query A3 shows distinct file paths |
| **3** | The matching ASN.1 schema is read | `/Volumes/bt_digital_poc/staging/uc_7/asn_schema/<ELEMENT>.asn1` | Schema path is in the onboarding JSON |
| **4** | Root PDU is auto-detected from the schema | ASN.1 decoder | `_choice` column is populated |
| **5** | Each record is BER-decoded into a struct | ASN.1 decoder, `asn1_codec: "ber"` | `_asn1_decode_error` stays NULL |
| **6** | Framework lineage columns are attached | FlowX ingestion engine | Seven `__framework_` columns appear |
| **7** | Data-quality rules are evaluated per row | FlowX DQ engine | Query F1 scorecard |
| **8** | Passing rows go to the business table | `bt_digital_poc.bronze.<table>` | Query A2 good_rows |
| **9** | Failing rows go to the quarantine twin | `bt_digital_poc.bronze.<table>_quarantine` | Query A2 quarantined_rows |

### 4.1 What makes step 4 unusual

- Most decoders require you to name the root PDU, the top-level record type, by hand.
- **UC7 auto-detects it** by introspecting the ASN.1 module.
- **Why this matters:** naming the wrong root PDU produces rows that decode without error but contain nonsense. Auto-detection removes an entire class of silent, invisible corruption.

### 4.2 Why there is no transformation step

- The onboarding specification contains **4 ingestion flows, 0 transformation flows, 0 reconciliation flows**.
- There is therefore **no join, no aggregation and no reconciliation** in the pipeline as built.
- Bronze is a faithful mirror. Section 8 explains what a silver layer would need to add.

---

## 5. Every Node in the Pipeline Explained

### 5.1 Node inventory

| Node | Type | Purpose | Is it queryable? |
|---|---|---|---|
| `emsc_cdr_raw` | Streaming table | Decoded EMSC voice and SMS records | Yes |
| `psgw_cdr_raw` | Streaming table | Decoded PSGW data-session records | Yes |
| `sgsn_cdr_raw` | Streaming table | Decoded SGSN data-session records | Yes |
| `tap310_raw` | Streaming table | Decoded TAP roaming settlement batches | Yes |
| `<table>_quarantine` (×4) | Streaming table | Records that failed a DQ rule | Yes |
| `__materialization_mat_*` (×8) | Internal | Databricks-managed backing storage | No. Ignore these |

### 5.2 Why you see `__materialization_mat_*` tables

- They appear in `information_schema` and confuse people on first look.
- **They are Databricks internals**, the physical storage behind each streaming table.
- **Never query them directly, and never grant access to them.** Use the logical table name.

### 5.3 The seven framework columns, and what each answers

| Column | The question it answers |
|---|---|
| `__framework_source_file_name` | Which physical file did this row come from? |
| `__framework_source_file_size` | How large was that file? |
| `__framework_source_file_modification_time` | When did that file last change on the volume? |
| `__framework_source_file_metadata_headers` | What other file metadata was captured? |
| `__framework_ingestion_timestamp_utc` | When did the framework write this row? |
| `__framework_pipeline_run_id` | Which pipeline update produced it? |
| `__framework_record_id` | What is this record's stable identifier? |

**Why this matters:** any row can be traced back to its file and its run **without joining to anything**. That is the audit answer, available directly on the row.

### 5.4 The five ASN.1 decoder columns

| Column | Purpose |
|---|---|
| `_choice` | Names which CHOICE variant was populated. **The most important column in the table** |
| `_asn1_decode_error` | NULL when decoding succeeded. Populated with the reason when it failed |
| `_asn1_record_index` | Position of this record inside its source file |
| `path` | Full volume path of the source file |
| `modificationTime` | File modification timestamp from the file system |

---

## 6. The CHOICE Structure, the Single Most Important Concept

### 6.1 The concept in four bullets

- A CDR file does **not** contain one record shape. It contains a stream of records.
- Each record is **one of** several possible shapes. ASN.1 calls this a **CHOICE**.
- The decoder creates **one struct column per possible shape**, and populates **exactly one** per row.
- The `_choice` column **names** the one that was populated. All the others are NULL, correctly.

### 6.2 What is actually present in the data

| Table | `_choice` value | Records | Share |
|---|---|---|---|
| `sgsn_cdr_raw` | `sgsnPDPRecord` | 175,048 | 100 percent |
| `psgw_cdr_raw` | `sGWRecord` | 68 | 73 percent |
| `psgw_cdr_raw` | `pGWRecord` | 25 | 27 percent |
| `emsc_cdr_raw` | `uMTSGSMPLMNCallDataRecord` | 350 | 99 percent |
| `emsc_cdr_raw` | `compositeCallDataRecord` | 3 | 1 percent |
| `tap310_raw` | `transferBatch` | 4 | 100 percent |

### 6.3 The three rules for writing SQL against CHOICE tables

| Rule | Correct | Wrong |
|---|---|---|
| **Always filter on `_choice`** | `WHERE _choice = 'sgsnPDPRecord'` | Selecting the struct with no filter |
| **Use COALESCE across variants** | `coalesce(sGWRecord.servedIMSI, pGWRecord.servedIMSI)` | `sGWRecord.servedIMSI` alone, which loses 27 percent of PSGW |
| **Expect NULLs in unused variants** | Treat NULL as "not this shape" | Reporting it as a data-quality defect |

**The single most common mistake:** querying `psgw_cdr_raw.sGWRecord.servedIMSI` without COALESCE, silently discarding the 25 `pGWRecord` rows. The query succeeds. The answer is wrong.

### 6.4 EMSC is nested four levels deep

- Path: `uMTSGSMPLMNCallDataRecord` → `callDataRecord` → `mSOriginating` → the field you want.
- `callDataRecord` has **its own nested `_choice`** with 19 possible arms, including `mSOriginating`, `mSTerminating`, `mSOriginatingSMSinMSC` and `locationServices`.
- **Practical consequence:** EMSC needs a dedicated silver flattening step more urgently than the other three elements.

---

## 7. Source Data Analysis

### 7.1 The candidate join keys

| Key | What it identifies | Stability | Present in |
|---|---|---|---|
| `servedIMSI` | The SIM card | **Stable.** The natural subscriber key | SGSN, PSGW |
| `servedMSISDN` | The phone number | Can be reassigned to another subscriber | SGSN, PSGW |
| `chargingID` | One data session (PDP context) | Unique per session | SGSN, PSGW |
| `recordOpeningTime` | When the session started | Used for time-window joins | SGSN, PSGW |

### 7.2 The TBCD encoding warning

- IMSI and MSISDN are stored as **TBCD-encoded binary**, surfaced as base64 text such as `MjSQAABxgPQ=`.
- **They are not readable phone numbers.**

| Consequence | Detail |
|---|---|
| **Joining works** | Equal subscribers produce equal encodings. Equality joins are valid |
| **Reading does not work** | No business user can interpret `MjSQAABxgPQ=` |
| **Silver must decode** | A TBCD decode step is required before any human-facing report |

### 7.3 SGSN key quality, measured

| Metric | Value | What it tells you |
|---|---|---|
| Total rows | 175,048 | The bulk of UC7 |
| IMSI not null | 175,048 | **Zero nulls.** The key is completely populated |
| Distinct IMSI | 106,384 | Number of unique subscribers |
| Distinct MSISDN | 106,384 | Matches IMSI exactly. One number per SIM in this sample |
| Distinct chargingID | 123,383 | Number of unique sessions |
| Average sessions per subscriber | **1.65** | A healthy many-to-one shape |

**What 1.65 sessions per subscriber means for a join:**

- A join on IMSI **alone** will **fan out**, producing more rows than either input.
- For a session-level join, use **IMSI plus chargingID** together.
- For a subscriber-level rollup, aggregate first, then join.

### 7.4 PSGW key quality, measured

| Metric | Value |
|---|---|
| Total rows | 93 |
| Distinct IMSI | **4** |
| Distinct MSISDN | 4 |
| Distinct chargingID | 52 |

**This is the finding that drives Section 8.** PSGW contains four subscribers. SGSN contains 106,384. They are different subscribers.

### 7.5 TAP roaming partners, measured

| Sender | Recipient | File sequence |
|---|---|---|
| `LTUOM` | `GBROR` | 07083 |
| `INDCC` | `GBROR` | 13246 |
| `ISR01` | `GBRME` | 17059 |
| `ISR01` | `GBRME` | 17056 |

- Four files, three sending operators, two receiving operators.
- **Business meaning:** these are inter-operator settlement files. `GBROR` and `GBRME` are the UK receiving operators.

---

## 8. Join Analysis, the Honest Answer

### 8.1 The headline finding

> **UC7 as built performs no joins. When you test the joins that a silver layer would use, they return zero rows on the current sample data, because SGSN and PSGW contain completely different subscribers.**

**This is a property of the sample data, not a defect in the design or the SQL.**

### 8.2 The measurements

| Test | SGSN distinct | PSGW distinct | Overlap |
|---|---|---|---|
| **IMSI overlap** | 106,384 | 4 | **0** |
| **chargingID overlap** | 123,383 | 52 | **0** |

- Two independent keys, both showing zero overlap, is **conclusive**.
- This is not a key-selection problem. It is a data-sample fact.

### 8.3 Why this matters so much

| If you do not know this | What happens |
|---|---|
| You build a correct silver join | It returns zero rows |
| You assume the SQL is broken | You spend days debugging correct code |
| You assume the pipeline is broken | You re-run ingestion repeatedly for no reason |

**Always run the overlap test before building a join.** Query D1 in the pack takes seconds and prevents this entirely.

### 8.4 The four join patterns, and their verdicts

| # | Pattern | Query | Verdict |
|---|---|---|---|
| **1** | SGSN inner join PSGW on IMSI + chargingID | D3 | **Structurally correct, returns 0 rows** on this data |
| **2** | SGSN left join PSGW, with match rate | D4 | **Works.** Reports 175,048 rows, 0 matched, 0.00 percent |
| **3** | SGSN self-join on IMSI | D5 | **Works and returns data.** 20 rows returned in testing |
| **4** | UNION ALL across elements | D6 | **Works and returns data.** The recommended pattern today |

### 8.5 Why LEFT JOIN is the correct diagnostic

- An **INNER JOIN returning nothing is ambiguous**. It could mean broken SQL, or no overlapping data.
- A **LEFT JOIN with a counted match rate removes the ambiguity**, because it proves the join executed and quantifies the miss.
- **Always diagnose with LEFT JOIN first**, then switch to INNER once you have confirmed overlap exists.

### 8.6 The recommended pattern today: UNION, not JOIN

When subscriber overlap is zero, a **UNION view** is more useful than a join:

| Property | Why it helps |
|---|---|
| Produces one unified session list across all elements | Exactly what a silver layer should publish |
| Each row keeps its `network_element` label | You never lose provenance |
| Returns data today | No dependency on overlapping samples arriving |
| Naturally extends | Adding a fifth element is one more UNION branch |

### 8.7 The joins that will work when real data arrives

| Join purpose | Keys to use | Why |
|---|---|---|
| **Session correlation** | `servedIMSI` + `chargingID` | Identifies one session for one subscriber. Avoids fan-out |
| **Subscriber rollup** | `servedIMSI` only, after aggregation | Aggregate to one row per subscriber first, then join |
| **Time-window correlation** | `servedIMSI` + `recordOpeningTime` range | For matching events that share no session id |
| **Roaming settlement** | TAP `sender` / `recipient` to network records | Requires TAP call events to be exploded first |

---

## 9. Target and Exported Data Analysis

### 9.1 Source-to-target reconciliation

| Table | Files consumed | Rows landed |
|---|---|---|
| `emsc_cdr_raw` | 3 | 353 |
| `psgw_cdr_raw` | 3 | 93 |
| `sgsn_cdr_raw` | 1 | 175,048 |
| `tap310_raw` | 4 | 4 |
| **Total** | **11** | **175,498** |

**How to read this as an auditor:**

- Count the files on the volume. It must equal 11.
- If a file is on the volume but absent from `__framework_source_file_name`, that is **silent data loss** and must be investigated.
- Query E2 gives this answer on one screen.

### 9.2 What a downstream consumer would export

Query E5 in the pack produces a flattened, export-ready extract from SGSN:

| Exported column | Source path | Business meaning |
|---|---|---|
| `imsi_base64` | `sgsnPDPRecord.servedIMSI` | Subscriber SIM identity, **still TBCD-encoded** |
| `msisdn_base64` | `sgsnPDPRecord.servedMSISDN` | Phone number, **still TBCD-encoded** |
| `charging_id` | `sgsnPDPRecord.chargingID` | Session identifier |
| `apn` | `sgsnPDPRecord.accessPointNameNI` | Access point name used |
| `session_start` | `sgsnPDPRecord.recordOpeningTime` | When the session began |
| `duration_secs` | `sgsnPDPRecord.duration` | Session length |
| `close_cause` | `sgsnPDPRecord.causeForRecClosing` | Why the session ended |
| `radio_access_type` | `sgsnPDPRecord.rATType` | 2G, 3G or 4G |
| `source_file` | `__framework_source_file_name` | Audit trail |
| `ingested_at` | `__framework_ingestion_timestamp_utc` | Audit trail |

**Two deliberate choices in that extract:**

- **Framework columns are kept.** An export without lineage cannot be audited later.
- **IMSI and MSISDN remain encoded.** Decoding them is a silver-layer responsibility, and doing it in an export would bypass PII controls.

### 9.3 There are no export files in UC7

| Question | Answer |
|---|---|
| Does UC7 write files out? | **No.** There is no sink flow in the specification |
| Where does the data go? | It stays in Unity Catalog tables, queried directly |
| How would a file export be added? | A transformation flow with `target_type: "sink"`, as UC6 does |

**Contrast with UC6**, which exports four physical files per run through a `pgp_zip` sink. UC7 has no such requirement today.

---

## 10. Testing SQL for Any User

**All queries live in [UC7_ANALYSIS_QUERIES.sql](UC7_ANALYSIS_QUERIES.sql).** Open it in a Databricks SQL editor, attach any warehouse, and run.

### 10.1 The five tests that matter most

| # | Query | Question it answers | Expected result |
|---|---|---|---|
| **1** | A2 | Did all the data arrive, and was any of it rejected? | 175,498 good rows, 0 quarantined |
| **2** | A4 | Did every record decode cleanly? | 0 decode errors in all four tables |
| **3** | B1 | Which record shapes are present? | Six distinct `_choice` values |
| **4** | D1 | Can SGSN and PSGW be joined? | Overlap 0. Joins return nothing |
| **5** | F1 | Overall health scorecard | Every line reads PASS |

### 10.2 The scorecard, for a demonstration

Query F1 combines five checks into a single go or no-go answer:

| Check | Expected |
|---|---|
| All four tables published | 4 |
| Zero quarantined records | 0 |
| Zero ASN.1 decode errors | 0 |
| Every row carries a source file | 0 nulls |
| Every SGSN row has an IMSI | 0 nulls |

### 10.3 How to interpret a difference

| Symptom | Likely cause | What to do |
|---|---|---|
| Row counts differ | The pipeline re-ran on different files | Normal. Check the **shape** of the answer, not the exact number |
| Quarantine count above zero | A DQ rule fired | Query the quarantine table. Read the failure reason |
| Decode errors above zero | Schema and binary disagree | **Stop.** Those rows must not be trusted |
| Zero rows from a join | Almost certainly zero overlap | Run D1 before assuming the SQL is wrong |

---

## 11. Data Quality Rules in Force

### 11.1 Eleven rules across four tables

| Table | Rule | What it catches |
|---|---|---|
| All four | `_asn1_decode_error IS NULL` | Records the decoder could not parse |
| All four | `_choice IS NOT NULL AND _choice <> ''` | Records where no CHOICE arm was selected |
| EMSC | `_choice <> 'uMTSGSMPLMNCallDataRecord' OR uMTSGSMPLMNCallDataRecord IS NOT NULL` | `_choice` claims an arm, but the struct is empty |
| SGSN | `_choice <> 'sgsnPDPRecord' OR sgsnPDPRecord IS NOT NULL` | Same check, SGSN arm |
| TAP | `_choice <> 'transferBatch' OR transferBatch IS NOT NULL` | Same check, TAP arm |

### 11.2 Why every action is `quarantine`, never `drop`

| Action | Behaviour | Why it was not chosen |
|---|---|---|
| `drop` | Row silently disappears | You can never explain what was lost |
| `fail` | Whole pipeline stops | One malformed record blocks 175,000 good ones |
| **`quarantine`** | **Row diverted to a twin table** | **Nothing is lost. Everything is explainable** |

### 11.3 The rule worth understanding

The third rule per table catches a subtle and dangerous condition:

- `_choice` says the record is a `sgsnPDPRecord`.
- But the `sgsnPDPRecord` struct is actually **NULL**.
- **Meaning:** the decoder identified the shape but failed to populate it.
- **Without this rule** the row would land looking valid, and every downstream query would silently skip it.

---

## 12. Known Gaps and Honest Limitations

| # | Gap | Impact | Recommended fix |
|---|---|---|---|
| **1** | **No silver or gold layer** | No conformed or aggregated view exists | Build silver flattening per element |
| **2** | **IMSI and MSISDN are TBCD-encoded** | No business user can read them | Add a TBCD decode function in silver |
| **3** | **Zero cross-element overlap in the sample** | Correlation joins return nothing | Obtain a matched sample covering the same subscribers |
| **4** | **No governance tags applied** | `information_schema.table_tags` returns 0 rows for UC7 | Add a `governance_tags` block to the specification |
| **5** | **TAP call events are not exploded** | Four rows hide many nested call events | Explode `transferBatch.callEventDetails` in silver |
| **6** | **EMSC nesting is four levels deep** | Difficult for analysts to query directly | Flatten in silver, prioritise this element |
| **7** | **No sink or file export** | Data can only be consumed in-place | Add a sink flow if a file interface is required |

### 12.1 On the governance-tag gap

- UC7's onboarding specification carries `governance_tags: {}`, an empty block.
- **Verified consequence:** `bt_digital_poc.information_schema.table_tags` returns **0 rows** for all four UC7 tables.
- **Contrast with UC6**, which applies 72 tags across 12 tables.
- **This is a real gap**, not a documentation oversight, and is the cheapest item on this list to close.

---

## 13. Comparison With UC3 and UC6

### 13.1 Three use cases, three different shapes

| Dimension | UC3 Excalibur | UC6 Flood Warning | UC7 CDR ASN.1 |
|---|---|---|---|
| **Source format** | Oracle CRM extracts | Pipe-delimited and gzip | **ASN.1 BER binary** |
| **Ingestion flows** | 3 | 6 | **4** |
| **Transformation flows** | Several | 10 | **0** |
| **Reconciliation flows** | Yes | 6 | **0** |
| **Joins in pipeline** | Yes | Yes, several | **None** |
| **CDC strategy** | SCD1 and SCD2 | APPEND and TRUNCATE_AND_LOAD | **APPEND only** |
| **File export** | No | **Yes, four files per run** | No |
| **Governance tags** | Yes | Yes, 72 tags | **None** |
| **Layers built** | Bronze, silver | Bronze, silver, gold | **Bronze only** |

### 13.2 Why UC7 looks so much simpler

- **It is not simpler. It is narrower by design.**
- The complexity sits in **one place**: decoding a binary standard with per-element schemas and nested CHOICE structures.
- UC3 and UC6 spend their complexity on **joins, CDC and business rules**. UC7 spends all of its complexity on **getting the bytes into a table correctly**.

### 13.3 What UC7 proves about the framework

| Capability proven | Evidence |
|---|---|
| ASN.1 BER decoding at scale | 175,048 records, zero errors |
| Root PDU auto-detection | No hand-configured PDU, correct results |
| Per-element schema handling | Four different schemas, four decoders |
| Quarantine without loss | Four quarantine tables, all empty, all ready |
| Complete lineage | Every row traces to a file and a run |

---

## Document Provenance

| Fact type | How it was obtained |
|---|---|
| Row counts, file counts, overlaps | Executed against `bt_digital_poc.bronze` on `hoonartek` |
| Column shapes and struct paths | Read from `information_schema.columns` and live struct expansion |
| DQ rules | Read from `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` |
| Join results | Executed. D4 and D5 confirmed running, D5 returned 20 rows |
| Governance tag gap | Confirmed by querying `information_schema.table_tags`, which returned 0 |

**Nothing in this document is estimated or assumed.** Where a figure could not be verified, the gap is stated in Section 12 rather than filled with a plausible number.
