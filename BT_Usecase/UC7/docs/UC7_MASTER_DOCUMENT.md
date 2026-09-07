<style>
:root { --aptos: Aptos, "Aptos Display", "Segoe UI Variable", "Segoe UI", system-ui, -apple-system, sans-serif; }
body, .md-typeset, .md-typeset table, .md-typeset h1, .md-typeset h2,
.md-typeset h3, .md-typeset h4, .md-typeset p, .md-typeset li { font-family: var(--aptos) !important; }
.md-typeset code, .md-typeset pre { font-family: "Cascadia Code", Consolas, "Courier New", monospace !important; }
.screenshot { border: 1px dashed #9aa0a6; background: #f6f7f9; padding: 14px 16px;
  margin: 12px 0; border-radius: 6px; color: #4a4f55; font-family: var(--aptos); }
.screenshot b { color: #1a1a1a; }
</style>

# UC7 CDR ASN.1 Decoders : Master Technical Walkthrough and Architecture Specification

**Use case:** UC7 — Telecom Call Detail Record (CDR) ASN.1 binary decoding into the Databricks Lakehouse
**Catalog:** `flowx`  **Target / profile:** `metaflow_v7`  **Framework:** FlowX metadata-driven ingestion, version `0.0.3`
**Job:** `001_lfj_uc7_cdr_asn` (ID `843342822766009`)  **Pipeline:** `001_ldp_uc7_cdr_asn` (ID `927a6e24-757f-495c-8a94-d807b74c4128`)
**Document owner:** Lead Databricks Solution Architect
**Prepared:** 2026-09-05
**Status:** Deployed and executed green end-to-end. Every figure in this document has been read back from the live workspace.

---

## 0.0 Typographic and Export Standard

- All exported documentation, PDFs, and slide representations of this document **must use "Aptos" as the primary font family**.
- Recommended fallback chain: `Aptos, "Aptos Display", "Segoe UI Variable", "Segoe UI", system-ui, sans-serif`.
- Code blocks, SQL, and JSON must be rendered in a monospaced family: `"Cascadia Code", Consolas, "Courier New", monospace`.
- Tables must remain in Aptos so that numeric columns align correctly in exported decks.
- Please do not substitute Calibri or Arial when exporting, as the table column widths in this document have been set against Aptos metrics.

---

## Index

| # | Section | What you will learn |
|---|---|---|
| 1.0 | [Executive Business Context and Solution Value](#10-executive-business-context-and-solution-value) | Why UC7 exists, who consumes it, which KPIs it serves |
| 2.0 | [As-Is Landscape vs Databricks To-Be Modernisation](#20-as-is-landscape-vs-databricks-to-be-modernisation) | Legacy mediation pain points and the Lakehouse answer |
| 3.0 | [End-to-End Data Pipeline Architecture and Storage Topology](#30-end-to-end-data-pipeline-architecture-and-storage-topology) | Exact paths, volumes, and timestamp traceability |
| 4.0 | [Metadata-Driven Framework and Onboarding JSON Configuration](#40-metadata-driven-framework-and-onboarding-json-configuration) | Every JSON attribute, and why it is set that way |
| 5.0 | [Databricks Orchestration and DLT Pipeline Execution](#50-databricks-orchestration-and-dlt-pipeline-execution) | Job tasks, compute, triggered vs continuous |
| 6.0 | [Step-by-Step DLT DAG and Data Layer Walkthrough](#60-step-by-step-dlt-dag-and-data-layer-walkthrough) | All 16 DAG nodes: table or view, and why |
| 7.0 | [ASN.1 Decode Logic and End-to-End Traceability](#70-asn1-decode-logic-and-end-to-end-traceability) | Byte-level decode walkthrough with real payloads |
| 8.0 | [Enterprise Governance, Security and Unity Catalog Controls](#80-enterprise-governance-security-and-unity-catalog-controls) | Namespace, PII masking, RBAC grants |
| 9.0 | [Data Validation, Auditability and DLT Event Log SQL Recipes](#90-data-validation-auditability-and-dlt-event-log-sql-recipes) | Copy-paste reconciliation queries |
| 10.0 | [Business User Testing and Acceptance Playbook](#100-business-user-testing-and-acceptance-playbook) | Non-technical validation guide with expected outputs |
| A | [Appendix A — Framework Defects Found](#appendix-a-framework-defects-found-by-this-build) | Five real defects this build surfaced and fixed |
| B | [Appendix B — Operational Runbook](#appendix-b-operational-runbook) | How to run it, and what to do when it breaks |
| C | [Appendix C — Databricks Reference Links](#appendix-c-databricks-reference-links) | Official documentation |

---

## 1.0 Executive Business Context and Solution Value

### 1.1 What a CDR is, in business terms

- A **CDR (Call Detail Record)** is the billing and usage receipt that telecom network equipment writes for every chargeable event.
- One record is produced for **every voice call, SMS, and data session** on the network.
- Network elements do not write CSV or JSON. They write **ASN.1 BER**, a compact binary standard defined by the ITU and 3GPP.
- ASN.1 is efficient on the wire but **completely opaque to SQL**. A `SELECT` cannot read it.
- UC7 exists to convert that binary into queryable Delta tables, without any manual decoding step.

### 1.2 Business objective of UC7

| # | Objective | Why it matters |
|---|---|---|
| 1 | Decode ASN.1 CDRs into queryable columns automatically | Removes dependency on legacy mediation vendors for basic data access |
| 2 | Preserve every single record | Under-counting CDRs is directly a revenue-leakage and regulatory issue |
| 3 | Keep full traceability from source file to row | Required for billing disputes and regulatory audit |
| 4 | Quarantine bad records rather than dropping them | A silently dropped CDR is unbilled revenue |
| 5 | Make onboarding a new network element a configuration change | New equipment should not require a code release |

### 1.3 Key stakeholders

| Stakeholder | Their interest in UC7 |
|---|---|
| **Revenue Assurance** | Confirming that every CDR generated by the network is accounted for in billing |
| **Billing and Mediation Operations** | Reducing dependency on the legacy mediation platform for data access |
| **Regulatory and Compliance Reporting** | Auditable, time-travellable record of usage data |
| **Roaming Settlement Team** | TAP 3.10 files exchanged with partner operators |
| **Network Engineering** | Cell-level and APN-level usage patterns |
| **Data Platform Engineering** | Owns the pipeline, the framework, and the SLAs |

### 1.4 Downstream consumers

| Consumer | Consumption pattern |
|---|---|
| **Power BI / Databricks SQL dashboards** | Direct SQL over the bronze tables, or over curated silver views once built |
| **Regulatory reporting extracts** | Scheduled SQL extracts with Delta time travel for point-in-time restatement |
| **Executive usage dashboards** | Aggregated volume and revenue-proxy metrics |
| **Revenue Assurance reconciliation** | Row counts compared against network element counters |
| **Data Science / churn and usage models** | Feature engineering over decoded session and call attributes |

### 1.5 Business impact

| Impact area | Before UC7 | After UC7 |
|---|---|---|
| **Latency to data availability** | Overnight batch mediation cycle, T+1 at best | Files decoded on arrival; run completes in about 5 minutes for the current volume |
| **Manual reconciliation** | Analysts manually tie mediation output to network counters in spreadsheets | Counts are reconciled from the pipeline's own event log with a SQL query (see 9.0) |
| **Trust in reporting** | Discrepancies discovered late, with no audit trail | Every row carries source file, record position, ingestion timestamp, and run ID |
| **Onboarding a new network element** | Vendor change request, weeks of lead time | Add one block to the onboarding JSON and re-run |
| **Handling a corrupt record** | Whole batch fails, or record silently vanishes | Record is quarantined with the error text; siblings load normally |

### 1.6 Key KPIs and operational metrics addressed

| KPI / Metric | How UC7 measures it | Current verified value |
|---|---|---|
| **Records decoded successfully** | `count(*)` across the four bronze tables | **175,498** |
| **Decode failure rate** | `count_if(_asn1_decode_error IS NOT NULL) / count(*)` | **0.00%** (0 of 175,498) |
| **Quarantined record count** | `count(*)` across the four quarantine tables | **0** |
| **Record completeness per file** | `max(_asn1_record_index) + 1` vs records loaded per file | Contiguous, no gaps |
| **Pipeline run duration** | Job run elapsed time | **302 seconds** (run `884721997952817`) |
| **Ingestion idempotency** | Row count stability across repeated runs | Stable at 175,048 for SGSN across three runs |
| **Sources onboarded** | Ingestion flows in the dataflow group | 4 of 6 (see 1.7) |

### 1.7 Scope statement — an important honesty note

- UC7 currently onboards **four** network elements: **EMSC, PSGW, SGSN, and TAP**.
- **SMSC and MMSC are deliberately out of scope.** Their raw files were examined and found to be **CSV text, not ASN.1**. There is no `SMSC.asn1` schema module available at all. Full evidence is in section 7.6.
- UC7 as built is a **bronze-layer decode product**. The `silver` and `gold` schemas exist in the catalog but hold **no UC7 objects yet**. Section 6.0 documents the bronze DAG as built, and the silver and gold layers as **designed but not yet implemented**. This distinction is maintained throughout so that nothing in this document reads as delivered when it is not.

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> UC7 executive summary tile row.
<b>UI location:</b> Databricks SQL → Dashboards → "UC7 CDR Decode Overview".
<b>Expected content:</b> Four large KPI tiles reading "175,498 Records Decoded", "0 Decode Errors", "0 Quarantined", "302s Last Run Duration", with a source-wise bar chart beneath.
</div>

---

## 2.0 As-Is Landscape vs Databricks To-Be Modernisation

### 2.1 Current As-Is architecture

**Flow of data today:**

```
Network Elements (EMSC, PSGW, SGSN, SMSC, MMSC)
        |
        |  FTP / SFTP push of binary ASN.1 files
        v
Landing server (plain filesystem)
        |
        |  Legacy mediation platform - proprietary ASN.1 decoders
        v
Flat file / CSV extracts
        |
        |  Overnight scheduled ETL (hand-written scripts)
        v
Staging database (RDBMS)
        |
        |  Manual spreadsheet reconciliation by analysts
        v
Reporting layer
```

**Step-by-step As-Is characteristics:**

- Source extraction happens by **FTP or SFTP file push** from each network element on its own schedule.
- Decoding is performed by a **proprietary mediation platform**, which is licensed per-stream and treated as a black box.
- Output is written as **flat files or CSV dumps**, losing the nested structure of the original CDR.
- ETL is a set of **hand-written scheduled scripts**, executed as an overnight batch window.
- Reconciliation between network counters and mediation output is a **manual spreadsheet exercise**.

### 2.2 As-Is pain points

| # | Pain point | Business consequence |
|---|---|---|
| 1 | **Latency** — overnight batch only | Usage data is T+1 at best; intra-day revenue assurance is impossible |
| 2 | **Batch run failures are all-or-nothing** | One malformed record can fail an entire night's load, requiring manual restart |
| 3 | **No data quality validation layer** | Bad records are either dropped silently or block the batch; no middle ground |
| 4 | **Brittle schema handling** | A new optional field from a network element upgrade breaks the fixed-width or fixed-column extract |
| 5 | **Silent record loss** | Concatenated multi-record files can be partially read with no error raised — this exact class of defect was found and fixed during this build (Appendix A) |
| 6 | **No lineage or audit trail** | Cannot answer "which file did this row come from, and when was it loaded?" |
| 7 | **Vendor lock-in on decoders** | Adding a network element requires a vendor change request |
| 8 | **High operational overhead** | Dedicated operations staff to babysit the nightly window |

### 2.3 To-Be Databricks Lakehouse architecture

```
Network Elements (EMSC, PSGW, SGSN, TAP)
        |
        |  file drop into a Unity Catalog managed Volume
        v
/Volumes/br_digital_poc/landing/uc_7/raw/<ELEMENT>/
        |
        |  Auto Loader (cloudFiles, binaryFile format) - incremental, checkpointed
        v
DLT streaming source node (one per element, pipeline-scoped)
        |
        |  ASN.1 decode via mapInPandas, schema derived from the .asn1 module
        v
_<table>_staged  (pipeline-scoped streaming table)
        |
        |  DLT expectations evaluated
        +-------------------------------+
        v                               v
br_digital_poc.bronze.<table>          br_digital_poc.bronze.<table>_quarantine
   (records that passed)          (records that failed, with reason)
        |
        |  [DESIGNED, NOT YET BUILT] conformance and business logic
        v
   silver -> gold -> Power BI / regulatory extracts
```

**Step-by-step To-Be characteristics:**

- **Unified ingestion** — Auto Loader watches the Volume and picks up new files automatically. No FTP polling script.
- **Incremental and exactly-once** — Auto Loader checkpoints which files it has consumed, so re-running never duplicates rows. Verified across three runs.
- **Schema derived from the standard itself** — the decoder reads the `.asn1` module and derives the Spark schema from it. A new optional field in the module becomes a new column; it does not break the load.
- **Built-in data quality** — DLT expectations route failures to a quarantine table instead of dropping them or failing the batch.
- **Governance by default** — every object lives under Unity Catalog's three-level namespace with lineage captured automatically.
- **Configuration-driven onboarding** — a new network element is a JSON block, not a code change.
- **Serverless compute** — no cluster to size, patch, or babysit.

### 2.4 Comparative analysis — As-Is vs Databricks To-Be

| Dimension | As-Is (legacy mediation and ETL) | Databricks To-Be (UC7 as built) |
|---|---|---|
| **Ingestion Mechanism** | FTP/SFTP push, then proprietary decoder, then CSV extract, then scheduled scripts | Auto Loader `cloudFiles` with `binaryFile` format directly against a UC Volume; ASN.1 decoded in-pipeline |
| **Decoding** | Black-box vendor platform, licensed per stream | Open `asn1tools` decode driven by the actual ITU/3GPP `.asn1` module in the framework |
| **Data Governance** | Filesystem permissions and database grants, no column-level control, no lineage | Unity Catalog three-level namespace, table and column tags, ABAC policies, automatic lineage |
| **Latency / SLA** | T+1 overnight batch window | Triggered run completes in about **5 minutes** at current volume; can move to continuous mode without code change |
| **CDC Handling** | Not applicable — full extract each cycle | `APPEND` strategy at bronze (immutable event data, correct for CDRs). Framework also supports SCD1, SCD2 and snapshot CDC for downstream layers |
| **Schema Evolution** | Manual change request; new field breaks the extract | Schema derived from the `.asn1` module; Auto Loader schema location tracks evolution |
| **Bad Record Handling** | Batch fails, or record silently dropped | Quarantined to a dedicated table with the error text; siblings unaffected |
| **Operational Overhead** | Dedicated operations staff for the nightly window | Serverless, self-healing retries, event log for observability |
| **Auditability** | None beyond file archives | Source file name, record index, ingestion timestamp, pipeline run ID on every row, plus Delta time travel |
| **Cost Model** | Per-stream decoder licence plus fixed infrastructure | Consumption-based serverless compute |
| **Onboarding a new element** | Vendor change request, weeks | JSON block plus re-run, same day |

### 2.5 Official Databricks documentation references

| Component used in UC7 | Official documentation |
|---|---|
| Lakeflow Declarative Pipelines (DLT) | https://docs.databricks.com/aws/en/dlt/ |
| DLT expectations and data quality | https://docs.databricks.com/aws/en/dlt/expectations |
| DLT event log reference | https://docs.databricks.com/aws/en/dlt/observability |
| Auto Loader | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/ |
| Auto Loader `binaryFile` and file formats | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options |
| Auto Loader schema inference and evolution | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema |
| Unity Catalog | https://docs.databricks.com/aws/en/data-governance/unity-catalog/ |
| Unity Catalog Volumes | https://docs.databricks.com/aws/en/volumes/ |
| Column masks and row filters | https://docs.databricks.com/aws/en/tables/row-and-column-filters |
| Attribute-based access control (ABAC) | https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/ |
| Delta Lake time travel | https://docs.databricks.com/aws/en/delta/history |
| Databricks Asset Bundles | https://docs.databricks.com/aws/en/dev-tools/bundles/ |
| Lakeflow Jobs | https://docs.databricks.com/aws/en/jobs/ |
| Serverless compute | https://docs.databricks.com/aws/en/compute/serverless/ |

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Side-by-side As-Is vs To-Be architecture comparison.
<b>UI location:</b> Architecture slide deck, slide 3.
<b>Expected content:</b> Left panel showing the legacy chain (network elements → FTP → mediation → CSV → RDBMS → manual Excel reconciliation) with red annotations at each bottleneck; right panel showing the Databricks chain (Volume → Auto Loader → DLT decode → bronze plus quarantine → UC governance) with green annotations.
</div>

---

## 3.0 End-to-End Data Pipeline Architecture and Storage Topology

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> High-level architectural diagram of the UC7 pipeline from source to consumption.
<b>UI location:</b> Architecture slide deck, slide 4 (master architecture).
<b>Expected content:</b> Left to right — six network element icons, the UC Volume landing zone, the Auto Loader boundary, the DLT pipeline box containing the four decode lanes, the bronze and quarantine table pair per lane, the Unity Catalog governance band across the bottom, and the consumption layer (Power BI, Databricks SQL, regulatory extracts) on the right. Overlay the job name <code>001_lfj_uc7_cdr_asn</code> and pipeline name <code>001_ldp_uc7_cdr_asn</code>.
</div>

### 3.1 Data profile — source datasets

| # | Source | Network function | Business event captured | Encoding | Files | Total size |
|---|---|---|---|---|---|---|
| 1 | **EMSC** | Enhanced Mobile Switching Centre | Voice calls, SMS in the switch | ASN.1 BER | 3 | 35.4 MB |
| 2 | **PSGW** | Packet Switched Gateway (PGW/SGW) | Data session charging | ASN.1 BER | 3 | 32 KB |
| 3 | **SGSN** | Serving GPRS Support Node | GPRS/data session charging | ASN.1 BER | 1 | 42.0 MB |
| 4 | **TAP** | Transferred Account Procedure 3.10 | Inbound/outbound roaming charges | ASN.1 BER | 4 | 1.7 MB |
| — | *MMSC* | *Multimedia Messaging Centre* | *MMS events* | *CSV — not ASN.1* | *1* | *73.7 KB* |
| — | *SMSC* | *Short Message Service Centre* | *SMS events* | *CSV — not ASN.1* | *1* | *681 B* |

### 3.2 Generation cadence and volume estimates

| Aspect | Observation from the current data set | Production planning note |
|---|---|---|
| **File arrival pattern** | Files are dropped per network element folder, no fixed cadence in this data set | In production, network elements typically close and push a CDR file every 15 minutes or on a size threshold |
| **Records per file** | Highly variable: 1 to **175,048** | The pipeline must handle both extremes; this drove the chunked decode design (Appendix A) |
| **Largest single file** | 42.0 MB / 175,048 records (SGSN) | A single production SGSN file can be far larger; the decode is now memory-bounded, not file-size-bounded |
| **Current total volume** | 112 MB across 28 objects | Bronze growth is roughly linear with record count |
| **Records currently loaded** | 175,498 | — |
| **Naming convention** | No consistent extension: `.raw`, `.fin`, and extensionless (TAP) | This is why **no file pattern filter is configured** — any glob would silently skip files |

### 3.3 Storage paths and locations

**3.3.1 External landing storage (Unity Catalog Volume)**

| Purpose | Path |
|---|---|
| **Landing root** | `/Volumes/br_digital_poc/landing/uc_7/` |
| Raw CDR files | `/Volumes/br_digital_poc/landing/uc_7/raw/<ELEMENT>/` |
| ASN.1 schema modules | `/Volumes/br_digital_poc/landing/uc_7/asn_schema/` |
| Auto Loader schema checkpoints | `/Volumes/br_digital_poc/landing/uc_7/_schemas/<table>/` |
| Reference output samples | `/Volumes/br_digital_poc/landing/uc_7/output_sample/` |
| Archive (currently empty) | `/Volumes/br_digital_poc/landing/uc_7/archive/` |
| **Backup copy of the whole tree** | `/Volumes/br_digital_poc/staging/uc_7/` |

- The Volume `br_digital_poc.landing.uc_7` is **MANAGED**, and its physical storage resolves to the workspace's account-managed S3 location.
- The Volume is **not declared as a bundle resource**, which means `bundle deploy` and `bundle destroy` cannot touch it. This is deliberate protection for source data.

**3.3.2 Unity Catalog object locations**

| Layer | Namespace | Status | Objects |
|---|---|---|---|
| **Landing** | `br_digital_poc.landing.uc_7` (Volume) | **Built** | Raw binary files, ASN.1 modules |
| **Bronze — published** | `br_digital_poc.bronze.*` | **Built** | 4 decoded tables + 4 quarantine tables |
| **Bronze — internal** | `br_digital_poc.bronze.__927a6e24_*` | **Built** | 8 pipeline-scoped nodes (see 6.0) |
| **Silver** | `br_digital_poc.silver.*` | **Not built** | Designed in 6.6; no UC7 objects exist |
| **Gold** | `br_digital_poc.gold.*` | **Not built** | Designed in 6.7; schema exists but is empty |
| **Control metadata** | `br_digital_poc.config.*` | **Built** | 9 framework control tables |
| **Observability** | `br_digital_poc.observability.app_logs` (Volume) | **Built** | Run telemetry as gzipped JSONL |

**3.3.3 Bronze table inventory — fully qualified**

| Source | Published table (`catalog.schema.table`) | Quarantine table |
|---|---|---|
| EMSC | `br_digital_poc.bronze.emsc_cdr_raw` | `br_digital_poc.bronze.emsc_cdr_raw_quarantine` |
| PSGW | `br_digital_poc.bronze.psgw_cdr_raw` | `br_digital_poc.bronze.psgw_cdr_raw_quarantine` |
| SGSN | `br_digital_poc.bronze.sgsn_cdr_raw` | `br_digital_poc.bronze.sgsn_cdr_raw_quarantine` |
| TAP | `br_digital_poc.bronze.tap310_raw` | `br_digital_poc.bronze.tap310_raw_quarantine` |

**Naming convention explained:**

- Network switch elements take the suffix **`_cdr_raw`**, matching the existing framework precedent.
- TAP is a **file-interchange format**, not a switch, so it follows the established `<source>_raw` pattern and carries its version: **`tap310_raw`**.
- Quarantine tables are always **`<target_table>_quarantine`**.
- **There is no environment suffix.** In this framework, environments are separated by **catalog**, not by schema name. There is no `bronze_dev` or `bronze_prod`.

### 3.4 Ingestion flow and timestamp traceability

**3.4.1 The timestamp chain**

A CDR carries its own event time inside the encoded payload. The platform adds its own timestamps as the record moves. Tracing all three is what makes billing disputes answerable.

| # | Stage | Timestamp | Source of the value |
|---|---|---|---|
| 1 | **Network event** | `source_event_timestamp` | Inside the decoded ASN.1 payload — for example `changeTime` in an SGSN traffic volume, or `fileCreationTimeStamp` in a TAP batch control header |
| 2 | **File lands on the Volume** | `landing_timestamp` | `modificationTime`, provided by Auto Loader from the object store |
| 3 | **Row written to bronze** | `bronze_ingested_timestamp` | `__framework_ingestion_timestamp_utc`, set by the framework |

**3.4.2 Traced example — one real SGSN record**

| Attribute | Value | Meaning |
|---|---|---|
| Source file | `PSHLD02_3699_03_20170815_094659_38723.fin` | File name encodes the network element and its close time |
| `_asn1_record_index` | `0` | The **first** of 175,048 records inside that one file |
| `_choice` | `sgsnPDPRecord` | The CDR type selected from the ASN.1 CHOICE |
| Event time (in payload) | `listOfTrafficVolumes[0].changeTime` | The network's own charging timestamp |
| `modificationTime` | File landing time on the Volume | When the file arrived |
| `__framework_ingestion_timestamp_utc` | Bronze write time | When the row was persisted |
| `__framework_pipeline_run_id` | Pipeline update ID | Which run produced this row |

**3.4.3 Why `_asn1_record_index` is essential**

- One CDR file holds **many** records. All rows from one file share the same file name and the same `modificationTime`.
- Without a record ordinal, those rows are **mutually indistinguishable** — you could not point at "the 41,000th record in that file" during a dispute.
- `_asn1_record_index` is a 0-based ordinal within the source file. It was **added by this build**; see Appendix A, defect 1.

**3.4.4 SQL to trace a record end to end**

```sql
SELECT
    __framework_source_file_name                AS source_file,
    _asn1_record_index                          AS record_position_in_file,
    _choice                                     AS cdr_type,
    __framework_source_file_modification_time   AS landing_timestamp,
    __framework_ingestion_timestamp_utc         AS bronze_ingested_timestamp,
    __framework_pipeline_run_id                 AS pipeline_run_id,
    _asn1_decode_error                          AS decode_error
FROM br_digital_poc.bronze.sgsn_cdr_raw
WHERE _asn1_record_index = 0
LIMIT 1;
```

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Unity Catalog Volume browser showing the UC7 landing tree.
<b>UI location:</b> Databricks → Catalog → <code>flowx</code> → <code>landing</code> → Volumes → <code>uc_7</code>.
<b>Expected content:</b> Expanded folder tree showing <code>raw/</code> with its six element subfolders, <code>asn_schema/</code> with the eight <code>.asn1</code> modules, <code>_schemas/</code>, <code>output_sample/</code> and <code>archive/</code>, with file sizes visible — notably the 42 MB SGSN <code>.fin</code> file.
</div>

### 3.5 Repo-side data assets and their provenance

The Volume tree in 3.3.1 is the runtime landing zone. A smaller set of assets is also held **in the
repository** under `BT_Usecase/UC7/data/`, for schema compilation, local decode tests and reproducible
fixtures. Every one of them is labelled below as either **[Customer-Provided]** — supplied by the
customer, never generated — or **[Simulated]** — produced by a generator in this repository.

| Repo path | Provenance | What it is |
|---|---|---|
| `BT_Usecase/UC7/data/asn_schema/EMSC.asn1` | **[Customer-Provided]** | Real telecom ASN.1 protocol module (module `MSC12A`), root PDU `CallDataRecord` |
| `BT_Usecase/UC7/data/asn_schema/GGSN.asn1` | **[Customer-Provided]** | Real ASN.1 module, byte-identical to `SGSN.asn1` (see 7.4.2) |
| `BT_Usecase/UC7/data/asn_schema/PSGW.asn1` | **[Customer-Provided]** | Real ASN.1 module (module `CDRF-R9`, superset of SGSN), root PDU `CallEventRecord` |
| `BT_Usecase/UC7/data/asn_schema/TAP.310.asn1` | **[Customer-Provided]** | Real ASN.1 module (module `TAP-0310`), root PDU `DataInterChange` |
| `BT_Usecase/UC7/data/asn_schema/TAP.311.asn1` | **[Customer-Provided]** | Real ASN.1 module for TAP 3.11; retained as the negative control that proved the 3.10 mapping (see 7.4.1) |
| `BT_Usecase/UC7/data/tap311_sample.ber` | **[Customer-Provided]** | Supplied sample TAP payload |
| `BT_Usecase/UC6/data/test_fixture/EE_2026-08-20-REQUEST_1OF1.customer_supplied.csv.gz.gpg` | **[Customer-Provided]** | Supplied GPG-encrypted gzipped CSV extract. **Relocated to UC6** — it is an Environment Agency artefact; no UC7 flow reads it. |
| `BT_Usecase/UC7/data/synthetic/emsc_synthetic.ber` | **[Simulated]** | Generated fixture — 10 concatenated BER records |
| `BT_Usecase/UC7/data/synthetic/ggsn_synthetic.ber` | **[Simulated]** | Generated fixture — 10 concatenated BER records |
| `BT_Usecase/UC7/data/synthetic/psgw_synthetic.ber` | **[Simulated]** | Generated fixture — 10 concatenated BER records |
| `BT_Usecase/UC7/data/synthetic/tap310_synthetic.ber` | **[Simulated]** | Generated fixture — 10 concatenated BER records |
| `BT_Usecase/UC7/data/synthetic/tap311_synthetic.ber` | **[Simulated]** | Generated fixture — 10 concatenated BER records |

**Why the distinction matters, stated explicitly:**

- The five `.asn1` modules are **[Customer-Provided]** and are **never** generated. No script in this
  repository emits them; `scripts/generate_synthetic_ber.py` **reads them as input**. They are the
  authoritative wire-format contract, and the correctness of every decoded column rests on them.
- The five `.ber` files under `synthetic/` are **[Simulated]**: `scripts/generate_synthetic_ber.py`
  writes each one as **10 BER-encoded records of the module's root PDU, concatenated back to back**
  with no length prefix, separator or terminator. Generation is **deterministic** — every value is
  derived from a seeded RNG plus the field path, so regenerating produces byte-identical files.
- `tap311_sample.ber` sits at `data/` root, **not** under `synthetic/`, precisely because it is
  **[Customer-Provided]** and must not be confused with the generated fixtures.
- **[Simulated]** assets are fixtures for decoder tests. **No production figure in this document —
  the 175,498 record count, the 0 decode errors, the per-source arm distributions in 7.4 — comes from
  a [Simulated] file.** Every one of those was read from **[Customer-Provided]** payloads landed on
  `/Volumes/br_digital_poc/landing/uc_7/raw/`.

---

## 4.0 Metadata-Driven Framework and Onboarding JSON Configuration

> **Active specification file:** `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json`
> — the single file that drives everything in this section. It declares dataflow group
> **`dfg_uc7_cdr_asn`** with **four** ASN.1 ingestion flows (EMSC, PSGW, SGSN, TAP 3.10), no
> transformation flows, no reconciliation flows, and one observability destination. Every attribute
> table in 4.3 is documented **against that file**, not against the framework's general capability.

### 4.1 The ingestion control mechanism

**Principle:** the pipeline contains **no source-specific code**. All source-specific behaviour is data, held in control tables, and read at runtime.

**Step-by-step control flow:**

1. An engineer authors an **onboarding JSON spec** describing each source and its target.
2. The **Config Onboarding job** validates the spec and writes it into control tables under `br_digital_poc.config`.
3. The **DLT pipeline** starts, reads its `dataflow.group.id` from its Spark configuration, and looks up its instructions.
4. The pipeline **builds its DAG dynamically** from those control rows — one ingestion lane per registered flow.
5. To add a network element, add a JSON block and re-run. **No Python change, no redeployment of pipeline logic.**

```
UC7_cdr_asn_bronze.json
        |
        |  Config Onboarding job (shared, parameterised)
        v
br_digital_poc.config.dataflow_group_spec        <- one row: the group
br_digital_poc.config.ingestion_flow_spec        <- four rows: one per source
br_digital_poc.config.observability_config       <- one row: the telemetry destination
br_digital_poc.config.onboarding_audit_log       <- audit trail of the onboarding itself
        |
        |  pipeline reads dataflow.group.id = dfg_uc7_cdr_asn
        v
DLT DAG built dynamically at pipeline start
```

**Verified control table contents for UC7:**

| Control table | Rows for `dfg_uc7_cdr_asn` | Content |
|---|---|---|
| `br_digital_poc.config.dataflow_group_spec` | 1 | `environment=metaflow_v7`, `catalog_name=flowx`, `is_active=true` |
| `br_digital_poc.config.ingestion_flow_spec` | 4 | One per source, each with `source_type=asn1` |
| `br_digital_poc.config.observability_config` | 1 | `dest-uc7-volume`, `DATABRICKS_VOLUME`, enabled |

### 4.2 Production onboarding JSON configuration

The live specification file is **`BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json`**. Below is the production-grade configuration. The spec carries exactly one author-comment key, the root-level `_about` header (use case, description, framework version, date, developer); any key beginning with an underscore is permitted and ignored by the engine, and `_about` is the only one used. It is abbreviated here for readability.

```json
{
  "$schema": "../../../onboarding_templates/onboarding_spec.schema.json",
  "_about": { "use_case": "UC7 - CDR ASN.1 decode to Bronze", "description": "...", "version": "FlowX 0.0.4", "date": "2026-09-05", "developer": "Madhan Raghu" },

  "dataflow_group_id": "dfg_uc7_cdr_asn",
  "pipeline_parameters": {},

  "ingestion_flows": [
    {
      "dataflow_id": "df_uc7_sgsn_cdr_ingest",
      "source_system": "sgsn_cdr_feed",
      "source_database": "uc_7",
      "source_table_name": "sgsn_call_event_record",
      "source_description": "UC_7 SGSN (serving GPRS support node) BER-encoded call event records, decoded against SGSN.asn1 (module CDRF-R9). Root PDU auto-detected as CallEventRecord; all observed records select sgsnPDPRecord. Concatenated-TLV and large: one 42MB .fin file holds 175,048 records.",
      "source_type": "asn1",

      "target_catalog": "{{catalog}}",
      "target_schema": "bronze",
      "target_table": "sgsn_cdr_raw",
      "target_type": "streaming_table",

      "source_config": {
        "path": "/Volumes/{{catalog}}/landing/uc_7/raw/SGSN/",
        "schema_location": "/Volumes/{{catalog}}/landing/uc_7/_schemas/sgsn_cdr_raw/",
        "capture_technical_metadata": true,
        "asn1_schema_path": "/Volumes/{{catalog}}/landing/uc_7/asn_schema/SGSN.asn1",
        "asn1_codec": "ber"
      },

      "target_config": {
        "cdc_load_strategy": "APPEND",
        "storage_format": "delta"
      },

      "dq_config": {
        "rules": [
          {
            "rule_id": "dq_sgsn_asn1_decode_ok",
            "expression": "_asn1_decode_error IS NULL",
            "action": "quarantine"
          },
          {
            "rule_id": "dq_sgsn_choice_arm_selected",
            "expression": "_choice IS NOT NULL AND _choice <> ''",
            "action": "quarantine"
          },
          {
            "rule_id": "dq_sgsn_arm_populated",
            "expression": "_choice <> 'sgsnPDPRecord' OR sgsnPDPRecord IS NOT NULL",
            "action": "quarantine"
          }
        ],
        "quarantine_table": "sgsn_cdr_raw_quarantine",
        "record_id_column": "_choice"
      },

      "governance_tags": {}
    }
  ],

  "transformation_flows": [],
  "reconciliation_flows": [],

  "observability": [
    {
      "id": "dest-uc7-volume",
      "enabled": true,
      "type": "DATABRICKS_VOLUME",
      "destination_config": {
        "volume_path": "/Volumes/{{catalog}}/observability/app_logs/",
        "compression": "GZIP",
        "file_format": "JSONL"
      }
    }
  ]
}
```

> **Note on scope fidelity:** the live UC7 spec contains **four** such blocks in `ingestion_flows` (EMSC, PSGW, SGSN, TAP). Only the SGSN block is reproduced above; the other three are structurally identical, differing in `dataflow_id`, `source_system`, `source_table_name`, `source_description`, `path`, `schema_location`, `asn1_schema_path`, `target_table`, `quarantine_table`, the DQ `rule_id` prefix, and the arm named in the third DQ rule. **PSGW is the one structural exception: it carries only two DQ rules**, omitting the arm-populated rule entirely, because it legitimately selects two different arms (see 4.3.5).

**4.2.1 The four flows, as declared in the live spec**

| `dataflow_id` | `source_system` | `source_table_name` | `source_config.path` | `asn1_schema_path` | `target_table` | `quarantine_table` | DQ rules |
|---|---|---|---|---|---|---|---|
| `df_uc7_emsc_cdr_ingest` | `emsc_cdr_feed` | `emsc_call_data_record` | `.../raw/EMSC/` | `.../asn_schema/EMSC.asn1` | `emsc_cdr_raw` | `emsc_cdr_raw_quarantine` | 3 — arm `uMTSGSMPLMNCallDataRecord` |
| `df_uc7_psgw_cdr_ingest` | `psgw_cdr_feed` | `psgw_call_event_record` | `.../raw/PSGW/` | `.../asn_schema/PSGW.asn1` | `psgw_cdr_raw` | `psgw_cdr_raw_quarantine` | **2** — no arm rule |
| `df_uc7_sgsn_cdr_ingest` | `sgsn_cdr_feed` | `sgsn_call_event_record` | `.../raw/SGSN/` | `.../asn_schema/SGSN.asn1` | `sgsn_cdr_raw` | `sgsn_cdr_raw_quarantine` | 3 — arm `sgsnPDPRecord` |
| `df_uc7_tap310_ingest` | `tap310_tap_feed` | `tap310_data_interchange` | `.../raw/TAP/` | `.../asn_schema/TAP.310.asn1` | `tap310_raw` | `tap310_raw_quarantine` | 3 — arm `transferBatch` |

- All four share `source_database: uc_7`, `source_type: asn1`, `target_catalog: {{catalog}}`, `target_schema: bronze`, `target_type: streaming_table`, `asn1_codec: ber`, `capture_technical_metadata: true`, `cdc_load_strategy: APPEND`, `storage_format: delta`, `record_id_column: _choice` and `governance_tags: {}`.
- **All four omit `asn1_pdu_name`** — every one relies on root-PDU auto-detection. See 4.3.3 and 7.3.

### 4.3 Attribute-by-attribute breakdown

**4.3.1 Root level**

| Attribute | Type | Value in UC7 | Why it is configured this way, and how the engine evaluates it |
|---|---|---|---|
| `$schema` | string | `../../../onboarding_templates/onboarding_spec.schema.json` | Editor/CI hook to the JSON schema. Consumed by IDEs and by the schema gate; **ignored by the engine at runtime**. Note that a spec must pass **both** gates — the JSON schema *and* `spec_validator.py` — because only the schema rejects unknown keys. |
| `_about` | object | `use_case`, `description`, `version`, `date`, `developer` | The spec's one author-comment key. Any key beginning with `_` is permitted and ignored by the engine; `_about` is the only one used. It carries a back-link to this document. |
| `dataflow_group_id` | string | `dfg_uc7_cdr_asn` | The join key between configuration and pipeline. The pipeline reads `dataflow.group.id` from its Spark config and selects exactly the control rows carrying this value. All four sources share one group so that one pipeline owns all four tables. |
| `pipeline_parameters` | object | `{}` | Empty because UC7 needs no runtime path parameters. When populated, `${param}` placeholders in paths are resolved **on every pipeline update**, unlike `{{catalog}}` which is resolved once at onboarding. |
| `ingestion_flows` | array of objects | 4 entries | Each entry becomes one ingestion lane in the DAG. The engine iterates this array to build nodes. |
| `transformation_flows` | array | `[]` | UC7 is a bronze decode product; no in-pipeline transformation is defined. |
| `reconciliation_flows` | array | `[]` | No in-pipeline reconciliation. Count reconciliation is done from the event log instead (section 9.0). |
| `observability` | array of objects | 1 destination | Upserted into `br_digital_poc.config.observability_config` and read at runtime by the observability task. Destinations are **not** configured in YAML. |

**4.3.1a `observability[0]` — the telemetry destination**

| Attribute | Type | Value in UC7 | Runtime effect |
|---|---|---|---|
| `id` | string | `dest-uc7-volume` | Primary key of the destination row in `br_digital_poc.config.observability_config`; re-onboarding upserts on it. |
| `enabled` | boolean | `true` | Gate. When `false` the row is still written but the `observability_export` task skips this destination. |
| `type` | string (enum) | `DATABRICKS_VOLUME` | Dispatch key selecting the exporter implementation. |
| `destination_config.volume_path` | string | `/Volumes/{{catalog}}/observability/app_logs/` | Where extracted event-log telemetry is written. `{{catalog}}` resolves at onboarding. |
| `destination_config.compression` | string (enum) | `GZIP` | Output files are gzipped. |
| `destination_config.file_format` | string (enum) | `JSONL` | One JSON object per line, so the export is appendable and streamable. |

**4.3.2 Flow identity and target mapping**

| Attribute | Type | Value (SGSN flow) | Why, and how the engine uses it |
|---|---|---|---|
| `dataflow_id` | string | `df_uc7_sgsn_cdr_ingest` | Unique identity of this lane. Appears in the event log and control tables; used to trace a specific lane's metrics. |
| `source_system` | string | `sgsn_cdr_feed` | Lineage label naming the originating system. Free text, carried into metadata. |
| `source_database` | string | `uc_7` | Logical grouping of the source. Free text. All four UC7 flows share this value. |
| `source_table_name` | string | `sgsn_call_event_record` | The logical name of the source entity, distinct from the physical target table. |
| `source_description` | string | Long text | Documentation carried into the control tables so the *why* travels with the config, not just the code. In UC7 each description records the module, the auto-detected root PDU and the observed arms. |
| `source_type` | string (enum) | `asn1` | **The dispatch key.** The engine selects its reader from this: `asn1` routes to the ASN.1 reader (Auto Loader `binaryFile` plus decode). Valid values are `autoloader`, `zerobus`, `asn1`. |
| `target_catalog` | string | `{{catalog}}` | Resolved to `flowx` at onboarding time from the job's `catalog` parameter. Using a placeholder keeps the spec portable across workspaces. |
| `target_schema` | string | `bronze` | The bronze layer. Deliberately **not** `bronze_dev` — environments separate by catalog. |
| `target_table` | string | `sgsn_cdr_raw` | Physical Delta table name. |
| `target_type` | string (enum) | `streaming_table` | Makes this an append-only incremental streaming table, the correct choice for immutable event data. Alternatives are `materialized_view`, `batch_table`, `external_sink`, `sink`. |

**4.3.3 `source_config` — the ASN.1 decode instructions**

| Attribute | Type | Value (SGSN flow) | Why, and how the engine evaluates it |
|---|---|---|---|
| `path` | string (required) | `/Volumes/br_digital_poc/landing/uc_7/raw/SGSN/` | The directory Auto Loader watches. Each source has its **own** path, which also avoids the framework's shared-path validation rule that requires identical retention settings when two flows read one directory. |
| `schema_location` | string (optional) | `/Volumes/.../_schemas/sgsn_cdr_raw/` | Auto Loader's checkpoint. **This is what makes re-runs idempotent** — it records which files have been consumed. Set explicitly on all four flows rather than relying on the framework default, so the checkpoint sits beside the data it belongs to. |
| `capture_technical_metadata` | boolean (optional) | `true` | Produces the `__framework_*` audit columns — source file name, size, modification time, ingestion timestamp, pipeline run ID, record ID. **This is what makes billing disputes answerable.** |
| `asn1_schema_path` | string (required for `source_type: asn1`) | `.../asn_schema/SGSN.asn1` | The ASN.1 module used to decode. This is the single most important attribute for correctness: the wrong module decodes to wrong data. The engine compiles this module and **derives the Spark output schema from it by introspection** — there is no second, hand-written schema description to drift. Each mapping was verified by decoding real bytes (section 7.0). |
| `asn1_codec` | string (enum) | `ber` | Basic Encoding Rules. Confirmed from the payloads: both definite and indefinite length forms are present, and **DER forbids indefinite length**, so BER is the correct and only valid choice here. Allowed values are strictly `ber` or `der`. |
| `asn1_pdu_name` | string (**OPTIONAL**) | **omitted on all four flows** | **Optional, not required — see the callout below.** When absent, blank or whitespace-only, `detect_root_pdu_name()` derives the root PDU from the module's own type-dependency graph. Verified to resolve `CallEventRecord` for SGSN. Omitting it means the standard is the source of truth, not a hand-typed string that can drift. |
| `file_pattern` | string (optional) | **omitted deliberately** | The raw folders hold `.raw`, `.fin`, and extensionless TAP files. Any glob would silently skip files, which is exactly the failure mode UC7 must avoid. |

> **Correctness callout — `asn1_pdu_name` is OPTIONAL, never mandatory.**
> Since framework **0.0.2** the root PDU no longer has to be supplied. The rule, as implemented in
> `src/flowx/lakeflow_framework/asn1/decoder.py` and enforced by `spec_validator.py`:
>
> - **Absent, `null`, `""` or whitespace-only → auto-detect.** `detect_root_pdu_name()` infers the
>   root as the one top-level `SEQUENCE`/`CHOICE` that no other type in the module references — the
>   entry point of the module's own type-dependency graph.
> - **Supplied → unconditional OVERRIDE.** A given name is used as-is and is **never second-guessed**;
>   it does not "hint" or "assist" detection, it replaces it entirely.
> - **An ambiguous module raises** rather than guessing, because a wrong root does not fail loudly —
>   it silently produces a full table of garbage columns.
> - **All four UC7 flows rely on auto-detection and omit the key**, which is the recommended posture
>   for a real telecom module. Validation trigger is on **presence**, not truthiness: the validator
>   only type-checks the value when the key is actually present.
>
> The JSON schema previously and **wrongly** marked `asn1_pdu_name` as required; that has been
> corrected. If you are working from an older copy of the schema or of this document that describes
> it as mandatory, that description is stale — the spec, the validator and the decoder all treat it
> as optional.

**4.3.4 `target_config` — write behaviour**

| Attribute | Type | Value (all four flows) | Why, and how the engine evaluates it |
|---|---|---|---|
| `cdc_load_strategy` | string (enum) | `APPEND` | CDRs are **immutable events** — a completed call is never updated. Append is semantically correct and requires no `primary_keys`. The engine dispatches to the append writer. Other strategies (`SCD1`, `SCD2`, `TRUNCATE_AND_LOAD`, `FULL_SNAPSHOT_CDC`) apply to mutable dimensions and belong in downstream layers. |
| `storage_format` | string (enum) | `delta` | Required for a `streaming_table`. `iceberg` is only permitted with `target_type: batch_table`. |
| `primary_keys` | array of strings (optional) | **not set** | Correctly absent: only required for SCD1, SCD2, SCD3 and snapshot CDC strategies. |
| `partition_columns` | array of strings (optional) | **not set** | At current volume (175k rows) partitioning would create small files and hurt performance. Revisit at multi-billion-row scale, likely on a date derived from the CDR event time. |

**4.3.5 `dq_config` — quality rules and quarantine**

| Attribute | Type | Value | Why, and how the engine evaluates it |
|---|---|---|---|
| `rules` | array of objects | 3 rules (SGSN, EMSC, TAP) / **2 rules (PSGW)** | Each entry becomes one DLT expectation on the `_<table>_staged` node. |
| `rules[].rule_id` | string | e.g. `dq_sgsn_asn1_decode_ok` | Names the expectation. This is the string that appears in the event log `flow_progress` `data_quality.expectations` array, so it is what Recipe 2 (9.3) filters on. Prefixed per source so the four lanes never collide. |
| `rules[].expression` | string (SQL) | see rows below | Evaluated as a **SQL boolean** against each staged row. `true` passes, `false` routes to quarantine. |
| `rules[].action` | string (enum) | `quarantine` on every rule | **Never `drop`, never `fail`.** A dropped CDR is unbilled revenue. A failed batch blocks the whole night. Quarantine keeps the record, keeps the reason, and lets siblings load. |
| `dq_*_asn1_decode_ok` | rule | `_asn1_decode_error IS NULL` | The universal check. Present on **all four** sources. |
| `dq_*_choice_arm_selected` | rule | `_choice IS NOT NULL AND _choice <> ''` | Present on **all four** sources. Guards a genuine hazard: `asn1tools` returns `(None, None)` rather than raising when a CHOICE matches no arm, which would otherwise write an all-NULL row indistinguishable from a valid empty record. |
| `dq_*_arm_populated` | rule | `_choice <> '<arm>' OR <arm> IS NOT NULL` | Present on **EMSC** (`uMTSGSMPLMNCallDataRecord`), **SGSN** (`sgsnPDPRecord`) and **TAP** (`transferBatch`); **absent on PSGW**. Confirms the selected arm actually decoded to a non-null struct. Written as an implication so it is vacuously true for other arms rather than false. |
| `quarantine_table` | string | `sgsn_cdr_raw_quarantine` | Name of the sibling table that receives failing rows. Materialised only because at least one rule uses `quarantine`. Always `<target_table>_quarantine` in UC7. |
| `record_id_column` | string | `_choice` | Stamped onto the quarantine row so a rejected record can be identified. Must be a **top-level** column present in `df.columns`. A dotted struct path is silently ignored by the quarantine writer, so a nested field would fail quietly. All four flows use `_choice`. |

> **Why the rules are deliberately thin:** in these ASN.1 modules **almost every member is `OPTIONAL`**. Asserting that any individual business field is non-null would false-quarantine perfectly valid records. The three rules above assert only what was **measured** to hold on 100% of decoded records. PSGW omits the third rule entirely because it legitimately selects two different arms (`sGWRecord` and `pGWRecord`).

**4.3.6 PII, masking and governance tags — current state**

| Attribute | Value in UC7 | Position |
|---|---|---|
| `governance_tags` | `{}` on all four flows | **No tags are applied today.** This is a known gap, not a design decision. |
| Masking policies | Not applied | Not applied at bronze. See 8.3 for the recommended implementation. |

> **Architect's note, stated plainly:** CDR data is **highly sensitive**. It contains IMSI and IMEI — subscriber and device identifiers that are personal data under GDPR and equivalent regimes. The decoded SGSN payload demonstrably contains `servedIMSI` and `servedIMEI`. UC7 as currently built applies **no tags and no masking**. Section 8.0 specifies the controls that **must** be applied before any non-privileged user is granted access to these tables. This is the single most important open action arising from this document.

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Onboarding JSON open in the FlowX Onboarding App spec builder.
<b>UI location:</b> Databricks Apps → FlowX Onboarding → Spec Builder → load <code>UC7_cdr_asn_bronze.json</code>.
<b>Expected content:</b> Left pane showing the four ingestion flows in a list; right pane showing the SGSN flow expanded with <code>source_type: asn1</code>, the <code>asn1_schema_path</code> field, and the three DQ rules with action <code>quarantine</code>.
</div>

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Control tables after onboarding.
<b>UI location:</b> Databricks SQL editor, querying <code>br_digital_poc.config.ingestion_flow_spec</code>.
<b>Expected content:</b> Result grid with four rows filtered on <code>dataflow_group_id = 'dfg_uc7_cdr_asn'</code>, showing <code>dataflow_id</code>, <code>source_type=asn1</code>, <code>target_schema=bronze</code>, and the four distinct <code>target_table</code> values.
</div>

---

## 5.0 Databricks Orchestration and DLT Pipeline Execution

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Databricks Workflows UI showing the UC7 job and its four tasks.
<b>UI location:</b> Databricks → Workflows → Jobs → <code>001_lfj_uc7_cdr_asn</code> → Tasks tab.
<b>Expected content:</b> Task DAG showing the linear chain <code>setup_control_tables</code> → <code>onboard_uc7</code> → <code>run_pipeline_update</code> → <code>observability_export</code>, all four green, with the run duration of 302s and run ID 884721997952817 visible in the run list below.
</div>

### 5.1 Job identity

| Property | Value |
|---|---|
| **Job name** | `001_lfj_uc7_cdr_asn` |
| **Job ID** | `843342822766009` |
| **Deployment kind** | `BUNDLE` (Databricks Asset Bundles) |
| **Defining file** | `resources/uc7/uc7_cdr_asn_job.yml` |
| **Framework wheel pinned** | `/Volumes/br_digital_poc/config/wheels/0.0.3/.internal/flowx-0.0.3-py3-none-any.whl` |
| **Environment version** | `4` |
| **Max concurrent runs** | 1 |

### 5.2 Task-by-task breakdown

| # | Task key | Type | Depends on | Purpose |
|---|---|---|---|---|
| 1 | `setup_control_tables` | Notebook | — | Creates or migrates the nine framework control tables in `br_digital_poc.config`. Idempotent. |
| 2 | `onboard_uc7` | **Run Job** | `setup_control_tables` | Delegates to the shared Config Onboarding job, passing the spec path, catalog, env and `CREATE`. |
| 3 | `run_pipeline_update` | Pipeline | `onboard_uc7` | Triggers an update of pipeline `001_ldp_uc7_cdr_asn`. |
| 4 | `observability_export` | Notebook | `run_pipeline_update` | Extracts the pipeline event log and dispatches OTel telemetry to the configured destination. |

**Design points worth stating:**

- **`setup_control_tables` is not boilerplate.** `bundle deploy` does **not** create or migrate control tables. Before this build, `br_digital_poc.config` was completely empty on this workspace. Wiring this as task 1 makes the job self-sufficient.
- **Onboarding is delegated, never inlined.** Task 2 is a `run_job_task` against the shared, parameterised onboarding job. Older jobs in this repository each pinned their own copy of the onboarding notebook; that pattern is legacy drift and is deliberately not copied here. One onboarding entry point for the whole bundle.
- **`pipeline_task_run_id` must be a task parameter.** The observability task receives `{{tasks.run_pipeline_update.run_id}}` as a task `base_parameter`, **not** a pipeline configuration key. Jobs dynamic values resolve per **job run**; pipeline configuration resolves per **pipeline update**, where no job run is in scope. A mistyped task key is passed through as literal text rather than failing at the platform layer, so the framework detects and names that case explicitly.

### 5.3 Upstream triggers and dependencies

| Aspect | Current state | Production recommendation |
|---|---|---|
| **Trigger** | Manual / on-demand (`Run now`) | File-arrival trigger on `/Volumes/br_digital_poc/landing/uc_7/raw/`, or a 15-minute schedule aligned to network element file close |
| **Upstream dependency** | None — the job is self-contained | Optionally gate on a network element push-completion signal |
| **Downstream dependency** | None yet | Once silver and gold are built, chain them as further tasks or a separate job |
| **Concurrency** | `max_concurrent_runs: 1` | Keep at 1. Auto Loader checkpoints are per-table; concurrent updates on one pipeline are not supported |

### 5.4 Compute configuration

| Property | Value | Rationale |
|---|---|---|
| **Compute type** | **Serverless** | No cluster to size, patch or babysit; matches the bursty file-arrival pattern |
| **Photon** | **Enabled** | Vectorised execution for the Delta write path |
| **Node topology** | Managed by serverless | Not a single-node/multi-node choice; Databricks manages the topology |
| **Autoscaling** | Managed by serverless | Scaling is automatic; no min/max worker tuning is exposed |
| **Environment version** | `4` | Required by the framework wheel |

> **One serverless constraint that materially shaped this build:** a Python UDF on serverless is capped at **1,024 MB**. The decode of the 42 MB / 175,048-record SGSN file initially exceeded this cap and failed the pipeline. The decoder now streams in bounded chunks, holding peak memory at about **42 MB**. Full detail is in Appendix A, defect 2. If UC7 is ever moved to classic compute, that constraint relaxes — but the chunked design should be retained regardless, because it makes memory independent of file size.

### 5.5 Pipeline execution parameters

| Property | Value | Rationale |
|---|---|---|
| **Pipeline name** | `001_ldp_uc7_cdr_asn` | — |
| **Pipeline ID** | `927a6e24-757f-495c-8a94-d807b74c4128` | — |
| **Defining file** | `resources/uc7/uc7_cdr_asn_pipeline.yml` | — |
| **Target catalog** | `flowx` | — |
| **Target schema** | `bronze` | — |
| **`dataflow.group.id`** | `dfg_uc7_cdr_asn` | The lookup key into the control tables |
| **`dataflow.control.catalog`** | `flowx` | Which catalog's `.config` schema to read control rows from |
| **Serverless** | `true` | — |
| **Photon** | `true` | — |
| **Development mode** | `true` | Faster iteration; **set to `false` for production** |
| **Continuous** | `false` — **Triggered** | See 5.6 |
| **Libraries** | `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` | The single generic engine notebook, shared by every pipeline in the framework |
| **Dependencies** | `../../dist/*.whl` | Rewritten at deploy time to the version-scoped wheel path |

### 5.6 Triggered vs continuous mode — the decision

| Criterion | Triggered (chosen) | Continuous (not chosen) |
|---|---|---|
| **Cost** | Compute runs only during the update, then stops | Compute runs permanently |
| **Fit to arrival pattern** | Matches batched file drops from network elements | Suits a constant event stream |
| **Latency** | Minutes, bounded by trigger frequency | Seconds |
| **Operational simplicity** | Clear start and end per run; easy to reconcile counts per run | Continuous state to monitor |
| **Verdict for UC7** | **Selected.** CDR files arrive in batches, not as a continuous stream. Triggered gives sufficient freshness at materially lower cost, and makes per-run reconciliation straightforward. | Revisit only if the business requires sub-minute CDR visibility. |

- Switching to continuous later requires **only** flipping `continuous: true` in the pipeline YAML. No change to the spec, the decode logic, or the tables.

### 5.7 Version pinning to framework 0.0.3

- Resource YAML references the relative glob **`../../dist/*.whl`**. No version string is hard-coded in any resource file.
- Databricks Asset Bundles rewrites that glob at deploy time to the uploaded artifact path under `<artifact_path>/.internal/`.
- `artifact_path` is version-scoped: `${var.wheels_root}/${var.framework_version}` resolves to `/Volumes/br_digital_poc/config/wheels/0.0.3`.
- The version is pinned in exactly **two** places, which must be bumped together: `pyproject.toml` and the `framework_version` variable in `databricks.yml`.
- **Verified on the deployed job:** the environment dependency reads `/Volumes/br_digital_poc/config/wheels/0.0.3/.internal/flowx-0.0.3-py3-none-any.whl`.

> **Operational hard rule:** never run `bundle deploy` while a pipeline or test wave is running. A deploy prunes superseded artifacts from `<artifact_path>/.internal/`, which kills an in-flight update with `ENVIRONMENT_PIP_INSTALL_ERROR`. Always use `--fail-on-active-runs`.

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Pipeline settings showing serverless, Photon and the group configuration.
<b>UI location:</b> Databricks → Pipelines → <code>001_ldp_uc7_cdr_asn</code> → Settings.
<b>Expected content:</b> Settings panel showing catalog <code>flowx</code>, target schema <code>bronze</code>, Serverless enabled, Photon enabled, Triggered mode, and the Configuration section listing <code>dataflow.group.id = dfg_uc7_cdr_asn</code> and <code>dataflow.control.catalog = flowx</code>.
</div>

---

## 6.0 Step-by-Step DLT DAG and Data Layer Walkthrough

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Delta Live Tables DAG showing all UC7 node dependencies.
<b>UI location:</b> Databricks → Pipelines → <code>001_ldp_uc7_cdr_asn</code> → Graph tab, after a successful update.
<b>Expected content:</b> Four parallel lanes. Each lane: a <code>_src___volumes_...__stream</code> node → a <code>_&lt;table&gt;_staged</code> node → branching to both <code>&lt;table&gt;</code> and <code>&lt;table&gt;_quarantine</code>. All 16 nodes green, with the SGSN lane showing 175,048 rows.
</div>

### 6.1 DAG topology — verified node inventory

The pipeline builds **16 nodes**, four identical lanes of four. Every node type below was read from `system.information_schema.tables`, not assumed.

| # | Node name | UC object type | Published? | Role |
|---|---|---|---|---|
| 1 | `_src___volumes_flowx_landing_uc_7_raw_emsc__5e807b99__stream` | `STREAMING_TABLE` | Pipeline-scoped | EMSC file reader |
| 2 | `_src___volumes_flowx_landing_uc_7_raw_psgw__c67361f1__stream` | `STREAMING_TABLE` | Pipeline-scoped | PSGW file reader |
| 3 | `_src___volumes_flowx_landing_uc_7_raw_sgsn__ee25e3cc__stream` | `STREAMING_TABLE` | Pipeline-scoped | SGSN file reader |
| 4 | `_src___volumes_flowx_landing_uc_7_raw_tap__e759a809__stream` | `STREAMING_TABLE` | Pipeline-scoped | TAP file reader |
| 5 | `_emsc_cdr_raw_staged` | `STREAMING_TABLE` | Pipeline-scoped | EMSC decoded, pre-DQ |
| 6 | `_psgw_cdr_raw_staged` | `STREAMING_TABLE` | Pipeline-scoped | PSGW decoded, pre-DQ |
| 7 | `_sgsn_cdr_raw_staged` | `STREAMING_TABLE` | Pipeline-scoped | SGSN decoded, pre-DQ |
| 8 | `_tap310_raw_staged` | `STREAMING_TABLE` | Pipeline-scoped | TAP decoded, pre-DQ |
| 9 | `emsc_cdr_raw` | `STREAMING_TABLE` | **Published** | EMSC records that passed DQ |
| 10 | `psgw_cdr_raw` | `STREAMING_TABLE` | **Published** | PSGW records that passed DQ |
| 11 | `sgsn_cdr_raw` | `STREAMING_TABLE` | **Published** | SGSN records that passed DQ |
| 12 | `tap310_raw` | `STREAMING_TABLE` | **Published** | TAP records that passed DQ |
| 13 | `emsc_cdr_raw_quarantine` | `STREAMING_TABLE` | **Published** | EMSC records that failed DQ |
| 14 | `psgw_cdr_raw_quarantine` | `STREAMING_TABLE` | **Published** | PSGW records that failed DQ |
| 15 | `sgsn_cdr_raw_quarantine` | `STREAMING_TABLE` | **Published** | SGSN records that failed DQ |
| 16 | `tap310_raw_quarantine` | `STREAMING_TABLE` | **Published** | TAP records that failed DQ |

### 6.2 Table vs View — the differentiation, stated explicitly

**Finding: every one of the 16 UC7 nodes is a physical, storage-persisted `STREAMING_TABLE`. There are no `LIVE VIEW` nodes in the UC7 DAG.**

| Node group | Object type | Storage persisted? | Visible to analysts? | Why this choice |
|---|---|---|---|---|
| **Source readers** (nodes 1-4) | Streaming table, pipeline-scoped | **Yes** | No | This is the **read-once boundary**. A view cannot serve here: a view is inlined into *each* consumer, so "declared once" would not be "read once" — two consumers would mean two physical reads of the same files. Only materialisation guarantees one external read per source. |
| **Staged decode** (nodes 5-8) | Streaming table, pipeline-scoped | **Yes** | No | Holds the decoded rows *before* DQ evaluation, so the same decoded set can feed **both** the pass branch and the quarantine branch without decoding twice. Decoding 175,048 records is expensive; doing it twice would be wasteful and could produce inconsistent results. |
| **Published bronze** (nodes 9-12) | Streaming table | **Yes** | **Yes** | The consumable data product. Must be queryable, time-travellable and grantable. |
| **Quarantine** (nodes 13-16) | Streaming table | **Yes** | **Yes** | Failed records must be **inspectable** by operations staff, which requires persistence. |

**How to tell a pipeline-scoped node from a published one:**

- Pipeline-scoped nodes are physically named with the pipeline ID as a prefix, for example `__927a6e24_757f_495c_8a94_d807b74c4128__sgsn_cdr_raw_staged`.
- They are internal implementation detail. **Do not query them and do not grant on them** — they are not part of the contract and their names change if the pipeline is recreated.
- Published nodes carry their plain names: `br_digital_poc.bronze.sgsn_cdr_raw`.

**Where a `LIVE VIEW` would be correct instead:** in the silver layer (6.6), a lightweight column rename or filter that needs no independent analytical exposure should be a `@dlt.view`, precisely to avoid creating a throwaway persistent table.

### 6.3 Layer 1 — Streaming Bronze Ingestion

**Step-by-step:**

1. Auto Loader is configured with `cloudFiles.format = binaryFile` — the **entire file** becomes a single row with a `content` column of bytes.
2. `cloudFiles.schemaLocation` points at `/Volumes/br_digital_poc/landing/uc_7/_schemas/<table>/`, which records consumed files and makes re-runs idempotent.
3. No `pathGlobFilter` is applied, so every file in the directory is picked up regardless of extension.
4. Auto Loader's `_metadata` pseudo-column is materialised into a real column so that file name, size and modification time survive the `mapInPandas` boundary.
5. The ASN.1 decode transform runs (section 7.0), expanding one file row into **one row per CDR record**.
6. `capture_technical_metadata: true` attaches the `__framework_*` audit columns.

**On schema inference and rescue:**

- UC7 does **not** rely on Auto Loader's JSON/CSV schema inference, because a binary file has no inferable schema. The output schema is **derived from the ASN.1 module** by introspecting its type definitions.
- Consequently `_rescued_data` is **not** the operative safety net here. The equivalent safety net is `_asn1_decode_error` plus the quarantine table.
- `schema_evolution_mode` is not set. When a network element upgrade adds an OPTIONAL member to the `.asn1` module, the derived schema gains a column on the next run.

### 6.4 Layer 2 — DQ evaluation and quarantine branch

**Step-by-step:**

1. The `_<table>_staged` node holds every decoded record, passed and failed alike.
2. DLT expectations from `dq_config.rules` are evaluated against each row.
3. Rows satisfying **all** rules flow to the published bronze table.
4. Rows failing **any** rule flow to the quarantine table, carrying the failure reason.
5. Neither branch fails the pipeline. The update completes.

**On the three DLT expectation flavours:**

| DLT construct | Effect | Used in UC7? |
|---|---|---|
| `expect` | Records the violation as a metric, keeps the row | Not used — a warning alone would let bad records into bronze |
| `expect_or_drop` | Drops the violating row | **Deliberately not used** — a dropped CDR is unbilled revenue with no audit trail |
| `expect_or_fail` | Fails the whole update | **Deliberately not used** — one corrupt record must not block an entire load |
| **Quarantine (framework pattern)** | Routes the violating row to a separate table with its reason | **Used on every rule.** Preserves the record, the reason, and the run |

### 6.5 Bronze metadata dictionary

**6.5.1 Table-level metadata**

| Table | Type | Business key | Partition strategy | Business description |
|---|---|---|---|---|
| `br_digital_poc.bronze.emsc_cdr_raw` | Streaming Table | `(__framework_source_file_name, _asn1_record_index)` | None | Voice and in-switch SMS CDRs from the Enhanced MSC, decoded from `EMSC.asn1` |
| `br_digital_poc.bronze.psgw_cdr_raw` | Streaming Table | `(__framework_source_file_name, _asn1_record_index)` | None | Data-session charging records from the Packet Switched Gateway (PGW and SGW arms) |
| `br_digital_poc.bronze.sgsn_cdr_raw` | Streaming Table | `(__framework_source_file_name, _asn1_record_index)` | None | GPRS/data session charging records from the Serving GPRS Support Node |
| `br_digital_poc.bronze.tap310_raw` | Streaming Table | `(__framework_source_file_name, _asn1_record_index)` | None | TAP 3.10 roaming settlement batches exchanged with partner operators |
| `*_quarantine` (×4) | Streaming Table | Same | None | Records that failed one or more DQ rules, with the reason retained |

> **On "business key":** bronze has **no declared primary key** — `APPEND` does not require one, and the framework does not enforce one. The composite shown above is the **de facto** unique identifier and is what you should use to deduplicate or trace. `_asn1_record_index` alone is not unique; it is only unique **within** a source file.

> **On partitioning:** deliberately none. At 175,498 rows, partitioning would create small files and degrade scan performance. At production scale, partition or liquid-cluster on a date column derived from the CDR's own event time — not on ingestion date, which would scatter a single call's records if a file is re-processed.

**6.5.2 Column-level metadata — common to all four tables**

| Column | Type | Origin | Business meaning |
|---|---|---|---|
| `path` | string | Auto Loader | Full Volume path of the source file |
| `modificationTime` | timestamp | Auto Loader | When the file landed (the `landing_timestamp`) |
| `length` | bigint | Auto Loader | Source file size in bytes |
| `_metadata` | struct | Auto Loader | File metadata struct |
| `_choice` | string | ASN.1 decoder | **Which CDR type this row is** — the selected CHOICE arm |
| `<arm name>` | struct | ASN.1 decoder | One nullable struct column **per CHOICE arm**; only the selected arm is populated |
| `_asn1_decode_error` | string | ASN.1 decoder | NULL on success; otherwise `ExceptionType: message` |
| `_asn1_record_index` | bigint | ASN.1 decoder | **0-based position of this record within its source file** |
| `__framework_source_file_name` | string | Framework | Source file name |
| `__framework_source_file_size` | bigint | Framework | Source file size |
| `__framework_source_file_modification_time` | timestamp | Framework | Source file modification time |
| `__framework_source_file_metadata_headers` | map | Framework | Additional file metadata |
| `__framework_ingestion_timestamp_utc` | timestamp | Framework | **When the row was written to bronze** |
| `__framework_pipeline_run_id` | string | Framework | Pipeline update that produced this row |
| `__framework_record_id` | string | Framework | Framework-generated record identity |

**6.5.3 CHOICE arm columns actually observed**

| Table | Arm column | Rows | Meaning |
|---|---|---|---|
| `emsc_cdr_raw` | `uMTSGSMPLMNCallDataRecord` | 350 | Standard UMTS/GSM PLMN call record |
| `emsc_cdr_raw` | `compositeCallDataRecord` | 3 | Composite record (one per file) |
| `psgw_cdr_raw` | `sGWRecord` | 68 | Serving Gateway data record |
| `psgw_cdr_raw` | `pGWRecord` | 25 | PDN Gateway data record |
| `sgsn_cdr_raw` | `sgsnPDPRecord` | 175,048 | SGSN PDP context record |
| `tap310_raw` | `transferBatch` | 4 | TAP transfer batch (each file is one batch) |

### 6.6 Layer 3 — Silver Cleaning and Conformance (DESIGNED, NOT YET BUILT)

> **Status: not implemented.** `br_digital_poc.silver` holds no UC7 objects. This subsection is the design specification for the next phase, and is clearly marked as such so it is not mistaken for delivered scope.

**Recommended silver design:**

| Proposed object | Type | Purpose |
|---|---|---|
| `br_digital_poc.silver.cdr_unified` | Streaming Table | Flatten the nested CHOICE arms into one conformed CDR row shape across all four sources |
| `br_digital_poc.silver.v_cdr_voice` | **Live View** | Voice-only projection — a view, not a table, because it needs no independent storage |
| `br_digital_poc.silver.v_cdr_data` | **Live View** | Data-session-only projection |
| `br_digital_poc.silver.cdr_roaming` | Streaming Table | TAP batches exploded to one row per `CallEventDetail` |

**Conformance work that belongs in silver:**

- Decode the binary-encoded IMSI, IMEI and TBCD-string fields into readable digit strings.
- Normalise the several ASN.1 timestamp encodings into proper Spark `TIMESTAMP` values.
- Explode `listOfTrafficVolumes` into one row per volume container.
- Unify `sGWRecord` / `pGWRecord` / `sgsnPDPRecord` into a single conformed data-session shape.
- **Apply PII masking here** (see 8.3) so that silver is the first layer any analyst may touch.

**Where the stricter expectations belong:**

- Bronze's expectations are deliberately thin because ASN.1 members are OPTIONAL. Silver, having conformed the data, can afford real business assertions:

```python
@dlt.expect_or_drop("valid_imsi", "imsi IS NOT NULL AND length(imsi) BETWEEN 14 AND 15")
@dlt.expect_or_drop("non_negative_duration", "call_duration_seconds >= 0")
@dlt.expect("event_time_plausible", "event_timestamp > '2000-01-01'")
```

### 6.7 Layer 4 — Gold Aggregation and Consumption (DESIGNED, NOT YET BUILT)

> **Status: not implemented.** The `br_digital_poc.gold` schema exists and is **empty**.

| Proposed object | Type | Business metric |
|---|---|---|
| `br_digital_poc.gold.daily_usage_by_subscriber` | Materialized View | Calls, SMS, data volume per subscriber per day |
| `br_digital_poc.gold.daily_network_element_volume` | Materialized View | Record counts per element per day — the Revenue Assurance reconciliation feed |
| `br_digital_poc.gold.roaming_settlement_summary` | Materialized View | Roaming charges by partner operator and settlement period |
| `br_digital_poc.gold.apn_usage_summary` | Materialized View | Data volume by APN and cell |

- Gold objects should be **Materialized Views**, not streaming tables: they are aggregates, they are recomputed, and they must serve BI concurrency.
- Gold is the correct grant boundary for Business Analysts. It contains aggregates, not raw IMSI.

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Unity Catalog lineage graph for a bronze table.
<b>UI location:</b> Databricks → Catalog → <code>flowx</code> → <code>bronze</code> → <code>sgsn_cdr_raw</code> → Lineage tab.
<b>Expected content:</b> Upstream lineage showing the Volume path <code>/Volumes/br_digital_poc/landing/uc_7/raw/SGSN/</code> and the pipeline-scoped staged node; downstream currently empty, ready to show silver once built.
</div>

---

## 7.0 ASN.1 Decode Logic and End-to-End Traceability

### 7.1 What ASN.1 is, and why decoding is non-trivial

- **ASN.1 (Abstract Syntax Notation One)** is an ITU-T standard for describing data structures, used throughout telecom for CDRs.
- **BER (Basic Encoding Rules)** encodes each value as a **TLV triplet**: a **T**ag saying what the field is, a **L**ength, and a **V**alue.
- A `.asn1` module file is effectively a **dictionary**: it maps tag numbers to field names and types. Without the right module, the bytes are meaningless.
- The same byte sequence decoded with the wrong module produces **plausible but wrong** data — which is far more dangerous than an outright error.

### 7.2 The decode pipeline, step by step

```
Step 1  Auto Loader reads the WHOLE FILE as one row
        content = <42,000,000 bytes of binary>

Step 2  iter_ber_tlv_records() walks the top-level TLV headers
        -> yields 175,048 separate record byte-strings

Step 3  For EACH record, asn1tools decodes against SGSN.asn1
        compiled.decode('CallEventRecord', record_bytes)
        -> ('sgsnPDPRecord', { ...nested dict... })

Step 4  The (arm, value) tuple is spread across arm columns
        _choice = 'sgsnPDPRecord'
        sgsnPDPRecord = <struct>
        every other arm column = NULL

Step 5  _asn1_record_index is stamped with the record's ordinal
        Rows are yielded in bounded chunks of 10,000

Step 6  DLT expectations route the row to bronze or quarantine
```

### 7.3 Root PDU auto-detection

- `asn1_pdu_name` is **deliberately omitted** from the spec.
- The framework calls `detect_root_pdu_name()`, which parses the module and identifies the root by its dependency structure. In these modules the root also carries the marker annotation `--snacc isMetadata:"TRUE"--`.
- **Verified results:**

| Module | Detected root PDU | ASN.1 type |
|---|---|---|
| `EMSC.asn1` (module `MSC12A`) | `CallDataRecord` | CHOICE |
| `PSGW.asn1` (module `CDRF-R9`) | `CallEventRecord` | CHOICE |
| `SGSN.asn1` (module `CDRF-R9`) | `CallEventRecord` | CHOICE |
| `TAP.310.asn1` (module `TAP-0310`) | `DataInterChange` | CHOICE |

- Because every root is a **CHOICE**, the decoder emits one nullable column per arm **plus** the `_choice` discriminator naming the selected arm. This is why the tables have arm-named struct columns.

### 7.4 How each schema mapping was verified — not guessed

**The rule applied: never map a schema from its file name.** Every mapping was proven by decoding real production bytes.

| Source | Schema chosen | Records decoded | Failures | Arms observed |
|---|---|---|---|---|
| EMSC | `EMSC.asn1` | **137 / 137** | 0 | `uMTSGSMPLMNCallDataRecord` ×136, `compositeCallDataRecord` ×1 |
| PSGW | `PSGW.asn1` | **91 / 91** | 0 | `sGWRecord` ×68, `pGWRecord` ×23 |
| SGSN | `SGSN.asn1` | **175,048 / 175,048** | 0 | `sgsnPDPRecord` ×175,048 |
| TAP | `TAP.310.asn1` | **3 files / 3** | 0 | `transferBatch` |

**7.4.1 The TAP version question — resolved from the data, not the file name**

`TAP.310.asn1` and `TAP.311.asn1` share the same outer envelope, so **both** decode the payload successfully. A trial decode alone cannot distinguish them. The version was therefore read out of the encoded bytes:

| BER tag | Schema type it maps to | Decoded value |
|---|---|---|
| `[APPLICATION 201]` | `SpecificationVersionNumber` | **3** |
| `[APPLICATION 189]` | `ReleaseVersionNumber` | **10** |
| `[APPLICATION 196]` | `Sender` | `INDCC` |
| `[APPLICATION 182]` | `Recipient` | `GBROR` |
| `[APPLICATION 109]` | `FileSequenceNumber` | `13246` |

- Specification version 3, release version 10 → **TAP 3.10** → `TAP.310.asn1` is correct.
- The two modules are **not** interchangeable: TAP 3.11 **removes** the `valueAddedService` arm from the `CallEventDetail` CHOICE. Decoding 3.10 data with the 3.11 module would misinterpret that arm.

**7.4.2 Duplicate and near-duplicate schema files**

| MD5 | Files | Note |
|---|---|---|
| `8acb26e323c110770bb84034dfed4475` | `SGSN.asn1`, `GGSN.asn1` | **Byte-identical** |
| `e8d3d2605c78da54afc0401c4b6a2024` | `TAP.311.asn1`, `TAP.311.MVR.asn1` | **Byte-identical** |
| `809b83b5d293c749a6eee3d19d4f40ce` | `PSGW.asn1` | SGSN plus `RANSecondaryRATUsageReport` and extended-bitrate QoS members |
| `781be834ca203b5d3bf64fd4712db48d` | `EMSC.asn1` | — |
| `5165a2b87cf168bb6927d4ffb15fcbd2` | `TAP.310.asn1` | — |
| `901bdd44a2b20388fdbe5eb1d17cb117` | `MMS.asn1` | Module `RadiusMMS`, root `TopLevel` — models RADIUS attributes, **not** MMSC CDRs |

- PSGW and SGSN share the module name `CDRF-R9` and the same root PDU, but are kept as **separate files**, each source pointing at its own. PSGW's module is a **superset**: pointing SGSN at `PSGW.asn1` would also work, but pointing PSGW at `SGSN.asn1` would **fail** on records carrying the extended members.

### 7.5 Raw payload to bronze row — a worked example

**Step 1 — the raw bytes as they arrive (first 12 bytes of the SGSN file):**

```
b4 81 f8 80 01 12 83 08 32 34 40 21
```

- `b4` — context-specific, constructed, tag 20 → this is the `sgsnPDPRecord` arm of `CallEventRecord`.
- `81 f8` — long-form length: the next 248 bytes are this record.
- `80 01 12` — tag 0, length 1, value `0x12` = **18** → `recordType = 18`.
- `83 08 32 34 40 21 ...` — tag 3, length 8 → `servedIMSI`, TBCD-encoded.

**Step 2 — the decoded record (`_asn1_record_index = 0`, abbreviated):**

```json
{
  "recordType": 18,
  "servedIMSI": "MjRAIQESEvM=",
  "servedIMEI": "U0iTcJOFVhA=",
  "sgsnAddress": {
    "_choice": "iPBinaryAddress",
    "iPBinaryAddress": { "_choice": "iPBinV4Address", "iPBinV4Address": "lf6ASQ==" }
  },
  "routingArea": "AQ==",
  "locationAreaCode": "BKQ=",
  "cellIdentifier": "PL4=",
  "chargingID": 2504098619,
  "accessPointNameNI": "everywhere",
  "pdpType": "8Vc=",
  "servedPDPAddress": {
    "_choice": "iPAddress",
    "iPAddress": {
      "_choice": "iPBinaryAddress",
      "iPBinaryAddress": { "_choice": "iPBinV6Address", "iPBinV6Address": "KgEEyBQIHhIAAQACzU9vtw==" }
    }
  },
  "listOfTrafficVolumes": [
    {
      "qosRequested": "ARuRH3GW/v50S/7+AJYA",
      "qosNegotiated": "AxuTH3OW/v50q/7+AGQA",
      "dataVolumeGPRSUplink": 6293,
      "dataVolumeGPRSDownlink": 21574,
      "changeCondition": "recordClosure",
      "changeTime": "FwgVEEZYKwEA"
    }
  ]
}
```

**Step 3 — how that lands as a bronze row:**

| Column | Value |
|---|---|
| `_choice` | `sgsnPDPRecord` |
| `sgsnPDPRecord` | the struct above |
| `uMTSGSMPLMNCallDataRecord` and all other arms | `NULL` |
| `_asn1_decode_error` | `NULL` |
| `_asn1_record_index` | `0` |
| `__framework_source_file_name` | `PSHLD02_3699_03_20170815_094659_38723.fin` |

**Observations that matter for the silver design:**

- Nested CHOICEs are resolved **recursively** — `sgsnAddress` → `iPBinaryAddress` → `iPBinV4Address`.
- `SEQUENCE OF` becomes a Spark **array** (`listOfTrafficVolumes`).
- **OCTET STRING fields render as base64 in JSON.** `servedIMSI = "MjRAIQESEvM="` is base64 of TBCD-encoded digits, **not** a readable IMSI. Converting these to readable digit strings is silver-layer work (6.6).
- `accessPointNameNI = "everywhere"` decodes as a readable string — a genuine APN value, confirming the decode is semantically correct and not merely structurally valid.

### 7.6 SMSC and MMSC — flagged, not guessed

**These two sources were investigated and found not to be ASN.1 at all.**

**SMSC** — `raw/SMSC/SMSCWPTD01_201711301432`, 681 bytes. Actual content is a single-line, 48-field quoted CSV:

```
"20171130143417","5","1","4C3F4F8ADBD5E761995F000002003267","","1","4","","20171130143415",...
```

**MMSC** — `raw/MMSC/vMMSC12_20200721013526Z_mediation_1a953ca07b.csv`, 73,689 bytes, a 70+ column CSV:

```
1,"","4.6","vmmscld01","HTTP Retrieve",1004,1,"20180430013538Z","20180430023538",0,1000,...
```

**Negative control — both payloads decoded against every available module:**

| Payload | vs `EMSC.asn1` | vs `SGSN.asn1` | vs `TAP.310.asn1` | vs `MMS.asn1` |
|---|---|---|---|---|
| SMSC | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` |
| MMSC | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` |

- There is additionally **no `SMSC.asn1` module** anywhere in `asn_schema/`.
- `MMS.asn1` exists and is suggestively named, but models **RADIUS accounting attributes**, and the MMSC payload is CSV. It is not the MMSC decoder.
- **Conclusion:** onboarding either source as `source_type: asn1` would quarantine **100%** of their rows. They require either their genuine ASN.1 modules, or onboarding as `source_type: autoloader` with `format: csv` and an agreed column contract.

### 7.7 Time travel and historical revisions

Bronze is `APPEND`-only, so a given row is never mutated. Delta time travel is therefore used to answer **"what did this table contain at the time that report was produced?"** — the restatement question that matters for regulatory reporting.

```sql
-- 1. Inspect the full version history of a bronze table
DESCRIBE HISTORY br_digital_poc.bronze.sgsn_cdr_raw;

-- 2. Read the table exactly as it stood at a specific version
SELECT count(*) AS rows_at_version
FROM   br_digital_poc.bronze.sgsn_cdr_raw VERSION AS OF 1;

-- 3. Read the table as at a point in time (regulatory restatement)
SELECT count(*) AS rows_at_timestamp
FROM   br_digital_poc.bronze.sgsn_cdr_raw TIMESTAMP AS OF '2026-09-05T08:00:00';

-- 4. Show exactly which rows arrived between two versions
SELECT __framework_source_file_name,
       count(*) AS rows_added
FROM   br_digital_poc.bronze.sgsn_cdr_raw VERSION AS OF 2
EXCEPT ALL
SELECT __framework_source_file_name,
       count(*)
FROM   br_digital_poc.bronze.sgsn_cdr_raw VERSION AS OF 1;

-- 5. Confirm re-runs did not duplicate: one row per (file, record index)
SELECT __framework_source_file_name,
       count(*)                              AS total_rows,
       count(DISTINCT _asn1_record_index)     AS distinct_positions
FROM   br_digital_poc.bronze.sgsn_cdr_raw
GROUP  BY __framework_source_file_name;
-- EXPECTED: total_rows = distinct_positions for every file
```

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Decoded SGSN record expanded in the SQL result viewer.
<b>UI location:</b> Databricks SQL editor, after running <code>SELECT * FROM br_digital_poc.bronze.sgsn_cdr_raw WHERE _asn1_record_index = 0</code>.
<b>Expected content:</b> The <code>sgsnPDPRecord</code> struct expanded in the JSON viewer showing <code>recordType</code>, <code>servedIMSI</code>, <code>accessPointNameNI: "everywhere"</code> and the <code>listOfTrafficVolumes</code> array, with <code>_choice</code> and <code>_asn1_record_index</code> visible.
</div>

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> <code>DESCRIBE HISTORY</code> output for a bronze table.
<b>UI location:</b> Databricks SQL editor, running <code>DESCRIBE HISTORY br_digital_poc.bronze.sgsn_cdr_raw</code>.
<b>Expected content:</b> Version history grid showing version numbers, timestamps, <code>STREAMING UPDATE</code> operations, and operation metrics with <code>numOutputRows</code> per version.
</div>

---

## 8.0 Enterprise Governance, Security and Unity Catalog Controls

### 8.1 Unity Catalog three-level namespace

**Structure: `catalog.schema.table_or_view`**

| Level | UC7 value | Purpose |
|---|---|---|
| **Catalog** | `flowx` | Environment boundary. Separate environments use separate catalogs, **not** schema suffixes |
| **Schema** | `landing`, `bronze`, `silver`, `gold`, `config`, `observability`, `staging` | Layer boundary |
| **Object** | `sgsn_cdr_raw`, `sgsn_cdr_raw_quarantine`, … | The data product |

**Full inventory of UC7 objects:**

| Object | Type | Purpose |
|---|---|---|
| `br_digital_poc.landing.uc_7` | Volume (MANAGED) | Raw binary CDRs and ASN.1 modules |
| `br_digital_poc.staging.uc_7` | Volume (MANAGED) | Backup copy of the landing tree |
| `br_digital_poc.bronze.{emsc_cdr_raw, psgw_cdr_raw, sgsn_cdr_raw, tap310_raw}` | Streaming Tables | Decoded CDRs |
| `br_digital_poc.bronze.*_quarantine` (×4) | Streaming Tables | Failed records with reasons |
| `br_digital_poc.config.*` (9 tables) | Delta Tables | Framework control metadata |
| `br_digital_poc.observability.app_logs` | Volume (MANAGED) | Run telemetry |

### 8.2 Current security posture — stated honestly

| Control | Status | Assessment |
|---|---|---|
| Unity Catalog namespace | **In place** | All objects governed; lineage automatic |
| Volume-level isolation | **In place** | Source data on a managed Volume, not bundle-managed so it cannot be destroyed by a deploy |
| Automatic lineage | **In place** | Volume → pipeline → table captured by UC |
| Delta time travel / audit history | **In place** | `DESCRIBE HISTORY` on every table |
| Row-level audit columns | **In place** | `__framework_*` on every row |
| **Column tags on PII** | **NOT APPLIED** | `governance_tags` is `{}` on all four flows |
| **Column masking on IMSI/IMEI** | **NOT APPLIED** | No masks defined |
| **Row-level filters** | **NOT APPLIED** | None defined |
| **Explicit RBAC grants** | **NOT APPLIED** | Tables inherit creator ownership only |

> **Architect's recommendation, stated as a gate:** `br_digital_poc.bronze` currently contains **unmasked IMSI and IMEI values**. These are personal data. **No non-privileged user should be granted `SELECT` on these bronze tables until the controls in 8.3 to 8.5 are applied.** The recommended access model is that analysts consume **silver and gold**, never bronze.

### 8.3 PII handling — recommended implementation

**8.3.1 Classify the sensitive fields**

| Field | Location | Sensitivity | Recommended treatment |
|---|---|---|---|
| `servedIMSI` | SGSN, PSGW, EMSC records | **High** — identifies the subscriber | Mask to last 4 digits; full value for Revenue Assurance only |
| `servedIMEI` | SGSN, PSGW records | **High** — identifies the device | Mask entirely for analysts |
| MSISDN / called and calling numbers | EMSC records, TAP records | **High** — phone numbers | Mask to country plus operator prefix |
| `cellIdentifier`, `locationAreaCode` | SGSN, PSGW records | **Medium** — location inference | Restrict to Network Engineering |
| `servedPDPAddress` | SGSN, PSGW records | **Medium** — IP address | Mask host portion |
| `chargingID`, `recordType`, volumes | All | **Low** | No masking needed |

**8.3.2 Apply Unity Catalog tags first**

Tags are preferred over direct grants because a policy written once applies to **every** tagged column, the tag travels with the column through renames, and the tag value *is* the classification.

```sql
-- Tag the sensitive columns. Extend to each table and arm as appropriate.
ALTER TABLE br_digital_poc.bronze.sgsn_cdr_raw
  ALTER COLUMN sgsnPDPRecord SET TAGS ('contains_pii' = 'true', 'sensitivity' = 'high');

ALTER TABLE br_digital_poc.bronze.sgsn_cdr_raw
  SET TAGS ('data_domain' = 'telecom_cdr', 'retention_class' = 'regulatory_7_year');
```

**8.3.3 Dynamic column masking with a SQL UDF**

```sql
-- Masking function: full value for privileged groups, last 4 digits otherwise.
CREATE OR REPLACE FUNCTION flowx.security.mask_imsi(imsi STRING)
RETURN CASE
         WHEN is_account_group_member('uc7_revenue_assurance') THEN imsi
         WHEN is_account_group_member('uc7_data_engineers')     THEN imsi
         WHEN imsi IS NULL                                       THEN NULL
         ELSE concat('***********', right(imsi, 4))
       END;

-- Fully redact device identifiers for everyone except engineers.
CREATE OR REPLACE FUNCTION flowx.security.mask_imei(imei STRING)
RETURN CASE
         WHEN is_account_group_member('uc7_data_engineers') THEN imei
         ELSE '[REDACTED]'
       END;

-- Bind the mask to a conformed silver column.
ALTER TABLE br_digital_poc.silver.cdr_unified
  ALTER COLUMN imsi SET MASK flowx.security.mask_imsi;

ALTER TABLE br_digital_poc.silver.cdr_unified
  ALTER COLUMN imei SET MASK flowx.security.mask_imei;
```

> **Implementation note:** column masks apply to **scalar** columns. Bronze's PII is buried inside nested struct arms, which cannot be masked in place. This is a further reason the analyst-facing layer must be **silver**, where the fields have been flattened to scalars and can carry masks.

**8.3.4 Row-level filtering**

```sql
-- Restrict roaming settlement rows to the team owning that partner relationship.
CREATE OR REPLACE FUNCTION flowx.security.roaming_partner_filter(partner_plmn STRING)
RETURN is_account_group_member('uc7_roaming_settlement')
       OR is_account_group_member('uc7_data_engineers');

ALTER TABLE br_digital_poc.silver.cdr_roaming
  SET ROW FILTER flowx.security.roaming_partner_filter ON (sender_plmn);
```

### 8.4 Data quality and NULL handling

**8.4.1 Three distinct meanings of NULL in UC7 — do not confuse them**

| # | Kind of NULL | Cause | How to identify it |
|---|---|---|---|
| 1 | **Unselected CHOICE arm** | The record was a different CDR type | `_choice` names a *different* arm. **Expected and correct** — for SGSN, 100% of `uMTSGSMPLMNCallDataRecord` values are NULL because every record is an `sgsnPDPRecord` |
| 2 | **Genuine absent OPTIONAL member** | The network element did not populate that field | Inside the selected arm's struct; some rows have it, some do not. **Not a defect** — nearly every ASN.1 member is OPTIONAL |
| 3 | **Decode failure** | The record could not be decoded | `_asn1_decode_error IS NOT NULL`, and the row is in the **quarantine** table, not the main table |

> **This distinction is why the DQ rules are thin.** Asserting `servedIMSI IS NOT NULL` would quarantine valid records where the network legitimately omitted it. Only assert what has been **measured** to hold on 100% of records.

**8.4.2 Schema constraint enforcement**

- The output schema is **derived from the ASN.1 module**, so a field of the wrong type cannot be written — the decode fails and the record is quarantined with its reason.
- `_asn1_decode_error` is the operative rescue mechanism, playing the role that `_rescued_data` plays for JSON and CSV sources.
- A CHOICE that matches no arm returns `(None, None)` from `asn1tools` **without raising**. Unguarded, this writes an all-NULL row with a NULL error, indistinguishable from a valid empty record and reported as success. The framework explicitly converts that case into a per-row error, and the `dq_*_choice_arm_selected` rule provides a second line of defence.

### 8.5 Access control and RBAC strategy

**8.5.1 Recommended role definitions**

| Role | Group | Bronze | Silver | Gold | Config | Volumes |
|---|---|---|---|---|---|---|
| **Platform Developer** | `uc7_developers` | Full | Full | Full | Full | Read/Write |
| **Data Engineer** | `uc7_data_engineers` | Read (unmasked) | Read/Write | Read/Write | Read | Read |
| **Revenue Assurance** | `uc7_revenue_assurance` | **No** | Read (IMSI unmasked) | Read | No | No |
| **Business Analyst** | `uc7_business_analysts` | **No** | Read (masked) | Read | No | No |
| **Network Engineer** | `uc7_network_engineers` | **No** | Read (location fields) | Read | No | No |
| **Roaming Settlement** | `uc7_roaming_settlement` | **No** | Read (roaming rows only) | Read | No | No |
| **Auditor** | `uc7_auditors` | Read (masked) | Read (masked) | Read | Read | No |

**8.5.2 GRANT statements**

```sql
-- ---------- Prerequisite: every consumer needs catalog access ----------
GRANT USE CATALOG ON CATALOG flowx TO `uc7_data_engineers`;
GRANT USE CATALOG ON CATALOG flowx TO `uc7_business_analysts`;
GRANT USE CATALOG ON CATALOG flowx TO `uc7_revenue_assurance`;
GRANT USE CATALOG ON CATALOG flowx TO `uc7_auditors`;

-- ---------- Platform Developer: full control of the pipeline ----------
GRANT ALL PRIVILEGES ON SCHEMA br_digital_poc.bronze TO `uc7_developers`;
GRANT ALL PRIVILEGES ON SCHEMA br_digital_poc.config TO `uc7_developers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME br_digital_poc.landing.uc_7 TO `uc7_developers`;

-- ---------- Data Engineer: read bronze, build silver and gold ----------
GRANT USE SCHEMA ON SCHEMA br_digital_poc.bronze TO `uc7_data_engineers`;
GRANT SELECT ON SCHEMA br_digital_poc.bronze TO `uc7_data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA br_digital_poc.silver TO `uc7_data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA br_digital_poc.gold TO `uc7_data_engineers`;
GRANT READ VOLUME ON VOLUME br_digital_poc.landing.uc_7 TO `uc7_data_engineers`;
GRANT SELECT ON SCHEMA br_digital_poc.config TO `uc7_data_engineers`;

-- ---------- Business Analyst: gold and masked silver only. NOT bronze ----------
GRANT USE SCHEMA ON SCHEMA br_digital_poc.gold TO `uc7_business_analysts`;
GRANT SELECT ON SCHEMA br_digital_poc.gold TO `uc7_business_analysts`;
GRANT USE SCHEMA ON SCHEMA br_digital_poc.silver TO `uc7_business_analysts`;
GRANT SELECT ON TABLE br_digital_poc.silver.cdr_unified TO `uc7_business_analysts`;

-- ---------- Revenue Assurance: needs unmasked IMSI, granted via the mask UDF ----------
GRANT USE SCHEMA ON SCHEMA br_digital_poc.silver TO `uc7_revenue_assurance`;
GRANT SELECT ON TABLE br_digital_poc.silver.cdr_unified TO `uc7_revenue_assurance`;
GRANT SELECT ON SCHEMA br_digital_poc.gold TO `uc7_revenue_assurance`;

-- ---------- Auditor: read-only breadth, including quarantine and control tables ----------
GRANT USE SCHEMA ON SCHEMA br_digital_poc.bronze TO `uc7_auditors`;
GRANT SELECT ON SCHEMA br_digital_poc.bronze TO `uc7_auditors`;
GRANT SELECT ON SCHEMA br_digital_poc.config TO `uc7_auditors`;
GRANT SELECT ON SCHEMA br_digital_poc.gold TO `uc7_auditors`;

-- ---------- Verify what has actually been granted ----------
SHOW GRANTS ON TABLE br_digital_poc.bronze.sgsn_cdr_raw;
SHOW GRANTS ON SCHEMA br_digital_poc.gold;
```

**8.5.3 Layers of control, weakest to strongest**

| Layer | Granularity | Mechanism |
|---|---|---|
| Catalog / schema | Coarse | `GRANT USE CATALOG`, `GRANT USE SCHEMA` |
| Table | Medium | `GRANT SELECT ON TABLE` |
| Column | Fine | Column masks, or ABAC policies over tags |
| Row | Fine | Row filter functions |
| **Absolute** | **Total** | **Do not project the column into silver at all** |

> **The strongest control is the one that removes the data.** For fields with no analytical purpose, the right answer is not to mask them but to omit them from the conformed silver layer entirely. A column that does not exist cannot leak.

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Unity Catalog permissions tab for a bronze table.
<b>UI location:</b> Databricks → Catalog → <code>flowx</code> → <code>bronze</code> → <code>sgsn_cdr_raw</code> → Permissions.
<b>Expected content:</b> Grants grid listing the UC7 groups against their privileges, demonstrating that <code>uc7_business_analysts</code> has <b>no</b> grant on bronze.
</div>

---

## 9.0 Data Validation, Auditability and DLT Event Log SQL Recipes

### 9.1 DLT event log architecture

- Every DLT pipeline writes a structured **event log** recording each flow's progress, metrics, expectations and errors.
- The log is queryable through the **`event_log()`** table-valued function, addressed via any table the pipeline publishes.
- This is the authoritative source for reconciliation: the counts come from the **pipeline's own telemetry**, not from a `count(*)` that a later job could have changed.

| Field | Content |
|---|---|
| `id`, `timestamp` | Event identity and time |
| `event_type` | `flow_progress`, `update_progress`, `flow_definition`, and others |
| `origin` | Nested struct: `flow_name`, `update_id`, `dataset_name`, `pipeline_id` |
| `level` | `INFO`, `WARN`, `ERROR` |
| `message` | Human-readable text |
| `details` | JSON payload holding `metrics`, `data_quality`, `error` |

> **Practical caution learned during this build:** do **not** write `SELECT *` against `event_log()`. The `origin` struct has roughly 30 fields, and Spark Connect returns the result as multiple Arrow batches whose schemas disagree on nullability, causing the collect to fail outright. Always project the columns you need. This was a genuine framework defect, now fixed (Appendix A, defect 3).

### 9.2 Recipe 1 — Reconcile source, staged and published counts

This is the headline reconciliation. It proves no records were lost between reading the file and publishing the table.

```sql
SELECT
    origin.flow_name                                                              AS flow,
    sum(cast(get_json_object(details, '$.flow_progress.metrics.num_output_rows')
             AS BIGINT))                                                          AS rows_written
FROM   event_log(TABLE(br_digital_poc.bronze.sgsn_cdr_raw))
WHERE  event_type = 'flow_progress'
  AND  get_json_object(details, '$.flow_progress.metrics.num_output_rows') IS NOT NULL
GROUP  BY origin.flow_name
ORDER  BY origin.flow_name;
```

**Verified actual output:**

| flow | rows_written |
|---|---|
| `br_digital_poc.bronze._src___volumes_flowx_landing_uc_7_raw_emsc__5e807b99__stream` | 353 |
| `br_digital_poc.bronze._emsc_cdr_raw_staged` | 353 |
| `br_digital_poc.bronze.emsc_cdr_raw` | **353** |
| `br_digital_poc.bronze.emsc_cdr_raw_quarantine` | **0** |
| `br_digital_poc.bronze._src___volumes_flowx_landing_uc_7_raw_psgw__c67361f1__stream` | 93 |
| `br_digital_poc.bronze._psgw_cdr_raw_staged` | 93 |
| `br_digital_poc.bronze.psgw_cdr_raw` | **93** |
| `br_digital_poc.bronze.psgw_cdr_raw_quarantine` | **0** |
| `br_digital_poc.bronze._src___volumes_flowx_landing_uc_7_raw_sgsn__ee25e3cc__stream` | 175,048 |
| `br_digital_poc.bronze._sgsn_cdr_raw_staged` | 175,048 |
| `br_digital_poc.bronze.sgsn_cdr_raw` | **175,048** |
| `br_digital_poc.bronze.sgsn_cdr_raw_quarantine` | **0** |
| `br_digital_poc.bronze._src___volumes_flowx_landing_uc_7_raw_tap__e759a809__stream` | 4 |
| `br_digital_poc.bronze._tap310_raw_staged` | 4 |
| `br_digital_poc.bronze.tap310_raw` | **4** |
| `br_digital_poc.bronze.tap310_raw_quarantine` | **0** |

**How to read this:** for each of the four lanes, the source reader, the staged node and the published table all report the **identical** count, with **zero** quarantined. Source equals staged equals published, at every hop, for every source. That is a clean end-to-end reconciliation.

### 9.3 Recipe 2 — Identify dropped or failed records violating expectations

```sql
-- Expectation outcomes per rule, per dataset
SELECT
    origin.flow_name                                     AS flow,
    exp.value:name::string                               AS rule_name,
    sum(exp.value:passed_records::bigint)                AS passed,
    sum(exp.value:failed_records::bigint)                AS failed
FROM   event_log(TABLE(br_digital_poc.bronze.sgsn_cdr_raw)) AS ev
       LATERAL VARIANT_EXPLODE(
         parse_json(get_json_object(ev.details,
                    '$.flow_progress.data_quality.expectations'))) AS exp
WHERE  ev.event_type = 'flow_progress'
  AND  get_json_object(ev.details, '$.flow_progress.data_quality.expectations') IS NOT NULL
GROUP  BY 1, 2
ORDER  BY failed DESC, 1, 2;
```

```sql
-- Direct inspection of the quarantine tables, with the reason
SELECT 'sgsn'  AS source, _asn1_decode_error, count(*) AS records
FROM   br_digital_poc.bronze.sgsn_cdr_raw_quarantine   GROUP BY 1, 2
UNION ALL
SELECT 'emsc',  _asn1_decode_error, count(*)
FROM   br_digital_poc.bronze.emsc_cdr_raw_quarantine   GROUP BY 1, 2
UNION ALL
SELECT 'psgw',  _asn1_decode_error, count(*)
FROM   br_digital_poc.bronze.psgw_cdr_raw_quarantine   GROUP BY 1, 2
UNION ALL
SELECT 'tap310', _asn1_decode_error, count(*)
FROM   br_digital_poc.bronze.tap310_raw_quarantine     GROUP BY 1, 2
ORDER  BY source;
```

**Expected output today:** **zero rows.** All four quarantine tables are empty.

### 9.4 Recipe 3 — Batch execution time, pipeline events and run statistics

```sql
-- Update-level outcome and wall-clock duration
SELECT
    origin.update_id                                        AS update_id,
    min(timestamp)                                          AS started_at,
    max(timestamp)                                          AS ended_at,
    timestampdiff(SECOND, min(timestamp), max(timestamp))    AS duration_seconds,
    max(CASE WHEN event_type = 'update_progress'
             THEN get_json_object(details, '$.update_progress.state') END) AS final_state
FROM   event_log(TABLE(br_digital_poc.bronze.sgsn_cdr_raw))
GROUP  BY origin.update_id
ORDER  BY started_at DESC;
```

```sql
-- All errors and warnings, newest first - the first place to look when a run fails
SELECT timestamp,
       origin.flow_name              AS flow,
       level,
       substr(message, 1, 300)       AS message
FROM   event_log(TABLE(br_digital_poc.bronze.sgsn_cdr_raw))
WHERE  level IN ('ERROR', 'WARN')
ORDER  BY timestamp DESC
LIMIT  50;
```

```sql
-- Throughput per flow: rows and backlog over time
SELECT timestamp,
       origin.flow_name                                                          AS flow,
       get_json_object(details, '$.flow_progress.metrics.num_output_rows')        AS rows_out,
       get_json_object(details, '$.flow_progress.metrics.backlog_bytes')          AS backlog_bytes,
       get_json_object(details, '$.flow_progress.status')                         AS status
FROM   event_log(TABLE(br_digital_poc.bronze.sgsn_cdr_raw))
WHERE  event_type = 'flow_progress'
ORDER  BY timestamp DESC
LIMIT  100;
```

### 9.5 Recipe 4 — Independent file-level reconciliation

The event log proves internal consistency. This query proves the pipeline read **the files that actually exist**.

```sql
-- Rows per source file, with the record range
SELECT __framework_source_file_name              AS source_file,
       count(*)                                  AS rows_loaded,
       min(_asn1_record_index)                   AS first_index,
       max(_asn1_record_index)                   AS last_index,
       max(_asn1_record_index) + 1               AS expected_if_contiguous,
       CASE WHEN count(*) = max(_asn1_record_index) + 1
            THEN 'CONTIGUOUS - no records lost'
            ELSE 'GAP DETECTED - investigate' END AS completeness_check
FROM   br_digital_poc.bronze.sgsn_cdr_raw
GROUP  BY __framework_source_file_name
ORDER  BY source_file;
```

**Expected output:**

| source_file | rows_loaded | first_index | last_index | expected_if_contiguous | completeness_check |
|---|---|---|---|---|---|
| `PSHLD02_3699_03_20170815_094659_38723.fin` | 175,048 | 0 | 175,047 | 175,048 | CONTIGUOUS - no records lost |

> **Why this check earns its place:** it is the query that would have caught the silent record-loss defect described in Appendix A. Before the fix, this file produced **1** row with `last_index = 0`. Any monitoring suite for UC7 should run this query on every load.

### 9.6 Observability export verification

```sql
-- Confirm the observability destination is registered and enabled
SELECT dataflow_group_id, destination_id, enabled, destination_type, mode
FROM   br_digital_poc.config.observability_config
WHERE  dataflow_group_id IN ('dfg_uc7_cdr_asn', '*');
```

**Verified output:** one row — `dfg_uc7_cdr_asn`, `dest-uc7-volume`, `enabled = true`, `DATABRICKS_VOLUME`, `mode = NULL` (which defaults to `triggered`, correct for a task-based export).

**Verified emitted artifact:**

```
/Volumes/br_digital_poc/observability/app_logs/dfg_uc7_cdr_asn/2026-09-05/
    dfg_uc7_cdr_asn_387734270983946.jsonl.gz     1,619 bytes
```

- Path pattern: `{volume_path}/{dataflow_group_id}/{YYYY-MM-DD}/{group}_{task_run_id}.jsonl.gz`.
- Content: **16 OpenTelemetry log records**, each a well-formed `resourceLogs` envelope.
- Resource attributes tie the telemetry to this specific run: `service.name = dlt-observability-uc7-cdr-asn`, `databricks.job_id = 843342822766009`, `databricks.pipeline_id = 927a6e24-...`, `deployment.environment = metaflow_v7`, `pipeline.update_id = 469ff760-...`.

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Event log reconciliation query result.
<b>UI location:</b> Databricks SQL editor, running Recipe 1 from section 9.2.
<b>Expected content:</b> 16-row result grid where each lane's source, staged and published counts match and each quarantine row reads 0 — with the SGSN triplet all showing 175,048.
</div>

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> DLT Data Quality tab.
<b>UI location:</b> Databricks → Pipelines → <code>001_ldp_uc7_cdr_asn</code> → select a dataset → Data quality tab.
<b>Expected content:</b> Expectation panel listing the three DQ rules per dataset with 100% passing records and zero failures.
</div>

---

## 10.0 Business User Testing and Acceptance Playbook

### 10.1 How to use this section

- Written for **non-technical business testers**. No Python, no cluster configuration.
- Every query runs directly in **Databricks SQL Warehouse**.
- Each test states the query, the expected result, and **how to interpret a failure**.
- Suggested order: Test 1 → Test 7. Test 1 is the single most important check.

**Getting started:**

1. Log in to Databricks and open **SQL Editor** from the left navigation.
2. Select a running SQL Warehouse from the dropdown at the top right.
3. Copy a query below, paste it, and press **Run**.

<div class="screenshot">
<b>[SCREENSHOT PLACEHOLDER:</b> Databricks SQL Editor ready for the acceptance tests.
<b>UI location:</b> Databricks → SQL Editor.
<b>Expected content:</b> Empty editor with the warehouse selector showing a Running warehouse, and the schema browser expanded to <code>flowx</code> → <code>bronze</code> showing the eight UC7 tables.
</div>

### 10.2 Test 1 — Record count verification (the headline test)

**Purpose:** confirm every source has loaded and nothing failed to decode.

```sql
SELECT 'EMSC'   AS source_system,
       count(*)                                          AS records_loaded,
       count_if(_asn1_decode_error IS NOT NULL)          AS decode_failures,
       count(DISTINCT __framework_source_file_name)      AS files_processed
FROM   br_digital_poc.bronze.emsc_cdr_raw
UNION ALL
SELECT 'PSGW', count(*), count_if(_asn1_decode_error IS NOT NULL),
       count(DISTINCT __framework_source_file_name)
FROM   br_digital_poc.bronze.psgw_cdr_raw
UNION ALL
SELECT 'SGSN', count(*), count_if(_asn1_decode_error IS NOT NULL),
       count(DISTINCT __framework_source_file_name)
FROM   br_digital_poc.bronze.sgsn_cdr_raw
UNION ALL
SELECT 'TAP 3.10', count(*), count_if(_asn1_decode_error IS NOT NULL),
       count(DISTINCT __framework_source_file_name)
FROM   br_digital_poc.bronze.tap310_raw
ORDER  BY source_system;
```

**Expected output:**

| source_system | records_loaded | decode_failures | files_processed |
|---|---:|---:|---:|
| EMSC | 353 | 0 | 3 |
| PSGW | 93 | 0 | 3 |
| SGSN | 175,048 | 0 | 1 |
| TAP 3.10 | 4 | 0 | 4 |

**How to interpret:**

- ✅ **Pass** — `decode_failures` is **0** on every row, and `records_loaded` is greater than zero.
- ❌ **Fail** — any non-zero `decode_failures`. Run Test 3 to see the reasons.
- ⚠️ **Investigate** — `records_loaded` of 0 for a source means no files arrived, or the pipeline has not run.

### 10.3 Test 2 — Quarantine check (no rejected records)

**Purpose:** confirm no records were rejected by the quality rules.

```sql
SELECT 'EMSC' AS source_system, count(*) AS quarantined_records
FROM   br_digital_poc.bronze.emsc_cdr_raw_quarantine
UNION ALL SELECT 'PSGW',     count(*) FROM br_digital_poc.bronze.psgw_cdr_raw_quarantine
UNION ALL SELECT 'SGSN',     count(*) FROM br_digital_poc.bronze.sgsn_cdr_raw_quarantine
UNION ALL SELECT 'TAP 3.10', count(*) FROM br_digital_poc.bronze.tap310_raw_quarantine
ORDER  BY source_system;
```

**Expected output:**

| source_system | quarantined_records |
|---|---:|
| EMSC | 0 |
| PSGW | 0 |
| SGSN | 0 |
| TAP 3.10 | 0 |

**How to interpret:**

- ✅ **Pass** — all zeros.
- ⚠️ **Investigate** — a non-zero count is **not** a pipeline failure. It means those specific records were set aside with a reason recorded. Run Test 3.

### 10.4 Test 3 — Why records were rejected

**Purpose:** when Test 2 shows a non-zero count, find out why.

```sql
SELECT _asn1_decode_error                     AS rejection_reason,
       count(*)                                AS affected_records,
       min(__framework_source_file_name)       AS example_file,
       min(_asn1_record_index)                 AS example_record_position
FROM   br_digital_poc.bronze.sgsn_cdr_raw_quarantine
GROUP  BY _asn1_decode_error
ORDER  BY affected_records DESC;
```

**Expected output today:** no rows returned, because nothing has been quarantined.

**How to interpret:** each row names a distinct failure reason and how many records it affected. The example file and record position let engineering reproduce it precisely.

### 10.5 Test 4 — Record completeness (no silent data loss)

**Purpose:** the most important data-integrity test. Confirms every record inside every file was loaded.

```sql
SELECT __framework_source_file_name                AS source_file,
       count(*)                                     AS records_loaded,
       max(_asn1_record_index) + 1                  AS records_expected,
       CASE WHEN count(*) = max(_asn1_record_index) + 1
            THEN 'PASS - all records loaded'
            ELSE 'FAIL - records missing' END       AS completeness_result
FROM   br_digital_poc.bronze.sgsn_cdr_raw
GROUP  BY __framework_source_file_name
ORDER  BY source_file;
```

**Expected output:**

| source_file | records_loaded | records_expected | completeness_result |
|---|---:|---:|---|
| `PSHLD02_3699_03_20170815_094659_38723.fin` | 175,048 | 175,048 | PASS - all records loaded |

**How to interpret:**

- ✅ **Pass** — `records_loaded` equals `records_expected` for every file.
- ❌ **Fail** — a mismatch means records are missing from the middle of a file. Escalate to engineering immediately; this is a revenue-impacting condition.

### 10.6 Test 5 — Business KPI reconciliation

**Purpose:** confirm the record type mix matches what the network is expected to produce.

```sql
SELECT 'SGSN' AS source_system,
       _choice                                                   AS cdr_type,
       count(*)                                                  AS record_count,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)        AS percentage
FROM   br_digital_poc.bronze.sgsn_cdr_raw
GROUP  BY _choice
UNION ALL
SELECT 'PSGW', _choice, count(*),
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)
FROM   br_digital_poc.bronze.psgw_cdr_raw
GROUP  BY _choice
UNION ALL
SELECT 'EMSC', _choice, count(*),
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)
FROM   br_digital_poc.bronze.emsc_cdr_raw
GROUP  BY _choice
ORDER  BY source_system, record_count DESC;
```

**Expected output:**

| source_system | cdr_type | record_count | percentage |
|---|---|---:|---:|
| EMSC | `uMTSGSMPLMNCallDataRecord` | 350 | 99.15 |
| EMSC | `compositeCallDataRecord` | 3 | 0.85 |
| PSGW | `sGWRecord` | 68 | 73.12 |
| PSGW | `pGWRecord` | 25 | 26.88 |
| SGSN | `sgsnPDPRecord` | 175,048 | 100.00 |

**How to interpret:**

- ✅ **Pass** — the CDR types present are the ones the network element is configured to produce, and percentages are plausible.
- ⚠️ **Investigate** — an unexpected type, or a large shift in the mix versus the previous load, may indicate a network element configuration change.

### 10.7 Test 6 — Edge case and NULL validation

**Purpose:** confirm the mandatory technical columns are always populated.

```sql
SELECT 'SGSN'                                            AS source_system,
       count(*)                                           AS total_records,
       count_if(_choice IS NULL)                          AS missing_cdr_type,
       count_if(_asn1_record_index IS NULL)               AS missing_record_position,
       count_if(__framework_source_file_name IS NULL)     AS missing_source_file,
       count_if(__framework_ingestion_timestamp_utc IS NULL) AS missing_load_time,
       CASE WHEN count_if(_choice IS NULL) = 0
             AND count_if(_asn1_record_index IS NULL) = 0
             AND count_if(__framework_source_file_name IS NULL) = 0
            THEN 'PASS - all mandatory fields populated'
            ELSE 'FAIL - mandatory field missing' END     AS validation_result
FROM   br_digital_poc.bronze.sgsn_cdr_raw;
```

**Expected output:**

| source_system | total_records | missing_cdr_type | missing_record_position | missing_source_file | missing_load_time | validation_result |
|---|---:|---:|---:|---:|---:|---|
| SGSN | 175,048 | 0 | 0 | 0 | 0 | PASS - all mandatory fields populated |

> **Important for testers:** NULLs **inside** the CDR struct (for example a missing `servedIMEI`) are **normal and expected**. Nearly every field in the ASN.1 standard is optional, and the network only populates what applies to that event. This test deliberately checks only the technical columns that must always be present.

### 10.8 Test 7 — Traceability spot check

**Purpose:** confirm any single record can be traced back to its exact origin.

```sql
SELECT _choice                                       AS cdr_type,
       _asn1_record_index                            AS position_in_file,
       __framework_source_file_name                  AS came_from_file,
       __framework_source_file_modification_time     AS file_landed_at,
       __framework_ingestion_timestamp_utc           AS loaded_to_bronze_at,
       __framework_pipeline_run_id                   AS loaded_by_run
FROM   br_digital_poc.bronze.sgsn_cdr_raw
ORDER  BY _asn1_record_index
LIMIT  5;
```

**How to interpret:**

- ✅ **Pass** — every column is populated, `file_landed_at` is earlier than `loaded_to_bronze_at`, and `position_in_file` increments 0, 1, 2, 3, 4.
- This is the evidence you would produce in a billing dispute: this record came from this file, at this position, and was loaded at this time by this run.

### 10.9 Acceptance sign-off checklist

| # | Test | Acceptance criterion | Result |
|---|---|---|---|
| 1 | Record counts | All four sources loaded, `decode_failures = 0` | ☐ |
| 2 | Quarantine | All four quarantine tables empty, or each entry explained | ☐ |
| 3 | Rejection reasons | No unexplained rejection reasons | ☐ |
| 4 | Completeness | `records_loaded = records_expected` for every file | ☐ |
| 5 | KPI mix | CDR type distribution plausible for each element | ☐ |
| 6 | NULL validation | All mandatory technical columns populated | ☐ |
| 7 | Traceability | Every record traceable to file, position and run | ☐ |
| 8 | Job execution | Job `001_lfj_uc7_cdr_asn` last run **SUCCESS** | ☐ |
| 9 | Observability | Telemetry file present for the run date | ☐ |
| 10 | Scope acknowledgement | Tester accepts SMSC and MMSC are out of scope, per 7.6 | ☐ |

**Current status against this checklist:** tests 1 through 9 **pass** as of run `884721997952817` on 2026-09-05.

---

## Appendix A — Framework Defects Found by This Build

This build surfaced five genuine defects. All five are fixed and re-verified. They are documented because each represents a class of risk that could recur.

### A.1 Defect 1 — Silent record loss on multi-record files (CRITICAL)

| Aspect | Detail |
|---|---|
| **Symptom** | Only the **first** record of each file was ingested. No error raised; the run reported success. |
| **Root cause** | Auto Loader reads a whole file as one row, and the decoder called `decode()` **once** on the entire payload. `asn1tools` decodes the first value and **ignores trailing octets** rather than raising. Production CDR files are concatenated TLVs. |
| **Measured impact** | SGSN: **1 row ingested instead of 175,048** — a loss of 175,047 records, or **99.9994%**, reported as success. EMSC 137 → 1. PSGW 91 → 1. TAP unaffected, as each file is one batch-wrapped record. |
| **Business risk** | Unbilled revenue and understated regulatory returns, with no signal that anything was wrong. |
| **Fix** | Added `iter_ber_tlv_records()`, which walks top-level TLV headers and yields each record's bytes. The decoder now emits **one row per record**, and a new `_asn1_record_index` column carries each record's ordinal. |
| **Verification** | 175,048 of 175,048 SGSN records decode with zero failures; EMSC 137/137; PSGW 91/91. |
| **Detection query** | Test 4 in section 10.5 would have caught this immediately. |

### A.2 Defect 2 — Serverless UDF memory limit exceeded

| Aspect | Detail |
|---|---|
| **Symptom** | Pipeline update failed: `UDF_PYSPARK_USER_CODE_ERROR.MEMORY_LIMIT_SERVERLESS — Function exceeded the limit of 1024 megabytes`. |
| **Root cause** | A direct consequence of the defect 1 fix. Emitting one row per record meant the partition function accumulated **all** decoded rows for an input row in a Python list before yielding — 175,048 wide nested structs at once. |
| **Fix** | `mapInPandas` consumes a generator, so the decoder now flushes in bounded chunks of `_DECODE_CHUNK_ROWS = 10000`. Peak memory is proportional to the chunk, not the file. |
| **Verification** | The real 42 MB file, measured with `tracemalloc`: **18 frames** yielded, **175,048 rows** total, peak Python memory **42 MB** against the 1,024 MB cap. |

### A.3 Defect 3 — `event_log()` Arrow schema mismatch

| Aspect | Detail |
|---|---|
| **Symptom** | Observability task failed: `Schema at index 1 was different: origin: struct not null ... vs origin: struct`. |
| **Root cause** | The extractor issued `SELECT * FROM event_log(...)`. `origin` is a ~30-field nested struct, and Spark Connect returns multiple Arrow batches whose schemas disagree on nullability, so `.collect()` fails. Independent of UC7 — it would affect any pipeline whose event log spans more than one batch. |
| **Fix** | Project explicitly and rebuild `origin` from only the four subfields the extractor reads, using `named_struct`. |
| **Verification** | 166 observability unit tests pass unchanged; the task now completes and emits telemetry. |

### A.4 Defect 4 — Observability destination Volume absent

| Aspect | Detail |
|---|---|
| **Symptom** | `ObservabilityDispatchError: [Errno 95] Operation not supported: '/Volumes/br_digital_poc/observability/app_logs'`. |
| **Root cause** | The destination path used by the framework's own example points at a Volume that did not exist on this workspace, and is not declared in the bootstrap resources, so no deploy would create it. |
| **Fix** | Created `br_digital_poc.observability.app_logs` as a MANAGED Volume. |
| **Note** | That the failure mode *changed* between attempts is itself the evidence that defect 3 was a separate, real fix. |

### A.5 Defect 5 — Resource layout registry not updated

| Aspect | Detail |
|---|---|
| **Symptom** | `test_resource_layout::test_group_folders_are_exactly_the_expected_set` failed after adding `resources/uc7/`. |
| **Root cause** | The repository maintains an explicit registry of resource group folders, deliberately, so that a new folder cannot be added without a matching `include:` line in `databricks.yml`. |
| **Fix** | Registered `uc7` in `EXPECTED_GROUPS`. The `include:` line was already present. |
| **Note** | This is the test working as designed, not a bug in the test. |

### A.6 Known open items

| # | Item | Status | Action required |
|---|---|---|---|
| 1 | **SMSC not onboarded** | Open | Obtain the genuine SMSC ASN.1 module, or agree a CSV column contract |
| 2 | **MMSC not onboarded** | Open | As above |
| 3 | **No PII masking or tags applied** | **Open — highest priority** | Implement section 8.3 before granting non-privileged access |
| 4 | **Silver and gold layers not built** | Open | Implement per the designs in 6.6 and 6.7 |
| 5 | **Indefinite-length BER records not split** | Open | EMSC and some TAP files use indefinite length; the reader yields the remainder whole and stops. Not losing data today, but a file **beginning** with an indefinite-length TLV would yield one row |
| 6 | **`bundle deploy` blocked by an unrelated Apps resource** | Open | The CLI sends `forward_user_access_token` in the Apps update mask and the workspace API rejects it. Unrelated to UC7. Workaround: deploy with `--select`. Fix: pin or upgrade the CLI |
| 7 | **Pipeline in development mode** | Open | Set `development: false` for production |
| 8 | **Two bronze naming conventions now coexist** | Open | UC7 uses `br_digital_poc.bronze`; older use cases use `br_digital_poc.bronze_<source>`. Agree one before onboarding more sources |

---

## Appendix B — Operational Runbook

### B.1 How to run UC7

**From the UI:** Databricks → **Workflows** → **`001_lfj_uc7_cdr_asn`** → **Run now**.

**From the CLI:**

```bash
databricks jobs run-now 843342822766009 -p metaflow_v7
```

- Re-running is **safe**. Auto Loader checkpoints mean already-consumed files are skipped, so no duplicate rows. Verified stable at 175,048 SGSN rows across three runs.

### B.2 How to add a new network element

1. Confirm the ASN.1 module exists in `/Volumes/br_digital_poc/landing/uc_7/asn_schema/`.
2. **Verify the mapping by decoding a real sample file** before writing any config. Never map from the file name.
3. Add a block to `ingestion_flows` in `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json`, following the SGSN pattern in section 4.2.
4. Deploy: `databricks bundle deploy -t metaflow_v7 -p metaflow_v7 --fail-on-active-runs --select pipelines.uc7_cdr_asn_pipeline,jobs.uc7_cdr_asn_job,jobs.onboarding_job`
5. Run the job. The new lane appears in the DAG automatically.

### B.3 Troubleshooting guide

| Symptom | Likely cause | Action |
|---|---|---|
| Job fails at `setup_control_tables` | Missing permission on `br_digital_poc.config` | Confirm the run-as principal can create tables in that schema |
| Job fails at `onboard_uc7` | Spec validation error | Read the child job's output; the validator names every problem at once |
| Pipeline fails with `MEMORY_LIMIT_SERVERLESS` | A single file far larger than any seen so far | Reduce `_DECODE_CHUNK_ROWS`, or move to classic compute |
| Pipeline fails with `ENVIRONMENT_PIP_INSTALL_ERROR` | A `bundle deploy` was run mid-update | Wait for the pipeline to reach IDLE, then re-run. Always use `--fail-on-active-runs` |
| Quarantine table suddenly non-empty | A network element upgrade changed the encoding | Run Test 3; compare the module version against the element's firmware |
| Row count did not increase after a run | No new files, or the checkpoint already consumed them | Confirm new files exist; check `_schemas/` checkpoint state |
| `observability_export` fails | Destination Volume missing or unreachable | Confirm `br_digital_poc.observability.app_logs` exists |
| `SELECT *` on `event_log()` fails | The Arrow nullability defect (A.3) | Project explicit columns; never `SELECT *` |

### B.4 Operational hard rules

1. **Never `bundle deploy` while a pipeline or test wave is running.** A deploy prunes wheel artifacts and kills in-flight updates. Always `--fail-on-active-runs`.
2. **Never map an ASN.1 module from its file name.** Decode a real sample first.
3. **Never change a DQ rule action from `quarantine` to `drop`.** A dropped CDR is unbilled revenue with no audit trail.
4. **Never grant non-privileged users `SELECT` on bronze** until masking is in place (section 8.3).
5. **Bump `pyproject.toml` and `databricks.yml`'s `framework_version` together**, in the same commit.

### B.5 Key file locations

| File | Purpose |
|---|---|
| `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` | The onboarding specification |
| `resources/uc7/uc7_cdr_asn_pipeline.yml` | Pipeline definition |
| `resources/uc7/uc7_cdr_asn_job.yml` | Job definition |
| `src/flowx/lakeflow_framework/asn1/decoder.py` | ASN.1 decode logic |
| `src/flowx/lakeflow_framework/ingestion/readers.py` | `read_asn1_source` |
| `src/flowx/lakeflow_framework/observability/event_log_extractor.py` | Event log extraction |
| `notebooks/03_engine/03_lakeflow_declarative_pipeline.py` | The generic pipeline engine |
| [`UC7_CDR_ASN_Test_Report.md`](UC7_CDR_ASN_Test_Report.md) | Detailed test report with full verification evidence |
| [`UC7_PLAIN_ENGLISH_GUIDE.md`](UC7_PLAIN_ENGLISH_GUIDE.md) | Plain-English summary for business readers |
| [`UC7_ANALYSIS_AND_JOIN_GUIDE.md`](UC7_ANALYSIS_AND_JOIN_GUIDE.md) | How to join and analyse the decoded bronze tables |
| [`UC7_ANALYSIS_QUERIES.sql`](UC7_ANALYSIS_QUERIES.sql) | Ready-to-run analysis SQL |
| `BT_Usecase/UC7/data/` | Repo-side UC7 data assets — see 3.5 for provenance |
| `scripts/generate_synthetic_ber.py` | Generator for the **[Simulated]** `.ber` fixtures under `BT_Usecase/UC7/data/synthetic/` |

---

## Appendix C — Databricks Reference Links

| Topic | Link |
|---|---|
| Lakeflow Declarative Pipelines | https://docs.databricks.com/aws/en/dlt/ |
| DLT Python reference | https://docs.databricks.com/aws/en/dlt/python-ref |
| DLT expectations | https://docs.databricks.com/aws/en/dlt/expectations |
| DLT event log | https://docs.databricks.com/aws/en/dlt/observability |
| Auto Loader | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/ |
| Auto Loader options | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options |
| Auto Loader schema evolution | https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema |
| Unity Catalog | https://docs.databricks.com/aws/en/data-governance/unity-catalog/ |
| Unity Catalog Volumes | https://docs.databricks.com/aws/en/volumes/ |
| Column masks and row filters | https://docs.databricks.com/aws/en/tables/row-and-column-filters |
| Attribute-based access control | https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/ |
| Unity Catalog tags | https://docs.databricks.com/aws/en/database-objects/tags |
| Delta Lake time travel | https://docs.databricks.com/aws/en/delta/history |
| Streaming tables | https://docs.databricks.com/aws/en/tables/streaming |
| Materialized views | https://docs.databricks.com/aws/en/materialized-views |
| Databricks Asset Bundles | https://docs.databricks.com/aws/en/dev-tools/bundles/ |
| Lakeflow Jobs | https://docs.databricks.com/aws/en/jobs/ |
| Serverless compute | https://docs.databricks.com/aws/en/compute/serverless/ |
| Databricks SQL | https://docs.databricks.com/aws/en/sql/ |
| ITU-T X.690 (BER/DER specification) | https://www.itu.int/rec/T-REC-X.690 |
| 3GPP TS 32.298 (CDR parameter description) | https://www.3gpp.org/dynareport/32298.htm |

---

**End of document.**

*All figures, table names, job identifiers, row counts and event log outputs in this document were read back from the live `metaflow_v7` workspace on 2026-09-05. Items not yet built are explicitly marked as designed-not-implemented, and open gaps are listed in Appendix A.6.*
