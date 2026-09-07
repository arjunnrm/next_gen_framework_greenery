# UC_7 — CDR ASN.1 Decoders into Bronze

A plain-English guide to what this use case does, what it reads, and what it writes.

**Last updated:** 2026-09-05
**Status:** Live and working on the `metaflow_v7` workspace

---

## 1. What this use case does — in one paragraph

Mobile networks write **CDRs** (Call Detail Records) — one record for every call, text
message, and data session. The network equipment writes them in a binary format called
**ASN.1**, which is compact but not readable by SQL. This use case picks those binary files
up automatically, decodes them into normal table columns, and saves them into the **bronze**
layer so analysts can query them with plain SQL.

In short: **binary CDR files in → queryable Delta tables out.**

---

## 2. The names you need to know

| What | Name |
|---|---|
| **Job name** | `001_lfj_uc7_cdr_asn` (job ID `843342822766009`) |
| **Pipeline name** | `001_ldp_uc7_cdr_asn` (pipeline ID `927a6e24-757f-495c-8a94-d807b74c4128`) |
| **Dataflow group ID** | `dfg_uc7_cdr_asn` |
| **Framework version** | `0.0.3` |
| **Catalog** | `flowx` |
| **Target layer** | `bronze` |
| **Workspace** | `metaflow_v7` |

- The **job** is the thing you click "Run" on. It does everything start to finish.
- The **pipeline** is the part inside the job that actually reads files and writes tables.
- The **dataflow group ID** is the label that ties the configuration to the pipeline. The
  pipeline looks up its instructions using this ID.

---

## 3. Source details — where the data comes from

All raw files live in one Databricks Volume, with one folder per piece of network equipment:

```
/Volumes/flowx/landing/uc_7/
├── raw/            <- the CDR files themselves
│   ├── EMSC/
│   ├── PSGW/
│   ├── SGSN/
│   ├── TAP/
│   ├── MMSC/       (not used - see section 8)
│   └── SMSC/       (not used - see section 8)
└── asn_schema/     <- the ASN.1 "dictionaries" used to decode them
```

### 3.1 The four sources we decode

| # | Source | What it is | Folder | File count | Size |
|---|---|---|---|---|---|
| 1 | **EMSC** | Mobile switching centre — voice calls | `raw/EMSC/` | 3 | 35.4 MB |
| 2 | **PSGW** | Packet gateway — mobile data sessions | `raw/PSGW/` | 3 | 32 KB |
| 3 | **SGSN** | GPRS support node — data sessions | `raw/SGSN/` | 1 | 42.0 MB |
| 4 | **TAP**  | Roaming charges exchanged with other operators | `raw/TAP/` | 4 | 1.7 MB |

### 3.2 The decoder dictionary each source uses

An ASN.1 file is a **dictionary**. It tells the decoder what the bytes mean. Using the wrong
dictionary produces wrong data, so each one below was checked by actually decoding real files
— **not** by matching file names.

| Source | Dictionary file | Record type it decodes |
|---|---|---|
| EMSC | `asn_schema/EMSC.asn1` | `CallDataRecord` |
| PSGW | `asn_schema/PSGW.asn1` | `CallEventRecord` |
| SGSN | `asn_schema/SGSN.asn1` | `CallEventRecord` |
| TAP  | `asn_schema/TAP.310.asn1` | `DataInterChange` |

Two useful things to know:

- **TAP is version 3.10, not 3.11.** Both dictionaries look similar, so the version number
  was read out of the file contents itself. The files say version 3, release 10 → TAP 3.10.
- **`SGSN.asn1` and `GGSN.asn1` are identical files.** So are `TAP.311.asn1` and
  `TAP.311.MVR.asn1`. Don't be confused by the extra copies.

### 3.3 How files are read

- The reader watches each folder and **automatically picks up new files** as they arrive.
- It **remembers which files it has already read**, so re-running the job does not create
  duplicate rows.
- **No file-name filter is used.** The folders contain a mix of `.raw`, `.fin`, and files with
  no extension at all, so any filter would silently skip files.

---

## 4. Target details — where the data goes

Everything lands in the **`flowx.bronze`** schema. Each source gets one table, plus one
matching "quarantine" table for anything that fails.

| Source | Main table | Quarantine table |
|---|---|---|
| EMSC | `flowx.bronze.emsc_cdr_raw` | `flowx.bronze.emsc_cdr_raw_quarantine` |
| PSGW | `flowx.bronze.psgw_cdr_raw` | `flowx.bronze.psgw_cdr_raw_quarantine` |
| SGSN | `flowx.bronze.sgsn_cdr_raw` | `flowx.bronze.sgsn_cdr_raw_quarantine` |
| TAP  | `flowx.bronze.tap310_raw`   | `flowx.bronze.tap310_raw_quarantine` |

Notes on the naming:

- Network equipment sources end in **`_cdr_raw`**. TAP is a file-exchange format rather than
  a switch, so it follows the other existing convention and is just **`tap310_raw`**.
- **There is no environment suffix** (no `bronze_dev`, `bronze_prod`). In this framework,
  environments are separated by *catalog*, not by schema name.
- Data is **appended** — new rows are added, existing rows are never changed or deleted.

### 4.1 How much data is in there now

| Source | Rows | Failed to decode | Quarantined |
|---|---:|---:|---:|
| SGSN | 175,048 | 0 | 0 |
| EMSC | 353 | 0 | 0 |
| PSGW | 93 | 0 | 0 |
| TAP | 4 | 0 | 0 |
| **Total** | **175,498** | **0** | **0** |

### 4.2 Useful columns in every table

| Column | What it means |
|---|---|
| `_choice` | Which kind of CDR this row is (e.g. `sgsnPDPRecord`) |
| `_asn1_decode_error` | Empty when the record decoded fine; holds the error text if not |
| `_asn1_record_index` | Position of this record inside its source file (0, 1, 2, …) |
| `__framework_source_file_name` | Which file this row came from |
| `__framework_ingestion_timestamp_utc` | When it was loaded |
| `__framework_pipeline_run_id` | Which pipeline run loaded it |

One CDR file usually holds **many** records. `_asn1_record_index` tells you exactly which
record inside the file each row came from.

---

## 5. What the job does, step by step

The job `001_lfj_uc7_cdr_asn` runs four steps in order. If a step fails, the ones after it
do not run.

| Step | Task name | What it does |
|---|---|---|
| 1 | `setup_control_tables` | Makes sure the framework's own bookkeeping tables exist in `flowx.config`. Safe to re-run. |
| 2 | `onboard_uc7` | Reads the configuration file and saves the four source definitions into the control tables. |
| 3 | `run_pipeline_update` | Runs the pipeline: reads the files, decodes them, writes the bronze tables. |
| 4 | `observability_export` | Writes a log of how the run went to a Volume, for monitoring. |

Step 2 does not do the onboarding itself — it hands the work to the shared
**FlowX Config Onboarding** job, so there is only one onboarding entry point for the whole
project.

---

## 6. Where the configuration lives

| File | What it is |
|---|---|
| `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` | The source and target definitions — the main config |
| `resources/uc7/uc7_cdr_asn_pipeline.yml` | Defines the pipeline |
| `resources/uc7/uc7_cdr_asn_job.yml` | Defines the job and its four steps |

To change what gets loaded, edit the **JSON** file and re-run the job. You do not need to
edit any Python code.

---

## 7. What happens when a record is bad

- A record that fails to decode is **not** dropped, and it does **not** stop the pipeline.
- It is written to that source's **quarantine table** with the reason in
  `_asn1_decode_error`.
- The other records in the same file still load normally. One bad record costs you one row,
  not the whole file.

The quality checks applied to every record are deliberately light, because almost every field
in a CDR is optional. Checking a field that is legitimately empty would wrongly quarantine
good data. The checks are:

1. Did the record decode without error?
2. Was a record type identified?
3. Did that record type actually contain data?

---

## 8. Two sources that are NOT loaded — and why

**SMSC and MMSC are not part of this use case.** They were investigated and found not to be
ASN.1 data at all:

| Source | What the file actually contains |
|---|---|
| **SMSC** | A comma-separated text file (CSV) with 48 fields on one line |
| **MMSC** | A CSV file with 70+ columns, saved with a `.csv` extension |

Both were tested against **every** available ASN.1 dictionary and all attempts failed. There
is also **no `SMSC.asn1` dictionary file** in the folder at all.

Loading them as ASN.1 would quarantine 100% of their rows. They need either their real ASN.1
dictionaries, or a decision to load them as CSV instead.

---

## 9. Monitoring the runs

After each run, a compressed log file is written to:

```
/Volumes/flowx/observability/app_logs/dfg_uc7_cdr_asn/<date>/<group>_<run id>.jsonl.gz
```

It records each flow's status and ties back to the job, pipeline, and run that produced it —
useful for confirming a run really happened and what it did.

---

## 10. A copy of the data

A full copy of the UC_7 Volume was taken. **Nothing was moved or deleted** — the original is
untouched.

| | Location |
|---|---|
| Original (still in use) | `/Volumes/flowx/landing/uc_7/` |
| Copy | `/Volumes/flowx/staging/uc_7/` |

Both hold 28 files totalling ~112 MB. **The pipeline still reads from the original location.**
The copy is a backup; pointing the pipeline at it would require re-onboarding and would reload
everything from scratch.

---

## 11. How to run it

From the Databricks UI: open **Workflows**, find **`001_lfj_uc7_cdr_asn`**, click **Run now**.

From the command line:

```bash
databricks jobs run-now 843342822766009 -p metaflow_v7
```

Re-running is safe — already-loaded files are skipped, so you will not get duplicate rows.

---

## 12. Things to be aware of

| # | Item | What it means for you |
|---|---|---|
| 1 | SMSC and MMSC are not loaded | Their data is CSV, not ASN.1 (section 8) |
| 2 | Some EMSC records use a variable-length format | Not losing data today, but a file that *starts* with one of these would load as a single row |
| 3 | Full `bundle deploy` currently fails | Caused by an unrelated app resource, not this use case. Use `--select` to deploy just the UC_7 parts |
| 4 | Two naming styles now exist for bronze | This use case uses `flowx.bronze`; older ones use `flowx.bronze_<source>`. Worth agreeing on one before adding more sources |

---

## 13. Related documents

- **`UC7_CDR_ASN_Test_Report.md`** (repo root) — the full technical test report: verification
  evidence, defects found and fixed, run IDs, and detailed validation results.
- **`docs/02_ingestion_and_sources.md`** — how ASN.1 ingestion works in the framework generally.
- **`docs/08_observability_and_telemetry.md`** — how observability works across the framework.
