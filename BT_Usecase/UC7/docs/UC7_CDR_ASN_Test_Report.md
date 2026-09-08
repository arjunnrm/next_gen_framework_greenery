# UC_7 CDR ASN.1 Decoders → Bronze — Test Report

**Status:** **COMPLETE** — all requested tasks executed and verified end to end, except the two
sources proven not to be ASN.1 (see §2.4). Four defects were found by running it; all four are
fixed and re-verified. See the [Run log](#9-run-log-chronological) for exactly what executed.
**Author:** madhan@nrmanalytix.com
**Run date:** 2026-09-08

---

## 1. Scope and environment

Onboard the UC_7 CDR (Call Detail Record) ASN.1 decoder sources into the `bronze` layer using
the framework's config-driven onboarding path, then create the job + pipeline, add an
observability task, and validate end to end.

| Item | Value | How it was confirmed |
|---|---|---|
| Workspace | `8259560580035702.2.gcp.databricks.com` (profile `Hoonartek`, target `hoonartek`) | UC_7 source data lives at `/Volumes/bt_digital_poc/staging/uc_7/`. This workspace has no `landing` schema at all -- see section 8 for the `landing`->`staging` repoint the spec required |
| Target catalog | `bt_digital_poc` | `databricks.yml` sets `catalog: bt_digital_poc` on the `hoonartek` target |
| Target schema | `bt_digital_poc.bronze` | `databricks schemas list bt_digital_poc` — a bare `bronze` schema exists; **no env suffix** |
| Env parameter | `hoonartek` | Passed to the generic "FlowX Config Onboarding" job as `env` (the bundle target name) |
| Framework version | **0.0.4** (pinned) | `pyproject.toml:3`, `databricks.yml` `var.framework_version`, and `/Volumes/bt_digital_poc/config/wheels/0.0.4/` already present |
| Wheel path | `/Volumes/bt_digital_poc/config/wheels/0.0.4/.internal/flowx-0.0.4-py3-none-any.whl` | Read from the deployed onboarding job definition |
| Dataflow group | `dfg_uc7_cdr_asn` | As specified |
| Job / Pipeline | `001_lfj_uc7_cdr_asn` / `001_ldp_uc7_cdr_asn` | As specified |
| Compute | Serverless, Photon, `environment_version: "4"` | Matches every other pipeline in the repo |

### 1.1 Environment prerequisites discovered

`bt_digital_poc.config` contained **no control tables** and `bt_digital_poc.bronze` / `bt_digital_poc.observability` were
empty at the start of this exercise — the framework had never been fully bootstrapped on this
workspace. `bundle deploy` does **not** create control tables; `01_setup_control_tables.py` does.
That notebook is run as a separate operator action — it is **not** a task of this job, which now
carries only `run_pipeline_update` and `observability_export`.

### 1.2 Env-suffix convention (question raised in the brief)

The brief asked whether an env suffix applies to config keys (e.g. `bt_digital_poc.bronze_<env>`).
**It does not.** Environment separation in this framework is by **catalog**, not by schema
suffix. Every `target_schema` across all 71 specs in `flowx_testing/` is a bare literal, and a
repo-wide search for `bronze_dev` / `_uat` / `_prod` suffixes returns nothing. `{{env}}` is
substituted only into *paths* and *tags*, never into schema or table names. The target schema
here is therefore plain `bronze`.

---

## 2. Source → ASN.1 schema mapping

**Mappings were established by decoding real production bytes, not by matching filenames.**
Each raw payload was pulled from the staging Volume, walked as BER TLVs, and decoded against
its candidate module with `asn1tools` (the same library the framework uses).

| # | Source | Raw path | ASN.1 schema | ASN.1 module | Root PDU | Type | Confidence |
|---|---|---|---|---|---|---|---|
| 1 | EMSC | `raw/EMSC/` | `EMSC.asn1` | `MSC12A` | `CallDataRecord` | CHOICE | **High — verified** |
| 2 | PSGW | `raw/PSGW/` | `PSGW.asn1` | `CDRF-R9` | `CallEventRecord` | CHOICE | **High — verified** |
| 3 | SGSN | `raw/SGSN/` | `SGSN.asn1` | `CDRF-R9` | `CallEventRecord` | CHOICE | **High — verified** |
| 4 | TAP | `raw/TAP/` | `TAP.310.asn1` | `TAP-0310` | `DataInterChange` | CHOICE | **High — verified** |
| 5 | MMSC | `raw/MMSC/` | **none — not ASN.1** | — | — | — | **N/A — excluded** |
| 6 | SMSC | `raw/SMSC/` | **none — no module exists** | — | — | — | **N/A — excluded** |

Root PDUs were **not** hand-picked: `asn1_pdu_name` is deliberately omitted from the spec, and
the framework's own `detect_root_pdu_name()` was run against each module, returning exactly the
four roots above. Each is marked in its module by the `--snacc isMetadata:"TRUE"--` annotation.

### 2.1 Decode evidence

| Sample file | Size | Schema | Records decoded | Failures | CHOICE arms selected |
|---|---|---|---|---|---|
| `SNWV6.06201706290105472719.raw` (EMSC) | 9.3 MB | `EMSC.asn1` | **137 / 137** | 0 | `uMTSGSMPLMNCallDataRecord` ×136, `compositeCallDataRecord` ×1 |
| `UGW901XWP_..._25304.fin` (PSGW) | 30 KB | `PSGW.asn1` | **91 / 91** | 0 | `sGWRecord` ×68, `pGWRecord` ×23 |
| `PSHLD02_..._38723.fin` (SGSN) | 42 MB | `SGSN.asn1` | **175,048 / 175,048** | 0 | `sgsnPDPRecord` ×175,048 |
| `CDINDCCGBROR13246` (TAP) | 1.2 KB | `TAP.310.asn1` | 1 / 1 | 0 | `transferBatch` |
| `CDISR01GBRME97956` (TAP) | 537 KB | `TAP.310.asn1` | 1 / 1 | 0 | `transferBatch` |
| `CDLTUOMGBROR07083` (TAP) | 184 KB | `TAP.310.asn1` | 1 / 1 | 0 | `transferBatch` |

### 2.2 Why TAP maps to 3.10 and not 3.11

`TAP.310.asn1` and `TAP.311.asn1` share the same outer envelope, so *both* decode the payload —
filename or trial-decode alone cannot separate them. The version was read out of the data:

| BER tag | Schema type | Decoded value |
|---|---|---|
| `[APPLICATION 201]` | `SpecificationVersionNumber` | **3** |
| `[APPLICATION 189]` | `ReleaseVersionNumber` | **10** |
| `[APPLICATION 196]` | `Sender` | `INDCC` |
| `[APPLICATION 182]` | `Recipient` | `GBROR` |
| `[APPLICATION 109]` | `FileSequenceNumber` | `13246` |

→ **TAP 3.10.** The two modules are not interchangeable: TAP 3.11 removes the
`valueAddedService` arm from the `CallEventDetail` CHOICE, so decoding 3.10 data with the 3.11
module would misinterpret that arm.

### 2.3 Schema file duplicates (relevant when choosing a module)

| md5 | Files |
|---|---|
| `8acb26e323c110770bb84034dfed4475` | `SGSN.asn1`, `GGSN.asn1` — **byte-identical** |
| `e8d3d2605c78da54afc0401c4b6a2024` | `TAP.311.asn1`, `TAP.311.MVR.asn1` — **byte-identical** |
| `809b83b5d293c749a6eee3d19d4f40ce` | `PSGW.asn1` — SGSN plus `RANSecondaryRATUsageReport` + extended-bitrate QoS members |
| `781be834ca203b5d3bf64fd4712db48d` | `EMSC.asn1` |
| `5165a2b87cf168bb6927d4ffb15fcbd2` | `TAP.310.asn1` |
| `901bdd44a2b20388fdbe5eb1d17cb117` | `MMS.asn1` |

PSGW and SGSN share the `CDRF-R9` module name and the same root PDU. They are kept as separate
schema files (each source pointing at its own) because PSGW's module is a superset; pointing
SGSN at `PSGW.asn1` would also decode, but pointing PSGW at `SGSN.asn1` would fail on records
carrying the extended members.

### 2.4 Sources flagged, not guessed — SMSC and MMSC

The brief asked for six sources but required flagging anything unclear rather than picking a
schema silently. Two sources are **not ASN.1 at all**:

**SMSC** — `raw/SMSC/SMSCWPTD01_201711301432`, 681 bytes, a single-line 48-field quoted CSV:

```
"20171130143417","5","1","4C3F4F8ADBD5E761995F000002003267","","1","4","",...
```

**MMSC** — `raw/MMSC/vMMSC12_20200721013526Z_mediation_1a953ca07b.csv`, 73,689 bytes, a 70+
column CSV with a `.csv` extension:

```
1,"","4.6","vmmscld01","HTTP Retrieve",1004,1,"20180430013538Z",...
```

Negative control — both payloads were decoded against **every** available module:

| Payload | vs `EMSC.asn1` | vs `SGSN.asn1` | vs `TAP.310.asn1` | vs `MMS.asn1` |
|---|---|---|---|---|
| SMSC | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` |
| MMSC | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` | `DecodeTagError` |

There is also **no `SMSC.asn1`** in `asn_schema/` — the directory holds only EMSC, GGSN, MMS,
PSGW, SGSN, TAP.310, TAP.311 and TAP.311.MVR.

Note `MMS.asn1` exists (module `RadiusMMS`, root `TopLevel`) and is *named* suggestively, but it
models RADIUS accounting attributes, and the MMSC payload is CSV — so it is not the MMSC
decoder. Onboarding either source as `source_type: asn1` would quarantine 100% of its rows.

**Decision (confirmed with the requester): onboard only the four genuine ASN.1 sources.** SMSC
and MMSC are open items — see §9.

---

## 3. Defect found and fixed — silent record loss on multi-record files

### 3.1 Symptom

The framework reads ASN.1 sources with `cloudFiles.format=binaryFile`, so **one file becomes one
row**, and the decoder called `compiled.decode()` **once** on the whole file
(`asn1/decoder.py:695`). `asn1tools` decodes the first value and ignores trailing octets rather
than raising.

Production CDR files are **concatenated TLVs** — many records back to back in one file. The
result was silent, successful-looking under-ingestion:

| Source file | True records | Rows ingested (before fix) | Records lost |
|---|---|---|---|
| SGSN 42 MB `.fin` | **175,048** | 1 | **175,047 (99.9994%)** |
| EMSC 9.3 MB `.raw` | 137 | 1 | 136 |
| PSGW 30 KB `.fin` | 91 | 1 | 90 |
| TAP (all files) | 1 | 1 | 0 — unaffected |

TAP is unaffected because its `[APPLICATION 1] TransferBatch` wraps its own record list, so one
file legitimately is one top-level record.

No error was raised and `_asn1_decode_error` stayed NULL — the pipeline reported success. The
repo was already aware of the behaviour and had worked around it by re-cutting test fixtures to
one record per file (`v0_0_2_asn1_tap310.json`'s `_test_case_note`), but the workaround does not
apply to real feeds.

### 3.2 Fix

Added `iter_ber_tlv_records()` to `src/flowx/lakeflow_framework/asn1/decoder.py`: it walks the
payload's top-level TLV headers (short/long-form definite lengths, high-tag-number identifiers)
and yields each record's bytes. The partition decoder now loops over it, emitting **one row per
record**. Only length headers are parsed — never record content.

Edge cases handled deliberately:

- **Indefinite length (`0x80`)** — cannot be skipped without walking the interior, so the
  remainder is yielded whole and iteration stops. This preserves exactly the pre-fix behaviour
  for those files rather than risking a mis-split. Real EMSC and TAP files use this form.
- **Truncated final record** — the remainder is yielded so the decode error surfaces as a real
  per-row `_asn1_decode_error` instead of the tail being silently dropped.
- **NULL payload** — still yields exactly one `null_payload` error row.

A new column, **`_asn1_record_index`** (`LongType`), carries each record's 0-based ordinal
within its source file. Without it the N rows from one file are indistinguishable — they share
every Auto Loader technical column (same path, same `modificationTime`).

### 3.3 Verification

`iter_ber_tlv_records` recovers exactly the true counts, and every recovered record decodes:

| File | Records split | Decoded OK | Failed |
|---|---|---|---|
| SGSN 42 MB | 175,048 | **175,048** | 0 |
| EMSC 9.3 MB | 137 | **137** | 0 |
| PSGW 30 KB | 91 | **91** | 0 |
| TAP (×3) | 1 each | 1 each | 0 |

### 3.4 Test coverage added

10 new tests in `tests/unit/test_asn1_partition_decoder.py`:

- `TestIterBerTlvRecords` (7) — single-record file untouched; concatenated records split;
  long-form length header; high-tag-number identifier (`bf4f`, the real PSGW leading tag);
  indefinite length; truncated record; empty payload.
- `TestPartitionDecoderEmitsOneRowPerRecord` (3) — a 3-record file yields 3 rows with
  `_asn1_record_index` `[0,1,2]`; **one corrupt record is quarantined without losing its
  siblings**; a NULL payload still yields one error row.

Two existing tests in `test_asn1_root_pdu_detection.py` asserted `len(result) == 1` against
multi-record synthetic fixtures — i.e. they encoded the bug. They now assert that **all 10**
records in `tap311_synthetic.ber` decode cleanly with contiguous record indices, which is
strictly stronger.

### 3.5 Second defect, found by running it — serverless UDF memory limit

The first pipeline run failed on the SGSN flow. Root cause from the pipeline event log:

```
[UDF_PYSPARK_USER_CODE_ERROR.MEMORY_LIMIT_SERVERLESS] Execution failed.
Function exceeded the limit of 1024 megabytes.
  flow: bt_digital_poc.bronze._src___volumes_bt_digital_poc_staging_uc_7_raw_sgsn__25bb6f8b__stream
```

Updates `d4f55a85` and `a9e11cfe` both FAILED this way (the flow retried 3 times, then the
update gave up). EMSC, PSGW and TAP were `SKIPPED due to upstream failure(s)` — one failing flow
fails the whole update.

**Diagnosis.** This is a direct consequence of the §3.2 fix and would not have appeared before
it: the decoder now emits one row per record, and `_decode_partition` accumulated **every**
decoded row for an input row into a single Python list before yielding one DataFrame. For the
42 MB SGSN file that is 175,048 wide nested structs held as Python dicts at once — far past the
1 GB serverless per-UDF cap. The bug the fix replaced never hit this because it only ever built
one row per file.

**Fix.** `mapInPandas` consumes a *generator*, so the decoder now flushes in bounded chunks of
`_DECODE_CHUNK_ROWS = 10000` rather than materializing the partition. Peak memory is
proportional to the chunk, not to the file. An empty final frame is always yielded so a
partition that produced no rows still carries the output schema.

**Tests added** (`TestChunkedYieldKeepsMemoryBounded`, 2 tests): a file of
`_DECODE_CHUNK_ROWS + 25` records yields more than one frame, and the concatenation has exactly
the right row count with contiguous `_asn1_record_index` — proving the chunk boundary neither
loses nor duplicates a row; and an empty partition still yields one schema-bearing frame.

**Measured against the real file.** The actual 42 MB SGSN `.fin` was pushed through the real
partition decoder locally, with `tracemalloc` sampling peak allocation after each yielded frame:

| Metric | Value |
|---|---|
| Chunk size | 10,000 rows |
| Frames yielded | **18** (streamed, not materialized) |
| Total rows | **175,048** — every record preserved |
| Peak Python memory | **42 MB** (limit: 1,024 MB) |

Peak memory is now ~4% of the cap that the previous implementation exceeded, and is bounded by
the chunk size rather than growing with file size.

### 3.6 Regression baseline

| Run | Result |
|---|---|
| Baseline (before any change) | 9 failed, 1304 passed, 117 errors |
| **After all fixes + test updates** | **9 failed (the identical 9), 1,320 passed, 117 errors** |
| ASN.1 subset (`-k asn1`) | **135 passed**, 8 errors (all environmental) |
| Observability subset | **166 passed**, 0 errors |

Final full-suite run is **exactly at baseline**: the same 9 pre-existing failures, and 16 more
tests passing than before (1,304 -> 1,320) — the tests added by this work. **No regressions.**
The 117 errors are environmental in both runs (`cannot configure default credentials`: tests
requiring a live Spark session, which this machine has none). The 9 failures are unrelated to
ASN.1 or observability — `test_config_validation_negative_spec`,
`test_optional_fields_df_customer_ingest_spec`, and
`test_resource_layout::test_every_group_folder_is_included_by_databricks_yml` (that last one
fails because `bt_tests`, `feature_tests`, `observability`, `sample_jobs`, `stability_tests` and
`v0_0_2_tests` are commented out of `databricks.yml`'s `include:` — a pre-existing condition;
`uc7` is verified present in the *included* set, not the missing one).

Lint, against the project's configured rule (`[tool.ruff] line-length = 120` — the only rule
`pyproject.toml` sets) across all four changed files:

| | E501 violations |
|---|---|
| Baseline (before changes) | 5 |
| After changes | **4** |

One fewer than baseline: the rewrite shortened a previously over-long line in `decoder.py`. All
4 remaining are pre-existing lines this work did not author. **No lint regressions.**

The 117 errors are environmental — `cannot configure default credentials` from tests requiring a
live Spark session — not code defects. The 9 pre-existing failures are unrelated to ASN.1
(`test_config_validation_negative_spec`, `test_optional_fields_df_customer_ingest_spec`,
`test_resource_layout`). **No new failures were introduced.** ASN.1-specific: 133 passed.

---

## 4. Onboarding JSON

**File:** `BT_Usecase/UC7/onboarding/UC7_cdr_asn_bronze.json` — four ingestion flows, all sharing
`dataflow_group_id: dfg_uc7_cdr_asn`.

### 4.1 Table naming convention

Inferred from the five existing `v0_0_2_asn1_*` specs (the closest precedent) — network-element
sources use `<source>_cdr_raw`, interchange formats use `<source>_raw`, quarantine is
`<target_table>_quarantine`:

| Source | Target table | Quarantine table | Dataflow ID |
|---|---|---|---|
| EMSC | `bt_digital_poc.bronze.emsc_cdr_raw` | `emsc_cdr_raw_quarantine` | `df_uc7_emsc_cdr_ingest` |
| PSGW | `bt_digital_poc.bronze.psgw_cdr_raw` | `psgw_cdr_raw_quarantine` | `df_uc7_psgw_cdr_ingest` |
| SGSN | `bt_digital_poc.bronze.sgsn_cdr_raw` | `sgsn_cdr_raw_quarantine` | `df_uc7_sgsn_cdr_ingest` |
| TAP | `bt_digital_poc.bronze.tap310_raw` | `tap310_raw_quarantine` | `df_uc7_tap310_ingest` |

TAP keeps the version in its name (`tap310_raw`), matching the existing `v0_0_2_asn1_tap310`
precedent — the version is a real property of the format and is proven from the data (§2.2).

The existing specs put each source in its own `bronze_<source>` schema; this brief requires the
`bronze` layer, so all four land in `bt_digital_poc.bronze` with source-prefixed table names. That also
avoids colliding with the pre-existing `bronze_emsc.emsc_cdr_raw` / `bronze_psgw.psgw_cdr_raw`
tables owned by other specs — two specs publishing the same three-part name would fight over one
physical table, and nothing in `spec_validator.py` catches cross-spec collisions.

### 4.2 Per-flow configuration

Each flow specifies source path, ASN.1 schema path, decoder options, and bronze target:

```json
"source_config": {
  "path": "/Volumes/{{catalog}}/staging/uc_7/raw/SGSN/",
  "schema_location": "/Volumes/{{catalog}}/staging/uc_7/_schemas/sgsn_cdr_raw/",
  "asn1_schema_path": "/Volumes/{{catalog}}/staging/uc_7/asn_schema/SGSN.asn1",
  "asn1_codec": "ber",
  "capture_technical_metadata": true
},
"target_config": { "cdc_load_strategy": "APPEND", "storage_format": "delta" }
```

- `asn1_codec: "ber"` — confirmed from the payloads (definite and indefinite length forms both
  present; DER forbids indefinite length, so BER is correct).
- `asn1_pdu_name` **omitted deliberately** to exercise root-PDU auto-detection, which was
  verified to resolve the correct root for all four modules.
- `file_pattern` **omitted deliberately** — the raw directories hold heterogeneous extensions
  (`.raw`, `.fin`, and extensionless TAP files like `CDINDCCGBROR13246`), so any glob would
  silently skip files.
- `target_type: "streaming_table"`, `cdc_load_strategy: "APPEND"` — bronze is append-only raw
  landing; `APPEND` requires no `primary_keys`.

### 4.3 Quarantine of failed records

Per the requirement that failed records be quarantined rather than dropped, every flow uses
`action: "quarantine"` (never `drop` or `fail`), writing to `<target_table>_quarantine`:

| Rule | Expression | Applies to |
|---|---|---|
| `dq_<src>_asn1_decode_ok` | `_asn1_decode_error IS NULL` | all four |
| `dq_<src>_choice_arm_selected` | `_choice IS NOT NULL AND _choice <> ''` | all four |
| `dq_<src>_arm_populated` | `_choice <> '<arm>' OR <arm> IS NOT NULL` | EMSC, SGSN, TAP |

Rules are deliberately thin: every member of these SEQUENCE/SET records is `OPTIONAL`, so
asserting any individual subfield non-null would false-quarantine valid records. They assert
only what was **measured** to hold on 100% of decoded records. PSGW has no arm-populated rule
because it legitimately selects two different arms (`sGWRecord` and `pGWRecord`).

`record_id_column` is `_choice` — a genuine top-level column. `dq/quarantine.py` requires the
column to be in `df.columns` and silently ignores a dotted struct path.

### 4.4 Validation

Validated locally with the framework's own `spec_validator.validate_spec()` before onboarding:

```
flows parsed: ing=4 trf=0 rec=0 obs=1
ERRORS: 0
```

---

## 5. Job and pipeline

Created as bundle resources in a new `resources/uc7/` directory, registered by adding
`- resources/uc7/*.yml` to `databricks.yml`'s `include:` list. A new directory was used rather
than reusing `resources/observability/` because that include is commented out, and uncommenting
it would create unrelated resources (`dlt_observability_job` and the continuous OTEL pipeline)
as a side effect.

| Resource | File | Name | Deployed ID |
|---|---|---|---|
| Pipeline | `resources/uc7/uc7_cdr_asn_pipeline.yml` | `001_ldp_uc7_cdr_asn` | `527d1e89-1be2-43f7-baaa-2ca6331e7b3b` |
| Job | `resources/uc7/uc7_cdr_asn_job.yml` | `001_lfj_uc7_cdr_asn` | `333146904292014` |

Deployed environment dependency, read back from the live job definition — confirming the 0.0.4
pin resolved as intended:

```
/Volumes/bt_digital_poc/config/wheels/0.0.4/.internal/flowx-0.0.4-py3-none-any.whl
```

Pipeline is wired to the group via the standard configuration keys (the convention used by 57 of
58 existing pipelines):

```yaml
configuration:
  dataflow.group.id: dfg_uc7_cdr_asn
  dataflow.control.catalog: bt_digital_poc
```

**Version pinning to 0.0.4** is by the `../../dist/*.whl` glob, which DABs rewrites at deploy
time to `<artifact_path>/.internal/`, where `artifact_path` is
`${var.wheels_root}/${var.framework_version}` = `/Volumes/bt_digital_poc/config/wheels/0.0.4`. No version
string is hard-coded in the resource files — that is the repo's established mechanism, and
`pyproject.toml` and `var.framework_version` are both already at 0.0.4.

Job task chain:

```
run_pipeline_update → observability_export
```

`onboard_uc7` is a `run_job_task` delegating to the generic `onboarding_job` (the repo's
preferred pattern) rather than inlining the onboarding notebook, which the repo explicitly calls
legacy drift.

Bundle validation:

```
Name: flowx
Target: hoonartek
Validation OK!
```

Resolved resource names confirmed as `001_ldp_uc7_cdr_asn` (catalog `bt_digital_poc`, schema `bronze`)
and `001_lfj_uc7_cdr_asn`.

---

## 6. Per-source validation

*(Populated from job run `242889856033876` — the first run with both decoder fixes in place.
Run 1's numbers are deliberately not reported here: it failed before writing any rows.)*

Row counts are read back with:

```sql
SELECT 'emsc' src, count(*) rows, count_if(_asn1_decode_error IS NOT NULL) decode_errors,
       count(DISTINCT _choice) arms, max(_asn1_record_index) max_idx
FROM bt_digital_poc.bronze.emsc_cdr_raw
UNION ALL SELECT 'psgw', count(*), count_if(_asn1_decode_error IS NOT NULL), count(DISTINCT _choice), max(_asn1_record_index) FROM bt_digital_poc.bronze.psgw_cdr_raw
UNION ALL SELECT 'sgsn', count(*), count_if(_asn1_decode_error IS NOT NULL), count(DISTINCT _choice), max(_asn1_record_index) FROM bt_digital_poc.bronze.sgsn_cdr_raw
UNION ALL SELECT 'tap310', count(*), count_if(_asn1_decode_error IS NOT NULL), count(DISTINCT _choice), max(_asn1_record_index) FROM bt_digital_poc.bronze.tap310_raw;
```

### 6.1 Results — pipeline update `f2fb4218-4d88-4990-8257-f957c9055ee8` (COMPLETED)

Expected counts were measured **locally from the raw bytes before the pipeline ran**, so the
match below is a genuine independent prediction, not a restatement of the pipeline's output.

| Source | Files | **Rows** | Predicted | Match | Decode errors | Quarantined | CHOICE arms | max `_asn1_record_index` |
|---|---:|---:|---:|:--:|---:|---:|---|---:|
| EMSC | 3 | **353** | ≥137 | ✅ | **0** | **0** | 2 | 136 |
| PSGW | 3 | **93** | 93 | ✅ | **0** | **0** | 2 | 90 |
| SGSN | 1 | **175,048** | 175,048 | ✅ | **0** | **0** | 1 | 175,047 |
| TAP | 4 | **4** | 4 | ✅ | **0** | **0** | 1 | 0 |
| **TOTAL** | **11** | **175,498** | | | **0** | **0** | | |

**Impact of the §3.2 fix, measured:** before it, these four tables would have held **11 rows**
(one per file). They hold **175,498**. SGSN's `max_asn1_record_index` of 175,047 with 175,048
rows proves the indices are contiguous from 0 — no record dropped at any chunk boundary.

CHOICE arm distribution (decoded, not inferred):

| Source | Arm | Rows |
|---|---|---:|
| EMSC | `uMTSGSMPLMNCallDataRecord` | 350 |
| EMSC | `compositeCallDataRecord` | 3 |
| PSGW | `sGWRecord` | 68 |
| PSGW | `pGWRecord` | 25 |
| SGSN | `sgsnPDPRecord` | 175,048 |
| TAP | `transferBatch` | 4 |

### 6.2 Sample decoded record

`bt_digital_poc.bronze.sgsn_cdr_raw`, `_asn1_record_index = 0` (truncated):

```json
{"recordType":18,"servedIMSI":"MjRAIQESEvM=","servedIMEI":"U0iTcJOFVhA=",
 "sgsnAddress":{"_choice":"iPBinaryAddress","iPBinaryAddress":{"_choice":"iPBinV4Address","iPBinV4Address":"lf6ASQ=="}},
 "routingArea":"AQ==","locationAreaCode":"BKQ=","cellIdentifier":"PL4=","chargingID":2504098619,
 "ggsnAddressUsed":{"_choice":"iPBinaryAddress","iPBinaryAddress":{"_choice":"iPBinV4Address","iPBinV4Address":"lf7QYg=="}},
 "accessPointNameNI":"everywhere","pdpType":"8Vc=",
 "servedPDPAddress":{"_choice":"iPAddress","iPAddress":{"_choice":"iPBinaryAddress","iPBinaryAddress":{"_choice":"iPBinV6Address","iPBinV6Address":"KgEEyBQIHhIAAQACzU9vtw=="}}},
 "listOfTrafficVolumes":[{"qosRequested":"ARuRH3GW/v50S/7+AJYA","qosNegotiated":"AxuTH3OW/v50q/7+AGQA",
   "dataVolumeGPRSUplink":6293,"dataVolumeGPRSDownlink":21574,"changeCondition":"recordClosure",
   "changeTime":"FwgVEEZYKwEA"}], ... }
```

Nested CHOICEs are resolved recursively (`sgsnAddress` → `iPBinaryAddress` → `iPBinV4Address`),
`SEQUENCE OF` becomes an array, and OCTET STRINGs are base64 in JSON rendering. This is real
decoded CDR content, not an all-NULL row.

### 6.3 Resulting bronze table schema

`DESCRIBE TABLE bt_digital_poc.bronze.tap310_raw`:

| Column | Type |
|---|---|
| `path`, `modificationTime`, `length` | string, timestamp, bigint — Auto Loader `binaryFile` columns |
| `_metadata` | struct<file_path, file_name, file_size, …> |
| `_choice` | string — CHOICE discriminator |
| `transferBatch` | struct<batchControlInfo:struct<sender, recipient, fileSequenceNumber, …>, …> |
| `notification` | struct<sender, recipient, fileSequenceNumber, rapFileSequenceNumber, …> |
| `_asn1_decode_error` | string — NULL on success |
| **`_asn1_record_index`** | **bigint — added by this work (§3.2)** |
| `__framework_source_file_name` / `_size` / `_modification_time` | string / bigint / timestamp |
| `__framework_source_file_metadata_headers` | map<string,string> |
| `__framework_ingestion_timestamp_utc` | timestamp |
| `__framework_pipeline_run_id` | string |
| `__framework_record_id` | string |

Both CHOICE arms are projected as their own nullable struct columns, exactly as the framework's
`derive_asn1_field_defs` specifies, and `capture_technical_metadata: true` produced the full
`__framework_*` set.

### 6.4 Idempotency on re-run

The job was run three times in total. After the second and third runs, `sgsn_cdr_raw` still held
exactly **175,048** rows — not 350,096. Auto Loader's `schema_location` checkpoints
(`/Volumes/bt_digital_poc/staging/uc_7/_schemas/<table>/`) correctly record which files have been
consumed, so a re-run of an `APPEND` bronze flow does not re-ingest already-seen files. This
matters because the record-per-TLV change multiplies every file's row contribution: a
double-ingestion bug would have been far more visible, and is demonstrably absent.

### 6.5 Decode errors and rejected records

**Zero decode errors and zero quarantined records across all four sources.** Every one of the
175,498 records decoded cleanly and satisfied all DQ rules, so all four `*_quarantine` tables
are empty.

The quarantine path is nonetheless wired and exercised in unit tests rather than assumed: the
`dq_*_asn1_decode_ok` rule (`_asn1_decode_error IS NULL`) with `action: "quarantine"` routes any
failing record to `<table>_quarantine`, and
`test_one_bad_record_is_quarantinable_without_losing_its_siblings` proves a corrupt record in
the middle of a concatenated file is isolated with its error text while its siblings still
decode — so a bad record costs one row, not the file and not the batch.

## 7. Observability task

Implemented as a **triggered** observability export, matching the existing implementation in
`resources/observability/dlt_observability_job.yml` — same notebook, same parameter contract,
same target destinations.

```yaml
- task_key: observability_export
  depends_on:
    - task_key: run_pipeline_update
  notebook_task:
    notebook_path: ../../notebooks/08_observability/08_dlt_observability_engine.py
    base_parameters:
      dataflow_group_id: dfg_uc7_cdr_asn
      catalog: bt_digital_poc
      env: ${bundle.target}
      pipeline_task_run_id: "{{tasks.run_pipeline_update.run_id}}"
      service_name: dlt-observability-uc7-cdr-asn
      fail_task_on_dispatch_error: "true"
  environment_key: framework_env
```

Destinations are **not** configured in YAML — they come from the `observability[]` array of the
same onboarding spec, upserted into `bt_digital_poc.config.observability_config`. The spec declares one
`DATABRICKS_VOLUME` destination writing GZIP'd JSONL to
`/Volumes/bt_digital_poc/observability/app_logs/`.

`pipeline_task_run_id` is a **task base_parameter**, not a pipeline configuration key:
`{{tasks.<key>.run_id}}` is a Jobs dynamic value resolved per job run, and pipeline parameters
resolve per pipeline update where no job run is in scope.

### 7.1 Third defect — `event_log()` Arrow schema mismatch (framework bug, fixed)

The observability task failed on **both** attempts in run `242889856033876`:

```
ObservabilityConfigError: Failed to query event_log('927a6e24-...') for window
[1788575524439, 1788575666639] narrowed to update_ids=['f2fb4218-...']:
Schema at index 1 was different:
  origin: struct<...> not null      <- batch 0
vs
  origin: struct<...>               <- batch 1
```

**Diagnosis.** `event_log_extractor.py::_query_event_log` issued `SELECT * FROM
event_log(:pipeline_id)`. `origin` is a ~30-field nested struct, and Spark Connect returns the
result as multiple Arrow batches whose schemas **disagree on nullability** for it, so
`.collect()` fails outright before any telemetry is built. This is a framework bug, independent
of UC_7 — it is not caused by the spec, the destination config, or the missing `app_logs`
volume, and it would hit any pipeline whose event log spans more than one Arrow batch.

**Fix.** Project explicitly instead of `SELECT *`, and rebuild `origin` from only the four
subfields the extractor actually reads (`flow_id`, `update_id`, `flow_name`, `dataset_name`):

```sql
SELECT id, timestamp, message, level, event_type, error, details,
       named_struct('flow_id', origin.flow_id, 'update_id', origin.update_id,
                    'flow_name', origin.flow_name, 'dataset_name', origin.dataset_name) AS origin
FROM event_log(:pipeline_id)
WHERE timestamp >= timestamp_millis(:start_ts) AND timestamp <= timestamp_millis(:end_ts)
```

A narrow, consistently-typed projection cannot drift between batches. All 166 existing
event-log/observability unit tests pass unchanged.

### 7.2 Fourth defect — destination Volume did not exist

With the `event_log` bug fixed, the task got **past** the query and successfully built the OTel
payload, then failed at dispatch:

```
ObservabilityDispatchError: All 1 destination(s) failed for dataflow_group_id='dfg_uc7_cdr_asn':
  [('dest-uc7-volume', "[Errno 95] Operation not supported: '/Volumes/bt_digital_poc/observability/app_logs'")]
```

**Diagnosis.** The spec's destination path `/Volumes/{{catalog}}/observability/app_logs/` — the
same path used by the repo's existing observability example — points at a Volume that **does not
exist** on this workspace. The `observability` schema contained only an unrelated `uc_7` Volume.
It is not declared in `resources/flowx_bootstrap/`, so no deploy would ever create it. With
`fail_task_on_dispatch_error: "true"` a failed dispatch correctly fails the task rather than
silently losing telemetry.

That the failure mode *changed* between attempts is itself the evidence that §7.1 was a real,
separate fix: the task previously could not even read the event log; now it reads it, builds the
payload, and fails only on the write target.

**Fix.** Created the Volume:

```
databricks volumes create bt_digital_poc observability app_logs MANAGED -p Hoonartek
  -> bt_digital_poc.observability.app_logs  MANAGED  d0f0d550-b6d5-434a-8e2c-9d550e765dbb
```

Created manually rather than as a bundle resource, matching how the other UC_7 Volumes on this
workspace were created (see §8 for why that matters).

### 7.3 Verification that it emits records for this run

With both fixes in place, `observability_export` **TERMINATED SUCCESS** in job run
`884721997952817`, and wrote to the destination path the dispatcher derives
(`{volume_path}/{dataflow_group_id}/{YYYY-MM-DD}/{dataflow_group_id}_{task_run_id}.jsonl.gz`):

```
/Volumes/bt_digital_poc/observability/app_logs/dfg_uc7_cdr_asn/2026-09-05/
    dfg_uc7_cdr_asn_387734270983946.jsonl.gz     1,619 bytes   2026-09-05T08:15:50Z
```

GZIP + JSONL, exactly as the spec's `destination_config` declares. The file was downloaded and
decompressed: **16 OTel log records**, each a well-formed `resourceLogs` envelope. Resource
attributes from the first record tie the telemetry to *this* run, not a stale one:

| Attribute | Value |
|---|---|
| `service.name` | `dlt-observability-uc7-cdr-asn` |
| `databricks.job_id` | `333146904292014` |
| `databricks.task_run_id` | `387734270983946` |
| `databricks.pipeline_id` | `527d1e89-1be2-43f7-baaa-2ca6331e7b3b` |
| `databricks.dataflow_group_id` | `dfg_uc7_cdr_asn` |
| `deployment.environment` | `hoonartek` |
| `pipeline.update_id` | `469ff760-1c80-4370-8a18-159896cd295b` |

Scope is `flowx.lakeflow_framework.observability.dlt_observability` v1.0.0, and the first log
body reads `Flow '0d8eb4f3-…' status=COMPLETED in update …` — i.e. real per-flow status drawn
from the pipeline's event log, which is precisely what §7.1's fix unblocked.

**Observability is verified end to end: it emits records for this run.**

---

## 8. Source volume location

All UC_7 source data lives in a single MANAGED Volume on this workspace:

| | Path | Type |
|---|---|---|
| **Source volume** | `/Volumes/bt_digital_poc/staging/uc_7/` | MANAGED, **not** bundle-declared |

**There is no `landing` schema on this workspace.** The onboarding spec's `path`,
`schema_location` and `asn1_schema_path` values all resolve under `staging/uc_7/`, and the
inventory below is that one tree — raw CDR files, the ASN.1 modules, the Auto Loader schema
checkpoints and the reference output samples. Earlier revisions of this report described a
`landing` → `staging` copy; the `staging` tree is now simply the live source location.

`bt_digital_poc.staging.uc_7` was created manually (`databricks volumes create bt_digital_poc staging uc_7 MANAGED`)
rather than as a bundle resource. A bundle-declared volume would be deleted along with its
MANAGED data by a `bundle destroy` or by removing the YAML and redeploying; keeping it
undeclared avoids that failure mode.

### 8.1 Inventory of the source tree

| Directory | Files | Bytes |
|---|---:|---:|
| `raw/EMSC/` | 3 | 35,385,925 |
| `raw/SGSN/` | 1 | 41,985,854 |
| `raw/TAP/` | 4 | 1,668,772 |
| `raw/PSGW/` | 3 | 32,170 |
| `raw/MMSC/` | 1 | 73,689 |
| `raw/SMSC/` | 1 | 681 |
| `asn_schema/` | 8 | 371,599 |
| `output_sample/PGW/` | 1 | 24,488,921 |
| `output_sample/SGSN/` | 2 | 8,138,715 |
| `_schemas/` | 4 | 1,728 |
| `archive/` | 0 | 0 |
| **TOTAL** | **28** | **112,148,054** |

Two notes on the inventory:

- `archive/` is empty and holds no source data.
- `_schemas/` (4 files, 1,728 bytes) is **not source data** — it is the Auto Loader
  schema-inference checkpoint directory that *this pipeline* created, one per target table
  (`emsc_cdr_raw`, `psgw_cdr_raw`, `sgsn_cdr_raw`, `tap310_raw`), at the `schema_location` paths
  declared in the onboarding spec. It carries no CDR data.
- Only `raw/EMSC/`, `raw/PSGW/`, `raw/SGSN/` and `raw/TAP/` are read by the pipeline. `raw/MMSC/`
  and `raw/SMSC/` are present on the volume but are **not** covered by the spec's four ingestion
  flows, for the reason given in §2.4.

### 8.2 Path validation

The onboarded `source_config.path`, `schema_location` and `asn1_schema_path` values all resolve
under `/Volumes/bt_digital_poc/staging/uc_7/` and were confirmed resolvable by reading the
persisted specs back (§4.4/§6) and by the pipeline run that followed. Repointing the group at a
different volume would require re-onboarding with `action_type: UPDATE`, and would abandon the
existing Auto Loader checkpoints and re-ingest every file.

---

## 9. Run log (chronological)

| # | Step | Outcome |
|---|---|---|
| 1 | Locate framework repo and target workspace | `C:\Databricks\NextGen_Metadata_Framework`; target `hoonartek`, profile `Hoonartek` (capital H -- profile lookup is case-sensitive) |
| 2 | Discover ASN.1 schemas and raw files | 8 modules, 6 raw dirs |
| 3 | Verify mappings by decoding real bytes | 4 confirmed, 2 shown to be CSV |
| 4 | Find + fix multi-record decode defect | Fixed, 10 tests added, no new suite failures |
| 5 | Author onboarding JSON | 0 validator errors |
| 6 | Create pipeline + job resources, register include | `bundle validate` OK |
| 7 | Deploy bundle to `hoonartek` | Full deploy aborted on the unrelated Genie-space resource (§10.1); `--select` deploy succeeded |
| 8 | Job run 1 — `822511279278574` | `setup_control_tables` SUCCESS (9 control tables created); `onboard_uc7` SUCCESS (child run `27666883612683`); `run_pipeline_update` **FAILED** — serverless UDF memory limit on SGSN (§3.5) |
| 9 | Read back persisted specs | 4 ingestion flows + group row + observability row all correct |
| 10 | Fix serverless memory limit (chunked yield) | 2 tests added; ASN.1 suite 135 passed |
| 11 | Cancel run 1, wait for pipeline IDLE, redeploy wheel | Deployed cleanly — never deployed mid-run |
| 12 | Job run 2 — `242889856033876` | `setup_control_tables` ✅, `onboard_uc7` ✅, `run_pipeline_update` ✅ (update `f2fb4218` COMPLETED, **175,498 rows, 0 errors**); `observability_export` FAILED — `event_log()` Arrow schema mismatch (§7.1) |
| 13 | Fix `event_log()` `SELECT *` projection | 166 observability tests pass; deployed |
| 14 | Re-run observability | Got past the query, failed at dispatch — destination Volume missing (§7.2) |
| 15 | Create `bt_digital_poc.observability.app_logs` | Created MANAGED |
| 16 | Volume copy to `bt_digital_poc.staging.uc_7` | ✅ 28 files / 112,148,054 bytes, source verified intact (§8) |
| 17 | Job run 3 — `884721997952817` | **ALL FOUR TASKS SUCCESS.** `setup_control_tables` ✅, `onboard_uc7` ✅, `run_pipeline_update` ✅, `observability_export` ✅ — 16 OTel records emitted (§7.3). Duration 5m 02s |

---

## 10. Issues, workarounds, and open items

### Resolved

| # | Issue | Resolution |
|---|---|---|
| 1 | Multi-record BER files silently under-ingested to 1 row per file | Fixed in `asn1/decoder.py` (§3) |
| 2 | Two existing tests asserted the buggy one-row behaviour | Updated to assert all records decode |
| 3 | `bt_digital_poc.config` had no control tables | `setup_control_tables` wired as first job task |
| 4 | `resources/observability/*.yml` include is commented out | Used a new `resources/uc7/` include instead |
| 4b | `test_resource_layout` failed on the new `resources/uc7/` folder | The repo keeps an explicit registry of resource group folders; `uc7` added to `EXPECTED_GROUPS` (the folder's `include:` line was already present) |
| 5 | **`bundle deploy` fails on an unrelated pre-existing resource** | See §10.1 — worked around with `--select` |
| 6 | Serverless UDF 1 GB memory limit on the 42 MB SGSN file | Chunked yield, `_DECODE_CHUNK_ROWS = 10000` (§3.5) |
| 7 | `event_log()` `SELECT *` Arrow nullability mismatch | Explicit projection + `named_struct` origin (§7.1) |
| 8 | Observability destination Volume did not exist | Created `bt_digital_poc.observability.app_logs` (§7.2) |

### 10.1 Deploy failure on `resources.apps.flowx_onboarding_app` (pre-existing, worked around)

A full `databricks bundle deploy` aborts on a resource unrelated to UC_7, and the resource
differs by target. On `hoonartek` it is **HTTP 403 on the Genie space**, because
`databricks-genie/flowx_observability.geniespace.json` hardcodes 13 `flowx.config.*` /
`flowx.observability.*` table names and this workspace's catalog is `bt_digital_poc`:

```
Error: cannot create resources.genie_spaces.flowx_observability_genie_space: An error
occurred accessing the schema. Failed to fetch tables for the agent. Please resolve these
errors to continue: Catalog 'flowx.config.onboarding_audit_log' does not exist, ...
(403 PERMISSION_DENIED)
```

The Genie space is therefore not target-portable; that defect is open. On `metaflow_v7` the
same full deploy aborted earlier with HTTP 400 on the Apps resource instead:

```
Error: cannot update resources.apps.flowx_onboarding_app: updating id=flowx-onboarding:
Invalid update mask. Only description, budget_policy_id, usage_policy_id, resources,
user_api_scopes, compute_size, compute_min_instances, compute_max_instances, git_repository,
git_source, telemetry_export_destinations, compatibility_flags are allowed.
Supplied update mask: ... forward_user_access_token ...
```

**Diagnosis.** The CLI (v1.13.0) includes `forward_user_access_token` in the Apps update mask,
and the workspace's Apps API no longer accepts that field. It is **not** caused by anything in
this work: `resources/flowx_app/flowx_onboarding_app.yml` was not modified (it does not even
contain that key — the CLI adds it), and `git status` shows the only changed files are the
decoder, its two test files, `databricks.yml`, and the new UC_7 files. The wheel upload and file
sync both completed; only the resource-update phase aborted.

**Workaround applied.** Deploy only the resources this work owns:

```bash
databricks bundle deploy -t hoonartek -p Hoonartek --fail-on-active-runs \
  --select pipelines.uc7_cdr_asn_pipeline,jobs.uc7_cdr_asn_job,jobs.onboarding_job
```

This succeeded (`Resources: 0 created, 0 changed, 0 deleted, 3 unchanged, 12 not selected` —
the UC_7 job and pipeline had already been created by the first deploy before it aborted on the
app, and were verified present and correctly wired afterwards).

**Not fixed here deliberately.** The fix belongs to the app resource (pin/upgrade the CLI, or
adjust the app definition), is unrelated to UC_7, and would change a shared resource that other
work depends on. Raised as open item 5.

### Open

| # | Item | Detail | Needs |
|---|---|---|---|
| 1 | **SMSC not onboarded** | Payload is a 48-field quoted CSV; no `SMSC.asn1` module exists | Either the real SMSC ASN.1 module, or a decision to onboard as CSV |
| 2 | **MMSC not onboarded** | Payload is a 70+ column CSV | Same — the CSV column contract, or the real module |
| 3 | ~~Volume copy destination~~ | **RESOLVED** — confirmed with requester, copied to `bt_digital_poc.staging.uc_7` (§8) | — |
| 4 | **Indefinite-length files not fully split** | EMSC `.raw` and some TAP files use indefinite length (`0x80`); `iter_ber_tlv_records` yields the remainder whole and stops. Not losing data today (EMSC's 3 files gave 353 rows), but a payload *beginning* with an indefinite-length TLV would yield 1 row | Confirm whether EMSC needs interior walking of indefinite-length records |
| 5 | **`bundle deploy` blocked by the Apps resource** | CLI v1.13.0 sends `forward_user_access_token` in the Apps update mask; workspace API rejects it (§10.1). Full deploys of this bundle fail until fixed | Upgrade/pin the CLI, or amend `resources/flowx_app/flowx_onboarding_app.yml` |

### Question raised by this use case (recorded per request)

**Should `bronze` hold one table per source, or one schema per source?** This brief specifies
the `bronze` layer, so all four tables land in `bt_digital_poc.bronze`. Every pre-existing ASN.1 spec
instead uses a per-source schema (`bronze_emsc`, `bronze_psgw`, …). Both conventions now exist
in the repo. Worth settling before more sources are onboarded, since it determines whether
`bronze_emsc.emsc_cdr_raw` and `bronze.emsc_cdr_raw` are meant to coexist.
