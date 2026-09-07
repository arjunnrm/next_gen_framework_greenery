# UC6 — Flood Warning System: Environment Agency → Leidos Address/MSISDN Matching
## Technical Implementation Design (flowx framework on Databricks Lakeflow)

| | |
|---|---|
| **Use case ID** | UC6 |
| **Platform** | Databricks — `flowx` internal framework, Lakeflow Jobs + Lakeflow Declarative Pipelines |
| **Status** | POC design |
| **Companion doc** | `UC6_Usecase_Explanation.md` (business context) |

> **Note on repo conventions:** this document proposes concrete names,
> paths, and patterns based on the functional design and the instructions
> given for this build. Wherever the `flowx` repo already has an
> established convention that differs (onboarding schema, table naming,
> tagging taxonomy, observability sink), **the repo convention wins** — this
> document should be updated to match once confirmed, not the other way
> round.

---

## 1. Architecture overview

Two Databricks assets, with a strict separation of concerns:

```
 ┌────────────────────────┐        triggers        ┌───────────────────────────────┐
 │ 007_uc6_lfj_EA         │ ─────────────────────▶ │ 008_uc6_ldp_EA                │
 │ Lakeflow Job           │                         │ Lakeflow Declarative Pipeline │
 │ (orchestration ONLY)   │                         │ (all data engineering)        │
 └────────────────────────┘                         └───────────────────────────────┘
                                                          │
                                                          ▼
                                         ingest → validate (DLT expectations)
                                         → unzip/decrypt → transform/join
                                         → generate outputs → archive
                                                          │
                                                          ▼
                                            observability.appl_logs (uc6)
```

- **`007_uc6_lfj_EA` (Lakeflow Job):** schedules/triggers the pipeline,
  owns retry policy, timeout, and failure alerting. **No file I/O, no
  business logic.**
- **`008_uc6_ldp_EA` (Lakeflow Declarative Pipeline):** owns everything
  else — file discovery, DQ gating, decryption/decompression, the join/
  transform logic, output generation, encryption, and archiving. All
  zip/unzip/archive/GPG steps use the framework's existing utilities.

This split matters operationally: it means data-engineering changes
(new business rules, new source files, encryption changes) only ever touch
the pipeline, while scheduling/alerting changes only ever touch the job —
they can be modified, tested, and deployed independently.

---

## 2. Volume & folder layout

Unity Catalog volume `uc_6`, e.g. `/Volumes/<catalog>/<schema>/uc_6/`:

```
uc_6/
├── raw/      landing zone — all 6 source files drop here per cycle
├── archive/  pipeline moves originals here after successful processing
└── output/   pipeline writes the 4 generated output files here
```

## 3. Onboarding

A single onboarding JSON entry registers UC6 with the framework, following
whatever schema existing entries use. At minimum it should declare:
- The 6 source files (logical name, filename pattern/regex, delimiter `|`,
  compression, encryption flag).
- The target volume (`uc_6`) and its raw/archive/output paths.
- The secret reference for the GPG passphrase (`br_digital_poc.config.pgpkey`) — by
  reference only, never the literal value.
- The job (`007_uc6_lfj_EA`) and pipeline (`008_uc6_ldp_EA`) identifiers.
- Any schedule/trigger config (cadence: currently weekly).

---

## 4. `007_uc6_lfj_EA` — Lakeflow Job (orchestration)

| Concern | Design |
|---|---|
| Trigger | Scheduled (weekly) or file-arrival event, per existing `lfj` convention |
| Task(s) | Single task: run pipeline `008_uc6_ldp_EA` (or a task sequence, if the framework splits ingest/transform/output pipelines — follow existing convention) |
| Retry policy | Per existing `lfj` template defaults |
| Alerting | On failure, notify per existing `lfj` template (email/Slack/etc. — whatever's standard) |
| Pre-flight "DW check" | If this repo's convention is a lightweight pre-trigger check against a warehouse/reference table, implement it here; if it's data-level, it belongs in §5 instead |

**Explicitly out of scope for this job:** unzip, decrypt, archive, file
validation, joins, transforms, output generation. If you find yourself
writing any of those inside the job definition, stop — that logic belongs
in the pipeline.

---

## 5. `008_uc6_ldp_EA` — Lakeflow Declarative Pipeline (all data engineering)

### 5.1 Bronze — ingest & validate

1. Discover the 6 files in `uc_6/raw/` via pattern-based file loader
   (extend the framework's generic loader for pipe-delimiter + glob/regex
   filename matching if it doesn't already support this — see §8).
2. **DLT expectation:** `file_count > 0 AND size/row_count metadata > 0` for
   each expected file. On failure: fail the pipeline flow and emit an
   observability event (§7) naming which file(s) failed.
3. Unzip/decrypt via framework utilities:
   - All files: gunzip.
   - EA request file only: GPG-decrypt (symmetric, AES256) using the
     passphrase from Databricks secret `br_digital_poc.config.pgpkey`, **then**
     gunzip.
4. Land as bronze tables (see §6 for naming):
   - `uc6_bronze_ea_request`
   - `uc6_bronze_css_account`
   - `uc6_bronze_css_account_address`
   - `uc6_bronze_css_subscription`
   - `uc6_bronze_jt_customer`
   - `uc6_bronze_excalibur_address`
5. Archive: move (not copy) each successfully processed original file from
   `raw/` to `archive/` via the framework's archive utility.

### 5.2 Silver — normalise & match

**EA base file**
```
uc6_silver_ea_base  := dedupe(uc6_bronze_ea_request, key = osapr)
uc6_silver_ea_addr  := dedupe(uc6_bronze_ea_request, key = (postcode, address, town))
```

**PAF normalisation** (mapping table below applies to CSS and Excalibur
paths; JT is reformatted directly)

| PAF element | PAF field | CSS source field | Excalibur source field |
|---|---|---|---|
| Organisation | Organisation Name | `org_name` | — |
| | Department Name | `dept_name` | — |
| Premises | Sub Building Name | `building_name1` | `ADR_DISTRICT` |
| | Building Name | `building_name` | `ADR_HOUSE_NAME` |
| | Building Number | `building_number` | `ADR_HOUSE_NO` |
| Thoroughfare | Dependent Thoroughfare Name | `street1` | `ADR_PRIMARY_LN` |
| | Thoroughfare Name | `street2` | `ADR_SECONDARY_LN` |
| Locality | Double Dependent Locality | `locality1` | `ADR_DISTRICT` |
| | Dependent Locality | `locality2` | — |
| | Post Town | `town` | `ADR_CITY` |
| Postcode | Postcode | `postcode` | `ADR_POST_CODE` |
| PO Box | PO Box Number | `postbox` | `ADR_POB` |

```
css_joined := uc6_bronze_css_subscription
                ⋈ uc6_bronze_css_account          ON customerid
                ⋈ uc6_bronze_css_account_address  ON customerid
              WHERE months_between(today, dateofbirth)/12 > 17
                AND customerbusinessunitcode != 'BS'

uc6_silver_ee_address_paf :=
    reformat_to_paf(css_joined)                       -- OUK / PAYM
  ∪ reformat_to_paf(uc6_bronze_jt_customer)           -- OUK / PAYG
  ∪ reformat_to_paf(uc6_bronze_excalibur_address)     -- EE/TMUK, via mapping table
```

**Matching**
```
uc6_silver_matched_address :=
    uc6_silver_ea_addr ⋈ uc6_silver_ee_address_paf
    ON postcode  (excluding PO boxes)
    -- carries: osapr, postcode, address, town, msisdn

count_of_msisdn_per_osapr        := rollup(uc6_silver_matched_address, by=osapr)
count_of_osapr_per_targetAreaID  := rollup(join(uc6_silver_ea_base, uc6_silver_matched_address, on=osapr), by=targetAreaID)
```

### 5.3 Gold — business rules & output tables

Match-strength threshold and the "> 1 address" thresholds are **pipeline
parameters**, not hardcoded.

**Telephone (`uc6_gold_telephone_output`)**
| Condition | Result |
|---|---|
| match_strength > threshold AND msisdn populated AND count_of_osapr > 1 | include |
| match_strength > threshold AND msisdn populated AND count_of_osapr not > 1 | exclude |

Fields: `targetAreaID, telephone`

**OSAPR (`uc6_gold_osapr_output`)**
| Condition | count | status |
|---|---|---|
| match_strength > threshold AND count_of_osapr > 1 | 1 | Found |
| match_strength > threshold AND count_of_osapr == 1 | 0 | Not Found |
| match_strength ≤ threshold | 0 | Bad OSAPR |
| only one address candidate exists at all | 0 | Single Addr |

Fields: `targetAreaID, osapr, count, status`

### 5.4 Output file generation → `uc_6/output/`

| Output | Filename pattern | Encoding |
|---|---|---|
| Leidos Telephone | `EE_YYYY-MM-DD-LEIDOS_TELEPHONE_NOfN.csv.gz` | gzip only |
| Leidos OSAPR | `EE_YYYY-MM-DD-LEIDOS_OSAPR_NOfN.csv.gz` | gzip only |
| Telephone Output | `EE_YYYY-MM-DD-TELEPHONE_NOfN.csv.gz.gpg` | gzip + GPG symmetric |
| OSAPR Output | `EE_YYYY-MM-DD-OSAPR_NOfN.csv.gz.gpg` | gzip + GPG symmetric |

Both GPG outputs use the passphrase from `br_digital_poc.config.pgpkey` — the same
secret used to decrypt the inbound EA file.

Field-size constraints carried into validation: `targetAreaID` 32 chars,
`osapr` ≤ 12 digits, `postcode` ≤ 7 chars, `telephone` ≤ 16 digits, `count`
numeric (≤ 999), `status` ≤ 100 chars.

---

## 6. Table naming convention & tagging

Pattern: `<catalog>.<schema>.uc6_<layer>_<entity>` (bronze / silver / gold).
See the full table list in §5.1–§5.3.

Minimum tags on every UC6 table:

| Tag | Example value |
|---|---|
| `use_case` | `uc6` |
| `source_system` | `env_agency` / `css` / `jt` / `excalibur` |
| `data_classification` | `confidential` / `pii` |
| `pii` | `true` / `false` |
| `owner` | `business_data_and_ai` |
| `environment` | `poc` / `dev` / `prod` |

If the repo has an existing tagging taxonomy, use its tag keys/allowed
values instead of the above.

---

## 7. Observability

Every pipeline stage (ingest, validate, decrypt, transform, output,
archive) and the job's own orchestration events emit structured log entries
via the framework's observability module, routed to:

```
observability.appl_logs   (filtered/tagged: usecase = 'uc6')
```

Minimum event fields: `run_id`, `job_or_pipeline_name`, `stage`, `status`
(started/succeeded/failed), `timestamp`, `row_count`/`file_count` where
relevant, `error_detail` on failure.

DQ-expectation failures (§5.1) must produce an observability event in
addition to failing the pipeline, so failures are queryable/alertable from
`observability.appl_logs` directly.

> Assumption: routed under the **`uc6`** namespace for consistency with the
> job/pipeline/volume/table naming used throughout this design (a stray
> reference to "uc3" was treated as a typo). Confirm before build if a
> separate `uc3` namespace is genuinely intended.

---

## 8. Framework enhancements needed (track separately from UC6 code)

1. **File loader:** pipe delimiter support + glob/regex filename matching
   (currently assumed comma-delimited + exact filename only — confirm).
2. **GPG symmetric encrypt/decrypt utility** — reusable, secret-backed.
3. **Archive-move utility** — reusable move-not-copy semantics with
   overwrite/collision handling.
4. **Table tagging helper**, if the repo doesn't already have one that
   pipelines can call at table-creation time.

These should land as framework-level changes (reviewable independently),
not embedded inside UC6-specific code.

---

## 9. Error handling summary

| Failure | Where caught | Behaviour |
|---|---|---|
| Expected source file missing | Pipeline DQ expectation | Fail pipeline, log to `observability.appl_logs`, no partial processing |
| Source file present but empty (size/row count = 0) | Pipeline DQ expectation | Same as above |
| GPG decrypt failure (bad/missing secret) | Pipeline ingest stage | Fail pipeline, log error detail (not the passphrase) |
| Output GPG encrypt failure | Pipeline output stage | Fail pipeline before partial output lands in `output/` |
| Job-level trigger/timeout issue | Job (`007_uc6_lfj_EA`) | Standard job retry/alert policy |

---

## 10. Test plan

See `claude_code_prompt_uc6_EA_flowx.md` §9 for the full acceptance test
list using the synthetic `uc_6_poc_bundle.zip` fixture.
