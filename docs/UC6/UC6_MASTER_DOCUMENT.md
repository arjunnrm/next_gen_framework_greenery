<style>
:root { --aptos: Aptos, "Aptos Display", "Segoe UI Variable", "Segoe UI", system-ui, -apple-system, sans-serif; }
body, .md-typeset, .md-typeset table, .md-typeset h1, .md-typeset h2,
.md-typeset h3, .md-typeset h4, .md-typeset p, .md-typeset li { font-family: var(--aptos) !important; }
.md-typeset code, .md-typeset pre { font-family: "Cascadia Code", Consolas, "Courier New", monospace !important; }
.screenshot { border: 1px dashed #9aa0a6; background: #f6f7f9; padding: 14px 16px;
  margin: 12px 0; border-radius: 6px; color: #4a4f55; font-family: var(--aptos); }
.screenshot b { color: #1a1a1a; }
</style>

# UC6 Flood Warning System : Master Document

**Use case:** UC6 — Environment Agency flood warnings to EE mobile subscribers, Fujitsu to Leidos supplier migration
**Catalog:** `flowx`  **Target / profile:** `metaflow_v7`  **Framework:** FlowX metadata-driven ingestion
**Pipeline:** `008_ldp_uc6_ea_flood_warning`  **Job:** `007_lfj_uc6_ea_flood_warning`
**Status:** Green end-to-end. Job run `583025937928399`, all five tasks SUCCESS. Every number in this document was read back from the live workspace on 2026-09-06.

---

## Index

| # | Section | What you will learn |
|---|---|---|
| 1 | [Core Functional Requirement](#1-core-functional-requirement) | **Read this first.** What UC6 must do, in seven rules |
| 2 | [Business Context](#2-business-context) | Why UC6 exists, who depends on it |
| 3 | [As-Is Process](#3-as-is-process-today-in-ab-initio) | The legacy Ab Initio job, and its pain points |
| 4 | [To-Be on Databricks](#4-to-be-the-databricks-implementation) | The target architecture in one picture |
| 5 | [The Data We Use](#5-the-data-we-use) | Six source files, what each contributes |
| 6 | [File Locations](#6-file-locations-the-exact-paths) | Exact paths, in repo and on the volume |
| 7 | [Step-by-Step Pipeline Walkthrough](#7-step-by-step-pipeline-walkthrough) | Twelve steps, file on disk to exported file |
| 8 | [Every Table Explained](#8-every-table-explained-purpose-and-use) | Layer by layer, what each table is for |
| 9 | [Joining Logic](#9-joining-logic-the-heart-of-uc6) | Every join, its keys, and why it is written that way |
| 10 | [The Matching Algorithm](#10-the-matching-algorithm-explained-with-real-data) | Match strength, worked on real rows |
| 11 | [The Four Statuses](#11-the-four-statuses-decision-table) | How Found, Not Found, Bad OSAPR and Single Addr are decided |
| 12 | [Business Rules](#12-business-rules-age-privacy-and-exclusions) | Age filter, privacy safeguard, business-unit exclusion |
| 13 | [Onboarding JSON](#13-onboarding-json-attribute-by-attribute) | Every attribute, and why it is set that way |
| 14 | [The Export Files](#14-the-export-files) | Four files, two channels, two envelopes |
| 15 | [Encryption and Secrets](#15-encryption-and-secrets) | Symmetric PGP, and how the key is held |
| 16 | [Testing SQL for Business Users](#16-testing-sql-for-business-users) | Copy-paste queries with expected answers |
| 17 | [Data Quality and Presence Gates](#17-data-quality-and-presence-gates) | How a missing file is caught |
| 18 | [Governance and Observability](#18-governance-and-observability) | 72 tags, and where the telemetry lands |
| 19 | [Framework Enhancements UC6 Required](#19-framework-enhancements-uc6-required) | Four capabilities this use case forced |
| A | [Defects Found by This Build](#appendix-a-defects-found-by-this-build) | Fifteen real defects, ten of them framework-level |
| B | [Operational Runbook](#appendix-b-operational-runbook) | How to run it, and what to do when it breaks |
| C | [Comparison With UC3 and UC7](#appendix-c-comparison-with-uc3-and-uc7) | Why the three use cases look so different |

---

## 1. Core Functional Requirement

**This is the section to read if you read nothing else.**

### 1.1 The requirement in one sentence

> When the Environment Agency issues a flood warning for a set of postcodes, UC6 must identify every EE mobile subscriber living at those addresses and produce a list of their phone numbers, so they can be warned by SMS.

### 1.2 The seven functional rules

| # | Rule | Why it exists |
|---|---|---|
| **FR1** | Accept a flood-warning request from the Environment Agency, listing target areas and addresses | This is the trigger. Nothing runs without it |
| **FR2** | Match each EA address against EE customer addresses on **postcode**, then score the address text | Postcode alone is too coarse. A postcode can cover many houses |
| **FR3** | Only warn subscribers whose address match strength **exceeds 50** | A weak match means a wrong person is warned. That is worse than not warning |
| **FR4** | Only warn areas with **more than one** matched address | **Privacy safeguard.** One matched address in an area identifies that household |
| **FR5** | Exclude subscribers **aged 17 or under** | Duty of care. Minors are not the account contact |
| **FR6** | Exclude business-unit code **BS** | Business accounts are handled by a different process |
| **FR7** | Produce **four output files**: two for Leidos, two encrypted equivalents for the Fujitsu interface | Both suppliers must run in parallel during the migration |

### 1.3 What the outputs must contain

| Output | Fields | Purpose |
|---|---|---|
| **Telephone list** | `targetAreaID`, `telephone` | The numbers to send SMS warnings to |
| **OSAPR list** | `targetAreaID`, `osapr`, `count`, `status` | The audit answer, address by address |

### 1.4 The four statuses that must be reported

| Status | Meaning | When it is assigned |
|---|---|---|
| **Found** | Address matched, and the area is safe to warn | Match strength above 50, **and** more than one matched address in the area |
| **Not Found** | Address matched, but the area is withheld | Match strength above 50, but **only one** matched address in the area |
| **Bad OSAPR** | The address did not match well enough | Match strength 50 or below |
| **Single Addr** | The area only ever had one candidate address | Candidate count for the area is 1 or fewer |

### 1.5 The non-negotiable constraint

> **The output file format cannot change during the migration.** Fujitsu and Leidos must receive byte-compatible content. This single constraint drove four framework enhancements, listed in Section 19.

---

## 2. Business Context

### 2.1 What the system does, in plain terms

- The **Environment Agency** monitors rivers and coastlines. When flooding is likely, it issues a warning for specific postcodes.
- **EE** holds the mobile numbers of people living at those postcodes.
- UC6 is the bridge: it takes the EA's list of at-risk addresses and returns EE's list of phone numbers to warn.
- Those numbers are then sent an SMS by the messaging platform. **UC6 does not send the SMS.** It produces the list.

### 2.2 Who depends on it

| Stakeholder | What they need from UC6 |
|---|---|
| **Environment Agency** | Confidence that warnings reach the right households |
| **EE operations** | A reliable weekly file, in a fixed format |
| **Leidos** | The new supplier. Needs plain files on the new interface |
| **Fujitsu** | The outgoing supplier. Needs encrypted files until cutover completes |
| **EE compliance** | Proof that minors were excluded and privacy rules were applied |

### 2.3 Why it matters

- **Public safety.** A missed warning can mean a household is not evacuated.
- **Privacy.** Warning a single identifiable household reveals that a specific person lives at a specific address.
- **Contractual.** The weekly file is a supplier obligation. A missing file is a breach, not an inconvenience.

---

## 3. As-Is Process, Today in Ab Initio

### 3.1 The legacy job

| Property | Detail |
|---|---|
| **Platform** | Ab Initio graph, running on premises |
| **Age** | Approximately ten years |
| **Cadence** | Weekly |
| **Inputs** | Six files, delivered to a landing directory |
| **Outputs** | Four files, collected by the supplier |

### 3.2 The pain points

| Pain point | Consequence |
|---|---|
| **Matching logic is embedded in the graph** | Nobody can read the rules without opening Ab Initio |
| **No lineage** | You cannot answer "why was this number included" without re-running |
| **Supplier change requires a code change** | Migrating Fujitsu to Leidos means editing the graph |
| **Manual file handling** | Decryption and decompression are separate operator steps |
| **No data-quality gate** | A truncated input file produces a short output file, silently |

---

## 4. To-Be, the Databricks Implementation

### 4.1 The architecture in one picture

```
   /Volumes/flowx/staging/uc_6/raw/        6 source files land here
                 |
                 v
   [ 1 ] BRONZE  6 streaming tables         faithful copy of each source
                 |
                 v
   [ 2 ] SILVER  4 materialized views        normalise, union, match
                 |
                 v
   [ 3 ] GOLD    2 materialized views        the business answer
                 |
                 v
   [ 4 ] SINKS   4 export files              2 plain, 2 GPG-encrypted
                 |
                 v
   /Volumes/flowx/staging/uc_6/output/
```

### 4.2 What changed against the As-Is

| Aspect | Ab Initio (As-Is) | Databricks (To-Be) |
|---|---|---|
| **Matching rules** | Buried in a graph | Plain SQL in an onboarding JSON |
| **Thresholds** | Hard-coded | `pipeline_parameters`, changed without code edits |
| **Lineage** | None | Every row carries file, run and timestamp |
| **Decryption** | Manual operator step | Automatic, inside the pipeline |
| **Missing-file detection** | None | Six presence gates fail the run |
| **Supplier change** | Code change | Add a sink flow in JSON |

### 4.3 The verified table inventory

| Layer | Tables | Type |
|---|---|---|
| **Bronze** | 6 | `STREAMING_TABLE` |
| **Silver** | 4 | `MATERIALIZED_VIEW` |
| **Gold** | 2 | `MATERIALIZED_VIEW` |
| **Sinks** | 4 | No table. Files only |

---

## 5. The Data We Use

### 5.1 The six source files

| File pattern | Bronze table | What it contributes | Delimiter |
|---|---|---|---|
| `EE_*-REQUEST_*.csv.gz.gpg` | `uc6_ea_request` | **The trigger.** EA target areas and addresses | Pipe |
| `CSS_account_[0-9]*.dat.gz` | `uc6_css_account` | Date of birth, business-unit code | Pipe |
| `CSS_account_address_*.dat.gz` | `uc6_css_account_address` | The customer address | Pipe |
| `CSS_subscription_*.dat.gz` | `uc6_css_subscription` | The MSISDN, the phone number | Pipe |
| `CM_JT_Customer_Details_*.dat.gz` | `uc6_jt_customer` | PAYG customers, a separate channel | Pipe |
| `CM_EXCALIBUR_ADDRESS_*.dat.gz` | `uc6_excalibur_address` | EE and TMUK legacy addresses | **Comma** |

### 5.2 Two traps in the source data

| Trap | Detail | How it is handled |
|---|---|---|
| **Excalibur is comma-delimited** | Every other file is pipe-delimited | `reader_options.delimiter` is set per flow |
| **`CSS_account_*` also matches `CSS_account_address_*`** | A naive glob loads the wrong file into the wrong table | Pattern is `CSS_account_[0-9]*.dat.gz`, forcing a digit after the underscore |

### 5.3 Verified row counts, current fixture

| Table | Rows | Note |
|---|---|---|
| `uc6_ea_request` | 7 | Seven EA addresses across four areas |
| `uc6_css_account` | 5 | Five CSS customers |
| `uc6_css_account_address` | 5 | One address each |
| `uc6_css_subscription` | 5 | One MSISDN each |
| `uc6_jt_customer` | 1 | The deliberate under-17 record |
| `uc6_excalibur_address` | 1 | The deliberate postcode-match-only record |

---

## 6. File Locations, the Exact Paths

### 6.1 In the repository

| Path | Contents |
|---|---|
| `onboarding/uc6/uc6_ea_flood_warning.json` | The onboarding specification |
| `onboarding/uc6/schema_configs/` | Five positional-to-named column maps |
| `resources/uc6/uc6_ea_flood_warning_job.yml` | The job definition |
| `resources/uc6/uc6_ea_flood_warning_pipeline.yml` | The pipeline definition |
| `docs/UC6/test_fixture/uc_6/raw/` | **The augmented fixture.** Exercises all four statuses |
| `docs/UC6/sample_bundle/uc_6/raw/` | The supplied bundle. Produces zero matches |

### 6.2 On the Unity Catalog volume

| Path | Purpose |
|---|---|
| `/Volumes/flowx/staging/uc_6/raw/` | Where the six source files land |
| `/Volumes/flowx/staging/uc_6/_extracted/ea_request/` | Where the decrypted EA file is written |
| `/Volumes/flowx/staging/uc_6/_schema_configs/` | The five schema-config JSON files |
| `/Volumes/flowx/staging/uc_6/output/` | **The four export files** |
| `/Volumes/flowx/staging/uc_6/output/_staging/` | Per-sink staging. Ignore |
| `/Volumes/flowx/observability/app_logs/uc6/` | Telemetry export |

### 6.3 A critical operational note

> **The EA request file is consumed on every run.** Its `delete_source_after_extract` action is `delete_now`. You must re-upload it from the fixture before each run. The other five files remain in place.

---

## 7. Step-by-Step Pipeline Walkthrough

### 7.1 The twelve steps

| Step | What happens | Where | How you verify it |
|---|---|---|---|
| **1** | Six files land on the volume | `raw/` | List the folder. Expect six files |
| **2** | Presence gates confirm every source has rows | Reconciliation flows | Query T7 |
| **3** | The EA file is **decrypted** with the shared passphrase | `crypto/pgp.py` | The `.gpg` disappears from `raw/` |
| **4** | The decrypted file is **gunzipped** to `_extracted/` | `archive/zip_utils.py` | A `.csv` appears in `_extracted/ea_request/` |
| **5** | Five plain `.gz` files are read directly | Auto Loader | Spark decompresses gzip natively |
| **6** | Positional columns are named via schema configs | `ingestion/schema_config.py` | Bronze tables have real column names |
| **7** | Six bronze tables are published | `flowx.bronze.uc6_*` | Query T1 |
| **8** | EA rows are normalised, deduplicated per OSAPR | `silver.uc6_ea_base`, `uc6_ea_address` | Query T2 |
| **9** | EE customers are unioned across three channels | `silver.uc6_ee_address_paf` | Query T3 |
| **10** | **The join.** EA addresses meet EE addresses on postcode | `silver.uc6_matched_address` | Query T4 |
| **11** | Statuses are decided, telephone list is filtered | `gold.uc6_osapr_output`, `uc6_telephone_output` | Query T5, T6 |
| **12** | Four files are written, two of them encrypted | `output/` | Query T9 |

### 7.2 Why the EA file needs steps 3 and 4, but the others do not

| File type | Handling | Reason |
|---|---|---|
| **`.csv.gz.gpg`** (EA) | Decrypt, then gunzip, then land | Spark cannot read through the encryption envelope |
| **`.dat.gz`** (five others) | Read directly | Spark decompresses plain gzip transparently. No staging needed |

**This is a common misunderstanding.** Turning on ZIP handling for a plain `.gz` file buys nothing and costs a staging copy.

---

## 8. Every Table Explained, Purpose and Use

### 8.1 Bronze layer, six tables

| Table | Purpose | Who uses it |
|---|---|---|
| `uc6_ea_request` | Faithful copy of the EA flood-warning request | Silver normalisation |
| `uc6_css_account` | CSS customer master. **Supplies date of birth and business-unit code** | The age filter and BS exclusion |
| `uc6_css_account_address` | CSS postal addresses | Address matching |
| `uc6_css_subscription` | CSS MSISDNs, the phone numbers | The final output |
| `uc6_jt_customer` | PAYG customers, a self-contained channel | Union branch 2 |
| `uc6_excalibur_address` | Legacy EE and TMUK addresses | Union branch 3 |

**Bronze rule:** these are faithful copies. No business logic is applied here. That is deliberate, so you can always prove what the source actually sent.

### 8.2 Silver layer, four tables

| Table | Purpose | Rows today |
|---|---|---|
| `uc6_ea_base` | One row per OSAPR, deduplicated. **The audit spine** | 7 |
| `uc6_ea_address` | EA addresses normalised for matching | 7 |
| `uc6_ee_address_paf` | **The union.** All EE customers from three channels, PAF-normalised, age-filtered | 6 |
| `uc6_matched_address` | **The join result.** Every EA-to-EE candidate pair with a score | 11 |

### 8.3 Gold layer, two tables

| Table | Purpose | Rows today |
|---|---|---|
| `uc6_osapr_output` | **The audit answer.** One row per EA address, with its status | 7 |
| `uc6_telephone_output` | **The action list.** Only numbers safe to warn | 2 |

### 8.4 Why 11 matched rows become 7 audit rows and 2 phone numbers

| Stage | Rows | What happened |
|---|---|---|
| `uc6_matched_address` | 11 | Every candidate pair. One EA address can match several customers |
| `uc6_osapr_output` | 7 | Aggregated back to one row per EA address |
| `uc6_telephone_output` | 2 | Only areas that passed **both** the strength and privacy rules |

---

## 9. Joining Logic, the Heart of UC6

### 9.1 The join inventory

| # | Join | Left | Right | Keys | Type |
|---|---|---|---|---|---|
| **J1** | Customer assembly | `uc6_css_sub` | `uc6_css_acct` | `customerid` | INNER |
| **J2** | Address attachment | (J1 result) | `uc6_css_addr` | `customerid` | INNER |
| **J3** | **The matching join** | `uc6_ee_paf_match` | `uc6_ea_addr_match` | `postcode_norm` | INNER |
| **J4** | Score aggregation | `uc6_ea_base_osapr` | `uc6_matched_osapr` | `osapr` | LEFT |
| **J5** | Area rollup | `scored` | `area` | `targetAreaID` | INNER |
| **J6** | Telephone filter | `uc6_matched_tel` | `uc6_osapr_tel` | `targetAreaID` + `osapr` | INNER |

### 9.2 J3, the matching join, explained line by line

```sql
FROM   uc6_ee_paf_match ee
INNER  JOIN uc6_ea_addr_match ea
       ON ee.postcode_norm = ea.postcode_norm
WHERE  ea.has_po_box = false
  AND  ee.has_po_box = false
```

| Element | Why it is written this way |
|---|---|
| **Join on `postcode_norm`, not the full address** | Postcode is the only field reliable enough to join on. Address text varies too much |
| **Normalised, not raw** | `postcode_norm` strips spaces and case. `AB12 3CD` and `ab123cd` must match |
| **INNER, not LEFT** | An EA address with no postcode match cannot be warned. It is correctly absent |
| **PO Box excluded on both sides** | A PO Box is not a dwelling. Warning it would be meaningless |

### 9.3 Why the join produces more rows than either input

- One postcode can cover **many customers**.
- Seven EA addresses joined to six EE customers produce **eleven candidate pairs**.
- **This is intentional.** The score in Section 10 then decides which pairs are good enough.

### 9.4 J4, and why it must be a LEFT JOIN

```sql
FROM   uc6_ea_base_osapr b
LEFT   JOIN uc6_matched_osapr m ON b.osapr = m.osapr
```

| If it were INNER | Consequence |
|---|---|
| EA addresses with no match would vanish | They would never receive a **Bad OSAPR** status |
| The audit file would be short | The EA could not tell what happened to those addresses |

> **This is the single most important join decision in UC6.** LEFT JOIN is what guarantees every EA address gets a status, matched or not.

### 9.5 J6, the privacy join

```sql
FROM   uc6_matched_tel m
INNER  JOIN uc6_osapr_tel o
       ON  m.targetAreaID = o.targetAreaID
       AND m.osapr        = o.osapr
WHERE  m.match_strength > 50
  AND  o.count_of_osapr > 1
```

| Condition | Rule enforced |
|---|---|
| `match_strength > 50` | **FR3.** Only confident matches |
| `count_of_osapr > 1` | **FR4.** The privacy safeguard |
| Joined on **both** area and OSAPR | Ensures the count applies to the right area |

---

## 10. The Matching Algorithm, Explained With Real Data

### 10.1 The scoring formula

```sql
CASE WHEN ea.address_norm IS NULL OR ee.address_norm IS NULL THEN 0
     ELSE cast(round(100.0 *
            size(array_intersect(split(ea.address_norm,' '), split(ee.address_norm,' ')))
            / greatest(size(split(ea.address_norm,' ')), 1)) AS INT)
END AS match_strength
```

### 10.2 In plain words, five steps

| Step | What happens |
|---|---|
| **1** | Split the EA address into words |
| **2** | Split the EE address into words |
| **3** | Count how many words appear in **both** |
| **4** | Divide by the number of words in the **EA** address |
| **5** | Multiply by 100 and round. That is the score out of 100 |

### 10.3 A worked example on real rows

| EA address | EE address | Words shared | Score |
|---|---|---|---|
| `ROSE COTTAGE 22 CYPRESS ROAD` | `ROSE COTTAGE 22 CYPRESS ROAD` | All | **100** |
| `ROSE COTTAGE 22 CYPRESS ROAD` | `14 BIRCH LANE` | None | **0** |

### 10.4 The actual join output, all eleven rows

| OSAPR | Area | Postcode | Score | Source | MSISDN |
|---|---|---|---|---|---|
| 3040045625 | AREA_FOUND | AB123CD | **100** | css | 07700900001 |
| 3040045625 | AREA_FOUND | AB123CD | 0 | css | 07700900002 |
| 3040045626 | AREA_FOUND | AB123CD | **100** | css | 07700900002 |
| 3040045626 | AREA_FOUND | AB123CD | 0 | css | 07700900001 |
| 3040045630 | AREA_NOTFOUND | EF456GH | **100** | css | 07700900003 |
| 3040045631 | AREA_NOTFOUND | EF456GH | 0 | css | 07700900003 |
| 3040045640 | AREA_BADOSAPR | IJ789KL | 0 | excalibur | 07700900006 |
| 3040045640 | AREA_BADOSAPR | IJ789KL | 0 | css | 07700900004 |
| 3040045641 | AREA_BADOSAPR | IJ789KL | 0 | excalibur | 07700900006 |
| 3040045641 | AREA_BADOSAPR | IJ789KL | 0 | css | 07700900004 |
| 3040045650 | AREA_SINGLE | MN102OP | **100** | css | 07700900005 |

**Read the AREA_BADOSAPR rows carefully.** The postcode matched, so the join produced rows. But the address text shares no words, so the score is 0. **A postcode match alone is not a match.** That is precisely what the score exists to catch.

### 10.5 Why the threshold is a parameter, not a constant

- `match_strength_threshold` is set in `pipeline_parameters` to **50**.
- Changing it requires **no code change** and no redeployment of logic.
- **This is a documented assumption.** The original Ab Initio scoring algorithm was not available, so this scoring rule is a reasonable reconstruction, not a recovered specification.

---

## 11. The Four Statuses, Decision Table

### 11.1 The decision logic

```sql
CASE WHEN a.candidate_osapr_count <= 1        THEN 'Single Addr'
     WHEN s.best_match_strength  <= 50        THEN 'Bad OSAPR'
     WHEN a.matched_osapr_count  >  1         THEN 'Found'
     ELSE                                          'Not Found'
END AS status
```

### 11.2 Evaluated in order, first match wins

| Order | Test | Status | Meaning |
|---|---|---|---|
| **1** | Area has 1 or fewer candidate addresses | **Single Addr** | Never enough to warn safely |
| **2** | Best score is 50 or below | **Bad OSAPR** | The address did not match |
| **3** | More than 1 matched address in the area | **Found** | Safe to warn |
| **4** | Otherwise | **Not Found** | Matched, but withheld for privacy |

### 11.3 The verified gold output, all seven rows

| Area | OSAPR | Count | Status | MSISDNs | Matched OSAPRs | Strength |
|---|---|---|---|---|---|---|
| AREA_FOUND | 3040045625 | **1** | **Found** | 2 | 2 | 100 |
| AREA_FOUND | 3040045626 | **1** | **Found** | 2 | 2 | 100 |
| AREA_NOTFOUND | 3040045630 | 0 | **Not Found** | 1 | 1 | 100 |
| AREA_NOTFOUND | 3040045631 | 0 | **Bad OSAPR** | 1 | 1 | 0 |
| AREA_BADOSAPR | 3040045640 | 0 | **Bad OSAPR** | 2 | 0 | 0 |
| AREA_BADOSAPR | 3040045641 | 0 | **Bad OSAPR** | 2 | 0 | 0 |
| AREA_SINGLE | 3040045650 | 0 | **Single Addr** | 1 | 1 | 100 |

### 11.4 The subtlety that looks like a bug and is not

> **AREA_NOTFOUND holds two different statuses.**

| OSAPR | Status | Why |
|---|---|---|
| 3040045630 | **Not Found** | Scored 100, but its area has only one matched address. Withheld by FR4 |
| 3040045631 | **Bad OSAPR** | Scored 0. It never matched at all |

**Status is decided per address, not per area.** Two addresses in the same area can legitimately have different statuses. Anyone expecting one status per area will report this as a defect. It is not.

---

## 12. Business Rules, Age, Privacy and Exclusions

### 12.1 The age filter, FR5

```sql
floor((months_between(current_date(), to_date(dateofbirth,'yyyy-MM-dd'))) / 12) > 17
```

| Aspect | Detail |
|---|---|
| **Threshold** | `min_age_years` parameter, set to 17 |
| **Applied to** | CSS branch and JT branch. Excalibur has no date of birth |
| **Date formats differ** | CSS uses `yyyy-MM-dd`, JT uses `yyyyMMdd`. Each branch parses its own |
| **Verified** | `07700900999`, the under-17 JT customer, is **absent** from the output |

**Note the parentheses.** `months_between(...)` is wrapped before dividing by 12. Without them, some SQL parsers apply the division to only part of the expression and let under-age customers through.

### 12.2 The privacy safeguard, FR4

| Aspect | Detail |
|---|---|
| **Rule** | An area must have **more than one** matched address |
| **Parameter** | `min_addresses_per_area`, set to 1 |
| **Why** | One matched address in an area identifies that specific household |
| **Verified** | `07700900003`, the sole match in AREA_NOTFOUND, is **absent** from the output |

### 12.3 The business-unit exclusion, FR6

| Aspect | Detail |
|---|---|
| **Rule** | `customerbusinessunitcode <> 'BS'` |
| **Parameter** | `excluded_business_unit_code` |
| **Applied to** | CSS branch only |

### 12.4 All six parameters

| Parameter | Value | Rule it drives |
|---|---|---|
| `match_strength_threshold` | 50 | FR3 |
| `min_addresses_per_area` | 1 | FR4 |
| `min_age_years` | 17 | FR5 |
| `excluded_business_unit_code` | BS | FR6 |
| `export_file_date` | 2026-08-20 | File naming |
| `export_file_sequence` | 1of1 | File naming |

**None of these is hard-coded.** Changing a threshold is a parameter edit, not a code change.

---

## 13. Onboarding JSON, Attribute by Attribute

### 13.1 The specification at a glance

| Section | Count | Purpose |
|---|---|---|
| `ingestion_flows` | 6 | One per source file |
| `transformation_flows` | 10 | 6 business logic, 4 export sinks |
| `reconciliation_flows` | 6 | Presence gates |
| `observability` | 1 | Telemetry destination |
| `pipeline_parameters` | 6 | The tunable rules |

### 13.2 Key ingestion attributes

| Attribute | Value | Why |
|---|---|---|
| `source_type` | `autoloader` | File-based incremental ingestion |
| `reader_options.delimiter` | `\|` or `,` | Excalibur is comma-delimited |
| `schema_config_path` | Set on five flows | Maps positional columns to names |
| `source_zip_handling.member_format` | `gzip` | **EA file only.** It is a gzip inside a GPG envelope |
| `pre_extraction_decryption.type` | `pgp_symmetric` | Passphrase-based, not key-based |

### 13.3 Key transformation attributes

| Attribute | Value | Why |
|---|---|---|
| `target_type` | `materialized_view` | Every silver and gold flow aggregates |
| `cdc_load_strategy` | `TRUNCATE_AND_LOAD` | Each run recomputes from scratch |
| `is_streaming` | `false` everywhere | The whole chain is batch. See 13.4 |
| `export_trigger` | `per_update` | **The v1.7.5 enhancement.** See Section 19 |

### 13.4 Why the entire chain is batch, not streaming

| Reason | Detail |
|---|---|
| **The SQL aggregates** | `GROUP BY`, `DISTINCT` and window functions are batch operations |
| **Delta cannot stream a recomputed table** | A `TRUNCATE_AND_LOAD` target is fully replaced each run |
| **A streaming union is illegal** | `uc6_ee_address_paf` unions three branches. Spark rejects mixing streaming and batch |
| **Windows need watermarks** | `ROW_NUMBER()` over a stream requires a watermark. Over a batch it does not |

**All four of these were discovered at runtime**, not at validation. See Appendix A.

---

## 14. The Export Files

### 14.1 The four files

| File | Channel | Envelope | Content |
|---|---|---|---|
| `EE_2026-08-20-LEIDOS_TELEPHONE_1of1.csv.gz` | Leidos | Plain gzip | Telephone list |
| `EE_2026-08-20-LEIDOS_OSAPR_1of1.csv.gz` | Leidos | Plain gzip | OSAPR audit list |
| `EE_2026-08-20-TELEPHONE_1of1.csv.gz.gpg` | Fujitsu | **gzip + GPG** | Telephone list |
| `EE_2026-08-20-OSAPR_1of1.csv.gz.gpg` | Fujitsu | **gzip + GPG** | OSAPR audit list |

### 14.2 The file format contract

| Property | Value |
|---|---|
| **Delimiter** | Pipe |
| **Header** | Included |
| **Line terminator** | LF, not CRLF |
| **Telephone header** | `targetAreaID\|telephone` |
| **OSAPR header** | `targetAreaID\|osapr\|count\|status` |

### 14.3 Verified content, byte-identical across channels

- The Leidos and Fujitsu files contain **identical rows**. Only the encryption envelope differs.
- Verified by test C7: **byte-identical after decryption**.
- **Why two channels exist:** the Fujitsu-to-Leidos cutover runs in parallel, so both suppliers must receive the same data.

### 14.4 Framework columns are stripped

- Bronze, silver and gold tables carry seven `__framework_*` lineage columns.
- **Export files carry none of them.**
- **Why:** the file format is a contractual supplier interface. A five-column header where the contract says two columns is a breach.

---

## 15. Encryption and Secrets

### 15.1 Symmetric PGP, both directions

| Direction | What is encrypted | Key |
|---|---|---|
| **Inbound** | The EA request file, `.csv.gz.gpg` | The shared passphrase |
| **Outbound** | Two of the four export files | **The same passphrase** |

### 15.2 Why symmetric, not a keypair

| Property | Symmetric (used here) | Asymmetric |
|---|---|---|
| **Key material** | One shared passphrase | A keypair per party |
| **Signing** | Not available | Available |
| **Provenance** | **None.** Anyone with the passphrase can forge a file | Signature proves the sender |
| **When correct** | Two parties already share a secret out of band | The receiver must prove who sent it |

> **The honest trade-off:** symmetric encryption gives confidentiality but **not provenance**. It was chosen because the interface specification requires it, and both parties already hold the passphrase.

### 15.3 How the passphrase is held

| Aspect | Detail |
|---|---|
| **Reference in the spec** | `secret_catalog`, `secret_schema`, `secret_key` |
| **Resolved value** | Never in the JSON, the control tables, or any log |
| **On this workspace** | Unity Catalog secrets are **disabled**, so the framework falls back to a classic scope named `flowx.config` |
| **Cipher** | AES256 |
| **Verified** | Round-tripped against the real GnuPG 2.4.9 CLI in both directions |

---

## 16. Testing SQL for Business Users

**All queries are read-only.** Open a Databricks SQL editor, attach any warehouse, and run.

### T1 — Row counts at every layer

```sql
SELECT 'bronze.uc6_ea_request'  AS table_name, count(*) AS rows FROM flowx.bronze.uc6_ea_request
UNION ALL SELECT 'bronze.uc6_css_account',         count(*) FROM flowx.bronze.uc6_css_account
UNION ALL SELECT 'bronze.uc6_css_account_address', count(*) FROM flowx.bronze.uc6_css_account_address
UNION ALL SELECT 'bronze.uc6_css_subscription',    count(*) FROM flowx.bronze.uc6_css_subscription
UNION ALL SELECT 'bronze.uc6_jt_customer',         count(*) FROM flowx.bronze.uc6_jt_customer
UNION ALL SELECT 'bronze.uc6_excalibur_address',   count(*) FROM flowx.bronze.uc6_excalibur_address
UNION ALL SELECT 'silver.uc6_ea_base',             count(*) FROM flowx.silver.uc6_ea_base
UNION ALL SELECT 'silver.uc6_ea_address',          count(*) FROM flowx.silver.uc6_ea_address
UNION ALL SELECT 'silver.uc6_ee_address_paf',      count(*) FROM flowx.silver.uc6_ee_address_paf
UNION ALL SELECT 'silver.uc6_matched_address',     count(*) FROM flowx.silver.uc6_matched_address
UNION ALL SELECT 'gold.uc6_osapr_output',          count(*) FROM flowx.gold.uc6_osapr_output
UNION ALL SELECT 'gold.uc6_telephone_output',      count(*) FROM flowx.gold.uc6_telephone_output;
```

**Expected:** bronze 7, 5, 5, 5, 1, 1. Silver 7, 7, 6, 11. Gold 7, 2.

### T2 — The EA request, as received

```sql
SELECT targetAreaID, osapr, postcode, address_norm
FROM   flowx.silver.uc6_ea_address
ORDER  BY targetAreaID, osapr;
```

**Expected:** 7 rows across 4 target areas.

### T3 — The EE customer union, by channel

```sql
SELECT source_system, count(*) AS customers, count(DISTINCT msisdn) AS distinct_msisdn
FROM   flowx.silver.uc6_ee_address_paf
GROUP  BY source_system
ORDER  BY customers DESC;
```

**Expected on the current fixture:** `css` 5 customers, `excalibur` 1 customer. **The `jt` channel returns no rows at all.**

**This is the age filter working, not a defect.** The bronze table `uc6_jt_customer` holds exactly one record, MSISDN `07700900999` with a date of birth of `20150101`. That customer is under 17, so the JT branch of the union filters them out entirely and the channel disappears from the result. Compare the bronze count against this query to see the filter take effect:

| Channel | Bronze rows | Rows surviving the age filter |
|---|---|---|
| `css` | 5 | 5 |
| `jt` | 1 | **0** |
| `excalibur` | 1 | 1 |

### T4 — The matching join, every candidate pair

```sql
SELECT targetAreaID, osapr, postcode, source_system, msisdn, match_strength
FROM   flowx.silver.uc6_matched_address
ORDER  BY targetAreaID, osapr, match_strength DESC;
```

**Expected:** 11 rows. Compare against the table in Section 10.4.

### T5 — The audit answer, the OSAPR decision table

```sql
SELECT targetAreaID, osapr, status, count,
       count_of_msisdn, count_of_osapr, match_strength
FROM   flowx.gold.uc6_osapr_output
ORDER  BY targetAreaID, osapr;
```

**Expected:** 7 rows. All four statuses present. Compare against Section 11.3.

### T6 — The action list, numbers to warn

```sql
SELECT targetAreaID, telephone
FROM   flowx.gold.uc6_telephone_output
ORDER  BY targetAreaID, telephone;
```

**Expected:** exactly 2 rows, both AREA_FOUND.

### T7 — Prove the privacy rule worked

```sql
SELECT '07700900003 must be ABSENT (privacy rule)' AS check_name,
       CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM   flowx.gold.uc6_telephone_output
WHERE  telephone = '07700900003';
```

**Why:** `07700900003` matched at strength 100, but its area had only one matched address. It must be withheld.

### T8 — Prove the age filter worked

```sql
SELECT '07700900999 must be ABSENT (under-17)' AS check_name,
       CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM   flowx.gold.uc6_telephone_output
WHERE  telephone = '07700900999';
```

### T9 — Prove the four export files exist

```sql
LIST '/Volumes/flowx/staging/uc_6/output/';
```

**Expected:** four files, two `.csv.gz` and two `.csv.gz.gpg`, plus a `_staging` folder you can ignore.

### T10 — Verify the scoring by hand

```sql
SELECT ea.osapr,
       ea.address_norm                        AS ea_address,
       ee.address_norm                        AS ee_address,
       size(split(ea.address_norm, ' '))      AS ea_words,
       size(array_intersect(split(ea.address_norm,' '), split(ee.address_norm,' '))) AS shared_words,
       cast(round(100.0 *
            size(array_intersect(split(ea.address_norm,' '), split(ee.address_norm,' ')))
            / greatest(size(split(ea.address_norm,' ')), 1)) AS INT) AS recomputed_score
FROM   flowx.silver.uc6_ea_address ea
JOIN   flowx.silver.uc6_ee_address_paf ee ON ea.postcode_norm = ee.postcode_norm
ORDER  BY recomputed_score DESC;
```

**Why:** recomputes `match_strength` from first principles so you can confirm the algorithm by eye.

### T11 — Governance tags

```sql
SELECT schema_name, table_name, tag_name, tag_value
FROM   flowx.information_schema.table_tags
WHERE  table_name LIKE 'uc6_%'
ORDER  BY schema_name, table_name, tag_name;
```

**Expected:** 72 rows across 12 tables.

**Important:** `information_schema` is catalog-scoped. The three-part name `flowx.information_schema` is mandatory. An unqualified query returns rows for the wrong catalog and looks convincingly like "tags are not supported here".

### T12 — Full data-quality scorecard, run this for a demonstration

```sql
WITH checks AS (
    SELECT 'OSAPR output has 7 rows' AS check_name,
           (SELECT count(*) FROM flowx.gold.uc6_osapr_output) AS actual, 7 AS expected
    UNION ALL SELECT 'Telephone output has 2 rows',
           (SELECT count(*) FROM flowx.gold.uc6_telephone_output), 2
    UNION ALL SELECT 'All four statuses are present',
           (SELECT count(DISTINCT status) FROM flowx.gold.uc6_osapr_output), 4
    UNION ALL SELECT 'Privacy rule held (07700900003 absent)',
           (SELECT count(*) FROM flowx.gold.uc6_telephone_output WHERE telephone='07700900003'), 0
    UNION ALL SELECT 'Age filter held (07700900999 absent)',
           (SELECT count(*) FROM flowx.gold.uc6_telephone_output WHERE telephone='07700900999'), 0
    UNION ALL SELECT 'Governance tags applied',
           (SELECT count(*) FROM flowx.information_schema.table_tags WHERE table_name LIKE 'uc6_%'), 72
)
SELECT check_name, expected, actual,
       CASE WHEN actual = expected THEN 'PASS' ELSE 'FAIL' END AS result
FROM   checks ORDER BY result DESC, check_name;
```

**Expected:** every line reads PASS.

---

## 17. Data Quality and Presence Gates

### 17.1 The problem presence gates solve

| Scenario | Without a gate | With a gate |
|---|---|---|
| A source file is missing | Pipeline succeeds, output is short | **Pipeline fails loudly** |
| A source file is empty | Same silent failure | Same loud failure |

### 17.2 Why a normal DQ rule cannot do this

- A DQ rule is a **per-row predicate**.
- **Zero rows means zero evaluations.** A missing file passes every rule trivially.
- **This is the exact failure the requirement exists to prevent.**

### 17.3 How UC6 solves it

| Aspect | Detail |
|---|---|
| **Mechanism** | Six reconciliation flows, one per source |
| **Assertion** | `source_record_count > 0` |
| **Why it works** | It attaches to a one-row metrics dataset, where the count is a real column |
| **Execution mode** | `pipeline_audit_only` |
| **Effect on failure** | The pipeline update fails, and a row is left in the control tables for alerting |

---

## 18. Governance and Observability

### 18.1 Governance tags, verified

| Aspect | Detail |
|---|---|
| **Tags applied** | **72 tags across 12 tables** |
| **Applied by** | The `apply_governance_uc6` job task, after the pipeline update |
| **Why post-deployment** | `ALTER TABLE ... SET TAGS` is DDL against a materialised table. It cannot run inside the pipeline graph |

### 18.2 The six tags on every table

| Tag | Value | Purpose |
|---|---|---|
| `use_case` | `uc6` | Which use case owns this table |
| `source_system` | `css`, `jt`, `excalibur`, `env_agency`, `derived` | Provenance |
| `data_classification` | `confidential` | Handling requirement |
| `pii` | `true` or `false` | Drives access policy |
| `owner` | `business_data_and_ai` | Accountability |
| `environment` | `poc` | Deployment stage |

### 18.3 Sink flows carry no tags, deliberately

- A sink has **no materialised table** to tag.
- Attempting to tag one raises `TABLE_OR_VIEW_NOT_FOUND` and **fails the whole group's tagging**.
- The framework now skips `target_type: "sink"` flows. See Appendix A, defect 13.

### 18.4 Observability

| Aspect | Detail |
|---|---|
| **Destination** | `/Volumes/flowx/observability/app_logs/uc6/` |
| **Format** | JSONL, gzip-compressed |
| **Written by** | The `observability_export` job task |
| **Verified** | One file, approximately 100 KB, per run |

---

## 19. Framework Enhancements UC6 Required

### 19.1 The four capabilities

| # | Enhancement | Why UC6 needed it |
|---|---|---|
| **F1** | **Symmetric PGP** | `crypto/pgp.py` was asymmetric-only. UC6's EA file and two outputs are passphrase-encrypted |
| **F2** | **gzip landing members** | A `.csv.gz.gpg` decrypts to a bare gzip stream, which the ZIP extractor cannot open |
| **F3** | **gzip egress with a CSV dialect** | The sink wrote comma-only, CRLF CSV inside a ZIP. UC6 needs pipe-delimited, LF, gzip |
| **F4** | **`export_trigger: per_update`** | **The largest.** An aggregating table could not be exported at all |

### 19.2 F4 explained, the structural contradiction

| Fact | Consequence |
|---|---|
| A Lakeflow sink accepts **only streaming** queries | Batch payloads are rejected |
| Delta **cannot stream** a fully-recomputed table | An aggregating MV cannot feed a sink |
| **Together** | A `GROUP BY` result could be computed and published, and then had **no way to leave the platform as a file** |

### 19.3 How `per_update` resolves it

| Component | What it does |
|---|---|
| **The trigger** | A one-row pulse carrying no data, making the append flow genuinely streaming |
| **The payload** | The gold table read as a **batch** |
| **The result** | Exactly one export per pipeline update, **including an update that ingested nothing** |

### 19.4 Why the pulse source matters

- The first implementation used Spark's plain `rate` source.
- **It counts rows as wall-clock seconds since the checkpoint was created.**
- Measured live: **1 row on a fresh checkpoint, 0 rows on the next incremental update.**
- Zero rows means **no export file at all**, silently.
- The shipped implementation uses `rate-micro-batch`, which is clock-independent.

---

## Appendix A — Defects Found by This Build

### A.1 Specification defects, found at runtime

| # | Defect | Impact |
|---|---|---|
| 1 | Landed gzip filename kept its `.gpg` suffix | Silent zero-row ingest |
| 2 | `CSS_account_*` glob also matched `CSS_account_address_*` | Wrong file into wrong table |
| 3 | Excalibur configured as pipe-delimited | Would produce a one-column table |
| 4 | Under-17 customer reached the telephone list | Age filter parenthesisation |
| 5 | Excalibur schema config off-by-one | Two columns mapped to the same name |
| 6 | Five sources pointed at `_extracted/`, which nothing writes | `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` |
| 7 | Schema-config files never uploaded | Pipeline failed at flow generation |
| 8 | Presence gates used `__framework_hash_key` as its own match key | Circular. The matcher hashes real columns into it |
| 9 | Streaming and batch mixed in a `UNION ALL` | Spark rejects it outright |
| 10 | `ROW_NUMBER()` over a stream without a watermark | Illegal in Spark |

### A.2 Framework defects, fixed as reusable capabilities

| # | Defect | Fix |
|---|---|---|
| 11 | An aggregating MV could not reach a sink at all | `export_trigger: per_update` |
| 12 | The export pulse used the wall-clock-dependent `rate` source | Switched to `rate-micro-batch` |
| 13 | Governance tagging failed the whole group when a sink flow was present | Skip `target_type: "sink"` |
| 14 | Every export leaked three `__framework_*` columns | Strip by prefix on all sink paths |
| 15 | Two JSON-schema defects affecting every reconciliation spec in the repository | `unevaluatedProperties` fix |

### A.3 The lesson worth recording

> **Ten of these fifteen defects passed both offline validation gates and surfaced only at Lakeflow graph planning or execution, one per ten-minute cycle.**

**The framework's most expensive missing capability is a pre-flight that exercises graph planning.** That is the single most valuable thing to build next.

---

## Appendix B — Operational Runbook

### B.1 Running UC6 from scratch

```bash
# 1. Re-upload the EA request file. It is CONSUMED on every run.
databricks fs cp docs/UC6/test_fixture/uc_6/raw/EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg \
  dbfs:/Volumes/flowx/staging/uc_6/raw/EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg \
  -p metaflow_v7 --overwrite

# 2. Deploy. NEVER do this while a pipeline is running.
databricks bundle deploy -t metaflow_v7

# 3. Run the job.
databricks bundle run uc6_ea_flood_warning_job -t metaflow_v7

# 4. Validate the output against the specification.
python scripts/validate_uc6_pipeline_output.py
```

### B.2 Prerequisites that must exist

| Prerequisite | How to check |
|---|---|
| Six files in `raw/` | `databricks fs ls dbfs:/Volumes/flowx/staging/uc_6/raw/` |
| Five schema configs in `_schema_configs/` | Same command, different folder |
| The passphrase secret | `databricks secrets list-secrets flowx.config` |
| Schemas `bronze`, `silver`, `gold`, `staging` | `databricks schemas list flowx` |
| Volume `flowx.observability.app_logs` | `databricks volumes read flowx.observability.app_logs` |

### B.3 Failure playbook

| Symptom | Likely cause | Fix |
|---|---|---|
| `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` | A source path has no matching files | Check the glob and the folder |
| `schema_config_path does not exist` | Schema configs not uploaded | Upload them to `_schema_configs/` |
| Zero-row export files | The pulse emitted no rows | Confirm `rate-micro-batch`, not `rate` |
| `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` | A streaming source was renamed | One-time `full_refresh=True` |
| `No usable value for offset` | A streaming source type changed | Targeted refresh of that table |
| `TABLE_OR_VIEW_NOT_FOUND` during tagging | A sink flow carries governance tags | Framework now skips sinks |
| Zero matches in the output | Using the **supplied bundle**, not the augmented fixture | Use `docs/UC6/test_fixture/` |

### B.4 The two fixtures, and which to use

| Fixture | Path | Behaviour |
|---|---|---|
| **Augmented** | `docs/UC6/test_fixture/` | **Use this.** Exercises all four statuses |
| **Supplied** | `docs/UC6/sample_bundle/` | Produces **zero matches**. No postcode overlap, no CSS join-key overlap |

> The supplied bundle cannot demonstrate the business rules. It is preserved unmodified for reference, and its zero-match behaviour is pinned by a test so nobody mistakes it for a defect.

---

## Appendix C — Comparison With UC3 and UC7

| Dimension | UC3 Excalibur | **UC6 Flood Warning** | UC7 CDR ASN.1 |
|---|---|---|---|
| **Source format** | Oracle CRM extracts | **Pipe and comma delimited, gzip, GPG** | ASN.1 BER binary |
| **Ingestion flows** | 3 | **6** | 4 |
| **Transformation flows** | Several | **10** | 0 |
| **Reconciliation flows** | Yes | **6 presence gates** | 0 |
| **Joins in pipeline** | Yes | **Six, including the matching join** | None |
| **CDC strategy** | SCD1 and SCD2 | **APPEND and TRUNCATE_AND_LOAD** | APPEND only |
| **File export** | No | **Yes, four files per run** | No |
| **Encryption** | No | **Symmetric PGP, both directions** | No |
| **Governance tags** | Yes | **72 tags on 12 tables** | None |
| **Layers built** | Bronze, silver | **Bronze, silver, gold, sinks** | Bronze only |

### C.1 Why UC6 is the most complete of the three

- It is the only use case that exercises **every layer**, from ingestion through to an encrypted file export.
- It is the only one with a **contractual output format** that cannot change.
- It forced **four framework enhancements**, more than the other two combined.

---

## Document Provenance

| Fact type | How it was obtained |
|---|---|
| Row counts at every layer | Executed against `flowx` on `metaflow_v7` |
| The decision table, all 7 rows | Read from `flowx.gold.uc6_osapr_output` |
| The join output, all 11 rows | Read from `flowx.silver.uc6_matched_address` |
| Join SQL and business rules | Read from `onboarding/uc6/uc6_ea_flood_warning.json` |
| Governance tag count | Counted in `flowx.information_schema.table_tags` |
| Export file names and sizes | Listed from the output volume |
| Job and pipeline status | Read from the Databricks Jobs and Pipelines APIs |

**Nothing in this document is estimated.** Where an assumption exists, such as the match-strength algorithm in Section 10.5, it is labelled as an assumption rather than presented as a recovered requirement.
