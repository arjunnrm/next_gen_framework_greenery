# Data-Variation Regression Plan — `dev_flowx`

> **Purpose.** The `dev_flowx` suite passes today, but every passing run used the *same*
> fixture, landed *once*, under an *unchanged* spec. This plan re-runs that green suite against
> **varied data and varied config** to find the failures a single-shot run cannot expose, then
> adds one net-new end-to-end use case (new schema + new volume + new pipeline).
>
> Target: `dev_flowx` · Profile: `dev_flowx` · Catalog: `flowx`
> Created 2026-08-30. Results are recorded in this same file (§9) as waves complete.

---

## 1. Why these scenarios, and what each one can actually break

| # | Scenario | The failure it is designed to expose |
|---|---|---|
| R1 | **Different file**, same config | Auto Loader incremental pickup; does standardization/DQ apply to batch 2 as it did to batch 1, or was batch-1 correctness an artefact of a first-ever empty-checkpoint run? |
| R2 | **Same filename, overwritten content** | Auto Loader's file registry keys on **path**, not content. A vendor re-sending `companies_batch2.csv` with corrected rows is silently ignored. This is the most common real-world ingestion surprise and the corpus has never tested it. |
| R3 | **Config value changed** on an already-onboarded flow | Does `action_type=UPDATE` actually take effect on a live streaming table, or does the pipeline keep serving the old `source_config_json` until a full refresh? |
| R4 | **Column normalization → lower** | The headline question. `data_standardization_sql` runs **after** normalization, so it must be written against normalized names. Today **zero specs in the corpus exercise this** — see §2. |
| R5 | **New use case**: new schema + new volume + new pipeline | Cold-start provisioning path, and first live coverage for `column_normalization` + `schema_config_path` together. |
| R6 | **Existing jobs re-run** with swapped fixtures | Proves the passing suite is data-independent, not fixture-shaped. |

## 2. The coverage gap this plan closes

```
grep -l "column_normalization|schema_config_path" flowx_testing/*.json
  -> (no matches)
```

`ingestion/column_normalization.py` and `ingestion/schema_config.py` shipped in v1.3.0 with unit
tests but **no live pipeline anywhere in the 44-spec corpus turns either of them on**. R4 and R5
give both their first end-to-end run.

### Open contradiction to settle in R4

Two sources disagree about ordering, and only a live run can decide which is authoritative:

* `ingestion/column_normalization.py` module docstring — *"this must run immediately after the
  raw source read — **before** `ingestion.schema_config`'s renames"*
* `notebooks/03_engine/03_lakeflow_declarative_pipeline.py:242-243` — runs
  `apply_schema_config` **first**, then `normalize_column_names`

The code is what executes; the docstring is what an operator reads when writing a spec. R4c
pins the real order, and whichever is wrong gets corrected.

---

## 3. Answering the column-normalization question up front

**Q: if I normalize column names to lower, what does the standardization SQL become?**

Runtime order in `03_lakeflow_declarative_pipeline.py` (lines 242-271), which is what a spec must
be written against:

```
raw read
  -> apply_schema_config          (renames declared in the external schema_config file)
  -> normalize_column_names       (trim / case-fold / [^A-Za-z0-9_] -> _ / collapse _ / strip edge _)
  -> attach_technical_metadata
  -> parse_json_string_columns
  -> apply_explode_columns
  -> apply_data_standardization_sql   <-- LAST. sees only normalized names.
```

Normalization is opt-in: `column_normalization` is the only switch, and `enabled` defaults to
false, so the step is a pass-through until a spec sets `"column_normalization": {"enabled": true}`.
`case` then selects the fold. So for a source whose CSV header is
`Company Name, Region , E-Mail Address, Country-Code`:

| `column_normalization.case` | Resulting column names |
|---|---|
| `lower` (default) | `company_name`, `region`, `e_mail_address`, `country_code` |
| `preserve` | `Company_Name`, `Region`, `E_Mail_Address`, `Country_Code` |
| `upper` | `COMPANY_NAME`, `REGION`, `E_MAIL_ADDRESS`, `COUNTRY_CODE` |

and `data_standardization_sql` **must** be written against the right-hand column:

```jsonc
// CORRECT  -- post-normalization names
"data_standardization_sql": [
  "TRIM(UPPER(region)) AS region",
  "LOWER(TRIM(e_mail_address)) AS e_mail_address"
]

// WRONG    -- raw source names. Passes onboarding validation, fails at pipeline runtime.
"data_standardization_sql": [
  "TRIM(UPPER(`Region`)) AS region"
]
```

The wrong form is dangerous specifically because it is **not caught at onboarding**:
`spec_validator.py::_validate_data_standardization_sql` enforces grammar only (one column
expression, ends in `AS <name>`, no `SELECT`/`FROM`/`JOIN`/`;`). Column *existence* is resolved
lazily by Spark, so the spec onboards clean and the pipeline dies later with
`UNRESOLVED_COLUMN`. **R4b runs this deliberately to confirm and document that blast radius.**

---

## 4. Pre-flight gates (Wave 0 — no compute)

1. `databricks bundle validate -t dev_flowx -p dev_flowx`
2. Confirm no active runs before any deploy — `databricks jobs list --active-only -p dev_flowx`.
   **Never `bundle deploy` while a wave or pipeline is running** (a deploy prunes the wheel a
   running update is installing → `ENVIRONMENT_PIP_INSTALL_ERROR`).
3. Quota headroom — measured 2026-08-30: **70 schemas, 82 volumes** in `flowx`. Well past the
   50-object free-tier ceiling that invalidated the earlier `dev` runs, so this workspace is not
   quota-limited. R5's new schema+volume double as the live confirmation.
4. Baseline capture — row counts + `DESCRIBE` for every table each wave will touch, so "changed"
   is provable rather than assumed.
5. **Back up before editing** (no git in this repo): copy `flowx_testing/`, `resources/`,
   `notebooks/00_seed_sample_data/` aside first.

---

## 5. Enabling change (one deploy, before any wave runs)

Every `flowx_test_*_job.yml` hardcodes its fixture in `base_parameters`, so today "re-run with
different data" means editing YAML and redeploying each time. Two anchor jobs get job-level
`parameters:` instead — the same pattern `framework_config_onboarding_job` already uses — after
which every variation below is a plain `bundle run --params`, no redeploy.

| Job | New parameters | Default (preserves today's behaviour) |
|---|---|---|
| `flowx_test_ing_007_standardize_job` | `fixture_file`, `spec_file`, `action_type`, `allow_overwrites` | `companies_batch1.csv`, `016_ing_007_standardize.json`, `CREATE`, `false` |
| `flowx_test_cdc_003_scd1_job` | `fixture_file`, `action_type` | `customer_scd1_day1.csv`, `CREATE` |

Matching change in the two seed notebooks: a `fixture_file` widget replacing the hardcoded
filename, and an `overwrite` widget for R2. Defaults are the current values, so the existing
green runs stay reproducible.

> **Verify the notebook edits actually landed.** DABs' sync-snapshot can go stale and report
> `Files: 0 uploaded` while genuinely-changed notebooks never reach the workspace. After deploy:
> `databricks workspace export <file_path>/notebooks/00_seed_sample_data/03_seed_ing_007_standardize_data -p dev_flowx`
> (no `.py` extension — with it you get "Path doesn't exist", which reads exactly like an empty file).

---

## 6. The waves

Anchor A = `flowx_test_ing_007_standardize_job` (Auto Loader CSV → Bronze, **APPEND**, already
uses `data_standardization_sql`; row *count* is the assertion).
Anchor B = `flowx_test_cdc_003_scd1_job` (**SCD1 MERGE**; row *identity* is the assertion — a
different failure mode from A, which is why both are run).

### Wave 1 — R1 · different file, same config

* New fixture `master_usecase/companies_batch2.csv` — 3 new companies, identical header, same
  dirty-value shape (padded/mixed-case `region` and `email`).
* Run: `bundle run flowx_test_ing_007_standardize_job --params fixture_file=companies_batch2.csv,action_type=UPDATE`
* **Assert:** `bronze_master.companies_standardized` = 6 rows; `__framework_source_file_name`
  shows both files; the 3 new rows are `TRIM(UPPER(...))`-normalized exactly as batch 1 was.
* Anchor B equivalent: land `customer_scd1_day2.csv` → assert `C001` **updated in place** to
  `tier=PLATINUM` (not duplicated) and `C002` inserted.

### Wave 2 — R2 · same filename, overwritten content

* Overwrite `companies_batch2.csv` **in the volume** with different values, same name, same schema.
* Run the job again, unchanged.
* **Expected — hypothesis, to be confirmed live:** Auto Loader does **not** re-ingest it; the row
  count stays 6 and the corrected values never land. If that holds it is a documented framework
  limitation, not a bug — and the fix is the R2b variant.
* **R2b (doubles as a config-change test):** add `"cloudFiles.allowOverwrites": "true"` to
  `reader_options`, re-onboard `UPDATE`, re-run → expect the overwritten file **is** re-read.
  Note the duplicate-row consequence on an APPEND target and record it.
* Both outcomes are findings. Neither is a pass/fail — the deliverable is documented behaviour.

### Wave 3 — R3 · config value changed on a live flow

Spec edits go to a **copy** (`016b_ing_007_standardize_v2.json`) so the green baseline stays intact:

1. add `"INITCAP(TRIM(company_name)) AS company_name"` to `data_standardization_sql`
2. flip `capture_technical_metadata` false → true
3. change `reader_options` (`cloudFiles.inferColumnTypes` true → false)

Run: `--params spec_file=016b_ing_007_standardize_v2.json,action_type=UPDATE`

* **Assert (control plane):** the `flowx.config` ingestion-flow row's `source_config_json`
  carries the new values, and the audit log records `UPDATE`.
* **Assert (runtime):** *new* rows show `INITCAP` applied and `__framework_*` metadata columns
  present. **Key question:** do already-written rows keep the old shape, and does the streaming
  table accept the schema change without a full refresh? Record whichever happens.

### Wave 4 — R4 · column normalization (the headline)

New fixture `master_usecase/companies_dirty_headers.csv`, header deliberately hostile:
`Company Id, Company Name, Region , E-Mail Address, Country-Code, Updated At`

| Variant | Spec | Expected |
|---|---|---|
| **4a** | `column_normalization: {enabled: true, case: "lower"}` + standardization SQL against **normalized** names | **PASS.** Record the actual resulting names; confirms the §3 table. |
| **4b** | same, but SQL against **raw** names (`` `Region` ``) | **Expected FAIL at runtime** with `UNRESOLVED_COLUMN`, *after* clean onboarding. Negative test — invert the job-status assertion. |
| **4c** | `case: "upper"` **plus** a `schema_config_path` declaring a rename | Confirms Spark's case-insensitive resolution still matches the SQL, **and settles the §2 ordering contradiction** by observing whether the schema_config rename or the normalization won. |
| **4d** | headers `Region` and `region ` (collide after normalization) | **Expected FAIL** — `FrameworkConfigError` duplicate-name guard. Cheap: caught at onboarding, no pipeline compute. |

Each variant gets its own `dfg_*` group id and its own Bronze target table, so they neither
collide nor need a shared pipeline serialised.

### Wave 5 — R5 · net-new end-to-end use case

New domain `retail`: schema `flowx.retail` + `flowx.bronze_retail`, volumes
`retail.landing_orders` + `retail._schemas`, new seed notebook, new spec, new pipeline yml, new
job yml. Deliberately combines the two zero-coverage features:

* `column_normalization` (`case: "lower"`) over a dirty-header orders CSV
* `schema_config_path` — external file declaring explicit casts, UC column comments, and
  source→target renames
* `data_standardization_sql` written against the post-normalization names
* a `dq_config` rule referencing a normalized column (proving §3's ordering rule holds for DQ too)

Then re-run it with a second orders file to confirm the new pipeline is itself incremental.

### Wave 6 — R6 · leverage the existing jobs

* `framework_config_onboarding_job --params action_type=UPDATE` over the whole `flowx_testing/`
  directory — re-onboards every spec after this plan's edits in one run, and reconfirms the
  43/43 bulk result.
* Re-run 3 already-green jobs with swapped fixtures to prove data-independence:
  `flowx_test_dq_004_quarantine_job` (rows that pass vs rows that quarantine),
  `flowx_test_ing_008_tech_metadata_job`, and `flowx_test_ing_009_rescue_schema_job`
  (a genuinely different set of rescued columns).

---

## 7. Operating rules for every wave

1. **Concurrency ≤ 3.** Above that, `RESOURCE_EXHAUSTED: limit for serverless compute`.
2. **Serialise any cases that share a Lakeflow pipeline** — otherwise
   `Pipeline update already in progress`, which reads as a framework failure and is not one.
3. **No `bundle deploy` while anything is running.** One deploy up front (§5), then runs only.
4. **Invert the assertion for expected-failure variants** — R4b and R4d *pass* by failing.
5. **Diagnose from the pipeline's own event log**, not the job runner's summary — the previous
   pass showed the runner's terminal state misattributes real causes.
6. **Every result recorded with its run id**, so any claim here is re-checkable.

## 8. Deliverables

* **Fixtures:** `companies_batch2.csv`, `companies_dirty_headers.csv`, `companies_collision.csv`,
  `retail_usecase/orders_batch{1,2}.csv` + its `schema_config.json`
* **Specs:** `016b_ing_007_standardize_v2.json`, `049_norm_001_lower.json`,
  `050_norm_002_raw_names_negative.json`, `051_norm_003_upper_schemaconfig.json`,
  `052_norm_004_collision_negative.json`, `053_retail_001_new_usecase.json`
* **Resources:** parameterized anchor jobs; new `flowx_test_norm_00X_*` and
  `flowx_test_retail_001_*` pipeline + job yml
* **Notebooks:** `fixture_file`/`overwrite` widgets on two seeders; one new retail seeder
* **Docs:** results table in §9 below; `TESTING_STATUS.md` updated; **`RELEASE_NOTES.md` entry**

## 9. Results

_(populated as waves execute — run id, outcome, and evidence per variant)_
