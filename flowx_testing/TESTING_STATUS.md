# FlowX Testing Status Tracker

> Companion to [`TESTING_PLAN.md`](TESTING_PLAN.md) — tracks actual progress against that plan.
> Last updated: 2026-08-31 (**`TC-R2-001` / scenario S1 is now VERIFIED LIVE** — see §0b, which
> supersedes every earlier row saying S3 is the only executable Module 12 scenario. Module 12 also
> gained `TC-DAG-003` (audit-only) plus `TESTING_PLAN.md` §3.12, the operator-facing scenario
> runbook for S1–S4 — see §4 below).

## How to read this doc

`TESTING_PLAN.md`'s 50 `TC-*` rows (45 pre-v1.5.0 plus Module 12's `TC-DAG-001`/`TC-DAG-002`/
`TC-DAG-003`/`TC-DAG-004`/`TC-R2-001`) are a **planned/aspirational** test matrix — each names its
own dedicated pipeline/job (e.g. `flowx_test_ing_001_zip_filter_pipeline`). Almost none of
those dedicated resources have actually been built yet; what exists today are 4 broader,
already-built scenarios (`001`/`002`+`003`/`100`) that each exercise *several* `TC-*` concerns
at once under different resource names, plus 2 new scenarios added in this pass. Section 1 tracks
the **real, deployable, executable** scenarios (the ground truth). Section 2 maps `TC-*` rows
against that real coverage — most remain `Not Started` (no dedicated resource exists), a few are
`Partially Covered` by a real scenario's broader test, and none are `Fully Covered` yet (no `TC-*`
has its own dedicated 1:1 pipeline/job built to TESTING_PLAN.md's literal template).

Status values: **Not Started** (no resource built) · **Built** (resource exists, not yet run
against the live workspace this pass) · **Deployed** (bundle-deployed, not yet run) ·
**Executed — Pass** · **Executed — Fail** · **Partially Covered** (a broader real scenario
exercises part of this, not a dedicated 1:1 build) · **Not Applicable** (superseded/out of scope).

---

## 0b. `TC-R2-001` / S1 — reconciliation inside the pipeline DAG, VERIFIED LIVE (2026-08-31, latest)

> **This section supersedes §4's and Module 12's earlier statement that "only S3 is fully executable
> today".** S1 has now been run end-to-end against a live workspace and passed. S2, S4 and S5 remain
> not-live, each behind a named blocker (below) — nothing here should be read as covering them.

### Result

| Field | Value |
|---|---|
| Test case | `TC-R2-001` (`TESTING_PLAN.md` §3.12.1, scenario **S1**) |
| Target | `dev_flowx` |
| Job | `flowx_test_recon_dag_job`, job id **`854232399214818`** |
| Pipeline | **`be78d88d-6064-414d-a10c-2aacd900fa86`** (`flowx_test_003_autoload_recon_pipeline`) |
| Date | **2026-08-31** |
| Spec | `flowx_testing/003_autoload_recon_append.json` (`execution_mode: "pipeline"`) |
| Outcome | **Executed — Pass** (job run SUCCESS) |

### Task results (all four)

| Task | Result |
|---|---|
| `setup_control_tables` | **SUCCESS** |
| `seed_flowx_testing_data` | **SUCCESS** |
| `onboard_003` | **SUCCESS** |
| `run_003_pipeline` | **SUCCESS** |

`setup_control_tables` is load-bearing here, not boilerplate: it is the *only* thing that applies the
new additive control-table column migration (`execution_mode` / `publish_schema` / `dq_config_json`
on `reconciliation_flow_spec`). `bundle deploy` does **not** apply it — see §0b "What this did and
did not prove".

`onboard_003` runs via the generic parameterised `resources/flowx_config_jobs/onboarding_job.yml` (`run_job_task`), not
an inlined `02_onboarding_engine.py` notebook task — the new convention for any *new* job. The
standalone `run_003_reconciliation` task was **deleted** from
`resources/feature_tests/flowx_test_002_003_job.yml`; its absence is part of the pass criteria, because with
`003` flipped to `execution_mode: "pipeline"` that task would have run reconciliation a second time
and risked a double append into `Excalibur_usecase.zerobus_source_bus`.

### Evidence — what pipeline `be78d88d` registered in ONE update

Taken from the pipeline's own event log (`dataset_definition` / `flow_definition` / `sink_definition`
events for that update), this is the direct proof of **R1** (ingestion + transformation +
reconciliation in one DAG):

| Registered object | Kind | Layer |
|---|---|---|
| `flowx.bronze_excalibur.autoload_bronze` | `STREAMING_TABLE` | ingestion |
| `_recon__recon_excalibur_autoload_vs_zerobus__src` | `STREAMING_TABLE` | L3 prepare (source side) |
| `_recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__tgt` | `MATERIALIZED_VIEW` | L3 prepare (target side) |
| `recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__classified` | `MATERIALIZED_VIEW` | L4 |
| `recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__metrics` | `MATERIALIZED_VIEW` | L4 |
| `recon__recon_excalibur_autoload_vs_zerobus__zerobus_bronze_target__mismatch` | `MATERIALIZED_VIEW` | L4 |
| `_recon__recon_excalibur_autoload_vs_zerobus__pulse` | `STREAMING_TABLE` | L5 gate |
| `recon__recon_excalibur_autoload_vs_zerobus__heal_flow` | `APPEND` flow | L5 |
| `_recon__recon_excalibur_autoload_vs_zerobus__heal_sink` | sink, `foreachBatch` | L5 |

### What this did and did not prove

- **`dlt.foreach_batch_sink` IS available on DBR serverless.** The design previously treated this as
  unproven and named `pipeline_audit_only` as the fallback if it turned out not to exist. The
  `_recon__…__heal_sink` `sink_definition` above settles it: the L5 heal lane is real, not
  theoretical. The `hasattr` guard in `reconciliation/graph_registration.py::register_foreach_batch_sink`
  stays — the locally installed `databricks-dlt` 0.3.0 stub still lacks the symbol, so the guard is
  still correct — but it is no longer expected to trip on DBR.
- **The additive control-table migration is required and is deploy-invisible.** On this workspace
  `flowx.config.reconciliation_flow_spec` had **none** of `execution_mode` / `publish_schema` /
  `dq_config_json`, and pipeline-mode onboarding failed with `UNRESOLVED_COLUMN` until
  `setup_control_tables` ran. Every statement in `get_all_control_table_ddls` is
  `CREATE TABLE IF NOT EXISTS`, a no-op against an existing table, so a column added to a `CREATE`
  reaches **new installations only**. Running `databricks bundle deploy` does not fix an existing
  workspace; running the `setup_control_tables` task does.
- **Backward compatibility held.** Every pre-existing reconciliation row on this workspace still
  reads `execution_mode` NULL, which `load_active_group_metadata` resolves to `"job"` in Python, and
  `register_reconciliation_flow` early-returns on it. No job-mode flow was registered into the graph.
- **It did NOT prove S2, S4 or S5.** One passing scenario is one scenario. In particular it does not
  prove the `pipeline_audit_only` branch (S2/`TC-DAG-003`), the read-once source plane across three
  flow kinds (S4/`TC-DAG-001`), or the geneva topology (S5/`TC-DAG-004`).

### Still not live, with blockers (unchanged by this run)

| Scenario | TC | State | Blocker |
|---|---|---|---|
| S2 — audit-only | `TC-DAG-003` | Not Started (spec-only) | No pipeline/job resource, no seed notebook for `dag_003_usecase` / `dag_003_downstream` / `dag_003_remediation`, and 5 new schemas needed against the 50-schema metastore ceiling. |
| S3 — negative cycle | `TC-DAG-002` | **Executed — Pass (offline validator)** | None — needs no workspace. |
| S4 — read-once source plane | `TC-DAG-001` | Not Started | **B3**: `flowx_testing/049_dag_001_unified_three_flow.json` depends on `flowx.dag_001_usecase.shared_source_bus`, which **no seed notebook populates**. Also no `observability` destinations array, so `run_observability_task` fails loudly. |
| S5 — geneva `e41a47ba` in-DAG | `TC-DAG-004` | **Partly Verified — OFFLINE ONLY** | **B1**: pipeline `e41a47ba-5ad0-4dc5-9535-5aa16cc97e65` is a `[dev arjun]` pipeline reading its wheel from `flowx@nrmanalytix.com`'s artifact path; every `dev_flowx` deploy builds a uniquely-named wheel and **prunes** the old one while updating only the pipelines that target owns, so this pipeline is orphaned by each deploy and fails with `ENVIRONMENT_PIP_INSTALL_ERROR`. No `dev_flowx` deploy repairs it; it needs its own `artifact_path` or to be brought under the bundle. Pre-existing bundle-topology problem, **not** caused by v1.5.0. **B2**: `flowx.bronze_excalibur.bronze_tariffelementband` grants `SELECT` to `arjun@`, `gowtham@` and `varadaraju@` only — not `flowx@nrmanalytix.com`, which is what the pipeline runs as (schema-level access is fine; `flowx@` reads `bronze_excalibur.autoload_bronze`). Needed: `GRANT SELECT ON TABLE flowx.bronze_excalibur.bronze_tariffelementband TO \`flowx@nrmanalytix.com\`;` |

**S5 is verified OFFLINE only** — validator + `plan_source_plane`, pinned by
`tests/unit/test_geneva_e41a47ba_topology.py` (6 tests). It has never had a live run. Do not report
it as a passing live scenario.

---

## 0a. Targeted pipeline-failure investigation — `dev_flowx` (2026-08-29)

> **This section supersedes the per-test rows below for the five pipelines it names.** Each was
> diagnosed from its own Lakeflow event stream rather than from runner logs, and each was re-run
> to a verified result. Wheels: `0.0.1788021618085` → `…22819141` → `…23330678` → `…23654366`
> (each round fixed the error the previous round exposed).

### Result

| Test | Pipeline | Before | After |
|---|---|---|---|
| `TC-ING-004` | `flowx_test_ing_004_asn1_decode` | `CF_UNKNOWN_OPTION_KEYS_ERROR` | ✅ **PASS** |
| `SCN-002` | `flowx_test_002_zerobus` | `DELTA_SOURCE_TABLE_IGNORE_CHANGES` | ✅ **PASS** (full refresh + end-to-end 002-003 job) |
| `TC-CDC-002` | `flowx_test_cdc_002_truncate` | graph self-cycle | ✅ **PASS** |
| `TC-CDC-006` | `flowx_test_cdc_006_snapshot_pk` | 3 stacked snapshot-wiring errors | ✅ **PASS** |
| `TC-CDC-007` | `flowx_test_cdc_007_snapshot_nopk` | same, then a `setup_control_tables` UC race | ✅ **PASS** |

**All five were real defects.** The earlier taxonomy in §0 attributed most failures to workspace
capacity; that was true of the first `dev` wave, but none of these five was environmental.

### What each defect actually was

1. **`TC-ING-004` — `cloudFiles.fileNamePattern` does not exist.** Auto Loader validates
   `cloudFiles.`-prefixed keys against a closed whitelist *without consulting
   `cloudFiles.format`*, so the prefixed spelling is invalid for csv/json/parquet/avro exactly as
   it is for `binaryFile`. The earlier format-aware fix corrected only `binaryFile` and left the
   trap in place for every other format. `TC-ING-004` is the **only** spec in the corpus that sets
   `file_pattern`, which is why nothing else ever exercised the broken branch. Now every format
   uses the generic, un-prefixed `pathGlobFilter`. Regression-tested in
   `tests/unit/test_autoloader_file_pattern.py`.

2. **`SCN-002` — an idempotent seeder poisoned an append-only stream.** `zerobus_source_bus` is
   the streaming source for `zerobus_bronze`. The seeder MERGEd with `whenMatchedUpdate()`, which
   rewrote already-present rows on every re-seed even when values were byte-identical. A Delta
   streaming source must be append-only, so the *second* run of the seeder broke the pipeline
   permanently. Both zerobus seeders are now insert-only — still idempotent, and a better model of
   an event bus. One full refresh cleared the historical MERGE commit.

3. **`TC-CDC-002` — E09's guard made the target read itself.** `Graph is not topologically
   sorted. There is a cycle between dim_fx_rates_current and dim_fx_rates_current`, raised before
   a single flow ran. **E09 is withdrawn**; see TESTING_PLAN.md §0 and
   `docs/03_transformation_and_cdc.md`.

4. **`TC-CDC-006`/`TC-CDC-007` — a snapshot lambda may not touch any pipeline dataset.** Three
   errors in sequence, each only visible after the previous was fixed:
   `TABLE_OR_VIEW_NOT_FOUND` (lambda can't resolve a pipeline-local view) →
   `REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION` (materializing it doesn't help — *no*
   pipeline dataset may be referenced from the lambda) → `View ... is a streaming view and must
   be referenced using readStream`. The strategy now registers a real `@dlt.table` snapshot-input
   dataset, does the delete-value filter and `primary_keys` presence guard inside it, hands
   `apply_changes_from_snapshot` that dataset's **name**, and threads `is_streaming` down so the
   upstream is read with the matching API. Regression-tested in
   `tests/unit/test_snapshot_input_dataset.py`.

5. **`setup_control_tables` — UC's `CREATE OR REPLACE FUNCTION` is not atomic.** Three concurrent
   test jobs raced on `preflight_check_onboarding_spec`; the loser got `[ROUTINE_ALREADY_EXISTS]`
   and every downstream task was skipped, which read as a `TC-CDC-007` framework failure. A narrow
   `is_already_exists_race()` predicate — shared by `schema_provisioner.py` and
   `01_setup_control_tables.py` — now treats "someone else created it" as success while letting
   permission, missing-schema, quota and syntax errors fail loudly. Regression-tested in
   `tests/unit/test_already_exists_race.py`.

### Carried limitation (not fixed)

`apply_changes_from_snapshot` reads its source dataset's *current contents* as the latest
snapshot. Over a streaming Auto Loader upstream those contents accumulate, so the Day-1/Day-2
diffing that `TC-CDC-006`/`TC-CDC-007` specify in TESTING_PLAN.md §3 is **not** achieved: after
Day-2 lands, the dataset holds Day-1 ∪ Day-2, so removed keys are never deleted. The passing runs
above exercise the **single-snapshot** path only. Supporting true multi-snapshot diffing requires
the path-based lambda pattern and a new spec contract — see the note in `cdc/snapshot.py`.

---

## 0. v1.3.0 Live Execution Results (2026-08-29)

> **This is the first-ever live execution of the 40-job `TC-*` backlog.** Everything below the
> previous "Build Pass Update" section described artifacts that were *built but never run*.
> They have now been run against the `dev` target (`dbc-0f3a637e-dc15.cloud.databricks.com`,
> catalog `flowx`) via the new `framework_config_onboarding_job` plus a bounded-concurrency
> wave runner.

### Headline

| Outcome | Count |
|---|---|
| **Executed — Pass** | **12** |
| Executed — Fail (workspace capacity, NOT a framework defect) | 28 |
| Executed — Fail (real platform incompatibility) | 1 (`TC-ING-004`) |
| **Total executed** | **41** |

Plus: **bulk onboarding 43/43 specs SUCCESS** (job run `789890290669369`) — a genuine
end-to-end validation of the v1.3.0 JSON schema, `spec_validator.py` and control-table DDL
against the entire real spec corpus, not a synthetic sample.

### Passed (12)

`TC-CDC-001`, `TC-CDC-002`, `TC-CDC-003`, `TC-CDC-004`, `TC-DQ-001`, `TC-DQ-002`,
`TC-GOV-001`, `TC-ING-001`, `TC-ING-002`, `TC-ING-008`, `TC-ING-009`, `TC-OBS-003`.

Notably this includes `TC-CDC-002` (TRUNCATE_AND_LOAD — exercises E09's new
`empty_target_if_source_empty` guard) and `TC-ING-001`/`TC-ING-002` (ZIP extraction plus the
E01/E02 retention and `delete_after_x_days` sweep paths).

### The dominant failure cause is the workspace, not the code

**28 of the 29 failures are workspace resource-limit errors.** This is a free-tier metastore
now at capacity, and the 44-scenario corpus collectively wants more resources than it allows.
Two distinct hard limits were hit, both confirmed from real task output:

1. **Unity Catalog object quota** —
   `QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED: Cannot create 1 Volume(s) in Metastore ... (estimated count: 51, limit: 50)`
   and `Cannot create 1 Schema(s) in Catalog ... (estimated count: 50, limit: 50)`.
   The `flowx` catalog currently holds **51 schemas against a 50 limit**. Seed notebooks
   that `CREATE SCHEMA` / `CREATE VOLUME` therefore fail before any framework code runs.
2. **Serverless compute quota** —
   `RESOURCE_EXHAUSTED: You've hit the limit for severless compute for free usage.`
   Hit whenever several pipelines were started concurrently.

These fail in the `setup_control_tables` / `seed_*_data` / `onboard_*` tasks — i.e. **before**
the Lakeflow pipeline under test ever starts. They say nothing about framework correctness.
Re-running them after freeing catalog/volume quota is expected to exercise the real code path.

### The one genuine defect found: `TC-ING-004` (ASN.1)

```
[CF_UNKNOWN_OPTION_KEYS_ERROR] Found unknown option keys: cloudFiles.filenamepattern
```

`ingestion/readers.py::_apply_common_autoloader_options` sets `cloudFiles.fileNamePattern`,
which Auto Loader **rejects when `cloudFiles.format` is `binaryFile`** — the format
`read_asn1_source` uses. Any `asn1` source that sets `file_pattern` therefore cannot start.

**This was PRE-EXISTING, not a v1.3.0 regression** — verified by diffing against the
pre-change backup. **It is now FIXED** (2026-08-29): the option is invalid for *every* format,
not just `binaryFile`, so `file_pattern` maps to the generic `pathGlobFilter` throughout.
`TC-ING-004` passes live — see §0a.

`cloudFiles.validateOptions=false` was listed here as an alternative workaround. **Do not use
it.** It suppresses the validator rather than removing the invalid key. Seeing
`CF_UNKNOWN_OPTION_KEYS_ERROR: cloudFiles.filenamepattern` today means the pipeline is pinned to
a pre-fix wheel — repoint it at `0.0.1788023654366` or later.

### Environment sanity correction (release brief §5)

The previously-documented workspace-wide `mapInPandas` sandbox constraint is **too broad**.
Measured this session on both execution paths:

| Probe | Databricks Connect (local) | Serverless **job** compute |
|---|---|---|
| plain Spark | OK | OK |
| Python UDF | **FAIL** `ISOLATION_STARTUP_FAILURE.SANDBOX_STARTUP` | **OK** |
| `mapInPandas` | **FAIL** (same) | **OK** |

Evidence: job run `129438225484662` (task run `1011715445275325`) passed all three probes.
The failure is confined to **Databricks Connect sessions** (the local `pytest` path). ASN.1
and ZIP/PGP code paths are **not** platform-blocked when run as jobs — as `TC-ING-001` and
`TC-ING-002` passing live now demonstrates.

### Per-test-case live result

| TC | Result | Run ID | Failing task (if any) | Cause |
|---|---|---|---|---|
| TC-CDC-001 | **Pass** | 367669341694546 | — | — |
| TC-CDC-002 | **Pass** | 842247946893826 | — | — |
| TC-CDC-003 | **Pass** | 860669431373082 | — | — |
| TC-CDC-004 | **Pass** | 8306326373744 | — | — |
| TC-CDC-005 | Fail | 1056501655070697 | run_pipeline_batch2 | serverless quota |
| TC-CDC-006 | Fail | 140505429081910 | — | serverless quota |
| TC-CDC-007 | Fail | 509013272161974 | — | serverless quota |
| TC-DQ-001 | **Pass** | 14751520434519 | — | — |
| TC-DQ-002 | **Pass** | 928449186848557 | — | — |
| TC-DQ-003 | Fail | 139552813092422 | run_pipeline | serverless quota |
| TC-DQ-004 | Fail | 624321280528114 | run_pipeline | serverless quota |
| TC-FLT-002 | Fail | 139933167671285 | onboard_flt_002 | UC quota (upstream seed) |
| TC-GOV-001 | **Pass** | 258178087552255 | — | — |
| TC-GOV-002 | Fail | 691695065615694 | apply_governance_run_2 | upstream failure |
| TC-ING-001 | **Pass** | 38370808944045 | — | — |
| TC-ING-002 | **Pass** | 381806026713600 | — | — |
| TC-ING-003 | Fail | 406049695116258 | seed_ing_003_zerobus_append | UC quota |
| TC-ING-004 | Fail | 1020140473467191 | run_ing_004_pipeline | **real defect — `cloudFiles.fileNamePattern` with `binaryFile`** |
| TC-ING-005 | Fail | 197950595379786 | — | serverless quota |
| TC-ING-006 | Fail | 192198860048190 | — | serverless quota |
| TC-ING-007 | Fail | 864977671178121 | setup_control_tables | UC quota |
| TC-ING-008 | **Pass** | 442251520741763 | — | — |
| TC-ING-009 | **Pass** | 398536018361817 | — | — |
| TC-OBS-001 | Fail | 159940208473336 | seed_obs_001_json_log_data | UC quota (volume limit) |
| TC-OBS-003 | **Pass** | 404986733785193 | — | — |
| TC-PRM-001 | Fail | 970153741049132 | run_prm_001_pipeline | serverless quota |
| TC-PRM-004 | Fail | 842669823926578 | onboard_prm_004 | upstream UC quota |
| TC-PRM-005 | Fail | 988921946475544 | seed_prm_005_sink_param_data | UC quota (schema limit) |
| TC-PRM-006 | Fail | 569180124601477 | onboard_prm_006 | *(negative test — designed to fail validation; not independently confirmed, quota noise)* |
| TC-REC-002 | Fail | 3902545343329 | run_rec_002_reconciliation | upstream failure |
| TC-REC-003 | Fail | 216882720968991 | run_pipeline | upstream failure |
| TC-SEC-001 | Fail | 380779467740983 | run_pipeline | upstream failure |
| TC-SEC-002 | Fail | 505715056711837 | onboard_sec_002 | upstream failure |
| TC-SEC-003 | Fail | 785734998557351 | run_pipeline | upstream failure |
| TC-SNK-001 | Fail | 490007813770971 | onboard_snk_001 | upstream UC quota |
| TC-SNK-002 | Fail | 1086585803503405 | onboard_snk_002 | upstream UC quota |
| TC-SNK-003 | Fail | 606228420668696 | onboard_snk_003 | upstream UC quota |
| TC-TRF-001 | Fail | 837968418372012 | seed_trf_001_4way_join_data | UC quota (volume limit) |
| TC-TRF-002 | Fail | 768232458543513 | run_pipeline | upstream failure |
| TC-TRF-003 | Fail | 198694273772739 | run_pipeline_task | upstream failure |
| SCN-004 | Fail | 828517087551109 | onboard_003 | upstream failure |

### `dev_flowx` re-run (fresh workspace, full quota headroom)

The `dev` workspace's quota ceiling made most of the run above uninformative, so the suite was
re-run on the `dev_flowx` target (`dbc-2f6b7d4f-8c5b.cloud.databricks.com`, profile
`<redacted-profile>`, catalog `flowx`), whose catalog held only 2 schemas
— i.e. full UC headroom. Deployed with `bundle deploy --target dev_flowx`; bulk onboarding
again **43/43 SUCCESS** (run `437346670852451`).

Results confirm the quota diagnosis: cases that failed purely on quota in `dev` pass here.

| TC | dev | dev_flowx | Run ID |
|---|---|---|---|
| TC-ING-007 | Fail (UC quota) | **Pass** | 610806122791505 |
| TC-ING-008 | Pass | **Pass** | 143876885778793 |
| TC-ING-009 | Pass | **Pass** | 193003582337840 |
| TC-ING-001 | Pass | **Pass** | 366629333575250 |
| TC-ING-002 | Pass | **Pass** | 1041284490734646 |
| TC-ING-006 | Fail (serverless quota) | **Pass** (after spec fix, below) | 997043878606671 |

#### TC-ING-006 — a genuine spec defect found and fixed

On the fresh workspace TC-ING-006 failed with a real framework error rather than a quota one:

```
FrameworkConfigError: explode_columns names 'metrics', which is type string
-- only struct or array columns can be exploded/flattened
```

Root cause: Auto Loader's `cloudFiles.inferColumnTypes` defaults to **false**, so a JSON
source's nested `metrics` array arrives as a `STRING`, not `array<struct>`. The spec declared
`explode_columns: ["metrics"]` with nothing to turn that string into a struct first. This is
precisely the case v1.3.0's **E03 JSON-string parity** feature exists for.

Fixed in `flowx_testing/015_ing_006_json_explode.json` by declaring the new field:

```json
"json_string_columns": [
  {"column": "metrics", "schema_ddl": "array<struct<sensor:string,val:double>>"}
]
```

**Verified live end-to-end** — 2 source records, each with a 2-element `metrics` array:

| device_id | device_type | event_ts | metrics_sensor | metrics_val |
|---|---|---|---|---|
| D1 | env_sensor | 2026-08-28T09:00:00Z | temp | 22.5 |
| D1 | env_sensor | 2026-08-28T09:00:00Z | pressure | 101.3 |
| D2 | env_sensor | 2026-08-28T09:05:00Z | temp | 24.1 |
| D2 | env_sensor | 2026-08-28T09:05:00Z | pressure | 99.8 |

4 rows from 2 records, `metrics` parsed STRING → `array<struct>` → exploded → flattened to
`metrics_sensor` / `metrics_val` with the documented `<column>_<subfield>` naming. This gives
E03's JSON-string parity path real live coverage, which it did not previously have.

### To finish this backlog

Free Unity Catalog quota in the `flowx` catalog (currently 51/50 schemas and 51+/50
volumes) by dropping superseded test schemas and volumes, then re-run the failed waves. The
runner and the per-wave result files are reusable as-is. Nothing in the framework needs to
change for these to run — with the single exception of `TC-ING-004`, which needs the
pre-existing `binaryFile` / `fileNamePattern` incompatibility fixed first.

---

## 1. Real, Executable Scenarios

| Scenario | Resource(s) | Spec | Doc | Status | Notes |
|---|---|---|---|---|---|
| 001 — ZIP join + export | `flowx_test_001_zip_join_export_pipeline` / `_job` | `flowx_testing/001_zip_file_onboarding.json` | — (pre-existing) | Deployed | Pre-existing scenario; not re-run in this pass. |
| 002+003 — Zerobus + Autoload recon | `flowx_test_002_zerobus_pipeline`, `flowx_test_003_autoload_recon_pipeline` / `flowx_test_002_003_job` | `002_zerobus_data_load.json`, `003_autoload_recon_append.json` | — (pre-existing) | Deployed; **known fragility found 2026-08-29**; **spec changed 2026-08-30 (v1.5.0, not yet re-deployed/re-run)** | Re-running the full `002_003`/`004` chain repeatedly against this same long-lived dev workspace can break scenario 002's Zerobus streaming read with `DELTA_MERGE_UNRESOLVED_EXPRESSION`/`DELTA_SOURCE_TABLE_IGNORE_CHANGES` — the seed script's idempotent re-merge into `zerobus_source_bus` and scenario 003's own self-healing appends both count as Delta "data updates" that a plain streaming reader can't tolerate mid-checkpoint. Worked around this pass via `databricks pipelines start-update --full-refresh` on `flowx_test_002_zerobus_pipeline`; not a code bug, but a real operational gotcha worth knowing before re-running this chain — see `docs/archive/legacy_docs/30_test_pipeline_recon_features.md`'s "Real live-run results" section. **2026-08-30**: `003`'s reconciliation flow now sets `execution_mode: "pipeline"` and an explicit `dataflow_group_id` (`TC-R2-001` — see `TESTING_PLAN.md` Module 12); offline-verified against `_validate_reconciliation_pipeline_placement` and the full JSON schema (0 placement errors, 0 schema errors). **2026-08-31 — now VERIFIED LIVE**: run through the dedicated `flowx_test_recon_dag_job` (job id `854232399214818`), pipeline `be78d88d-6064-414d-a10c-2aacd900fa86`, all four tasks SUCCESS, with ingestion + L3/L4/L5 reconciliation nodes registered in one update — see §0b for the full evidence table. Note this ran through the *new* job, not `flowx_test_002_003_job`; the `flowx_test_002_003_job` result recorded above still reflects the pre-flip, job-mode behaviour, and its standalone `run_003_reconciliation` task has since been deleted so the flow cannot reconcile twice. |
| **049/050/051 — Unified pipeline-mode reconciliation / read-once source plane (new specs, v1.5.0)** | `resources/flowx_test_dag_001_unified_{pipeline,job}.yml` and `flowx_test_104_geneva_tariffs_recon_{pipeline,job}.yml` **now exist** (they were "a later build step" when this row was first written); `050` has none by design | `049_dag_001_unified_three_flow.json`, `050_dag_002_recon_cycle_negative.json`, `051_geneva_tariffs_recon.json` | `TESTING_PLAN.md` Module 12 + §3.12 | **Not Started** (resources built, never deployed or run; `049` and `051` are blocked — see `TESTING_PLAN.md` §3.12.4 / §3.12.5) | Blockers, restated so "resources exist" is not read as "runnable": **`049`** has no seed notebook for `flowx.dag_001_usecase.shared_source_bus` (its job wires `02_seed_flowx_testing_data.py`, which seeds only the `EA_usecase`/`Excalibur_usecase` families), and no `observability` destinations array, so `run_observability_task` fails loudly. **`051`** is blocked on grants: the `flowx` CLI identity holds neither `USE SCHEMA` on `flowx.geneva_admin` nor `SELECT` on the far side `flowx.bronze_excalibur.bronze_tariffelementband`, while live pipeline `e41a47ba` runs as another principal; its far-side producer `dfg_zerobus_tariffelementband_cdc` is outside this repo and still carries the v1.4.0-removed `normalize_column_names: true`. **`050`** is the one Module 12 fixture fully executable today (offline validator only). All three offline-verified this pass: JSON-schema-valid (`jsonschema.Draft202012Validator`, 0 errors each) and checked directly against `onboarding/spec_validator.py::_validate_reconciliation_pipeline_placement` after `{{catalog}}`→`flowx` substitution (the same substitution `onboarding/spec_loader.py::substitute_environment_placeholders` performs before `validate_spec()` ever sees the spec). `049`: 0 placement errors (the only `validate_spec()` finding at all is the expected `spark=None` `EXPLAIN` artifact on its `transformation_sql`, identical in kind to every other transformation-flow fixture in this repo). `050`: exactly 1 error, the intended V-CYC-3 rejection naming `ingestion_flow[df_dag_002_tariffs_zerobus_ingest]`. `051`: 0 errors (deliberately — see `TESTING_PLAN.md`'s note on the cross-spec blind spot). None have been onboarded against a live workspace or run. |
| **052 — Audit-only pipeline-mode reconciliation (new spec, 2026-08-31)** | **None** — no `resources/flowx_test_dag_003_audit_only_{pipeline,job}.yml` exists; the names used in `TESTING_PLAN.md` §3.12.2 are proposed, not built | `052_dag_003_recon_audit_only.json` | `TESTING_PLAN.md` `TC-DAG-003` + §3.12.2 | **Not Started** (spec-only) | One of TWO fixtures setting `execution_mode: "pipeline_audit_only"` — the other is `053` (the live geneva pipeline `e41a47ba`, forced into audit-only because its `TRUNCATE_AND_LOAD` source cannot be streamed; see `TC-DAG-004`). `049`/`003` use plain `"pipeline"`, `051` is a synthetic mirror superseded by `053`, `050` is the negative case. `052` remains the only *purpose-built* audit-only fixture. Before it, the audit-only branch of `reconciliation/graph_registration.py` had no fixture coverage at all (unit-only: `tests/unit/test_flow_generators.py`, `tests/unit/test_read_once_wiring.py`). Offline-verified: `validate_spec(None, spec)` returns only the expected `spark=None` `EXPLAIN` artifact on `tf_dag_003_audit_source_direct_view.transformation_sql` — 0 placement errors. **Not runnable**: no resources, no seed for `dag_003_usecase.audit_source_bus` / `dag_003_downstream.audit_target_copy` / `dag_003_remediation.audit_heal_landing`, and 5 new schemas needed against the 50-schema metastore ceiling. Do not report audit-only as covered on the strength of this fixture existing. |
| **053 — Geneva `e41a47ba` recon-in-pipeline variant (new spec, 2026-08-31)** | Shares `flowx_test_104_geneva_tariffs_recon_{pipeline,job}.yml` with `051` | `053_geneva_e41a47ba_recon_in_pipeline.json` (`dfg_geneva_tariffs_recon`, reconciliation flow `rec_tariff_element_band`) | `TESTING_PLAN.md` §3.12.5 | **Not Started** | Second capture of the same live geneva group under a different `reconciliation_id`. Blocked by exactly the same grants/seed/cross-repo issues as `051` — see §3.12.5. |
| 100 — ZIP+CSV, SCD1, quarantine, OTel | `flowx_test_100_zipcsv_pipeline` / `_job` | `100_zipcsv_onbaording.json` | — (pre-existing) | **Executed — Pass** | Re-run 2026-08-29 as a regression check on this session's changes (run `322114205956865`, all 3 tasks SUCCESS, ~4 min). Data sanity: `customer_raw` 3 rows, `customer_plain_raw` 5 rows, `customer_raw_quarantine` 0 rows. No regression from `ingestion/json_flattening.py`, `ingestion/column_normalization.py`, CDC dispatch, or ZIP-extraction changes. |
| **004 — Reconciliation feature test (new)** | `flowx_test_004_recon_features_job` (no dedicated pipeline — reconciliation is a notebook task, not a Lakeflow flow type) | `004_recon_features_test.json` | [`docs/07_reconciliation_engine.md`](../docs/07_reconciliation_engine.md) | **Executed — Pass (2/3 flows), needs a re-run (1/3)** | `recon_004_logging_on`/`recon_004_logging_off`: **Executed — Pass**, fully verified live. `recon_004_streaming` (task `run_004_recon_streaming`): the streaming-side flow. Its last live run predates v1.4.0, when this flow was `recon_004_continuous` and ran an unbounded trigger that serverless job compute rejects outright (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`); the error handling and logging around it were verified correct at the time. Reconciliation is now triggered-only — a `read_mode: "streaming"` side runs under `trigger(availableNow=True)`, drains its backlog and stops — so the platform constraint no longer applies and this flow is expected to pass. **Re-run needed to confirm live.** |
| **Observability OTel streaming pipeline (new, Design 1)** | `observability_otel_streaming_pipeline` (`continuous: true`, no job) | — (not onboarding-spec-driven) | [`docs/archive/legacy_docs/29_test_pipeline_otel_streaming.md`](../docs/archive/legacy_docs/29_test_pipeline_otel_streaming.md) | Deployed, **not started live** | Deliberately not started as part of this pass — it is a `continuous: true` pipeline that would keep running (and consuming compute) indefinitely once started; starting it needs your explicit go-ahead, not a bundled "run the tests" pass. Verified structurally only (see that doc's Expected Results section). |

## Build Pass Update (2026-08-29)

40 of the 45 TC-* test cases from Section 2 below now have built artifacts (spec/resource/doc)
as of 2026-08-29 -- see each row's note for the exact job resource name. 5 were deliberately not
built this pass: `TC-REC-001`/`TC-OBS-002` are already meaningfully covered by pre-existing
scenarios (003's own reconciliation flow; the existing batch observability engine), and
`TC-FLT-001`/`TC-PRM-002`/`TC-PRM-003` test capabilities/scenarios of lower incremental value
right now (crash-simulation, and SQL/reconciliation parameterization that predates this session
and is already exercised elsewhere).

**Validated 2026-08-29**: all 39 new onboarding specs pass `validate_spec()` -- 31 offline
(`spark=None`), 7 more confirmed live against real Databricks Connect (their `transformation_sql`
needed a real Spark session to syntax-check; all 7 resolve to the expected "table not found yet,
tolerated pre-deployment" outcome, not a real error), and 1 (`TC-PRM-006`, the deliberate negative
test) correctly *fails* validation with exactly its intended error message. `databricks bundle
validate` and `databricks bundle deploy --profile dev` both succeeded cleanly (75 resources
created, 11 changed, 0 errors) -- every new pipeline/job resource definition now exists in the
`dev` workspace.

**Live execution intentionally deferred, per explicit direction**: none of these 40 test jobs
have been *run* yet, and won't be in this workspace -- live testing of this backlog is planned
for a separate/new workspace, not this session's `dev` target. Everything here is built,
validated, and deploy-ready; treat "Built" status below as "ready to run whenever/wherever you
choose to execute it," not "still needs work before it can run."

**Known active platform issue affecting live execution when it does happen**: this workspace's
serverless compute is currently unable to start `mapInPandas` Python-worker sandbox containers
(`ISOLATION_STARTUP_FAILURE.SANDBOX_STARTUP` / "exec format error" loading
`/databricks/python3/bin/python`) -- confirmed live 2026-08-29 with a trivial, dependency-free
repro, unrelated to any of this session's code. This will block `TC-ING-004` (ASN.1, uses
`mapInPandas` for decoding) and likely affects PGP/ZIP-heavy paths too
(`archive/zip_utils.py`'s executor-distribution added this session also uses `mapInPandas`)
until the sandbox issue clears (retry later, or escalate to Databricks support if it persists) --
worth checking whether this affects the new workspace too before assuming a test failure there is
a real bug.

### Known gaps/judgment calls from the build pass

- **TC-PRM-004** (Parameterized DQ Rule Expressions): flagged a real gap in `${param}`-substitution coverage for `dq_config` — the path-parameter feature this session's `substitute_path_parameters` work covers doesn't fully extend to DQ rule expressions yet, so this build only exercises what substitution exists today, not full parameterized-DQ coverage.
- **TC-SEC-002** (Hash Key & Value Determinism): the build agent had to make a judgment call on whether to route this test through the reconciliation path or the ingestion path to exercise `__framework_hash_key`/`__framework_hash_value` generation; picked one — worth confirming it's the intended coverage point before treating this as authoritative.
- **TC-SEC-003** (Secret Masking & Redaction): built against the still-open `crypto/column_crypto.py` plaintext-key-in-plan issue (see Section 2's TC-SEC-003 row and memory) — this is a known-failing/known-issue build, not expected to pass live until that underlying issue is resolved.
- **TC-GOV-002** (Tag Application Idempotency): has no dedicated spec or pipeline of its own; it reuses TC-GOV-001's job/pipeline (a second run of the same tagging job) to assert idempotency rather than a net-new build.
- **TC-ING-003, TC-OBS-003, TC-GOV-001, TC-GOV-002**: all reuse pre-existing pipelines rather than a new dedicated pipeline being built — only a new job (and, for ING-003/OBS-003, a new spec) was added pointing at existing pipeline infrastructure (Scenario 002's Zerobus pipeline, the pre-existing batch observability engine, etc.). Keep this in mind when interpreting these four as "Built": the job/spec layer is dedicated, the pipeline layer is not.

## 2. `TC-*` Coverage Map (against `TESTING_PLAN.md`)

### Module 1: Ingestion Engine & Source Adapters
| TC | Status | Note |
|---|---|---|
| TC-ING-001 Selective Glob ZIP Extraction | Built | Dedicated build: `flowx_test_ing_001_zip_filter_job` / `_pipeline`; spec `flowx_testing/010_ing_001_zip_filter.json`. Not yet run live. |
| TC-ING-002 ZIP Source Purge Retention | Built | `flowx_test_ing_002_zip_retention_job` / `_pipeline`; spec `flowx_testing/011_ing_002_zip_retention.json`. Not yet run live. |
| TC-ING-003 Zerobus Streaming Ingestion | Built | `flowx_test_ing_003_zerobus_append_job`; spec `flowx_testing/012_ing_003_zerobus_append.json`. Reuses an existing pipeline (no dedicated pipeline built — see Known gaps above). Not yet run live. |
| TC-ING-004 ASN.1 Binary CDR Decoding | Built | `flowx_test_ing_004_asn1_decode_job` / `_pipeline`; spec `flowx_testing/013_ing_004_asn1_decode.json`. Not yet run live. |
| TC-ING-005 Landing PGP Decryption | Built | `flowx_test_ing_005_pgp_decrypt_job` / `_pipeline`; spec `flowx_testing/014_ing_005_pgp_decrypt.json`. Not yet run live. |
| TC-ING-006 Nested JSON Flattening | Built | `flowx_test_ing_006_json_explode_job` / `_pipeline`; spec `flowx_testing/015_ing_006_json_explode.json`. Not yet run live. |
| TC-ING-007 Inline Data Standardization SQL | Built | `flowx_test_ing_007_standardize_job` / `_pipeline`; spec `flowx_testing/016_ing_007_standardize.json`. Not yet run live. |
| TC-ING-008 Technical Metadata Enrichment | Built | `flowx_test_ing_008_tech_metadata_job` / `_pipeline`; spec `flowx_testing/017_ing_008_tech_metadata.json`. Not yet run live. |
| TC-ING-009 Schema Evolution Rescue | Built | `flowx_test_ing_009_rescue_schema_job` / `_pipeline`; spec `flowx_testing/018_ing_009_rescue_schema.json`. Not yet run live. |

### Module 2: CDC Strategies
| TC | Status | Note |
|---|---|---|
| TC-CDC-001 APPEND | Built | `flowx_test_cdc_001_append_job` / `_pipeline`; spec `flowx_testing/019_cdc_001_append.json`. Not yet run live. |
| TC-CDC-002 TRUNCATE_AND_LOAD | Built | `flowx_test_cdc_002_truncate_job` / `_pipeline`; spec `flowx_testing/020_cdc_002_truncate.json`. Not yet run live. |
| TC-CDC-003 SCD1 | Built | Dedicated `flowx_test_cdc_003_scd1_job` / `_pipeline` (previously only partially covered by Scenario 100); spec `flowx_testing/021_cdc_003_scd1.json`. Not yet run live. |
| TC-CDC-004 SCD2 | Built | `flowx_test_cdc_004_scd2_job` / `_pipeline`; spec `flowx_testing/022_cdc_004_scd2.json`. Not yet run live. |
| TC-CDC-005 SCD3 | Built | `flowx_test_cdc_005_scd3_job` / `_pipeline`; spec `flowx_testing/023_cdc_005_scd3.json`. Not yet run live. |
| TC-CDC-006 FULL_SNAPSHOT_CDC | Built | `flowx_test_cdc_006_snapshot_pk_job` / `_pipeline`; spec `flowx_testing/024_cdc_006_snapshot_pk.json`. Not yet run live. |
| TC-CDC-007 FULL_SNAPSHOT_CDC (legacy extract) | Built | `flowx_test_cdc_007_snapshot_nopk_job` / `_pipeline`; spec `flowx_testing/025_cdc_007_snapshot_nopk.json`. Not yet run live. (v1.4.0: this case was `FULL_SNAPSHOT_CDC_NO_PK` and is now `FULL_SNAPSHOT_CDC` keyed on `primary_keys: ["customer_name"]`; the `_nopk` in the job/pipeline/spec names is kept deliberately so the artefacts still line up.) |

### Module 3: Transformations
| TC | Status | Note |
|---|---|---|
| TC-TRF-001 4-Way Streaming/Batch Join | Built | Dedicated `flowx_test_trf_001_4way_join_job` / `_pipeline` (previously only partially covered by Scenario 001); spec `flowx_testing/026_trf_001_4way_join.json`. Not yet run live. |
| TC-TRF-002 UNION ALL Consolidation | Built | `flowx_test_trf_002_union_all_job` / `_pipeline`; spec `flowx_testing/027_trf_002_union_all.json`. Not yet run live. |
| TC-TRF-003 In-DAG Column Decryption | Built | `flowx_test_trf_003_decrypt_transform_job` / `_pipeline`; spec `flowx_testing/028_trf_003_decrypt_transform.json`. Not yet run live. |

### Module 4: Data Quality
| TC | Status | Note |
|---|---|---|
| TC-DQ-001 `warn` | Built | `flowx_test_dq_001_warn_job` / `_pipeline`; spec `flowx_testing/029_dq_001_warn.json`. Not yet run live. |
| TC-DQ-002 `drop` | Built | `flowx_test_dq_002_drop_job` / `_pipeline`; spec `flowx_testing/030_dq_002_drop.json`. Not yet run live. |
| TC-DQ-003 `fail` | Built | `flowx_test_dq_003_fail_job` / `_pipeline`; spec `flowx_testing/031_dq_003_fail.json`. Not yet run live. |
| TC-DQ-004 `quarantine` | Built | Dedicated `flowx_test_dq_004_quarantine_job` / `_pipeline` (previously only partially covered by Scenario 100); spec `flowx_testing/032_dq_004_quarantine.json`. Not yet run live. |

### Module 5: Cryptography & Secrets
| TC | Status | Note |
|---|---|---|
| TC-SEC-001 AES-GCM Column Encryption | Built | `flowx_test_sec_001_aes_encrypt_job` / `_pipeline`; spec `flowx_testing/033_sec_001_aes_encrypt.json`. Not yet run live. |
| TC-SEC-002 Hash Key & Value Determinism | Built | `flowx_test_sec_002_hashing_job` / `_pipeline`; spec `flowx_testing/034_sec_002_hashing.json`. Note: `__framework_hash_key`/`__framework_hash_value` generation is confirmed already implemented in `cdc/hashing.py` (see architecture-review remediation pass). Build agent made a judgment call routing this test via reconciliation vs. ingestion — see Known gaps above. Not yet run live. |
| TC-SEC-003 Secret Masking & Redaction | Built | `flowx_test_sec_003_redaction_job` / `_pipeline`; spec `flowx_testing/035_sec_003_redaction.json`. Built against the still-open `crypto/column_crypto.py` plaintext-key-in-plan issue (see memory) — known-failing until that's resolved; see Known gaps above. Not yet run live. |

### Module 6: Governance & Tagging
| TC | Status | Note |
|---|---|---|
| TC-GOV-001 UC Table/Column Tag Application | Built | `flowx_test_gov_001_tagging_job`; spec `flowx_testing/036_gov_001_tagging.json`. Reuses an existing pipeline (no dedicated pipeline built — see Known gaps above). Not yet run live. |
| TC-GOV-002 Tag Application Idempotency | Built | `flowx_test_gov_002_idempotent_tag_job`; no dedicated spec/pipeline — reuses TC-GOV-001's job/pipeline for a second run to assert idempotency (see Known gaps above). Not yet run live. |

### Module 7: Cross-Dataset Reconciliation & Self-Healing

> All three rows below are **job-mode** and form part of the v1.5.0 no-impact regression set —
> see §4 and `TESTING_PLAN.md` §3.12.6. `TC-REC-001`'s flow is the one `TC-R2-001`/S1 flipped to
> `execution_mode: "pipeline"`; `TC-REC-002`/`TC-REC-003` must keep their existing behaviour with
> no new or relaxed assertions.

| TC | Status | Note |
|---|---|---|
| TC-REC-001 Missing-Record Self-Healing Backfill | Partially Covered | Scenario 003's own reconciliation flow (`recon_excalibur_autoload_vs_zerobus`) already does exactly this. |
| TC-REC-002 Value Drift Detection | Built | Dedicated `flowx_test_rec_002_drift_job` / `_pipeline` (previously only partially covered by Scenario 004); spec `flowx_testing/037_rec_002_drift.json`. Not yet run live. |
| TC-REC-003 Pre-Computed Hash Matcher Perf | Built | `flowx_test_rec_003_precomputed_hash_job` / `_pipeline`; spec `flowx_testing/038_rec_003_precomputed_hash.json`. Not yet run live. |

### Module 8: External Egress & Sinks
| TC | Status | Note |
|---|---|---|
| TC-SNK-001 External Sink Dual Materialization | Built | Dedicated `flowx_test_snk_001_external_sink_job` / `_pipeline` (previously only partially covered by Scenario 001); spec `flowx_testing/039_snk_001_external_sink.json`. Not yet run live. |
| TC-SNK-002 Pure Sink (no table) | Built | `flowx_test_snk_002_pure_sink_job` / `_pipeline`; spec `flowx_testing/040_snk_002_pure_sink.json`. Not yet run live. |
| TC-SNK-003 PGP-Encrypted ZIP Sink | Built | `flowx_test_snk_003_pgp_zip_sink_job` / `_pipeline`; spec `flowx_testing/041_snk_003_pgp_zip_sink.json`. Not yet run live. |

### Module 9: Observability & Telemetry
| TC | Status | Note |
|---|---|---|
| TC-OBS-001 Structured JSON Logging | Built | Dedicated `flowx_test_obs_001_json_log_job` / `_pipeline` (previously only partially covered as a side effect of every scenario); spec `flowx_testing/042_obs_001_json_log.json`. Not yet run live. |
| TC-OBS-002 DLT Event Log → OTel Formatting | Partially Covered | Covered by the pre-existing *batch* observability engine (`08_dlt_observability_engine.py`), not this pass's new *continuous* streaming design (see the Observability OTel streaming pipeline row above, which is a materially different, additive capability). |
| TC-OBS-003 OTel Export to UC Volume `.jsonl.gz` | Built | `flowx_test_obs_003_vol_export_job`; spec `flowx_testing/043_obs_003_vol_export.json`. Reuses the pre-existing batch observability engine's pipeline (`DATABRICKS_VOLUME` destination type) rather than a dedicated pipeline — see Known gaps above. Not yet run live. |

### Module 10: Fault Tolerance & Recovery
| TC | Status | Note |
|---|---|---|
| TC-FLT-001 Mid-Stream Crash Recovery | Not Started | |
| TC-FLT-002 Day-1→Day-2 Incremental Cycle | Built | Dedicated `flowx_test_flt_002_lifecycle_job` / `_pipeline` (previously only conceptually covered by Scenario 100's SCD1 day-1/day-2 pattern); spec `flowx_testing/044_flt_002_lifecycle.json`. Not yet run live. |

### Module 11: Dynamic Parameterization & Templating
| TC | Status | Note |
|---|---|---|
| TC-PRM-001 Parameterized Dynamic Volume/File Paths | Built | `flowx_test_prm_001_path_param_job` / `_pipeline`; spec `flowx_testing/045_prm_001_path_param.json`. Directly exercises this session's new `${param}`-in-path-fields feature (`substitute_path_parameters`). Not yet run live. |
| TC-PRM-002 Parameterized Transformation SQL | Not Started | Underlying `${param}` SQL substitution already existed pre-session; only the dedicated test is missing. |
| TC-PRM-003 Parameterized Reconciliation `filter_condition`/`transform_sql` | Not Started | Underlying substitution already existed; scenario 004 (this pass) tests *different* new reconciliation capabilities (logging/mode), not this one. |
| TC-PRM-004 Parameterized DQ Rule Expressions | Built | `flowx_test_prm_004_dq_param_job` / `_pipeline`; spec `flowx_testing/046_prm_004_dq_param.json`. Build agent flagged a real gap in `${param}` substitution coverage for `dq_config` — see Known gaps above. Not yet run live. |
| TC-PRM-005 Parameterized Egress Sink Paths | Built | `flowx_test_prm_005_sink_param_job` / `_pipeline`; spec `flowx_testing/047_prm_005_sink_param.json`. Directly exercises the new path-parameter feature — `sink_config.path` is one of the fields `substitute_path_parameters` now covers. Not yet run live. |
| TC-PRM-006 Negative: Undefined Parameter Error | Built | `flowx_test_prm_006_negative_param_job`; spec `flowx_testing/048_prm_006_negative_param.json`. No dedicated pipeline (negative/error-path test). Not yet run live. |

### Module 12: Unified Pipeline-Mode Reconciliation & Read-Once Source Plane (v1.5.0)

> Scenario-level detail (pre-conditions, run commands, observable pass criteria, named blockers)
> now lives in [`TESTING_PLAN.md`](TESTING_PLAN.md) §3.12, which maps these `TC-*` rows onto four
> operator scenarios: **S1** full pipeline mode incl. the L5 heal lane (`TC-R2-001`), **S2**
> audit-only (`TC-DAG-003`), **S3** negative cycle rejection (`TC-DAG-002`), **S4** read-once
> source plane (`TC-DAG-001`), **S5** the live geneva pipeline `e41a47ba` moved in-DAG
> (`TC-DAG-004`). Of the five, **S1 has now passed live** (2026-08-31, job `854232399214818`,
> pipeline `be78d88d-6064-414d-a10c-2aacd900fa86` — see §0b, which supersedes this doc's earlier
> "only S3 is fully executable today"), and **S3** remains fully executable offline — it needs no
> workspace at all. **S2 and S4 are still not-live** (no resources/seed for S2; no seed for
> `dag_001_usecase.shared_source_bus` for S4 — blocker B3). S5's offline half (validator +
> `plan_source_plane` assertions, pinned by `tests/unit/test_geneva_e41a47ba_topology.py`) passes
> today; its live half is still blocked — the control-table column migration now exists and is
> applied by running `setup_control_tables`, but pipeline `e41a47ba` is orphaned by every
> `dev_flowx` deploy (blocker B1) and `flowx@nrmanalytix.com` still lacks `SELECT` on the
> far-side bronze table (blocker B2). See §0b and `TESTING_PLAN.md` §3.12.5.

| TC | Status | Note |
|---|---|---|
| TC-DAG-001 Read-Once Source Plane Across Three Flow Kinds | Not Started | Spec `flowx_testing/049_dag_001_unified_three_flow.json`; resources `resources/flowx_test_dag_001_unified_{pipeline,job}.yml` **now exist** (no longer "a later build step") and `tests/integration/test_flowx_dag_001_unified_three_flow.py` already encodes every S4 assertion. Offline-verified: 0 JSON-schema errors, 0 `_validate_reconciliation_pipeline_placement` errors (see Section 1). **Not deployed, not run, and not runnable** (**blocker B3**): nothing seeds `flowx.dag_001_usecase.shared_source_bus` — see `TESTING_PLAN.md` §3.12.4 Blockers. S1 passing live (§0b) does **not** cover this: `003` has a single reconciliation source and so proves R1, not R2 across three flow kinds. Note the pytest module *skips* rather than fails when the pipeline is absent, so a green run against an un-deployed bundle is not evidence. Also: `049` is deliberately heal-less (`target_to_source`, no `append_target_table`), so it proves R1/R2 but **cannot** prove the L5 lane. |
| TC-DAG-002 Negative: Recon Append Into Own Ingestion Source (V-CYC-3) | **Executed — Pass (offline validator)** | Spec-only: `flowx_testing/050_dag_002_recon_cycle_negative.json`. Deliberately never deployed (negative/error-path test, same pattern as `TC-PRM-006`). **Re-confirmed 2026-08-31** by running `validate_spec(None, spec)` with `{{catalog}}`→`flowx`: the error list has length **exactly 1** and is the intended V-CYC-3 rejection naming `recon_dag_002_cycle_negative` and `df_dag_002_tariffs_zerobus_ingest`. The verbatim message is quoted in `TESTING_PLAN.md` §3.12.3 criterion 2. Note `validate_spec()` returns a 5-tuple — the errors are the **last** element; the earlier elements hold collected rows and are populated even for a rejected spec. Still un-run against a live onboarding job; per §0 runner requirement 3 its job-level verdict must be inverted when it is. |
| TC-DAG-003 Audit-Only: Compare In-DAG, Heal In-Job (`pipeline_audit_only`) | Not Started | Spec-only: `flowx_testing/052_dag_003_recon_audit_only.json`; no pipeline/job resource and no seed notebook exist. One of two fixtures covering the `pipeline_audit_only` branch (with `053`), and the only purpose-built one. The test is chiefly a *negative* one — zero `sink_definition` for `_recon__..._heal_sink`, zero `_recon__..._pulse` dataset, 0 rows appended to `dag_003_remediation.audit_heal_landing` — while L3/L4 and the `dq_config` expectation on `__metrics` must still be present and executed. The internal `__missing` dataset **is** still expected (gated on `_wants_heal` alone, not on `execution_mode`). See `TESTING_PLAN.md` §3.12.2. |
| TC-DAG-004 The Live Geneva Pipeline `e41a47ba` Moved In-DAG (`pipeline_audit_only`) | Partly Verified (offline only) | Spec: `flowx_testing/053_geneva_e41a47ba_recon_in_pipeline.json`, whose `dataflow_id` and `reconciliation_id` are transcribed from the LIVE `flowx.config.*` rows so re-onboarding MERGEs in place instead of adding duplicate flows — unlike `051`, which declares the same group but different flows. **Green today**: `validate_spec()` returns 0 errors; `plan_source_plane()` rejects `execution_mode: "pipeline"` (the source, `geneva_admin.stg_tariffelementband`, is this group's own `TRUNCATE_AND_LOAD` ingestion target, and Delta refuses to stream from a full recompute) and accepts `pipeline_audit_only` with the source bound `in_graph_sibling` — R2 read-once on a real production topology. All pinned by `tests/unit/test_geneva_e41a47ba_topology.py` (6 tests). **Blocked from a live run** by three environmental issues, none a code defect: (1) `flowx.config.reconciliation_flow_spec` is missing `execution_mode`/`publish_schema`/`dq_config_json` until the additive migration in `schema_provisioner.ensure_control_table_columns` is applied by re-running `setup_control_tables` — `bundle deploy` alone does NOT apply it (this half is now *solved*, and was proved by S1's own `setup_control_tables` task in §0b — it just has to be run on whatever workspace hosts `e41a47ba`); (2) **blocker B1** — `e41a47ba` is a `[dev arjun]` pipeline reading its wheel from `flowx@nrmanalytix.com`'s artifact path, so every `dev_flowx` deploy builds a new uniquely-named wheel, prunes the old one and updates only the pipelines that target owns, orphaning this one with `ENVIRONMENT_PIP_INSTALL_ERROR`; no `dev_flowx` deploy repairs it, and it needs its own `artifact_path` or to be brought under the bundle (a pre-existing bundle-topology problem, **not** caused by v1.5.0); (3) **blocker B2** — the `flowx@nrmanalytix.com` identity lacks `SELECT` on `flowx.bronze_excalibur.bronze_tariffelementband` (table-level gap only; schema-level access is fine). Do not record this as a passing live scenario on the strength of the offline assertions — it is verified **offline only**. See §0b and `TESTING_PLAN.md` §3.12.5. |
| TC-R2-001 Existing Scenario Regression Under `execution_mode: "pipeline"` | **Executed — Pass (live, 2026-08-31)** | Run via the dedicated `flowx_test_recon_dag_job` (job id **`854232399214818`**) on `dev_flowx`; pipeline **`be78d88d-6064-414d-a10c-2aacd900fa86`**. All four tasks SUCCESS (`setup_control_tables`, `seed_flowx_testing_data`, `onboard_003`, `run_003_pipeline`). The pipeline's own event log shows ingestion (`bronze_excalibur.autoload_bronze`), the L3 prepare pair, the three L4 `classified`/`__metrics`/`__mismatch` MVs, the L5 `__pulse` streaming table, the `__heal_flow` APPEND flow and the `_recon__…__heal_sink` `foreachBatch` sink all registered in **one** update — full evidence table in §0b. This also settles the open question about `dlt.foreach_batch_sink` on DBR serverless: it exists, and the L5 heal lane is real rather than theoretical. Spec: `flowx_testing/003_autoload_recon_append.json` (`execution_mode: "pipeline"`). This is **S1** in `TESTING_PLAN.md` §3.12.1 — the only fixture that both heals and has a working seed notebook. Carried into the run and confirmed: (a) the `run_003_reconciliation` task was **deleted** from `resources/feature_tests/flowx_test_002_003_job.yml`, and its absence is part of the pass criteria (with `003` in pipeline mode it would have reconciled twice and risked a double append into `Excalibur_usecase.zerobus_source_bus`); (b) `TC-REC-001`/`TC-R2-001`'s prose says the missing keys are `C011`/`C012`, but the actual seeded fixtures make them **`C006`/`C007`** (`zerobus_source_bus_batch1.csv` = `C001`–`C005`, `autoload_batch1.csv` = `C001`–`C007`) — assert against `C006`/`C007`. |

---

## 3. Suggested Next Batch (if continuing this backlog)

Highest-value next picks, given what this session already built/verified:
1. ~~**TC-R2-001 (S1)**~~ — **DONE, passed live 2026-08-31** via `flowx_test_recon_dag_job` (job `854232399214818`), see §0b. Everything else in Module 12 is still blocked behind a seed notebook or a grant.
2. **A seed notebook for `flowx.dag_001_usecase.shared_source_bus`** (`notebooks/00_seed_sample_data/03_seed_dag_001_shared_source_bus.py`) — a few dozen rows of `record_id`/`value`, insert-only. This one file unblocks **S4** (blocker B3), the only real proof of R2, whose resources and integration tests are already written and waiting. This is now the **highest-value** remaining pick.
2b. **Unblock S5** — give pipeline `e41a47ba` its own `artifact_path` or bring it under the bundle (B1), and `GRANT SELECT ON TABLE flowx.bronze_excalibur.bronze_tariffelementband TO \`flowx@nrmanalytix.com\`` (B2). S5 is verified offline only until both land.
3. **TC-PRM-001 / TC-PRM-005** — directly exercise the new path-parameter substitution feature; no other prerequisite work needed.
4. **TC-SEC-003** — directly relevant to the still-open `crypto/column_crypto.py` secret-exposure investigation; picking this up would need to resolve that open question first (see memory), not just write a test around today's behavior.
5. Any `TC-CDC-*`/`TC-DQ-*` row — genuinely net-new dedicated scenarios, straightforward to build following the `002_003`/`004` job pattern.

---

## 4. Job-Mode Reconciliation Regression Set (must stay green through v1.5.0)

`execution_mode` is `NULL`/absent on every reconciliation row written before v1.5.0, and
`control_plane/repository.py::load_active_group_metadata` resolves that to `"job"` in **Python**
(`getattr(r, "execution_mode", None) or "job"`) rather than via a Spark `.filter()` — deliberately,
so a control table created by an older `01_setup` run still reads as job-mode instead of raising on
a missing column. `reconciliation/graph_registration.py::register_reconciliation_flow` independently
early-returns on a job-mode row. **An untouched flow therefore keeps its exact previous behaviour,
and nothing job-mode is ever registered into a pipeline graph.** Confirmed live on 2026-08-31 (§0b):
every pre-existing reconciliation row on `dev_flowx` still reads `execution_mode` NULL after the
S1 run, and no `_recon__%`/`recon__%` dataset was registered for any job-mode flow.

Note the missing-column case is now *also* repaired rather than merely tolerated: `ddl_definitions`
gained `ADDITIVE_CONTROL_TABLE_COLUMNS` / `get_add_column_ddl()` and `schema_provisioner` gained
`ensure_control_table_columns()`, called at the end of `ensure_control_schema_exists`. It is applied
**only** by running the `setup_control_tables` task — `bundle deploy` does not apply it — and it is
strictly additive (never a drop or a retype). The `getattr(..., None) or "job"` read stays as the
belt to that braces.

These are the real `TC-*` rows that must be re-run unchanged to hold that claim — full criteria in
`TESTING_PLAN.md` §3.12.6:

| TC / scenario | Resource | Status carried forward | Why it is in the regression set |
|---|---|---|---|
| `TC-REC-001` | Scenario `003`'s own flow | Partially Covered (see Module 7) | This is the flow S1 flipped to pipeline mode; its job-mode outcome is S1's before/after baseline. |
| `TC-REC-002` | `flowx_test_rec_002_drift_job` / `_pipeline` (`037_rec_002_drift.json`) | Built; last live attempt Fail (upstream failure, `dev`) | Job-mode `VALUE_DRIFT` detection must be byte-identical. |
| `TC-REC-003` | `flowx_test_rec_003_precomputed_hash_job` / `_pipeline` (`038_rec_003_precomputed_hash.json`) | Built; last live attempt Fail (upstream failure, `dev`) | `hash_precomputed` is now threaded into the new L3 nodes as well as the job path — a change to `matcher.prepare_dataset_for_matching` would break both at once. |
| `SCN-004` (`004_recon_features_test.json`) | `flowx_test_004_recon_features_job` | Executed — Pass (2/3 flows); `recon_004_streaming` still owes a re-run | Only coverage of `logging_config` precedence and of a `read_mode: "streaming"` side under `trigger(availableNow=True)`. All three flows are job-mode and must stay so. That re-run debt predates v1.5.0 — do not close it by assumption. |
| `TC-PRM-003` | — | Not Started | Listed for completeness: `${param}` substitution now runs on **both** paths (`substitute_path_parameters` is called inside `register_reconciliation_flow` too) and is tested on neither. |

Two cheap no-impact checks with no home anywhere yet:

- **A job-mode group registers nothing.** For a group whose reconciliation rows are all job-mode,
  its pipeline event log must contain **zero** `dataset_definition` events matching `_recon__%` or
  `recon__%`. Non-zero means the repository filter regressed.
- **`publish_schema` / `dq_config` must be rejected on presence under `execution_mode: "job"`**
  (`RECONCILIATION_FLOW_KEYS_REQUIRING_PIPELINE_MODE`, trigger is presence not truthiness — so
  `"dq_config": {"rules": []}` must still be rejected). `tests/unit/test_spec_validator.py`
  contains **no** assertion mentioning `publish_schema`, and no `flowx_testing/` fixture covers
  the combination. Currently unverified.
