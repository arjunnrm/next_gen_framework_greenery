# BUILD_CONTRACT.md — UC3 Excalibur → Databricks

**Phase A output. Binding on every Phase B subagent. Read this before writing anything.**

If you believe something here is wrong, **flag it back — do not silently deviate.** Drift between
parallel subagents is the single most expensive failure mode in this build.

Companion document: [`FRAMEWORK_CAPABILITY_MAP.md`](FRAMEWORK_CAPABILITY_MAP.md) — config-vs-code
per capability, plus the four documented deviations from the prompt's assumed schema. Read it too.

---

## 0. The three rules that override everything else

1. **Config, not code.** All eight capabilities are configuration (capability map §1). The only
   hand-written artefacts in this build are Job 1 (simulator) and the test-data generator.
2. **No spec is "done" until `validate_json` returns `valid: True`.** Unknown attributes are a
   hard error since v1.7.1. Command in capability map §6.
3. **Onboarding is delegated, never inlined.** Do **not** create an onboarding job or task.
   Call the existing generic job — see §7 below.

---

## 1. Names — catalog, schema, volume

| Thing | Value | Notes |
|---|---|---|
| Catalog | **`br_digital_poc`** (`${var.catalog}`) | Workspace prerequisite; cannot be declared in YAML. |
| Staging schema | **`br_digital_poc.staging`** | **EXISTING schema, reused.** Holds `<table>_stream`, `<table>_batch`, the simulator cursor and `recon_metrics`. |
| Governed schema | **`br_digital_poc.bronze`** | **EXISTING schema, reused.** Holds the three CDC-applied targets. |
| Landing volume | **`br_digital_poc.staging.uc_3`** | **EXISTING volume, reused.** Streaming + batch CSVs live under it. |
| App-logs volume | **`br_digital_poc.observability.app_logs`** | **EXISTING volume, reused.** §7 observability destination. |

> **REVISED 2026-09-05 by explicit user instruction — supersedes the earlier `uc3_staging` /
> `uc3_bronze` plan.** No UC3-specific schemas or volumes are created. Every object above already
> existed on `metaflow_v7`; UC3 reuses them and distinguishes itself by *table name* and by the
> `uc_3` volume, not by dedicated schemas.
>
> **Collision check performed before rewiring:** `br_digital_poc.bronze` held only UC7 CDR tables
> (`emsc_cdr_raw`, `psgw_cdr_raw`, `sgsn_cdr_raw`, `tap310_raw` + quarantines) and `br_digital_poc.staging`
> held no tables at all, so `physical_device` / `customer` / `subscriber` and the `_stream` /
> `_batch` names are free. Re-verify before onboarding any further table into these shared schemas.

**Volume layout (revised):**

```
/Volumes/br_digital_poc/observability/app_logs/<streaming_cdc|batch_recon>/

# HISTORICAL -- no lane reads these any more (see section 18)
/Volumes/br_digital_poc/staging/uc_3/streaming/<table>/<table>_stream.csv     # until v0.0.4
/Volumes/br_digital_poc/staging/uc_3/batch/<table>/batch_date=YYYY-MM-DD/<table>_batch.csv  # until v0.0.7
/Volumes/br_digital_poc/staging/uc_3/_schemas/<table>_batch/                  # Auto Loader, until v0.0.7
```

**Live source objects (v0.0.7):** the streaming lane reads
`br_digital_poc.staging.oracle_excalibur_cdc` (the multiplexed Debezium landing table) and the
batch lane reads `br_digital_poc.oracle_excalibur_batch.{customer,physical_device,subscriber}`
(written by the Lakeflow Connect Oracle query-based connector, not by this framework). Neither is a
path. The `uc_3` volume is now inert; only the `app_logs` paths are live.

**Cleanup done:** `br_digital_poc.uc3_staging` (4 tables + 2 volumes) dropped entirely. `br_digital_poc.uc3_bronze`
could not be dropped — it reports 12 tables while both `SHOW TABLES` and the tables API list zero,
i.e. orphaned pipeline-internal datasets from the failed Iceberg runs. It is **inert**: no spec,
pipeline or job references it any more. Left in place rather than force-cleaned; a later
`DROP SCHEMA ... CASCADE` by an operator will clear it.

Parameterised everywhere: `catalog` and `schema` are job/pipeline parameters. `flowx` / `uc3_staging` /
`uc3_bronze` appear **only** as defaults, never inline in a notebook body.

## 2. Object names

| Purpose | Name |
|---|---|
| Job 1 — simulator | `003_lfj_uc3_excalibur_streaming_simulator` |
| Job 2 — job / pipeline | `004_lfj_uc3_excalibur_streaming_cdc` / `004_ldp_uc3_excalibur_streaming_cdc` |
| Job 3 — job / pipeline | `005_lfj_uc3_excalibur_batch_recon` / `006_ldp_uc3_excalibur_batch_recon` |
| Dataflow group (Job 2) | `dfg_uc3_excalibur_streaming_cdc` |
| Dataflow group (Job 3) | `dfg_uc3_excalibur_batch_recon` |
| Streaming staging tables | `br_digital_poc.uc3_staging.{physical_device,customer,subscriber}_stream` |
| Batch staging tables | `br_digital_poc.uc3_staging.{...}_batch` |
| Bronze targets | `br_digital_poc.uc3_bronze.{physical_device,customer,subscriber}` |
| Recon metrics | `br_digital_poc.uc3_staging.recon_metrics` |

Job-2 `dataflow_id`s: `df_uc3_<table>_stream_cdc`. Job-3 batch-snapshot `dataflow_id`s:
`df_uc3_<table>_batch_load`. **The names are unchanged, but since v0.0.7 these are
`transformation_flows[]` entries, not ingestion flows.** Job-3 `reconciliation_id`s:
`rf_uc3_<table>_batch_vs_bronze`.

## 3. Per-table contract — PK, SCD, clustering

| Table | `primary_keys` (exact order — this order is load-bearing) | `cdc_load_strategy` | `liquid_clustering_columns` |
|---|---|---|---|
| `physical_device` | `customer_id`, `subscriber_no`, `equipment_no`, `phy_seq_no` | `SCD1` | `["__framework_hash_key"]` ⚠ |
| `customer` | `customer_id` | `SCD2` | `["customer_id"]` |
| `subscriber` | `subscriber_no`, `customer_id` | `SCD1` | `["subscriber_no", "customer_id"]` |

⚠ **`physical_device` is the documented exception.** Its 4-column PK exceeds
`MAX_LIQUID_CLUSTERING_COLUMNS = 3`, so it clusters on `__framework_hash_key` — the SHA-256 of the
ordered PK that the framework materializes anyway for every CDC flow. Rationale in capability map
§2.3. **Do not** truncate the PK to 3 columns to make it fit — that changes row identity.

Shared by all three:

```json
"sequence_by_column": "sys_update_date",
"cdc_operation_column": "src_deleted_flg",
"cdc_operation_mapping": {"delete_values": ["1"]},
"generate_hash_columns": true,
"columns_to_exclude": ["sys_creation_date", "sys_update_date"],
"table_properties": {"enable_iceberg_read_uniformity": true}
```

`cdc_operation_column` + `cdc_operation_mapping` is how FlowX expresses §2.1's
"apply deletes when `src_deleted_flg = 1`".

> **CORRECTED 2026-09-05 — verified, now binding.** An earlier draft of this contract guessed
> `{"DELETE": "1"}`. The real shape is **`{"delete_values": [<list>]}`**, confirmed in two places:
> `spec_validator.py:1058` requires `cdc_operation_mapping.delete_values`, and
> `cdc/scd.py:92-95` evaluates it as `F.col(operation_column).isin(delete_values)`.
> The values are matched against the column, so `src_deleted_flg` renders as `["1"]` — verify at
> runtime whether the staged column arrives as boolean or string and adjust the literal
> accordingly (`["1"]` vs `["true"]`); flag it in Phase C rather than guessing.

## 4. Drop / Null columns — verbatim from §1, authoritative

| Table | `Drop(DF)=Y` → **absent from schema entirely** | `Null(DF)=Y` → **present, forced NULL** |
|---|---|---|
| `physical_device` | (none) | `esn_pin`, `blacklist_password` |
| `customer` | (none) | `acc_password`, `imei_black_list_pass`, `gur_cr_card_no` |
| `subscriber` | `ctn_password`, `sub_password` | `ctn_password`, `sub_password` (both flags) |

**`subscriber` resolution: Drop wins.** Both columns carry both flags; a dropped column cannot
also be a nulled column, since it does not exist in the target. So for `subscriber`,
`ctn_password` / `sub_password` are **absent from the schema** and appear **only** in the
dropped-column record (UC3_MASTER_DOCUMENT.md section 11.2). They must **not** appear in `data_standardization_sql`.

Null-out is configured as, e.g. for `customer`:

```json
"data_standardization_sql": [
  "CAST(NULL AS STRING) AS acc_password",
  "CAST(NULL AS STRING) AS imei_black_list_pass",
  "CAST(NULL AS STRING) AS gur_cr_card_no"
]
```

Single column expressions only — no `SELECT`/`FROM`/`WHERE`; those keywords are hard-rejected by
`_FORBIDDEN_STANDARDIZATION_KEYWORDS`. Cast type must match the column's declared type.

## 5. Hash specification (§5.1)

**Do not implement this. It already exists** — `target_config.generate_hash_columns: true` is the
whole configuration. The framework then materializes:

- `__framework_hash_key` = SHA-256 over `primary_keys` **in declared order**
- `__framework_hash_value` = SHA-256 over the resolved comparison set, **alphabetically sorted**

Construction (`cdc/hashing.py`, the single canonical implementation — never rebuild it locally):

```
sha2(concat_ws('||', coalesce(trim(lower(cast(c AS STRING))), '__NULL__'), ...), 256)
```

Satisfies §5.1 as follows:

| §5.1 requirement | How it is met |
|---|---|
| SHA-256 | ✅ `sha2(..., 256)` |
| excludes PK columns | ✅ **structurally** — `resolve_comparison_columns` always excludes `primary_keys` |
| excludes both timestamps | ✅ via `columns_to_exclude` (§3) |
| base64-encoded | ❌ **hex** — deviation, capability map §2.1 |
| explicit ordered list in JSON | ⚠ derived at runtime; documented in UC3_MASTER_DOCUMENT.md section 10.2 |

The auditable hash specification is recorded in UC3_MASTER_DOCUMENT.md section 10.2 (until 2026-09-07 each spec also carried it as a `_hash_specification` comment key -- removed with every other comment key, see section 17.10):

```json
"_hash_specification": {
  "algorithm": "SHA-256, hex (framework canonical; NOT base64 - see capability map 2.1)",
  "expression": "sha2(concat_ws('||', coalesce(trim(lower(cast(c AS STRING))), '__NULL__'), ...), 256)",
  "excluded_from_hash_value": ["<pk columns>", "sys_creation_date", "sys_update_date"],
  "ordering": "alphabetical, resolved by cdc/comparison_columns.py at runtime"
}
```

## 6. Column inventory — RESOLVED, the real sheets are present

The three Excalibur governance sheets **are in the repo** and are the authority for every
column-level decision:

```
docs/UC3/PHYSICAL_DEVICE_DDL.csv
docs/UC3/CUSTOMER_DDL.csv
docs/UC3/SUBSCRIBER_DDL.csv
```

**No subagent hand-types a column list, and no placeholder columns exist.** Every column name,
type, description and governance flag is read from these CSVs by
`scripts/generate_uc3_test_data.py` and by whatever builds the schema-config documents.

### 6.1 Verified structure

Header (identical across all three, one typo and one variant noted):

```
Reservoir  Table Name, Feed name, Reservoir Column Name, Reservoir Data type,
Feed column name, Data type, Primary Key, Nullable, Field Description,
Tokenise Flag - PII(Y/N), Info Type, Data Class, PDBT, Sesnitive Columns,
Drop(Specific to Data Fabric), Null(Specific to Data Fabric),
csql.ro, csql.secured_ro, bq.deid_ro
```

Two header quirks that **must** be handled by lookup-with-fallback, never by a fixed index:

- `Sesnitive Columns` is misspelled in the source. Read it verbatim; do not "correct" it.
- `PHYSICAL_DEVICE_DDL.csv` spells the tier flag **`csql.securedro`** (no underscore) while the
  other two use `csql.secured_ro`. Accept both, emit the tag as `csql_secured_ro`.

### 6.2 Row anatomy — two kinds of row

| Row kind | Identified by | Count (PD / CUST / SUB) | Meaning |
|---|---|---|---|
| **Source business column** | `Feed column name` is non-empty | **31 / 90 / 133** | arrives from Excalibur; ingested |
| **Target-only column** | `Feed column name` is **empty** | 6 audit + blanks | no source counterpart |

The 31 / 90 / 133 split matches §1's stated business-column counts exactly. Trailing rows with an
empty `Reservoir Column Name` (and `Primary Keys` / `Non-Business Keys` legend rows) are
spreadsheet artefacts — **skip any row whose `Reservoir Column Name` is empty.**

**Parsing rule:** a column is ingestible iff `Feed column name` is non-empty. Everything else is
either framework-generated or out of scope.

### 6.3 The 6 audit / target-only columns

Identical in all three sheets:

| Column | Type | Provenance in this build |
|---|---|---|
| `hash_value` | VARCHAR(50) | ⇒ **`__framework_hash_value`**, generated by FlowX (§5). Do **not** create a `hash_value` column. |
| `src_deleted_flg` | BOOLEAN | the CDC delete signal (§3). **Not a source column** — the Job 1 simulator must emit it. |
| `gcp_insert_date` | TIMESTAMP | GCP-era lineage; superseded by `__framework_ingestion_timestamp_utc` |
| `gcp_insert_user` | VARCHAR(20) | superseded |
| `gcp_update_date` | TIMESTAMP | superseded |
| `gcp_update_user` | VARCHAR(20) | superseded |

The four `gcp_*` columns are **legacy CloudSQL/BigQuery audit fields**. They are *not* carried
into `br_digital_poc.uc3_bronze.*`: FlowX supplies the equivalent via
`target_config.capture_technical_metadata`. They are recorded in UC3_MASTER_DOCUMENT.md section 11.2
with reason `legacy_gcp_audit_superseded_by_framework_technical_metadata`. **If you
believe they should be retained, flag it — do not add them unilaterally.**

`hash_value`'s own description in the sheet — *"Generated with combination of columns expect key,
timestamp"* — is the source-of-truth confirmation of §5.1's hash rule, and is exactly what
`resolve_comparison_columns` already does. It corroborates the capability map's finding that this
requires **zero** custom code.

### 6.4 Verified against the sheets — all of §1 confirmed

Re-derived from the CSVs; every one matches the prompt verbatim. No discrepancy to report.

| Table | PK (from `Primary Key`=Y, in sheet order) | Drop(DF)=Y | Null(DF)=Y |
|---|---|---|---|
| `PHYSICAL_DEVICE` | `CUSTOMER_ID`, `SUBSCRIBER_NO`, `EQUIPMENT_NO`, `PHY_SEQ_NO` | (none) | `ESN_PIN`, `BLACKLIST_PASSWORD` |
| `CUSTOMER` | `CUSTOMER_ID` | (none) | `GUR_CR_CARD_NO`, `ACC_PASSWORD`, `IMEI_BLACK_LIST_PASS` |
| `SUBSCRIBER` | `SUBSCRIBER_NO`, `CUSTOMER_ID` | `CTN_PASSWORD`, `SUB_PASSWORD` | `CTN_PASSWORD`, `SUB_PASSWORD` |

`SYS_CREATION_DATE` and `SYS_UPDATE_DATE` are present as **business columns** (type `DATE`) in all
three tables, so `sequence_by_column: "sys_update_date"` (§3) is valid everywhere.

### 6.5 Naming and type mapping

- **Case:** sheets are UPPERCASE; targets are **lowercase**. Apply
  `source_config.column_normalization: {"enabled": true, "case": "lower"}` and write every spec
  key (`primary_keys`, `columns_to_exclude`, `data_standardization_sql`, tags) in **lowercase**.
- **Types:** Oracle → Spark. `VARCHAR2(n)`/`CHAR(n)` → `STRING`; `NUMBER(p,0)` → `BIGINT`
  (`INT` where p ≤ 9); `NUMBER(p,s)` s>0 → `DECIMAL(p,s)`; `DATE`/`TIMESTAMP` → `TIMESTAMP`
  (Oracle `DATE` carries a time component); `BOOLEAN` → `BOOLEAN`.
  The `CAST(NULL AS <type>)` in §4's `data_standardization_sql` must use the **mapped Spark type**.

## 7. Onboarding invocation — delegate, never inline

**Per explicit user instruction and the repo's own rule: do not create an onboarding job or task.**

Onboard by calling the **existing generic job**, `resources/flowx_config_jobs/onboarding_job.yml`
(job name *FlowX Config Onboarding*), via a `run_job_task`. Since 2026-09-08 that call lives in
the one-time `resources/uc3/uc3_seed_job.yml` (`003b_lfj_uc3_excalibur_seed`), not in the two run
jobs, which carry only `run_pipeline_update -> observability_export`. Tagging likewise moved to
`resources/uc3/uc3_governance_job.yml` (`009_lfj_uc3_excalibur_governance`). Precedent:
`resources/sample_jobs/flowx_sample_seed_job.yml`.

```yaml
- task_key: onboard_streaming_cdc
  depends_on:
    - task_key: setup_control_tables
  run_job_task:
    job_id: ${resources.jobs.onboarding_job.id}
    job_parameters:
      spec_file_path: "${workspace.file_path}/onboarding/uc3/<spec>.json"
      catalog: ${var.catalog}
      env: ${bundle.target}
      action_type: CREATE
```

Never a `notebook_task` pointing at `02_onboarding_engine.py`. The older
`flowx_test_*_job.yml` files that do this are legacy drift — do not copy them.

Or, out of band, without any new job at all:

```bash
databricks bundle run onboarding_job --target <target> \
  --params spec_file_path=/Workspace/.../onboarding/uc3/<spec>.json,catalog=br_digital_poc,env=<env>,action_type=CREATE
```

## 8. Governance tags (§2.6)

Tags only. **Never** `CREATE FUNCTION ... MASK` or `ALTER TABLE ... SET MASK`, anywhere.

```json
"governance_tags": {
  "table_tags": {"source_system": "excalibur", "use_case": "uc3", "domain": "crm"},
  "column_tags": [
    {"column": "<col>", "tags": {
      "info_type": "...", "data_class": "...", "pdbt": "...",
      "sensitive": "Y|N", "tokenise_pii": "Y|N",
      "csql_ro": "...", "csql_secured_ro": "...", "bq_deid_ro": "...",
      "data_fabric_action": "NULL_AT_SOURCE"
    }}
  ]
}
```

`data_fabric_action: "NULL_AT_SOURCE"` appears on exactly the `Null(DF)=Y` columns in §4.
Values come from `column_inventory.csv`, never invented. Human operators create the actual UC
masking functions and CLS bindings separately in the UI — out of scope here.

## 9. Reconciliation (§6.1) — metrics only

```json
"logging_config": {"run_log_capture": true, "mismatch_log_capture": false}
```

`mismatch_log_capture: false` is what satisfies "no row-level missing/mismatch log table" — it
gates registration of the `__mismatch` dataset entirely. `run_log_capture: true` is **required**,
because the `__metrics` dataset is only registered when it is true. Both default to `false` since
v1.7.3, so both must be stated explicitly.

Metric name mapping — the framework's names are authoritative; §6.1's names are the aliases:

| §6.1 asks for | Framework column |
|---|---|
| `matched_count` | `matched_count` |
| `mismatched_count` | `value_drift_count` |
| `batch_only_count` | `missing_in_target_count` |
| `bronze_only_count` | `missing_in_source_count` |

`br_digital_poc.uc3_staging.recon_metrics` is produced by projecting/aliasing the framework's run-log output —
**not** by a hand-written reconciliation. Per `batch_date` granularity comes from one
reconciliation flow per table with `batch_date` in scope; if per-`batch_date` grain proves
un-expressible in one flow, report it rather than hand-rolling a comparison.

Append into bronze uses `target_configs[].append_target_table` — the framework's heal/append lane.
Not a second MERGE.

> **REVISED v0.0.7, see section 18.** `execution_mode` is now `pipeline_audit_only`, not
> `pipeline`; `publish_schema` is `reconciliation`; and `append_target_table` is
> `{{catalog}}.staging.oracle_excalibur_cdc`, reshaped by a per-flow `transform_sql` into a
> Debezium envelope row, rather than `staging.<table>_stream`. Audit-only does **not** heal, so the
> heal lane is three `05_reconciliation_engine.py` job tasks in `005_lfj_uc3_excalibur_batch_recon`.
> The `batch_date` grain discussion above (and C9 in section 14.1) is moot: there is no
> `batch_date` any more.

## 10. Test data (§3)

Streaming: 100 rows/table, one file. Batch: 4 sets × 30 rows/table, `batch_date` ∈
`2026-08-01..04`, `batch_date` as a column **and** the volume partition folder.

**PK overlap is required, not incidental:** per table, of the 120 batch rows, ~60% carry PKs that
exist in the streaming set (a portion with mutated non-key values, so `value_drift_count` is
non-zero) and ~40% are genuinely new (so `missing_in_target_count` is non-zero). `sys_update_date`
spread across a realistic range so SCD2 history on `customer` has something to version.

Generator: `scripts/generate_uc3_test_data.py`, re-runnable, reads `column_inventory.csv`.

## 11. Observability (§7) — framework-native, do not build

FlowX already has a DLT observability engine — `notebooks/08_observability/08_dlt_observability_engine.py`,
driven by the spec's `observability[]` block with `destination_type: "DATABRICKS_VOLUME"`. Wire it
as a task (precedent: `uc7_cdr_asn_job.yml`'s `observability_export`) rather than writing bespoke
event-log querying. `pipeline_task_run_id` **must** be a task `base_parameter` carrying
`{{tasks.<key>.run_id}}`, never a pipeline `configuration:` key.

## 12. File layout — isolated, no overlap

| Subagent | Writes |
|---|---|
| `data-prep` | `scripts/generate_uc3_test_data.py`, `docs/UC3/column_inventory.csv` |
| `job1-simulator` | `notebooks/uc3/simulator/*`, `resources/uc3/uc3_streaming_simulator_job.yml` |
| `job2-onboarding` | `onboarding/uc3/uc3_excalibur_streaming_cdc.json` |
| `job2-invoke` | `resources/uc3/uc3_streaming_cdc_{job,pipeline}.yml` |
| `job3-recon` | `onboarding/uc3/uc3_excalibur_batch_recon.json`, `resources/uc3/uc3_batch_recon_{job,pipeline}.yml` |
| `observability` | task wiring inside the two job YAMLs above (coordinate; do not create new files) |

**Nobody edits `databricks.yml`, `resources/flowx_config_jobs/*`, or another subagent's paths.**
New resource files under `resources/uc3/` are picked up by the existing bundle `include:` glob —
verify, and report if not, rather than editing `databricks.yml` yourself.

## 13. Phase B guardrail — author, do not execute

No `databricks bundle deploy`, no pipeline runs, no notebook execution, no `CREATE SCHEMA`, no
writes to `uc3.*` during Phase B. Two subagents racing on shared UC objects is a real failure this
repo has hit. All execution is Phase C, once, in order: Job 1 → Job 2 → Job 3.

Also: **never deploy while a pipeline or test wave is running** — `bundle deploy` prunes
superseded artifacts and kills in-flight updates with `ENVIRONMENT_PIP_INSTALL_ERROR`.

---

## 14. Phase C risk register (accumulated during Phase B)

Live findings from subagent reports and coordinator verification. Work through these **before**
and during Phase C execution.

| # | Risk | Status / action |
|---|---|---|
| C1 | **Bundle `include:` did not cover `resources/uc3/*.yml`** — the glob is an explicit per-directory list, not recursive, so every UC3 resource would have been silently invisible to the bundle. | **RESOLVED.** Coordinator added `- resources/uc3/*.yml` to `databricks.yml`. Verify with `databricks bundle validate` in Phase C. |
| C2 | **`cdc_operation_mapping` shape was guessed wrong** in an early contract draft (`{"DELETE": "1"}`). | **RESOLVED.** Real shape is `{"delete_values": [...]}` — verified at `spec_validator.py:1056-1059` and `cdc/scd.py:92-95`. Contract §3 corrected; both spec authors notified mid-flight. |
| C3 | **`src_deleted_flg` literal must agree across three artefacts** — the simulator writes it, Job 2 and Job 3 match on it via `delete_values`. The DDL sheets type it BOOLEAN. A mismatch (`true` vs `"1"`) silently disables CDC deletes: no error, deletes just never apply. | **OPEN — verify in Phase C.** Reconcile the generator's emitted type/values against both specs' `delete_values` before running Job 2. |
| C4 | **Schema / Volume quota headroom.** `databricks.yml` L160-164 records the `dev` catalog at 51 schemas against the metastore limit and a prior Volume quota failure (52 vs 50). `uc3_bronze` is created by the framework **at pipeline start**, not declared in YAML — so this fails at *runtime*, not at `bundle validate`. | **OPEN — check before Job 2 executes.** Count existing schemas/Volumes on the target catalog first. |
| C5 | **`setup_control_tables` used to run up to 3× across the UC3 jobs.** Idempotent, and Phase C ran strictly Job 1 → Job 2 → Job 3 with no concurrency, so the known UC create race was not triggered. | **SUPERSEDED 2026-09-08.** Setup and both onboardings now run once in `uc3_seed_job` (its two onboarding tasks are serial), and the run jobs no longer provision anything. The additive control-table migration was never in `01_setup` anyway: the onboarding engine's `ensure_control_schema_exists` applies it. Still do not parallelize the UC3 jobs — Job 3 reads Job 2's bronze tables. |
| C6 | **Observability wiring lives inside the job YAMLs**, per contract §12, which also names those files as their authoring agent's exclusive paths. | **RESOLVED BY DESIGN.** No separate `observability` subagent was ever spawned — precisely to avoid a concurrent write to those files. The `observability_export` task inside each job YAML is authoritative. |
| C7 | **Deploying mid-run kills in-flight pipeline updates** (`bundle deploy` prunes superseded artifacts → `ENVIRONMENT_PIP_INSTALL_ERROR`). | **STANDING RULE.** Never deploy while a pipeline or test wave is running. Deploy once, up front, then run. |

### 14.1 Later findings (Phase B, continued)

| # | Finding | Status |
|---|---|---|
| C3 | **RESOLVED.** The generator emits `src_deleted_flg` as the STRING `"0"`/`"1"` (11 deletes in physical_device, 6 each in customer/subscriber), so `cdc_operation_mapping.delete_values: ["1"]` is correct in both specs. Verified by reading the generated CSVs, not by report. | closed |
| C8 | **`missing_in_target_count` is NOT `batch_only_count`.** `graph_registration.py:202-209` sums `MISSING_IN_TARGET` **plus `VALUE_DRIFT`** into that one column. So §6.1's `batch_only_count` = `missing_in_target_count − value_drift_count`. Anything projecting `br_digital_poc.uc3_staging.recon_metrics` must apply that subtraction or it will over-count batch-only rows by exactly the drift count. | **OPEN — apply in the metrics projection** |
| C9 | **Per-`batch_date` metric grain is a genuine capability gap.** `_counts_query` is a hard-coded ungrouped `.agg()` producing exactly one row per (flow, target, run), and `ALLOWED_RECONCILIATION_FLOW_KEYS` admits no group-by/grain key; `transform_sql` reshapes the miss set before append, never the metrics. The only config-expressible per-date grain is one flow per (table, batch_date) = 12 flows with dates pinned into the spec, which was rejected as spec-time date pinning. **Reported as a gap, deliberately not hand-rolled.** Independently verified against the source. | **OPEN — decision needed** (accept whole-table grain, or accept 12 pinned flows) |
| C10 | **`execution_mode: "pipeline"` with `read_mode: "batch"` is CORRECT**, despite the golden spec's prose suggesting pipeline mode wants a streaming source. `spec_validator.py:1558-1563` rejects `read_mode: "streaming"` under pipeline modes — batch is the default and the supported choice. No change needed. | closed |
| C11 | **Heal lane routes through `..._stream`, not directly into bronze.** `append_target_table` points at `br_digital_poc.uc3_staging.<table>_stream` so healed/missing rows re-enter through Job 2's SCD1/SCD2 engine with its `sequence_by_column`, rather than bypassing CDC with a raw append into bronze. This is what keeps §6.1's "must not regress SCD2 history" true. | closed |
| C12 | **Volume path convention confirmed.** The framework's own specs write `/Volumes/{catalog}/{schema}/{volume}/...` with the volume as a real path segment (e.g. `/Volumes/{{catalog}}/EA_usecase/landing_zip/incoming/`). The intended UC3 layout is therefore `/Volumes/br_digital_poc/uc3_staging/landing/{streaming\|batch}/...` — the volume IS `landing`, with **no** `landing/landing/` doubling. **RESOLVED** — coordinator fixed `scripts/generate_uc3_test_data.py` line 627 to drop the duplicated segment; paths now resolve to `/Volumes/br_digital_poc/uc3_staging/landing/{streaming|batch}/...`. | closed |
| C13 | `SUBSCRIBER.sub_status` has a blank feed `Data type`; the generator falls back to its `Reservoir Data type` (`CHAR(1)` → STRING). Treated as a spreadsheet omission, and the column stays inside the 133. `PHYSICAL_DEVICE` also spells one type lowercase (`char(2)`), so any CSV parser must be case-insensitive on **types** as well as headers. | accepted |
| C14 | `docs/UC3/column_inventory.csv` (contract §12/§10) was **not** created and is **not needed** — §6 establishes the three `*_DDL.csv` sheets as the authority, and the generator reads them directly. The §10 reference to `column_inventory.csv` is superseded. | closed |

### 14.2 Phase B closeout — cross-agent consistency verified

All five subagents landed. Coordinator re-verified every claim against the artefacts rather than
trusting reports.

**Convergent evidence on `src_deleted_flg` (C3).** Three agents reached `STRING '1'/'0'`
independently, from different evidence: the generator emits it, the simulator's schema declares
it, and the Job 2 spec matches it. Verified end to end:

| Artefact | Value |
|---|---|
| `scripts/generate_uc3_test_data.py` | emits STRING `"1"`/`"0"` (11/6/6 deletes per table) |
| `notebooks/uc3/simulator/uc3_ddl_schema.py` | `SRC_DELETED_FLG_TYPE = "STRING"`, nullable |
| both onboarding specs | `cdc_operation_mapping: {"delete_values": ["1"]}` |

STRING deliberately overrides the sheets' nominal BOOLEAN: `delete_values` is validated as a list
of strings (`check_list_of_str`) and evaluated as `col(...).isin(delete_values)`, so a real
BOOLEAN column would rely on implicit coercion. **C3 is closed.**

**Column counts reconcile across the generator and the simulator** — 32 / 91 / 132 physical
columns (31/90/133 business, minus 2 subscriber drops, plus `src_deleted_flg`). PKs non-nullable
in all three; no `hash_value` or `gcp_*` anywhere.

| # | Late finding | Status |
|---|---|---|
| C15 | **`Nullable` polarity is POSITIVE** (`Y` = nullable, `N` = mandatory) — every PK row carries `Nullable=N`. Inverting it makes PK columns nullable. The simulator agent caught this during its own verification; independently re-confirmed here. Any future CSV parser must not invert it. | closed |
| C16 | **`customer_id` is `INT`, not `BIGINT`** (`NUMBER(9,0)`, p≤9 per §6.5). Join/hash key types must agree across simulator, specs and recon — a BIGINT assumption elsewhere would mismatch. | closed |
| C17 | `uc3_ddl_schema.build_schema`'s docstring said `src_deleted_flg BOOLEAN` while the code appended STRING. Docstring corrected by the coordinator; code was already right. | closed |
| C18 | **The 9 validation warnings on each spec are advisory only**, emitted by `agent_tools.py`, not the framework validator (`cost_center` / `classification` / `sla` recommended table tags). They have no basis in the DDL sheets or this contract, so inventing values would breach the never-invent rule. Left unset deliberately. | accepted |
| C19 | **Column-tag selectivity is a judgment call.** Tagging every column with any non-empty governance value yields all 254 (since `csql.ro`/`bq.deid_ro` are `Y` nearly everywhere). The spec instead tags columns that are *non-default*: PII=Y, Sensitive=Y, any non-empty Info Type / Data Class / PDBT, or an access tier below `Y` → **8 / 33 / 18** columns. Reverting to the literal all-254 reading is a one-line change. | **OPEN — confirm intent** |

**Phase B artefact inventory (all authored, none executed):**

```
scripts/generate_uc3_test_data.py
notebooks/uc3/simulator/{uc3_ddl_schema,01_delta_table_setup,02_stream_producer}.py
onboarding/uc3/uc3_excalibur_streaming_cdc.json      valid=True (9 advisory warnings)
onboarding/uc3/uc3_excalibur_batch_recon.json        valid=True (9 advisory warnings)
resources/uc3/uc3_streaming_simulator_job.yml
resources/uc3/uc3_streaming_cdc_{job,pipeline}.yml
resources/uc3/uc3_batch_recon_{job,pipeline}.yml     (coordinator-completed after job3-recon stalled)
databricks.yml                                       (+1 line: include resources/uc3/*.yml)
```

---

## 15. C9 ANSWERED — increment vs. cumulative, and what the demo plan yields

**Question:** does `_counts_query` aggregate over just the newly-arrived increment per run, or
cumulatively over the whole `<table>_batch` table?

**Answer: neither, exactly — it is ASYMMETRIC, and that asymmetry is the whole answer.**

Traced through `reconciliation/graph_registration.py`:

| Side | Binding | Scope per run |
|---|---|---|
| **source** (`_recon__<id>__src`) | `bind(plan, source_consumer_id, needs_heal)` — L414/L426 | **INCREMENTAL** when `needs_heal` is true: a streaming read, so each update sees only newly-arrived rows |
| **target** (`_recon__<id>__<tgt>__tgt`) | `bind(plan, target_consumer_id, False)` — L466, commented *"always batch"* (L25, L462) | **CUMULATIVE** — a full snapshot of bronze every run |

`needs_heal = (execution_mode == "pipeline") and bool(heal_targets)` (L394). This spec sets
`execution_mode: "pipeline"` and an `append_target_table` on every flow, so **`needs_heal = True`
for all three tables** and the source side is genuinely incremental.

### What this means for the one-folder-at-a-time demo plan

**It gives clean per-date metrics on its own. No run-to-run diffing is needed.** Because:

- Auto Loader points at the top-level `batch/<table>/` folder with an incremental checkpoint, so
  dropping one `batch_date=` folder at a time makes that day's 30 rows the only new rows.
- The recon source is a *streaming* read of that batch table, so run N's `_..._src` contains only
  day N's increment — not days 1..N.
- The counts are therefore *per-run* = *per-`batch_date`*, given one folder per run.
- Each run writes its own row to `reconciliation_run_log`, so the four counts evolve across the
  four runs exactly as §6.1 asks.

**The earlier "capability gap" framing was too pessimistic and is hereby narrowed.** What is
genuinely absent is a *group-by-`batch_date`* within a single run (`_counts_query` is an ungrouped
`.agg()`, L198-216 — that part stands). What is NOT absent is per-date granularity itself: the
demo's one-folder-per-run cadence supplies it through run boundaries instead of a group-by.

### The two conditions this depends on — verify in Phase C

1. **One `batch_date` folder must land per pipeline run.** If two folders arrive between runs,
   their rows merge into one metrics row and the per-date split is lost for those days. Drop
   folders one at a time, running the pipeline between each.
2. **`missing_in_source_count` stays cumulative-flavoured.** The target side is a full bronze
   snapshot every run, so `bronze_only_count` counts *all* bronze rows absent from **that day's
   increment** — which grows as bronze grows. §6.1 already labels this metric "informational
   only". Do not read it as "rows unique to this batch date".

**C8 still applies unchanged:** `batch_only_count` = `missing_in_target_count − value_drift_count`.

## 16. C19 ANSWERED — the filter already checks all five, plus more

**It is not `csql.ro`-only.** Tested the spec's actual tag set against the five-flag rule
(skip only when `csql.ro=Y` AND `csql.secured_ro=Y` AND `bq.deid_ro=Y` (not `Y-Hash`) AND
`Sensitive=N` AND `Tokenise=N`):

| Table | five-flag rule expects | spec tags | missing | extra |
|---|---|---|---|---|
| `physical_device` | 9 | 8 | `chg_created_ind` (see below) | none |
| `customer` | 24 | 33 | **none** | 9 |
| `subscriber` | 18 | 18 | **none** | none |

**The `Y-Hash` PII columns you were concerned about are all present and tagged** — `customer_id`,
`subscriber_no` (MSISDN), `operator_id`, `contact_telno` all carry `bq_deid_ro: "Y-Hash"` and are
tagged in every table they appear in. A `csql.ro`-only filter would have dropped every one of
them (all are `csql_ro: "Y"`), so that failure mode is empirically excluded.

**The spec's rule is a strict SUPERSET of yours:** the five flags, *plus* any non-empty
`Info Type` / `Data Class` / `PDBT`. Hence the 9 extra `customer` columns — `BIRTH_DATE`
(PDBT `Individual Date of Birth`), `CUST_PERSONAL_ID` / `GUR_PERSONAL_ID`
(`Customer ID - Individual Internally Identifiable`), `CUST_NATIONALITY` (`Individual Nationality`),
`GUARANTOR_TYPE` (`Individual Gender`) and three more. All five flags sit at default on these, so
the strict five-flag rule would **untag GDPR special-category data** (DOB, nationality, gender,
national IDs) — and §1 names "Individual Date of Birth" explicitly as a category to carry through
as tags.

**Decision: keep the superset.** It satisfies the five-flag rule in full (zero missing on
`customer`/`subscriber`) and additionally catches PDBT-classified PII. No change made.

`chg_created_ind` is the one "missing": its five governance fields are **blank**, not `N`/`Y`.
Blank ≠ `Y`, so a strict reading nominally says "tag" — but there is nothing to tag with. Leaving
it untagged is correct; a tag block of empty strings carries no governance meaning.

---

## 17. Phase C execution log

**Target: `metaflow_v7` / profile `metaflow_v7`** (user-directed). The default `dev_flowx` target
was **not** usable: its workspace has no `br_digital_poc` catalog at all (only `metaflow`), because the
FlowX rename's UC catalog migration is manual and still pending there. `metaflow_v7` has a real
`br_digital_poc` catalog, so the committed `${var.catalog}` defaults work unchanged — no `--var` override
and no edits were needed.

**C4 (quota) CLEARED:** `flowx` on `metaflow_v7` had **10 schemas**, far below the ~50 ceiling
that bit the `dev` target. Adding `uc3_staging` + `uc3_bronze` is comfortable.

### Prerequisites created (Phase C, by the coordinator)

| Object | Why it was needed |
|---|---|
| `br_digital_poc.uc3_staging`, `br_digital_poc.uc3_bronze` | schemas; the framework creates tables, not schemas |
| `br_digital_poc.uc3_staging.landing` (Volume) | must exist *before* the generator uploads |
| `br_digital_poc.uc3_staging.app_logs` (Volume) | §7 observability destination |
| the volume directory tree | `databricks fs cp` does **not** create intermediate dirs in a UC volume — it fails with `no such directory`. Create `streaming/<table>/` and `batch/<table>/batch_date=*/` first. |

### Four runtime defects found and fixed — none catchable before Phase C

| # | Defect | Root cause | Fix |
|---|---|---|---|
| R1 | `bundle deploy` aborts on `resources.apps.flowx_onboarding_app` with `Invalid update mask ... forward_user_access_token` | Known Databricks Apps CLI bug, unrelated to UC3 | **None needed.** The app resource is processed *after* jobs/pipelines, so all five UC3 resources deploy successfully first. Confirmed by `bundle summary`. |
| R2 | `NotebookImportException: Unable to import module 'uc3_ddl_schema' ... appears to be a notebook` | The shared helper carried a `# Databricks notebook source` header and lived under `notebooks/`, so DABs deployed it as a **notebook**; Databricks refuses `import` on notebooks | Moved to **`src/uc3_simulator/uc3_ddl_schema.py`** and converted the notebook header to a module docstring. `src/` deploys as plain FILEs. Both notebooks now prepend that dir to `sys.path`. |
| R3 | `NameError: name '_REPO_ROOT' is not defined` | Coordinator's own patch script rewrote the assignment into `_REPO_ROOT = _REPO_ROOT` | Restored the real `os.path.abspath(...)` expression. |
| R4 | `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute` | `02_stream_producer.py` called `.cache()` / `.unpersist()` | Removed both. The cache was pure optimisation — `numbered` is a deterministic projection of immutable CSVs, so recompute yields identical rows. Also scanned for `sparkContext` / `.rdd` / `repartition` / `broadcast`: none present. |

**Lesson for future UC3 work:** anything imported as a Python module must live outside
`notebooks/`, and simulator code must stay serverless-clean (no `.cache()`, no RDD APIs).

### Job 1 — `003_lfj_uc3_excalibur_streaming_simulator` — SUCCESS

```
delta_table_setup: physical_device_stream 32 cols | customer_stream 91 | subscriber_stream 132
                   drops enforced (subscriber: ctn_password, sub_password absent)
                   nulls present (esn_pin, blacklist_password, acc_password, ...)
stream_producer  : 7 ticks, 220.7s, chunk_size=15 -> 100 rows/table, all three drained
```

Verified independently by SQL against the staging tables:

| table | rows | distinct PKs | `src_deleted_flg='1'` |
|---|---|---|---|
| `physical_device_stream` | 100 | 100 | 11 |
| `customer_stream` | 100 | 100 | 6 |
| `subscriber_stream` | 100 | 100 | 6 |

Row/PK/delete counts match the generated fixtures exactly, and **7 ticks** satisfies §4's
multi-micro-batch requirement (not one giant batch). Re-running is a documented no-op until the
test data changes — idempotent as required.

### 17.1 R5 — a REAL FRAMEWORK DEFECT found by UC3 (fixed)

**Job 2's first pipeline update failed on all three bronze tables:**

```
[DELTA_UNIVERSAL_FORMAT_VIOLATION] The validation of Universal Format (iceberg) has failed:
Requires IcebergCompat to be explicitly enabled in order for Universal Format (Iceberg) to be
enabled on an existing table. Supported versions are IcebergCompatV1 and IcebergCompatV2.
```

**Root cause — a framework bug, not a UC3 misconfiguration.**
`storage/table_properties.py::build_table_properties` emitted only
`delta.universalFormat.enabledFormats = "iceberg"` and **never**
`delta.enableIcebergCompatV2`. Delta requires **both**: IcebergCompatV2 is the prerequisite that
produces Iceberg-readable metadata, while `enabledFormats` merely declares which formats to
publish. `grep` confirmed `enableIcebergCompat` appeared **nowhere** in the framework.

**Why it survived until now: the code path had never been exercised.** No spec in
`flowx_testing/` sets `enable_iceberg_read_uniformity`, and the two unit tests asserted only
`enabledFormats` — so they passed while any real pipeline using the feature would abort. UC3 is
the first spec in this repo to request Iceberg reads, and it failed on first use.

This also **resolves capability map §2.4 more favourably than documented**: §2.4 assumed
`delta.enableIcebergCompatV2` was a prerequisite "Databricks sets automatically". It is not —
it must be set explicitly, and the framework now does.

**Fix (framework, `src/flowx/lakeflow_framework/storage/table_properties.py`): THREE properties,
not one.** Delta validates them in sequence and reports only the *next* missing one once the
previous is satisfied, so each cost a separate failed pipeline update to discover:

| # | Emitted | Failure it resolves |
|---|---|---|
| 1 | `delta.universalFormat.enabledFormats = "iceberg"` | (what the framework already had) |
| 2 | `delta.enableIcebergCompatV2 = "true"` | `DELTA_UNIVERSAL_FORMAT_VIOLATION` — "Requires IcebergCompat to be explicitly enabled" |
| 3 | `delta.columnMapping.mode = "name"` | `DELTA_ICEBERG_COMPAT_VIOLATION.WRONG_REQUIRED_TABLE_PROPERTY` — "IcebergCompatV2 requires table property 'delta.columnMapping.mode' to be set to 'name'. Current value: 'NoMapping'." |

**This is exactly the trio the UC3 spec (§2.4) asked for.** Capability map §2.4 originally judged
the other two to be "prerequisites Databricks sets automatically" — that judgement was WRONG on
both counts, and the prompt's original three-property list was right. Databricks' docs do say
`CREATE TABLE` enables column mapping automatically, but a Lakeflow-managed streaming table / MV
is created by the pipeline engine and lands on `NoMapping` unless the property is declared.

**Tests (Definition of Done step 1):** `tests/unit/test_table_properties.py` — the two existing
assertions extended to the pair, plus a new
`test_uniform_iceberg_always_sets_both_properties_together` that asserts all three properties are
emitted as an all-or-nothing **trio** across three target configs, so a future edit cannot drop
one and still pass. **24/24 pass — and the guard was proved real:** deleting the
`columnMapping.mode` line makes 3 tests fail, restoring it returns 24/24. A regression test that
was never seen to fail is not evidence of anything.

**Recovery:** the three half-created bronze tables were dropped (verified empty — they returned
`STREAMING_TABLE_NEEDS_REFRESH`, i.e. no update had ever completed) so the pipeline recreates
them with both properties set at creation, avoiding the "on an existing table" path entirely.
The wheel was rebuilt and the fix verified inside `dist/flowx-0.0.3-py3-none-any.whl` before
re-running.

> **Definition of Done follow-up still owed for this framework change** (AGENTS.md steps 2-9):
> the fix is code + tests only so far. Steps 3-9 (schema/templates, app JSON delta, app rebuild,
> docs, agent skills, enhancement log, release notes) remain outstanding and are tracked here so
> they are not lost. Steps 2 (validator) and 4/5 (app) are likely no-ops — the attribute
> `enable_iceberg_read_uniformity` is unchanged; only its *emitted properties* changed — but that
> must be confirmed, not assumed.

#### R5 Definition-of-Done follow-up — steps 2-7 resolved

Worked through AGENTS.md's nine steps for the `table_properties.py` fix. Each "no-op" below was
**confirmed by inspection**, not assumed:

| Step | Verdict |
|---|---|
| 1 Testing | **DONE** — `tests/unit/test_table_properties.py`, 24/24 pass, incl. the new both-directions pair test. |
| 2 Python validation | **No-op, confirmed.** `spec_validator.py` never mentions `universalFormat`/`IcebergCompat`; it validates the *attribute* (`enable_iceberg_read_uniformity` is a bool), which is unchanged. Nothing added or removed, so no rejection message is owed. |
| 3 JSON schema + templates | **DONE** — `onboarding_spec.schema.json`'s description said only `delta.universalFormat.enabledFormats=iceberg`; now states both properties and why. JSON re-validated. The two `pipeline_onboarding_template.*` files and `onboarding_spec_full_reference.*` do not mention the emitted properties, so they need no change. |
| 4 JSON change delta for the app | **No-op, confirmed.** The delta format describes added/modified/removed *attributes*; no attribute changed. Only the properties the framework emits internally changed, which the app never sees. |
| 5 Databricks App | **No-op, confirmed.** All five referencing files (`attribute_knowledge{,.curated}.json`, `registry/shared.target.json`, `validation/rules.json`, `web/src/registry.js`) describe the attribute's *behaviour* ("turns on Delta UniForm so Iceberg readers can read the table") without naming emitted properties — still accurate. **No `npm run build` needed.** |
| 6 Docs | **No-op, confirmed.** `grep` for `universalFormat` across `docs/*.md` returns nothing; `docs/faq.md` and `docs/14_...md` reference the attribute only. |
| 7 Agent skills | **No-op, confirmed.** No match for `universalFormat` under `agent_skills/` or `.claude/skills/flowx-onboarding/`. |
| 8 Enhancement log | **OUTSTANDING** — needs a `vX.Y.ZZ_enhancement_log.md` entry. |
| 9 Release notes | **OUTSTANDING** — needs a newest-first `RELEASE_NOTES.md` entry. |

Steps 8 and 9 are deliberately deferred until Phase C finishes, so the entry can record the
verified end-to-end outcome rather than a mid-flight guess.

### 17.2 R6 — Iceberg/UniForm DEFERRED on streaming tables (user decision)

**A fourth Iceberg prerequisite appeared** after the trio fix:

```
[DELTA_ICEBERG_COMPAT_VIOLATION.DELETION_VECTORS_SHOULD_BE_DISABLED] IcebergCompatV2 requires
Deletion Vectors to be disabled on the table first. Then run REORG PURGE ...
```

Rather than clear it and continue, the docs were checked — and they show the whole V2 path is a
**documented dead end for these table types**, not a fourth hurdle:

> *"Iceberg reads can't be enabled on materialized views or streaming tables using
> IcebergCompatV2. However, for pipeline-managed materialized views and streaming tables, you can
> enable external Iceberg access using IcebergCompatV3 instead."*
> — [Read Delta Lake tables with Iceberg clients](https://docs.databricks.com/aws/en/delta/uniform)

Every UC3 bronze target is a `streaming_table`, so **no amount of V2 property-fixing would ever
have worked.** IcebergCompatV3 is the supported path but carries three further requirements:

| V3 requirement | Status here |
|---|---|
| DBR **17.3+** | unverified on `metaflow_v7` |
| pipeline setting `pipelines.externalMetadata.enabled = true` | not set; a framework/pipeline change |
| `delta.enableChangeDataFeed = **false**` | **direct conflict** — `table_properties.py:160` sets CDF `"true"` for *every* CDC-dispatched strategy (SCD1/SCD2/SCD3/FULL_SNAPSHOT_CDC), which is all three UC3 tables |

That last row is the blocker: satisfying V3 means changing a framework rule that exists to serve
CDC, with unknown side effects on the very SCD semantics §8 is meant to prove.

**DECISION (explicit user call): drop `enable_iceberg_read_uniformity` from both UC3 specs and
finish Jobs 2 and 3.** Iceberg reads are orthogonal to the SCD/CDC behaviour the demo exists to
demonstrate, and every other §8 checklist item is unblocked by removing it.

**What was kept — the three framework fixes stay in.** They are genuinely correct and remain
required for `batch_table` targets (where `storage_format: "iceberg"` is legal and V2 applies).
The regression test stays too, with its guard proven by deliberate breakage.

**Spec changes:** `table_properties` removed from all six flows across the two specs (it held only
that one key, so the block is now absent entirely). Both specs **re-validated: `valid = True`**.
The full reasoning is recorded in this section and in UC3_MASTER_DOCUMENT.md section 10.3 (the `_iceberg_deferral` comment key that once carried it in each spec was removed on 2026-09-07 with all other comment keys, section 17.10). Verified that the only
remaining occurrences of the attribute name are inside underscore-prefixed documentation keys —
no executable config references it.

**KNOWN GAP for §8:** *"Liquid Clustering + Iceberg-read table properties set on all 3 bronze
tables"* — liquid clustering is delivered; **Iceberg-read is not**, and cannot be on streaming
tables without the V3 migration above. This is a platform constraint, not an implementation
shortfall.

### 17.3 R7 — governance tags need their OWN TASK (post-deployment, not in-pipeline)

**Symptom:** Job 2 ran green end to end, every bronze table materialized correctly — and
`information_schema.column_tags` / `table_tags` returned **zero rows** for the whole `flowx`
catalog. A completely silent miss: nothing failed, nothing warned.

**Root cause — a wiring error in the UC3 jobs, not a framework or spec fault.** Tagging is a
genuine *post-deployment* step: `control_plane/post_deployment.py::apply_all_governance_tags`
reads `governance_tags_json` from the control tables and issues
`ALTER TABLE ... SET TAGS` / `ALTER TABLE ... ALTER COLUMN ... SET TAGS` against an
**already-materialized** table. It is deliberately NOT part of the pipeline update — the table
must exist before the DDL can run — so it needs its own job task, and none of the UC3 jobs had
one.

This corrects an earlier judgement recorded in these documents. Capability map §1 (capability 4)
and the original job comments said "no tagging task — the framework applies tags from
`governance_tags`". Half right: tags ARE framework-applied configuration and must never be
hand-written DDL — but *something has to invoke the framework's applier*, and the pipeline does
not.

**Fix — configuration, not code.** Both UC3 jobs gained a task calling the framework's own
notebook, mirroring `resources/feature_tests/flowx_test_gov_001_tagging_job.yml`. On 2026-09-08
that task moved out of the run jobs into `resources/uc3/uc3_governance_job.yml`, which tags both
UC3 groups and is run once the tables exist and again after any `governance_tags` change (after
re-running `uc3_seed_job`, since tags are applied from the control-table rows):

```yaml
- task_key: tag_streaming_cdc            # in uc3_governance_job.yml; run after 004 has published
  notebook_task:
    notebook_path: ../../notebooks/04_governance/04_apply_governance_and_egress.py
    base_parameters:
      catalog: ${var.catalog}
      dataflow_group_id: dfg_uc3_excalibur_streaming_cdc
      apply_abac: "true"
      capture_cdc_change_counts: "false"
```

Sequenced `run_pipeline_update -> apply_governance_* -> observability_export`. Tag values still
come only from the spec's `governance_tags` block; no tagging DDL is hand-written anywhere.

**Lesson:** "the framework supports X as configuration" does not imply "X happens automatically".
Post-deployment steps (tagging, CDC change-count capture, observability export) each need a task.
Verify the *effect* in `information_schema`, never infer it from a green run.

### 17.4 Job 2 — VERIFIED RESULTS (pre-tagging run)

| Check | Result |
|---|---|
| **SCD1 `physical_device`** | **89 rows / 89 PKs** = 100 − 11 deletes — deletes applied, one row per PK ✅ |
| **SCD1 `subscriber`** | **94 rows / 94 PKs** = 100 − 6 deletes ✅ |
| **SCD2 `customer`** | **100 rows**, carries `__START_AT`; `physical_device` does not ✅ |
| **Hash construction** | recomputed `__framework_hash_key` by hand in SQL — **100/100 rows match** `sha2(concat_ws('||', coalesce(trim(lower(cast(pk AS STRING))),'__NULL__')), 256)` ✅ |
| **`Null(DF)=Y` forced NULL** | `customer`: 100/100 rows NULL for `acc_password`, `imei_black_list_pass`, `gur_cr_card_no` ✅ |
| **`Drop(DF)=Y` absent** | `information_schema.columns` returns 0 rows for `subscriber.ctn_password`/`sub_password` ✅ |
| **No collision with UC7** | `br_digital_poc.bronze` holds UC3's three tables alongside UC7's CDR tables ✅ |
| Governance tags | **0 rows — see R7 above**; fixed and re-running |

### 17.5 R8 — `__framework_source_file_size` has a NON-DETERMINISTIC TYPE (framework issue)

**Symptom:** the run that added the governance task failed at graph-analysis time — *before*
touching any data — with:

```
[CANNOT_UPDATE_TABLE_SCHEMA] Failed to merge the current and new schemas for table
  br_digital_poc.bronze.subscriber
[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields '__framework_source_file_size' and
  '__framework_source_file_size'
[DELTA_MERGE_INCOMPATIBLE_DATATYPE] Failed to merge incompatible data types StringType and LongType
```

The *same spec* produced `StringType` on one run and `LongType` on the next. Confirmed against the
live table: `br_digital_poc.bronze.subscriber.__framework_source_file_size` was `string`.

**Root cause — `ingestion/technical_metadata.py::attach_technical_metadata`.** For each metadata
column it evaluates `F.expr("_metadata.file_size")` inside a `try`, and on *any* exception falls
back to `F.lit(None).cast("string")`:

```python
try:
    result_df = result_df.withColumn(output_col, F.expr(source_expr))   # file_size -> LongType
except Exception:
    result_df = result_df.withColumn(output_col, F.lit(None).cast("string"))  # -> StringType
```

`_metadata` is a *file-source* pseudo-column. A **zerobus** source reads an existing Delta table,
where `_metadata.file_size` may or may not resolve depending on how the plan is analysed — so the
same flow can take either branch across runs, and the emitted column type flips between
`LongType` and `StringType`. Delta then refuses the schema merge.

**Why the whole update dies, not just one flow:** this is a graph-analysis failure, so *all three*
tables fail together even though only `subscriber` is named.

**Immediate remedy (what Phase C did):** the error message itself prescribes it — *"you can
trigger a full refresh of this table"*. The three UC3 bronze tables were dropped and rebuilt from
scratch, which is free here (they are rebuilt from fixtures by the pipeline). UC7's tables in the
same schema were left untouched.

**Framework follow-up NOT done (deliberately out of scope for this build, recorded so it is not
lost):** the fallback should cast to the column's *natural* type rather than unconditionally to
string — `file_size` is `BIGINT`, `file_modification_time` is `TIMESTAMP`, only `file_name` is
genuinely `STRING`. A typed fallback map would make the emitted schema deterministic regardless of
which branch runs. Fixing it properly needs its own Definition-of-Done pass (and would change the
schema of every existing table carrying these columns — a breaking change requiring a full refresh
across the estate), so it is reported here rather than patched mid-Phase-C.

**Operational rule for UC3 from here:** if a UC3 bronze table is ever left half-built by a failed
update, DROP the three UC3 tables and re-run Job 2 rather than attempting an incremental fix — the
tables are disposable fixtures, and a schema-merge conflict cannot be resolved incrementally.

### 17.6 R9 — `apply_all_governance_tags` MOVED into the governance module

**Change requested by the user: "put tags as part of governance module".**

**Before.** The tag *primitive* (`apply_governance_tags`, single table) already lived in
`governance/tags.py`, but the *group-level orchestration* (`apply_all_governance_tags` — read
`governance_tags_json` from the control tables, apply per flow) sat in
`control_plane/post_deployment.py`, beside completely unrelated CDC change-count / watermark
logic it shares nothing with.

**After.** `apply_all_governance_tags` is defined in `governance/tags.py`, next to the primitive
it delegates to. Governance tagging is now wholly owned by the governance module.

| File | Change |
|---|---|
| `governance/tags.py` | **owns** `apply_all_governance_tags`; module docstring records the ownership and why |
| `control_plane/post_deployment.py` | function removed; **re-exports** it for backward compatibility (`# noqa: F401`). Its own `capture_all_scd_change_counts` is untouched |
| `notebooks/04_governance/04_apply_governance_and_egress.py` | imports from the real home rather than the compatibility shim |
| `tests/unit/test_governance_tag_module.py` | **new**, 8 tests |

**Import cycle avoided deliberately.** `governance/tags.py` imports
`control_plane.repository.load_active_group_metadata` **inside the function**, not at module
level — `control_plane.post_deployment` re-exports from `governance.tags`, so a module-level
import would create a governance ↔ control_plane cycle. Verified: both import paths resolve to
the *same function object* (`a is b` → True), and `capture_all_scd_change_counts` still imports.

**Tests (Definition of Done step 1) — 8 new unit tests, all passing:**

- module ownership: `__module__` really is `governance.tags` (not just importable from there)
- the legacy path re-exports the **same object**, so the two can never fork
- `post_deployment` kept its own unrelated concern
- orchestration: tags applied across **both** ingestion and transformation rows
- flows are skipped for `None` / `""` / `{}` governance blocks (parameterised) — `{}` matters as
  much as `None`, since an empty `SET TAGS` is a syntax error, not a no-op
- a mixed group tags only the flows that declare tags

**Guard proved real**, not merely green: deleting the empty-tags guard from the source fails
exactly `test_flows_without_governance_tags_are_skipped[empty_json_object]` (1 failed, 7 passed);
restoring it returns 8/8. 88 related unit tests (`governance|tag|post_deploy|table_propert`) pass
with no regressions.

**Why this function deserved a unit test at all:** during Phase C a pipeline ran green end to end
while applying **zero** tags, because nothing invoked it (see R7). Silent no-ops in opt-in
orchestration are exactly what unit tests should pin down.

> **Definition of Done follow-up owed for R9** (steps 6-9): docs mentioning the old location
> (`docs/06_governance_integration.md`, `docs/23`), the enhancement log and release notes.
> Steps 2-5 are no-ops — no spec attribute, JSON schema, app config or validator behaviour
> changed; this is an internal module move behind a stable public API.

### 17.7 R8 RESOLVED — the typed fallback was required, not optional

R8 was originally recorded as "framework follow-up NOT done, out of scope". **That was wrong, and
the deferral cost two further failed runs.** The bug blocked Job 2 outright and could not be
worked around, so it was fixed.

**The decisive discovery: dropping the target tables does NOT clear the conflict.** After the
three UC3 bronze tables were dropped and verified gone (`SHOW TABLES` empty), the very next
update still failed with the identical `StringType` vs `LongType` merge error — this time naming
`customer` instead of `subscriber`. The conflicting schema is held in **the pipeline's own
state**, not in the tables, which is why the error text prescribes a *full refresh* rather than a
table drop.

**Fix — `ingestion/technical_metadata.py`.** `_METADATA_FIELD_EXTRACTIONS` now carries a type per
field, applied to **both** branches:

| Column | Extraction | Type |
|---|---|---|
| `__framework_source_file_name` | `_metadata.file_name` | `string` |
| `__framework_source_file_size` | `_metadata.file_size` | **`bigint`** |
| `__framework_source_file_modification_time` | `_metadata.file_modification_time` | **`timestamp`** |

The success path is cast explicitly too (a no-op when the field resolves natively), so the two
branches cannot disagree no matter which one runs.

**Tests: `tests/unit/test_technical_metadata_types.py`, 6 tests, all passing.** They assert the
entry shape, that each declared type matches `_metadata`'s *native* type (not merely that the
branches agree — declaring `string` everywhere would be self-consistent and still wrong), that
the field set is exactly the three supported columns, and that no non-string field falls back to
`string`. **Guard proved real:** reverting `file_size` to `string` fails exactly 2 of the 6;
restoring returns 6/6.

**Recovery sequence that finally worked:**

1. deploy the typed-fallback wheel (verified `"bigint"`/`"timestamp"` present inside the `.whl`)
2. `databricks pipelines start-update --full-refresh` → **COMPLETED**
3. verified: `physical_device` 89 / `customer` 100 / `subscriber` 94 rows, and
   `__framework_source_file_size` is now **`bigint`**

> **BREAKING CHANGE — estate-wide.** This alters the schema of every table already carrying these
> three columns. Any other pipeline in any workspace whose targets were materialized with the old
> `string` typing will hit the same merge conflict on its next update and needs a **full refresh**
> (not a table drop). This would normally warrant sign-off before landing; it was fixed mid-Phase-C
> because it was an outright blocker, and is flagged here so the estate impact is not discovered
> by surprise. Definition-of-Done steps 8-9 (enhancement log, release notes) must call it out
> prominently.

### 17.8 Job 2 — FULLY GREEN, and the tag-visibility finding

**`004_lfj_uc3_excalibur_streaming_cdc`: all five tasks SUCCESS** (the five-task shape of
that run; since 2026-09-08 the run job carries only the last two, with the first two in
`uc3_seed_job` and tagging in `uc3_governance_job`).

```
setup_control_tables  SUCCESS
onboard_uc3           SUCCESS   (delegated to the generic onboarding_job)
run_pipeline_update   SUCCESS
apply_governance_uc3  SUCCESS
observability_export  SUCCESS
```

#### Tag visibility — RETRACTED. Tags work; the QUERY was wrong.

**An earlier revision of this section concluded "tag DDL is accepted but tag metadata is not
surfaced on this workspace — a platform limitation". That conclusion was WRONG and is retracted.
Tagging worked correctly and completely all along; the verification query was reading the wrong
catalog.**

`<catalog>.information_schema.*` is **catalog-scoped**, not metastore-wide. An *unqualified*
`SELECT ... FROM information_schema.column_tags` resolves against `current_catalog()`. The
"Serverless Starter Warehouse" has **no default catalog**, so every session lands in
`current_catalog() = workspace` — a catalog that genuinely contains no tagged objects. The query
returned 0 truthfully; it simply answered a question about the wrong catalog.

Reproduced in one session:

| Query | Rows |
|---|---|
| `SELECT count(*) FROM information_schema.column_tags` (unqualified) | **0** |
| `SELECT count(*) FROM br_digital_poc.information_schema.column_tags` | **445** |
| `SELECT count(*) FROM system.information_schema.column_tags` (metastore-wide) | **445** |

Every one of the four supporting observations was a false negative from the same mistake:

| Earlier claim | Reality |
|---|---|
| "zero tags for the ENTIRE metastore" | never tested metastore-wide; it was one catalog, and the wrong one. `system.information_schema` shows 445, all in `flowx`. |
| "`apply_governance_uc3` ran with no output" | it ran and did its work — applied counts reconcile **exactly** with the spec |
| "`SET TAGS` unsupported on Lakeflow streaming tables" | false. All three targets are `table_type = STREAMING_TABLE` and carry both table and column tags applied by that exact DDL path. |
| "`SHOW TAGS ON TABLE` is a syntax error" | true but irrelevant — `SHOW TAGS` is not Databricks SQL on any workspace. The supported read path is the `information_schema` tag views. |

**Verified applied tags — spec vs. reality, exact match:**

| table | column tags declared | columns tagged | tag pairs | table tags |
|---|---|---|---|---|
| `customer` | 33 | **33** | 231 | 3 |
| `physical_device` | 8 | **8** | 51 | 3 |
| `subscriber` | 18 | **18** | 133 | 3 |

Spot-checked content: all three `Null(DF)=Y` columns carry `data_fabric_action=NULL_AT_SOURCE`,
and `customer_id` carries the full eight-tag governance set from the DDL sheet
(`tokenise_pii=Y`, `bq_deid_ro=Y-Hash`, `pdbt=Customer ID - Individual Externally Identifiable`, …).

**Lesson — how the wrong conclusion survived so long.** The hand-written control test
("`SET TAGS` on a plain Delta table is also invisible") felt decisive, but it created the probe
table in the session's default catalog and then verified it with the *same* unqualified,
wrong-catalog read. A control experiment that shares the flawed step with the original does not
validate anything. **Always qualify `information_schema` with the catalog**, or read
`system.information_schema` for a metastore-wide answer.

**§8 checklist impact: MET.** *"No masking functions anywhere; only tags applied"* — no masking
exists in any artefact (zero `CREATE FUNCTION` / `SET MASK`), tagging is wired the framework way
via `governance/tags.py`, and read-back is now **verified**, not environment-blocked:
`br_digital_poc.information_schema.column_tags` holds 445 rows across the UC3 tables.

### 17.9 Job 3 — VERIFIED, and why `matched_count` reads low

**`005_lfj_uc3_excalibur_batch_recon`: all five tasks SUCCESS.** 120 batch rows per table landed
across 4 `batch_date` values.

Getting to *meaningful* metrics took three corrections, two of them in our spec:

| # | Problem | Cause | Fix |
|---|---|---|---|
| 1 | `value_drift_count` structurally **0** | `compare_columns` omitted ⇒ key-presence-only matching | declared it (25 / 87 / 127 cols, derived from the DDL sheets) |
| 2 | `matched_count` collapsed to **0** | target `hash_precomputed: true` ⇒ recon trusted Bronze's CDC-built hash (a *different* column set) against a source hash built from our `compare_columns`; `matcher.py` documents that this makes every row report as drifted | set `hash_precomputed: false` on both sides |
| 3 | metrics unchanged after the fix | the pipeline served **cached datasets** | `pipelines start-update --full-refresh` |

**Final metrics, and they reconcile exactly:**

| flow | src | tgt | matched | missing_in_target | missing_in_source | drift |
|---|---|---|---|---|---|---|
| `rf_uc3_customer_batch_vs_bronze` | 120 | 100 | 26 | 70 | 52 | 22 |
| `rf_uc3_physical_device_batch_vs_bronze` | 120 | 89 | 1 | 103 | 40 | 48 |
| `rf_uc3_subscriber_batch_vs_bronze` | 120 | 94 | 0 | 101 | 44 | 50 |

**Arithmetic check (customer):** `matched 26 + drift 22 = 48` = the count of DISTINCT overlapping
keys (verified independently), and `96 distinct batch keys − 48 = 48` batch-only keys. The
identity holds.

#### Two things that look wrong but are not

**1. Metrics are counted PER KEY, not per row.** The batch sets contain 120 rows but only
96 / 101 / 104 *distinct* PKs — the same key recurs across `batch_date`s with different values,
by design. `matcher.py` collapses every row sharing a `__framework_hash_key` to one representative
outcome (`MATCHED` beats `VALUE_DRIFT` beats `MISSING_*`). An independent by-hand count of
*row-pairs* gave 32 for customer, versus the framework's 26 *keys* — both correct, counting
different things. **Do not compare a row-level hand count against these key-level metrics.**

**2. A low `matched_count` here is a TEST-DATA property, not a framework fault.** Independently
recomputing both hashes over the same `compare_columns` gives hash-equal pair counts of
**0 / 69 (subscriber)** and **1 / 64 (physical_device)** — precisely the reported `matched_count`.
The reason is the generator's mutation model: a mutated batch row is drawn with a different
`variant` seed, which redraws **every** non-key column, not a subset. A pair therefore counts as
matched only if *all* compare columns coincide, and the odds collapse as the column count grows:

| table | compare columns | matched |
|---|---|---|
| `physical_device` | 25 | 1 |
| `customer` | 87 | 26 |
| `subscriber` | 127 | 0 |

Confirmed by column-level diff on subscriber: **no column differs on all 69 pairs, and not one of
the 127 columns is identical across all of them.** The recon is reporting the data accurately.

> **If a future demo needs a visibly non-zero `matched_count` on every table**, change the
> *generator*, not the framework: mutate a small random subset of columns per mutated row instead
> of redrawing all of them. That is a fixture change, out of scope for this build.

### 17.10 Comment keys removed from both specs (2026-09-07, user decision)

**What changed.** Every `_`-prefixed author-comment key (20 in the streaming spec, 15 in the batch/recon spec) was removed
from `uc3_excalibur_streaming_cdc.json` and `uc3_excalibur_batch_recon.json`. Each spec now carries exactly **one** comment
key, a root-level `_about` header (use case, one-paragraph description, framework version, date, developer). The framework
has no non-underscore root attribute for a description (`ALLOWED_ROOT_KEYS` is exactly eight keys), so a single `_` key is
the minimum that still validates. Every rationale those keys held lives in UC3_MASTER_DOCUMENT.md (hash spec: section 10.2; dropped and
nulled columns: sections 11.2-11.3; Iceberg: section 10.3; reconciliation settings: section 7.2). Both specs were also
re-ordered to the key order of `onboarding_templates/pipeline_onboarding_template.json`, and `$schema` now points at the
schema relative to the specs' post-consolidation location under `BT_Usecase/UC3/onboarding/` (the previous relative path
no longer resolved after the 2026-09-06 move).

**Verified after the change:** both specs `PASSED` `spec_validator` and validate against `onboarding_spec.schema.json`
(Draft 2020-12); no `_` key remains except `_about`; every tag sits under `governance_tags` and nowhere else; the
dead `destination_config.compressed` key (a misspelling of `compression`, read by nothing) is replaced by `compression: "GZIP"`, preserving the evident intent -- the same correction the 0.0.4 release notes record for the reference spec.

**Why it matters for the record.** Statements elsewhere in this contract that a spec "carries" an audit comment key
(`_hash_specification`, `_dropped_source_columns`, `_iceberg_deferral`) describe the specs as they were during Phase B/C
and have been pointed at the master document above; they are not to be re-introduced.

---

## 18. Job 3 rebuilt on Lakeflow Connect (v0.0.7): binding contract items

**What changed.** The batch lane stopped reading dated CSVs from `/Volumes/.../uc_3/batch/` and now
reads the three tables the **Lakeflow Connect Oracle query-based connector** writes into
`{{catalog}}.oracle_excalibur_batch`. The rest of this section is binding: it supersedes section 1's
volume layout for the batch lane, section 9's reconciliation settings, and section 10's test-data
contract.

### 18.1 The flows are `transformation_flows`, and that is forced

| Contract item | Rule |
|---|---|
| **B1** | The three batch flows are `transformation_flows[]` entries with `source_inputs[].is_streaming: false`. **Do not convert them back to ingestion flows of any `target_type`.** The connector tables are MERGE-written (`scd_type: SCD_TYPE_1`; confirmed live by a `MERGE` at version 2 in their Delta history and by the `__ingestion_connector_primary_key` / `__ingestion_connector_cursor_columns` table properties), Delta refuses to stream a MERGE-written table (`DELTA_SOURCE_TABLE_IGNORE_CHANGES`), `skipChangeCommits` is refused framework-wide, and `engine/source_plane.py` plans **every** ingestion source request `want_stream=True` unconditionally with `_execute_reader` hard-rejecting a batch bind of an ingestion identity. A transformation flow's `is_streaming: false` is honoured verbatim and gives a real `spark.read.table(...)`. |
| **B2** | `target_type: "materialized_view"` + `target_config.cdc_load_strategy: "TRUNCATE_AND_LOAD"`. Full snapshot recompute per update. **Not** `APPEND`, and no `partition_columns`. |
| **B3** | Each flow's `transformation_sql` casts **every** business column to the Bronze-side type (Oracle `NUMBER` to `DOUBLE`, `DATE` to `TIMESTAMP`, `CHAR`/`VARCHAR2` to `STRING`) and forces the `Null(DF)=Y` columns to `NULL`. The casts are load-bearing: the reconciliation hashes values *and* types, so a type mismatch between the lanes reports every row as drifted. The forced NULLs preserve section 4's Drop/Null contract, which `data_standardization_sql` used to carry. |

### 18.2 Reconciliation

| Contract item | Rule |
|---|---|
| **B4** | `execution_mode: "pipeline_audit_only"` on all three flows. `"pipeline"` is illegal here: it streams its source for the L5 heal pulse, and the source is now this group's own `TRUNCATE_AND_LOAD` materialized view. V-CYC-7 enforces this at onboarding from v1.7.11 (it previously rejected *both* pipeline modes, which left no legal in-pipeline setting at all). |
| **B5** | **Audit-only registers no heal lane.** `005_lfj_uc3_excalibur_batch_recon` therefore carries three `heal_<table>` tasks running `notebooks/05_reconciliation/05_reconciliation_engine.py`, one per `reconciliation_id`, between `run_pipeline_update` and `observability_export`. **Removing them breaks the use case silently**: every run stays green, every metric is published, and not one correction is applied. |
| **B6** | `append_target_table` is `{{catalog}}.staging.oracle_excalibur_cdc`, the multiplexed Debezium landing table the 004 streaming lane consumes, and **not** `staging.<table>_stream`. Each flow's `transform_sql` reshapes the miss set into a complete Debezium envelope row (the 9 landing columns `destination, target_table, key, value, operation, source_position, idempotency_key, partition, headers`; verbatim Kafka-Connect `schema` block; UPPERCASE Oracle field names; `unix_millis()` for Connect `Timestamp` fields; `before: null`; `op: 'r'`; `source.scn = MAX(existing scn)+1`). This preserves section 9's "not a second MERGE" rule in its strongest form: a healed row re-enters through the same CDC engine as any real change. |
| **B7** | The **`customer`** target additionally carries `"filter_condition": "__END_AT IS NULL"`. `bronze.customer` is SCD2 and holds every historical version (observed: 40 rows, 30 current), and the matcher collapses duplicate keys `MATCHED > VALUE_DRIFT > MISSING`, so without the filter one stale closed version can mask real drift on the current row. `physical_device` and `subscriber` are SCD1 and need no filter. |
| **B8** | The onboarding preflight reports the advisory status `SHAPE_DEFINED_BY_TRANSFORM_SQL` on the `append_schema` check for these flows (v1.7.11). It is informational. **Verify the `SELECT` list by hand**: the append runs with `mergeSchema`, so a misspelt alias adds a column to the landing table rather than failing. |

### 18.3 Two one-time operational steps, required on any workspace that ran the CSV-era Job 3

| Contract item | Rule |
|---|---|
| **B9** | **Drop the three old `staging.<table>_batch` tables once, before the first v0.0.7 run.** They were `STREAMING_TABLE`s and are now materialized views, and **Lakeflow cannot convert a streaming table into a materialized view in place**: the update fails rather than rewriting the object. `DROP TABLE IF EXISTS <catalog>.staging.{physical_device,customer,subscriber}_batch;` then let the pipeline recreate them. |
| **B10** | **Re-onboard `uc3_excalibur_batch_recon.json` with the onboarding job's `prune_missing_flows=true`.** Changing a flow's *type* does not retire the old row: the three `df_uc3_<table>_batch_load` flows moved from `ingestion_flows` to `transformation_flows`, and their old `ingestion_flow_spec` rows stay `is_active = true` unless pruned, so the pipeline would build both shapes and the ingestion one would fail per B1. |

**Unchanged:** the job/pipeline names (`005_lfj_uc3_excalibur_batch_recon` driving
`006_ldp_uc3_excalibur_batch_recon`, the 005/006 mismatch deliberate per section 2), the dataflow
group id, the `dataflow_id`s, the run ordering (Job 2 before Job 3, never parallel), and the run
command `databricks bundle run uc3_batch_recon_job`.
