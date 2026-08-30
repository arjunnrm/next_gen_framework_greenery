# Test Pipeline: Reconciliation Feature Test (Scenario 004)

> See also: [Documentation index](README.md), [metaflow_testing/TESTING_PLAN.md](../metaflow_testing/TESTING_PLAN.md) (Module 7 & 11), [metaflow_testing/TESTING_STATUS.md](../metaflow_testing/TESTING_STATUS.md).

## Purpose

Prove the new **Design 2** reconciliation capabilities added this pass:
1. Conditional `reconciliation_run_log`/`reconciliation_mismatch_log` capture, gated per-flow by
   `logging_config.run_log_capture`/`mismatch_log_capture`.
2. The always-populated `reconciliation_result` summary table, independent of that gating.
3. `task_run_id` threading from a parent job into every log/result row for correlation.
4. `recon_mode: "continuous"` — a genuinely always-on streaming trigger, as opposed to today's
   default `"triggered"` (`availableNow`-drain) mode.

Hash key + hash value generation (`__framework_hash_key`/`__framework_hash_value`) is **not**
new — it already existed in `cdc/hashing.py`/`reconciliation/matcher.py` before this pass; this
scenario exercises it only incidentally (as a regression check), not as new functionality.

## Responsibilities

* Reuse scenario 002/003's own already-onboarded bronze tables
  (`{catalog}.bronze_excalibur.autoload_bronze` / `zerobus_bronze`) as a source/target pair —
  reconciliation is not a Lakeflow Declarative Pipeline flow type (see
  `metaflow_test_003_autoload_recon_pipeline.yml`'s own header comment), so this scenario needs
  no dedicated pipeline of its own, only onboarding + `05_reconciliation_engine.py` tasks.
* Three reconciliation flows, all comparing the same table pair, all `comparison_direction:
  "target_to_source"` (audit-only — deliberately never mutates anything, unlike scenario 003's
  own flow, which already exercises the self-healing append path):
  * `recon_004_logging_on` — `logging_config: {run_log_capture: true, mismatch_log_capture: true}`.
  * `recon_004_logging_off` — both `false`.
  * `recon_004_continuous` — target `read_mode: "streaming"`, run via the `recon_mode:
    "continuous"` widget.

## Inputs

* `metaflow_testing/004_recon_features_test.json` — reconciliation-only onboarding spec (no
  `ingestion_flows`/`transformation_flows`).
* Depends on scenario 002/003's own fixtures — `metaflow_test_004_recon_features_job` re-runs
  their full setup/seed/onboard/pipeline chain itself first, so this scenario is self-contained
  (works from a clean environment, not only after 002/003 happen to have already run).

## Outputs

* `{catalog}.config.reconciliation_run_log` — one new row for `recon_004_logging_on` only (not
  `_logging_off`, not `_continuous` unless it independently defaults logging on — it does, since
  its `logging_config` is both `true`).
* `{catalog}.config.reconciliation_mismatch_log` — rows for `recon_004_logging_on` (and
  `recon_004_continuous`) if any `VALUE_DRIFT`/`MISSING_IN_SOURCE` records exist between the two
  tables; **none** for `recon_004_logging_off`.
* `{catalog}.config.reconciliation_result` — **one row per reconciliation_id, every time**,
  regardless of the above — this is the "always populated" output file/table this scenario is
  specifically designed to prove.
* All rows carry the same `task_run_id` (the job's own `{{job.run_id}}`) for the three tasks run
  together in one job execution.

## Configuration

`metaflow_testing/004_recon_features_test.json`'s three `reconciliation_flows[]` entries (see
file for full detail) — each sets `logging_config` explicitly (even the default-`true` case, for
clarity in this test) and `match_keys: ["customer_id"]`, `compare_columns: ["customer_name",
"status"]`.

## Main execution flow

```bash
databricks bundle deploy --target dev
databricks bundle run metaflow_test_004_recon_features_job --target dev
```

Task chain: `setup_control_tables → seed_metaflow_testing_data → onboard_002 → run_002_pipeline
→ onboard_003 → run_003_pipeline → onboard_004 → {run_004_recon_logging_on,
run_004_recon_logging_off, run_004_recon_continuous}` (the last three run in parallel, all
depending only on `onboard_004`).

**`run_004_recon_continuous` does not finish on its own** — `recon_mode: "continuous"` puts its
one streaming target under an indefinite trigger (see `reconciliation/streaming.py`), so that
task will show as still-running in the Jobs UI rather than completing, exactly as designed. Stop
it manually (cancel the task/run) once you've confirmed it started successfully and its first
micro-batch committed — do not expect this job run to reach a terminal `SUCCESS` state on its own
while that task is included.

## Expected results — simulation notes

**SQL assertions** (after a run, replace `{catalog}` with your actual target catalog):

```sql
-- reconciliation_result: exactly 3 rows (one per reconciliation_id), all sharing task_run_id
SELECT reconciliation_id, task_run_id, status, matched_count, value_drift_count
FROM {catalog}.config.reconciliation_result
WHERE reconciliation_id IN ('recon_004_logging_on', 'recon_004_logging_off', 'recon_004_continuous')
ORDER BY run_at DESC LIMIT 3;

-- reconciliation_run_log: present for logging_on, ABSENT for logging_off
SELECT reconciliation_id, count(*) FROM {catalog}.config.reconciliation_run_log
WHERE reconciliation_id IN ('recon_004_logging_on', 'recon_004_logging_off')
GROUP BY reconciliation_id;
-- expect: recon_004_logging_on -> >=1 row; recon_004_logging_off -> 0 rows
```

**This pass's actual verification status**: see
[`metaflow_testing/TESTING_STATUS.md`](../metaflow_testing/TESTING_STATUS.md) for whether this
job was actually run against the live workspace and what the real result was — this document
describes what *should* happen structurally; the status tracker records what *did* happen, if
anything, each time this scenario is executed.

## Real live-run results (2026-08-29)

**`recon_004_logging_on` / `recon_004_logging_off` — fully verified live, exactly as designed.**
Real query results against `metaflow.config.*` after a live run (job run id `830403105449437`):

```
reconciliation_result:  recon_004_logging_on  -> SUCCESS, matched_count=7, task_run_id=830403105449437
                        recon_004_logging_off -> SUCCESS, matched_count=7, task_run_id=830403105449437
reconciliation_run_log: recon_004_logging_on  -> 1 row  (SUCCESS, task_run_id=830403105449437)
                        recon_004_logging_off -> 0 rows (correctly suppressed)
```

**`recon_004_continuous` — code path executes correctly, but blocked by a real Databricks
platform constraint, not a bug in this session's code.** Running it live surfaced:
`[INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED] Trigger type ProcessingTime is not supported for
this cluster type. Use a different trigger type e.g. AvailableNow, Once.` Root cause: Spark
Structured Streaming's own *default* trigger (used whenever `.trigger(...)` is never called —
which is what `continuous=True, processing_time=None` in `reconciliation/streaming.py` produces)
is itself a `ProcessingTime` trigger under the hood — and Databricks **serverless job compute**
(the `environment_key: framework_env` every job in this project already uses) rejects *any*
unbounded streaming trigger outright, whether explicit or default. This is a platform-level
restriction, not something fixable by passing a different `processing_time` value — the error
message's own wording ("not supported for this cluster type") implies a non-serverless (classic)
cluster would allow it, which this project doesn't use anywhere today.

What this run *did* prove working correctly: `run_streaming_target_reconciliation(...,
continuous=True)` reaches the intended code path and genuinely attempts the continuous trigger
(not silently falling back to `availableNow`); the failure propagated up cleanly through
`streaming.py`'s own `except` handling into `05_reconciliation_engine.py`'s outer error handler;
because this flow's `error_handling.on_failure` is `"warn"`, the job task still completed
`SUCCESS` overall rather than hard-failing; and — the actual thing this scenario exists to
prove — **both** `reconciliation_run_log` (`run_log_capture: true`) *and*
`reconciliation_result` (always-on) received a `FAILED` row with the full real error message,
confirming the failure-path logging/task_run_id threading works exactly like the success path.

**Open follow-up, not fixed in this pass**: making `recon_mode: "continuous"` actually run
long-lived requires either a classic (non-serverless) cluster for that one job task, or
restructuring continuous reconciliation to run inside a genuine Lakeflow `continuous: true`
pipeline (the same mechanism Design 1 uses) rather than a plain serverless job task — a real
architectural decision, not a quick patch.

**Incidental fixes made to unblock this verification** (pre-existing bugs, not introduced this
session, but found and fixed while getting a real end-to-end run):
* `notebooks/00_seed_sample_data/02_seed_metaflow_testing_data.py`'s `seed_zerobus_style_table_from_csv()`
  used `whenMatchedUpdateAll()`/`whenNotMatchedInsertAll()` (star merge), which fails the moment
  the target table has a column the fixture CSV doesn't (confirmed live: `zerobus_source_bus`
  had accumulated an extra `updated_at` column from scenario 003's own self-healing appends).
  Fixed to scope both clauses to `source_df.columns`, matching this codebase's own established
  dynamic-column MERGE pattern (`onboarding/metadata_upsert.py`).
* `onboarding/metadata_upsert.py`'s five control-table upsert functions all used
  `whenNotMatchedInsertAll()` — the same star-insert problem, confirmed live the moment a new
  nullable column (`last_processed_cdc_version`, `logging_config_json` — both added earlier this
  session) existed on an already-provisioned control table and a genuinely new row was inserted.
  Fixed all five to `whenNotMatchedInsert(values={col: f"s.{col}" for col in source_df.columns})`,
  consistent with their own `whenMatchedUpdate` clauses.
* This workspace's `metaflow.config.*` control tables (provisioned before this session) needed
  four manual `ALTER TABLE ... ADD COLUMNS` statements run once to pick up new columns added
  this session (`reconciliation_run_log`/`reconciliation_mismatch_log`.`task_run_id`,
  `reconciliation_flow_spec`.`logging_config_json`, `ingestion_flow_spec`/
  `transformation_flow_spec`.`last_processed_cdc_version`) — `CREATE TABLE IF NOT EXISTS` never
  retroactively adds a column to an existing table, exactly as flagged when those columns were
  first added. A fresh catalog would not need this step.
