# BT Digital POC — Deployment Issues

Deployment record for the `bt_digital_poc` target
(workspace `8259551512366201.1.gcp.databricks.com`, catalog `bt_digital_poc`),
first deployed **2026-09-13**.

**Status: 28 of 29 resources deployed and verified.** The Genie space is the one
outstanding resource, and it is blocked by a **platform fault in the BT workspace**,
not by anything in this repo. Details in [§4](#4-blocker-cloud-provider-storage-unavailable).

---

## 1. Target configuration — three attributes that must not be copied from another target

The `bt_digital_poc` target was initially added by copying an existing target block.
All three of the following failed the deploy, and all three fail *silently at validate
time or loudly at deploy time* rather than being caught by review:

- **`schema: dev`, never `config`.**
  `${var.schema}` is the framework's **working** schema (where job data lands). It is a
  different thing from the literal `config` schema that `schemas.config_schema` declares
  (control tables + the wheels/spec Volumes). Setting `schema: config` makes two bundle
  resources create the same schema and the deploy dies with:
  ```
  Error: cannot create resources.schemas.working_schema:
         Schema 'config' already exists (400 SCHEMA_ALREADY_EXISTS)
  ```
  This also fails the dependent Volume: `cannot create resources.volumes.framework_docs_volume:
  dependency failed: resources.schemas.working_schema`.
  - **Note:** the `hoonartek` target still carries `schema: config` and therefore still
    carries this collision.

- **`dashboard_warehouse_id` is per-workspace.**
  The value inherited from `metaflow_v7` (`3942c176e8c4dee8`) names a warehouse that does
  not exist in the BT workspace. Set to `ff89f2ac8656a792`
  (`bt_df_sql_warehouse_serverless`). Find it with `databricks warehouses list -p bt_digital_poc`.

- **`genie_space_file` must match the catalog.**
  DABs inlines this file into `serialized_space` **verbatim** and does **not** substitute
  `${var.catalog}` inside it, so the catalog is baked into each rendering. The default
  rendering targets the `flowx` catalog. This target uses
  `../../databricks-genie/flowx_observability.bt_digital_poc.geniespace.json`, which
  already existed (created for `hoonartek`, same catalog name) and was verified to contain
  zero `flowx.`-qualified references.

## 2. Fresh-workspace bootstrap was required

The workspace had never held this bundle: no `config` schema, no `wheels` Volume. The
`artifact_path` pre-check runs during *config resolution*, before any resource is created,
so the Volume declared in this same bundle cannot satisfy it. Ran the documented two-phase
bootstrap (see [05_new_workspace_bootstrap.md](05_new_workspace_bootstrap.md)):

```bash
python scripts/bootstrap_workspace.py -t bt_digital_poc -p bt_digital_poc
```

- Phase 1 created `schemas.config_schema` + `volumes.framework_wheels_volume` with
  `wheels_root` overridden away from the Volume.
- Phase 2 deployed the rest; the wheel landed at
  `/Volumes/bt_digital_poc/config/wheels/0.0.6/`.
- The catalog itself is a **prerequisite** and was already present — the script does not
  create it.

> **Caveat on the bootstrap script's exit code.** `scripts/bootstrap_workspace.py` exited
> **0** even though its own final line read `ERROR: the full deploy failed`. Do not treat a
> zero exit from this script as success — read its output, or check
> `databricks bundle summary`.

## 3. Orphans left by network-interrupted deploys

The first deploy attempt hit repeated client-side network drops
(`wsarecv: An existing connection was forcibly closed by the remote host`, and one
`dial tcp: no such host`). Several resources were **created server-side but never recorded
in DABs state**, because the connection died between the create call and the state write.
This leaves orphans that the bundle will never manage or clean up:

- **App `flowx-onboarding`** — existed but was untracked, so the next deploy tried to
  create it again and failed with `409 ALREADY_EXISTS`. Fixed by binding, not re-creating:
  ```bash
  databricks bundle deployment bind flowx_onboarding_app flowx-onboarding -t bt_digital_poc -p bt_digital_poc
  ```
  Note the key is **bare** (`flowx_onboarding_app`), not `apps.flowx_onboarding_app` —
  the prefixed form returns `Error: no such resource`.

- **Duplicate job `005_lfj_uc3_excalibur_batch_recon`** — created twice. Both copies
  self-report `deployment.kind: BUNDLE`, so *that field cannot be used to tell them apart*.
  The authoritative check is the deployment state file:
  ```bash
  databricks workspace export "/Workspace/Users/<you>/.bundle/flowx/bt_digital_poc/state/resources.json" -p bt_digital_poc
  ```
  (CLI v1.13.0 uses `resources.json`; there is no `terraform.tfstate`.) The tracked id was
  `49850736416296`; the orphan `414751375246576` was deleted. A sweep found no other
  duplicate jobs and no duplicate pipelines.

**Recommendation:** on a flaky link, deploy in smaller `--select` batches and re-run
`databricks bundle summary -t <target>` afterwards to spot resources that exist but are
untracked.

## 4. BLOCKER — `Cloud provider storage unavailable`

The Genie space cannot be deployed until the 13 `bt_digital_poc.observability.*` views
exist; the Genie API validates every table at create time and rejects the space otherwise:

```
Error: cannot create resources.genie_spaces.flowx_observability_genie_space:
  Failed to fetch tables for the agent ... Table 'bt_digital_poc.observability.v_flow_metrics'
  does not exist ... (403 PERMISSION_DENIED)
```

Those views are created by `notebooks/01_setup/01_setup_control_tables.py` (section 5).
**That notebook cannot currently run in this workspace.** Two attempts, ~6 minutes apart:

| Run ID | Result | Message |
|---|---|---|
| `289243458832753` | `INTERNAL_ERROR` / `FAILED` | Cloud provider storage unavailable |
| `226790777794862` | `INTERNAL_ERROR` / `FAILED` | Cloud provider storage unavailable |

### Evidence that this is a platform fault, not a framework defect

- **No Python traceback.** `jobs get-run-output` returns
  `Run failed with error message\n Cloud provider storage unavailable` with an **empty**
  `error_trace`. The notebook never reached user code — the failure is in compute
  provisioning, before execution.
- **An unrelated team's job fails identically.** Run `996683975989971`, `copy-files`,
  owned by `gopi.siripurapu@bt.com` — nothing to do with FlowX — failed at
  **2026-09-13 15:59**, hours before any FlowX activity, with the same
  `Cloud provider storage unavailable`. The condition pre-dates this deployment.
- **Serverless SQL fails on a query touching no user table.** `SELECT 1` returns:
  ```
  BAD_REQUEST [QUERY_RESULT_WRITE_TO_CLOUD_STORE_FAILED] An internal error occurred
  while uploading the result set to the cloud store. SQLSTATE: XX000
  ```
  The result-set upload is the failing step, so the fault is in the **write path to
  managed cloud storage**.
- **UC metadata and Volume writes are healthy.** `schemas list` / `tables list` respond
  normally, and `databricks fs cp` into `/Volumes/bt_digital_poc/config/wheels/` succeeded
  (probe file written and removed). The wheel uploaded fine throughout. So this is *not*
  a blanket storage outage — it is specific to **managed/system storage writes used by
  compute**, while the Volume path works.

### Configuration detail for whoever investigates

```
catalog        : bt_digital_poc
storage_root   : gs://databricks-bt-catalog/catalog/bt_digital_poc
isolation_mode : ISOLATED
```

- The catalog uses a **customer-managed GCS bucket**, not UC Default Storage.
- `databricks external-locations list` and `databricks storage-credentials list` both
  return **empty** in this workspace.
- Both the `bronze` and `config` schemas contain **zero tables** — consistent with a
  workspace where managed-storage writes have not been succeeding.

**This needs a Databricks workspace admin / BT platform team**, not a change in this repo.
Likely areas: the GCS service-account permissions on `gs://databricks-bt-catalog`, or the
bucket/VPC-SC configuration for the serverless compute plane. The fix is out of scope for
the framework.

## 5. Remaining steps once the platform fault is cleared

1. Create the control tables and observability views:
   ```bash
   databricks bundle run framework_config_onboarding_job -t bt_digital_poc -p bt_digital_poc \
     --only setup_control_tables --params catalog=bt_digital_poc
   ```
   The `catalog` parameter **must** be overridden — the job's default is `flowx`, which
   does not exist in this workspace.
2. Confirm the 13 views exist:
   `databricks tables list bt_digital_poc observability -p bt_digital_poc`
3. Deploy the Genie space:
   ```bash
   databricks bundle deploy -t bt_digital_poc -p bt_digital_poc \
     --select genie_spaces.flowx_observability_genie_space
   ```

## 6. Operational notes for this workspace

- **`bundle run` reporting an error does not mean the operation failed.** The app
  deployment logged
  `Error: Get .../deployments/01f1af87827c10ce8e4537d26c0ce9e1: wsasend: An existing
  connection was forcibly closed` — yet that exact deployment id is now the **active**
  one, `SUCCEEDED`, with the app `ACTIVE`. Always confirm server-side
  (`databricks apps get`, `databricks jobs get-run`) before retrying; a blind retry here
  would have redeployed a working app.
- Use `--no-wait` on `bundle run` in this workspace to avoid the long-poll connection
  drops, then poll `jobs get-run` separately.
- **The app needs two steps.** `bundle deploy` uploads source but leaves the app on its
  previous code; `databricks bundle run flowx_onboarding_app` is required. Rebuild
  `databricks-app/web` (`npm run build`) **before** deploying — Databricks Apps does not
  build at deploy time.
- CLI v1.13.0 argument shapes that differ from older docs:
  `databricks jobs get <JOB_ID>` (positional, **no** `--job-id`), and
  `bundle deployment bind <bare_key> <id>`.
- In Git Bash, prefix workspace-path commands with `MSYS_NO_PATHCONV=1` or the leading `/`
  is rewritten into a Windows path.

---

## Deployed inventory (verified 2026-09-13)

| Kind | Count | Notes |
|---|---|---|
| Schemas | 3 | `config`, `dev`, `flowx_sample` |
| Volumes | 7 | incl. `config.wheels` holding `flowx-0.0.6` |
| Jobs | 12 | all created; duplicate orphan removed |
| Pipelines | 4 | all `IDLE` |
| Dashboards | 2 | |
| Apps | 1 | `ACTIVE`, deployment `SUCCEEDED` |
| **Genie spaces** | **0 of 1** | **blocked — see §4** |

App URL: <https://flowx-onboarding-8259551512366201.gcp.databricksapps.com>
