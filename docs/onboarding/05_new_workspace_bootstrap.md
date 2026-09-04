# 5 · Deploying to a brand-new workspace

A step-by-step runbook for standing FlowX up on a workspace that has **never held this bundle**.
Follow it top to bottom; every step ends with something you can check.

Deploying to a workspace that is *already* running FlowX is the ordinary
[4 · Deploying with DABs](04_deploying.md) loop — come here only for the first time.

!!! tip "The whole runbook in one command"
    Steps 3–6 are automated:

    ```bash
    python scripts/bootstrap_workspace.py -t <target> -p <profile> --run-app
    ```

    Add `--dry-run` first to see the exact commands without changing anything. The script is
    idempotent — re-running it on a bootstrapped workspace skips straight to a normal deploy.
    The manual steps below are what it does, and are worth reading once so you can debug it.

---

## Step 0 · What you need before you start

| Requirement | Check |
|---|---|
| Databricks CLI **≥ v1.13.0** (`--select` needs it) | `databricks --version` |
| An authenticated profile for the new workspace | `databricks auth profiles` |
| Permission to create catalogs, schemas and volumes in UC | — |

If the profile is missing:

```bash
databricks auth login --host https://<workspace-host> --profile <profile>
```

---

## Step 1 · Create the catalog (UI, once per workspace)

**In the UI: Catalog → Create catalog → Default storage.** Name it to match the target's
`${var.catalog}` (`flowx` on every current target).

!!! danger "The catalog is a prerequisite, not a bundle resource"
    Do not try to add a `catalogs.*` resource. It was tried on 2026-09-02 and removed the same
    day: these accounts use UC **Default Storage**, so `CREATE CATALOG` without a
    `MANAGED LOCATION` is rejected (`Metastore storage root URL does not exist … 400
    INVALID_STATE`), and a Default-Storage catalog's `storage_root` is an account-managed bucket
    path with generated UUIDs that cannot be committed to YAML. Worse, the failing catalog
    propagated **down** the dependency edge and took the schemas with it. Full write-up in
    `resources/flowx_bootstrap/README.md`; a unit test keeps it undeclared.

**Check:** `databricks catalogs list -p <profile>` lists your catalog.

---

## Step 2 · Add the target to `databricks.yml`

Copy an existing target block and change the host, profile and warehouse:

```yaml
  <target_name>:
    workspace:
      host: https://<workspace-host>/
      profile: <profile>
      artifact_path: ${var.wheels_root}/${var.framework_version}

    variables:
      catalog: flowx
      schema: dev
      dashboard_warehouse_id: <id from the command below>
```

!!! warning "`dashboard_warehouse_id` is per workspace — never copy it between targets"
    Warehouse ids are not portable. A copied id points at a warehouse that does not exist on the
    new workspace, and the AI/BI dashboard resource fails at deploy. Get the real one:

    ```bash
    databricks warehouses list -p <profile>
    ```

Leave `artifact_path` exactly as written — it composes from `${var.wheels_root}`, which is what
makes Step 3 a flag rather than a file edit.

**Check:** the target appears in `databricks bundle validate -t <target> -p <profile>` output —
it will still fail on the volume pre-check, which Step 3 fixes.

---

## Step 3 · Bootstrap the wheels Volume

A first `validate`/`deploy` fails like this:

```
Error: volume flowx.config.wheels does not exist
  at workspace.artifact_path
     resources.volumes.framework_wheels_volume
```

**Why:** every target publishes the framework wheel into a UC Volume, and the bundle declares
that Volume itself — but the `artifact_path` check runs during *config resolution*, **before any
resource is created**. So the Volume that would satisfy the check cannot be created by the deploy
that the check is rejecting. A genuine chicken-and-egg.

**The fix — one deploy with `wheels_root` pointed somewhere harmless:**

```bash
databricks bundle deploy -t <target> -p <profile> \
  --select schemas.config_schema,volumes.framework_wheels_volume \
  --var="wheels_root=/Workspace/Users/<your-email>/.bundle/flowx/<target>/artifacts"
```

That override applies to this **one command only**. It moves the wheel out of the Volume just
long enough for the Volume to be created; `--select` keeps the deploy to the schema and the
Volume, so nothing else is touched.

**Check:** `Created schemas.config_schema` and `Created volumes.framework_wheels_volume`, then

```bash
databricks bundle validate -t <target> -p <profile>     # now prints Validation OK!
```

!!! note "Once per workspace"
    From here on, `wheels_root` stays at its default and every deploy is ordinary. Never commit
    an override — the fixed, shared Volume path is what makes a deploy reproducible regardless of
    who runs it (the v1.6.0 decision record in `databricks.yml`).

!!! info "Older instructions said to comment out `artifact_path:` — don't"
    Before v1.7.4 this step meant hand-editing each target's `artifact_path:` line out and back
    in. The `--var` override replaces it: no file is edited, so there is no half-finished edit to
    commit by mistake.

---

## Step 4 · Build the app frontend

```bash
cd databricks-app/web && npm run build
```

!!! danger "Databricks Apps does not build at deploy time"
    An un-rebuilt `web/dist/` keeps serving the previous UI — including fields the framework may
    now reject. This step is not optional.

**Check:** `vite build` reports `✓ built`, and `databricks-app/web/dist/` is freshly written.

---

## Step 5 · Full deploy

```bash
databricks bundle deploy -t <target> -p <profile>
```

**Check:** the summary lists the created resources — on a fresh workspace, 2 jobs, 1 app,
1 dashboard, 3 schemas and 6 volumes. Confirm the wheel landed:

```bash
databricks fs ls dbfs:/Volumes/<catalog>/config/wheels/<framework_version>/.internal -p <profile>
```

!!! danger "Never deploy while a pipeline or test wave is running"
    `bundle deploy` prunes superseded artifacts from `<artifact_path>/.internal/` — on a UC Volume
    exactly as in the workspace — so a deploy issued mid-update kills it with
    `ENVIRONMENT_PIP_INSTALL_ERROR`. Per-version directories mean a deploy can only disturb *its
    own* release, but that is still the release a running pipeline may be installing. Use
    `--fail-on-active-runs` to enforce it mechanically. See `flowx_testing/TESTING_PLAN.md` §0.

---

## Step 6 · Start the app

```bash
databricks bundle run flowx_onboarding_app -t <target> -p <profile>
```

!!! warning "Deploying the app is two steps"
    `bundle deploy` uploads the source but leaves the **running app on its previous code**. On a
    brand-new workspace, skipping this leaves an app that has never started at all.

**Check:** the command prints `App started successfully` and its URL. Confirm:

```bash
databricks apps get flowx-onboarding -p <profile>   # state: RUNNING, compute: ACTIVE
```

---

## Step 7 · Create the control tables, then onboard

Deploying creates the *infrastructure*; the framework's 8 control tables are created by running
the setup job, and pipelines do nothing until a spec is onboarded.

```bash
# 1. control tables
databricks bundle run framework_config_onboarding_job -t <target> -p <profile>

# 2. onboard a spec
databricks bundle run onboarding_job -t <target> -p <profile> \
  --params spec_file_path=/Volumes/<catalog>/config/onboarding_specs/<spec>.json,action_type=CREATE
```

**Check:** the control tables exist and hold your `dataflow_group_id`:

```bash
databricks experimental aitools tools query \
  "SELECT dataflow_group_id, is_active FROM <catalog>.config.dataflow_spec" -p <profile>
```

!!! warning "Onboarding and running are separate"
    Onboarding writes control-table rows; the pipeline reads them to build its graph. **Editing a
    spec changes nothing until you re-run onboarding.**

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `volume … does not exist at workspace.artifact_path` | Step 3 not done on this workspace | Run the Step 3 bootstrap deploy |
| `Schema 'flowx.config' does not exist` | Catalog missing, or Step 3 skipped | Step 1, then Step 3 |
| `Schema 'config' already exists` | Schema predates the bundle, so DABs does not own it | `databricks bundle deployment bind schemas.config_schema <catalog>.config -t <target> -p <profile>` |
| Dashboard fails with an invalid warehouse | `dashboard_warehouse_id` copied from another target | Step 2 — get the id from `databricks warehouses list` |
| App serves old UI after a deploy | `web/dist/` not rebuilt, or `bundle run` skipped | Steps 4 and 6 |
| `ENVIRONMENT_PIP_INSTALL_ERROR` | Deployed while a pipeline was running | Re-deploy when idle; use `--fail-on-active-runs` |
| `Cannot create 1 Volume(s) … limit: 50` | Metastore at its Volume ceiling | Free a Volume slot; see the LIVE CAVEAT in `databricks.yml` |
| `Invalid update mask … forward_user_access_token` when re-deploying the app | CLI/Apps-API mismatch: the CLI sends a field the *update* endpoint rejects (it is accepted on *create*, so the first deploy succeeds) | Not caused by this bundle. Deploy the rest with `--select` excluding `apps.flowx_onboarding_app`, and update the app's code with `databricks bundle run flowx_onboarding_app` — which re-uploads source without touching the app definition. A newer CLI fixes it. |

!!! danger "Do not scope a deploy by commenting out an `include:` line"
    DABs treats a resource absent from the config as one to **delete from the target**, so
    commenting out `resources/feature_tests/*.yml` to "skip the tests" destroys 83 deployed jobs
    and pipelines on the next deploy. Use `--select`, which leaves unselected resources untouched.
