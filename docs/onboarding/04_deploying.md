# 4 · Deploying with DABs

## The order that matters

```bash
# 1. deploy -- this builds AND uploads the wheel; there is no separate publish step
databricks bundle deploy -t dev_metaflow -p dev_metaflow

# 2. onboard a spec
databricks bundle run onboarding_job -t dev_metaflow \
  --params spec_path=/Volumes/metaflow/framework/onboarding_specs/orders.json
```

There is no wheel-path variable to pass (the old `framework_wheel_path` variable and the hand-run
`build_and_upload_wheel.py` step are gone). `bundle deploy` runs `scripts/bump_and_build.py` via
the `artifacts.framework_wheel` block, which stamps a unique patch version so every deploy
produces a uniquely-named wheel, then uploads it to the target's `workspace.artifact_path` --
`/Volumes/<catalog>/config/wheels/.internal/` as of v1.6.0 (a UC Volume the bundle itself
declares, `volumes.framework_wheels_volume`). Resources reference the wheel as
`- ../../dist/*.whl`; DABs rewrites that to the uploaded path at deploy time.

### The UC container hierarchy is declared (v1.7.1)

`resources/metaflow_bootstrap/` declares the schemas every other resource hangs off, so a deploy
to a fresh workspace creates them itself:

| Resource | What |
|---|---|
| `schemas.config_schema` | `config` -- control tables, the wheels Volume, the onboarding-spec Volume |
| `schemas.working_schema` | `${var.schema}` |
| `schemas.sample_suite_schema` | `metaflow_sample` |
| `volumes.sample_{landing,exports,observability,configs}_volume` | the sample suite's four Volumes |

DABs rewrites each Volume's `schema_name` into a `${resources.schemas.*.name}` reference, so
create order **schema -> volume** is a real dependency edge, not a convention. Before this existed,
a first deploy died on
`cannot create resources.volumes.framework_wheels_volume: Schema 'metaflow.config' does not exist`.

All three schemas set `lifecycle.prevent_destroy: true` -- DABs deletes any resource missing from
the config, and these hold the control tables, the published wheels and the sample datasets.

!!! danger "The catalog is a prerequisite -- do NOT declare it as a resource"
    `${var.catalog}` must exist before the first deploy. Create it in the UI
    (**Catalog -> Create catalog -> Default storage**). A `catalogs.*` resource was tried on
    2026-09-02 and removed the same day: these accounts use UC **Default Storage**, so
    `CREATE CATALOG` without a `MANAGED LOCATION` is rejected with
    `Metastore storage root URL does not exist ... (400 INVALID_STATE)`, and a Default-Storage
    catalog's `storage_root` is an account-managed bucket path with generated UUIDs that cannot be
    committed to YAML. Worse, the failing catalog propagated **down** the dependency edge and took
    the schemas with it (`cannot create resources.schemas.config_schema: dependency failed`) --
    strictly worse than declaring no catalog. Full write-up in
    `resources/metaflow_bootstrap/README.md`; a unit test keeps it undeclared.

!!! warning "Bind, do not re-create, a schema that already exists"
    `metaflow.config` predates this group on `dev_metaflow` and `hoonartek`, so DABs does not own
    it there and a create attempt fails with `Schema 'config' already exists`. Bind once per
    workspace:
    ```
    databricks bundle deployment bind schemas.config_schema metaflow.config -t dev_metaflow -p dev_metaflow
    ```
    `bundle deployment unbind` reverses it without deleting anything. Not needed on `arjun_2` --
    the bundle created that schema on 2026-09-02 and owns it.

!!! note "One-time `artifact_path` bootstrap -- a *different* mechanism, and still required"
    Declaring the Volume does **not** satisfy the `artifact_path` check, because that check runs
    during config resolution, **before** any resource is created (`Error: volume
    <catalog>.config.wheels does not exist at workspace.artifact_path` -- re-verified on CLI
    v1.13.0 with the catalog and schema resources in place). What changed in v1.7.1 is that the
    bootstrap is now a deploy of declared resources instead of hand-run `CREATE` statements:

    0. create the catalog in the UI, if it does not exist yet;
    1. comment out that target's `artifact_path:` line -- safe, it is a workspace setting, not a
       resource, so unsetting it deletes nothing;
    2. `databricks bundle deploy -t <target> -p <profile> --select schemas.config_schema,volumes.framework_wheels_volume`
    3. uncomment the `artifact_path:` line and deploy normally.

    Once per workspace; `bundle validate` prints OK from then on. **Completed for `arjun_2` on
    2026-09-02** -- schemas and all six Volumes created, `artifact_path` restored, full deploy green.

!!! danger "Never deploy while a pipeline or test wave is running"
    Unique per-deploy filenames stop a redeploy overwriting the exact file a running update is
    installing -- but DABs **prunes** superseded artifacts from `<artifact_path>/.internal/`, on a
    UC Volume exactly as in the workspace. A deploy issued mid-update can therefore still delete
    the wheel that update is installing, killing it with `ENVIRONMENT_PIP_INSTALL_ERROR`. The only
    mitigation is operational: never `bundle deploy` while a pipeline or test wave is running
    (`metaflow_testing/TESTING_PLAN.md` §0).

## Targets

| Target | Workspace | Catalog |
|---|---|---|
| `dev` | `dbc-0f3a637e-dc15` | `poc` |
| `dev_metaflow` *(default)* | `dbc-2f6b7d4f-8c5b` | `metaflow` |

Each target pairs with its own CLI profile. Deploying with a mismatched profile fails
authentication rather than deploying somewhere unexpected.

## Verify

```bash
databricks bundle validate -t dev_metaflow
databricks bundle summary  -t dev_metaflow
```

## Bulk onboarding

Point the onboarding job at a directory to process every spec in it:

```bash
databricks bundle run framework_config_onboarding_job -t dev_metaflow \
  --params config_dir=/Volumes/metaflow/framework/onboarding_specs/
```

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `ENVIRONMENT_PIP_INSTALL_ERROR` | A deploy pruned the wheel a running update was installing | Never deploy mid-run; rerun the update after the deploy finishes |
| Auth failure on deploy | Target/profile mismatch | Use the profile that matches the target |
| App serves nothing at `/` | `web/dist/` not built | `npm run build` before deploying |
| Onboarding succeeds, pipeline unchanged | Pipeline not restarted | Onboarding writes rows; the pipeline reads them at start |
