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

!!! note "One-time bootstrap on a workspace that has never held the Volume"
    The CLI refuses an `artifact_path` inside a Volume that does not exist yet -- even one the
    bundle itself declares (`Error: volume <catalog>.config.wheels does not exist`). Comment out
    that target's `artifact_path:` line in `databricks.yml` (safe -- it is a workspace setting,
    not a resource, so unsetting it deletes nothing), run
    `databricks bundle deploy --select volumes.framework_wheels_volume`, restore the line, and
    deploy normally. Once per workspace; `bundle validate` prints OK from then on.

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
