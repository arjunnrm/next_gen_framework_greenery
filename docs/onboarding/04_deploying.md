# 4 · Deploying with DABs

## The order that matters

```bash
# 1. publish the wheel to a Volume (NOT a bundle artifact -- see below)
python scripts/build_and_upload_wheel.py --profile <profile> --catalog metaflow

# 2. deploy, pinning the exact wheel the previous step printed
databricks bundle deploy -t dev_metaflow \
  --var="framework_wheel_path=/Volumes/metaflow/framework/wheels/nextgen_metadata_framework-<version>-py3-none-any.whl"

# 3. onboard a spec
databricks bundle run onboarding_job -t dev_metaflow \
  --params spec_path=/Volumes/metaflow/framework/onboarding_specs/orders.json
```

!!! danger "Why the wheel is not a bundle artifact"
    An `artifacts.python_artifact` block stamps a new version on every deploy, and DABs prunes the
    superseded wheel. A deploy issued while a Lakeflow update was running therefore **deleted the
    wheel that update was installing**, killing it with `ENVIRONMENT_PIP_INSTALL_ERROR`.

    A Volume never prunes. Publishing there and pinning an explicit version is what makes a redeploy
    safe while pipelines are running.

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
| `ENVIRONMENT_PIP_INSTALL_ERROR` | Wheel replaced mid-update | Pin an explicit version; never glob |
| Auth failure on deploy | Target/profile mismatch | Use the profile that matches the target |
| App serves nothing at `/` | `web/dist/` not built | `npm run build` before deploying |
| Onboarding succeeds, pipeline unchanged | Pipeline not restarted | Onboarding writes rows; the pipeline reads them at start |
