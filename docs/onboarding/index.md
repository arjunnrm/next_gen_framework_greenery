# Get started

Short steps from an empty workspace to a running pipeline. Each ends with something you
can actually check.

<div class="grid cards" markdown>

- **1 · [Prerequisites](01_prerequisites.md)** — catalog, schemas, Volumes, grants. Once per environment.
- **2 · [Your first pipeline](02_first_pipeline.md)** — land a CSV, ingest it into Bronze.
- **3 · [Using the Spec Builder](03_spec_builder_app.md)** — author and validate without hand-writing JSON.
- **4 · [Deploying with DABs](04_deploying.md)** — publish the wheel, deploy, run onboarding.
- **5 · [New-workspace bootstrap](05_new_workspace_bootstrap.md)** — the first-time runbook for a workspace that has never held this bundle.
- **6 · [BT Digital POC deployment issues](06_bt_digital_poc_deployment_issues.md)** — the `bt_digital_poc` target: per-target attributes that must not be copied, orphans left by interrupted deploys, and the workspace storage fault blocking the Genie space.

</div>

## How the pieces fit

```
   you author                    the framework does
   ----------                    ------------------
   one JSON/YAML spec   ------>  onboarding job
        |                              |
        |                              v
        |                        control tables
        |                       (8 tables, keyed by
        |                        dataflow_group_id)
        |                              |
        |                              v
        +--------------------->  Lakeflow pipeline
                                 (graph compiled at runtime
                                  from the control tables)
```

Two consequences, and they explain most early confusion:

!!! warning "Onboarding and running are separate steps"
    Onboarding writes control-table rows. The pipeline reads those rows and builds its graph.
    **Editing a spec changes nothing until you re-run onboarding.**

!!! warning "`dataflow_group_id` is the identity of the whole document"
    Re-running onboarding with the same id updates in place. Changing it creates a second,
    independent set of rows — and the original flows keep running.

## Where things live

| Path | What it is |
|---|---|
| `databricks-app/` | Spec Builder app — FastAPI backend, React frontend |
| `databricks-app/templates/` | Template catalogue. Drop a `.json` here and the app lists it |
| `databricks-app/config/` | App config, attribute registry, validation rules |
| `resources/` | DAB resource definitions — jobs and pipelines |
| `src/` | The framework wheel |
| `docs/` | This documentation |

## Getting unstuck

- Unfamiliar attribute → [JSON reference](../reference/json/index.md)
- Pipeline runs but data is wrong → [Known limitations](../13_known_limitations_and_gotchas.md)
- General question → [FAQ](../faq.md)
