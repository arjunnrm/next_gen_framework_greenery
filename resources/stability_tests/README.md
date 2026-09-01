# `resources/stability_tests/` — reserved, not yet built

This folder holds the bundle resources for
[`metaflow_testing/STABILITY_TEST_PLAN.md`](../../metaflow_testing/STABILITY_TEST_PLAN.md) —
the plan that asks *"does feature X produce the **same** answer every time"* by running each
case 4–5 times under a fixed protocol, as opposed to `TESTING_PLAN.md`'s one-run-per-feature
`TC-*` corpus (whose resources live in [`../feature_tests/`](../feature_tests/)).

**It is empty on purpose.** None of the plan's A1–G4 cases has a resource YAML yet.
`databricks.yml` still includes `resources/stability_tests/*.yml`, which matches nothing today
and is accepted by DABs; the include line is already in place so that adding the first
stability resource needs no change to `databricks.yml`.

## When you add the first one

The plan's §1.1 makes three settings non-negotiable for every resource in this folder, and each
is a setting whose *default* would silently invalidate the results:

| Layer | Required setting | Why the default ruins the measurement |
|---|---|---|
| Pipeline | `configuration: {"pipelines.maxFlowRetryAttempts": "0"}` | Defaults to **5** for triggered pipelines — a flow that fails transiently is retried and the update still reports SUCCESS, erasing exactly the instability being measured |
| Job task | `max_retries: 0` on **every** task | A seed/onboard task that only succeeds on attempt 2 is a finding, not a pass |
| Pipeline | `development: true`, `continuous: false` | Triggered updates only; also suppresses automatic update restart |

Object footprint is fixed by §2 (4 schemas — `config`/`land`/`bronze`/`silver` — and 4 volumes,
isolating cases by *folder*, never by new schema or volume). Do not let a new stability resource
create a schema or volume pair per case: that sprawl is what exhausted the UC volume quota on
the previous workspaces.

Name resources `metaflow_stab_<case>_<slug>_{job,pipeline}.yml` (e.g.
`metaflow_stab_g4_update_in_place_job.yml`) so a stability resource is never mistaken for a
`TC-*` feature test in the Jobs UI.
