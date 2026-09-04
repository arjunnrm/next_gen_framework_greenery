# Deployment Guide

See also: [README.md](README.md) for the full FlowX documentation index.

Uses [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
(DAB) — `databricks.yml` at the repo root, resource definitions under `resources/`.

## 0. Prerequisites

* [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) installed and
  authenticated (`databricks configure`, or `databricks auth login`).
* [`uv`](https://docs.astral.sh/uv/getting-started/installation/) installed (used to build
  the wheel — see `databricks.yml`'s `artifacts.python_artifact.build: uv build --wheel`).
* A Unity Catalog catalog you can create schemas/[volumes](https://docs.databricks.com/aws/en/connect/unity-catalog/volumes)/tables in.
* **Manual, admin-audited step — not automated by this framework**: create the classic
  (workspace-level) secret scope referenced by `test_specs/*.json` -- **not** a Unity
  Catalog secret; see `crypto/secrets.py`'s module docstring for why the SQL `secret()`
  function this framework's encryption relies on only resolves classic scopes:

  ```bash
  databricks secrets create-scope security
  # aes_encrypt/aes_decrypt require the key to be EXACTLY 16, 24, or 32 raw bytes --
  # NOT a base64-encoded 32-byte value (which is 44 characters/bytes as a string, and
  # fails with INVALID_PARAMETER_VALUE.AES_KEY_LENGTH). `openssl rand -hex 16` produces
  # 32 hex characters == 32 bytes when stored as a string -- a valid AES-256 key.
  databricks secrets put-secret security pii_encryption_key --string-value "$(openssl rand -hex 16)"
  # ZIP passphrases have no such byte-length constraint (pyzipper derives its own key
  # from the passphrase via its own KDF) -- any string works.
  databricks secrets put-secret security cdr_zip_passphrase --string-value "<your passphrase>"
  databricks secrets put-secret security egress_zip_password --string-value "<your password>"
  ```

  See [Secrets](https://docs.databricks.com/en/security/secrets/index.html) for scope
  ACLs and other provisioning options. Provisioning encryption key material is
  deliberately kept a manual action.

* **Test-only, only if running `crypto_abac_exhaustive_test_job`** (see
  [20_crypto_abac_exhaustive_test_suite.md](20_crypto_abac_exhaustive_test_suite.md)) --
  two additional secrets in the *same* `security` scope, clearly test-only, never real key
  material:

  ```bash
  # A second, different, VALID-length key -- used only to prove decrypting with the wrong
  # key fails/returns garbage rather than silently succeeding.
  databricks secrets put-secret security pii_encryption_key_wrong_test --string-value "$(openssl rand -hex 16)"
  # Deliberately the WRONG length (44 raw chars, not 16/24/32) -- used only to prove
  # aes_encrypt actually rejects it with INVALID_PARAMETER_VALUE.AES_KEY_LENGTH.
  databricks secrets put-secret security pii_encryption_key_badlength --string-value "$(openssl rand -base64 32)"
  ```

## 1. How the wheel gets built and used

`pyproject.toml` declares package `flowx` (hatchling, `src/` layout).
`src/flowx/lakeflow_framework/` — this entire framework — is part of
that package, so **no extra packaging config was needed**: `uv build --wheel` produces one
wheel containing everything under `src/`.

`databricks bundle deploy` doesn't call `uv build --wheel` directly — `databricks.yml`'s
`artifacts.python_artifact.build` points at `scripts/bump_and_build.py`, a small wrapper
that runs on every deploy, in order: (1) moves any wheel(s) already in `dist/` into
`dist/archive/` (never deletes them), (2) stamps `pyproject.toml`'s patch version to the
current UTC epoch-milliseconds, (3) runs `uv build --wheel`. This guarantees every deploy
produces a **new, uniquely-named wheel file** rather than overwriting the previous one —
every resource's `environment.dependencies: [../dist/*.whl]` glob only ever matches the one
just built. This matters because a plain repeated `uv build --wheel` at an unchanged
version overwrites the *same* wheel filename both locally and at the workspace path it gets
uploaded to; if a deploy happens while another pipeline/job update is still running or
restarting, that run could end up reading a corrupted or unexpectedly different file under
a path it already resolved. Old wheels are never deleted, only moved to `dist/archive/`
(already outside git, same as `dist/` itself) — prune that folder by hand whenever
convenient, it's pure local disk hygiene with no effect on deployed resources.

Every notebook and the pipeline resource attach that wheel as a library dependency
(`environment.dependencies: [../dist/*.whl]` — the pattern already used by the template's
own `resources/sample_job.job.yml`), so in a deployed job/pipeline,
`import flowx.lakeflow_framework...` resolves like any other
site-packages import — no `sys.path` tricks. Each notebook's bootstrap cell *does* still
carry a local-`src/`-folder fallback purely for convenience when iterating on a notebook
interactively before running `bundle deploy` at all; production never exercises that path.

### A bug fix in a streaming source's code needs `--full-refresh`, not just a redeploy

Confirmed live, the hard way, while fixing the `{{catalog}}` substitution bug in
`asn1/decoder.py` (below): a code fix that changes how a **streaming** ingestion flow
(`autoloader`/Auto Loader-backed, or any `create_streaming_table` target) processes its
source rows does **not** retroactively apply to files Auto Loader has already ingested and
checkpointed. `databricks bundle deploy` + a plain `databricks bundle run <pipeline>`
correctly picks up the new code, but a streaming source with no *new* files to read simply
has nothing to (re)process — the already-quarantined rows from the broken run just sit
there unchanged, and it's easy to misread that as "the fix didn't take effect" (two
red herrings were chased here before landing on the real cause: a stale-wheel-caching
theory, and a warm-process/stale-import theory — both plausible-sounding, both wrong).
The actual fix is `databricks bundle run <pipeline> --full-refresh-all` (or
`databricks pipelines start-update --full-refresh` for a single pipeline), which resets
Auto Loader's checkpoint and reprocesses every source file from scratch under the new
code. A brand-new pipeline's very first run is naturally unaffected (nothing was
checkpointed yet), which is why this class of bug is easy to miss during initial
development and only bites when fixing something that was already ingested once.

Two secondary things worth knowing, surfaced while chasing this down:
* `pyproject.toml`'s `version` needs to change between deploys for `databricks bundle
  deploy` to report pipeline resources as `changed` rather than `unchanged` — not because
  environments are content-cached (they aren't, in the end this wasn't the cause of the
  stale result above), but because the bundle tool's own change-detection keys off the
  resolved dependency string (`../dist/*.whl`'s matched filename), and an unchanged version
  means an unchanged filename/string across deploys either way. This is now automatic —
  `scripts/bump_and_build.py` (see §1) stamps a new version on every deploy — so there's
  nothing to remember here anymore.
* On this workspace's free-tier serverless compute quota, `--full-refresh-all` runs
  triggered back-to-back on two different pipelines can independently hit
  `RESOURCE_EXHAUSTED` (the same limited-concurrent-compute constraint noted in §4 below)
  — run them sequentially, not concurrently, if you hit this.

## 2. Deploy

```bash
# Builds the wheel and deploys everything under resources/ to your dev workspace
databricks bundle deploy

# Point the sample pipeline at a different dataflow_group_id than the default
# (dfg_iot_telemetry_unified, the unified dual-engine demo):
databricks bundle deploy --var="dataflow_group_id=dfg_finance_txn_ingest"

# Deploy to prod
databricks bundle deploy --target prod
```

This deploys two resources (see `resources/*.yml`):

* **`metadata_lakeflow_pipeline`** (`lakeflow_metadata_pipeline.yml`) — the Lakeflow
  Declarative Pipeline itself, configured via `dataflow.group.id`/`dataflow.control.catalog`
  pipeline configuration values (themselves driven by the `dataflow_group_id`/`catalog`
  bundle variables).
* **`metadata_framework_job`** (`metadata_framework_job.yml`) — an end-to-end test job:
  `setup_control_tables → seed_sample_data → onboard_spec_01..06 (parallel) →
  run_pipeline_update → apply_governance_and_egress`.

## 3. The reusable onboarding job

Every job wired up in §2 above onboards one or more *specific* `test_specs/*.json` files,
hardcoded into that job's own resource YAML as a notebook task's
`base_parameters.spec_file_path`. `resources/onboarding_job.yml` (resource key
`onboarding_job`, displayed in the Jobs UI as **"FlowX Config Onboarding"**) is
different: a single, generic job that can `CREATE`/`UPDATE`/`VALIDATE_ONLY` **any**
onboarding spec — JSON or YAML, from a UC Volume or Workspace Files path, any
`dataflow_group_id` — driven entirely by job parameters supplied at `databricks bundle run`
time, not by a bundle variable and not by anything baked into the resource file at deploy
time.

```yaml
resources:
  jobs:
    onboarding_job:
      name: FlowX Config Onboarding

      parameters:
        - name: spec_file_path
          default: ""
        - name: catalog
          default: ${var.catalog}
        - name: env
          default: ${bundle.target}
        - name: action_type
          default: CREATE

      tasks:
        - task_key: onboard
          notebook_task:
            notebook_path: ../notebooks/02_onboarding/02_onboarding_engine.py
            base_parameters:
              spec_file_path: "{{job.parameters.spec_file_path}}"
              catalog: "{{job.parameters.catalog}}"
              env: "{{job.parameters.env}}"
              action_type: "{{job.parameters.action_type}}"
          environment_key: framework_env
```

`spec_file_path` defaults to `""` — the Databricks Jobs schema requires every declared job
parameter to have a default value, but `02_onboarding_engine.py` itself raises a plain
`ValueError` (`"The 'spec_file_path' widget is required."`) the instant it sees an empty
string, so a run that forgets to override it fails fast and obviously rather than silently
no-op'ing or onboarding some unrelated leftover spec. `catalog`/`env`/`action_type` default
to this project's usual dev-loop values (`${var.catalog}`, the current bundle target,
`CREATE`) but can be overridden the same way.

Usage:

```bash
databricks bundle run onboarding_job --target dev \
  --params spec_file_path=/Workspace/.../test_specs/spec_09_reconciliation_volume_vs_cdc.json,catalog=poc,env=dev,action_type=CREATE

# VALIDATE_ONLY: runs the same structural/type/SQL-syntax validation and writes an
# onboarding_audit_log row, but never upserts control-table rows — useful as a CI check
# before a real deploy.
databricks bundle run onboarding_job --target dev \
  --params spec_file_path=/Workspace/.../test_specs/spec_09_reconciliation_volume_vs_cdc.json,catalog=poc,env=dev,action_type=VALIDATE_ONLY
```

This is a plain **notebook-task** job — `{{job.parameters.*}}` substituted into
`base_parameters` is a stable, **GA** Databricks Jobs feature. That distinction matters:
Databricks also has a `pipeline_task.parameters` mechanism for injecting a runtime value
into a *pipeline* task specifically, but it's currently Beta and — confirmed against
Databricks' own documentation while designing this job — readable only from **SQL** pipeline
source code, not Python. This framework's engine notebook
(`03_lakeflow_declarative_pipeline.py`) has to stay Python — it uses `mapInPandas` for ASN.1
decoding, [`dlt.create_sink`/`@dlt.append_flow`](https://docs.databricks.com/aws/en/dlt/dlt-sinks.html)
for sink targets, and a [custom PySpark Data Source](https://docs.databricks.com/aws/en/pyspark/datasources.html)
for the PGP-ZIP sink, none of which a SQL pipeline can express — so
`pipeline_task.parameters` was never an option for parameterizing *this* framework's actual
DLT pipeline, only for a plain notebook task, which is exactly what `onboarding_job` is. See
§3.1 for what this means for the pipeline resources themselves.

### 3.1 Why the pre-existing per-suite job/pipeline resource files still hardcode `configuration:`

The six job/pipeline resource-file pairs —
`metadata_framework_job.yml`; `sample_pipelines_job.yml` + `sample_pipelines.yml`;
`bt_group_test_suite_job.yml` + `bt_group_pipelines.yml`;
`crypto_abac_exhaustive_test_job.yml` + `crypto_abac_exhaustive_pipeline.yml`;
`auto_ttl_verification_job.yml` + `auto_ttl_verification_pipeline.yml`; and
`optional_fields_verification_job.yml` — are left **structurally untouched** relative to
newer additions: same task graphs, same one-dedicated-pipeline-per-test-scenario resource
shape, same `configuration: dataflow.group.id: dfg_<literal>` in every pipeline block. This
is deliberate, not an oversight (see `resources/onboarding_job.yml`'s own header comment) —
new orchestration is purely additive alongside them, never a replacement.
`spec_01_ingest_gcs_autoloader_quarantine.json`, for instance, still carries a legacy
filename, but the `source_type` inside the file itself is the current value
`"autoloader"`.

The reason every pipeline resource in this project — old and new alike — still hardcodes its
`dataflow.group.id` into a `configuration:` block rather than accepting it as a runtime
parameter traces back to the same Python-vs-SQL constraint from §3 above.
`resources/sample_pipelines.yml` established the pattern this project has followed ever
since (its own header comment says as much): one dedicated Lakeflow Declarative Pipeline
resource per test scenario, all pointing at the exact same generic
`03_lakeflow_declarative_pipeline.py` engine notebook, differing *only* in the
`dataflow.group.id` configuration value baked in at `databricks bundle deploy` time. That
resource-per-group shape exists specifically because a Python pipeline's only way to receive
a configuration value at all is `spark.conf.get(...)` reading the pipeline's own deploy-time
`configuration:` block (see `notebooks/03_engine/03_lakeflow_declarative_pipeline.py`, which
does exactly this for both `dataflow.group.id` and `dataflow.control.catalog`) — there is no
supported way for a Python DLT pipeline to read a value injected at `bundle run` time the
way `onboarding_job`'s plain notebook task can. A single, generically-reusable *pipeline*
resource (the pipeline-side equivalent of `onboarding_job`) genuinely isn't buildable today;
the closest available substitute is what this project already does — one resource block per
group, which still proves the *engine* is metadata-driven (a new scenario is a new spec plus
one resource block, never an engine-code change) even though the *pipeline resource* itself
isn't runtime-parameterized. Don't "fix" this into a `pipeline_task.parameters` attempt — it
was researched for this pass and confirmed not to reach a Python pipeline's source.

### 3.2 Newest example of the pattern: `new_27_08_test_pipeline` / `new_27_08_test_job`

`resources/new_27_08_test_pipeline.yml` + `resources/new_27_08_test_job.yml` are the newest
pipeline in the project and follow this same established pattern — proof it's still the
right shape for a brand-new pipeline, not just legacy inertia. The pipeline resource is the
one deliberate exception to `${var.catalog}`: `catalog`/`dataflow.control.catalog` are the
literal string `test_2026_08_07`, not `${var.catalog}`, because this is the one pipeline in
the project intentionally scoped to its own dedicated test catalog rather than the shared
`poc` catalog every other pipeline resource targets:

```yaml
configuration:
  dataflow.group.id: dfg_new_27_08_test_flagship
  dataflow.control.catalog: test_2026_08_07
```

`new_27_08_test_job` chains:

```
setup_control_tables -> onboard_spec_24 -> run_pipeline_update
  -> apply_governance_and_egress
  -> run_flagship_reconciliation
```

(the last two run in parallel, both depending only on `run_pipeline_update`). Its own
`onboard_spec_24` task inlines a hardcoded `spec_file_path` exactly like the six legacy jobs
— it could equally have called the reusable `onboarding_job` instead, but every sample/test
job in this project inlines its own onboarding task by convention, and this one follows that
convention rather than introducing a one-off cross-job dependency (see the job resource
file's own header comment). Run it with:

```bash
databricks bundle run new_27_08_test_job
```

`test_specs/spec_24_new_27_08_test_flagship.json` is the widest single-pipeline exercise in
the project: PII column encryption (ingestion) plus decryption with `cast_to_type`
validation (transformation), governance tags, SCD1 with framework-computed hash columns, a
`target_type: "sink"` direct-to-Volume export with no intermediate table, a
`target_type: "external_sink"` PGP+ZIP egress, and a reconciliation flow comparing two
independently-materialized SCD1 tables (`customer_scd1`/`customer_scd1_replica`) fed by the
same shared ingestion source — the "shared ingestion/reconciliation table" scenario.

## 4. Run an end-to-end test pass

```bash
databricks bundle run metadata_framework_job
```

This will:

1. Create the `config` schema + control tables (idempotent).
2. Provision the `landing`/`egress` Unity Catalog Volumes and copy `sample_data/*` fixtures
   into place (see `notebooks/00_seed_sample_data/00_seed_sample_data.py` for exactly
   which files go where, and which "already exists" reference tables — `raw_orders`,
   `dim_accounts`, `raw_fx_rates` — it seeds directly since none of the six sample specs
   has an ingestion flow that produces them).
3. Onboard all six `test_specs/*.json` scenarios (each independently — a failure in one
   doesn't block the others, since they all only `depends_on: seed_sample_data`).
4. Run the pipeline (whichever `dataflow_group_id` the bundle variable points at).
5. Apply governance tags and capture SCD/CDC change counts for that group
   (`notebooks/04_governance/04_apply_governance_and_egress.py` — the `apply_abac` widget
   name is a legacy holdover, but it now drives `apply_all_governance_tags`, the tags-only
   model described in [01_control_metadata_schema.md §6](01_control_metadata_schema.md#6-governance_tags-ingestion-and-transformation-flows).
   `external_sink`/`sink` exports no longer run here at all — every one is now a genuine
   `dlt.create_sink`/`@dlt.append_flow` registered inside the pipeline's own graph and
   already ran as part of step 4; a `run_egress_exports` job parameter, if still passed by
   an older resource file, is silently ignored).

To exercise a *different* `dataflow_group_id` than the one the pipeline resource is
currently configured for, redeploy with `--var`, then `databricks bundle run
metadata_lakeflow_pipeline` directly.

### Testing `FULL_SNAPSHOT_CDC_NO_PK` (spec_03) end to end

The seeding notebook only copies `sample_mainframe_customer_master_day1.csv`. After the
first pipeline update has run, manually copy
`sample_data/sample_mainframe_customer_master_day2.csv` into the same
`/Volumes/{catalog}/landing/mainframe_raw_zone/daily_customer_master/` path and trigger a
second pipeline update — you should see John Smith's status update, Wei Zhang disappear,
Fatima Ali's city update, and Noah Kim appear as a new row, exactly as described in
[02_cdc_load_strategies.md](02_cdc_load_strategies.md).

### Testing `asn1` ingestion end to end (spec_02)

The decoded-CDR fixture (`sample_data/sample_telecom_cdr_decoded.json`) is always
available, but the *actual* end-to-end path needs a real BER-encoded, AES-encrypted ZIP.
`asn1tools`/`pyzipper` are now declared project dependencies (`pyproject.toml`), installed
automatically by `uv sync --dev`:

```bash
python sample_data/generate_sample_datasets.py   # writes sample_data/encoded/cdr_batch.zip
databricks bundle run metadata_framework_job      # seeding notebook picks it up automatically
```

If that file is absent, `00_seed_sample_data.py` logs a warning and skips that one copy
step — everything else in the job still runs.

**Known limitation:** if you do generate `cdr_batch.zip` locally, be aware that
Databricks Workspace Files import auto-extracts `.zip`-suffixed uploads (see
[12_zip_ingestion_pipeline.md](12_zip_ingestion_pipeline.md)'s "Platform gotcha" section)
-- a pre-built `.zip` synced this way will not arrive intact. `spec_02`'s copy step has
carried this latent issue since early in the project; the newer ZIP-ingestion and ASN.1
sample pipelines avoid it by generating their binary fixtures directly on the cluster at
seed time instead of syncing pre-built archives.

### Test Pipelines 1/2, reconciliation, ZIP ingestion, ASN.1 DQ/quarantine

`resources/sample_pipelines_job.yml` (`sample_pipelines_job`) exercises everything added
beyond the original six specs -- self-contained (provisions its own control schema/seed
data, doesn't require `metadata_framework_job` to have run first):

```bash
databricks bundle run sample_pipelines_job
```

This workspace's serverless compute quota is limited (a free-tier `RESOURCE_EXHAUSTED`
error was observed with 3 concurrent Lakeflow pipeline updates) -- the job deliberately
chains its 5 DLT pipeline-update tasks sequentially rather than in parallel. If your
workspace has more serverless capacity, feel free to relax those `depends_on` edges for
faster wall-clock time.

To observe Test Pipeline 1's SCD1 overwrite / SCD2 history-row behavior, drop
`sample_data/sample_customer_profile_day2.csv` into
`/Volumes/{catalog}/landing/customer_ops_raw_zone/customer_profile/` and re-run
`customer_360_volume_scd_pipeline`. To observe Test Pipeline 2's insert/update/delete
handling, re-run `00_seed_sample_data.py`'s
`seed_zerobus_event_table_from_csv("sample_account_events_batch2.csv", ...)` call (or the
whole notebook, after adding it to the seeding plan) and re-run
`account_events_streaming_cdc_pipeline`. To observe the wide-table SCD1
`columns_to_exclude` behavior, drop `sample_data/sample_wide_customer_master_day2.csv`
into `/Volumes/{catalog}/landing/wide_customer_master_zone/incoming/` and re-run
`wide_customer_scd1_exclusion_pipeline` -- see
[18_test_pipeline_scd1_wide_table_column_exclusion.md](18_test_pipeline_scd1_wide_table_column_exclusion.md).
To observe reconciliation's idempotency, just run `sample_pipelines_job` a second time
unchanged.

## 5. Local / IDE development

```bash
uv sync --dev            # installs pytest, ruff, databricks-connect, etc.
uv run pytest            # existing tests/ (unrelated scaffold tests; add framework unit
                          # tests under tests/ following the same pattern if desired)
```

Because every notebook's bootstrap cell falls back to adding `../../src` to `sys.path`
when the wheel isn't installed, you can open and run any notebook under `notebooks/`
directly against a cluster attached via Databricks Connect / the VS Code extension without
running `bundle deploy` first — useful while iterating on `lakeflow_framework` code.

## 6. Rolling back / redeploying

`databricks bundle deploy` is idempotent — redeploying after a code change simply updates
the existing pipeline/job definitions and uploads a new wheel (see §1 — every deploy builds
a fresh, uniquely-versioned wheel via `scripts/bump_and_build.py` rather than overwriting
the previous one, so an already-running pipeline/job update is never affected by a deploy
that happens mid-run). To remove everything this bundle created: `databricks bundle
destroy` (this does **not** drop the Unity Catalog schemas/tables/volumes created by
`01_setup`/`00_seed_sample_data` — those are data, not bundle-managed resources, and must be
dropped manually if you want a full teardown; it also doesn't touch `dist/` or
`dist/archive/`, which are local build output, not bundle-managed either).
