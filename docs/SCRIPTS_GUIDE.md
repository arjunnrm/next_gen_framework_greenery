# Scripts Guide

Every script in `scripts/`: what it is for, when you need it, and how to run it.
One line of purpose, then the command. Details live in each script's own docstring.

Run everything from the repository root.

---

## Build & release

### `bump_and_build.py`
Builds the framework wheel with a unique per-deploy filename. **You rarely call this
directly** — `databricks.yml` invokes it via `artifacts.framework_wheel.build` on every
`bundle deploy`.

```bash
python scripts/bump_and_build.py
```

### `build_and_upload_wheel.py`
Builds the wheel and publishes it to a Unity Catalog Volume, printing the resolved
`/Volumes/...` path on the last stdout line. Use when a pipeline must pin a wheel that
`bundle deploy` will not prune.

```bash
python scripts/build_and_upload_wheel.py --profile dev --catalog flowx
```

### `archive_deployed_wheel.py`
Copies the wheel a `bundle deploy` just published into a prune-proof archive folder, so a
later deploy cannot delete a wheel a running pipeline still needs. Idempotent — an
already-archived wheel is reported as `skip`.

```bash
python scripts/archive_deployed_wheel.py --profile arjun --catalog flowx
python scripts/archive_deployed_wheel.py --profile arjun --prune-keep 20
```

> **Never deploy while a pipeline or test wave is running.** `bundle deploy` prunes
> superseded artifacts from `<artifact_path>/.internal/`, killing an in-flight update with
> `ENVIRONMENT_PIP_INSTALL_ERROR`. See `flowx_testing/TESTING_PLAN.md` §0.

---

## Environment setup

### `bootstrap_workspace.py`
Prepares a brand-new workspace: catalogs, schemas and the Volumes the bundle expects.
Run once per workspace, **before** the first `bundle deploy`.

```bash
python scripts/bootstrap_workspace.py --profile <profile> --catalog flowx
```

---

## Documentation

### `build_docs_reference.py`
Regenerates the derived documentation trees so they cannot drift from source:
`docs/reference/json/` from the app registry, `docs/reference/code/` from `src/flowx/`
(via AST — nothing is imported), and stages `BT_Usecase/<UC>/docs` into `docs/UC*/` for
mkdocs. Run after changing any spec attribute, framework module, or use-case document.

```bash
python scripts/build_docs_reference.py
python scripts/build_docs_reference.py --check   # CI: fail if committed output is stale
```

### `build_app_docs.py`
Builds the mkdocs site into the Databricks App's `docs_site/`, so the app ships the wiki.

```bash
python scripts/build_app_docs.py
```

### `sync_agent_skill.py`
Copies `agent_skills/` into `.claude/skills/`. **Required after editing anything under
`agent_skills/`** — `test_agent_skill_layout.py` compares the two trees byte-for-byte and
fails on drift (line endings included).

```bash
python scripts/sync_agent_skill.py
```

### `build_golden_specs.py`
Regenerates the golden onboarding specs the agent skill references.

```bash
python scripts/build_golden_specs.py
```

---

## Test data generation

All three write **[Simulated]** data. See
[Data Provenance Classification](DATA_PROVENANCE_CLASSIFICATION.md) for what is
customer-provided and must never be regenerated.

### `generate_uc3_test_data.py`
Generates UC3 Excalibur streaming + batch CSVs. Column names, types and governance flags
are read at runtime from the three Excalibur DDL sheets in `BT_Usecase/UC3/data/` — no
column list is hand-typed. Local by default; `--upload` is opt-in.

```bash
python scripts/generate_uc3_test_data.py --out-dir build/uc3_test_data
python scripts/generate_uc3_test_data.py --out-dir build/uc3_test_data --upload --profile <p>
```

### `generate_uc6_test_data.py`
Generates the **augmented** UC6 fixture. The supplied bundle has no postcode overlap and
no CSS join-key overlap, so it can only ever exercise the no-match path; this fixture hits
every branch of the decision table. The supplied bundle is never modified.

```bash
python scripts/generate_uc6_test_data.py [--out <dir>] [--passphrase <pw>]
```

### `generate_synthetic_ber.py`
Generates deterministic synthetic BER payloads (10 concatenated records per file) for the
five real ASN.1 modules in `BT_Usecase/UC7/data/asn_schema/`. These are **load-bearing
test fixtures** — `tests/unit/test_asn1_root_pdu_detection.py` skips rather than fails
without them.

```bash
python scripts/generate_synthetic_ber.py
```

---

## UC6 verification

### `validate_uc6_schema_configs.py`
Checks the five UC6 `schema_configs/*.json` against their source layouts.

```bash
python scripts/validate_uc6_schema_configs.py
```

### `verify_uc6_business_rules.py`
Runs **the spec's own transformation SQL** (not a copy) offline via sqlglot + DuckDB and
checks the decision table. Deliberately not a pytest: `tests/conftest.py` eagerly builds a
Spark session, and there is no local Java. Caveat: DuckDB is not Spark — strong evidence,
not proof.

```bash
python scripts/verify_uc6_business_rules.py
```

### `validate_uc6_pipeline_output.py`
Validates a completed UC6 pipeline run's output tables in the workspace.

```bash
python scripts/validate_uc6_pipeline_output.py --profile <profile> --catalog flowx
```

---

## Maintenance

### `cleanup_dead_code.py`
Static-analysis-backed removal of obsolete scaffold files and empty directories.
Inspect its plan before trusting a run — it deletes.

```bash
python scripts/cleanup_dead_code.py
```
