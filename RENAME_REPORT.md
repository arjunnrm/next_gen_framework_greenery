# Rename report — Metaflow / NextGen Metadata Framework → FlowX

Branch `rename/flowx`, 4 commits on top of `aa93b56`. Not pushed.

**830 files changed, 6831 insertions(+), 5994 deletions(-)** — 346 renames (`R`), 471 modified
(`M`), 9 added, 4 deleted. Every path change used `git mv`; `git log --follow` traces through the
rename into pre-existing history.

## Substitution rules applied

Case-sensitive, longest-token-first, anchored on exact tokens (never a broad regex — this repo has
twice been damaged by one):

| From | To | Count |
|---|---|---|
| `metaflow` | `flowx` | 3856 |
| `NextGen_Metadata_Framework` | `flowx` | 655 |
| `Metaflow` | `FlowX` | 1019 |
| `NextGen Metadata Framework` | `FlowX` | 667 |
| `METAFLOW` | `FLOWX` | 68 |
| `MetaFlow` | `FlowX` | 65 |
| `nextgen_metadata_framework` / `nextgen-metadata-framework` | `flowx` | 13 |

`MetaFlow` (65 occurrences) was a fifth case variant not in the original brief; mapped to `FlowX`.

## What changed, by area

| Area | Change |
|---|---|
| Python package | `src/NextGen_Metadata_Framework/` → `src/flowx/` (81 files, all `R`). Every import, logger name, test reference. Wheel is now `flowx-0.0.3-py3-none-any.whl`. |
| Version | `pyproject.toml` `0.0.2` → `0.0.3`, and `framework_version` in `databricks.yml` to match (the repo requires these to be equal). `uv.lock` regenerated. |
| Bundle | Bundle name → `flowx`; target `dev_metaflow` → `dev_flowx`. |
| Unity Catalog | Catalog `metaflow` → `flowx`; schema `metaflow_sample` → `flowx_sample`. **Requires manual migration — see below.** |
| Resources | 258 paths renamed: `resources/metaflow_{app,bi,bootstrap,config_jobs}/` → `flowx_*`, and every `metaflow_test_*` / `metaflow_sample_*` job, pipeline and spec. |
| Test assets | `metaflow_testing/` → `flowx_testing/`; `sample_data/metaflow_testing/` → `sample_data/flowx_testing/`; `tests/integration/test_metaflow_*.py` → `test_flowx_*.py`. |
| Databricks App | Registered name `metaflow-onboarding` → `flowx-onboarding`; env vars `METAFLOW_*` → `FLOWX_*`; `web/dist/` rebuilt; `docs_site/` regenerated. |
| Agent skill | `.claude/skills/metaflow-onboarding/` → `flowx-onboarding/`, copies re-synced. |
| Docs | 105 files; derived trees regenerated (`build_docs_reference.py`, `mkdocs build`, `build_app_docs.py`). |

## Verification

| Check | Result |
|---|---|
| `pytest tests/unit` | **9 failed, 1302 passed, 117 errors** — identical to baseline. **0 new failures** (set-compared by test id, not by count). |
| `pytest databricks-app/tests` | **163 passed, 19 skipped, 0 failed** (was 3 failed + 1 collection error mid-rename). |
| `databricks bundle validate` | **`Validation OK!`** on `dev_flowx`, `arjun_2`, `arjun_3` — with `artifact_path` pointed at an existing volume. Against the real path it fails on the missing `flowx` catalog, which is the expected consequence of the catalog rename, not a defect. |
| `${resources.*}` references | 85 references resolve against 135 keys; **0 dangling**, 0 keys still containing `metaflow`. |
| YAML / JSON parse | All 130 YAML and 67 JSON files parse. |
| `node --check` / esbuild | `Builder.jsx`, `Shell.jsx`, `registry.js` parse. Line counts 1784 / 978 / 538 — nothing truncated. |
| mkdocs warnings | **231 before, 231 after** (verified by building the original commit in a throwaway worktree) — no doc link regressed. |
| Residual scan | 0 tracked files or paths still named `metaflow`, except the 3 intentional cases below. |

The unit baseline is flaky: two consecutive pre-rename runs gave `9 failed / 1302 passed` and
`34 failed / 1277 passed` with no code change between them. 117 errors in every run are
`ValueError: default auth: cannot configure default credentials` — sandbox noise. Comparison was
therefore done on the **set of failing test ids**, which is why "0 new failures" is meaningful
where a count comparison would not be.

One regression *was* introduced and fixed: renaming the framework made
`test_agent_skill_layout.py` expect `.claude/skills/flowx-onboarding/`. Renaming that directory
and running `scripts/sync_agent_skill.py` cleared all 10.

## Deliberately NOT renamed

- **The upstream open-source Metaflow (Netflix/Outerbounds) — no dependency exists.** Verified: no
  `metaflow` requirement in `pyproject.toml`, `uv.lock` or `databricks-app/requirements.txt`, and no
  `import metaflow` anywhere. Nothing in this rename touches that project. There was nothing to flag.
- **CLI profile `dev_metaflow`.** That name lives in the operator's `~/.databrickscfg` — outside the
  repo — and is a live credential. Renaming it in `databricks.yml` would break auth for anyone whose
  local profile is unchanged. Deploy with `-t dev_flowx -p dev_metaflow`. Rename it locally if you
  prefer, then update `profile:` to match.
- **Absolute filesystem paths naming the repo directory.** The folder on disk is still
  `C:\Databricks\NextGen_Metadata_Framework`. The rename rewrote 8 such links in
  `flowx_testing/README.md` to `/Databricks/flowx/`, which does not exist; restored.
- **`RELEASE_NOTES.md` mentions of the old names** — a changelog must name what was renamed.

## Post-merge migration checklist (manual, in order)

The catalog rename cannot be completed by a find/replace. Until steps 1–3 are done, pipelines and
the control plane do not run.

1. Create the `flowx` catalog in the workspace UI (Catalog → Create catalog → Default storage).
   DABs cannot declare it: UC Default Storage rejects `CREATE CATALOG` without a managed location.
2. `databricks bundle deploy -t dev_flowx -p dev_metaflow` to create the `config`, `dev` and
   `flowx_sample` schemas and the Volumes. **Not while a pipeline or test wave is running** —
   `bundle deploy` prunes superseded artifacts and will kill a running update.
3. Migrate control-table data `metaflow.config.*` → `flowx.config.*` (`DEEP CLONE` or `CTAS`).
   38 unique three-part table names are affected, including
   `dataflow_group_spec`, `reconciliation_flow_spec`, `reconciliation_run_log`,
   `reconciliation_mismatch_log`, `preflight_check_onboarding_spec`. Re-onboarding regenerates the
   spec rows but **not** run history.
4. Re-provision the UC secret `flowx.flowx_sample.sample_zip_passkey` (samples 04/05).
5. Rebuild and redeploy the wheel so pipelines resolve `flowx-0.0.3`.
6. Rename the app in the workspace if the old `metaflow-onboarding` instance persists — the URL
   changes with the name.

## Outstanding / flagged

- **Scope item 3 had an unfilled placeholder** — "[INSERT CONTENT HERE — the text/config you want
  added to the app]". Nothing was invented for it. The logo was wired up per the trailing
  instruction; anything else intended for the app is still to be supplied.
- **The logo is the *hoonartek* company wordmark, not a FlowX product mark.** It is placed beside
  the "FlowX" product name rather than replacing it. Confirm that is the intended branding.
- **`logo.png` is 713 KB (13056×2213)** — far larger than a 22px-tall header needs. It is served at
  full size on every page load. Downscaling to ~44px tall would cut it to a few KB; Pillow is not in
  the venv and installing it would be pruned by `uv sync`, so this was left rather than done badly.
- **`databricks-app/_cfg.json`** is tracked but referenced by no code — a stale artifact, possible
  deletion candidate outside this rename.
- **4 resource folders are absent from `databricks.yml` `include:`** (`bt_tests`, `feature_tests`,
  `observability`, `stability_tests`), so they deploy nothing. Pre-existing — the same test failed
  identically before the rename, and it reports "stale include lines match nothing: []", confirming
  the rename left `include:` and the folder tree consistent.
- **`pyproject.toml` name is `flowx`** (lowercase), matching the import name.
